package leveldb

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"io"
	"leveldb/collections"
	"leveldb/comparer"
	"leveldb/errors"
	"leveldb/options"
	"leveldb/storage"
	"leveldb/utils"
	"leveldb/wal"
	"sort"
	"sync"
)

type VersionSet struct {
	versions    *list.List
	current     *Version
	compactPtrs [options.KLevelNum]compactPtr
	cmp         comparer.BasicComparer

	comparerName   []byte
	nextFileNum    uint64
	stJournalNum   uint64
	stSeqNum       Sequence // current memtable start seq num
	manifestFd     storage.Fd
	manifestWriter *wal.JournalWriter

	tableOperation *tableOperation
	tableCache     *TableCache
	storage        storage.Storage
}

type Version struct {
	element *list.Element
	vSet    *VersionSet
	*utils.BasicReleaser
	levels [options.KLevelNum]tFiles

	// compaction
	cScore float64
	cLevel int
}

func newVersion(vSet *VersionSet) *Version {
	return &Version{
		vSet: vSet,
	}
}

type vBuilder struct {
	vSet     *VersionSet
	base     *Version
	inserted [options.KLevelNum]*tFileSortedSet
	deleted  [options.KLevelNum]*uintSortedSet
}

func newBuilder(session *VersionSet, base *Version) *vBuilder {
	builder := &vBuilder{
		vSet: session,
		base: base,
	}
	for i := 0; i < options.KLevelNum; i++ {
		builder.inserted[i] = newTFileSortedSet()
	}
	for i := 0; i < options.KLevelNum; i++ {
		builder.deleted[i] = newUintSortedSet()
	}
	return builder
}

func (builder *vBuilder) apply(edit VersionEdit) {
	for _, cPtr := range edit.compactPtrs {
		builder.vSet.compactPtrs[cPtr.level] = cPtr
	}
	for _, delTable := range edit.delTables {
		level, number := delTable.level, delTable.number
		builder.deleted[level].add(number)
	}
	for _, addTable := range edit.addedTables {
		level, number := addTable.level, addTable.number
		builder.deleted[level].remove(number)
		builder.inserted[level].add(addTable)
	}
}

func (builder *vBuilder) saveTo(v *Version) {

	for level := 0; level < options.KLevelNum; level++ {
		baseFile := v.levels[level]
		beginPos := 0
		iter := builder.inserted[level].NewIterator()
		v.levels[level] = make(tFiles, 0, len(baseFile)+builder.inserted[level].size) // reverse pre alloc capacity
		for iter.Next() {
			addTable, ok := iter.Value().(tFile)
			if !ok {
				panic("vBuilder iter convert value to tFile failed...")
			}
			pos := upperBound(baseFile, level, iter, builder.vSet.cmp)
			for i := beginPos; i < pos; i++ {
				builder.maybeAddFile(v, baseFile[i], level)
			}
			builder.maybeAddFile(v, addTable, level)
			beginPos = pos
		}

		for i := beginPos; i < len(baseFile); i++ {
			builder.maybeAddFile(v, baseFile[i], level)
		}
	}

}

func upperBound(s tFiles, level int, iter *BTreeIter, cmp BasicComparer) int {

	ok, ikey, fileNum := decodeBinaryToTFile(iter.Key())
	if !ok {
		panic("leveldb decodeBinaryToTFile failed")
	}

	if level == 0 {
		idx := sort.Search(len(s), func(i int) bool {
			return s[i].fd.Num > fileNum
		})
		return idx
	}
	idx := sort.Search(len(s), func(i int) bool {
		return cmp.Compare(s[i].iMax, ikey) > 0
	})
	return idx
}

func (builder *vBuilder) maybeAddFile(v *Version, file tFile, level int) {

	if builder.deleted[level].contains(file.fd.Num) {
		return
	}

	files := v.levels[level]
	cmp := builder.vSet.cmp
	if level > 0 && len(files) > 0 {
		assert(cmp.Compare(files[len(files)-1].iMax, file.iMin) < 0)
	}

	v.levels[level] = append(v.levels[level], file)
}

type uintSortedSet struct {
	*anySortedSet
}

func newUintSortedSet() *uintSortedSet {
	uSet := &uintSortedSet{
		anySortedSet: &anySortedSet{
			BTree:                 InitBTree(3, &uint64Comparer{}),
			anySortedSetEncodeKey: encodeUint64ToBinary,
		},
	}
	return uSet
}

func encodeUint64ToBinary(item interface{}) (bool, []byte) {

	num, ok := item.(uint64)
	if !ok {
		return false, nil
	}

	switch {
	case num < uint64(1<<16)-1:
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(num))
		return true, buf
	case num < uint64(1<<32)-1:
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(num))
		return true, buf
	default:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, num)
		return true, buf
	}
}

func decodeBinaryToUint64(b []byte) uint64 {

	size := len(b)
	var value uint64
	switch {
	case size == 2:
		value = uint64(binary.LittleEndian.Uint16(b))
	case size == 4:
		value = uint64(binary.LittleEndian.Uint32(b))
	case size == 8:
		value = binary.LittleEndian.Uint64(b)
	default:
		panic("unsupport type decode to uint64")
	}
	return value
}

type uint64Comparer struct{}

func (uc uint64Comparer) Compare(a, b []byte) int {
	uinta, uintb := decodeBinaryToUint64(a), decodeBinaryToUint64(b)
	if uinta < uintb {
		return -1
	} else if uinta == uintb {
		return 0
	} else {
		return 1
	}
}

func (uc uint64Comparer) Name() []byte {
	return []byte("leveldb.uint64comparator")
}

type tFileSortedSet struct {
	*anySortedSet
}

func newTFileSortedSet() *tFileSortedSet {
	tSet := &tFileSortedSet{
		anySortedSet: &anySortedSet{
			BTree:                 collections.InitBTree(3, &tFileComparer{}),
			anySortedSetEncodeKey: encodeTFileToBinary,
		},
	}
	return tSet
}

type tFileComparer struct {
	*iComparer
}

func (tc *tFileComparer) Compare(a, b []byte) int {

	ia := a[:len(a)-8]
	ib := a[:len(b)-8]
	r := tc.iComparer.Compare(ia, ib)
	if r != 0 {
		return r
	}

	if aNum, bNum := binary.LittleEndian.Uint64(a[len(a)-8:]), binary.LittleEndian.Uint64(b[len(b)-8:]); aNum < bNum {
		return -1
	}
	return 1
}

func (tc *tFileComparer) Name() []byte {
	return []byte("leveldb.tFilecomparator")
}

func encodeTFileToBinary(item interface{}) (bool, []byte) {
	tFile, ok := item.(tFile)
	if !ok {
		return false, nil
	}
	fileNum := make([]byte, 8)
	binary.LittleEndian.PutUint64(fileNum, tFile.fd.Num)
	key := append(tFile.iMax, fileNum...)
	return true, key
}

func decodeBinaryToTFile(b []byte) (bool, InternalKey, uint64) {
	if len(b) < 16 {
		return false, nil, 0
	}
	fileNumBuf := b[len(b)-8:]
	fileNum := binary.LittleEndian.Uint64(fileNumBuf)
	return true, InternalKey(b[:len(b)-8]), fileNum
}

type anySortedSet struct {
	*collections.BTree
	anySortedSetEncodeKey
	addValue bool
	size     int
}

type anySortedSetEncodeKey func(item interface{}) (bool, []byte)

func (set *anySortedSet) add(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet add item encode failed, please check...")
	}
	if !set.Has(key) {
		if set.addValue {
			set.Insert(key, item)
		} else {
			set.Insert(key, nil)
		}
		set.size++
		return true
	}
	return false
}

func (set *anySortedSet) remove(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet remove item encode failed, please check...")
	}
	ok = set.Remove(key)
	if ok {
		set.size--
	}
	return ok
}

func (set *anySortedSet) contains(item interface{}) bool {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet contains item encode failed, please check...")
	}
	return set.Has(key)
}

// LogAndApply apply a new version and record change into manifest file
// noted: thread not safe
// caller should hold a mutex
func (vSet *VersionSet) logAndApply(edit *VersionEdit, mutex *sync.RWMutex) error {

	utils.AssertMutexHeld(mutex)

	/**
	case 1: compactMemtable
		edit.setReq(db.frozenSeqNum)
		edit.setJournalNum(db.journal.Fd)
		edit.addTable(xx)

	case 2:
		table compaction:
			edit.addTable(xx)
			edit.delTable(xx)

	conclusion: all compaction need to be serialize execute.
	but minor compaction, only affect level 0 and is add file. major compaction not affect minor compaction.
	so we can let mem compact and table compact concurrently execute. when install new version, we need a lock
	to protect.
	*/
	if edit.hasRec(kJournalNum) {
		utils.Assert(edit.journalNum >= vSet.stJournalNum)
		utils.Assert(edit.journalNum < vSet.nextFileNum)
	} else {
		edit.journalNum = vSet.stJournalNum
	}

	if edit.hasRec(kSeqNum) {
		utils.Assert(edit.lastSeq >= vSet.stSeqNum)
	} else {
		edit.lastSeq = vSet.stSeqNum
	}

	edit.setNextFile(vSet.nextFileNum)

	// apply new version
	v := newVersion(vSet)
	builder := newBuilder(vSet, vSet.current)
	builder.apply(*edit)
	builder.saveTo(v)
	finalize(v)

	mutex.Unlock() // cause compaction is run in single thread, so we can avoid expensive write syscall

	var (
		writer         storage.SequentialWriter
		storage        = vSet.storage
		manifestWriter = vSet.manifestWriter
		manifestFd     = vSet.manifestFd
		newManifest    bool
		err            error
	)

	if manifestWriter == nil {
		newManifest = true
	}

	if manifestWriter != nil && manifestWriter.size() >= kManifestSizeThreshold {
		newManifest = true
		manifestFd = Fd{
			FileType: KJournalFile,
			Num:      vSet.allocFileNum(),
		}
	}

	if newManifest {
		writer, err = storage.Create(manifestFd)
		if err == nil {
			manifestWriter = NewJournalWriter(writer)
			err = vSet.writeSnapShot(manifestWriter) // write current version snapshot into manifest
		}
	}

	if err == nil {
		edit.EncodeTo(manifestWriter)
		err = edit.err
	}

	if err == nil {
		err = storage.SetCurrent(manifestFd.Num)
		if err == nil {
			vSet.manifestFd = manifestFd
			vSet.manifestWriter = manifestWriter
		}
	}

	mutex.Lock()

	if err == nil {
		vSet.appendVersion(v)
		vSet.stSeqNum = edit.lastSeq
		vSet.stJournalNum = edit.journalNum
	} else {
		if newManifest {
			err = storage.Remove(manifestFd)
		}
	}

	return err
}

// required: mutex held
// noted: thread not safe
func (vSet *VersionSet) appendVersion(v *Version) {
	element := vSet.versions.PushFront(v)
	old := vSet.current
	if old != nil {
		old.UnRef()
	}
	v.element = element
	vSet.current = v
	vSet.current.Ref()
}

func finalize(v *Version) {

	var (
		bestLevel int
		bestScore float64
	)

	for level := 0; level < len(v.levels); level++ {
		if level == 0 {
			length := len(v.levels[level])
			bestScore = float64(length) / kLevel0StopWriteTrigger
			bestLevel = 0
		} else {
			totalSize := uint64(v.levels[level].size())
			score := float64(totalSize / maxBytesForLevel(level))
			if score > bestScore {
				bestScore = score
				bestLevel = level
			}
		}
	}

	v.cScore = bestScore
	v.cLevel = bestLevel
}

func (vSet *VersionSet) levelFilesNum(level int) int {
	c := vSet.current
	c.Ref()
	defer c.UnRef()
	return len(c.levels[level])
}

func (vSet *VersionSet) recover(manifest Fd) (err error) {

	var (
		hasComparerName, hasLogFileNum, hasNextFileNum bool
		comparerName                                   []byte
		logFileNum                                     uint64
		seqNum                                         Sequence
		nextFileNum                                    uint64
	)

	reader, rErr := vSet.storage.Open(manifest)
	if rErr != nil {
		err = rErr
		return
	}

	var (
		edit     VersionEdit
		vBuilder vBuilder
		version  Version
	)

	journalReader := NewJournalReader(reader)
	vBuilder.vSet = vSet
	for {

		chunkReader, err := journalReader.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return
		}

		edit.DecodeFrom(chunkReader)

		if edit.err != nil {
			err = edit.err
			return
		}

		if edit.hasRec(kComparerName) {
			hasComparerName = true
			if bytes.Compare(edit.comparerName, vSet.cmp.Name()) != 0 {
				err = NewErrCorruption("invalid comparator")
				return
			}
			comparerName = edit.comparerName
		}

		if edit.hasRec(kNextFileNum) {
			hasNextFileNum = true
			nextFileNum = edit.nextFileNum
		}

		if edit.hasRec(kJournalNum) {
			hasLogFileNum = true
			logFileNum = edit.journalNum
		}
		vBuilder.apply(edit)
		edit.reset()
	}

	if !hasComparerName {
		err = NewErrCorruption("missing comparer name")
		return
	}

	if !hasLogFileNum {
		err = NewErrCorruption("missing log num")
		return
	}

	if !hasNextFileNum {
		err = NewErrCorruption("missing next file num")
		return
	}

	vSet.markFileUsed(logFileNum)
	vSet.markFileUsed(nextFileNum)

	vBuilder.saveTo(&version)
	finalize(&version)
	vSet.appendVersion(&version)
	vSet.current = &version
	vSet.manifestFd = Fd{
		FileType: KDescriptorFile,
		Num:      nextFileNum,
	}
	vSet.nextFileNum = nextFileNum + 1
	vSet.stSeqNum = seqNum
	vSet.stJournalNum = logFileNum
	vSet.comparerName = comparerName
	return
}

func (vSet *VersionSet) getCurrent() *Version {
	return vSet.current
}

func (vSet *VersionSet) addLiveFiles(expected map[Fd]struct{}) {
	ele := vSet.versions.Front()
	for ele != nil {
		ver := ele.Value.(*Version)
		for _, level := range ver.levels {
			for _, v := range level {
				expected[v.fd] = struct{}{}
			}
		}
		ele = ele.Next()
	}
}

// UnRef required mutex held
func (v *Version) UnRef() int32 {
	res := v.BasicReleaser.UnRef()
	if res == 0 {
		v.vSet.versions.Remove(v.element)
	}
	return res
}

type getStat uint8

const (
	kStatNotFound getStat = iota
	kStatFound
	kStatDelete
	kStatCorruption
)

func (v *Version) get(ikey InternalKey, value *[]byte) (err error) {

	userKey := ikey.UserKey()
	stat := kStatNotFound

	match := func(level int, tFile tFile) bool {
		getErr := v.vSet.tableCache.Get(ikey, tFile, func(rkey InternalKey, rValue []byte, rErr error) {
			if rErr == errors.ErrNotFound {
				stat = kStatNotFound
			}
			ukey, kt, _, pErr := parseInternalKey(rkey)
			if pErr != nil {
				stat = kStatCorruption
			} else if bytes.Compare(ukey, userKey) == 0 {
				switch kt {
				case KeyTypeValue:
					*value = rValue
					stat = kStatFound
				case KeyTypeDel:
					stat = kStatDelete
				}
			}
			return
		})

		if getErr != nil {
			err = getErr
			return false
		}

		switch stat {
		case kStatCorruption:
			return false
		case kStatNotFound:
			return true
		case kStatDelete:
			return false
		case kStatFound:
			return false
		default:
			return false
		}
	}

	v.foreachOverlapping(ikey, match)

	if err != nil {
		return
	}

	switch stat {
	case kStatNotFound, kStatDelete:
		err = errors.ErrNotFound
	case kStatCorruption:
		err = errors.NewErrCorruption("leveldb/get key corruption")
	}

	return
}

func (v *Version) foreachOverlapping(ikey InternalKey, f func(level int, tFile tFile) bool) {
	tmp := make([]tFile, 0)
	ukey := ikey.ukey()
	for _, level0 := range v.levels[0] {
		if bytes.Compare(level0.iMin.ukey(), ukey) <= 0 && bytes.Compare(level0.iMax.ukey(), ukey) >= 0 {
			tmp = append(tmp, level0)
		}
	}
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].fd.Num > tmp[j].fd.Num
	})

	for idx := 0; idx < len(tmp); idx++ {
		if !f(0, tmp[idx]) {
			return // match case
		}
	}

	for level := 1; level < len(v.levels); level++ {
		lf := v.levels[level]
		idx := sort.Search(len(lf), func(i int) bool {
			return bytes.Compare(lf[i].iMax.ukey(), ukey) >= 0
		})
		if idx < len(lf) && bytes.Compare(lf[idx].iMin.ukey(), ukey) <= 0 {
			if !f(level, lf[idx]) {
				return
			}
		}
	}
}

func (vSet *VersionSet) allocFileNum() uint64 {
	nextFileNum := vSet.nextFileNum
	vSet.nextFileNum++
	return nextFileNum
}

func (vSet *VersionSet) reuseFileNum(fileNum uint64) bool {
	if vSet.nextFileNum-1 == fileNum {
		vSet.nextFileNum = fileNum
		return true
	}
	return false
}

func (vSet *VersionSet) markFileUsed(fileNum uint64) bool {
	if vSet.nextFileNum <= fileNum {
		vSet.nextFileNum = fileNum + 1
		return true
	}
	return false
}

func (vSet *VersionSet) loadCompactPtr(level int) InternalKey {
	if level < len(vSet.compactPtrs) {
		return nil
	}
	return vSet.compactPtrs[level].ikey
}
