package leveldb

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"io"
	"sort"
	"sync"

	"github.com/KierenEinar/leveldb/collections"
	"github.com/KierenEinar/leveldb/comparer"
	"github.com/KierenEinar/leveldb/errors"
	"github.com/KierenEinar/leveldb/options"
	"github.com/KierenEinar/leveldb/storage"
	"github.com/KierenEinar/leveldb/utils"
	"github.com/KierenEinar/leveldb/wal"
)

type VersionSet struct {
	versions    *list.List
	current     *Version
	compactPtrs [options.KLevelNum]*compactPtr
	cmp         comparer.Comparer

	comparerName   []byte
	nextFileNum    uint64
	stJournalNum   uint64
	stSeqNum       sequence // current memtable start seq num
	manifestFd     storage.Fd
	manifestWriter *wal.JournalWriter
	writer         storage.SequentialWriter
	tableOperation *tableOperation
	tableCache     *TableCache

	opt *options.Options
}

type Version struct {
	element *list.Element
	vSet    *VersionSet
	*utils.BasicReleaser
	levels [options.KLevelNum]tFiles

	// compaction
	cScore float64
	cLevel int

	fileToCompact      *tFile
	fileToCompactLevel int
}

func newVersion(vSet *VersionSet) *Version {
	return &Version{
		vSet:          vSet,
		BasicReleaser: &utils.BasicReleaser{},
	}
}

type vBuilder struct {
	vSet     *VersionSet
	base     *Version
	inserted [options.KLevelNum]*tFileSortedSet
	deleted  [options.KLevelNum]*uintSortedSet
}

func newBuilder(vSet *VersionSet, base *Version) *vBuilder {
	builder := &vBuilder{
		vSet: vSet,
		base: base,
	}
	for i := 0; i < options.KLevelNum; i++ {
		builder.inserted[i] = newTFileSortedSet(vSet.opt.InternalComparer)
	}
	for i := 0; i < options.KLevelNum; i++ {
		builder.deleted[i] = newUintSortedSet()
	}
	return builder
}

func (builder *vBuilder) apply(edit VersionEdit) {
	for idx, cPtr := range edit.compactPtrs {
		builder.vSet.compactPtrs[cPtr.level] = &edit.compactPtrs[idx]
	}
	for _, delTable := range edit.delTables {
		level, number := delTable.level, delTable.number
		builder.deleted[level].add(number)
	}
	for _, addTable := range edit.addedTables {
		level, number := addTable.level, addTable.number
		utils.Assert(level <= options.KLevelNum)
		builder.deleted[level].remove(number)
		tFile := &tFile{
			fd:   int(addTable.number),
			iMax: addTable.imax,
			iMin: addTable.imin,
			size: addTable.size,
		}

		allowSeeks := addTable.size / 1 << 14
		if allowSeeks < 100 {
			allowSeeks = 100
		}
		tFile.allowSeeks = allowSeeks
		builder.inserted[level].add(tFile)
	}
}

func (builder *vBuilder) saveTo(v *Version) {

	for level := 0; level < options.KLevelNum; level++ {
		baseFile := builder.base.levels[level]
		beginPos := 0
		iter := builder.inserted[level].NewIterator()
		v.levels[level] = make(tFiles, 0, len(baseFile)+builder.inserted[level].size) // reverse pre alloc capacity
		for iter.Next() {
			tFile := decodeTFile(iter.Key())
			pos := upperBound(baseFile, level, tFile, builder.vSet.opt.InternalComparer)
			for i := beginPos; i < pos; i++ {
				builder.maybeAddFile(v, baseFile[i], level)
			}
			builder.maybeAddFile(v, tFile, level)
			beginPos = pos
		}

		for i := beginPos; i < len(baseFile); i++ {
			builder.maybeAddFile(v, baseFile[i], level)
		}
	}

}

func upperBound(s tFiles, level int, tFile *tFile, cmp comparer.BasicComparer) int {

	if level == 0 {
		idx := sort.Search(len(s), func(i int) bool {
			return s[i].fd > tFile.fd
		})
		return idx
	}
	idx := sort.Search(len(s), func(i int) bool {
		return cmp.Compare(s[i].iMax, tFile.iMax) > 0
	})
	return idx
}

func (builder *vBuilder) maybeAddFile(v *Version, file *tFile, level int) {

	if builder.deleted[level].contains(file.fd) {
		return
	}

	files := v.levels[level]
	cmp := builder.vSet.opt.InternalComparer
	if level > 0 && len(files) > 0 {
		utils.Assert(cmp.Compare(files[len(files)-1].iMax, file.iMin) < 0)
	}

	v.levels[level] = append(v.levels[level], file)
}

type uintSortedSet struct {
	*anySortedSet
}

func newUintSortedSet() *uintSortedSet {
	uSet := &uintSortedSet{
		anySortedSet: &anySortedSet{
			BTree:                 collections.InitBTree(3, &uint64Comparer{}),
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

func newTFileSortedSet(cmp comparer.Comparer) *tFileSortedSet {
	tSet := &tFileSortedSet{
		anySortedSet: &anySortedSet{
			BTree: collections.InitBTree(3, &tFileComparer{
				iComparer: cmp,
			}),
			anySortedSetEncodeKey: encodeTFileToBinary,
		},
	}
	return tSet
}

type tFileComparer struct {
	iComparer comparer.Comparer
}

func (tc *tFileComparer) Compare(a, b []byte) int {
	tFileA, tFileB := decodeBinaryToTFile(a), decodeBinaryToTFile(b)
	r := tc.iComparer.Compare(tFileA.iMax, tFileB.iMax)
	if r != 0 {
		return r
	}
	if tFileA.fd < tFileB.fd {
		return -1
	} else if tFileA.fd == tFileB.fd {
		return 0
	} else {
		return 1
	}
}

func (tc *tFileComparer) Name() []byte {
	return []byte("leveldb.tFilecomparator")
}

func encodeTFileToBinary(item interface{}) (bool, []byte) {
	tFile, ok := item.(tFile)
	if !ok {
		return false, nil
	}
	key := encodeTFile(&tFile)
	return true, key
}

func decodeBinaryToTFile(b []byte) *tFile {
	return decodeTFile(b)
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
	current := vSet.getCurrent()
	current.Ref()
	builder := newBuilder(vSet, current)
	builder.apply(*edit)
	builder.saveTo(v)
	finalize(v)
	current.UnRef()

	var (
		stor           = vSet.opt.Storage
		writer         storage.SequentialWriter
		manifestWriter = vSet.manifestWriter
		manifestFd     = vSet.manifestFd
		newManifest    bool
		err            error
	)

	if manifestWriter == nil {
		newManifest = true
	}

	if manifestWriter != nil && manifestWriter.FileSize() >= vSet.opt.MaxManifestFileSize {
		newManifest = true
		manifestFd = storage.Fd{
			FileType: storage.KDescriptorFile,
			Num:      vSet.allocFileNum(),
		}
	}

	if newManifest {
		writer, err = stor.NewAppendableFile(manifestFd)
		if err == nil {
			manifestWriter = wal.NewJournalWriter(writer)
			err = vSet.writeSnapShot(manifestWriter) // write current version snapshot into manifest
		}
	}

	mutex.Unlock() // cause compaction is run in single thread, so we can avoid expensive write syscall

	utils.Assert(manifestWriter != nil, "manifest writer must not nil")

	if err == nil {
		edit.EncodeTo(manifestWriter)
		err = edit.err
	}

	if err == nil && manifestWriter != nil { // make compiler happy
		err = manifestWriter.Sync()
	}

	if err == nil && newManifest {
		err = stor.SetCurrent(manifestFd.Num)
		if err == nil {
			_ = vSet.writer.Close()
		}
	}

	mutex.Lock()

	if err == nil {
		if newManifest {
			vSet.manifestWriter = manifestWriter
			vSet.writer = writer
		}
		vSet.appendVersion(v)
		vSet.stSeqNum = edit.lastSeq
		vSet.stJournalNum = edit.journalNum
	} else {
		if newManifest && writer != vSet.writer {
			_ = writer.Close()
			_ = stor.Remove(manifestFd)
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

func (vSet *VersionSet) writeSnapShot(w io.Writer) error {

	var edit VersionEdit

	for _, cPtr := range vSet.compactPtrs {
		edit.addCompactPtr(cPtr.level, cPtr.ikey)
	}

	for level, fileMetas := range vSet.current.levels {
		for _, fileMeta := range fileMetas {
			edit.addNewTable(level, fileMeta.size, uint64(fileMeta.fd), fileMeta.iMin, fileMeta.iMax)
		}
	}

	edit.setNextFile(vSet.nextFileNum)
	edit.setLogNum(vSet.stJournalNum)
	edit.setLastSeq(vSet.stSeqNum)
	edit.setCompareName(vSet.opt.InternalComparer.Name())

	edit.EncodeTo(w)
	return edit.err
}

func finalize(v *Version) {

	var (
		bestLevel int
		bestScore float64
	)

	for level := 0; level < len(v.levels)-1; level++ {
		if level == 0 {
			length := len(v.levels[level])
			bestScore = float64(length) / options.KLevel0StopWriteTrigger
			bestLevel = 0
		} else {
			totalSize := uint64(v.levels[level].size())
			score := float64(totalSize / options.MaxBytesForLevel(level))
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

func (vSet *VersionSet) recover(manifest storage.Fd) (err error) {

	var (
		hasComparerName, hasLogFileNum, hasNextFileNum, hasLastSeq bool
		logFileNum                                                 uint64
		seqNum                                                     sequence
		nextFileNum                                                uint64
	)

	reader, rErr := vSet.opt.Storage.NewSequentialReader(manifest)
	if rErr != nil {
		err = rErr
		return
	}

	var (
		edit    VersionEdit
		version Version
	)

	vBuilder := newBuilder(vSet, newVersion(vSet))
	journalReader := wal.NewJournalReader(reader)
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

		vBuilder.apply(edit)

		if edit.hasRec(kComparerName) {
			hasComparerName = true
			if bytes.Compare(edit.comparerName, vSet.opt.InternalComparer.Name()) != 0 {
				err = errors.NewErrCorruption("invalid comparator")
				return
			}
		}

		if edit.hasRec(kNextFileNum) {
			hasNextFileNum = true
			nextFileNum = edit.nextFileNum
		}

		if edit.hasRec(kJournalNum) {
			hasLogFileNum = true
			logFileNum = edit.journalNum
		}

		if edit.hasRec(kSeqNum) {
			hasLastSeq = true
			seqNum = edit.lastSeq
		}

		edit.reset()
	}

	if !hasComparerName {
		err = errors.NewErrCorruption("missing comparer name")
		return
	}

	if !hasLogFileNum {
		err = errors.NewErrCorruption("missing log num")
		return
	}

	if !hasNextFileNum {
		err = errors.NewErrCorruption("missing next file num")
		return
	}

	if !hasLastSeq {
		err = errors.NewErrCorruption("missing last seq num")
		return
	}

	vSet.markFileUsed(logFileNum)
	vSet.markFileUsed(nextFileNum)

	vBuilder.saveTo(&version)
	finalize(&version)
	vSet.appendVersion(&version)
	vSet.current = &version
	vSet.manifestFd = storage.Fd{
		FileType: storage.KDescriptorFile,
		Num:      nextFileNum,
	}
	vSet.nextFileNum = nextFileNum + 1
	vSet.stSeqNum = seqNum
	vSet.stJournalNum = logFileNum
	return
}

func (vSet *VersionSet) getCurrent() *Version {
	return vSet.current
}

func (vSet *VersionSet) addLiveFiles(expected map[uint64]struct{}) {
	ele := vSet.versions.Front()
	for ele != nil {
		ver := ele.Value.(*Version)
		for _, level := range ver.levels {
			for _, v := range level {
				expected[uint64(v.fd)] = struct{}{}
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

func (v *Version) updateStat(gStat *GetStat) bool {
	seekFile := gStat.SeekFile
	if seekFile != nil {
		seekFile.allowSeeks--
		if seekFile.allowSeeks == 0 && v.fileToCompact == nil {
			v.fileToCompact = seekFile
			v.fileToCompactLevel = gStat.SeekFileLevel
			return true
		}
	}
	return false
}

type getStat uint8

const (
	kStatNotFound getStat = iota
	kStatFound
	kStatDelete
	kStatCorruption
)

func (v *Version) get(ikey internalKey, value *[]byte) (gStat *GetStat, err error) {

	userKey := ikey.userKey()
	stat := kStatNotFound
	gStat = new(GetStat)

	var (
		lastFileRead      *tFile
		lastFileReadLevel int
	)

	match := func(level int, tFile *tFile) (continueLoop bool) {
		v.vSet.tableCache.Get(ikey, tFile, func(rkey internalKey, rValue []byte, rErr error) {

			if lastFileRead != nil {
				gStat.SeekFile = lastFileRead
				gStat.SeekFileLevel = lastFileReadLevel
			}

			lastFileRead = tFile
			lastFileReadLevel = level

			if rErr == errors.ErrNotFound {
				stat = kStatNotFound
				return
			}

			if rErr != nil {
				err = rErr
				return
			}

			ukey, kt, _, pErr := parseInternalKey(rkey)
			if pErr != nil {
				stat = kStatCorruption
			} else if bytes.Compare(ukey, userKey) == 0 {
				switch kt {
				case keyTypeValue:
					*value = rValue
					stat = kStatFound
				case keyTypeDel:
					stat = kStatDelete
				}
			}
			return
		})

		if err != nil {
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

func (v *Version) foreachOverlapping(ikey internalKey, f func(level int, tFile *tFile) (continueLoop bool)) {
	tmp := make(tFiles, 0)
	ukey := ikey.userKey()
	for _, level0 := range v.levels[0] {
		if bytes.Compare(level0.iMin.userKey(), ukey) <= 0 && bytes.Compare(level0.iMax.userKey(), ukey) >= 0 {
			tmp = append(tmp, level0)
		}
	}
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].fd > tmp[j].fd
	})

	for idx := 0; idx < len(tmp); idx++ {
		if !f(0, tmp[idx]) {
			return // match case
		}
	}

	for level := 1; level < len(v.levels); level++ {
		lf := v.levels[level]
		idx := sort.Search(len(lf), func(i int) bool {
			return bytes.Compare(lf[i].iMax.userKey(), ukey) >= 0
		})
		if idx < len(lf) && bytes.Compare(lf[idx].iMin.userKey(), ukey) <= 0 {
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

func (vSet *VersionSet) needCompaction() bool {

	current := vSet.current
	current.Ref()
	defer current.UnRef()

	if current.cScore >= 1.0 {
		return true
	}

	if current.fileToCompact != nil {
		return true
	}

	return false
}

func (vSet *VersionSet) Close() error {
	vSet.tableCache.Close()
	return nil
}
