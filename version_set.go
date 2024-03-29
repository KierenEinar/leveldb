package leveldb

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/KierenEinar/leveldb/logger"

	"github.com/KierenEinar/leveldb/cache"

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
	compactPtrs [KLevelNum]*compactPtr

	nextFileNum       uint64
	stJournalNum      uint64
	stSeqNum          sequence // current memtable start seq num
	manifestFd        storage.Fd
	manifestSeqWriter storage.SequentialWriter
	manifestWriter    *wal.JournalWriter
	tableOperation    *tableOperation
	tableCache        *TableCache
	blockCache        cache.Cache
	opt               *options.Options

	// statics
	manifestRewriteFailed int64
}

func newVersionSet(opt *options.Options) *VersionSet {
	tableCache := NewTableCache(opt)
	vSet := &VersionSet{
		versions:       list.New(),
		current:        nil,
		compactPtrs:    [7]*compactPtr{},
		tableCache:     tableCache,
		tableOperation: newTableOperation(opt, tableCache),
		blockCache:     opt.BlockCache,
		opt:            opt,
	}
	return vSet
}

type Version struct {
	element *list.Element
	vSet    *VersionSet
	*utils.BasicReleaser
	levels [KLevelNum]tFiles

	closed bool

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
	inserted [KLevelNum]*tFileSortedSet
	deleted  [KLevelNum]*uintSortedSet
}

func newBuilder(vSet *VersionSet, base *Version) *vBuilder {
	builder := &vBuilder{
		vSet: vSet,
		base: base,
	}
	for i := 0; i < KLevelNum; i++ {
		builder.inserted[i] = newTFileSortedSet(vSet.opt.InternalComparer)
	}
	for i := 0; i < KLevelNum; i++ {
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
		utils.Assert(level < KLevelNum)
		builder.deleted[level].remove(number)
		tFile := &tFile{
			fd:   int(addTable.number),
			iMax: addTable.imax,
			iMin: addTable.imin,
			size: addTable.size,
		}

		allowSeeks := addTable.size / (1 << 14)
		if allowSeeks < 100 {
			allowSeeks = 100
		}
		tFile.allowSeeks = allowSeeks
		builder.inserted[level].add(tFile)
	}
}

func (builder *vBuilder) saveTo(v *Version) {
	for level := 0; level < KLevelNum; level++ {
		baseFile := tFiles{}
		if builder.base != nil {
			baseFile = builder.base.levels[level]
		}
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
		iter.UnRef()
		for i := beginPos; i < len(baseFile); i++ {
			builder.maybeAddFile(v, baseFile[i], level)
		}
	}

}

func upperBound(s tFiles, level int, tFile *tFile, cmp comparer.BasicComparer) int {

	if level == 0 {
		idx := sort.Search(len(s), func(i int) bool {
			return tFile.fd > s[i].fd
		})
		return idx
	}
	idx := sort.Search(len(s), func(i int) bool {
		return cmp.Compare(tFile.iMax, s[i].iMax) < 0
	})
	return idx
}

func (builder *vBuilder) maybeAddFile(v *Version, file *tFile, level int) {

	if contains, err := builder.deleted[level].contains(uint64(file.fd)); err != nil || contains {
		return
	}

	files := v.levels[level]
	cmp := builder.vSet.opt.InternalComparer
	if level > 0 && len(files) > 0 {
		if cmp.Compare(files[len(files)-1].iMax, file.iMin) >= 0 {
			for _, tFile := range files {
				logger.Infof("level=%d, fd=%d, min=%s, max=%s\n", level, tFile.fd,
					tFile.iMin.userKey(), tFile.iMax.userKey())
				logger.Infof("last file add, level=%d, fd=%d, min=%s, max=%s\n",
					level, file.fd, file.iMin.userKey(), file.iMax.userKey())
			}
		}
		utils.Assert(cmp.Compare(files[len(files)-1].iMax, file.iMin) < 0,
			fmt.Sprintf("last file imax=%s, newfile min=%s", files[len(files)-1].iMax, file.iMin))
	}

	dupFile := &tFile{
		fd:         file.fd,
		iMax:       append(file.iMax),
		iMin:       append(file.iMin),
		size:       file.size,
		allowSeeks: file.allowSeeks,
	}

	v.levels[level] = append(v.levels[level], dupFile)
}

type uintSortedSet struct {
	*anySortedSet
}

func newUintSortedSet() *uintSortedSet {
	uSet := &uintSortedSet{
		anySortedSet: &anySortedSet{
			SkipList:              collections.NewSkipList(time.Now().UnixNano(), 0, &uint64Comparer{}),
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
			SkipList: collections.NewSkipList(time.Now().UnixNano(), 0, &tFileComparer{
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
	tFile, ok := item.(*tFile)
	if !ok {
		return false, nil
	}

	return true, encodeTFile(tFile)
}

func decodeBinaryToTFile(b []byte) *tFile {
	return decodeTFile(b)
}

type anySortedSet struct {
	*collections.SkipList
	anySortedSetEncodeKey
	size int
}

type anySortedSetEncodeKey func(item interface{}) (bool, []byte)

func (set *anySortedSet) add(item interface{}) (bool, error) {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet add item encode failed, please check...")
	}

	found, err := set.Has(key)
	if err != nil {
		return false, err
	}

	if !found {
		if err := set.Put(key, nil); err != nil {
			return false, err
		}
		set.size++
		return true, nil
	}
	return false, nil
}

func (set *anySortedSet) remove(item interface{}) (bool, error) {
	ok, key := set.anySortedSetEncodeKey(item)
	if !ok {
		panic("anySortedSet remove item encode failed, please check...")
	}
	ok, err := set.Del(key)
	if ok {
		set.size--
	}
	return ok, err
}

func (set *anySortedSet) contains(item interface{}) (bool, error) {
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

	// apply new version
	v := newVersion(vSet)
	current := vSet.getCurrent()
	current.Ref()
	builder := newBuilder(vSet, current)
	builder.apply(*edit)
	builder.saveTo(v)
	finalize(v)
	current.UnRef()

	if err := vSet.openNewManifestIfNeed(); err != nil {
		return err
	}

	mutex.Unlock() // cause compaction is run in single thread, so we can avoid expensive write syscall

	utils.Assert(vSet.manifestWriter != nil, "manifest writer must not nil")
	edit.EncodeTo(vSet.manifestWriter)
	err := vSet.manifestWriter.Sync()

	mutex.Lock()

	if err != nil {
		return err
	}

	vSet.appendVersion(v)
	vSet.stSeqNum = edit.lastSeq
	vSet.stJournalNum = edit.journalNum
	return nil
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
		if cPtr != nil {
			edit.addCompactPtr(cPtr.level, cPtr.ikey)
		}
	}

	version := vSet.getCurrent()
	version.Ref()
	defer version.UnRef()

	for level, fileMetas := range version.levels {
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
			bestScore = float64(length / v.vSet.opt.Level0StopWriteTrigger)
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
	version := vSet.getCurrent()
	version.Ref()
	defer version.UnRef()
	return len(version.levels[level])
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

	edit := newVersionEdit()

	vBuilder := newBuilder(vSet, newVersion(vSet))
	journalReader := wal.NewJournalReader(reader, !vSet.opt.NoDropWholeBlockOnParseChunkErr)
	for {

		chunkReader, cErr := journalReader.NextChunk()
		if cErr == io.EOF {
			break
		}

		if cErr != nil {
			err = cErr
			return
		}

		edit.DecodeFrom(chunkReader)

		if edit.err == io.EOF {
			continue
		}

		if edit.err != nil {
			err = edit.err
			return
		}

		vBuilder.apply(*edit)

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
	version := newVersion(vSet)
	vBuilder.saveTo(version)
	finalize(version)
	vSet.appendVersion(version)

	vSet.nextFileNum = nextFileNum
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

func (vSet *VersionSet) addLiveFilesForTest(expected map[uint64]*tFile) {
	ele := vSet.versions.Front()
	for ele != nil {
		ver := ele.Value.(*Version)
		for _, level := range ver.levels {
			for idx, v := range level {
				expected[uint64(v.fd)] = level[idx]
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
	seekFile := gStat.FirstMissSeekFile
	if seekFile != nil {
		seekFile.allowSeeks--
		if seekFile.allowSeeks == 0 && v.fileToCompact == nil {
			v.fileToCompact = seekFile
			v.fileToCompactLevel = gStat.FirstMissSeekFileLevel
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

	if v.closed {
		return nil, errors.ErrClosed
	}

	userKey := ikey.userKey()
	stat := kStatNotFound
	gStat = new(GetStat)

	var (
		lastFileRead      *tFile
		lastFileReadLevel int
	)

	match := func(level int, tFile *tFile) (continueLoop bool) {
		v.vSet.tableCache.Get(ikey, tFile, func(rkey internalKey, rValue []byte, rErr error) {

			if lastFileRead != nil && gStat.FirstMissSeekFile == nil {
				gStat.FirstMissSeekFile = lastFileRead
				gStat.FirstMissSeekFileLevel = lastFileReadLevel
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
			gStat.MissHint++
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
	for ix := range v.levels[0] {
		level0 := v.levels[0][ix]
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

	current := vSet.getCurrent()
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

func (vSet *VersionSet) release(mu *sync.RWMutex) error {

	mu.Unlock()
	vSet.tableCache.Close()
	vSet.blockCache.Close()

	if vSet.manifestWriter != nil {
		_ = vSet.manifestWriter.Close()
		vSet.manifestWriter = nil
	}

	mu.Lock()

	version := newVersion(vSet)
	version.closed = true
	vSet.appendVersion(version)

	return nil
}

// noted: is not thread safe, caller should held mutex
func (vSet *VersionSet) openNewManifestIfNeed() (err error) {

	var (
		opt            = vSet.opt
		writer         = vSet.manifestSeqWriter
		manifestWriter = vSet.manifestWriter
		manifestFd     = vSet.manifestFd
		stor           = vSet.opt.Storage
		newManifest    bool
		reWrite        bool
	)

	if manifestWriter == nil {
		newManifest = true
	}

	if manifestWriter != nil && manifestWriter.FileSize() >= vSet.opt.MaxManifestFileSize {
		reWrite = true
		manifestFd = storage.Fd{
			FileType: storage.KDescriptorFile,
			Num:      vSet.allocFileNum(),
		}
	}

	// no need create new manifest writer
	if !newManifest {
		return
	}

	defer func() {
		if err != nil {
			if reWrite {
				if (vSet.manifestRewriteFailed) < opt.AllowManifestRewriteIgnoreFailed {
					err = nil
				}
				vSet.manifestRewriteFailed++
			}
			_ = stor.Remove(manifestFd)
		}
	}()

	if writer, err = stor.NewWritableFile(manifestFd); err != nil {
		return
	}
	manifestWriter = wal.NewJournalWriter(writer, vSet.opt.NoWriteSync)
	err = vSet.writeSnapShot(manifestWriter) // write current version snapshot into manifest
	if err != nil {
		return
	}

	err = stor.SetCurrent(manifestFd.Num)
	if err != nil {
		return
	}

	if reWrite {
		_ = writer.Close()
		_ = manifestWriter.Close()
	}

	vSet.manifestSeqWriter = writer
	vSet.manifestWriter = manifestWriter
	vSet.manifestFd = manifestFd
	return
}
