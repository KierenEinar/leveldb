package leveldb

import (
	"container/list"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KierenEinar/leveldb/cache"
	"github.com/KierenEinar/leveldb/errors"
	"github.com/KierenEinar/leveldb/options"
	"github.com/KierenEinar/leveldb/storage"
	"github.com/KierenEinar/leveldb/utils"
	"github.com/KierenEinar/leveldb/wal"
)

type DBImpl struct {
	rwMutex    sync.RWMutex
	versionSet *VersionSet

	journalWriter *wal.JournalWriter

	shuttingDown uint32
	closed       chan struct{}
	// these state are protect by mutex
	seqNum    sequence
	journalFd storage.Fd

	frozenSeq       sequence
	frozenJournalFd storage.Fd

	mem *MemDB
	imm *MemDB

	memPool chan *MemDB

	backgroundWorkFinishedSignal *sync.Cond

	backgroundCompactionScheduled bool

	bgErr error

	scratchBatch *WriteBatch

	writers *list.List

	writersFinishedSignal *sync.Cond

	// atomic state
	hasImm uint32

	tableOperation *tableOperation
	tableCache     *TableCache
	snapshots      *list.List

	scheduler *Scheduler

	opt *options.Options

	dbLock storage.FileLock

	pendingOutputs map[uint64]struct{}
}

func (dbImpl *DBImpl) Put(key []byte, value []byte) error {
	wb := NewWriteBatch()
	if err := wb.Put(key, value); err != nil {
		return err
	}
	return dbImpl.write(wb)
}

func (dbImpl *DBImpl) Del(key []byte) error {
	wb := NewWriteBatch()
	if err := wb.Delete(key); err != nil {
		return err
	}
	return dbImpl.write(wb)
}

func (dbImpl *DBImpl) Get(key []byte) ([]byte, error) {

	if atomic.LoadUint32(&dbImpl.shuttingDown) == 1 {
		return nil, errors.ErrClosed
	}

	dbImpl.rwMutex.RLock()
	v := dbImpl.versionSet.getCurrent()
	mem := dbImpl.mem
	imm := dbImpl.imm
	v.Ref()
	mem.Ref()
	if imm != nil {
		imm.Ref()
	}
	seq := dbImpl.seqNum
	dbImpl.rwMutex.RUnlock()

	ikey := buildInternalKey(nil, key, keyTypeSeek, seq)

	var (
		mErr       error
		value      []byte
		havaUpdate bool
		gStat      *GetStat
	)

	if memGet(mem, ikey, &value, &mErr) {
		// done
	} else if imm != nil && memGet(imm, ikey, &value, &mErr) {
		// done
	} else {
		havaUpdate = true
		gStat, mErr = v.get(ikey, &value)
	}

	dbImpl.rwMutex.Lock()
	defer dbImpl.rwMutex.Unlock()

	if havaUpdate && v.updateStat(gStat) {
		dbImpl.maybeScheduleCompaction()
	}

	v.UnRef()
	mem.UnRef()
	if imm != nil {
		imm.UnRef()
	}

	if mErr != nil {
		return nil, mErr
	}

	return value, nil
}

func (dbImpl *DBImpl) Close() error {

	if !atomic.CompareAndSwapUint32(&dbImpl.shuttingDown, 0, 1) {
		return errors.ErrClosed
	}

	close(dbImpl.closed)

	dbImpl.rwMutex.Lock()
	defer dbImpl.rwMutex.Unlock()

	// wait write queue
	for dbImpl.writers.Len() > 0 {
		dbImpl.writersFinishedSignal.Wait()
	}

	// wait compaction
	for dbImpl.backgroundCompactionScheduled {
		dbImpl.backgroundWorkFinishedSignal.Wait()
	}

	// todo

	return nil

}

func memGet(mem *MemDB, ikey internalKey, value *[]byte, err *error) (ok bool) {

	_, rValue, rErr := mem.Find(ikey)
	if rErr != nil {
		if rErr == errors.ErrKeyDel {
			*err = errors.ErrNotFound
			ok = true
			return
		}
		*err = rErr
		ok = false
		return
	}

	val := append([]byte(nil), rValue...)
	*value = val
	ok = true
	return
}

func (dbImpl *DBImpl) write(batch *WriteBatch) error {

	if atomic.LoadUint32(&dbImpl.shuttingDown) == 1 {
		return errors.ErrClosed
	}

	if batch.Len() == 0 {
		return nil
	}

	w := newWriter(batch, &dbImpl.rwMutex)
	dbImpl.rwMutex.Lock()
	dbImpl.writers.PushBack(w)

	for dbImpl.writers.Front().Value.(*writer) != w && !w.done {
		w.cv.Wait()
	}

	if w.done {
		dbImpl.rwMutex.Unlock()
		return w.err
	}

	// may temporary unlock and lock mutex
	err := dbImpl.makeRoomForWrite()

	lastSequence := dbImpl.seqNum
	var (
		newWriteBatch = batch
		lastWriter    = dbImpl.writers.Front()
	)

	if err == nil {
		newWriteBatch, lastWriter = dbImpl.mergeWriteBatch() // write into scratchbatch
		newWriteBatch.SetSequence(lastSequence + 1)
		lastSequence += sequence(newWriteBatch.Len())
		mem := dbImpl.mem
		mem.Ref()
		dbImpl.rwMutex.Unlock()
		defer mem.UnRef()
		// expensive syscall need to unlock !!!
		_, err = dbImpl.journalWriter.ReadFrom(newWriteBatch.rep)
		if err == nil {
			err = newWriteBatch.insertInto(mem)
		}

		dbImpl.rwMutex.Lock()
		if err == nil {
			dbImpl.seqNum = lastSequence
		}

		if err != nil {
			dbImpl.recordBackgroundError(err)
		}

		if newWriteBatch == dbImpl.scratchBatch {
			newWriteBatch.Reset()
		}

	}

	for {

		ready := dbImpl.writers.Front()
		dbImpl.writers.Remove(ready)

		readyW := ready.Value.(*writer)
		if readyW != w {
			readyW.done = true
			readyW.err = err
			readyW.cv.Signal()
		}

		if ready == lastWriter {
			break
		}
	}

	if dbImpl.writers.Len() > 0 {
		readyW := dbImpl.writers.Front().Value.(*writer)
		readyW.cv.Signal()
	} else {
		dbImpl.writersFinishedSignal.Signal()
	}

	dbImpl.rwMutex.Unlock()

	return err
}

func (dbImpl *DBImpl) makeRoomForWrite() error {

	utils.AssertMutexHeld(&dbImpl.rwMutex)
	allowDelay := true

	for {
		if dbImpl.bgErr != nil {
			return dbImpl.bgErr
		} else if allowDelay && dbImpl.versionSet.levelFilesNum(0) >= options.KLevel0SlowDownTrigger {
			allowDelay = false
			dbImpl.rwMutex.Unlock()
			time.Sleep(time.Microsecond * 1000)
			dbImpl.rwMutex.Lock()
		} else if dbImpl.mem.ApproximateSize() <= int(dbImpl.opt.WriteBufferSize) {
			break
		} else if dbImpl.imm != nil { // wait background compaction compact imm table
			dbImpl.backgroundWorkFinishedSignal.Wait()
		} else if dbImpl.versionSet.levelFilesNum(0) >= options.KLevel0StopWriteTrigger {
			dbImpl.backgroundWorkFinishedSignal.Wait()
		} else {
			journalFd := storage.Fd{
				FileType: storage.KJournalFile,
				Num:      dbImpl.versionSet.allocFileNum(),
			}
			s := dbImpl.opt.Storage
			writer, err := s.NewAppendableFile(journalFd)
			if err == nil {
				_ = dbImpl.journalWriter.Close()
				dbImpl.frozenSeq = dbImpl.seqNum
				dbImpl.frozenJournalFd = dbImpl.journalFd
				dbImpl.journalFd = journalFd
				dbImpl.journalWriter = wal.NewJournalWriter(writer)
				imm := dbImpl.mem
				dbImpl.imm = imm

				atomic.StoreUint32(&dbImpl.hasImm, 1)
				mem := dbImpl.NewMemTable(int(dbImpl.opt.WriteBufferSize), dbImpl.opt.InternalComparer)
				mem.Ref()
				dbImpl.mem = mem
			} else {
				dbImpl.versionSet.reuseFileNum(journalFd.Num)
				return err
			}
			dbImpl.maybeScheduleCompaction()
		}
	}

	return nil
}

func (dbImpl *DBImpl) mergeWriteBatch() (result *WriteBatch, lastWriter *list.Element) {

	utils.AssertMutexHeld(&dbImpl.rwMutex)
	utils.Assert(dbImpl.writers.Len() > 0)

	var err error

	front := dbImpl.writers.Front()
	firstBatch := front.Value.(*writer).batch
	size := firstBatch.Size()

	maxSize := 1 << 20  // 1m
	if size < 128<<10 { // limit the growth while in small write
		maxSize = size + 128<<10
	}

	result = firstBatch
	lastWriter = front
	w := front.Next()
	for w != nil {
		wr := w.Value.(*writer)
		if size+wr.batch.Size() > maxSize {
			break
		}
		if result == firstBatch {
			result = dbImpl.scratchBatch
			err = result.append(firstBatch)
		}
		if err == nil {
			err = result.append(wr.batch)
		}

		if err != nil {
			goto ERR
		}
		lastWriter = w
		w = w.Next()
	}

ERR:
	if err != nil { // rollback merge
		// todo warming
		if result == dbImpl.scratchBatch {
			result.Reset()
		}
		lastWriter = front
		result = firstBatch
	}

	return

}

func (dbImpl *DBImpl) recordBackgroundError(err error) {
	if dbImpl.bgErr == nil {
		dbImpl.bgErr = err
		dbImpl.backgroundWorkFinishedSignal.Broadcast()
	}
}

// maybeScheduleCompaction required mutex held
func (dbImpl *DBImpl) maybeScheduleCompaction() {
	utils.AssertMutexHeld(&dbImpl.rwMutex)

	if dbImpl.backgroundCompactionScheduled {
		// do nothing
	} else if dbImpl.bgErr != nil {
		// do nothing
	} else if atomic.LoadUint32(&dbImpl.shuttingDown) == 1 {
		// do nothing
	} else if atomic.LoadUint32(&dbImpl.hasImm) == 0 && !dbImpl.versionSet.needCompaction() {
		// do nothing
	} else {
		dbImpl.backgroundCompactionScheduled = true
		dbImpl.scheduler.Enqueue(bgWork, dbImpl)
	}

}

func bgWork(args ...interface{}) {
	dbImpl := args[0].(*DBImpl)
	dbImpl.backgroundCall()
}

func (dbImpl *DBImpl) backgroundCall() {

	dbImpl.rwMutex.Lock()

	utils.Assert(dbImpl.backgroundCompactionScheduled)

	if dbImpl.bgErr != nil {
		// do nothing
	} else if atomic.LoadUint32(&dbImpl.shuttingDown) == 1 {
		// do nothing
	} else {
		dbImpl.backgroundCompaction()
	}

	dbImpl.backgroundCompactionScheduled = false
	dbImpl.maybeScheduleCompaction()

	dbImpl.backgroundWorkFinishedSignal.Broadcast()
	dbImpl.rwMutex.Unlock()
}

func (dbImpl *DBImpl) backgroundCompaction() {
	utils.AssertMutexHeld(&dbImpl.rwMutex)

	if dbImpl.imm != nil {
		dbImpl.compactMemTable()
		return
	}

	c := dbImpl.versionSet.pickCompaction1()
	if c == nil {
		return
	} else if len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0 && c.gp.size() <= c.gpOverlappedLimit {

		edit := &VersionEdit{}
		addTable := c.inputs[0][0]
		edit.addNewTable(c.sourceLevel+1, addTable.size, uint64(addTable.fd), addTable.iMin, addTable.iMax)
		err := dbImpl.versionSet.logAndApply(edit, &dbImpl.rwMutex)
		if err != nil {
			dbImpl.recordBackgroundError(err)
		}
	} else {
		err := dbImpl.doCompactionWork(c)
		if err != nil {
			dbImpl.recordBackgroundError(err)
		}
		c.releaseInputs()
		dbImpl.cleanupCompaction(c)
		err = dbImpl.removeObsoleteFiles()
		if err != nil {
			// todo log warn msg
		}
	}

}

func (dbImpl *DBImpl) compactMemTable() {

	utils.AssertMutexHeld(&dbImpl.rwMutex)
	utils.Assert(dbImpl.imm != nil)

	var err error
	defer func() {
		if err != nil {
			dbImpl.recordBackgroundError(err)
		}
	}()

	edit := &VersionEdit{}
	err = dbImpl.writeLevel0Table(dbImpl.imm, edit)
	if err == nil {
		if atomic.LoadUint32(&dbImpl.shuttingDown) == 1 {
			err = errors.ErrClosed
			return
		}
		edit.setLogNum(dbImpl.journalFd.Num)
		edit.setLastSeq(dbImpl.frozenSeq)
		err = dbImpl.versionSet.logAndApply(edit, &dbImpl.rwMutex)
	}

	if err == nil {
		imm := dbImpl.imm
		imm.UnRef()
		dbImpl.imm = nil
		atomic.StoreUint32(&dbImpl.hasImm, 0)
		_ = dbImpl.removeObsoleteFiles()
	}

	return

}

func (dbImpl *DBImpl) writeLevel0Table(memDb *MemDB, edit *VersionEdit) (err error) {

	utils.AssertMutexHeld(&dbImpl.rwMutex)

	var fileMeta tFile
	fileMeta.fd = int(dbImpl.versionSet.allocFileNum())
	dbImpl.pendingOutputs[uint64(fileMeta.fd)] = struct{}{}
	dbImpl.rwMutex.Unlock()
	defer func() {
		dbImpl.rwMutex.Lock()
		if err != nil {
			dbImpl.versionSet.reuseFileNum(uint64(fileMeta.fd))
			delete(dbImpl.pendingOutputs, uint64(fileMeta.fd))
		}
	}()

	tWriter, err := dbImpl.tableOperation.create(fileMeta)
	if err != nil {
		return err
	}

	iter := memDb.NewIterator()
	defer iter.UnRef()

	for iter.Next() {
		err = tWriter.append(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
	}

	err = tWriter.finish()
	if err == nil {
		fileMeta = tWriter.fileMeta
		edit.addNewTable(0, fileMeta.size, uint64(fileMeta.fd), fileMeta.iMin, fileMeta.iMax)
	}
	return
}

func (dbImpl *DBImpl) recover(edit *VersionEdit) error {

	fLock, err := dbImpl.opt.Storage.LockFile(storage.FileLockFd())
	if err != nil {
		return err
	}
	dbImpl.dbLock = fLock

	manifestFd, err := dbImpl.opt.Storage.GetCurrent()
	if err != nil {
		if err != os.ErrNotExist {
			return err
		}

		if !dbImpl.opt.CreateIfMissingCurrent {
			return errors.ErrMissingCurrent
		}

		err = dbImpl.newDb()
	}
	if err != nil {
		return err
	}

	err = dbImpl.versionSet.recover(manifestFd)

	if err != nil {
		return err
	}

	fds, err := dbImpl.opt.Storage.List()
	if err != nil {
		return err
	}

	// check table files is exists
	expected := make(map[uint64]struct{})
	dbImpl.versionSet.addLiveFiles(expected)

	logFiles := make([]storage.Fd, 0)
	for _, fd := range fds {
		if fd.FileType == storage.KJournalFile && fd.Num >= dbImpl.versionSet.stJournalNum {
			logFiles = append(logFiles, fd)
		} else if fd.FileType == storage.KTableFile {
			delete(expected, fd.Num)
		}
	}

	utils.Assert(len(expected) == 0, fmt.Sprintf("ldb file not exists, [%v].ldb", expected))

	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].Num < logFiles[j].Num
	})

	for _, logFile := range logFiles {
		err = dbImpl.recoverLogFile(logFile, edit)
		if err != nil {
			return err
		}
	}

	return nil
}

func (dbImpl *DBImpl) newDb() (err error) {
	dbImpl.seqNum = 0

	newDb := &VersionEdit{
		comparerName: dbImpl.opt.InternalComparer.Name(),
		nextFileNum:  2,
		lastSeq:      0,
	}

	manifestFd := storage.Fd{
		FileType: storage.KDescriptorFile,
		Num:      1,
	}

	writer, wErr := dbImpl.opt.Storage.NewAppendableFile(manifestFd)
	if wErr != nil {
		err = wErr
		return
	}

	defer func() {
		_ = writer.Close()
	}()

	newDb.EncodeTo(writer)
	if newDb.err != nil {
		err = newDb.err
		return
	}

	err = writer.Sync()
	if err != nil {
		return
	}

	err = dbImpl.opt.Storage.SetCurrent(manifestFd.Num)

	if err != nil {
		_ = dbImpl.opt.Storage.Remove(manifestFd)
	}

	return
}

func (dbImpl *DBImpl) recoverLogFile(fd storage.Fd, edit *VersionEdit) error {

	reader, err := dbImpl.opt.Storage.NewSequentialReader(fd)
	if err != nil {
		return err
	}
	journalReader := wal.NewJournalReader(reader)
	memDB := dbImpl.NewMemTable(0, dbImpl.opt.InternalComparer)
	memDB.Ref()
	defer func() {
		memDB.UnRef()
		journalReader.Close()
		_ = reader.Close()
	}()
	for {
		chunkReader, err := journalReader.NextChunk()
		if err == io.EOF {
			break
		}

		for {
			writeBatch, err := decodeBatchChunk(chunkReader, dbImpl.versionSet.stSeqNum)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if memDB.ApproximateSize() > int(dbImpl.opt.WriteBufferSize) {
				err = dbImpl.writeLevel0Table(memDB, edit)
				if err != nil {
					return err
				}
				memDB.UnRef()
				memDB = dbImpl.NewMemTable(0, dbImpl.opt.InternalComparer)
				memDB.Ref()
			}

			err = writeBatch.insertInto(memDB)
			if err != nil {
				return err
			}

			dbImpl.seqNum += writeBatch.seq + sequence(writeBatch.count) - 1
			dbImpl.versionSet.markFileUsed(fd.Num)
		}

	}

	if memDB.ApproximateSize() > 0 {
		err = dbImpl.writeLevel0Table(memDB, edit)
		if err != nil {
			return err
		}
	}

	edit.setLastSeq(dbImpl.seqNum)
	return nil

}

// clear the obsolete files
func (dbImpl *DBImpl) removeObsoleteFiles() (err error) {

	utils.AssertMutexHeld(&dbImpl.rwMutex)

	fds, lErr := dbImpl.opt.Storage.List()
	if lErr != nil {
		err = lErr
		return
	}

	liveTableFileSet := dbImpl.pendingOutputs
	dbImpl.versionSet.addLiveFiles(liveTableFileSet)

	fileToClean := make([]storage.Fd, 0)

	for _, fd := range fds {
		var keep bool
		switch fd.FileType {
		case storage.KDescriptorFile:
			keep = fd.Num >= dbImpl.versionSet.manifestFd.Num
		case storage.KJournalFile:
			keep = fd.Num >= dbImpl.versionSet.stJournalNum
		case storage.KTableFile:
			if _, ok := liveTableFileSet[fd.Num]; ok {
				keep = true
			}
		case storage.KCurrentFile, storage.KDBLockFile, storage.KDBTempFile:
			keep = true
		}

		if !keep {
			fileToClean = append(fileToClean, fd)
		}
	}

	dbImpl.rwMutex.Unlock()

	for _, fd := range fileToClean {
		rErr := dbImpl.opt.Storage.Remove(fd)
		if rErr != nil {
			err = rErr
		}

		if fd.FileType == storage.KTableFile {
			dbImpl.tableCache.Evict(fd.Num)
		}
	}

	dbImpl.rwMutex.Lock()
	return
}

func sanitizeOptions(dbpath string, o *options.Options) (opt *options.Options, err error) {

	opt = new(options.Options)

	if o == nil || o.Storage == nil {
		opt.Storage, err = storage.OpenPath(dbpath)
		if err != nil {
			return
		}
	}

	if o == nil || o.InternalComparer == nil {
		opt.InternalComparer = IComparer
	}

	if o == nil || o.FilterPolicy == nil {
		opt.FilterPolicy = IFilter
	}

	if o == nil || o.FilterBaseLg == 0 {
		opt.FilterBaseLg = 8
	}

	if o == nil || o.Hash32 == nil {
		opt.Hash32 = fnv.New32()
	}

	if o == nil || o.BlockCache == nil {
		opt.BlockCache = cache.NewCache(8<<20, opt.Hash32)
	}

	if o == nil || o.BlockRestartInterval == 0 {
		opt.BlockRestartInterval = 16
	}

	if o == nil || o.BlockSize == 0 {
		opt.BlockSize = 4 << 10 // 4k
	}

	if o == nil || o.MaxEstimateFileSize == 0 {
		opt.MaxEstimateFileSize = 2 << 20 // 2m
	}

	if o == nil || o.GPOverlappedLimit == 0 {
		opt.GPOverlappedLimit = 10
	}

	if o == nil || o.MaxCompactionLimitFactor == 0 {
		opt.MaxCompactionLimitFactor = 25
	}

	if o == nil || o.WriteBufferSize == 0 {
		opt.WriteBufferSize = options.KMemTableWriteBufferSize // 4m
	}

	if o == nil || o.MaxManifestFileSize == 0 {
		opt.MaxManifestFileSize = 64 << 20 // 64m
	}

	if o == nil || o.MaxOpenFiles == 0 {
		opt.MaxOpenFiles = 1000 // 1000
	}
	return
}

func newDBImpl(opt *options.Options) *DBImpl {

	db := &DBImpl{
		rwMutex: sync.RWMutex{},
		versionSet: &VersionSet{
			versions:    list.New(),
			compactPtrs: [7]*compactPtr{},
			tableCache:  NewTableCache(opt),
			opt:         opt,
		},
		closed:         make(chan struct{}),
		scratchBatch:   NewWriteBatch(),
		writers:        list.New(),
		hasImm:         0,
		tableCache:     NewTableCache(opt),
		snapshots:      list.New(),
		opt:            opt,
		pendingOutputs: make(map[uint64]struct{}),
		memPool:        make(chan *MemDB, 1),
	}

	db.backgroundWorkFinishedSignal = sync.NewCond(&db.rwMutex)
	db.writersFinishedSignal = sync.NewCond(&db.rwMutex)
	db.tableOperation = newTableOperation(opt, db.tableCache)
	db.scheduler = NewSchedule(db.closed)
	runtime.SetFinalizer(db, (*DBImpl).Close)
	return db
}

func (dbImpl *DBImpl) cleanupCompaction(c *compaction1) {

	for output := range c.outputs {
		delete(dbImpl.pendingOutputs, output)
	}

	if c.err == nil {
		utils.Assert(c.tWriter == nil)
		return
	}

	var max uint64
	for output := range c.outputs {
		if max < output {
			max = output
		}
	}

	dbImpl.versionSet.reuseFileNum(max)

	if c.tWriter != nil {
		_ = c.tWriter.w.Close()
	}

}

func (dbImpl *DBImpl) mPoolPut(memDb *MemDB) bool {
	select {
	case <-dbImpl.closed:
		return false
	case dbImpl.memPool <- memDb:
		return true
	default:
		return false
	}
}

func (dbImpl *DBImpl) mPoolGet(n int) *MemDB {

	var mdb *MemDB

	select {
	case mdb = <-dbImpl.memPool:
	default:
	}

	if mdb == nil || mdb.Capacity() < n {
		mdb = dbImpl.NewMemTable(n, dbImpl.opt.InternalComparer)
	}
	return mdb
}

func (dbImpl *DBImpl) mPoolDrain() {

}
