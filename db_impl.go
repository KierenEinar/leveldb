package leveldb

import (
	"container/list"
	"encoding/binary"
	"io"
	"leveldb/errors"
	"leveldb/options"
	"leveldb/storage"
	"leveldb/utils"
	"leveldb/wal"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type DBImpl struct {
	rwMutex    sync.RWMutex
	versionSet *VersionSet

	journalWriter *wal.JournalWriter

	shutdown uint32
	closed   chan struct{}
	// these state are protect by mutex
	seqNum    Sequence
	journalFd storage.Fd

	frozenSeq       Sequence
	frozenJournalFd storage.Fd

	mem *MemDB
	imm *MemDB

	backgroundWorkFinishedSignal *sync.Cond

	backgroundCompactionScheduled bool

	bgErr error

	scratchBatch *WriteBatch

	writers *list.List

	// atomic state
	hasImm uint32

	tableOperation *tableOperation
	tableCache     *TableCache
	snapshots      *list.List

	scheduler *Scheduler

	opt *options.Options
}

func (dbImpl *DBImpl) Put(key []byte, value []byte) error {
	wb := NewWriteBatch()
	wb.Put(key, value)
	return dbImpl.write(wb)
}

func (dbImpl *DBImpl) Get(key []byte) ([]byte, error) {

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

	ikey := BuildInternalKey(nil, key, KeyTypeSeek, seq)
	var (
		mErr  error
		value []byte
	)
	if memGet(mem, ikey, &value, &mErr) {
		// done
	} else if imm != nil && memGet(imm, ikey, &value, &mErr) {
		// done
	} else {
		mErr = v.get(ikey, &value)
	}

	dbImpl.rwMutex.Lock()
	v.UnRef()
	dbImpl.rwMutex.Unlock()

	mem.UnRef()
	if imm != nil {
		imm.UnRef()
	}

	if mErr != nil {
		return nil, mErr
	}

	return value, nil
}

func memGet(mem *MemDB, ikey InternalKey, value *[]byte, err *error) (ok bool) {

	_, rValue, rErr := mem.Find(ikey)
	if rErr != nil {
		if rErr == errors.ErrNotFound {
			ok = false
			return
		}
		if rErr == errors.ErrKeyDel {
			*err = errors.ErrNotFound
			ok = true
			return
		}
		*err = rErr
		ok = true
		return
	}

	val := append([]byte(nil), rValue...)
	*value = val
	ok = true
	return
}

func (dbImpl *DBImpl) write(batch *WriteBatch) error {

	if atomic.LoadUint32(&dbImpl.shutdown) == 1 {
		return errors.ErrClosed
	}

	if batch.Len() == 0 {
		return nil
	}

	w := newWriter(batch, &dbImpl.rwMutex)
	dbImpl.rwMutex.Lock()
	dbImpl.writers.PushBack(w)

	header := dbImpl.writers.Front().Value.(*writer)
	for w != header {
		w.cv.Wait()
	}

	if w.done {
		return w.err
	}

	// may temporary unlock and lock mutex
	err := dbImpl.makeRoomForWrite()
	lastWriter := w

	lastSequence := dbImpl.seqNum

	if err == nil {
		newWriteBatch := dbImpl.mergeWriteBatch(&lastWriter) // write into scratchbatch
		dbImpl.scratchBatch.SetSequence(lastSequence + 1)
		lastSequence += Sequence(dbImpl.scratchBatch.Len())
		mem := dbImpl.mem
		mem.Ref()
		dbImpl.rwMutex.Unlock()
		// expensive syscall need to unlock !!!
		_, syncErr := dbImpl.journalWriter.Write(newWriteBatch.Contents())
		if syncErr == nil {
			err = dbImpl.writeMem(mem, newWriteBatch)
		}

		dbImpl.rwMutex.Lock()
		dbImpl.seqNum = lastSequence

		if syncErr != nil {
			dbImpl.recordBackgroundError(syncErr)
		}

		if newWriteBatch == dbImpl.scratchBatch {
			dbImpl.scratchBatch.Reset()
		}

		ready := dbImpl.writers.Front()
		for {
			readyW := ready.Value.(*writer)
			if readyW != lastWriter {
				readyW.done = true
				readyW.err = err
				readyW.cv.Signal()
			}
			dbImpl.writers.Remove(ready)
			if readyW == lastWriter {
				break
			}
			ready = ready.Next()
		}

		if ready.Next() != nil {
			readyW := ready.Value.(*writer)
			readyW.cv.Signal()
		}

	}

	dbImpl.rwMutex.Unlock()

	return err
}

func (dbImpl *DBImpl) writeMem(mem *MemDB, batch *WriteBatch) error {

	seq := batch.seq
	idx := 0

	reqLen := len(batch.rep)

	for i := 0; i < batch.count; i++ {
		c := batch.rep[idx]
		idx += 1
		utils.Assert(idx < reqLen)
		kLen, n := binary.Uvarint(batch.rep[idx:])
		idx += n

		utils.Assert(idx < reqLen)
		var (
			key []byte
			val []byte
		)

		switch KeyType(c) {
		case KeyTypeValue:
			vLen, n := binary.Uvarint(batch.rep[idx:])
			idx += n
			utils.Assert(idx < reqLen)

			key = batch.rep[idx : idx+int(kLen)]
			idx += int(kLen)

			val = batch.rep[idx : idx+int(vLen)]
			idx += int(vLen)
			utils.Assert(idx < reqLen)

			err := mem.Put(key, seq+Sequence(i), val)
			if err != nil {
				return err
			}

		case KeyTypeDel:
			key = batch.rep[idx : idx+int(kLen)]
			idx += int(kLen)
			utils.Assert(idx < reqLen)

			err := mem.Del(key, seq+Sequence(i))
			if err != nil {
				return err
			}

		default:
			panic("invalid key type support")
		}

	}

	return nil

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
		} else if dbImpl.mem.ApproximateSize() <= options.KMemTableWriteBufferSize {
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
				imm := dbImpl.imm
				imm.UnRef()
				dbImpl.imm = dbImpl.mem
				atomic.StoreUint32(&dbImpl.hasImm, 1)
				mem := NewMemTable(int(dbImpl.opt.WriteBufferSize), dbImpl.opt.InternalComparer)
				mem.Ref()
				dbImpl.mem = mem
			} else {
				dbImpl.versionSet.reuseFileNum(journalFd.Num)
				return err
			}
			dbImpl.MaybeScheduleCompaction()
		}
	}

	return nil
}

func (dbImpl *DBImpl) mergeWriteBatch(lastWriter **writer) *WriteBatch {

	utils.AssertMutexHeld(&dbImpl.rwMutex)

	utils.Assert(dbImpl.writers.Len() > 0)

	front := dbImpl.writers.Front()
	firstBatch := front.Value.(*writer).batch
	size := firstBatch.Size()

	maxSize := 1 << 20  // 1m
	if size < 128<<10 { // limit the growth while in small write
		maxSize = size + 128<<10
	}

	result := firstBatch
	w := front.Next()
	for w != nil {
		wr := w.Value.(*writer)
		if size+wr.batch.Size() > maxSize {
			break
		}
		if result == firstBatch {
			result = dbImpl.scratchBatch
			result.append(firstBatch)
		}
		result.append(wr.batch)
		lastWriter = &wr
		w = w.Next()
	}

	return result

}

func (dbImpl *DBImpl) recordBackgroundError(err error) {
	if dbImpl.bgErr == nil {
		dbImpl.bgErr = err
		dbImpl.backgroundWorkFinishedSignal.Broadcast()
	}
}

// MaybeScheduleCompaction required mutex held
func (dbImpl *DBImpl) MaybeScheduleCompaction() {
	utils.AssertMutexHeld(&dbImpl.rwMutex)

	if dbImpl.backgroundCompactionScheduled {
		// do nothing
	} else if dbImpl.bgErr != nil {
		// do nothing
	} else if atomic.LoadUint32(&dbImpl.shutdown) == 1 {
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
	} else if atomic.LoadUint32(&dbImpl.shutdown) == 1 {
		// do nothing
	} else {
		dbImpl.backgroundCompaction()
	}

	dbImpl.backgroundCompactionScheduled = false
	dbImpl.MaybeScheduleCompaction()

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
		err = dbImpl.removeObsoleteFiles()
		if err != nil {
			// todo log warn msg
		}

	}

}

func (dbImpl *DBImpl) compactMemTable() {

	utils.AssertMutexHeld(&dbImpl.rwMutex)
	utils.Assert(dbImpl.imm != nil)

	edit := &VersionEdit{}
	err := dbImpl.writeLevel0Table(dbImpl.imm, edit)
	if err == nil {
		imm := dbImpl.imm
		imm.UnRef()
		dbImpl.imm = nil
		atomic.StoreUint32(&dbImpl.hasImm, 0)
		edit.setLogNum(dbImpl.journalFd.Num)
		edit.setLastSeq(dbImpl.frozenSeq)
		err = dbImpl.versionSet.logAndApply(edit, &dbImpl.rwMutex)
		if err == nil {
			err = dbImpl.removeObsoleteFiles()
		}
	}

	if err != nil {
		dbImpl.recordBackgroundError(err)
	}
}

func (dbImpl *DBImpl) writeLevel0Table(memDb *MemDB, edit *VersionEdit) (err error) {

	utils.AssertMutexHeld(&dbImpl.rwMutex)

	var fileMeta tFile
	fileMeta.fd = int(dbImpl.versionSet.allocFileNum())
	dbImpl.rwMutex.Unlock()
	defer func() {
		dbImpl.rwMutex.Lock()
		if err != nil {
			dbImpl.versionSet.reuseFileNum(uint64(fileMeta.fd))
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

func Open(dbpath string, option options.Options) (db DB, err error) {
	opt, err := sanitizeOptions(dbpath, option)
	if err != nil {
		return
	}
	dbImpl := newDBImpl(opt)
	dbImpl.rwMutex.Lock()
	defer dbImpl.rwMutex.Unlock()

	err = dbImpl.recover()
	if err != nil {
		return
	}

	if dbImpl.mem == nil {
		memDB := NewMemTable(0, opt.InternalComparer)
		memDB.Ref()
		dbImpl.mem = memDB

		journalFd := storage.Fd{
			FileType: storage.KJournalFile,
			Num:      dbImpl.versionSet.allocFileNum(),
		}
		sequentialWriter, err := dbImpl.opt.Storage.NewAppendableFile(journalFd)
		if err != nil {
			return
		}

		dbImpl.journalFd = journalFd
		dbImpl.journalWriter = wal.NewJournalWriter(sequentialWriter)
		edit := &VersionEdit{}
		edit.setLastSeq(dbImpl.seqNum)
		edit.setLogNum(dbImpl.journalFd.Num)
		err = dbImpl.versionSet.logAndApply(edit, &dbImpl.rwMutex)
		if err != nil {
			return
		}
	}

	err = dbImpl.removeObsoleteFiles()
	if err != nil {
		//todo warm log
		err = nil
	}
	dbImpl.MaybeScheduleCompaction()
	db = dbImpl
	return
}

func (dbImpl *DBImpl) recover() error {
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

	fds, err := dbImpl.opt.Storage.List()
	if err != nil {
		return err
	}

	logFiles := make([]storage.Fd, 0)

	for _, fd := range fds {
		if fd.FileType == storage.KJournalFile && fd.Num >= dbImpl.versionSet.stJournalNum {
			logFiles = append(logFiles, fd)
		}
	}

	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].Num < logFiles[j].Num
	})

	var edit VersionEdit

	for _, logFile := range logFiles {
		err = dbImpl.recoverLogFile(logFile, &edit)
		if err != nil {
			return err
		}
	}

	err = dbImpl.versionSet.logAndApply(&edit, &dbImpl.rwMutex)
	if err != nil {
		return err
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
	memDB := NewMemTable(0, dbImpl.opt.InternalComparer)
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
			if memDB.ApproximateSize() > options.KMemTableWriteBufferSize {
				err = dbImpl.writeLevel0Table(memDB, edit)
				if err != nil {
					return err
				}
				memDB.UnRef()
				memDB = NewMemTable(0, dbImpl.opt.InternalComparer)
				memDB.Ref()
			}

			err = writeBatch.insertInto(memDB)
			if err != nil {
				return err
			}

			dbImpl.seqNum += writeBatch.seq + Sequence(writeBatch.count) - 1
			dbImpl.versionSet.markFileUsed(fd.Num)
		}

	}

	if memDB.Size() > 0 {
		err = dbImpl.writeLevel0Table(memDB, edit)
		if err != nil {
			return err
		}
	}

	edit.setLastSeq(dbImpl.seqNum)
	edit.setLogNum(fd.Num)
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

	liveTableFileSet := make(map[uint64]struct{})
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

func sanitizeOptions(dbpath string, options options.Options) (*options.Options, error) {
	return nil, nil
}

func newDBImpl(opt *options.Options) *DBImpl {

	db := &DBImpl{
		versionSet: &VersionSet{
			versions:    list.New(),
			compactPtrs: [7]compactPtr{},
			tableCache:  newTableCache(opt),
			opt:         opt,
		},
		writers:    list.New(),
		hasImm:     0,
		tableCache: newTableCache(opt),
		snapshots:  list.New(),
		opt:        opt,
		closed:     make(chan struct{}),
	}

	db.backgroundWorkFinishedSignal = sync.NewCond(&db.rwMutex)
	db.tableOperation = newTableOperation(opt, db.tableCache)
	db.scheduler = NewSchedule(db.closed)
	return db
}
