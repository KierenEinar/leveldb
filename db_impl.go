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
	VersionSet *VersionSet

	journalWriter *wal.JournalWriter

	shutdown uint32

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

	opt *options.Options
}

func (dbImpl *DBImpl) Get(key []byte) ([]byte, error) {

	dbImpl.rwMutex.RLock()
	v := dbImpl.VersionSet.getCurrent()
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
		} else if allowDelay && dbImpl.VersionSet.levelFilesNum(0) >= options.KLevel0SlowDownTrigger {
			allowDelay = false
			dbImpl.rwMutex.Unlock()
			time.Sleep(time.Microsecond * 1000)
			dbImpl.rwMutex.Lock()
		} else if dbImpl.mem.ApproximateSize() <= options.KMemTableWriteBufferSize {
			break
		} else if dbImpl.imm != nil { // wait background compaction compact imm table
			dbImpl.backgroundWorkFinishedSignal.Wait()
		} else if dbImpl.VersionSet.levelFilesNum(0) >= options.KLevel0StopWriteTrigger {
			dbImpl.backgroundWorkFinishedSignal.Wait()
		} else {
			journalFd := storage.Fd{
				FileType: storage.KJournalFile,
				Num:      dbImpl.VersionSet.allocFileNum(),
			}
			s := dbImpl.VersionSet.storage
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
				dbImpl.VersionSet.reuseFileNum(journalFd.Num)
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
	} else if atomic.LoadUint32(&dbImpl.hasImm) == 0 && !dbImpl.VersionSet.needCompaction() {
		// do nothing
	} else {
		go dbImpl.backgroundCall()
	}

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

	c := dbImpl.VersionSet.pickCompaction1()
	if c == nil {
		return
	} else if len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0 && c.gp.size() <= c.gpOverlappedLimit {

		edit := &VersionEdit{}
		addTable := c.inputs[0][0]
		edit.addNewTable(c.cPtr.level+1, addTable.Size, addTable.fd.Num, addTable.iMin, addTable.iMax)
		err := db.VersionSet.logAndApply(edit, &db.rwMutex)
		if err != nil {
			db.recordBackgroundError(err)
		}
	} else {
		err := db.doCompactionWork(c)
		if err != nil {
			db.recordBackgroundError(err)
		}
		c.releaseInputs()
		err = db.removeObsoleteFiles()
		if err != nil {
			// todo log warn msg
		}

	}

}

func (db *DB) compactMemTable() {

	assertMutexHeld(&db.rwMutex)
	assert(db.imm != nil)

	edit := &VersionEdit{}
	err := db.writeLevel0Table(db.imm, edit)
	if err == nil {
		imm := db.imm
		db.imm = nil
		imm.UnRef()
		atomic.StoreUint32(&db.hasImm, 1)
		edit.setLogNum(db.journalFd.Num)
		edit.setLastSeq(db.frozenSeq)
		err = db.VersionSet.logAndApply(edit, &db.rwMutex)
		if err == nil {
			err = db.removeObsoleteFiles()
		}
	}

	if err != nil {
		db.recordBackgroundError(err)
	}
}

func (db *DB) writeLevel0Table(memDb *MemDB, edit *VersionEdit) (err error) {

	db.rwMutex.Unlock()
	defer db.rwMutex.Lock()

	tWriter, err := db.tableOperation.create()
	if err != nil {
		return err
	}

	iter := memDb.NewIterator()
	defer iter.UnRef()

	for iter.Next() {
		err = tWriter.append(iter.Key(), iter.Value())
		if err != nil {
			db.rwMutex.Lock()
			return err
		}
	}

	tFile, err := tWriter.finish()
	if err == nil {
		edit.addNewTable(0, tFile.Size, tFile.fd.Num, tFile.iMin, tFile.iMax)
	}
	return
}

func Open(dbpath string, option options.Options) (*DB, error) {
	opt := sanitizeOptions(option)

	db := &DBImpl{
		VersionSet: &VersionSet{
			cmp:      opt.InternalComparer,
			storage:  opt.Storage,
			versions: list.New(),
		},
		tableCache: newTableCache(opt),
	}

	tableOperation := newTableOperation(opt, db.VersionSet)
	db.VersionSet.tableOperation = tableOperation

	db.rwMutex.Lock()
	defer db.rwMutex.Unlock()

	err = db.recover()
	if err != nil {
		return nil, err
	}

	memDB := NewMemTable(0, db.VersionSet.cmp)
	memDB.Ref()

	db.mem = memDB
	journalFd := Fd{
		FileType: KJournalFile,
		Num:      db.VersionSet.allocFileNum(),
	}
	sequentialWriter, err := storage.Create(journalFd)
	if err != nil {
		return nil, err
	}
	db.journalFd = journalFd
	db.journalWriter = NewJournalWriter(sequentialWriter)

	edit := &VersionEdit{}
	edit.setLastSeq(db.seqNum)
	edit.setLogNum(db.journalFd.Num)
	err = db.VersionSet.logAndApply(edit, &db.rwMutex)
	if err != nil {
		return nil, err
	}

	//todo warn err log
	err = db.removeObsoleteFiles()
	db.MaybeScheduleCompaction()

	return db, nil
}

func (dbImpl *DBImpl) recover() error {
	manifestFd, err := dbImpl.VersionSet.storage.GetCurrent()
	if err != nil {
		if err != os.ErrNotExist {
			return err
		}

		if !dbImpl.opt.CreateIfMissingCurrent {
			return errors.ErrMissingCurrent
		}

		err = dbImpl.newDb()
	} else {
		err = dbImpl.VersionSet.recover(manifestFd)
	}

	if err != nil {
		return err
	}

	fds, err := db.VersionSet.storage.List()
	if err != nil {
		return err
	}

	var expectedFiles = make(map[uint64]struct{})
	db.VersionSet.addLiveFiles(expectedFiles)

	logFiles := make([]Fd, 0)

	for _, fd := range fds {
		if fd.FileType == KTableFile {
			delete(expectedFiles, fd.Num)
		} else if fd.FileType == KJournalFile && fd.Num >= db.VersionSet.stJournalNum {
			logFiles = append(logFiles, fd)
		}
	}

	if len(expectedFiles) > 0 {
		err = NewErrCorruption("invalid table file, file not exists")
		return err
	}

	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].Num < logFiles[j].Num
	})

	var edit VersionEdit

	for _, logFile := range logFiles {
		err = db.recoverLogFile(logFile, &edit)
		if err != nil {
			return err
		}
	}

	err = db.VersionSet.logAndApply(&edit, &db.rwMutex)
	if err != nil {
		return err
	}
	return nil
}

func (dbImpl *DBImpl) newDb() (err error) {
	dbImpl.seqNum = 0

	manifestFd := storage.Fd{
		FileType: storage.KDescriptorFile,
		Num:      1,
	}

	writer, wErr := dbImpl.VersionSet.storage.NewAppendableFile(manifestFd)
	if wErr != nil {
		err = wErr
		return
	}

	defer func() {
		if err == nil {
			return
		}
		_ = writer.Close()
		_ = dbImpl.VersionSet.storage.Remove(manifestFd)
	}()

	dbImpl.VersionSet.manifestFd = manifestFd
	dbImpl.VersionSet.manifestWriter = wal.NewJournalWriter(writer)

	newDb := &VersionEdit{
		journalNum:   dbImpl.journalFd.Num,
		nextFileNum:  3,
		lastSeq:      0,
		comparerName: dbImpl.opt.InternalComparer.Name(),
	}

	newDb.EncodeTo(dbImpl.VersionSet.manifestWriter)
	if newDb.err != nil {
		err = newDb.err
		return
	}

	err = dbImpl.VersionSet.manifestWriter.Sync()
	if err != nil {
		return
	}

	err = dbImpl.VersionSet.storage.SetCurrent(manifestFd.Num)
	return

}

func (db *DB) recoverLogFile(fd Fd, edit *VersionEdit) error {

	reader, err := db.VersionSet.storage.Open(fd)
	if err != nil {
		return err
	}
	journalReader := NewJournalReader(reader)
	memDB := NewMemTable(0, db.VersionSet.cmp)
	memDB.Ref()
	defer func() {
		memDB.UnRef()
		journalReader.Close()
		_ = reader.Close()
	}()
	for {
		sequentialReader, err := journalReader.NextChunk()

		if err == io.EOF {
			break
		}
		writeBatch, err := buildBatchGroup(sequentialReader, db.VersionSet.stSeqNum)

		if err != nil {
			return err
		}

		if memDB.ApproximateSize() > kMemTableWriteBufferSize {
			err = db.writeLevel0Table(memDB, edit)
			if err != nil {
				return err
			}
			memDB.UnRef()

			memDB = NewMemTable(0, db.VersionSet.cmp)
			memDB.Ref()
		}

		err = writeBatch.insertInto(memDB)
		if err != nil {
			return err
		}

		db.seqNum += writeBatch.seq + Sequence(writeBatch.count) - 1

		db.VersionSet.markFileUsed(fd.Num)

	}

	if memDB.Size() > 0 {
		err = db.writeLevel0Table(memDB, edit)
		if err != nil {
			return err
		}
	}

	if db.VersionSet.nextFileNum != db.VersionSet.manifestFd.Num {
		db.VersionSet.manifestFd = Fd{
			FileType: KDescriptorFile,
			Num:      db.VersionSet.allocFileNum(),
		}
	}

	return nil

}

// clear the obsolete files
func (db *DB) removeObsoleteFiles() (err error) {

	assertMutexHeld(&db.rwMutex)

	fds, lErr := db.VersionSet.storage.List()
	if lErr != nil {
		err = lErr
		return
	}

	liveTableFileSet := make(map[Fd]struct{})
	db.VersionSet.addLiveFiles(liveTableFileSet)

	fileToClean := make([]Fd, 0)

	for _, fd := range fds {
		var keep bool
		switch fd.FileType {
		case KDescriptorFile:
			keep = fd.Num >= db.VersionSet.manifestFd.Num
		case KJournalFile:
			keep = fd.Num >= db.VersionSet.stJournalNum
		case KTableFile:
			if _, ok := liveTableFileSet[fd]; ok {
				keep = true
			}
		case KCurrentFile, KDBLockFile, KDBTempFile:
			keep = true
		}

		if !keep {
			fileToClean = append(fileToClean, fd)
		}

	}

	db.rwMutex.Unlock()

	for _, fd := range fileToClean {
		rErr := db.VersionSet.storage.Remove(fd)
		if rErr != nil {
			err = rErr
		}

		// todo evict table cache

	}

	db.rwMutex.Lock()
	return
}

func sanitizeOptions(options options.Options) *options.Options {
	return nil
}
