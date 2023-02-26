package leveldb

import (
	"github.com/KierenEinar/leveldb/options"
	"github.com/KierenEinar/leveldb/storage"
	"github.com/KierenEinar/leveldb/wal"
)

type DB interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Close() error
}

const KLevelNum = 7

const KLevel1SizeThreshold = 10 * (1 << 20) //10m

func MaxBytesForLevel(level int) uint64 {
	result := uint64(KLevel1SizeThreshold)
	for level > 1 {
		result *= 10
		level--
	}
	return result
}

func Open(dbpath string, option *options.Options) (db DB, err error) {
	opt, err := sanitizeOptions(dbpath, option)
	if err != nil {
		return
	}

	dbImpl := newDBImpl(opt)
	dbImpl.rwMutex.Lock()
	defer dbImpl.rwMutex.Unlock()
	edit := newVersionEdit()
	err = dbImpl.recover(edit)
	if err != nil {
		return
	}

	if dbImpl.mem == nil {
		memDB := dbImpl.mPoolGet(0)
		memDB.Ref()
		dbImpl.mem = memDB

		journalFd := storage.Fd{
			FileType: storage.KJournalFile,
			Num:      dbImpl.versionSet.allocFileNum(),
		}
		sequentialWriter, wErr := dbImpl.opt.Storage.NewAppendableFile(journalFd)
		if wErr != nil {
			err = wErr
			return
		}
		dbImpl.journalSeqWriter = sequentialWriter
		dbImpl.journalFd = journalFd
		dbImpl.journalWriter = wal.NewJournalWriter(sequentialWriter, opt.NoWriteSync)
		edit.setLogNum(dbImpl.journalFd.Num)
		err = dbImpl.versionSet.logAndApply(edit, &dbImpl.rwMutex)
		if err != nil {
			return
		}
	}

	for _, v := range edit.addedTables {
		delete(dbImpl.pendingOutputs, v.number)
	}

	err = dbImpl.removeObsoleteFiles()
	if err != nil {
		//todo warm log
		err = nil
	}

	dbImpl.maybeScheduleCompaction()
	go dbImpl.mPoolDrain()
	db = dbImpl
	return
}
