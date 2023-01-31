package leveldb

import (
	"container/list"
	"sync"

	"github.com/KierenEinar/leveldb/errors"
)

type snapshotElement struct {
	seq     sequence
	element *list.Element
	ref     int
}

func (dbImpl *DBImpl) acquireSnapshot() *snapshotElement {
	dbImpl.snapsMu.Lock()
	defer dbImpl.snapsMu.Unlock()
	lastSeq := dbImpl.getSeq()
	if e := dbImpl.snapshots.Back(); e != nil {
		se := e.Value.(*snapshotElement)
		if se.seq == dbImpl.getSeq() {
			se.ref++
			return se
		}
	}
	se := &snapshotElement{
		seq: lastSeq,
		ref: 1,
	}
	se.element = dbImpl.snapshots.PushBack(se)
	return se
}

func (dbImpl *DBImpl) releaseSnapshot(se *snapshotElement) {

	dbImpl.snapsMu.Lock()
	defer dbImpl.snapsMu.Unlock()
	se.ref--
	if se.ref == 0 {
		dbImpl.snapshots.Remove(se.element)
		se.element = nil
	}
}

type Snapshot struct {
	mu       sync.Mutex
	db       *DBImpl
	ele      *snapshotElement
	released bool
}

func (dbImpl *DBImpl) GetSnapshot() (*Snapshot, error) {
	if dbImpl.ok() {
		return dbImpl.newSnapshot(), nil
	}
	return nil, errors.ErrClosed
}

func (dbImpl *DBImpl) newSnapshot() *Snapshot {
	ele := dbImpl.acquireSnapshot()
	return &Snapshot{
		db:  dbImpl,
		ele: ele,
	}
}
