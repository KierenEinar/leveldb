package leveldb

import (
	"container/list"
	"runtime"
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
	mu       sync.RWMutex
	db       *DBImpl
	ele      *snapshotElement
	released bool
}

// GetSnapshot caller should call Release after done
func (dbImpl *DBImpl) GetSnapshot() (*Snapshot, error) {
	if dbImpl.ok() {
		return dbImpl.newSnapshot(), nil
	}
	return nil, errors.ErrClosed
}

func (snapshot *Snapshot) Release() {
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()

	if !snapshot.released {
		snapshot.released = true
		snapshot.db.releaseSnapshot(snapshot.ele)
		snapshot.db = nil
		snapshot.ele = nil
		runtime.SetFinalizer(snapshot, nil)
	}
}

func (dbImpl *DBImpl) newSnapshot() *Snapshot {
	ele := dbImpl.acquireSnapshot()
	s := &Snapshot{
		db:  dbImpl,
		ele: ele,
	}
	runtime.SetFinalizer(s, (*Snapshot).Release)
	return s
}

func (snapshot *Snapshot) Get(key []byte) ([]byte, error) {
	snapshot.mu.RLock()
	defer snapshot.mu.RUnlock()
	if snapshot.released {
		return nil, errors.ErrReleased
	}
	return snapshot.db.get(snapshot.ele.seq, key)
}
