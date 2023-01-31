package leveldb

import (
	"container/list"
	"sync"
)

type Snapshot struct {
	mu        sync.RWMutex
	db        *DBImpl
	snapshots *list.List
}

func (db *DBImpl) NewSnapShot() *Snapshot {
	return &Snapshot{
		mu:        sync.RWMutex{},
		db:        db,
		snapshots: list.New(),
	}
}

type SnapshotElement struct {
	seq     sequence
	element *list.Element
}

func (snapshot *Snapshot) AcquireSnapShot() *SnapshotElement {
	seq := snapshot.db.getSeq()

}
