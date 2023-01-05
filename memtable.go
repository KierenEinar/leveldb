package leveldb

import (
	"bytes"
	"leveldb/collections"
	"leveldb/comparer"
	"time"
)

type MemDB struct {
	cmp comparer.BasicComparer
	*collections.SkipList
}

func NewMemTable(capacity int, cmp comparer.BasicComparer) *MemDB {
	memDB := &MemDB{
		SkipList: collections.NewSkipList(time.Now().UnixNano(), capacity, cmp),
		cmp:      cmp,
	}
	return memDB
}

func (memTable *MemDB) Put(ukey []byte, sequence Sequence, value []byte) error {
	ikey := BuildInternalKey(nil, ukey, KeyTypeValue, sequence)
	return memTable.SkipList.Put(ikey, value)
}

func (memTable *MemDB) Del(ukey []byte, sequence Sequence) error {
	ikey := BuildInternalKey(nil, ukey, KeyTypeDel, sequence)
	return memTable.SkipList.Put(ikey, nil)
}

// Find find the ukey whose eq ikey.uKey(), if keytype is del, err is ErrNotFound, and will return the rkey
func (memTable *MemDB) Find(ukey []byte) (rkey []byte, value []byte, err error) {
	node, _, err := memTable.SkipList.FindGreaterOrEqual(ukey)
	if err != nil {
		return
	}
	if node != nil { // node is ge ikey
		ikeyN := node.key(memTable.SkipList.kvData)
		valueN := node.value(memTable.SkipList.kvData)
		memTable.SkipList.rw.RUnlock()
		ukey, kt, _, err := parseInternalKey(ikeyN)
		if err != nil {
			return nil, nil, err
		}
		if bytes.Compare(ukey, ikey.ukey()) == 0 {
			rkey = ikeyN
			if kt == keyTypeDel {
				err = ErrKeyDel
				return
			}
			value = valueN
			return
		}
	}
	err = ErrNotFound
	return
}

func (memTable *MemDB) ApproximateSize() int {
	return memTable.Size()
}
