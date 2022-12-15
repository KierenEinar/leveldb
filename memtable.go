package leveldb

import (
	"bytes"
	"leveldb/collections"
	"leveldb/ikey"
	"time"
)

type MemDB struct {
	iCmp *iComparer
	*collections.SkipList
}

func NewMemTable(capacity int, iCmp *iComparer) *MemDB {
	memDB := &MemDB{
		SkipList: collections.NewSkipList(time.Now().UnixNano(), capacity, iCmp),
		iCmp:     iCmp,
	}
	return memDB
}

func (memTable *MemDB) Put(ukey []byte, sequence ikey.Sequence, value []byte) error {
	ikey := ikey.BuildInternalKey(nil, ukey, ikey.KeyTypeValue, sequence)
	return memTable.SkipList.Put(ikey, value)
}

func (memTable *MemDB) Del(ukey []byte, sequence ikey.Sequence) error {
	ikey := ikey.BuildInternalKey(nil, ukey, ikey.KeyTypeDel, sequence)
	return memTable.SkipList.Put(ikey, nil)
}

// Find find the ukey whose eq ikey.uKey(), if keytype is del, err is ErrNotFound, and will return the rkey
func (memTable *MemDB) Find(ikey ikey.InternalKey) (rkey []byte, value []byte, err error) {
	node, _, err := memTable.SkipList.FindGreaterOrEqual(ikey)
	if err != nil {
		return
	}
	if node != nil { // node is ge ikey
		memTable.SkipList.rw.RLock()
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
