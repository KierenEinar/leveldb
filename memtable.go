package leveldb

import (
	"bytes"
	"leveldb/collections"
	"leveldb/comparer"
	"leveldb/errors"
	"leveldb/utils"
	"time"
)

type MemDB struct {
	cmp comparer.BasicComparer
	*collections.SkipList
	*utils.BasicReleaser
}

func NewMemTable(capacity int, cmp comparer.BasicComparer) *MemDB {
	memDB := &MemDB{
		SkipList:      collections.NewSkipList(time.Now().UnixNano(), capacity, cmp),
		BasicReleaser: &utils.BasicReleaser{},
	}
	return memDB
}

func (memTable *MemDB) Put(ukey []byte, sequence sequence, value []byte) error {
	ikey := buildInternalKey(nil, ukey, keyTypeValue, sequence)
	return memTable.SkipList.Put(ikey, value)
}

func (memTable *MemDB) Del(ukey []byte, sequence sequence) error {
	ikey := buildInternalKey(nil, ukey, keyTypeDel, sequence)
	return memTable.SkipList.Put(ikey, nil)
}

// Find find the ukey whose eq ikey.uKey(), if keytype is del, err is ErrNotFound, and will return the rkey
func (memTable *MemDB) Find(ikey internalKey) (rkey []byte, value []byte, err error) {
	node, _, err := memTable.SkipList.FindGreaterOrEqual(ikey)
	if err != nil {
		return
	}
	if node != nil { // node is ge ikey
		ikeyN := memTable.SkipList.Key(node)
		ukey, kt, _, err := parseInternalKey(ikeyN)
		if err != nil {
			return nil, nil, err
		}
		if bytes.Compare(ukey, ikey.userKey()) == 0 {
			rkey = ikeyN
			if kt == keyTypeDel {
				err = errors.ErrKeyDel
				return
			}
			value = memTable.SkipList.Value(node)
			return
		}
	}
	err = errors.ErrNotFound
	return
}

func (memTable *MemDB) ApproximateSize() int {
	return memTable.SkipList.Size()
}
