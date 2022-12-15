package leveldb

import (
	"encoding/binary"
	hash2 "hash"
	"runtime"
)

type TableCache struct {
	cache   Cache
	storage Storage
}

func (c *TableCache) Close() {
	c.cache.Close()
}

func NewTableCache(storage Storage, capacity uint32, hash32 hash2.Hash32) *TableCache {
	c := &TableCache{
		cache:   NewCache(capacity, hash32),
		storage: storage,
	}
	runtime.SetFinalizer(c, (*TableCache).Close)
	return c
}

func (c *TableCache) Get(ikey InternalKey, tFile TFile, f func(rkey InternalKey, value []byte)) error {
	var cacheHandle *LRUHandle
	if err := c.findTable(tFile, &cacheHandle); err != nil {
		return err
	}
	tReader, ok := cacheHandle.value.(*TableReader)
	if !ok {
		panic("leveldb/cache value not type *TableReader")
	}
	rKey, rValue, rErr := tReader.Find(ikey)
	if rErr != nil {
		return rErr
	}
	f(rKey, append([]byte(nil), rValue...))
	c.cache.UnRef(cacheHandle)
	return nil
}

func (c *TableCache) Evict(tFile TFile) {
	lookupKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(lookupKey, tFile.fd.Num)
	c.cache.Erase(lookupKey)
}

func (c *TableCache) findTable(tFile TFile, cacheHandle **LRUHandle) (err error) {
	lookupKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(lookupKey, tFile.fd.Num)
	handle := c.cache.Lookup(lookupKey)
	if handle == nil {
		reader, oErr := c.storage.Open(tFile.fd)
		if oErr != nil {
			err = oErr
			return
		}
		tReader, tErr := NewTableReader(reader, tFile.Size)
		if tErr != nil {
			err = tErr
			return
		}
		handle = c.cache.Insert(lookupKey, 1, tReader, c.deleteEntry)
	}
	*cacheHandle = handle
	return
}

func (c *TableCache) deleteEntry(key []byte, value interface{}) {
	tReader, ok := value.(*TableReader)
	if !ok {
		panic("leveldb/cache value not type *TableReader")
	}
	tReader.UnRef()
}
