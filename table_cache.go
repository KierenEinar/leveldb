package leveldb

import (
	"encoding/binary"
	hash2 "hash"
	"leveldb/cache"
	"leveldb/comparer"
	"leveldb/storage"
	"leveldb/table"
	"runtime"
)

type TableCache struct {
	cache   cache.Cache
	storage storage.Storage
	cmp     comparer.BasicComparer
}

func (c *TableCache) Close() {
	c.cache.Close()
}

func NewTableCache(storage storage.Storage, capacity uint32, hash32 hash2.Hash32, cmp comparer.BasicComparer) *TableCache {
	c := &TableCache{
		cache:   cache.NewCache(capacity, hash32),
		storage: storage,
	}
	runtime.SetFinalizer(c, (*TableCache).Close)
	return c
}

func (c *TableCache) Get(ikey InternalKey, tFile tFile, f func(rkey InternalKey, value []byte)) error {
	var cacheHandle *cache.LRUHandle
	if err := c.findTable(tFile, &cacheHandle); err != nil {
		return err
	}
	tReader, ok := cacheHandle.Value().(*table.TableReader)
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

func (c *TableCache) Evict(tFile tFile) {
	lookupKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(lookupKey, tFile.fd.Num)
	c.cache.Erase(lookupKey)
}

func (c *TableCache) findTable(tFile tFile, cacheHandle **cache.LRUHandle) (err error) {
	lookupKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(lookupKey, tFile.fd.Num)
	handle := c.cache.Lookup(lookupKey)
	if handle == nil {
		reader, oErr := c.storage.Open(tFile.fd)
		if oErr != nil {
			err = oErr
			return
		}
		tReader, tErr := table.NewTableReader(reader, tFile.Size, c.cmp)
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
	tReader, ok := value.(*table.TableReader)
	if !ok {
		panic("leveldb/cache value not type *TableReader")
	}
	tReader.UnRef()
}
