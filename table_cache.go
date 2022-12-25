package leveldb

import (
	"encoding/binary"
	"leveldb/cache"
	"leveldb/comparer"
	"leveldb/options"
	"leveldb/storage"
	"leveldb/table"
	"runtime"
)

type TableCache struct {
	cache   cache.Cache
	storage storage.Storage
	cmp     comparer.Comparer
	opt     *options.Options
}

func (c *TableCache) Close() {
	c.cache.Close()
}

func NewTableCache(opt *options.Options) *TableCache {
	c := &TableCache{
		cache:   cache.NewCache(opt.MaxOpenFiles, opt.Hash32),
		storage: opt.Storage,
		cmp:     opt.InternalComparer,
		opt:     opt,
	}
	runtime.SetFinalizer(c, (*TableCache).Close)
	return c
}

func (c *TableCache) Get(ikey InternalKey, tFile tFile, f func(rkey InternalKey, value []byte)) error {
	var cacheHandle *cache.LRUHandle
	if err := c.findTable(tFile, &cacheHandle); err != nil {
		return err
	}
	tReader, ok := cacheHandle.Value().(*table.Reader)
	if !ok {
		panic("leveldb/cache value not type *Reader")
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
		reader, oErr := c.storage.NewRandomAccessReader(tFile.fd)
		if oErr != nil {
			err = oErr
			return
		}
		tReader, tErr := table.NewTableReader(c.opt, reader, tFile.Size)
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
	tReader, ok := value.(*table.Reader)
	if !ok {
		panic("leveldb/cache value not type *Reader")
	}
	tReader.UnRef()
}
