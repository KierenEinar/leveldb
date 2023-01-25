package leveldb

import (
	"encoding/binary"
	"leveldb/cache"
	"leveldb/iterator"
	"leveldb/options"
	"leveldb/storage"
	"leveldb/table"
	"runtime"
)

type TableCache struct {
	cache   cache.Cache
	storage storage.Storage
	opt     *options.Options
}

func (c *TableCache) Close() {
	c.cache.Close()
	runtime.SetFinalizer(c, nil)
}

func newTableCache(opt *options.Options) *TableCache {
	c := &TableCache{
		cache:   cache.NewCache(opt.MaxOpenFiles, opt.Hash32),
		storage: opt.Storage,
		opt:     opt,
	}
	runtime.SetFinalizer(c, (*TableCache).Close)
	return c
}

func (c *TableCache) Get(ikey internalKey, tFile tFile, f func(rkey internalKey, value []byte, err error)) error {
	var cacheHandle *cache.LRUHandle
	if err := c.findTable(tFile, &cacheHandle); err != nil {
		return err
	}
	tReader, ok := cacheHandle.Value().(*table.Reader)
	if !ok {
		panic("leveldb/cache value not type *Reader")
	}
	rKey, rValue, rErr := tReader.Find(ikey)
	f(rKey, append([]byte(nil), rValue...), rErr)
	c.cache.UnRef(cacheHandle)
	return nil
}

func (c *TableCache) NewIterator(tFile tFile) (iter iterator.Iterator, err error) {
	var cacheHandle *cache.LRUHandle
	if err = c.findTable(tFile, &cacheHandle); err != nil {
		return
	}
	tReader, ok := cacheHandle.Value().(*table.Reader)
	if !ok {
		panic("leveldb/cache value not type *Reader")
	}

	iter = tReader.NewIterator()
	iter.RegisterCleanUp(c.releaseHandle, cacheHandle)
	return
}

func (c *TableCache) releaseHandle(args ...interface{}) {
	h, ok := args[0].(*cache.LRUHandle)
	if !ok {
		panic("TableCache releaseHandle args 0 not *cache.LRUHandle")
	}
	c.cache.UnRef(h)
}

func (c *TableCache) Evict(fd uint64) {
	lookupKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(lookupKey, fd)
	c.cache.Erase(lookupKey)
}

func (c *TableCache) findTable(tFile tFile, cacheHandle **cache.LRUHandle) (err error) {
	lookupKey := make([]byte, 8)
	binary.LittleEndian.PutUint64(lookupKey[8:], uint64(tFile.fd))
	handle := c.cache.Lookup(lookupKey)
	if handle == nil {
		reader, oErr := c.storage.NewRandomAccessReader(storage.Fd{
			FileType: storage.KTableFile,
			Num:      uint64(tFile.fd),
		})
		if oErr != nil {
			err = oErr
			return
		}
		tReader, tErr := table.NewTableReader(c.opt, reader, tFile.size)
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
