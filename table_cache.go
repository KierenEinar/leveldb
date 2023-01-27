package leveldb

import (
	"encoding/binary"
	"runtime"

	"github.com/KierenEinar/leveldb/cache"
	"github.com/KierenEinar/leveldb/errors"
	"github.com/KierenEinar/leveldb/iterator"
	"github.com/KierenEinar/leveldb/options"
	"github.com/KierenEinar/leveldb/storage"
	"github.com/KierenEinar/leveldb/table"
)

type TableCache struct {
	cache cache.Cache
	opt   *options.Options
}

func (c *TableCache) Close() {
	c.cache.Close()
	runtime.SetFinalizer(c, nil)
}

func NewTableCache(opt *options.Options) *TableCache {
	c := &TableCache{
		cache: cache.NewCache(opt.MaxOpenFiles, opt.Hash32),
		opt:   opt,
	}
	runtime.SetFinalizer(c, (*TableCache).Close)
	return c
}

func (c *TableCache) Get(ikey internalKey, tFile tFile, f func(rkey internalKey, value []byte, err error)) {
	var cacheHandle *cache.LRUHandle
	var err error
	if err = c.findTable(tFile, &cacheHandle); err != nil {
		f(nil, nil, err)
		return
	}
	tReader, ok := cacheHandle.Value().(*table.Reader)
	if !ok {
		f(nil, nil, errors.ErrCacheHandleConvertErr)
		return
	}
	rKey, rValue, rErr := tReader.Find(ikey)
	f(append([]byte(nil), rKey...), append([]byte(nil), rValue...), rErr)
	c.cache.UnRef(cacheHandle)
	return
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
	binary.LittleEndian.PutUint64(lookupKey, uint64(tFile.fd))
	handle := c.cache.Lookup(lookupKey)
	if handle == nil {
		reader, err := c.opt.Storage.NewRandomAccessReader(storage.Fd{
			FileType: storage.KTableFile,
			Num:      uint64(tFile.fd),
		})
		if err != nil {
			return
		}
		tReader, err := table.NewTableReader(c.opt, reader, tFile.size)
		if err != nil {
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
