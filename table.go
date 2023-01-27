package leveldb

import (
	"bytes"
	"leveldb/comparer"
	"leveldb/errors"
	"leveldb/iterator"
	"leveldb/options"
	"leveldb/storage"
	"leveldb/table"
	"leveldb/utils"
	"sort"
	"unsafe"
)

type tFile struct {
	fd         int
	iMax       internalKey
	iMin       internalKey
	size       int
	allowSeeks int
}

type tFileHeader struct {
	addr uintptr
	len  int
	cap  int
}

func encodeTFile(t *tFile) []byte {
	l := unsafe.Sizeof(t)
	header := &tFileHeader{
		addr: uintptr(unsafe.Pointer(t)),
		len:  int(l),
		cap:  int(l),
	}
	data := *(*[]byte)(unsafe.Pointer(header))
	return data
}

func decodeTFile(buf []byte) *tFile {
	tFile := *(**tFile)(unsafe.Pointer(&buf))
	return tFile
}

type tFiles []tFile

func (sf tFiles) size() (size int) {
	for _, v := range sf {
		size += v.size
	}
	return
}

type Levels [options.KLevelNum]tFiles

func (s tFile) isOverlapped(umin []byte, umax []byte) bool {
	smin, smax := s.iMin.userKey(), s.iMax.userKey()
	return !(bytes.Compare(smax, umin) < 0) && !(bytes.Compare(smin, umax) > 0)
}

func (s tFiles) getOverlapped(imin internalKey, imax internalKey, overlapped bool) (dst tFiles) {

	if !overlapped {

		var (
			umin, umax        = imin.userKey(), imax.userKey()
			smallest, largest int
			sizeS             = len(s)
		)

		// use binary search begin
		n := sort.Search(sizeS, func(i int) bool {
			return bytes.Compare(s[i].iMin.userKey(), umin) >= 0
		})

		if n == 0 {
			smallest = 0
		} else if bytes.Compare(s[n-1].iMax.userKey(), umin) >= 0 {
			smallest = n - 1
		} else {
			smallest = sizeS
		}

		n = sort.Search(sizeS, func(i int) bool {
			return bytes.Compare(s[i].iMax.userKey(), umax) >= 0
		})

		if n == sizeS {
			largest = sizeS
		} else if bytes.Compare(s[n].iMin.userKey(), umax) >= 0 {
			largest = n + 1
		} else {
			largest = n
		}

		if smallest >= largest {
			return
		}

		dst = make(tFiles, largest-smallest)
		copy(dst, s[smallest:largest])
		return
	}

	var (
		i          = 0
		restart    = false
		umin, umax = imin.userKey(), imax.userKey()
	)

	for i < len(s) {
		sFile := s[i]
		if sFile.isOverlapped(umin, umax) {
			if bytes.Compare(sFile.iMax.userKey(), umax) > 0 {
				umax = sFile.iMax.userKey()
				restart = true
			}
			if bytes.Compare(sFile.iMin.userKey(), umin) < 0 {
				umin = sFile.iMin.userKey()
				restart = true
			}
			if restart {
				dst = dst[:0]
				i = 0
				restart = false // reset
			} else {
				dst = append(dst, sFile)
			}
		}
	}
	return
}

type tableOperation struct {
	opt        *options.Options
	tableCache *TableCache
}

func newTableOperation(opt *options.Options, tableCache *TableCache) *tableOperation {
	return &tableOperation{
		opt:        opt,
		tableCache: tableCache,
	}
}

func (tableOperation *tableOperation) create(fileMeta tFile) (*tWriter, error) {
	w, err := tableOperation.opt.Storage.NewWritableFile(storage.Fd{
		FileType: storage.KTableFile,
		Num:      uint64(fileMeta.fd),
	})
	if err != nil {
		return nil, err
	}
	return &tWriter{
		w:          w,
		fileMeta:   fileMeta,
		tableCache: tableOperation.tableCache,
		tw:         table.NewWriter(w, tableOperation.opt),
	}, nil
}

type tWriter struct {
	w           storage.SequentialWriter
	fileMeta    tFile
	tw          *table.Writer
	tableCache  *TableCache
	first, last internalKey
}

func (t *tWriter) append(ikey internalKey, value []byte) error {
	if t.first == nil {
		t.first = append([]byte(nil), ikey...)
	}
	t.last = append(t.last[:0], ikey...)
	return t.tw.Append(ikey, value)
}

func (t *tWriter) finish() error {

	err := t.tw.Close()
	if err != nil {
		return err
	}

	err = t.w.Sync()
	if err != nil {
		return err
	}

	err = t.w.Close()
	if err != nil {
		return err
	}

	iter, err := t.tableCache.NewIterator(t.fileMeta)
	if err == nil {
		iter.UnRef()
		t.fileMeta.iMax = t.last
		t.fileMeta.iMin = t.first
		t.fileMeta.size = t.approximateSize()
	}

	return err

}

func (t *tWriter) approximateSize() int {
	return t.tw.ApproximateSize()
}

type tFileArrIteratorIndexer struct {
	*utils.BasicReleaser
	err        error
	tFiles     tFiles
	tableIter  iterator.Iterator
	tableCache *TableCache
	cmp        comparer.Comparer
	index      int
	len        int
}

func (indexer *tFileArrIteratorIndexer) cleanUp(args ...interface{}) {
	if indexer.tableIter != nil {
		indexer.tableIter.UnRef()
	}
	indexer.len = 0
	indexer.tFiles = nil
	indexer.index = 0
}

func newTFileArrIteratorIndexer(tFiles tFiles, cmp comparer.Comparer) iterator.IteratorIndexer {
	indexer := &tFileArrIteratorIndexer{
		tFiles:        tFiles,
		index:         0,
		len:           len(tFiles),
		cmp:           cmp,
		BasicReleaser: &utils.BasicReleaser{},
	}
	indexer.RegisterCleanUp(indexer.cleanUp)
	indexer.Ref()
	return indexer
}

func (indexer *tFileArrIteratorIndexer) Next() bool {

	if indexer.err != nil {
		return false
	}
	if indexer.Released() {
		indexer.err = errors.ErrReleased
		return false
	}

	if indexer.index < indexer.len {
		tFile := indexer.tFiles[indexer.index]
		iter, err := indexer.tableCache.NewIterator(tFile)
		if err != nil {
			indexer.err = err
			return false
		}
		if indexer.tableIter != nil {
			indexer.tableIter.UnRef()
		}
		indexer.tableIter = iter
		indexer.index++
		return true
	}

	return false

}

func (indexer *tFileArrIteratorIndexer) SeekFirst() bool {
	if indexer.err != nil {
		return false
	}
	if indexer.Released() {
		indexer.err = errors.ErrReleased
		return false
	}
	indexer.index = 0
	if indexer.tableIter != nil {
		indexer.tableIter.UnRef()
		indexer.tableIter = nil
	}

	return indexer.Next()
}

func (indexer *tFileArrIteratorIndexer) Seek(key []byte) bool {

	if indexer.err != nil {
		return false
	}
	if indexer.Released() {
		indexer.err = errors.ErrReleased
		return false
	}

	if indexer.tableIter != nil {
		indexer.tableIter.UnRef()
		indexer.index = 0
	}

	n := sort.Search(indexer.len, func(i int) bool {
		tFile := indexer.tFiles[i]
		return indexer.cmp.Compare(tFile.iMax, key) >= 0
	})

	if n == indexer.len {
		return false
	}

	indexer.index = n
	indexer.tableIter, indexer.err = indexer.tableCache.NewIterator(indexer.tFiles[n])
	return indexer.err == nil
}

func (indexer *tFileArrIteratorIndexer) Get() iterator.Iterator {
	return indexer.tableIter
}

func (indexer *tFileArrIteratorIndexer) Valid() error {
	return indexer.err
}
