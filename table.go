package leveldb

import (
	"bytes"
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
	iMax       InternalKey
	iMin       InternalKey
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
	smin, smax := s.iMin.UserKey(), s.iMax.UserKey()
	return !(bytes.Compare(smax, umin) < 0) && !(bytes.Compare(smin, umax) > 0)
}

func (s tFiles) getOverlapped(imin InternalKey, imax InternalKey, overlapped bool) (dst tFiles) {

	if !overlapped {

		var (
			umin, umax        = imin.UserKey(), imax.UserKey()
			smallest, largest int
			sizeS             = len(s)
		)

		// use binary search begin
		n := sort.Search(sizeS, func(i int) bool {
			return bytes.Compare(s[i].iMin.UserKey(), umin) >= 0
		})

		if n == 0 {
			smallest = 0
		} else if bytes.Compare(s[n-1].iMax.UserKey(), umin) >= 0 {
			smallest = n - 1
		} else {
			smallest = sizeS
		}

		n = sort.Search(sizeS, func(i int) bool {
			return bytes.Compare(s[i].iMax.UserKey(), umax) >= 0
		})

		if n == sizeS {
			largest = sizeS
		} else if bytes.Compare(s[n].iMin.UserKey(), umax) >= 0 {
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
		umin, umax = imin.UserKey(), imax.UserKey()
	)

	for i < len(s) {
		sFile := s[i]
		if sFile.isOverlapped(umin, umax) {
			if bytes.Compare(sFile.iMax.UserKey(), umax) > 0 {
				umax = sFile.iMax.UserKey()
				restart = true
			}
			if bytes.Compare(sFile.iMin.UserKey(), umin) < 0 {
				umin = sFile.iMin.UserKey()
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

// todo finish it
func (vSet *VersionSet) createNewTable(fd Fd, fileSize int) (*TableWriter, error) {
	return nil, nil
}

type tableOperation struct {
	opt *options.Options
}

func newTableOperation(opt *options.Options) *tableOperation {
	return &tableOperation{
		opt: opt,
	}
}

func (tableOperation *tableOperation) open(f tFile) (*TableReader, error) {
	reader, err := tableOperation.storage.Open(f.fd)
	if err != nil {
		return nil, err
	}
	return NewTableReader(reader, f.Size)
}

func (tableOperation *tableOperation) newIterator(f tFile) (iterator.Iterator, error) {
	tr, err := tableOperation.open(f)
	if err != nil {
		return nil, err
	}
	return tr.NewIterator()
}

func (tableOperation *tableOperation) create(tableCache *TableCache, fileMeta tFile) (*tWriter, error) {
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
		tableCache: tableCache,
		tw:         table.NewWriter(w, tableOperation.opt.FilterPolicy, tableOperation.opt.InternalComparer),
	}, nil
}

type tWriter struct {
	w           storage.SequentialWriter
	fileMeta    tFile
	tw          *table.Writer
	tableCache  *TableCache
	first, last InternalKey
}

func (t *tWriter) append(ikey InternalKey, value []byte) error {
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
	}

	t.fileMeta.iMax = t.last
	t.fileMeta.iMin = t.first
	t.fileMeta.size = t.size()
	return nil

}

func (t *tWriter) size() int {
	return t.tw.FileSize()
}

type tFileArrIteratorIndexer struct {
	*utils.BasicReleaser
	err       error
	tFiles    tFiles
	tableIter Iterator
	index     int
	len       int
}

func newTFileArrIteratorIndexer(tFiles tFiles) iteratorIndexer {
	indexer := &tFileArrIteratorIndexer{
		tFiles: tFiles,
		index:  0,
		len:    len(tFiles),
	}
	indexer.OnClose = func() {
		if indexer.tableIter != nil {
			indexer.tableIter.UnRef()
		}
		indexer.index = 0
		indexer.tFiles = indexer.tFiles[:0]
	}
	return indexer
}

func (indexer *tFileArrIteratorIndexer) Next() bool {

	if indexer.err != nil {
		return false
	}
	if indexer.released() {
		indexer.err = ErrReleased
		return false
	}

	if indexer.index <= indexer.len-1 {
		tFile := indexer.tFiles[indexer.index]
		tr, err := NewTableReader(nil, tFile.Size)
		if err != nil {
			indexer.err = err
			return false
		}
		if indexer.tableIter != nil {
			indexer.tableIter.UnRef()
		}
		indexer.tableIter, indexer.err = tr.NewIterator()
		if indexer.err != nil {
			return false
		}
		indexer.index++
		return true
	}

	return false

}

func (indexer *tFileArrIteratorIndexer) SeekFirst() bool {
	if indexer.err != nil {
		return false
	}
	if indexer.released() {
		indexer.err = ErrReleased
		return false
	}
	indexer.index = 0
	return indexer.Next()
}

func (indexer *tFileArrIteratorIndexer) Seek(ikey InternalKey) bool {

	if indexer.err != nil {
		return false
	}
	if indexer.released() {
		indexer.err = ErrReleased
		return false
	}

	n := sort.Search(indexer.len, func(i int) bool {
		r := indexer.tFiles[i].iMax.compare(ikey)
		return r >= 0
	})

	if n == indexer.len {
		return false
	}

	indexer.index = n
	return indexer.Next()
}

func (indexer *tFileArrIteratorIndexer) Get() Iterator {
	return indexer.tableIter
}

func (indexer *tFileArrIteratorIndexer) Valid() error {
	return indexer.err
}
