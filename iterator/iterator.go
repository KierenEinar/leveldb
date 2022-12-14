package sstable

import (
	"sort"
	"sync/atomic"
)

type CommonIterator interface {
	Releaser
	Seek(key InternalKey) bool
	SeekFirst() bool
	Next() bool
	Valid() error
}

type Iterator interface {
	CommonIterator
	Key() []byte
	Value() []byte
}

type iteratorIndexer interface {
	CommonIterator
	Get() Iterator
}

type emptyIterator struct{}

func (ei *emptyIterator) Seek(key InternalKey) bool {
	return false
}

func (ei *emptyIterator) SeekFirst() bool {
	return false
}

func (ei *emptyIterator) Next() bool {
	return false
}

func (ei *emptyIterator) Key() []byte {
	return nil
}

func (ei *emptyIterator) Value() []byte {
	return nil
}

func (ei *emptyIterator) Ref() int32 {
	return 0
}

func (ei *emptyIterator) UnRef() int32 {
	return 0
}

func (ei *emptyIterator) Valid() error {
	return nil
}

type Releaser interface {
	Ref() int32
	UnRef() int32
}

type BasicReleaser struct {
	release uint32
	ref     int32
	OnClose func()
	OnRef   func()
	OnUnRef func()
}

func (br *BasicReleaser) Ref() int32 {
	if br.OnRef != nil {
		br.OnRef()
	}
	return atomic.AddInt32(&br.ref, 1)
}

func (br *BasicReleaser) UnRef() int32 {
	newInt32 := atomic.AddInt32(&br.ref, -1)
	if newInt32 < 0 {
		panic("duplicated UnRef")
	}
	if br.OnUnRef != nil {
		br.OnUnRef()
	}
	if newInt32 == 0 {
		if br.OnClose != nil {
			atomic.StoreUint32(&br.release, 1)
			br.OnClose()
		}
	}
	return newInt32
}

func (br *BasicReleaser) released() bool {
	return atomic.LoadUint32(&br.release) == 1
}

type indexedIterator struct {
	*BasicReleaser
	indexed iteratorIndexer
	data    Iterator
	err     error
	ikey    InternalKey
	value   []byte
}

func newIndexedIterator(indexed iteratorIndexer) Iterator {
	ii := &indexedIterator{
		indexed: indexed,
		BasicReleaser: &BasicReleaser{
			OnClose: func() {
				indexed.UnRef()
			},
		},
	}
	return ii
}

func (iter *indexedIterator) clearData() {
	if iter.data != nil {
		iter.data.UnRef()
		iter.data = nil
	}
}

func (iter *indexedIterator) setData() {
	iter.data = iter.indexed.Get()
}

func (iter *indexedIterator) Next() bool {

	if iter.err != nil {
		return false
	}

	if iter.released() {
		iter.err = ErrReleased
		return false
	}

	if iter.data != nil && iter.data.Next() {
		return true
	}

	iter.clearData()

	if iter.indexed.Next() {
		iter.setData()
		return iter.Next()
	}

	return false
}

func (iter *indexedIterator) SeekFirst() bool {

	if iter.err != nil {
		return false
	}

	if iter.released() {
		iter.err = ErrReleased
		return false
	}

	iter.clearData()
	if !iter.indexed.SeekFirst() {
		return false
	}

	iter.setData()
	return iter.Next()
}

func (iter *indexedIterator) Seek(key InternalKey) bool {
	if iter.err != nil {
		return false
	}

	if iter.released() {
		iter.err = ErrReleased
		return false
	}

	iter.clearData()

	if !iter.indexed.Seek(key) {
		return false
	}

	iter.setData()

	if !iter.data.Seek(key) {
		iter.clearData()
		if !iter.Next() {
			return false
		}
	}

	return true
}

func (iter *indexedIterator) Key() []byte {
	if iter.data != nil {
		return iter.data.Key()
	}
	return nil
}

func (iter *indexedIterator) Value() []byte {
	if iter.data != nil {
		return iter.data.Value()
	}
	return nil
}

func (iter *indexedIterator) Valid() error {
	return iter.err
}

type MergeIterator struct {
	*BasicReleaser
	err     error
	iters   []Iterator
	heap    *Heap
	keys    [][]byte
	iterIdx int // current iter need to fill the heap
	dir     direction
	ikey    []byte
	value   []byte
}

func NewMergeIterator(iters []Iterator) *MergeIterator {

	mi := &MergeIterator{
		iters: iters,
		keys:  make([][]byte, len(iters)),
	}

	mi.heap = InitHeap(mi.minHeapLess)
	mi.OnClose = func() {
		mi.heap.Clear()
		for i := range iters {
			iters[i].UnRef()
		}
		mi.keys = mi.keys[:0]
		mi.ikey = nil
		mi.value = nil
	}
	return mi
}

func (mi *MergeIterator) SeekFirst() bool {

	if mi.err != nil {
		return false
	}

	if mi.released() {
		mi.err = ErrReleased
		return false
	}

	mi.heap.Clear()
	mi.dir = dirSOI
	mi.ikey = mi.ikey[:0]
	mi.value = mi.value[:0]
	for i := 0; i < len(mi.iters); i++ {
		iter := mi.iters[i]
		if !iter.SeekFirst() {
			return false
		}
		mi.heap.Push(i)
		mi.keys[i] = iter.Key()
	}

	return mi.next()

}

func (mi *MergeIterator) Next() bool {

	if mi.err != nil {
		return false
	}

	if mi.released() {
		mi.err = ErrReleased
		return false
	}

	if mi.dir == dirForward {
		return mi.next()
	} else if mi.dir == dirEOI {
		return false
	} else {
		mi.SeekFirst()
		return mi.next()
	}
}

func (mi *MergeIterator) Seek(ikey InternalKey) bool {

	mi.heap.Clear()
	mi.iterIdx = 0
	if mi.err != nil {
		return false
	}
	if mi.released() {
		mi.err = ErrReleased
		return false
	}
	for i := range mi.iters {
		if mi.iters[i].Seek(ikey) {
			mi.heap.Push(i)
		}
	}
	return mi.next()
}

func (mi *MergeIterator) Key() []byte {
	return mi.ikey
}

func (mi *MergeIterator) Value() []byte {
	return mi.value
}

func (mi *MergeIterator) next() bool {
	mi.dir = dirForward
	idx := mi.heap.Pop()
	if idx == nil {
		mi.dir = dirEOI
		return false
	}
	mi.iterIdx = idx.(int)
	iter := mi.iters[mi.iterIdx]
	mi.ikey = iter.Key()
	mi.value = iter.Value()
	if iter.Next() {
		mi.heap.Push(mi.iterIdx)
		mi.keys[mi.iterIdx] = iter.Key()
	} else {
		mi.keys[mi.iterIdx] = nil
	}
	return true
}

func (iter *MergeIterator) Valid() error {
	return iter.err
}

func (mi *MergeIterator) minHeapLess(data []interface{}, i, j int) bool {

	indexi := data[i].(int)
	indexj := data[j].(int)

	keyi := mi.keys[indexi]
	keyj := mi.keys[indexj]

	r := InternalKey(keyi).compare(keyj)

	if r > 0 {
		return true
	}
	return false
}

type tFileArrIteratorIndexer struct {
	*BasicReleaser
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
