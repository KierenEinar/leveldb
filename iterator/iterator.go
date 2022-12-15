package iterator

import (
	"leveldb/collections"
	"leveldb/comparer"
	"leveldb/errors"
	"leveldb/utils"
)

type Direction int

const (
	DirSOI     Direction = 1
	DirForward Direction = 2
	DirEOI     Direction = 3
)

type CommonIterator interface {
	utils.Releaser
	Seek(key []byte) bool
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

type EmptyIterator struct{}

func (ei *EmptyIterator) Seek(key []byte) bool {
	return false
}

func (ei *EmptyIterator) SeekFirst() bool {
	return false
}

func (ei *EmptyIterator) Next() bool {
	return false
}

func (ei *EmptyIterator) Key() []byte {
	return nil
}

func (ei *EmptyIterator) Value() []byte {
	return nil
}

func (ei *EmptyIterator) Ref() int32 {
	return 0
}

func (ei *EmptyIterator) UnRef() int32 {
	return 0
}

func (ei *EmptyIterator) Valid() error {
	return nil
}

type IndexedIterator struct {
	*utils.BasicReleaser
	indexed iteratorIndexer
	data    Iterator
	err     error
	ikey    []byte
	value   []byte
}

func NewIndexedIterator(indexed iteratorIndexer) Iterator {
	ii := &IndexedIterator{
		indexed: indexed,
		BasicReleaser: &utils.BasicReleaser{
			OnClose: func() {
				indexed.UnRef()
			},
		},
	}
	return ii
}

func (iter *IndexedIterator) clearData() {
	if iter.data != nil {
		iter.data.UnRef()
		iter.data = nil
	}
}

func (iter *IndexedIterator) setData() {
	iter.data = iter.indexed.Get()
}

func (iter *IndexedIterator) Next() bool {

	if iter.err != nil {
		return false
	}

	if iter.Released() {
		iter.err = errors.ErrReleased
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

func (iter *IndexedIterator) SeekFirst() bool {

	if iter.err != nil {
		return false
	}

	if iter.Released() {
		iter.err = errors.ErrReleased
		return false
	}

	iter.clearData()
	if !iter.indexed.SeekFirst() {
		return false
	}

	iter.setData()
	return iter.Next()
}

func (iter *IndexedIterator) Seek(key []byte) bool {
	if iter.err != nil {
		return false
	}

	if iter.Released() {
		iter.err = errors.ErrReleased
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

func (iter *IndexedIterator) Key() []byte {
	if iter.data != nil {
		return iter.data.Key()
	}
	return nil
}

func (iter *IndexedIterator) Value() []byte {
	if iter.data != nil {
		return iter.data.Value()
	}
	return nil
}

func (iter *IndexedIterator) Valid() error {
	return iter.err
}

type MergeIterator struct {
	*utils.BasicReleaser
	err     error
	iters   []Iterator
	heap    *collections.Heap
	keys    [][]byte
	iterIdx int // current iter need to fill the heap
	dir     Direction
	ikey    []byte
	value   []byte
	cmp     comparer.BasicComparer
}

func NewMergeIterator(iters []Iterator) *MergeIterator {

	mi := &MergeIterator{
		iters: iters,
		keys:  make([][]byte, len(iters)),
	}

	mi.heap = collections.InitHeap(mi.minHeapLess)
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

	if mi.Released() {
		mi.err = errors.ErrReleased
		return false
	}

	mi.heap.Clear()
	mi.dir = DirSOI
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

	if mi.Released() {
		mi.err = errors.ErrReleased
		return false
	}

	if mi.dir == DirForward {
		return mi.next()
	} else if mi.dir == DirEOI {
		return false
	} else {
		mi.SeekFirst()
		return mi.next()
	}
}

func (mi *MergeIterator) Seek(ikey []byte) bool {

	mi.heap.Clear()
	mi.iterIdx = 0
	if mi.err != nil {
		return false
	}
	if mi.Released() {
		mi.err = errors.ErrReleased
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
	mi.dir = DirForward
	idx := mi.heap.Pop()
	if idx == nil {
		mi.dir = DirEOI
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

	r := mi.cmp.Compare(keyi, keyj)

	if r > 0 {
		return true
	}
	return false
}
