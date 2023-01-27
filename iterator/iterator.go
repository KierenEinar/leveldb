package iterator

import (
	"github.com/KierenEinar/leveldb/collections"
	"github.com/KierenEinar/leveldb/comparer"
	"github.com/KierenEinar/leveldb/errors"
	"github.com/KierenEinar/leveldb/utils"
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

type IteratorIndexer interface {
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
	indexed IteratorIndexer
	data    Iterator
	err     error
}

func NewIndexedIterator(indexed IteratorIndexer) Iterator {

	ii := &IndexedIterator{
		indexed:       indexed,
		BasicReleaser: &utils.BasicReleaser{},
	}

	ii.Ref()

	ii.RegisterCleanUp(func(args ...interface{}) {
		ii.clearData()
		ii.indexed.UnRef()
	})

	return ii
}

func (iter *IndexedIterator) clearData() {
	if iter.data != nil {
		iter.data.UnRef()
		iter.data = nil
	}
}

func (iter *IndexedIterator) setData() bool {
	iter.clearData()
	dataIter := iter.indexed.Get()
	if dataIter != nil {
		dataIter.Ref()
		iter.data = dataIter
		return true
	}
	return false
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

	if iter.indexed.Next() && iter.setData() {
		return iter.data.Next()
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

	if iter.indexed.SeekFirst() && iter.setData() {
		return iter.data.SeekFirst()
	}

	return false
}

func (iter *IndexedIterator) Seek(key []byte) bool {
	if iter.err != nil {
		return false
	}

	if iter.Released() {
		iter.err = errors.ErrReleased
		return false
	}

	if !iter.indexed.Seek(key) {
		return false
	}

	iter.setData()

	if !iter.data.Seek(key) {
		iter.clearData()
		return iter.Next()
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
	cmp     comparer.Comparer

	key   []byte
	value []byte
}

func NewMergeIterator(iters []Iterator, cmp comparer.Comparer) *MergeIterator {

	mi := &MergeIterator{
		iters: iters,
		keys:  make([][]byte, len(iters)),
		cmp:   cmp,
	}

	mi.heap = collections.InitHeap(mi.minHeapLess)
	mi.OnClose = func() {
		mi.heap.Clear()
		for i := range iters {
			iters[i].UnRef()
		}
		mi.keys = mi.keys[:0]
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
	for i := 0; i < len(mi.iters); i++ {
		iter := mi.iters[i]
		if !iter.SeekFirst() {
			mi.err = iter.Valid()
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

	if mi.err != nil {
		return false
	}
	if mi.Released() {
		mi.err = errors.ErrReleased
		return false
	}
	mi.heap.Clear()
	mi.iterIdx = 0
	for i := range mi.iters {
		if mi.iters[i].Seek(ikey) {
			mi.heap.Push(i)
		}
	}
	return mi.next()
}

func (mi *MergeIterator) Key() []byte {
	return mi.key
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
	mi.key = iter.Key()
	mi.value = iter.Value()

	if iter.Next() {
		mi.heap.Push(mi.iterIdx)
		mi.keys[mi.iterIdx] = iter.Key()
	} else {
		mi.keys[mi.iterIdx] = nil
	}
	return true
}

func (mi *MergeIterator) Valid() error {
	return mi.err
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
