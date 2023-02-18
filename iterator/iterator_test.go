package iterator

import (
	"bytes"
	"sort"
	"testing"

	"github.com/KierenEinar/leveldb/comparer"

	"github.com/KierenEinar/leveldb/utils"
)

type stringListIterator struct {
	sortedStr []string
	iterIdx   int
	*utils.BasicReleaser
}

func initStringListIterator(strs []string) Iterator {
	sort.Strings(strs)
	iter := &stringListIterator{
		sortedStr:     strs,
		BasicReleaser: &utils.BasicReleaser{},
		iterIdx:       -1,
	}
	iter.Ref()
	return iter
}

func (iter *stringListIterator) Seek(key []byte) bool {
	idx := sort.Search(len(iter.sortedStr), func(i int) bool {
		return bytes.Compare([]byte(iter.sortedStr[i]), key) > 0
	})
	iter.iterIdx = idx

	if idx == 0 {
		return true
	}

	if bytes.Compare([]byte(iter.sortedStr[idx-1]), key) == 0 {
		iter.iterIdx = idx - 1
		return true
	}

	if idx < len(iter.sortedStr) {
		return true
	}

	return false
}

func (iter *stringListIterator) SeekFirst() bool {
	if len(iter.sortedStr) == 0 {
		return false
	}

	iter.iterIdx = 0
	return true

}

func (iter *stringListIterator) Next() bool {
	if len(iter.sortedStr) == 0 {
		return false
	}
	if iter.iterIdx >= len(iter.sortedStr)-1 {
		return false
	}
	iter.iterIdx++
	return true
}

func (iter *stringListIterator) Valid() error {
	return nil
}

func (iter *stringListIterator) Key() []byte {
	if iter.iterIdx >= 0 && iter.iterIdx < len(iter.sortedStr) {
		return []byte(iter.sortedStr[iter.iterIdx])
	}
	return nil
}

func (iter *stringListIterator) Value() []byte {
	if iter.iterIdx >= 0 && iter.iterIdx < len(iter.sortedStr) {
		return []byte(iter.sortedStr[iter.iterIdx])
	}
	return nil
}

func TestMergerIterator(t *testing.T) {

	iters := make([]Iterator, 0, 10)
	for i := 0; i < 10; i++ {
		strs := make([]string, 0, 10)
		for j := 0; j < 1; j++ {
			strs = append(strs, utils.RandHexByLen(4))
		}
		t.Logf("strs=%v", strs)
		iters = append(iters, initStringListIterator(strs))
	}

	mIter := NewMergeIterator(iters, comparer.DefaultComparer, true)
	idx := 0
	for mIter.Next() {
		t.Logf("k=%s, v=%s", string(mIter.Key()), string(mIter.Value()))
		idx++
	}
	t.Logf("idx=%d", idx)
}
