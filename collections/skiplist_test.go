package collections

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/KierenEinar/leveldb/utils"

	"github.com/KierenEinar/leveldb/comparer"
)

func TestSkipList_Put(t *testing.T) {
	skl := NewSkipList(time.Now().UnixNano(), 0, comparer.DefaultComparer)
	defer skl.UnRef()
	// test add
	for i := 0; i < 9; i++ {
		is := strconv.Itoa(i)
		err := skl.Put([]byte(is), []byte(is))
		if err != nil {
			t.Fatal(err)
		}
	}

	// test replace
	err := skl.Put([]byte("1"), []byte("2"))
	if err != nil {
		t.Fatal(err)
	}

}

func TestSkipList_Get(t *testing.T) {
	skl := NewSkipList(time.Now().UnixNano(), 0, comparer.DefaultComparer)
	defer skl.UnRef()
	// test add
	for i := 0; i < 9; i++ {
		is := strconv.Itoa(i)
		err := skl.Put([]byte(is), []byte(is))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 9; i++ {
		is := strconv.Itoa(i)
		val, err := skl.Get([]byte(is))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(val, []byte(is)) != 0 {
			t.Fatal("get failed")
		}
	}

}

func TestSkipList_FindGreaterOrEqual(t *testing.T) {
	skl := NewSkipList(time.Now().UnixNano(), 0, comparer.DefaultComparer)
	defer skl.UnRef()
	// test add
	for i := 0; i < 9; i++ {
		is := strconv.Itoa(i)
		err := skl.Put([]byte(is), []byte(is))
		if err != nil {
			t.Fatal(err)
		}
	}

	n, e, err := skl.FindGreaterOrEqual([]byte("11"))
	if err != nil {
		t.Fatal(err)
	}

	if e {
		t.Fatalf("should not exists")
	}

	k := skl.Key(n)
	v := skl.Value(n)
	if bytes.Compare(k, []byte("2")) != 0 {
		t.Fatalf("key should 2")
	}

	if bytes.Compare(v, []byte("2")) != 0 {
		t.Fatalf("value should 2")
	}
}

func BenchmarkPut1000(b *testing.B)   { benchmarkPut(1000, b) }
func BenchmarkPut10000(b *testing.B)  { benchmarkPut(10000, b) }
func BenchmarkPut100000(b *testing.B) { benchmarkPut(100000, b) }

func BenchmarkGet1000(b *testing.B)  { benchmarkGet(1000, b) }
func BenchmarkGet10000(b *testing.B) { benchmarkGet(10000, b) }

func TestSkipListIter_Next(t *testing.T) {

	skl := NewSkipList(time.Now().UnixNano(), 0, comparer.DefaultComparer)
	defer skl.UnRef()
	n := 100
	inputs := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := utils.RandHexByLen(6)
		v := k
		err := skl.Put([]byte(k), []byte(v))
		if err != nil {
			t.Fatal(err)
		}
		inputs[i] = []byte(k)
	}

	iter := skl.NewIterator()
	defer iter.UnRef()
	for iter.Next() {
		t.Logf("k=%s, v=%s", string(iter.Key()), string(iter.Value()))
	}

}

func TestSkipListIter_Seek(t *testing.T) {

	skl := NewSkipList(time.Now().UnixNano(), 0, comparer.DefaultComparer)
	defer skl.UnRef()

	for i := 0; i < 100; i++ {
		s := strconv.Itoa(i)
		err := skl.Put([]byte(s), []byte(s))
		if err != nil {
			t.Fatal(err)
		}
	}

	iter := skl.NewIterator()
	defer iter.UnRef()

	if !iter.Seek([]byte("551")) {
		t.Fatalf("should seek")
	}
	t.Logf("k=%s, v=%s", string(iter.Key()), string(iter.Value()))
	if !bytes.Equal(iter.Key(), []byte("56")) {
		t.Fatalf("should eq")
	}

	// only for debug log
	for iter.Next() {
		t.Logf("k=%s, v=%s", string(iter.Key()), string(iter.Value()))
	}

}

func benchmarkPut(n int, b *testing.B) {

	skl := NewSkipList(time.Now().UnixNano(), 0, comparer.DefaultComparer)
	defer skl.UnRef()
	for i := 0; i < n; i++ {
		k := utils.RandHexByLen(6)
		v := k
		err := skl.Put([]byte(k), []byte(v))
		if err != nil {
			b.Error(err)
		}
	}
}

func benchmarkGet(n int, b *testing.B) {

	skl := NewSkipList(time.Now().UnixNano(), 0, comparer.DefaultComparer)
	defer skl.UnRef()

	inputs := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := utils.RandHexByLen(6)
		v := k
		err := skl.Put([]byte(k), []byte(v))
		if err != nil {
			b.Error(err)
		}
		inputs[i] = []byte(k)
	}

	for i := 0; i < n; i++ {
		k := inputs[i]
		v, err := skl.Get(k)
		if err != nil {
			b.Error(err)
		}
		if bytes.Compare(k, v) != 0 {
			b.Fatalf("should eq")
		}
	}

}
