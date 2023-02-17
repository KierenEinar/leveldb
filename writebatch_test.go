package leveldb

import (
	"bytes"
	"strconv"
	"testing"
)

func TestWriteBatch_Put(t *testing.T) {
	wb := NewWriteBatch()
	for i := 0; i < 100; i++ {
		s := strconv.Itoa(i)
		err := wb.Put([]byte(s), []byte(s))
		if err != nil {
			t.Fatal(err)
		}
	}

	wb.SetSequence(sequence(0))

	idx := 0
	wb.foreach(func(kt keyType, key []byte, seq sequence, value []byte) error {
		if kt != keyTypeValue {
			t.Fatalf("kt should keytypeval")
		}
		if idx != int(seq) {
			t.Fatalf("sq should eq")
		}
		s := strconv.Itoa(idx)
		if !bytes.Equal(key, []byte(s)) {
			t.Fatal("key should eq ")
		}
		if !bytes.Equal(value, []byte(s)) {
			t.Fatal("value should eq ")
		}

		idx++
		return nil
	})

}
