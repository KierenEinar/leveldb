package table

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/KierenEinar/leveldb/comparer"
)

type kv struct {
	key   []byte
	value []byte
}

func Test_newBlockWriter(t *testing.T) {

	bw := newBlockWriter(16, comparer.DefaultComparer)
	fmt.Println(bw)

	inputs := []kv{
		{[]byte("k01"), []byte("hello01")},
		{[]byte("k02"), []byte("hello02")},
		{[]byte("k03"), bytes.Repeat([]byte{'x'}, 100)},
		{[]byte("04k"), bytes.Repeat([]byte{'y'}, 15)},
	}

	loopTimes := 0xff

	for i := 0; i < loopTimes; i++ {
		for _, input := range inputs {
			bw.append(input.key, input.value)
		}
	}

	bw.finish()

	blockContent := blockContent{
		data: bw.data.Bytes(),
	}

	block, err := newDataBlock(blockContent, comparer.DefaultComparer)
	if err != nil {
		t.Fatal(err)
	}

	iter := newBlockIter(block, comparer.DefaultComparer)
	idx := 0
	for iter.Next() {
		input := inputs[idx%len(inputs)]
		if !bytes.Equal(input.key, iter.Key()) {
			t.Fatalf("key not eq input, input=%s, key=%s", input.key, iter.Key())
		}

		if !bytes.Equal(input.value, iter.Value()) {
			t.Fatalf("value not eq input, input=%s, key=%s", input.value, iter.Value())
		}

		idx++
	}

	if idx != loopTimes*len(inputs) {
		t.Fatalf("looptimes not right, expect=%d, actual=%d", loopTimes*len(inputs), idx)
	}

	iter.RegisterCleanUp(func(args ...interface{}) {
		t.Logf("iter will clean up")
	})

	iter.UnRef()
	t.Logf("iter.released=%v", iter.Released())

}
