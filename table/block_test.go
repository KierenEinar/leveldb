package table

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/KierenEinar/leveldb/utils"

	"github.com/KierenEinar/leveldb/comparer"
)

var (
	rnd = rand.New(rand.NewSource(time.Now().Unix()))
)

func Test_dataBlock(t *testing.T) {

	bw := newBlockWriter(16, comparer.DefaultComparer)
	fmt.Println(bw)

	inputs := randInputs(10, 20, false)

	loopTimes := 0xff

	for i := 0; i < loopTimes; i++ {
		for _, input := range inputs {
			bw.append(input.k, input.v)
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
		if !bytes.Equal(input.k, iter.Key()) {
			t.Fatalf("key not eq input, input=%s, key=%s", input.k, iter.Key())
		}

		if !bytes.Equal(input.v, iter.Value()) {
			t.Fatalf("value not eq input, input=%s, key=%s", input.v, iter.Value())
		}
		t.Logf("key=%s, value=%s", iter.Key(), iter.Value())
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

func randInputs(minLen, maxLen int, enableSort bool) []kv {

	size := rnd.Int()%maxLen + 1
	if size < minLen {
		size = minLen
	}
	inputs := make([]kv, size)
	for i := 0; i < len(inputs); i++ {
		key := utils.RandString(16)
		value := utils.RandString(100)
		inputs[i].k = []byte(key)
		inputs[i].v = []byte(value)
	}

	if enableSort {
		sort.Slice(inputs, func(i, j int) bool {
			return bytes.Compare(inputs[i].k, inputs[j].k) < 0
		})
	}

	return inputs

}
