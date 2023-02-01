package table

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/KierenEinar/leveldb/comparer"
)

func Test_newBlockWriter(t *testing.T) {

	bw := newBlockWriter(16, comparer.DefaultComparer)
	fmt.Println(bw)

	for i := 0; i < 100; i++ {
		bw.append([]byte("k01"), []byte("hello01"))
		bw.append([]byte("k02"), []byte("hello02"))
		bw.append([]byte("k03"), bytes.Repeat([]byte{'x'}, 100))
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
	for iter.Next() {
		t.Logf("key=%s", iter.Key())
		t.Logf("value=%s", iter.Value())
	}

}
