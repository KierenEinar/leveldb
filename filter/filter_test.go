package filter

import (
	"bytes"
	"testing"

	"github.com/KierenEinar/leveldb/utils"
)

func TestBloomFilter_MayContains(t *testing.T) {
	f := NewBloomFilter(10)
	g := f.NewGenerator()
	b := bytes.NewBuffer(nil)
	utils.ForeachLetter(1, func(i int, c rune) {
		g.AddKey([]byte{byte(c)})
	})
	g.Generate(b)

	utils.ForeachLetter(1, func(i int, c rune) {
		if !f.MayContains([]byte{byte(c)}, b.Bytes()) {
			t.Fatalf("MayContains c=%s, expected=true, actual=false", string(c))
		}
	})
}
