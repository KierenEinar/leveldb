package iterator

import (
	"fmt"
	"math/rand"
	"testing"
)

var (
	MaxIntHeapLess = func(data []interface{}, i, j int) bool {
		intI := data[i].(int)
		intJ := data[j].(int)
		return intI < intJ
	}
)

func TestHeap_Push(t *testing.T) {

	h := InitHeap(MaxIntHeapLess)

	for i := 0; i < 20; i++ {
		h.Push(int(rand.Int31n(100)))
	}
	for h.tailIndex >= 1 {
		fmt.Println(h.Pop())
	}
}
