package collections

type HeapLess func(data []interface{}, i, j int) bool

// Heap represent an sorted queue in logic, using an array to store the data.
/**
    0      1       2         3       4        5
/------/-------/--------/--------/--------/--------/

it look like a binary search tree
						1
					/-----/
		   2       	   				3
	   /------/    				/------/
   4           5				6         7
/------/   /------/			/------/   /------/

*/
type Heap struct {
	data      []interface{}
	tailIndex int
	Less      HeapLess
}

func InitHeap(hl HeapLess) *Heap {
	h := new(Heap)
	h.Less = hl
	h.data = make([]interface{}, 0)
	h.data = append(h.data, nil)
	return h
}

func (h *Heap) Clear() {
	h.data = h.data[:1]
	h.tailIndex = 0
}

func (h *Heap) Push(data interface{}) {
	h.data = append(h.data, data)
	h.tailIndex++
	// swim up
	h.swim()
}

func (h *Heap) Pop() interface{} {

	if h.tailIndex <= 0 { // head no need return
		return nil
	}

	data := h.data[1]

	if h.tailIndex == 1 {
		h.data = h.data[:1]
		h.tailIndex--
		return data
	}

	h.swap(1, h.tailIndex)
	h.data = h.data[:h.tailIndex]
	h.tailIndex--
	h.sink()

	return data
}

func (h *Heap) swap(i, j int) {
	h.data[j], h.data[i] = h.data[i], h.data[j]
}

// last up to first
func (h *Heap) swim() {

	for n, k := h.tailIndex, (h.tailIndex)>>1; k >= 1; {
		/**
		if this is a max heap,
					30
			27     			29
		 20   22          28
		*/
		if h.Less(h.data, n, k) {
			h.swap(n, k)
			n = k
			k = k >> 1
		} else {
			break
		}
	}

}

// first down to last
func (h *Heap) sink() {

	for n, k := 1, 1<<1; k <= h.tailIndex; {
		/**
		if this is a max heap, n must be gt than k and k + 1
		so we just compare the k and k + 1, find the max,
		and if n less than max, then swap(n, max), continue sink
		*/
		/**
			17                   18
		15      18           15      17
		*/
		if k+1 <= h.tailIndex {
			if h.Less(h.data, k+1, k) {
				k = k + 1
			}
		}

		if h.Less(h.data, k, n) {
			h.swap(n, k)
			n = k
			k = k << 1
		} else {
			break
		}
	}

}
