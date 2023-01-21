package collections

import (
	"errors"
	"io"
)

type LinkedBlockBuffer struct {
	head     *bufferBlock
	tail     *bufferBlock
	writeCur *bufferBlock
	readCur  *bufferBlock
	cap      int
	writePos int
	readPos  int
}

const _1g = 1 << 30
const blockSize = 1 << 10

// NewLinkedBlockBuffer create a LinkedBuffer, which means contains multi buffer block
// each buffer block has 1024 bytes
// noted: the max cap is limit 1G, other wise will panic
func NewLinkedBlockBuffer(cap int) *LinkedBlockBuffer {
	// less cap is 1k
	if cap < blockSize {
		cap = blockSize
	}

	lb := &LinkedBlockBuffer{}
	lb.Grow(cap)
	return lb
}

func (lb *LinkedBlockBuffer) Grow(grow int) (n int, ok bool) {

	blocks := incrBlocks(grow)
	newCap := lb.cap + blocks*blockSize
	if newCap > _1g {
		return
	}

	var (
		tail *bufferBlock
		head *bufferBlock
	)

	for i := 0; i < blocks; i++ {
		if tail == nil {
			tail = &bufferBlock{}
			head = tail
		} else {
			tail.next = &bufferBlock{}
			tail = tail.next
		}
	}

	if lb.head == nil {
		lb.head = head
	} else {
		lb.tail.next = head
	}

	lb.tail = tail
	lb.cap = newCap

	n = blocks * blockSize
	ok = true
	return
}

func (lb *LinkedBlockBuffer) Write(p []byte) (int, error) {

	if lb.cap-lb.writePos < len(p) {
		grow := len(p) - (lb.cap - lb.writePos)
		_, ok := lb.Grow(grow)
		if !ok {
			return 0, errors.New("LinkedBlockBuffer Grow failed")
		}
	}

	writeRemain := len(p)
	writePos := 0
	m := 0
	for writeRemain > 0 {
		posInBlock := lb.writePos & (blockSize - 1)
		if posInBlock == 0 {
			if lb.writeCur == nil {
				lb.writeCur = lb.head
			} else {
				lb.writeCur = lb.writeCur.next
			}
		}
		n := copy(lb.writeCur.rep[posInBlock:], p[writePos:])
		m += n
		writeRemain -= n
		writePos += n
		lb.writePos += n
	}
	return m, nil
}

func (lb *LinkedBlockBuffer) Read(p []byte) (n int, err error) {

	pLen := len(p)

	if lb.readPos == lb.writePos {
		if pLen == 0 {
			return 0, nil
		}
		err = io.EOF
		return
	}

	readN := 0
	for pLen > 0 {
		if lb.readPos == lb.writePos {
			return
		}
		posInBlock := lb.readPos & (blockSize - 1)
		if posInBlock == 0 {
			if lb.readCur == nil {
				lb.readCur = lb.head
			} else {
				lb.readCur = lb.readCur.next
			}
		}

		end := blockSize
		if lb.writePos-lb.readPos < blockSize {
			end = lb.writePos & (blockSize - 1)
		}
		m := copy(p[readN:], lb.readCur.rep[posInBlock:end])
		pLen -= m
		readN += m
		lb.readPos += m
		n += m
	}
	return readN, nil
}

func (lb *LinkedBlockBuffer) ReadByte() (b byte, err error) {
	p := make([]byte, 1)
	_, err = lb.Read(p)
	if err != nil {
		return
	}
	b = p[0]
	return
}

func (lb *LinkedBlockBuffer) Update(s int, p []byte) (n int) {

	pLen := len(p)

	if s >= lb.writePos {
		n = 0
		return
	}

	if lb.writePos-s < pLen {
		pLen = lb.writePos - s
	}

	blockIndex := s / blockSize
	posInBlock := s & (blockIndex - 1)

	block := lb.head

	for i := 0; i < blockIndex; i++ {
		block = block.next
	}

	for pLen > 0 {
		m := copy(block.rep[posInBlock:], p[n:])
		n += m
		pLen -= m
	}

	return

}

func (lb *LinkedBlockBuffer) Reset() {
	lb.readCur = nil
	lb.readPos = 0
	lb.writeCur = nil
	lb.writePos = 0
}

func (lb *LinkedBlockBuffer) Cap() int {
	return lb.cap
}

func (lb *LinkedBlockBuffer) Len() int {
	return lb.writePos - lb.readPos
}

type bufferBlock struct {
	rep  [blockSize]byte
	next *bufferBlock
}

func incrBlocks(grow int) int {
	blocks := grow / blockSize
	if grow&(blockSize-1) != 0 {
		blocks++
	}
	return blocks
}
