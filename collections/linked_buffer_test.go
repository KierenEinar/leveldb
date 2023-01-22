package collections

import (
	"bytes"
	"io"
	"testing"
)

func TestNewLinkedBlockBuffer(t *testing.T) {
	linkedBlockBuffer := NewLinkBlockBuffer(1025)
	t.Logf("%v", linkedBlockBuffer)
}

func TestLinkedBlockBuffer_Grow(t *testing.T) {
	linkedBlockBuffer := NewLinkBlockBuffer(1025)
	t.Logf("%v", linkedBlockBuffer)

	n, ok := linkedBlockBuffer.Grow(2000)
	if !ok {
		t.Fatalf("not ok")
	}

	if n != blockSize*2 {
		t.Fatalf("n not 2048")
	}

	t.Logf("n=%d, ok=%v", n, ok)
	t.Logf("%v", linkedBlockBuffer)
}

func TestLinkedBlockBuffer_Write(t *testing.T) {
	linkedBlockBuffer := NewLinkBlockBuffer(1025)
	t.Logf("%v", linkedBlockBuffer)

	content := []byte{1}
	p := bytes.Repeat(content, 3000)

	n, err := linkedBlockBuffer.Write(p)
	if err != nil {
		t.Fatalf("Write err [%v]", err)
	}

	if n != 3000 {
		t.Fatalf("Write count less than  3000")
	}

	if linkedBlockBuffer.writePos != 3000 {
		t.Fatalf("Write writePos failed")
	}

}

func TestLinkedBlockBuffer_Read(t *testing.T) {
	linkedBlockBuffer := NewLinkBlockBuffer(1025)
	t.Logf("%v", linkedBlockBuffer)

	content := []byte{1}
	p := bytes.Repeat(content, 3000)

	_, _ = linkedBlockBuffer.Write(p)

	readCount := 1025
	for {
		p1 := make([]byte, readCount)
		n, err := linkedBlockBuffer.Read(p1)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Write err [%v]", err)
		}

		t.Logf("readCount=%d", n)
		t.Logf("readPos=%d", linkedBlockBuffer.readPos)
		t.Logf("%v", p1)
	}
}

func TestLinkedBlockBuffer_Update(t *testing.T) {
	linkedBlockBuffer := NewLinkBlockBuffer(1025)
	t.Logf("%v", linkedBlockBuffer)

	content := []byte{1}
	p := bytes.Repeat(content, 3000)

	_, _ = linkedBlockBuffer.Write(p)

	p1 := []byte{2}
	p1 = bytes.Repeat(p1, 10)

	n := linkedBlockBuffer.Update(100, p1)
	t.Logf("n=%d", n)

}
