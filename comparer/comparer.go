package comparer

import "bytes"

type BasicComparer interface {
	Compare(a, b []byte) int
	Name() []byte
}

type BytesComparer struct{}

func (bc BytesComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (bc BytesComparer) Name() []byte {
	return []byte("bytes.comparer")
}
