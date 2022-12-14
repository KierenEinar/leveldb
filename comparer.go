package sstable

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

type iComparer struct {
	uCmp BasicComparer
}

func (ic iComparer) Compare(a, b []byte) int {
	ia, ib := InternalKey(a), InternalKey(b)
	r := ic.uCmp.Compare(ia.ukey(), ib.ukey())
	if r != 0 {
		return r
	}
	m, n := ia.seq(), ib.seq()
	if m < n {
		return 1
	}
	return -1
}

func (ic iComparer) Name() []byte {
	return []byte("leveldb.InternalKeyComparator")
}

var DefaultComparer = &BytesComparer{}
var IComparer = &iComparer{
	uCmp: DefaultComparer,
}
