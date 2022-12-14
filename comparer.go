package leveldb

import (
	"bytes"
	"leveldb/comparer"
)

type iComparer struct {
	uCmp comparer.BasicComparer
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
