package leveldb

import (
	"leveldb/comparer"
	"leveldb/ikey"
)

type iComparer struct {
	uCmp comparer.BasicComparer
}

func (ic iComparer) Compare(a, b []byte) int {
	ia, ib := ikey.InternalKey(a), ikey.InternalKey(b)
	r := ic.uCmp.Compare(ia.UserKey(), ib.UserKey())
	if r != 0 {
		return r
	}
	m, n := ia.Seq(), ib.Seq()
	if m < n {
		return 1
	}
	return -1
}

func (ic iComparer) Name() []byte {
	return []byte("leveldb.InternalKeyComparator")
}

var IComparer = &iComparer{
	uCmp: comparer.DefaultComparer,
}
