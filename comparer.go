package leveldb

import (
	"leveldb/comparer"
)

type iComparer struct {
	uCmp comparer.Comparer
}

func (ic *iComparer) Compare(a, b []byte) int {
	ia, ib := InternalKey(a), InternalKey(b)
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

func (ic *iComparer) Name() []byte {
	return []byte("leveldb.InternalKeyComparator")
}

func (ic *iComparer) Successor(a []byte) (dest []byte) {
	au := InternalKey(a)
	destU := ic.uCmp.Successor(au.UserKey())
	dest = append(destU, kMaxNumBytes...)
	return
}

func (ic *iComparer) Separator(a []byte, b []byte) (dest []byte) {
	ia := InternalKey(a)
	ib := InternalKey(b)
	destU := ic.uCmp.Separator(ia.UserKey(), ib.UserKey())
	dest = append(destU, kMaxNumBytes...)
	return
}

func (ic *iComparer) Prefix(a []byte, b []byte) (dest []byte) {
	ia := InternalKey(a)
	ib := InternalKey(b)
	destU := ic.uCmp.Prefix(ia.UserKey(), ib.UserKey())
	dest = append(destU, kMaxNumBytes...)
	return
}

var IComparer = &iComparer{
	uCmp: comparer.DefaultComparer,
}
