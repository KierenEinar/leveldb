package leveldb

import (
	"github.com/KierenEinar/leveldb/comparer"
)

type iComparer struct {
	uCmp comparer.Comparer
}

func (ic *iComparer) Compare(a, b []byte) int {
	ia, ib := internalKey(a), internalKey(b)
	r := ic.uCmp.Compare(ia.userKey(), ib.userKey())
	if r != 0 {
		return r
	}
	m, n := ia.seq(), ib.seq()
	if m < n {
		return -1
	}
	return 1
}

func (ic *iComparer) Name() []byte {
	return []byte("leveldb.InternalKeyComparator")
}

func (ic *iComparer) Successor(a []byte) (dest []byte) {
	au := internalKey(a)
	destU := ic.uCmp.Successor(au.userKey())
	buildInternalKey(dest, destU, keyTypeValue, sequence(kMaxSequenceNum))
	return
}

func (ic *iComparer) Separator(a []byte, b []byte) (dest []byte) {
	ia := internalKey(a)
	ib := internalKey(b)
	destU := ic.uCmp.Separator(ia.userKey(), ib.userKey())
	buildInternalKey(dest, destU, keyTypeValue, sequence(kMaxSequenceNum))
	return
}

func (ic *iComparer) Prefix(a []byte, b []byte) (dest []byte) {
	ia := internalKey(a)
	ib := internalKey(b)
	dest = ic.uCmp.Prefix(ia.userKey(), ib.userKey())
	return
}

var IComparer = &iComparer{
	uCmp: comparer.DefaultComparer,
}
