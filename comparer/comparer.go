package comparer

import "bytes"

type BasicComparer interface {
	Compare(a, b []byte) int
	Name() []byte
}

type Comparer interface {
	BasicComparer

	// Successor return the successor that Gte ikey
	// e.g. abc => b
	// e.g. 0xff 0xff abc => 0xff 0xff b
	Successor(a []byte) []byte

	// Separator return x that is gte a and lt b
	Separator(a, b []byte) (dest []byte)

	// Prefix return
	Prefix(a []byte, b []byte) (dest []byte)
}

type BytesComparer struct{}

func (bc BytesComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (bc BytesComparer) Name() []byte {
	return []byte("bytes.comparer")
}

var DefaultComparer = &BytesComparer{}

// Successor return the successor that Gte ikey
// e.g. abc => b
// e.g. 0xff 0xff abc => 0xff 0xff b
func (b BytesComparer) Successor(a []byte) (dest []byte) {
	for i := range a {
		c := a[i]
		if c < 0xff {
			dest = append(dest, a[:i+1]...)
			dest[len(dest)-1]++
			return
		}
	}
	dest = append(dest, a...)
	return
}

// Separator return x that is gte a and lt b
func (BytesComparer) Separator(a, b []byte) (dest []byte) {

	if bytes.Compare(a, b) > 0 {
		panic("BytesComparer a must lte b")
	}

	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}

	for ; i < n && a[i] == b[i]; i++ {

	}

	if i == n {

	} else if c := a[i]; c < 0xff && c+1 < b[i] {
		dest = append(dest, a[:i+1]...)
		dest[len(dest)-1]++
		return
	}

	dest = append(dest, a...)
	return
}

func (BytesComparer) Prefix(a, b []byte) (dest []byte) {

	size := len(a)
	if len(b) < size {
		size = len(b)
	}

	dest = make([]byte, size)

	var sharePrefixIndex = 0

	for sharePrefixIndex = 0; sharePrefixIndex < size; sharePrefixIndex++ {
		c1 := a[sharePrefixIndex]
		c2 := b[sharePrefixIndex]
		if c1 == c2 {
			dest[sharePrefixIndex] = c1
		} else {
			break
		}
	}

	return dest[:sharePrefixIndex]
}
