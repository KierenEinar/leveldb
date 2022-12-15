package ikey

import (
	"encoding/binary"
	"leveldb/errors"
	"leveldb/utils"
)

type Sequence uint64

type KeyType uint8

const (
	KeyTypeValue KeyType = 0
	KeyTypeDel   KeyType = 1
)

type InternalKey []byte

func (ik InternalKey) assert() {
	if len(ik) < 8 {
		panic("invalid internal key")
	}
}

func (ik InternalKey) UserKey() []byte {
	ik.assert()
	dst := make([]byte, len(ik)-8)
	copy(dst, ik[:len(ik)-8])
	return dst
}

func (ik InternalKey) Seq() Sequence {
	ik.assert()
	x := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	return Sequence(x >> 8)
}

func (ik InternalKey) KeyType() KeyType {
	ik.assert()
	x := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	kt := uint8(x & 1 << 7)
	return KeyType(kt)
}

func parseInternalKey(ikey InternalKey) (ukey []byte, kt KeyType, seq uint64, err error) {
	if len(ikey) < 8 {
		err = errors.NewErrCorruption("invalid internal ikey len")
		return
	}

	num := binary.LittleEndian.Uint64(ikey[len(ikey)-8:])
	seq, kty := num>>8, num&0xff
	kt = KeyType(kty)
	if kt > KeyTypeDel {
		err = errors.NewErrCorruption("invalid internal ikey keytype")
		return
	}
	return
}

func BuildInternalKey(dst, uKey []byte, kt KeyType, sequence Sequence) InternalKey {
	dst = utils.EnsureBuffer(dst, len(dst)+8)
	n := copy(dst, uKey)
	binary.LittleEndian.PutUint64(dst[n:], (uint64(sequence)<<8)|uint64(kt))
	return dst
}
