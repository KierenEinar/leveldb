package leveldb

import (
	"encoding/binary"
	"fmt"

	"github.com/KierenEinar/leveldb/errors"
	"github.com/KierenEinar/leveldb/utils"
)

type sequence uint64

type keyType uint8

const (
	keyTypeValue keyType = 0
	keyTypeDel   keyType = 1
	keyTypeSeek          = keyTypeValue
)

const kMaxSequenceNum = (uint64(1) << 56) - 1
const kMaxNum = sequence(kMaxSequenceNum | uint64(keyTypeValue))

type internalKey []byte

func (ik internalKey) assert() {
	_, _, _, err := parseInternalKey(ik)
	utils.Assert(err == nil, fmt.Sprintf("internal key parse failed, err=%v", err),
		fmt.Sprintf("ik=%s", ik))
}

func (ik internalKey) userKey() []byte {
	ik.assert()
	dst := make([]byte, len(ik)-8)
	copy(dst, ik[:len(ik)-8])
	return dst
}

func (ik internalKey) seq() sequence {
	ik.assert()
	x := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	return sequence(x >> 8)
}

func (ik internalKey) keyType() keyType {
	ik.assert()
	x := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	kt := uint8(x & 1 << 7)
	return keyType(kt)
}

func parseInternalKey(ikey internalKey) (ukey []byte, kt keyType, seq uint64, err error) {
	if len(ikey) < 8 {
		err = errors.NewErrCorruption("invalid internal ikey len")
		return
	}

	ukey = ikey[:len(ikey)-8]
	num := binary.LittleEndian.Uint64(ikey[len(ikey)-8:])
	seq, kty := num>>8, num&0xff
	kt = keyType(kty)
	if kt > keyTypeDel {
		err = errors.NewErrCorruption(fmt.Sprintf("invalid internal ikey keytype, kt=%d", kt))
		return
	}
	return
}

func buildInternalKey(dst, uKey []byte, kt keyType, sequence sequence) internalKey {
	dst = utils.EnsureBuffer(dst, len(uKey)+8)
	n := copy(dst, uKey)
	binary.LittleEndian.PutUint64(dst[n:], (uint64(sequence)<<8)|uint64(kt))
	return dst
}

func debugInternalKey(ikey internalKey) string {
	return fmt.Sprintf("user_key=%s, seq=%d, key_type=%d", ikey.userKey(), ikey.seq(), ikey.keyType())
}
