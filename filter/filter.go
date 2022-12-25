package filter

import (
	"bytes"
	"hash/fnv"
)

var (
	hash = fnv.New32()
)

type IFilter interface {
	MayContains(filter, key []byte) bool
	NewGenerator() IFilterGenerator
	Name() string
}

type IFilterGenerator interface {
	AddKey(key []byte)
	Generate(b *bytes.Buffer)
}

type BloomFilterGenerator struct {
	k             uint8
	numBitsPerKey uint8
	keysHash      []uint32
}

type BloomFilter uint8

func NewBloomFilter(numBitsPerKey uint8) BloomFilter {
	return BloomFilter(numBitsPerKey)
}

func (bf BloomFilter) NewGenerator() IFilterGenerator {
	return &BloomFilterGenerator{
		numBitsPerKey: uint8(bf),          // per keys using number bits represent in filter bits
		k:             uint8(bf) * 7 / 10, // number hash function
	}
}

func (bf BloomFilter) MayContains(filter, key []byte) bool {

	bloomData := filter[:len(filter)-1]

	k := filter[len(filter)-1]
	h := hash32(key)
	delta := h>>17 | h<<15
	for i := uint8(0); i < k; i++ {
		bitPos := uint8(h % uint32(8))
		bytePos := h / uint32(8)
		bloomByte := bloomData[bytePos]
		if bitPos<<(7-bitPos)&bloomByte != bitPos<<(7-bitPos) {
			return false
		}
		h += delta
	}
	return true
}

func (bf *BloomFilter) Name() string {
	return "bloomfilter"
}

func (bf *BloomFilterGenerator) AddKey(key []byte) {
	bf.keysHash = append(bf.keysHash, hash32(key))
}

func (bf *BloomFilterGenerator) Generate(b *bytes.Buffer) {
	n := len(bf.keysHash)

	numBits := n * int(bf.numBitsPerKey)

	numBytes := numBits / 8
	if numBits%8 != 0 {
		numBytes += 1
	}
	numBits = numBytes * 8

	data := make([]byte, numBytes)

	for _, h := range bf.keysHash {
		delta := h>>17 | h<<15
		for i := uint8(0); i < bf.k; i++ {
			bitPos := h % uint32(8)
			bytePos := h / uint32(8)
			b := data[bytePos]
			b = b | 1>>bitPos
			data[bytePos] = b
			h += delta
		}
	}

	b.Write(data)

	numBytes += 1 // 1byte represent k (hash function number)
	b.WriteByte(bf.k)

	bf.keysHash = bf.keysHash[:0]

}

func hash32(key []byte) uint32 {
	_, _ = hash.Write(key)
	defer hash.Reset()
	return hash.Sum32()
}

var DefaultFilter = NewBloomFilter(10)
