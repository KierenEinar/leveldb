package leveldb

import (
	"encoding/binary"
	"io/ioutil"
	"sync"
)

type WriteBatch struct {
	seq     Sequence
	count   int
	scratch [binary.MaxVarintLen64]byte
	rep     []byte
	once    sync.Once
}

func (wb *WriteBatch) Put(key, value []byte) {

	wb.once.Do(func() {
		wb.rep = make([]byte, kWriteBatchHeaderSize)
	})

	wb.count++
	wb.rep = append(wb.rep, kTypeValue)
	n := binary.PutUvarint(wb.scratch[:], uint64(len(key)))
	wb.rep = append(wb.rep, wb.scratch[:n]...)
	wb.rep = append(wb.rep, key...)

	n = binary.PutUvarint(wb.scratch[:], uint64(len(value)))
	wb.rep = append(wb.rep, wb.scratch[:n]...)
	wb.rep = append(wb.rep, value...)
}

func (wb *WriteBatch) Delete(key []byte) {
	wb.count++
	wb.rep = append(wb.rep, kTypeDel)
	n := binary.PutUvarint(wb.scratch[:], uint64(len(key)))
	wb.rep = append(wb.rep, wb.scratch[:n]...)
	wb.rep = append(wb.rep, key...)
}

func (wb *WriteBatch) SetSequence(seq Sequence) {
	wb.seq = seq
	binary.LittleEndian.PutUint64(wb.rep[:8], uint64(seq))
}

func (wb *WriteBatch) Contents() []byte {
	binary.LittleEndian.PutUint32(wb.rep[8:], uint32(wb.count))
	return wb.rep[:]
}

func (wb *WriteBatch) Reset() {
	wb.count = 0
	wb.rep = wb.rep[:kWriteBatchHeaderSize] // resize to header
}

func (wb *WriteBatch) Len() int {
	return wb.count
}

func (wb *WriteBatch) Size() int {
	return len(wb.rep)
}

func (wb *WriteBatch) Capacity() int {
	return cap(wb.rep)
}

func (dst *WriteBatch) append(src *WriteBatch) {
	dst.count += src.count
	dst.rep = append(dst.rep, src.rep...)
}

type writer struct {
	batch *WriteBatch
	done  bool
	err   error
	cv    *sync.Cond
}

func newWriter(batch *WriteBatch, mutex *sync.RWMutex) *writer {
	return &writer{
		batch: batch,
		done:  false,
		cv:    sync.NewCond(mutex),
	}
}

func buildBatchGroup(reader SequentialReader, seqNum Sequence) (wb WriteBatch, err error) {

	chunk, rErr := ioutil.ReadAll(reader)
	if rErr != nil {
		err = rErr
		return
	}

	wb, err = decodeBatchChunk(chunk, seqNum)
	return
}

func decodeBatchChunk(p []byte, seqNum Sequence) (wb WriteBatch, err error) {
	if len(p) < kWriteBatchHeaderSize {
		err = NewErrCorruption("batch group header less than header size")
		return
	}

	seq := Sequence(binary.LittleEndian.Uint64(p[:kWriteBatchHeaderSize]))
	batchCount := binary.LittleEndian.Uint32(p[kWriteBatchHeaderSize:kWriteBatchHeaderSize])
	assert(seq >= seqNum)
	assert(seq+Sequence(batchCount) > seqNum)

	for i := uint32(0); i < batchCount; i++ {
		offset, err := decodeBatchData(p, &wb, seq+Sequence(i))
		if err != nil {
			return
		}
		p = p[offset:]
	}

	return
}

func decodeBatchData(p []byte, wb *WriteBatch, seq Sequence) (offset int, err error) {
	var (
		kLen, vLen uint64
		value      []byte
		m, n       int
	)

	kt := p[m]
	m += 1
	kLen, n = binary.Uvarint(p[m:])
	m += n

	uKey := p[m : m+int(kLen)]
	m += int(kLen)

	if kt == byte(keyTypeValue) {
		vLen, n = binary.Uvarint(p[m:])
		m += n
		value = p[m : m+int(vLen)]
		m += int(vLen)
	}

	assert(m+int(kLen)+int(vLen) <= len(p))

	iKey := buildInternalKey(nil, uKey, keyType(kt), seq)
	wb.Put(iKey, value)

	offset = m
	return
}

func (wb *WriteBatch) insertInto(memDb *MemDB) error {
	assert(memDb != nil)
	err := wb.foreach(func(kt keyType, ukey []byte, seq Sequence, value []byte) error {
		if kt == keyTypeValue {
			return memDb.Del(ukey, seq)
		} else {
			return memDb.Put(ukey, seq, value)
		}
	})
	return err
}

func (wb *WriteBatch) foreach(fn func(kt keyType, ukey []byte, seq Sequence, value []byte) error) error {

	pos := kWriteBatchHeaderSize

	for i := 0; i < wb.count; i++ {

		var (
			ukey, value []byte
		)

		kt := wb.rep[pos]
		pos += 1
		keyLen, m := binary.Uvarint(wb.rep[pos:])
		pos += m
		ukey = wb.rep[m : m+int(keyLen)]

		if keyType(kt) == keyTypeValue {
			pos += int(keyLen)
			valLen, m := binary.Uvarint(wb.rep[pos:])
			pos += m
			value = wb.rep[pos : pos+int(valLen)]
		}

		err := fn(keyType(kt), ukey, wb.seq+Sequence(i), value)
		if err != nil {
			return err
		}
	}

	return nil
}
