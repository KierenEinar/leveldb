package leveldb

import (
	"encoding/binary"
	"leveldb/errors"
	"leveldb/options"
	"leveldb/storage"
	"leveldb/utils"
	"sync"
)

type WriteBatch struct {
	seq     Sequence
	count   int
	scratch [binary.MaxVarintLen64]byte
	rep     []byte
}

func NewWriteBatch() *WriteBatch {
	wb := new(WriteBatch)
	wb.rep = make([]byte, options.KWriteBatchSeqSize)
	return wb
}

func (wb *WriteBatch) Put(key, value []byte) {

	wb.count++
	wb.rep = append(wb.rep, byte(KeyTypeValue))
	n := binary.PutUvarint(wb.scratch[:], uint64(len(key)))
	wb.rep = append(wb.rep, wb.scratch[:n]...)
	wb.rep = append(wb.rep, key...)

	n = binary.PutUvarint(wb.scratch[:], uint64(len(value)))
	wb.rep = append(wb.rep, wb.scratch[:n]...)
	wb.rep = append(wb.rep, value...)
}

func (wb *WriteBatch) Delete(key []byte) {
	wb.count++
	wb.rep = append(wb.rep, byte(KeyTypeDel))
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
	wb.rep = wb.rep[:options.KWriteBatchHeaderSize] // resize to header
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

func decodeBatchChunk(reader storage.SequentialReader, seqNum Sequence) (wb WriteBatch, err error) {
	p := make([]byte, options.KWriteBatchHeaderSize)
	n, err := reader.Read(p)
	if err != nil {
		return
	}
	if n < options.KWriteBatchHeaderSize {
		err = errors.NewErrCorruption("batch group header less than header size")
		return
	}

	seq := Sequence(binary.LittleEndian.Uint64(p[:options.KWriteBatchSeqSize]))
	batchCount := binary.LittleEndian.Uint32(p[options.KWriteBatchSeqSize:options.KWriteBatchHeaderSize])
	utils.Assert(seq >= seqNum)
	utils.Assert(seq+Sequence(batchCount) > seqNum)
	wb.SetSequence(seq)
	for i := uint32(0); i < batchCount; i++ {
		err = decodeBatchData(reader, &wb)
		if err != nil {
			return
		}
	}

	return
}

func decodeBatchData(r storage.SequentialReader, wb *WriteBatch) (err error) {

	kt, err := r.ReadByte()
	if err != nil {
		return
	}
	kLen, err := binary.ReadUvarint(r)
	if err != nil {
		return
	}
	key := utils.PoolGetBytes(int(kLen))
	defer utils.PoolPutBytes(key)
	_, err = r.Read(*key)
	if err != nil {
		return
	}

	if kt == byte(KeyTypeValue) {
		vLen, err := binary.ReadUvarint(r)
		if err != nil {
			return
		}
		value := utils.PoolGetBytes(int(vLen))
		defer utils.PoolPutBytes(value)
		_, err = r.Read(*value)
		if err != nil {
			return
		}
		wb.Put(*key, *value)
	} else if kt == byte(KeyTypeDel) {
		wb.Delete(*key)
	}
	return
}

func (wb *WriteBatch) insertInto(memDb *MemDB) error {
	utils.Assert(memDb != nil)
	err := wb.foreach(func(kt KeyType, ukey []byte, seq Sequence, value []byte) error {
		if kt == KeyTypeDel {
			return memDb.Del(ukey, seq)
		} else {
			return memDb.Put(ukey, seq, value)
		}
	})
	return err
}

func (wb *WriteBatch) foreach(fn func(kt KeyType, ukey []byte, seq Sequence, value []byte) error) error {

	pos := options.KWriteBatchHeaderSize

	for i := 0; i < wb.count; i++ {

		var (
			ukey, value []byte
		)

		kt := wb.rep[pos]
		pos += 1
		keyLen, m := binary.Uvarint(wb.rep[pos:])
		pos += m
		ukey = wb.rep[m : m+int(keyLen)]

		if KeyType(kt) == KeyTypeValue {
			pos += int(keyLen)
			valLen, m := binary.Uvarint(wb.rep[pos:])
			pos += m
			value = wb.rep[pos : pos+int(valLen)]
		}

		err := fn(KeyType(kt), ukey, wb.seq+Sequence(i), value)
		if err != nil {
			return err
		}
	}

	return nil
}
