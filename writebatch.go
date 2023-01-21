package leveldb

import (
	"encoding/binary"
	"io"
	"leveldb/collections"
	"leveldb/errors"
	"leveldb/options"
	"leveldb/storage"
	"leveldb/utils"
	"sync"
)

type WriteBatch struct {
	seq     Sequence
	count   uint32
	scratch [binary.MaxVarintLen64]byte
	header  [options.KWriteBatchHeaderSize]byte
	rep     *collections.LinkedBlockBuffer
}

func NewWriteBatch() *WriteBatch {
	wb := new(WriteBatch)
	wb.rep = collections.NewLinkedBlockBuffer(1 << 10)
	wb.Reset()
	return wb
}

func (wb *WriteBatch) Put(key, value []byte) (err error) {
	wb.count++
	wb.SetCount(wb.count)

	// key type
	if err = wb.rep.WriteByte(byte(KeyTypeValue)); err != nil {
		return
	}
	// key len
	n := binary.PutUvarint(wb.scratch[:], uint64(len(key)))
	if _, err = wb.rep.Write(wb.scratch[:n]); err != nil {
		return
	}
	// key
	if _, err = wb.rep.Write(key); err != nil {
		return
	}

	// value len
	n = binary.PutUvarint(wb.scratch[:], uint64(len(value)))
	if _, err = wb.rep.Write(wb.scratch[:n]); err != nil {
		return
	}

	// value
	if _, err = wb.rep.Write(value); err != nil {
		return
	}

	return
}

func (wb *WriteBatch) Delete(key []byte) (err error) {
	wb.count++
	wb.SetCount(wb.count)

	// key type
	if err = wb.rep.WriteByte(byte(KeyTypeDel)); err != nil {
		return
	}
	// key len
	n := binary.PutUvarint(wb.scratch[:], uint64(len(key)))
	if _, err = wb.rep.Write(wb.scratch[:n]); err != nil {
		return
	}
	// key
	if _, err = wb.rep.Write(key); err != nil {
		return
	}
	return
}

func (wb *WriteBatch) SetSequence(seq Sequence) {
	wb.seq = seq
	binary.LittleEndian.PutUint64(wb.header[:options.KWriteBatchSeqSize], uint64(seq))
	wb.rep.Update(0, wb.header[:options.KWriteBatchSeqSize])
}

func (wb *WriteBatch) SetCount(count uint32) {
	binary.LittleEndian.PutUint32(wb.header[options.KWriteBatchSeqSize:], count)
	wb.rep.Update(options.KWriteBatchSeqSize, wb.header[options.KWriteBatchSeqSize:])
}

func (wb *WriteBatch) Reset() {
	wb.count = 0
	wb.rep.Reset()
	_, _ = wb.rep.Write(wb.header[:])
}

func (wb *WriteBatch) Len() int {
	return int(wb.count)
}

func (wb *WriteBatch) Size() int {
	return wb.rep.Len()
}

func (wb *WriteBatch) Capacity() int {
	return wb.rep.Cap()
}

func (wb *WriteBatch) append(src *WriteBatch) error {
	wb.count += src.count
	minRead := 1024
	tmp := make([]byte, minRead)
	firstRead := true
	for {
		n, rErr := src.rep.Read(tmp)
		if rErr != nil && rErr != io.EOF {
			return rErr
		}

		if n == 0 {
			break
		}

		if firstRead {
			_, err := src.rep.Write(tmp[options.KWriteBatchHeaderSize:n])
			if err != nil {
				return err
			}
			firstRead = false
			continue
		}

		_, err := src.rep.Write(tmp[:n])
		if err != nil {
			return err
		}

		if rErr == io.EOF {
			break
		}
	}

	return nil

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

	for i := 0; i < int(wb.count); i++ {

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
