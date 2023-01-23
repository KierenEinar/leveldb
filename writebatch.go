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
	rep     *collections.LinkBlockBuffer
}

func NewWriteBatch() *WriteBatch {
	wb := new(WriteBatch)
	wb.rep = collections.NewLinkBlockBuffer(1 << 10)
	wb.Reset()
	return wb
}

func (wb *WriteBatch) Put(key, value []byte) (err error) {
	wb.count++
	wb.setCount(wb.count)

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
	wb.setCount(wb.count)

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

func (wb *WriteBatch) setCount(count uint32) {
	binary.LittleEndian.PutUint32(wb.header[options.KWriteBatchSeqSize:], count)
	wb.rep.Update(options.KWriteBatchSeqSize, wb.header[options.KWriteBatchSeqSize:])
}

func (wb *WriteBatch) Reset() {
	wb.count = 0
	wb.seq = 0
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
	wb.setCount(wb.count)
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
	n, err := reader.Read(wb.header[:])
	if err != nil {
		return
	}
	if n < options.KWriteBatchHeaderSize {
		err = errors.NewErrCorruption("leveldb/chunk batch group header less than header size")
		return
	}

	seq := Sequence(binary.LittleEndian.Uint64(wb.header[:options.KWriteBatchSeqSize]))
	batchCount := binary.LittleEndian.Uint32(wb.header[options.KWriteBatchSeqSize:options.KWriteBatchHeaderSize])
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
		err = wb.Put(*key, *value)
	} else if kt == byte(KeyTypeDel) {
		err = wb.Delete(*key)
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

func (wb *WriteBatch) foreach(fn func(kt KeyType, key []byte, seq Sequence, value []byte) error) error {

	wb.rep.ResetRead(0)
	seqN, err := wb.rep.Read(wb.header[:options.KWriteBatchSeqSize])
	if err != nil {
		return err
	}
	utils.Assert(seqN == options.KWriteBatchSeqSize)
	seq := binary.LittleEndian.Uint64(wb.header[:options.KWriteBatchSeqSize])

	countN, err := wb.rep.Read(wb.header[options.KWriteBatchSeqSize:options.KWriteBatchHeaderSize])
	utils.Assert(countN == options.KWriteBatchCountSize)
	count := binary.LittleEndian.Uint32(wb.header[options.KWriteBatchHeaderSize:options.KWriteBatchHeaderSize])

	utils.Assert(Sequence(seq) == wb.seq)
	utils.Assert(count == wb.count)

	for i := 0; i < int(wb.count); i++ {

		var (
			key   []byte
			value []byte
		)

		kt, err := wb.rep.ReadByte()
		if err != nil {
			return err
		}

		keyLen, err := binary.ReadUvarint(wb.rep)
		if err != nil {
			return err
		}

		key = *utils.PoolGetBytes(int(keyLen))
		_, err = wb.rep.Read(key)
		if err != nil {
			goto END
		}

		if KeyType(kt) == KeyTypeValue {
			valLen, err := binary.ReadUvarint(wb.rep)
			if err != nil {
				goto END
			}
			value = *utils.PoolGetBytes(int(valLen))
			err = fn(KeyType(kt), key, wb.seq+Sequence(i), value)
			goto END
		} else {
			err = fn(KeyType(kt), key, wb.seq+Sequence(i), nil)
			goto END
		}

	END:
		if key != nil {
			utils.PoolPutBytes(&key)
		}

		if value != nil {
			utils.PoolPutBytes(&value)
		}

		if err != nil {
			return err
		}

	}

	return nil
}
