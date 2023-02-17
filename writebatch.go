package leveldb

import (
	"encoding/binary"
	"io"
	"sync"

	"github.com/KierenEinar/leveldb/collections"
	"github.com/KierenEinar/leveldb/errors"
	"github.com/KierenEinar/leveldb/storage"
	"github.com/KierenEinar/leveldb/utils"
)

const KWriteBatchSeqSize = 8
const KWriteBatchCountSize = 4
const KWriteBatchHeaderSize = 12 // first 8 bytes represent sequence, last 4 bytes represent batch count

type WriteBatch struct {
	seq     sequence
	count   uint32
	scratch [binary.MaxVarintLen64]byte
	header  [KWriteBatchHeaderSize]byte
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
	if err = wb.rep.WriteByte(byte(keyTypeValue)); err != nil {
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
	if err = wb.rep.WriteByte(byte(keyTypeDel)); err != nil {
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

func (wb *WriteBatch) SetSequence(seq sequence) {
	wb.seq = seq
	binary.LittleEndian.PutUint64(wb.header[:KWriteBatchSeqSize], uint64(seq))
	wb.rep.Update(0, wb.header[:KWriteBatchSeqSize])
}

func (wb *WriteBatch) setCount(count uint32) {
	binary.LittleEndian.PutUint32(wb.header[KWriteBatchSeqSize:], count)
	wb.rep.Update(KWriteBatchSeqSize, wb.header[KWriteBatchSeqSize:])
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
			_, err := src.rep.Write(tmp[KWriteBatchHeaderSize:n])
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

func decodeBatchChunk(reader storage.SequentialReader, seqNum sequence) (wb WriteBatch, err error) {
	n, err := reader.Read(wb.header[:])
	if err != nil {
		return
	}
	if n < KWriteBatchHeaderSize {
		err = errors.NewErrCorruption("leveldb/chunk batch group header less than header approximateSize")
		return
	}

	seq := sequence(binary.LittleEndian.Uint64(wb.header[:KWriteBatchSeqSize]))
	batchCount := binary.LittleEndian.Uint32(wb.header[KWriteBatchSeqSize:KWriteBatchHeaderSize])
	utils.Assert(seq >= seqNum)
	utils.Assert(seq+sequence(batchCount) > seqNum)
	wb.SetSequence(seq)
	for i := uint32(0); i < batchCount; i++ {
		err = decodeBatchData(reader, &wb)
		if err != nil {
			return
		}
	}

	return
}

func decodeBatchData(r storage.SequentialReader, wb *WriteBatch) error {

	kt, err := r.ReadByte()
	if err != nil {
		return err
	}
	kLen, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	key := utils.PoolGetBytes(int(kLen))
	defer utils.PoolPutBytes(key)
	_, err = r.Read(key)
	if err != nil {
		return err
	}

	if kt == byte(keyTypeValue) {
		vLen, err := binary.ReadUvarint(r)
		if err != nil {
			return err
		}
		value := utils.PoolGetBytes(int(vLen))
		defer utils.PoolPutBytes(value)
		_, err = r.Read(value)
		if err != nil {
			return err
		}
		err = wb.Put(key, value)
	} else if kt == byte(keyTypeDel) {
		err = wb.Delete(key)
	}
	return err
}

func (wb *WriteBatch) insertInto(memDb *MemDB) error {
	utils.Assert(memDb != nil)
	err := wb.foreach(func(kt keyType, ukey []byte, seq sequence, value []byte) error {
		if kt == keyTypeDel {
			return memDb.Del(ukey, seq)
		} else {
			return memDb.Put(ukey, seq, value)
		}
	})
	return err
}

func (wb *WriteBatch) foreach(fn func(kt keyType, key []byte, seq sequence, value []byte) error) error {

	wb.rep.ResetRead(0)
	seqN, err := wb.rep.Read(wb.header[:KWriteBatchSeqSize])
	if err != nil {
		return err
	}
	utils.Assert(seqN == KWriteBatchSeqSize)
	seq := binary.LittleEndian.Uint64(wb.header[:KWriteBatchSeqSize])

	countN, err := wb.rep.Read(wb.header[KWriteBatchSeqSize:KWriteBatchHeaderSize])
	utils.Assert(countN == KWriteBatchCountSize)
	count := binary.LittleEndian.Uint32(wb.header[KWriteBatchSeqSize:KWriteBatchHeaderSize])

	utils.Assert(sequence(seq) == wb.seq)
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

		key = utils.PoolGetBytes(int(keyLen))
		_, err = wb.rep.Read(key)
		if err != nil {
			goto END
		}

		if keyType(kt) == keyTypeValue {
			valLen, err := binary.ReadUvarint(wb.rep)
			if err != nil {
				goto END
			}
			value = utils.PoolGetBytes(int(valLen))
			_, err = wb.rep.Read(value)
			err = fn(keyType(kt), key, wb.seq+sequence(i), value)
			goto END
		} else {
			err = fn(keyType(kt), key, wb.seq+sequence(i), nil)
			goto END
		}

	END:
		if key != nil {
			utils.PoolPutBytes(key)
		}

		if value != nil {
			utils.PoolPutBytes(value)
		}

		if err != nil {
			return err
		}

	}

	return nil
}
