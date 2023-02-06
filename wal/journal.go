package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/KierenEinar/leveldb/errors"
	"github.com/KierenEinar/leveldb/storage"
)

/**
journal:
using physical record
each block is 32kb, block header is
		checksum      len  type
	/--------------/------/--/
	|	  4B	   |  2B  |1B|
	/--------------/-----/--/

	type including lastType, middleType, firstType

	when type is lastType, that means current record is in last chunk, only occur when chunk split multi record or chunk not cross block
	when type is middleType, that means current record neither first nor last record in chunk
	when type is firstType, that means current record is in first record and current chunk cross block


	| 		chunk0 		|					chunk1				|			chunk2			|   chunk3   |
	/--------------------------/-------------------------/-----------------------/-----------------------/
	|	                       |                         |                  	 |						 |
	/--------------------------/-------------------------/-----------------------/-----------------------/

**/

const kJournalBlockSize = 1 << 15
const kWritableBufferSize = 1 << 16
const kJournalBlockHeaderLen = 7

const (
	kRecordFull    = byte(1)
	kRecordFirst   = byte(2)
	kRecordMiddle  = byte(3)
	kRecordLast    = byte(4)
	kRecordMaxType = kRecordLast
	kBadRecord     = kRecordMaxType + 1
	kEof           = kRecordMaxType + 2
)

type JournalWriter struct {
	err         error
	w           storage.SequentialWriter
	blockOffset int
}

func NewJournalWriter(writer storage.SequentialWriter) *JournalWriter {
	return &JournalWriter{
		w: writer,
	}
}

func (jw *JournalWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	minRead := 1024
	tmp := make([]byte, minRead)

	for {
		m1, rErr := reader.Read(tmp)
		if rErr != nil && rErr != io.EOF {
			err = rErr
			return
		}

		// read end
		if m1 == 0 {
			break
		}

		m2, wErr := jw.Write(tmp[:m1])
		n += int64(m2)
		if wErr != nil {
			err = wErr
			return
		}

		if rErr == io.EOF {
			break
		}
	}
	return
}

func (jw *JournalWriter) Write(chunk []byte) (n int, err error) {

	if jw.err != nil {
		return 0, jw.err
	}

	chunkLen := len(chunk)
	chunkRemain := chunkLen

	var (
		writeNums   int
		chunkType   byte
		blockRemain int
	)

	for {

		var (
			effectiveWrite int
		)

		if chunkRemain == 0 {
			break
		}

		bufRemain := kJournalBlockSize - jw.blockOffset%kJournalBlockSize
		blockRemain = bufRemain - kJournalBlockHeaderLen

		if bufRemain <= kJournalBlockHeaderLen {
			_, _ = jw.w.Write(make([]byte, bufRemain))
			jw.blockOffset += bufRemain
			continue
		}

		if chunkRemain > blockRemain {
			effectiveWrite = blockRemain
		} else {
			effectiveWrite = chunkRemain
		}

		chunkRemain = chunkRemain - effectiveWrite

		if writeNums == 0 {
			if chunkRemain == 0 {
				chunkType = kRecordFull
			} else {
				chunkType = kRecordFirst
			}
		} else {
			if chunkRemain == 0 {
				chunkType = kRecordLast
			} else {
				chunkType = kRecordMiddle
			}
		}

		if effectiveWrite > 0 {
			writeNums++
		}

		jw.err = jw.writePhysicalRecord(chunk[n:n+effectiveWrite], chunkType)
		if jw.err != nil {
			return 0, jw.err
		}
		n = n + effectiveWrite

	}

	return

}

func (jw *JournalWriter) writePhysicalRecord(data []byte, chunkType byte) error {
	avail := len(data)
	record := make([]byte, kJournalBlockHeaderLen)
	checkSum := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(record, checkSum)
	binary.LittleEndian.PutUint16(record[4:], uint16(avail))
	record[6] = chunkType
	jw.blockOffset += kJournalBlockHeaderLen
	_, err := jw.w.Write(record)
	if err != nil {
		return err
	}
	jw.blockOffset += avail
	_, err = jw.w.Write(data)
	if err != nil {
		return err
	}
	return jw.w.Flush()
}

func (jw *JournalWriter) Close() error {
	_ = jw.w.Sync()
	return jw.w.Close()
}

func (jw *JournalWriter) Sync() error {
	return jw.w.Sync()
}

func (jw *JournalWriter) FileSize() int64 {
	return int64(jw.blockOffset)
}

// JournalReader journal reader
// usage:
//	jr := JournalReader{}
//	for {
//		chunkReader, err := jr.NextChunk()
//		if err == io.EOF {
//			return
//		}
//		if err != nil {
//			return err
//		}
//		chunk, err:= ioutil.ReadAll(chunkReader)
//		if err == io.EOF {
//			return
//		}
//	    if err == ErrSkip {
//	   		continue
//	    }
//		if err != nil {
//			return err
//		}
//		process chunk
//	}
type JournalReader struct {
	src                           *sequentialFile
	scratch                       bytes.Buffer // for reused read
	dropWholeBlockOnParseChunkErr bool
}

func NewJournalReader(reader storage.SequentialReader, dropWholeBlockOnParseChunkErr bool) *JournalReader {
	return &JournalReader{
		src: &sequentialFile{
			SequentialReader: reader,
		},
		scratch:                       *bytes.NewBuffer(nil),
		dropWholeBlockOnParseChunkErr: dropWholeBlockOnParseChunkErr,
	}
}

type chunkReader struct {
	jr               *JournalReader
	inFragmentRecord bool // current fragment is part of chunk ?
	eof              bool
}

func (jr *JournalReader) NextChunk() (storage.SequentialReader, error) {

	for {
		kRecordType, fragment, err := jr.seekNextFragment(true)
		if err == io.EOF {
			return nil, io.EOF
		}
		if err == errors.ErrChunkSkipped && jr.dropWholeBlockOnParseChunkErr {
			jr.scratch.Reset()
			continue
		}
		if err != nil {
			return nil, err
		}
		jr.scratch.Write(fragment)

		inFragmentRecord := kRecordType != kRecordFull
		eof := !inFragmentRecord
		return &chunkReader{jr, inFragmentRecord, eof}, nil
	}
}

func (jr *JournalReader) Close() error {
	jr.scratch.Reset()
	return nil
}

func (chunk *chunkReader) ReadByte() (b byte, err error) {

	jr := chunk.jr
	b, err = jr.scratch.ReadByte()
	if err == io.EOF && !chunk.eof {
		p := make([]byte, 1)
		_, err = chunk.Read(p)
		if err != nil {
			return
		}
		b = p[0]
	}

	return
}

func (chunk *chunkReader) Read(p []byte) (nRead int, rErr error) {

	jr := chunk.jr
	for {

		n, _ := jr.scratch.Read(p)

		nRead += n

		// p is fill full
		if n == cap(p) {
			return
		}

		if chunk.eof {
			if nRead == 0 {
				rErr = io.EOF
			}
			return
		}

		// p is not fill full, only if there has next chunk should read next chunk
		_, fragment, err := jr.seekNextFragment(false)

		if err == io.EOF {
			chunk.eof = true
		}

		if err == errors.ErrChunkSkipped && jr.dropWholeBlockOnParseChunkErr {
			chunk.eof = true
			err = io.ErrUnexpectedEOF
		}

		if err != nil {
			rErr = err
			return
		}

		jr.scratch.Write(fragment)

	}
}

func (chunk *chunkReader) Close() error {
	return nil
}

func (jr *JournalReader) seekNextFragment(first bool) (kRecordType byte, fragment []byte, err error) {

	kRecordType, fragment = jr.src.readPhysicalRecord()

	switch kRecordType {
	case kEof:
		err = io.EOF
	case kBadRecord:
		err = errors.ErrChunkSkipped
	case kRecordFirst, kRecordFull:
		if !first {
			err = errors.ErrChunkSkipped
		}
	case kRecordMiddle, kRecordLast:
		if first {
			err = errors.ErrChunkSkipped
		}
	}
	return
}

type sequentialFile struct {
	storage.SequentialReader
	physicalReadOffset int // current cursor read offset
	physicalN          int // current physical offset
	buf                [kJournalBlockSize]byte
	eof                bool
}

func (s *sequentialFile) readPhysicalRecord() (kRecordType byte, fragment []byte) {

	for {
		if s.physicalReadOffset+kJournalBlockHeaderLen >= s.physicalN {
			if !s.eof {
				n, err := s.Read(s.buf[:])
				s.physicalN = n
				s.physicalReadOffset = 0
				if err != nil {
					s.eof = true
					kRecordType = kEof
					return
				}
				if n < kJournalBlockSize {
					s.eof = true
				}
				continue
			} else {
				kRecordType = kEof
				return
			}
		}

		expectedSum := binary.LittleEndian.Uint32(s.buf[s.physicalReadOffset : s.physicalReadOffset+4])
		dataLen := int(binary.LittleEndian.Uint16(s.buf[s.physicalReadOffset+4 : s.physicalReadOffset+6]))
		kRecordType = s.buf[s.physicalReadOffset+6]

		if dataLen+s.physicalReadOffset > s.physicalN {
			kRecordType = kBadRecord
			s.physicalReadOffset = s.physicalN // drop whole record
			return
		}

		actualSum := crc32.ChecksumIEEE(s.buf[s.physicalReadOffset+kJournalBlockHeaderLen : s.physicalReadOffset+kJournalBlockHeaderLen+dataLen])
		if expectedSum != actualSum {
			kRecordType = kBadRecord
			s.physicalReadOffset = s.physicalN // drop whole record
			return
		}

		fragment = s.buf[s.physicalReadOffset : s.physicalReadOffset+dataLen]
		s.physicalReadOffset += dataLen + kJournalBlockHeaderLen

		return

	}
}
