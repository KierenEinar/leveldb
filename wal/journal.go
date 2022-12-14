package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
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
	dest        *writableFile
	blockOffset int
}

func NewJournalWriter(writer SequentialWriter) *JournalWriter {
	return &JournalWriter{
		dest: &writableFile{
			w: writer,
		},
	}
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

		blockRemain = kJournalBlockSize - (jw.blockOffset + journalBlockHeaderLen)

		if blockRemain < journalBlockHeaderLen {
			_ = jw.dest.append(make([]byte, blockRemain))
			jw.blockOffset = 0
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
	record := make([]byte, journalBlockHeaderLen)
	checkSum := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(record, checkSum)
	binary.LittleEndian.PutUint16(record[4:], uint16(avail))
	record[6] = chunkType
	jw.blockOffset += journalBlockHeaderLen
	err := jw.dest.append(record)
	if err != nil {
		return err
	}
	jw.blockOffset += avail
	err = jw.dest.append(data)
	if err != nil {
		return err
	}
	return jw.dest.flush()
}

func (jw *JournalWriter) Close() error {
	return jw.dest.Close()
}

func (jw *JournalWriter) Sync() error {
	return jw.dest.w.Sync()
}

type writableFile struct {
	optionFlush bool
	w           SequentialWriter
	pos         int
	buf         [kWritableBufferSize]byte
}

func (w *writableFile) append(data []byte) error {

	writeSize := len(data)
	copySize := copy(w.buf[w.pos:], data)
	// buf can hold entire data
	if copySize == writeSize {
		w.pos += copySize
		return nil
	}

	// buf is full and still need to add the data
	// so just writer to file and clear the buf
	if err := w.flush(); err != nil {
		return err
	}

	// calculate remain write size
	writeSize -= copySize
	if writeSize <= kWritableBufferSize {
		n := copy(w.buf[:], data[copySize:])
		w.pos = n
		return nil
	}

	// otherwise, the data is too large, so write to file direct
	if _, err := w.w.Write(data); err != nil {
		return err
	}
	return nil
}

func (w *writableFile) flush() error {
	if w.pos == 0 {
		return nil
	}
	_, err := w.w.Write(w.buf[:w.pos])
	w.pos = 0
	if err != nil {
		return err
	}
	return nil
}

func (w *writableFile) Close() error {
	return w.w.Close()
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
	src     *sequentialFile
	scratch bytes.Buffer // for reused read
}

func NewJournalReader(reader SequentialReader) *JournalReader {
	return &JournalReader{
		src: &sequentialFile{
			SequentialReader: reader,
		},
		scratch: *bytes.NewBuffer(nil),
	}
}

type chunkReader struct {
	jr               *JournalReader
	inFragmentRecord bool // current fragment is part of chunk ?
	eof              bool
}

func (jr *JournalReader) NextChunk() (SequentialReader, error) {

	for {
		kRecordType, fragment, err := jr.seekNextFragment(true)
		if err == io.EOF {
			return nil, io.EOF
		}
		if err == ErrJournalSkipped {
			continue
		}
		if err == ErrMissingChunk {
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

func (chunk *chunkReader) ReadByte() (byte, error) {

	jr := chunk.jr

	for {
		if jr.scratch.Len() > 0 {
			return jr.scratch.ReadByte()
		}
		if chunk.eof {
			return byte(0), io.EOF
		}
		rt, fragment, err := jr.seekNextFragment(false)
		if err == io.EOF {
			return byte(0), io.EOF
		}
		if err != nil {
			return byte(0), err
		}
		chunk.eof = rt == kRecordLast
		jr.scratch.Write(fragment)
	}

}

func (chunk *chunkReader) Read(p []byte) (nRead int, rErr error) {

	jr := chunk.jr
	for {
		if jr.scratch.Len() == 0 && chunk.eof {
			return nRead, io.EOF
		}

		n, _ := jr.scratch.Read(p)

		nRead += n

		// p is fill full
		if n == cap(p) {
			return nRead, nil
		}

		// p is not fill full, only if there has next chunk should read next chunk
		if jr.scratch.Len() == 0 && !chunk.eof {
			recordType, fragment, err := jr.seekNextFragment(false)
			if err == io.EOF {
				chunk.eof = true
				return nRead, nil
			}
			if err != nil {
				jr.scratch.Reset()
				return nRead, err
			}
			if recordType == kRecordLast {
				chunk.eof = true
			}
			jr.scratch.Write(fragment)
			continue

		}
	}
}

func (chunk *chunkReader) Close() error {
	return nil
}

func (jr *JournalReader) seekNextFragment(first bool) (kRecordType byte, fragment []byte, err error) {

	kRecordType, fragment = jr.src.readPhysicalRecord()
	if kRecordType == kEof {
		err = io.EOF
		return
	}

	if kRecordType == kBadRecord {
		err = ErrJournalSkipped
		return
	}

	switch kRecordType {
	case kRecordFirst, kRecordFull:
		if !first {
			err = ErrMissingChunk
		}
		return
	case kRecordMiddle, kRecordLast:
		if first {
			err = ErrMissingChunk
		}
		return
	default:
		err = ErrJournalSkipped
		return
	}
}

type sequentialFile struct {
	SequentialReader
	physicalReadOffset int // current cursor read offset
	physicalN          int // current physical offset
	buf                [kJournalBlockSize]byte
	eof                bool
}

func (s *sequentialFile) readPhysicalRecord() (kRecordType byte, fragment []byte) {

	for {
		if s.physicalReadOffset+journalBlockHeaderLen > s.physicalN {
			if !s.eof {
				n, err := s.Read(s.buf[:])
				s.physicalN += n
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

		actualSum := crc32.ChecksumIEEE(s.buf[s.physicalReadOffset+journalBlockHeaderLen : s.physicalReadOffset+journalBlockHeaderLen+dataLen])
		if expectedSum != actualSum {
			kRecordType = kBadRecord
			s.physicalReadOffset = s.physicalN // drop whole record
			return
		}

		s.physicalReadOffset += dataLen

		// last empty block
		if dataLen == 0 {
			continue
		}

		return

	}
}
