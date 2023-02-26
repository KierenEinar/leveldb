package leveldb

import (
	"encoding/binary"
	"io"

	"github.com/KierenEinar/leveldb/storage"
)

const (
	kComparerName = iota + 1
	kJournalNum
	kNextFileNum
	kSeqNum
	kCompact
	kDelTable
	kAddTable
)

type VersionEdit struct {
	scratch      [binary.MaxVarintLen64]byte
	rec          uint64
	comparerName []byte
	journalNum   uint64
	nextFileNum  uint64
	lastSeq      sequence
	compactPtrs  []compactPtr
	delTables    []delTable
	addedTables  []addTable
	err          error
	buffer       []byte
}

func newVersionEdit() *VersionEdit {
	return &VersionEdit{
		buffer: make([]byte, 0),
	}
}

func (edit *VersionEdit) resetScratch() {
	for idx := range edit.scratch {
		edit.scratch[idx] = 0
	}
}

func (edit *VersionEdit) reset() {
	edit.rec = 0
	edit.comparerName = nil
	edit.journalNum = 0
	edit.nextFileNum = 0
	edit.lastSeq = 0
	edit.compactPtrs = nil
	edit.delTables = nil
	edit.addedTables = nil
	edit.resetScratch()
	edit.buffer = edit.buffer[:0]
	edit.err = nil
}

type compactPtr struct {
	level int
	ikey  internalKey
}

type delTable struct {
	level  int
	number uint64
}

type addTable struct {
	level  int
	size   int
	number uint64
	imin   internalKey
	imax   internalKey
}

func (edit *VersionEdit) hasRec(bitPos uint8) bool {
	// 01110 & 1 << 1 -> 01110 & 00010 ==
	return edit.rec&uint64(1<<bitPos) != 0
}

func (edit *VersionEdit) setRec(bitPos uint8) {
	edit.rec |= 1 << bitPos
}

func (edit *VersionEdit) setCompareName(cmpName []byte) {
	edit.setRec(kComparerName)
	edit.comparerName = cmpName
}

func (edit *VersionEdit) setLogNum(logNum uint64) {
	edit.setRec(kJournalNum)
	edit.journalNum = logNum
}

func (edit *VersionEdit) setNextFile(nextFileNum uint64) {
	edit.setRec(kNextFileNum)
	edit.nextFileNum = nextFileNum
}

func (edit *VersionEdit) setLastSeq(seq sequence) {
	edit.setRec(kSeqNum)
	edit.lastSeq = seq
}

func (edit *VersionEdit) addCompactPtr(level int, ikey internalKey) {
	edit.setRec(kCompact)
	edit.compactPtrs = append(edit.compactPtrs, compactPtr{
		level: level,
		ikey:  ikey,
	})
}

func (edit *VersionEdit) addDelTable(level int, number uint64) {
	edit.setRec(kDelTable)
	edit.delTables = append(edit.delTables, delTable{
		level:  level,
		number: number,
	})
}

func (edit *VersionEdit) addTableFile(level int, file *tFile) {
	edit.addNewTable(level, file.size, uint64(file.fd), file.iMin, file.iMax)
}

func (edit *VersionEdit) addNewTable(level, size int, fileNumber uint64, imin, imax internalKey) {
	edit.setRec(kAddTable)
	edit.addedTables = append(edit.addedTables, addTable{
		level:  level,
		size:   size,
		number: fileNumber,
		imin:   append([]byte(nil), imin...),
		imax:   append([]byte(nil), imax...),
	})
}

func (edit *VersionEdit) EncodeTo(dest io.Writer) {
	defer edit.resetScratch()
	switch {
	case edit.hasRec(kComparerName):
		edit.writeHeader(kComparerName)
		edit.writeBytes(edit.comparerName)
		fallthrough
	case edit.hasRec(kJournalNum):
		edit.writeHeader(kJournalNum)
		edit.putUVarInt(edit.journalNum)
		fallthrough
	case edit.hasRec(kNextFileNum):
		edit.writeHeader(kNextFileNum)
		edit.putUVarInt(edit.nextFileNum)
		fallthrough
	case edit.hasRec(kSeqNum):
		edit.writeHeader(kSeqNum)
		edit.putUVarInt(uint64(edit.lastSeq))
		fallthrough
	case edit.hasRec(kCompact):
		for _, cptr := range edit.compactPtrs {
			edit.writeHeader(kCompact)
			edit.putVarInt(cptr.level)
			edit.writeBytes(cptr.ikey)
		}
		fallthrough
	case edit.hasRec(kDelTable):
		for _, dt := range edit.delTables {
			edit.writeHeader(kDelTable)
			edit.putVarInt(dt.level)
			edit.putUVarInt(dt.number)
		}
		fallthrough
	case edit.hasRec(kAddTable):
		for _, dt := range edit.addedTables {
			edit.writeHeader(kAddTable)
			edit.putVarInt(dt.level)
			edit.putVarInt(dt.size)
			edit.putUVarInt(dt.number)
			edit.writeBytes(dt.imin)
			edit.writeBytes(dt.imax)
		}
	default:
		panic("leveldb/version_edit unsupport type")
	}

	_, edit.err = dest.Write(edit.buffer)
	edit.buffer = edit.buffer[:0]
}

func (edit *VersionEdit) DecodeFrom(src storage.SequentialReader) {

	var typ int

	for {

		typ = edit.readHeader(src)

		if edit.err != nil {
			if edit.err == io.EOF {
				edit.err = nil
			}
			return
		}

		switch typ {
		case kComparerName:
			cName := edit.readBytes(src)
			if edit.err != nil {
				return
			}
			edit.comparerName = cName
			edit.setRec(kComparerName)
		case kNextFileNum:
			nextFileNum := edit.readUVarInt(src)
			if edit.err != nil {
				return
			}
			edit.nextFileNum = nextFileNum
			edit.setRec(kNextFileNum)
		case kJournalNum:
			logNum := edit.readUVarInt(src)
			if edit.err != nil {
				return
			}
			edit.journalNum = logNum
			edit.setRec(kJournalNum)
		case kSeqNum:
			seqNum := edit.readUVarInt(src)
			if edit.err != nil {
				return
			}
			edit.lastSeq = sequence(seqNum)
			edit.setRec(kSeqNum)
		case kCompact:
			level := edit.readVarInt(src)
			ikey := edit.readBytes(src)
			if edit.err != nil {
				return
			}
			edit.compactPtrs = append(edit.compactPtrs, compactPtr{
				level: level,
				ikey:  ikey,
			})
			edit.setRec(kCompact)
		case kDelTable:
			level := edit.readVarInt(src)
			fileNum := edit.readUVarInt(src)
			if edit.err != nil {
				return
			}
			edit.delTables = append(edit.delTables, delTable{
				level:  level,
				number: fileNum,
			})
			edit.setRec(kDelTable)
		case kAddTable:
			level := edit.readVarInt(src)
			size := edit.readVarInt(src)
			fileNum := edit.readUVarInt(src)
			imin := edit.readBytes(src)
			imax := edit.readBytes(src)
			if edit.err != nil {
				return
			}
			edit.addedTables = append(edit.addedTables, addTable{
				level:  level,
				size:   size,
				number: fileNum,
				imin:   imin,
				imax:   imax,
			})
			edit.setRec(kAddTable)
		}
	}

}

func (edit *VersionEdit) readHeader(src io.ByteReader) int {
	return edit.readVarInt(src)
}

func (edit *VersionEdit) writeHeader(typ int) {
	edit.putVarInt(typ)
}

func (edit *VersionEdit) writeBytes(value []byte) {
	size := len(value)
	edit.putVarInt(size)
	edit.buffer = append(edit.buffer, value...)
}

func (edit *VersionEdit) putVarInt(value int) {
	x := binary.PutVarint(edit.scratch[:], int64(value))
	edit.buffer = append(edit.buffer, edit.scratch[:x]...)
	return
}

func (edit *VersionEdit) putUVarInt(value uint64) {
	x := binary.PutUvarint(edit.scratch[:], value)
	edit.buffer = append(edit.buffer, edit.scratch[:x]...)
	return
}

func (edit *VersionEdit) readVarInt(src io.ByteReader) int {
	var value int64
	value, edit.err = binary.ReadVarint(src)
	return int(value)
}

func (edit *VersionEdit) readUVarInt(src io.ByteReader) uint64 {
	var value uint64
	value, edit.err = binary.ReadUvarint(src)
	return value
}

func (edit *VersionEdit) readBytes(src storage.SequentialReader) []byte {

	size, err := binary.ReadVarint(src)
	if err != nil {
		edit.err = err
		return nil
	}
	b := make([]byte, size)
	var n int
	n, edit.err = src.Read(b)
	if edit.err != nil {
		return b[:n]
	}

	return b[:n]

}
