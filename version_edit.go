package leveldb

import (
	"encoding/binary"
	"io"
	"leveldb/storage"
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
	lastSeq      Sequence
	compactPtrs  []compactPtr
	delTables    []delTable
	addedTables  []addTable
	err          error
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
	edit.err = nil
}

type compactPtr struct {
	level int
	ikey  InternalKey
}

type delTable struct {
	level  int
	number uint64
}

type addTable struct {
	level  int
	size   int
	number uint64
	imin   InternalKey
	imax   InternalKey
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

func (edit *VersionEdit) setLastSeq(seq Sequence) {
	edit.setRec(kSeqNum)
	edit.lastSeq = seq
}

func (edit *VersionEdit) addCompactPtr(level int, ikey InternalKey) {
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

func (edit *VersionEdit) addNewTable(level, size int, fileNumber uint64, imin, imax InternalKey) {
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
	switch {
	case edit.hasRec(kComparerName):
		edit.writeHeader(dest, kComparerName)
		edit.writeBytes(dest, edit.comparerName)
		fallthrough
	case edit.hasRec(kJournalNum):
		edit.writeHeader(dest, kJournalNum)
		edit.putUVarInt(dest, edit.journalNum)
		fallthrough
	case edit.hasRec(kNextFileNum):
		edit.writeHeader(dest, kNextFileNum)
		edit.putUVarInt(dest, edit.nextFileNum)
		fallthrough
	case edit.hasRec(kSeqNum):
		edit.writeHeader(dest, kSeqNum)
		edit.putUVarInt(dest, uint64(edit.lastSeq))
		fallthrough
	case edit.hasRec(kCompact):
		edit.writeHeader(dest, kCompact)
		for _, cptr := range edit.compactPtrs {
			edit.putVarInt(dest, cptr.level)
			edit.writeBytes(dest, cptr.ikey)
		}
		fallthrough
	case edit.hasRec(kDelTable):
		edit.writeHeader(dest, kDelTable)
		for _, dt := range edit.delTables {
			edit.putVarInt(dest, dt.level)
			edit.putUVarInt(dest, dt.number)
		}
		fallthrough
	case edit.hasRec(kAddTable):
		edit.writeHeader(dest, kAddTable)
		for _, dt := range edit.addedTables {
			edit.putVarInt(dest, dt.level)
			edit.putVarInt(dest, dt.size)
			edit.putUVarInt(dest, dt.number)
			edit.writeBytes(dest, dt.imin)
			edit.writeBytes(dest, dt.imax)
		}
	default:
		panic("leveldb/version_edit unsupport type")
	}
}

func (edit *VersionEdit) DecodeFrom(src storage.SequentialReader) {

	var typ int

	for {

		typ = edit.readHeader(src)
		if edit.err == io.EOF {
			edit.err = nil
			return
		}
		if edit.err != nil {
			return
		}

		switch typ {
		case kComparerName:
			cName := edit.readBytes(src)
			if edit.err != nil {
				return
			}
			edit.comparerName = cName
		case kNextFileNum:
			nextFileNum := edit.readUVarInt(src)
			if edit.err != nil {
				return
			}
			edit.nextFileNum = nextFileNum
		case kJournalNum:
			logNum := edit.readUVarInt(src)
			if edit.err != nil {
				return
			}
			edit.journalNum = logNum
		case kSeqNum:
			seqNum := edit.readUVarInt(src)
			if edit.err != nil {
				return
			}
			edit.lastSeq = Sequence(seqNum)
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
		}
	}

}

func (edit *VersionEdit) readHeader(src io.ByteReader) int {
	return edit.readVarInt(src)
}

func (edit *VersionEdit) writeHeader(w io.Writer, typ int) {
	edit.putVarInt(w, typ)
}

func (edit *VersionEdit) writeBytes(w io.Writer, value []byte) {
	size := len(value)
	edit.putVarInt(w, size)
	_, edit.err = w.Write(value)
}

func (edit *VersionEdit) putVarInt(w io.Writer, value int) {
	x := binary.PutVarint(edit.scratch[:], int64(value))
	_, edit.err = w.Write(edit.scratch[:x])
	return
}

func (edit *VersionEdit) putUVarInt(w io.Writer, value uint64) {
	x := binary.PutUvarint(edit.scratch[:], value)
	_, edit.err = w.Write(edit.scratch[:x])
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
