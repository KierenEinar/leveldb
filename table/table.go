package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
)

var (
	kMaxNumBytes = make([]byte, 8)
)

func init() {
	binary.PutUvarint(kMaxNumBytes, kMaxNum)
}

type InternalKey []byte

type CompressionType uint8

const (
	compressionTypeNone   CompressionType = 0
	compressionTypeSnappy CompressionType = 1
)

var defaultFilter = NewBloomFilter(10)

func (ik InternalKey) assert() {
	if len(ik) < 8 {
		panic("invalid internal key")
	}
}

func (ik InternalKey) ukey() []byte {
	ik.assert()
	dst := make([]byte, len(ik)-8)
	copy(dst, ik[:len(ik)-8])
	return dst
}

func (ik InternalKey) seq() Sequence {
	ik.assert()
	x := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	return Sequence(x >> 8)
}

func (ik InternalKey) keyType() keyType {
	ik.assert()
	x := binary.LittleEndian.Uint64(ik[len(ik)-8:])
	kt := uint8(x & 1 << 7)
	return keyType(kt)
}

func parseInternalKey(ikey InternalKey) (ukey []byte, kt keyType, seq uint64, err error) {
	if len(ikey) < 8 {
		err = errors.New("invalid internal ikey len")
		return
	}

	num := binary.LittleEndian.Uint64(ikey[len(ikey)-8:])
	seq, kty := num>>8, num&0xff
	kt = keyType(kty)
	if kt > keyTypeDel {
		err = errors.New("invalid internal ikey keytype")
		return
	}
	return
}

type keyType uint8

const (
	keyTypeValue keyType = 0
	keyTypeDel   keyType = 1
)

type tFile struct {
	fd   Fd
	iMax InternalKey
	iMin InternalKey
	Size int
}

type tFiles []tFile

func (sf tFiles) size() (size int) {
	for _, v := range sf {
		size += v.Size
	}
	return
}

type Levels [kLevelNum]tFiles

func (vSet *VersionSet) allocFileNum() uint64 {
	nextFileNum := vSet.nextFileNum
	vSet.nextFileNum++
	return nextFileNum
}

func (vSet *VersionSet) reuseFileNum(fileNum uint64) bool {
	if vSet.nextFileNum-1 == fileNum {
		vSet.nextFileNum = fileNum
		return true
	}
	return false
}

func (vSet *VersionSet) markFileUsed(fileNum uint64) bool {
	if vSet.nextFileNum <= fileNum {
		vSet.nextFileNum = fileNum + 1
		return true
	}
	return false
}

func (vSet *VersionSet) loadCompactPtr(level int) InternalKey {
	if level < len(vSet.compactPtrs) {
		return nil
	}
	return vSet.compactPtrs[level].ikey
}

func (s tFile) isOverlapped(umin []byte, umax []byte) bool {
	smin, smax := s.iMin.ukey(), s.iMax.ukey()
	return !(bytes.Compare(smax, umin) < 0) && !(bytes.Compare(smin, umax) > 0)
}

func (s tFiles) getOverlapped(imin InternalKey, imax InternalKey, overlapped bool) (dst tFiles) {

	if !overlapped {

		var (
			umin, umax        = imin.ukey(), imax.ukey()
			smallest, largest int
			sizeS             = len(s)
		)

		// use binary search begin
		n := sort.Search(sizeS, func(i int) bool {
			return bytes.Compare(s[i].iMin.ukey(), umin) >= 0
		})

		if n == 0 {
			smallest = 0
		} else if bytes.Compare(s[n-1].iMax.ukey(), umin) >= 0 {
			smallest = n - 1
		} else {
			smallest = sizeS
		}

		n = sort.Search(sizeS, func(i int) bool {
			return bytes.Compare(s[i].iMax.ukey(), umax) >= 0
		})

		if n == sizeS {
			largest = sizeS
		} else if bytes.Compare(s[n].iMin.ukey(), umax) >= 0 {
			largest = n + 1
		} else {
			largest = n
		}

		if smallest >= largest {
			return
		}

		dst = make(tFiles, largest-smallest)
		copy(dst, s[smallest:largest])
		return
	}

	var (
		i          = 0
		restart    = false
		umin, umax = imin.ukey(), imax.ukey()
	)

	for i < len(s) {
		sFile := s[i]
		if sFile.isOverlapped(umin, umax) {
			if bytes.Compare(sFile.iMax.ukey(), umax) > 0 {
				umax = sFile.iMax.ukey()
				restart = true
			}
			if bytes.Compare(sFile.iMin.ukey(), umin) < 0 {
				umin = sFile.iMin.ukey()
				restart = true
			}
			if restart {
				dst = dst[:0]
				i = 0
				restart = false // reset
			} else {
				dst = append(dst, sFile)
			}
		}
	}
	return
}

// todo finish it
func (vSet *VersionSet) createNewTable(fd Fd, fileSize int) (*TableWriter, error) {
	return nil, nil
}

type tableOperation struct {
	session *VersionSet
	storage Storage
}

func newTableOperation(s Storage, meta *VersionSet) *tableOperation {
	return &tableOperation{
		session: meta,
		storage: s,
	}
}

func (tableOperation *tableOperation) open(f tFile) (*TableReader, error) {
	reader, err := tableOperation.storage.Open(f.fd)
	if err != nil {
		return nil, err
	}
	return NewTableReader(reader, f.Size)
}

func (tableOperation *tableOperation) newIterator(f tFile) (Iterator, error) {
	tr, err := tableOperation.open(f)
	if err != nil {
		return nil, err
	}
	return tr.NewIterator()
}

func (tableOperation *tableOperation) create() (*tWriter, error) {
	fd := Fd{Num: tableOperation.session.allocFileNum(), FileType: KTableFile}
	w, err := tableOperation.storage.Create(fd)
	if err != nil {
		tableOperation.session.reuseFileNum(fd.Num)
		return nil, err
	}
	return &tWriter{
		fd:    fd,
		fw:    w,
		tw:    NewTableWriter(w),
		first: nil,
		last:  nil,
	}, nil
}

type tWriter struct {
	fd          Fd
	fw          SequentialWriter
	tw          *TableWriter
	first, last InternalKey
}

func (t *tWriter) append(ikey InternalKey, value []byte) error {
	if t.first == nil {
		t.first = append([]byte(nil), ikey...)
	}
	t.last = append(t.last[:0], ikey...)
	return t.tw.Append(ikey, value)
}

func (t *tWriter) finish() (*tFile, error) {

	err := t.tw.Close()
	if err != nil {
		return nil, err
	}

	err = t.fw.Sync()
	if err != nil {
		return nil, err
	}

	err = t.fw.Close()
	if err != nil {
		return nil, err
	}

	return &tFile{
		fd:   t.fd,
		iMax: t.last,
		iMin: t.first,
		Size: t.tw.fileSize(),
	}, nil

}

func (t *tWriter) size() int {
	return t.tw.fileSize()
}
