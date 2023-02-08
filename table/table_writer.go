package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/KierenEinar/leveldb/comparer"
	"github.com/KierenEinar/leveldb/filter"
	"github.com/KierenEinar/leveldb/options"
	"github.com/KierenEinar/leveldb/storage"
	"github.com/KierenEinar/leveldb/utils"
)

/**
sstable detail

	/--------------------------------/
	|			data block 0 (2k)    |
	/--------------------------------/
	/--------------------------------/
	|		    data block 1         |
	/--------------------------------/
	/--------------------------------/
	|			data block n         |
	/--------------------------------/
	/--------------------------------/
	|		meta block (filterPolicy)      |
	/--------------------------------/
	/--------------------------------/
	| meta index block (filterPolicy type) |
	/--------------------------------/
	/--------------------------------/
	|	      index block            |
	/--------------------------------/
	/--------------------------------/
	|             footer             |
	/--------------------------------/

block detail

	/------------------------------------/----------------/----------------------/
	|	              data               | 4byte checksum |1byte compression type|
	/------------------------------------/----------------/----------------------/

data block entry

	/------------------------------------------------/
	|share klen|unsharekey len|vlen|unshare key|value|
	/------------------------------------------------/

data block entries

	|                 index group 0                |     index group 1           |    4byte     |    4byte   |   4byte  |
	/----------------------------------------------/-----------------------------/--------------/------------/----------/
	|	entry 0  |  entry 1  | ........|  entry 15 | entry 16 |.......| entry 31 |  rs offset0  | rs offset1 |  rs nums |
	/----------------------------------------------/-----------------------------/--------------/------------/----------/

meta block (each data block's would be in offset [k, k+1]  )

	of0		   of1        of2        of3        data offset's offset
	/----------/----------/----------/----------/-----/-----/-----/-----/-----/----/
	| filterPolicy 0 | filterPolicy 1 | filterPolicy 2 | filterPolicy 3 | of0 | of1 | of2 | of3 | dof | lg |
	/----------/----------/----------/----------/-----/-----/-----/-----/-----/----/

e.g.
   	data block.. 			0      1      2               	  3                    		4
					    /------/------/------/----------------------------------/----------------/

	filterPolicy partition...
						/---------------/----------------/---------------/----------------/--------------/

	filterPolicy data...             filterPolicy 0                      filterPolicy 1               filterPolicy 2
					   /--------------------/----------------------------------/----------------/
	filterPolicy offset 	   0                  2500							     7000              9000
						  0      1     2      3 	 4(offset's offset)
					  /------/------/------/------/------/
	value				 0     2500   7000   7000   9000

meta index block

	/---------------------/------------------------/
	|  key(filterPolicy.bloom)  |		block handle       |
	/---------------------/------------------------/

footer

	/-----------------------------/
	|      index block handle     |  0-20byte(2 varint)
	/-----------------------------/
	/-----------------------------/
	|	 meta index block handle  |  0-20byte(2 varint)
	/-----------------------------/
	/-----------------------------/
	|          padding            |
	/-----------------------------/
	/-----------------------------/
	|		     magic            |   8byte
	/-----------------------------/
**/

type compressionType uint8

const (
	kCompressionTypeNone   compressionType = 0
	kCompressionTypeSnappy compressionType = 1
)

const (
	kBlockTailLen   = 5
	kTableFooterLen = 48
)

var magicByte = []byte("\x57\xfb\x80\x8b\x24\x75\x47\xdb")

type blockWriter struct {
	uVarIntScratch   [binary.MaxVarintLen64]byte
	data             *bytes.Buffer
	prevKey          *bytes.Buffer
	entries          int
	restarts         []int
	restartThreshold int
	offset           int
	comparer         comparer.Comparer
}

func newBlockWriter(blockRestartInterval int, comparer comparer.Comparer) *blockWriter {
	bw := &blockWriter{
		data:             bytes.NewBuffer(nil),
		prevKey:          bytes.NewBuffer(nil),
		restartThreshold: blockRestartInterval,
		comparer:         comparer,
	}
	return bw
}

func (bw *blockWriter) append(key []byte, value []byte) {

	if bw.entries%bw.restartThreshold == 0 {
		bw.restarts = append(bw.restarts, bw.offset)
	}

	bw.writeEntry(key, value)
	bw.entries++

	bw.prevKey.Reset()
	bw.prevKey.Write(key)

}

func (bw *blockWriter) finish() {

	if bw.entries == 0 {
		bw.restarts = append(bw.restarts, 0)
	}
	bw.restarts = append(bw.restarts, len(bw.restarts))
	buf4 := make([]byte, 4)
	for _, v := range bw.restarts {
		binary.LittleEndian.PutUint32(buf4, uint32(v))
		bw.data.Write(buf4)
	}
}

func (bw *blockWriter) bytesLen() int {
	restartsLen := len(bw.restarts)
	if restartsLen == 0 {
		restartsLen = 1
	}
	return bw.offset + restartsLen*4 + 4
}

func (bw *blockWriter) writeEntry(key []byte, value []byte) {

	var (
		shareUKey     = bw.comparer.Prefix(bw.prevKey.Bytes(), key)
		shareUKeyLen  = len(shareUKey)
		unShareKeyLen = len(key) - shareUKeyLen
		unShareKey    = key[shareUKeyLen:]
		vLen          = len(value)
	)

	s1 := binary.PutUvarint(bw.uVarIntScratch[:], uint64(shareUKeyLen))
	n1, _ := bw.data.Write(bw.uVarIntScratch[:s1])

	s2 := binary.PutUvarint(bw.uVarIntScratch[:], uint64(unShareKeyLen))
	n2, _ := bw.data.Write(bw.uVarIntScratch[:s2])

	s3 := binary.PutUvarint(bw.uVarIntScratch[:], uint64(vLen))
	n3, _ := bw.data.Write(bw.uVarIntScratch[:s3])

	n4, _ := bw.data.Write(unShareKey)

	n5, _ := bw.data.Write(value)

	bw.offset += n1 + n2 + n3 + n4 + n5

}

func (bw *blockWriter) reset() {
	bw.data.Reset()
	bw.prevKey.Reset()
	bw.offset = 0
	bw.restarts = bw.restarts[:0]
	bw.entries = 0
}

type filterWriter struct {
	data            *bytes.Buffer
	offsets         []int
	nkeys           int
	baseLg          uint8
	filterGenerator filter.IFilterGenerator
}

func newFilterWriter(iFilter filter.IFilter, baseLg uint8) *filterWriter {
	fw := &filterWriter{
		data:            bytes.NewBuffer(nil),
		baseLg:          baseLg,
		filterGenerator: iFilter.NewGenerator(),
	}
	return fw
}

func (fw *filterWriter) addKey(key []byte) {
	fw.filterGenerator.AddKey(key)
	fw.nkeys++
}

func (fw *filterWriter) flush(offset int) {

	/**
	data block.. 			0      1      2               	  3                    		4
					    /------/------/------/----------------------------------/----------------/

	filterPolicy partition...
						/---------------/----------------/---------------/----------------/--------------/

	filterPolicy data...             filterPolicy 0                      filterPolicy 1               filterPolicy 2
					   /--------------------/----------------------------------/----------------/
	filterPolicy offset 	   0                  2500							     7000              9000
						  0      1     2      3 	 4(offset's offset)
					  /------/------/------/------/------/
	value				 0     2500   7000   7000   9000
		**/

	for c := offset / (1 << fw.baseLg); c > len(fw.offsets); {
		fw.generate()
	}

}

func (fw *filterWriter) generate() {
	// don't forget to insert offset 0 pos into head in final finish
	if fw.nkeys > 0 {
		fw.filterGenerator.Generate(fw.data)
		fw.nkeys = 0
	}
	fw.offsets = append(fw.offsets, fw.data.Len())
}

type blockHandle struct {
	offset uint64
	length uint64
}

func writeBH(dest []byte, bh blockHandle) []byte {
	dest = utils.EnsureBuffer(dest, binary.MaxVarintLen64*2)
	n1 := binary.PutUvarint(dest, bh.offset)
	n2 := binary.PutUvarint(dest[n1:], bh.length)
	return dest[:n1+n2]
}

func readBH(buf []byte) (bhLen int, bh blockHandle) {
	offset, n := binary.Uvarint(buf)
	length, m := binary.Uvarint(buf[n:])
	bhLen = n + m
	bh.offset = offset
	bh.length = length
	return
}

type Writer struct {
	writer           storage.SequentialWriter
	dataBlockWriter  *blockWriter
	indexBlockWriter *blockWriter
	metaBlockWriter  *blockWriter
	filterWriter     *filterWriter

	pendingBH *blockHandle
	offset    int
	entries   int

	scratch  [50]byte // tail 20 bytes used to encode block handle
	comparer comparer.Comparer

	opt *options.Options
}

func NewWriter(writer storage.SequentialWriter, opt *options.Options) *Writer {
	w := &Writer{
		writer:           writer,
		dataBlockWriter:  newBlockWriter(int(opt.BlockRestartInterval), opt.InternalComparer),
		indexBlockWriter: newBlockWriter(1, opt.InternalComparer),
		metaBlockWriter:  newBlockWriter(1, opt.InternalComparer),
		filterWriter:     newFilterWriter(opt.FilterPolicy, opt.FilterBaseLg),
		comparer:         opt.InternalComparer,
		opt:              opt,
	}
	return w
}

func (tableWriter *Writer) Append(key, value []byte) (err error) {

	dataBlockWriter := tableWriter.dataBlockWriter
	filterWriter := tableWriter.filterWriter

	if tableWriter.entries > 0 && tableWriter.comparer.Compare(dataBlockWriter.prevKey.Bytes(), key) > 0 {
		err = errors.New("tableWriter Append ikey not sorted")
		return
	}

	tableWriter.flushPendingBH(key)
	dataBlockWriter.append(key, value)
	filterWriter.addKey(key)

	if dataBlockWriter.bytesLen() >= int(tableWriter.opt.BlockSize) {
		err = tableWriter.finishDataBlock()
		if err != nil {
			return
		}
	}

	return
}

// Close current sstable
func (tableWriter *Writer) Close() error {

	dataBlockWriter := tableWriter.dataBlockWriter

	// finish all data block
	if len(dataBlockWriter.prevKey.Bytes()) > 0 {
		err := tableWriter.finishDataBlock()
		if err != nil {
			return err
		}
	}

	// flush indexed block
	tableWriter.flushPendingBH(nil)

	// flush filterPolicy
	bh, err := tableWriter.finishFilterBlock()
	if err != nil {
		return err
	}

	// flush meta block
	metaBlockWriter := tableWriter.metaBlockWriter
	metaBlockWriter.append([]byte(tableWriter.opt.FilterPolicy.Name()), writeBH(nil, *bh))
	metaBH, err := tableWriter.writeBlock(metaBlockWriter.data, kCompressionTypeNone)
	if err != nil {
		return err
	}

	// flush index block
	indexBlockWriter := tableWriter.indexBlockWriter
	indexBH, err := tableWriter.writeBlock(indexBlockWriter.data, kCompressionTypeNone)
	if err != nil {
		return err
	}

	// flush footer
	err = tableWriter.flushFooter(*indexBH, *metaBH)
	if err != nil {
		return err
	}
	return nil
}

func (tableWriter *Writer) finishDataBlock() error {

	bh, err := tableWriter.writeBlock(tableWriter.dataBlockWriter.data, kCompressionTypeNone)
	if err != nil {
		return err
	}
	tableWriter.pendingBH = bh
	tableWriter.dataBlockWriter.reset()
	filterWriter := tableWriter.filterWriter
	filterWriter.flush(tableWriter.offset)
	return nil
}

func (tableWriter *Writer) finishFilterBlock() (*blockHandle, error) {

	filterWriter := tableWriter.filterWriter
	filterWriter.generate()
	filterWriter.offsets = append([]int{0}, filterWriter.offsets...)
	offsetBuf := make([]byte, 4)
	for _, offset := range filterWriter.offsets {
		binary.LittleEndian.PutUint32(offsetBuf, uint32(offset))
		filterWriter.data.Write(offsetBuf)
	}
	return tableWriter.writeBlock(filterWriter.data, kCompressionTypeNone)
}

func (tableWriter *Writer) writeBlock(buf *bytes.Buffer, compressionType compressionType) (*blockHandle, error) {

	w := tableWriter.writer
	offset := tableWriter.offset
	length := buf.Len()

	blockTail := make([]byte, 5)
	checkSum := crc32.ChecksumIEEE(buf.Bytes())
	binary.LittleEndian.PutUint32(blockTail[:4], checkSum)
	blockTail[4] = byte(compressionType)

	n, err := w.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}

	m, err := buf.Write(blockTail)
	if err != nil {
		return nil, err
	}

	tableWriter.offset += n + m

	bh := &blockHandle{
		offset: uint64(offset),
		length: uint64(length),
	}
	return bh, nil
}

func (tableWriter *Writer) flushPendingBH(key []byte) {

	if tableWriter.pendingBH == nil {
		return
	}
	prevKey := tableWriter.dataBlockWriter.prevKey.Bytes()

	var separator []byte
	if len(key) == 0 {
		separator = tableWriter.comparer.Successor(prevKey)
	} else {
		separator = tableWriter.comparer.Separator(prevKey, key)
	}
	indexBlockWriter := tableWriter.indexBlockWriter
	bhEntry := writeBH(tableWriter.scratch[30:], *tableWriter.pendingBH)
	indexBlockWriter.append(separator, bhEntry)
	tableWriter.pendingBH = nil
	return
}

func (tableWriter *Writer) flushFooter(indexBH, metaBH blockHandle) error {
	footer := make([]byte, kTableFooterLen)
	n1 := copy(footer, writeBH(nil, indexBH))
	_ = copy(footer[n1:], writeBH(nil, metaBH))
	copy(footer[40:], magicByte)
	w := tableWriter.writer
	_, err := w.Write(footer)
	if err != nil {
		return err
	}

	err = w.Sync()
	if err != nil {
		return err
	}

	tableWriter.offset += kTableFooterLen
	return nil
}

func (tableWriter *Writer) ApproximateSize() int {
	return tableWriter.offset
}
