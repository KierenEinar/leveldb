package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"leveldb/comparer"
	"leveldb/filter"
	"leveldb/storage"
	"leveldb/utils"
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
	|		meta block (filter)      |
	/--------------------------------/
	/--------------------------------/
	| meta index block (filter type) |
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
	| filter 0 | filter 1 | filter 2 | filter 3 | of0 | of1 | of2 | of3 | dof | lg |
	/----------/----------/----------/----------/-----/-----/-----/-----/-----/----/

e.g.
   	data block.. 			0      1      2               	  3                    		4
					    /------/------/------/----------------------------------/----------------/

	filter partition...
						/---------------/----------------/---------------/----------------/--------------/

	filter data...             filter 0                      filter 1               filter 2
					   /--------------------/----------------------------------/----------------/
	filter offset 	   0                  2500							     7000              9000
						  0      1     2      3 	 4(offset's offset)
					  /------/------/------/------/------/
	value				 0     2500   7000   7000   9000

meta index block

	/---------------------/------------------------/
	|  key(filter.bloom)  |		block handle       |
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

const defaultDataBlockSize = 1 << 11

type blockWriter struct {
	scratch          []byte
	data             bytes.Buffer
	prevIKey         []byte
	entries          int
	restarts         []int
	restartThreshold int
	offset           int
	cmp              comparer.Comparer
}

func (bw *blockWriter) append(ikey []byte, value []byte) {

	if bw.entries%bw.restartThreshold == 0 {
		bw.prevIKey = bw.prevIKey[:0]
		bw.restarts = append(bw.restarts, bw.offset)
	}

	bw.writeEntry(ikey, value)
	bw.entries++

	bw.prevIKey = append([]byte(nil), ikey...)

}

func (bw *blockWriter) finish() {

	if bw.entries == 0 {
		bw.restarts = append(bw.restarts, 0)
	}

	bw.restarts = append(bw.restarts, len(bw.restarts))

	for _, v := range bw.restarts {
		buf4 := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf4, uint32(v))
		bw.data.Write(buf4)
	}
}

func (bw *blockWriter) bytesLen() int {
	restartsLen := len(bw.restarts)
	if restartsLen == 0 {
		restartsLen = 1
	}
	return bw.data.Len() + restartsLen*4 + 4
}

func (bw *blockWriter) writeEntry(ikey []byte, value []byte) {

	var (
		shareUKey     = bw.cmp.Prefix(bw.prevIKey, ikey)
		shareUKeyLen  = len(shareUKey)
		unShareKeyLen = len(ikey) - shareUKeyLen
		unShareKey    = ikey[unShareKeyLen:]
		vLen          = len(value)
	)

	s1 := binary.PutUvarint(bw.scratch, uint64(shareUKeyLen))
	n1, _ := bw.data.Write(bw.scratch[:s1])

	s2 := binary.PutUvarint(bw.scratch, uint64(unShareKeyLen))
	n2, _ := bw.data.Write(bw.scratch[:s2])

	s3 := binary.PutUvarint(bw.scratch, uint64(vLen))
	n3, _ := bw.data.Write(bw.scratch[:s3])

	n4, _ := bw.data.Write(unShareKey)

	n5, _ := bw.data.Write(value)

	bw.offset += n1 + n2 + n3 + n4 + n5

}

func (bw *blockWriter) reset() {
	bw.data.Reset()
	bw.prevIKey = nil
	bw.offset = 0
	bw.restarts = bw.restarts[:0]
	bw.entries = 0
}

type FilterWriter struct {
	data            bytes.Buffer
	offsets         []int
	nkeys           int
	baseLg          int
	filterGenerator filter.IFilterGenerator
	numBitsPerKey   uint8
}

func (fw *FilterWriter) addKey(ikey []byte) {
	fw.filterGenerator.AddKey(ikey)
	fw.nkeys++
}

func (fw *FilterWriter) flush(offset int) {

	/**
	data block.. 			0      1      2               	  3                    		4
					    /------/------/------/----------------------------------/----------------/

	filter partition...
						/---------------/----------------/---------------/----------------/--------------/

	filter data...             filter 0                      filter 1               filter 2
					   /--------------------/----------------------------------/----------------/
	filter offset 	   0                  2500							     7000              9000
						  0      1     2      3 	 4(offset's offset)
					  /------/------/------/------/------/
	value				 0     2500   7000   7000   9000
		**/

	for c := offset / (1 << fw.baseLg); c > len(fw.offsets); {
		fw.generate()
	}

}

func (fw *FilterWriter) generate() {
	// don't forget to insert offset 0 pos into head in final finish
	if fw.nkeys > 0 {
		fw.filterGenerator.Generate(&fw.data)
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

type TableWriter struct {
	writer      storage.SequentialWriter
	dataBlock   *blockWriter
	indexBlock  *blockWriter
	metaBlock   *blockWriter
	filterBlock *FilterWriter

	blockHandle *blockHandle
	prevKey     []byte
	offset      int
	entries     int

	iFilter filter.IFilter

	scratch [50]byte // tail 20 bytes used to encode block handle
	cmp     comparer.Comparer
}

func NewTableWriter(w storage.SequentialWriter) *TableWriter {
	return &TableWriter{
		writer:  w,
		iFilter: filter.DefaultFilter,
	}
}

func (tableWriter *TableWriter) Append(ikey, value []byte) error {

	dataBlock := tableWriter.dataBlock
	filterBlock := tableWriter.filterBlock

	if tableWriter.entries > 0 && bytes.Compare(dataBlock.prevIKey, ikey) > 0 {
		return errors.New("tableWriter Append ikey not sorted")
	}

	err := tableWriter.flushPendingBH(ikey)
	if err != nil {
		return err
	}

	dataBlock.append(ikey, value)

	filterBlock.addKey(ikey)

	if dataBlock.bytesLen() >= defaultDataBlockSize {
		ferr := tableWriter.finishDataBlock()
		if ferr != nil {
			return ferr
		}
	}

	return nil
}

// Close current sstable
func (tableWriter *TableWriter) Close() error {

	dataBlock := tableWriter.dataBlock

	// finish all data block
	if len(dataBlock.prevIKey) > 0 {
		err := tableWriter.finishDataBlock()
		if err != nil {
			return err
		}
	}

	// flush indexed block
	err := tableWriter.flushPendingBH(nil)
	if err != nil {
		return err
	}

	// flush filter
	bh, err := tableWriter.finishFilterBlock()
	if err != nil {
		return err
	}

	// flush meta block
	metaBlock := tableWriter.metaBlock
	metaBlock.append([]byte("filter.bloomFilter"), writeBH(nil, *bh))
	metaBH, err := tableWriter.writeBlock(&metaBlock.data, compressionTypeNone)
	if err != nil {
		return err
	}

	// flush index block
	indexBlock := tableWriter.indexBlock
	indexBH, err := tableWriter.writeBlock(&indexBlock.data, compressionTypeNone)
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

func (tableWriter *TableWriter) finishDataBlock() error {

	bh, err := tableWriter.writeBlock(&tableWriter.dataBlock.data, compressionTypeNone)
	if err != nil {
		return err
	}
	tableWriter.blockHandle = bh
	tableWriter.dataBlock.reset()
	filterWriter := tableWriter.filterBlock
	filterWriter.flush(tableWriter.offset)
	return nil
}

func (tableWriter *TableWriter) finishFilterBlock() (*blockHandle, error) {

	filterWriter := tableWriter.filterBlock
	filterWriter.generate()
	filterWriter.offsets = append([]int{0}, filterWriter.offsets...)
	offsetBuf := make([]byte, 4)
	for _, offset := range filterWriter.offsets {
		binary.LittleEndian.PutUint32(offsetBuf, uint32(offset))
		filterWriter.data.Write(offsetBuf)
	}
	bh, err := tableWriter.writeBlock(&filterWriter.data, compressionTypeNone)
	if err != nil {
		return nil, err
	}
	return bh, nil
}

func (tableWriter *TableWriter) writeBlock(buf *bytes.Buffer, compressionType CompressionType) (*blockHandle, error) {

	w := tableWriter.writer
	offset := tableWriter.offset
	length := buf.Len()

	blockTail := make([]byte, 5)
	checkSum := crc32.ChecksumIEEE(buf.Bytes())

	binary.LittleEndian.PutUint32(blockTail, checkSum)
	blockTail[4] = byte(compressionType)

	buf.Write(blockTail)

	n, err := w.Write(buf.Bytes())
	if err != nil {
		return nil, err
	}

	tableWriter.offset += n

	bh := &blockHandle{
		offset: uint64(offset),
		length: uint64(length),
	}
	return bh, nil
}

func (tableWriter *TableWriter) flushPendingBH(ikey []byte) error {

	if tableWriter.blockHandle == nil {
		return nil
	}
	var separator []byte
	if len(ikey) == 0 {
		separator = tableWriter.cmp.Successor(tableWriter.prevKey)
	} else {
		separator = tableWriter.cmp.Separator(tableWriter.prevKey, ikey)
	}
	indexBlock := tableWriter.indexBlock
	bhEntry := writeBH(tableWriter.scratch[30:], *tableWriter.blockHandle)
	indexBlock.append(separator, bhEntry)
	tableWriter.blockHandle = nil
	return nil
}

func (tableWriter *TableWriter) flushFooter(indexBH, metaBH blockHandle) error {
	footer := make([]byte, tableFooterLen)
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

	tableWriter.offset += tableFooterLen
	return nil
}

func (tableWriter *TableWriter) fileSize() int {
	return tableWriter.offset
}
