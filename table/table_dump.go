package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"

	"github.com/KierenEinar/leveldb/filter"

	"github.com/KierenEinar/leveldb/errors"
)

/**
dump ldb file

data_block
datablock_0
body: ["A00":"A00", "B00":"B00", "C00":"C00", "D00":"D00", "E00":"E00", "F00":"F00", "G00":"G00", "H00":"H00",
"I00":"I00", "J00":"J00", "K00":"K00", "L00":"L00", "M00":"M00", "N00":"N00", "O00":"O00", "P00":"P00",
"Q00":"Q00", "R00":"R00", "S00":"S00", "T00":"T00", "U00":"U00", "V00":"V00", "W00":"W00", "X00":"X00",
"Y00":"Y00", "Z00":"Z00", "a00":"a00", "b00":"b00", "c00":"c00", "d00":"d00", "e00":"e00", "f00":"f00",
"g00":"g00", "h00":"h00", "i00":"i00", "j00":"j00", "k00":"k00", "l00":"l00", "m00":"m00", "n00":"n00",
"o00":"o00", "p00":"p00", "q00":"q00", "r00":"r00", "s00":"s00", "t00":"t00", "u00":"u00", "v00":"v00",
"w00":"w00", "x00":"x00", "y00":"y00", "z00":"z00"]

restart_point:["A00", "Q00", "g00", "w00"]

restart_num: 4

filter_block
filter_data_0: [00011010000110010110100001100010011110010111100000111100111100001110010101011011001110010001101011001
01011000001100110110011000011100111001100100011110100010011100010110110100001101001100110001100010111
00001010001101011011101100111101110111011000101101111110001101010101001101111011100110010111100111011
01110100011000111000101011101010010000011101110101010111001000001001001010101000100001110011011101101
00001010110100101010101111001101100100100011110101110010100111010100010111110100100111101011001010101
01101100001100000000111]
offsets: [0, 66]

meta_block: ["filter.bloomfilter""offset":493, "len":75]

index_block
datablock_0
body: ["{":"{"offset":0, "length":488}"]

restart_point:["{"]

restart_num: 1

footer
index_block: {"offset":610, "length":15}
meta_block: {"offset":573, "length":32}
magic: ["W���$uG�"]

statics: ["datablock key_count=52", "indexblock key_count=1"]

*/
type Dump struct {
	data        []byte
	dumpSSTable *dumpSSTable
	iFilter     filter.IFilter
	r           io.Reader
	w           io.Writer
}

type formatter interface {
	print(w io.Writer)
}

const maxColumn = 100

type dumpFooter struct {
	indexBlock blockHandle
	metaBlock  blockHandle
	magic      string
}

func (f *dumpFooter) print(w io.Writer) {
	/**
	footer:
	index_block: ["offset":0, "length":100]
	meta_block: ["offset":0, "length":100]
	magic: ["xxsdsds"]
	**/

	buf := bytes.NewBuffer(nil)
	buf.WriteString("footer\n")

	buf.WriteString(fmt.Sprintf("index_block: {\"offset\":%d, \"length\":%d}\n", f.indexBlock.offset,
		f.indexBlock.length))

	buf.WriteString(fmt.Sprintf("meta_block: {\"offset\":%d, \"length\":%d}\n", f.metaBlock.offset,
		f.metaBlock.length))

	buf.WriteString(fmt.Sprintf("magic: [\"%s\"]\n\n", f.magic))

	_, _ = buf.WriteTo(w)

}

type dumpDataBlock struct {
	blockId          int
	kvPairs          []kv
	restartPointKeys [][]byte
	restartNum       int
	printValue       func(value []byte) []byte
}

func (d *dumpDataBlock) print(w io.Writer) {

	// title
	buf := bytes.NewBuffer(nil)

	/**
	datablock_0:
	body: ["a":"xx", "b":"xx", "c":"xx", "d":"xx", "e":"xx", "f":"xx", "g":"xx"]
	restart_point: [a,d]
	restart_num: 2
	**/

	buf.WriteString(fmt.Sprintf("datablock_%d\n", d.blockId))

	// main body
	buf.WriteString("body: ")
	buf.WriteString("[")
	_, _ = buf.WriteTo(w)

	for idx, kv := range d.kvPairs {
		buf.WriteString(fmt.Sprintf("\"%s\"", kv.k))
		buf.WriteString(":")
		buf.WriteString(fmt.Sprintf("\"%s\"", d.printValue(kv.v)))
		if idx < len(d.kvPairs)-1 {
			buf.WriteString(", ")
		}
		if buf.Len() > maxColumn {
			buf.WriteString("\n")
			_, _ = buf.WriteTo(w)
		}
	}
	buf.WriteString("]\n\n")
	_, _ = buf.WriteTo(w)

	// restart_point
	buf.WriteString("restart_point:[")
	for idx, key := range d.restartPointKeys {
		buf.WriteString(fmt.Sprintf("\"%s\"", key))
		if idx < len(d.restartPointKeys)-1 {
			buf.WriteString(", ")
		}
		if buf.Len() > maxColumn {
			buf.WriteString("\n")
			_, _ = buf.WriteTo(w)
		}
	}
	buf.WriteString("]\n\n")
	_, _ = buf.WriteTo(w)

	// restart_nums
	buf.WriteString(fmt.Sprintf("restart_num: %d\n\n", d.restartNum))

	_, _ = buf.WriteTo(w)

}

func (d *dumpDataBlock) foreach(f func(i int, k, v []byte)) {
	for i := 0; i < len(d.kvPairs); i++ {
		f(i, d.kvPairs[i].k, d.kvPairs[i].v)
	}
}

type dumpFilter struct {
	dumpBitmap []*dumpBitmap
	offsets    []int
	baseLg     uint8
}

func (f *dumpFilter) print(w io.Writer) {

	/**
	filter_data_0: [0100001100010110101010001010101010101]
	filter_data_1: [0100001100010110101010001010101010101]

	offsets: [1000, 10000, 1000000]
	filter_baselg: 10
	*/

	buf := bytes.NewBuffer(nil)
	buf.WriteString("filter_block\n")
	_, _ = buf.WriteTo(w)

	for idx, block := range f.dumpBitmap {
		buf.WriteString(fmt.Sprintf("filter_data_%d: ", idx))
		buf.WriteString("[")
		_, _ = buf.WriteTo(w)
		for _, bit := range block.bitmap {
			buf.WriteString(strconv.Itoa(int(bit)))
			if buf.Len() > maxColumn {
				buf.WriteString("\n")
				_, _ = buf.WriteTo(w)
			}
		}
		buf.WriteString("]\n")
		_, _ = buf.WriteTo(w)
	}
	_, _ = buf.WriteTo(w)

	buf.WriteString("offsets: ")
	buf.WriteString("[")
	_, _ = buf.WriteTo(w)
	for idx, offset := range f.offsets {
		buf.WriteString(strconv.Itoa(offset))
		if idx < len(f.offsets)-1 {
			buf.WriteString(", ")
		}
		if buf.Len() > maxColumn {
			buf.WriteString("\n")
			_, _ = buf.WriteTo(w)
		}
	}
	buf.WriteString("]\n\n")

	_, _ = buf.WriteTo(w)

}

type dumpBitmap struct {
	bitmap []uint8
	data   []byte
	filter filter.IFilter
}

type dumpMetaBlock struct {
	filterName string
	blockHandle
}

func (d *dumpMetaBlock) print(w io.Writer) {
	/**
	meta block: ["filter.name", offset: xx, len: xxx]
	*/

	buf := bytes.NewBuffer(nil)
	buf.WriteString("meta_block: [")

	buf.WriteString(fmt.Sprintf("\"%s\"", d.filterName))
	buf.WriteString(":")
	buf.WriteString(fmt.Sprintf("{\"offset\":%d, \"length\":%d}", d.offset, d.length))

	buf.WriteString("]\n\n")

	_, _ = w.Write(buf.Bytes())

}

type kv struct {
	k []byte
	v []byte
}

type dumpSSTable struct {
	dataBlocks  []*dumpDataBlock
	filterBlock *dumpFilter
	metaBlock   *dumpMetaBlock
	indexBlock  *dumpDataBlock
	footer      *dumpFooter
	statics
}

type statics struct {
	dataBlockKeyCount  int
	indexBlockKeyCount int

	dataBlockKeyNotInFilter   []kv
	dataBlockKeyInFilterCount int
}

func (f *dumpSSTable) print(w io.Writer) {

	_, _ = w.Write([]byte("data_block\n"))
	for _, d := range f.dataBlocks {
		d.print(w)
		f.statics.dataBlockKeyCount += len(d.kvPairs)
	}

	f.filterBlock.print(w)
	f.metaBlock.print(w)
	_, _ = w.Write([]byte("index_block\n"))
	f.indexBlock.print(w)
	f.footer.print(w)

	f.statics.indexBlockKeyCount += len(f.indexBlock.kvPairs)

	statics := fmt.Sprintf("data.statics: [\"datablock key_count=%d\", \"indexblock key_count=%d\"]\n",
		f.statics.dataBlockKeyCount, f.statics.indexBlockKeyCount)
	_, _ = w.Write([]byte(statics))

	statics = fmt.Sprintf("filter.statics: [\"keys_contains_count=%d\"]\n", f.statics.dataBlockKeyInFilterCount)
	_, _ = w.Write([]byte(statics))

}

func (dump *Dump) readFooter() (*dumpFooter, error) {
	data := dump.data
	footerData := data[len(data)-kTableFooterLen:]
	magic := footerData[40:]
	if bytes.Compare(magic, magicByte) != 0 {
		return nil, errors.NewErrCorruption("footer decode failed")
	}
	footer := &dumpFooter{}
	bhLen, indexBH := readBH(footerData)
	footer.indexBlock = indexBH
	_, footer.metaBlock = readBH(footerData[bhLen:])
	footer.magic = string(magic)
	return footer, nil
}

func (dump *Dump) readDataBlock(bh blockHandle) *dumpDataBlock {

	data := dump.data[bh.offset : bh.offset+bh.length]
	restartPointNums := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartPoints := make(map[int]struct{})
	restartPointOffset := len(data) - (restartPointNums+1)*4

	for i := 0; i < restartPointNums; i++ {
		offset := int(binary.LittleEndian.Uint32(data[restartPointOffset+i*4 : restartPointOffset+(i+1)*4]))
		restartPoints[offset] = struct{}{}
	}

	readIdx := 0
	prevKey := []byte(nil)
	block := &dumpDataBlock{
		restartNum:       restartPointNums,
		kvPairs:          make([]kv, 0),
		restartPointKeys: make([][]byte, 0),
		printValue: func(value []byte) []byte {
			return value
		},
	}

	for readIdx < restartPointOffset {

		if _, ok := restartPoints[readIdx]; ok {
			prevKey = []byte(nil)
		}

		shareKeyLenU, n := binary.Uvarint(data[readIdx:])
		shareKeyLen := int(shareKeyLenU)
		unShareKeyLenU, m := binary.Uvarint(data[readIdx+n:])
		unShareKeyLen := int(unShareKeyLenU)
		vLenU, k := binary.Uvarint(data[readIdx+n+m:])
		vLen := int(vLenU)
		unShareKey := data[readIdx+n+m+k : readIdx+n+m+k+unShareKeyLen]
		value := data[readIdx+n+m+k+unShareKeyLen : readIdx+n+m+k+unShareKeyLen+vLen]
		key := append(prevKey[:shareKeyLen], unShareKey...)
		kv := kv{
			k: key,
			v: value,
		}
		// append block data
		block.kvPairs = append(block.kvPairs, kv)

		if len(prevKey) == 0 {
			block.restartPointKeys = append(block.restartPointKeys, key)
		}
		// reset prevkey
		prevKey = append([]byte(nil), key...)
		entryLen := n + m + k + unShareKeyLen + vLen
		readIdx += entryLen
	}

	return block
}

func NewDump(r io.Reader, w io.Writer, iFilter filter.IFilter) *Dump {
	return &Dump{
		r:       r,
		w:       w,
		iFilter: iFilter,
	}
}

func (dump *Dump) Format() error {
	if err := dump.readAll(); err != nil {
		return err
	}

	// data block
	dump.dumpSSTable.print(dump.w)
	return nil
}

func (dump *Dump) readAll() error {

	// ready readAll
	if dump.dumpSSTable != nil {
		return nil
	}

	ssTable := &dumpSSTable{}

	ssTable.dataBlocks = make([]*dumpDataBlock, 0)

	data, err := ioutil.ReadAll(dump.r)
	if err != nil {
		return err
	}

	dump.data = data

	// read footer
	footer, err := dump.readFooter()
	if err != nil {
		return err
	}
	ssTable.footer = footer

	// read index block
	indexBlock := dump.readDataBlock(footer.indexBlock)
	indexBlock.printValue = func(value []byte) []byte {
		_, bh := readBH(value)
		s := fmt.Sprintf("{\"offset\":%d, \"length\":%d}", bh.offset, bh.length)
		return []byte(s)
	}
	ssTable.indexBlock = indexBlock

	// read all data block
	indexBlock.foreach(func(i int, k, v []byte) {
		_, bh := readBH(v)
		dataBlock := dump.readDataBlock(bh)
		dataBlock.blockId = i
		ssTable.dataBlocks = append(ssTable.dataBlocks, dataBlock)
	})

	// read meta block
	metaBlock := dump.readDataBlock(footer.metaBlock)
	metaBlock.foreach(func(i int, k, v []byte) {
		ssTable.metaBlock = dump.readMetaBlock(k, v)
	})

	// read filter

	filterBlock := dump.readFilterBlock(ssTable.metaBlock.blockHandle)
	ssTable.filterBlock = filterBlock

	dump.data = data
	dump.dumpSSTable = ssTable

	// output whole datablock in filter block static console
	//dump.calculateFilterStatic()

	return nil
}

func (dump *Dump) readMetaBlock(k, v []byte) *dumpMetaBlock {
	_, bh := readBH(v)
	dmb := &dumpMetaBlock{
		filterName:  string(k),
		blockHandle: bh,
	}
	return dmb
}

func (dump *Dump) readFilterBlock(bh blockHandle) *dumpFilter {

	data := dump.data[bh.offset : bh.offset+bh.length]
	baseLg := data[len(data)-1]
	lastOffset := int(binary.LittleEndian.Uint32(data[len(data)-5 : len(data)-1]))
	offsetNums := (len(data) - 1 - lastOffset) / 4
	bitmaps := make([]*dumpBitmap, 0)
	offsets := make([]int, 0)
	for i := 0; i < offsetNums-1; i++ {
		s := int(binary.LittleEndian.Uint32(data[lastOffset+i*4 : lastOffset+(i+1)*4]))
		e := int(binary.LittleEndian.Uint32(data[lastOffset+(i+1)*4 : lastOffset+(i+2)*4]))
		bitmap := convertFilterBlockToBitmap(data[s:e], dump.iFilter)
		bitmaps = append(bitmaps, bitmap)
		offsets = append(offsets, s)
	}
	offsets = append(offsets, lastOffset)
	return &dumpFilter{
		dumpBitmap: bitmaps,
		offsets:    offsets,
		baseLg:     baseLg,
	}
}

func convertFilterBlockToBitmap(data []byte, iFilter filter.IFilter) *dumpBitmap {

	bitmap := make([]uint8, 0, len(data)*8)
	for _, b := range data {
		bitmap = append(bitmap, byte2Bits(b)[:]...)
	}

	return &dumpBitmap{
		bitmap: bitmap,
		data:   data,
		filter: iFilter,
	}
}

func byte2Bits(b byte) []uint8 {

	// e.g 00111100
	//
	r := [8]uint8{0, 0, 0, 0, 0, 0, 0, 0}
	for i := 7; i >= 0; i-- {
		if b>>i&1 > 0 {
			r[7-i] = 1
		}
	}
	return r[:]
}

func (dump *Dump) calculateFilterStatic() {
	dataBlocks := dump.dumpSSTable.dataBlocks
	filterBlock := dump.dumpSSTable.filterBlock
	for idx, dataBlock := range dataBlocks {
		bitmap := filterBlock.dumpBitmap[idx]
		dataBlock.foreach(func(i int, k, v []byte) {
			ok := bitmap.filter.MayContains(k, bitmap.data)
			if !ok {
				dump.dumpSSTable.dataBlockKeyNotInFilter = append(dump.dumpSSTable.dataBlockKeyNotInFilter, kv{
					k: k,
					v: v,
				})
			} else {
				dump.dumpSSTable.dataBlockKeyInFilterCount++
			}
		})
	}
}
