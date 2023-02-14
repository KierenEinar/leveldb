package table

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"

	"github.com/KierenEinar/leveldb/errors"
)

/**
dump output e.g
restart_interval: 4
inputs : ["a":"xx", "b":"xx", "c":"xx", "d":"xx", "e":"xx", "f":"xx", "g":"xx"]

output:
datablock_0:
body: ["a":"xx", "b":"xx", "c":"xx", "d":"xx", "e":"xx", "f":"xx", "g":"xx"]
restart_point: [a,d]
restart_num: 2

datablock_1:
body: ["a":"xx", "b":"xx", "c":"xx", "d":"xx", "e":"xx", "f":"xx", "g":"xx"]
restart_point: [a,d]
restart_num: 2

.
.
.

filter body: [byte ....]
filter_baselg: 10
.
.
.

meta block:
body: ["filter.name", offset: xx, len: xxx]
.
.
.

index block:
body: ["a":{"offset":0, "length":100} "b", "c", "d", "e", "f", "g"]
restart_point: [a,d]
restart_num: 2


footer:
index_block: ["offset":0, "length":100]
meta_block: ["offset":0, "length":100]
magic: ["xxsdsds"]


*/
type Dump struct {
	data        []byte
	dumpSSTable *dumpSSTable
	r           io.Reader
	w           io.Writer
}

type dumpFooter struct {
	indexBlock blockHandle
	metaBlock  blockHandle
	magic      string
}

type dumpDataBlock struct {
	kvPairs          []kv
	restartPointKeys []byte
	restartNum       int
}

func (d *dumpDataBlock) foreach(f func(k, v []byte)) {
	for i := 0; i < len(d.kvPairs); i++ {
		f(d.kvPairs[i].k, d.kvPairs[i].v)
	}
}

type dumpFilter struct {
	dumpBitmap []*dumpBitmap
	offsets    []int
	baseLg     uint8
}

type dumpBitmap struct {
	bitmap []uint8
}

type dumpMetaBlock struct {
	filterName string
	blockHandle
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
		restartPointKeys: make([]byte, 0),
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
			block.restartPointKeys = append(block.restartPointKeys, key...)
		}
		// reset prevkey
		prevKey = key[:]
		entryLen := n + m + k + unShareKeyLen + vLen
		readIdx += entryLen
	}

	return block
}

func NewDump(r io.Reader, w io.Writer) *Dump {
	return &Dump{
		r: r,
		w: w,
	}
}

func (dump *Dump) TableFormat() error {
	if err := dump.readAll(); err != nil {
		return err
	}

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

	// read footer
	footer, err := dump.readFooter()
	if err != nil {
		return err
	}
	ssTable.footer = footer

	// read index block
	indexBlock := dump.readDataBlock(footer.indexBlock)
	ssTable.indexBlock = indexBlock

	// read all data block
	indexBlock.foreach(func(k, v []byte) {
		_, bh := readBH(v)
		dataBlock := dump.readDataBlock(bh)
		ssTable.dataBlocks = append(ssTable.dataBlocks, dataBlock)
	})

	// read meta block
	metaBlock := dump.readDataBlock(footer.metaBlock)
	metaBlock.foreach(func(k, v []byte) {
		ssTable.metaBlock = dump.readMetaBlock(k, v)
	})

	// read filter

	ssTable.filterBlock = dump.readFilterBlock(ssTable.metaBlock.blockHandle)

	dump.data = data
	dump.dumpSSTable = ssTable
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
		bitmap := convertFilterBlockToBitmap(data[s:e])
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

func convertFilterBlockToBitmap(data []byte) *dumpBitmap {

	bitmap := make([]uint8, 0, len(data)*8)
	for _, b := range data {
		bitmap = append(bitmap, byte2Bits(b)[:]...)
	}
	return &dumpBitmap{bitmap: bitmap}
}

func byte2Bits(b byte) []uint8 {

	// e.g 00111100
	//
	r := [8]uint8{0, 0, 0, 0, 0, 0, 0, 0}
	for i := 7; i >= 0; i++ {
		if b>>i&1 > 0 {
			r[7-i] = 1
		}
	}
	return r[:]
}
