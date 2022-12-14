package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sort"
)

type dataBlock struct {
	*BasicReleaser
	data               []byte
	restartPointOffset int
	restartPointNums   int
}

func newDataBlock(data []byte) (*dataBlock, error) {
	dataLen := len(data)
	if dataLen < 4 {
		return nil, NewErrCorruption("block data corruption")
	}
	restartPointNums := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartPointOffset := len(data) - (restartPointNums+1)*4
	block := &dataBlock{
		data:               data,
		restartPointNums:   restartPointNums,
		restartPointOffset: restartPointOffset,
	}
	block.OnClose = block.Close
	block.Ref()
	return block, nil
}

func (a InternalKey) compare(b InternalKey) int {
	au := a.ukey()
	bu := b.ukey()
	ukeyC := bytes.Compare(au, bu)
	if ukeyC > 0 {
		return 1
	} else if ukeyC < 0 {
		return -1
	} else {
		aSeq := a.seq()
		bSeq := b.seq()
		if aSeq < bSeq {
			return 1
		} else if aSeq > bSeq {
			return -1
		}
		return 0
	}
}

func (br *dataBlock) entry(offset int) (entryLen, shareKeyLen int, unShareKey, value []byte, err error) {
	if offset >= br.restartPointOffset {
		err = ErrIterOutOfBounds
		return
	}
	shareKeyLenU, n := binary.Uvarint(br.data[offset:])
	shareKeyLen = int(shareKeyLenU)
	unShareKeyLenU, m := binary.Uvarint(br.data[offset+n:])
	unShareKeyLen := int(unShareKeyLenU)
	vLenU, k := binary.Uvarint(br.data[offset+n+m:])
	vLen := int(vLenU)
	unShareKey = br.data[offset+n+m+k : offset+n+m+k+unShareKeyLen]
	value = br.data[offset+n+m+k+unShareKeyLen : offset+n+m+k+unShareKeyLen+vLen]
	entryLen = n + m + k + unShareKeyLen + vLen
	return
}

func (br *dataBlock) readRestartPoint(restartPoint int) (unShareKey InternalKey) {
	_, n := binary.Uvarint(br.data[restartPoint:])
	unShareKeyLen, m := binary.Uvarint(br.data[restartPoint+n:])
	_, k := binary.Uvarint(br.data[restartPoint+n+m:])
	unShareKey = br.data[restartPoint+n+m+k : restartPoint+n+m+k+int(unShareKeyLen)]
	return
}

func (br *dataBlock) SeekRestartPoint(key InternalKey) int {

	n := sort.Search(br.restartPointNums, func(i int) bool {
		restartPoint := binary.LittleEndian.Uint32(br.data[br.restartPointOffset : br.restartPointOffset+i*4])
		unShareKey := br.readRestartPoint(int(restartPoint))
		result := unShareKey.compare(key)
		return result > 0
	})

	if n == 0 {
		return 0
	}

	return n - 1
}

func (br *dataBlock) Close() {
	br.data = br.data[:0]
	br.data = nil
	br.restartPointNums = 0
	br.restartPointOffset = 0
}

type blockIter struct {
	*dataBlock
	*BasicReleaser
	ref     int32
	offset  int
	prevKey []byte
	dir     direction
	err     error
	ikey    []byte
	value   []byte
}

func newBlockIter(dataBlock *dataBlock) *blockIter {
	bi := &blockIter{
		dataBlock: dataBlock,
	}
	br := &BasicReleaser{
		OnClose: func() {
			dataBlock.UnRef()
			bi.prevKey = nil
			bi.ikey = nil
			bi.value = nil
		},
	}
	bi.BasicReleaser = br
	bi.Ref()
	return bi
}

type direction int

const (
	dirSOI     direction = 1
	dirForward direction = 2
	dirEOI     direction = 3
)

func (bi *blockIter) SeekFirst() bool {
	bi.dir = dirSOI
	bi.offset = 0
	bi.prevKey = bi.prevKey[:0]
	return bi.Next()
}

func (bi *blockIter) Seek(key InternalKey) bool {

	bi.offset = bi.SeekRestartPoint(key)
	bi.prevKey = bi.prevKey[:0]

	for bi.Next() {
		if bi.Valid() != nil {
			return false
		}
		ikey := InternalKey(bi.ikey)
		if ikey.compare(key) >= 0 {
			return true
		}
	}
	return false
}

func (bi *blockIter) Next() bool {

	if bi.offset >= bi.dataBlock.restartPointOffset {
		bi.dir = dirEOI
		return false
	}

	bi.dir = dirForward
	entryLen, shareKeyLen, unShareKey, value, err := bi.entry(bi.offset)
	if err != nil {
		bi.err = err
		return false
	}

	if len(bi.prevKey) < shareKeyLen {
		bi.err = ErrIterInvalidSharedKey
		return false
	}

	ikey := append(bi.prevKey[:shareKeyLen], unShareKey...)
	bi.ikey = ikey
	bi.value = value

	bi.offset = bi.offset + entryLen
	return true
}

func (bi *blockIter) Valid() error {
	return bi.err
}

func (bi *blockIter) Key() []byte {
	return bi.ikey
}

func (bi *blockIter) Value() []byte {
	return bi.value
}

type TableReader struct {
	*BasicReleaser
	r           Reader
	tableSize   int
	filterBlock *filterBlock
	indexBlock  *dataBlock
	indexBH     blockHandle
	metaIndexBH blockHandle
	iFilter     IFilter
}

func NewTableReader(r Reader, fileSize int) (*TableReader, error) {
	footer := make([]byte, tableFooterLen)
	_, err := r.ReadAt(footer, int64(fileSize-tableFooterLen))
	if err != nil {
		return nil, err
	}
	tr := &TableReader{
		r:         r,
		tableSize: fileSize,
		BasicReleaser: &BasicReleaser{
			OnClose: func() {
				_ = r.Close()
			},
		},
		iFilter: defaultFilter,
	}
	err = tr.readFooter()
	if err != nil {
		return nil, err
	}
	metaIndexData := make([]byte, tr.metaIndexBH.length)
	_, err = r.ReadAt(metaIndexData, int64(tr.metaIndexBH.offset))
	if err != nil {
		return nil, err
	}

	metaBlock, err := newDataBlock(metaIndexData)
	if err != nil {
		return nil, err
	}
	defer metaBlock.UnRef()

	metaIter := newBlockIter(metaBlock)
	defer metaIter.UnRef()

	for metaIter.Next() {
		k := metaIter.Key()
		if len(k) > 0 && bytes.Compare(k, []byte("filter.bloomFilter")) == 0 {
			_, bh := readBH(metaIter.Value())
			tr.filterBlock, err = tr.readFilterBlock(bh)
			if err != nil {
				return nil, err
			}
		}
	}

	tr.Ref()

	return tr, nil
}

func (tableReader *TableReader) readFooter() error {
	footer := make([]byte, tableFooterLen)
	_, err := tableReader.r.ReadAt(footer, int64(tableReader.tableSize-tableFooterLen))
	if err != nil {
		return err
	}

	magic := footer[40:]
	if bytes.Compare(magic, magicByte) != 0 {
		return NewErrCorruption("footer decode failed")
	}

	bhLen, indexBH := readBH(footer)
	tableReader.indexBH = indexBH

	_, tableReader.metaIndexBH = readBH(footer[bhLen:])
	return nil
}

// todo used cache
func (tr *TableReader) readRawBlock(bh blockHandle) (*dataBlock, error) {
	r := tr.r

	data := make([]byte, bh.length+blockTailLen)

	n, err := r.ReadAt(data, int64(bh.offset))
	if err != nil {
		return nil, err
	}

	if n < 5 {
		return nil, NewErrCorruption("too short")
	}

	rawData := data[:n-5]
	checkSum := binary.LittleEndian.Uint32(data[n-5 : n-1])
	compressionType := CompressionType(data[n-1])

	if crc32.ChecksumIEEE(rawData) != checkSum {
		return nil, NewErrCorruption("checksum failed")
	}

	switch compressionType {
	case compressionTypeNone:
	default:
		return nil, ErrUnSupportCompressionType
	}

	block, err := newDataBlock(data[:n])
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (tr *TableReader) getIndexBlock() (b *dataBlock, err error) {

	defer func() {
		if b != nil {
			b.Ref() // for caller
		}
	}()

	if tr.indexBlock != nil {
		return tr.indexBlock, nil
	}
	b, err = tr.readRawBlock(tr.indexBH)
	if err != nil {
		return nil, err
	}
	tr.indexBlock = b
	return
}

// Seek return gte key
func (tr *TableReader) find(key InternalKey, noValue bool, filtered bool) (ikey InternalKey, value []byte, err error) {
	indexBlock, err := tr.getIndexBlock()
	if err != nil {
		return
	}
	defer indexBlock.UnRef()

	indexBlockIter := newBlockIter(indexBlock)
	defer indexBlockIter.UnRef()

	if !indexBlockIter.Seek(key) {
		err = ErrNotFound
		return
	}

	_, blockHandle := readBH(indexBlockIter.Value())

	if filtered {
		contains := tr.filterBlock.mayContains(tr.iFilter, blockHandle, key)
		if !contains {
			err = ErrNotFound
			return
		}
	}

	dataBlock, err := tr.readRawBlock(blockHandle)
	if err != nil {
		return
	}
	defer dataBlock.UnRef()

	dataBlockIter := newBlockIter(dataBlock)
	defer dataBlockIter.UnRef()

	if dataBlockIter.Seek(key) {
		ikey = dataBlockIter.Key()
		if !noValue {
			value = append([]byte(nil), dataBlockIter.value...)
		}
		return
	}

	/**
	special case
	0..block last  abcd
	1..block first abcz
	so the index block key is abce
	if search abce, so block..0 won't exist abce, should go to the next block
	*/

	if !indexBlockIter.Next() {
		err = ErrNotFound
		return
	}

	_, blockHandle1 := readBH(indexBlockIter.Value())

	dataBlock1, err := tr.readRawBlock(blockHandle1)
	if err != nil {
		return
	}
	dataBlock1.UnRef()

	dataBlockIter1 := newBlockIter(dataBlock1)
	defer dataBlockIter1.UnRef()

	if !dataBlockIter1.Seek(key) {
		err = ErrNotFound
		return
	}

	ikey = dataBlockIter1.Key()
	if !noValue {
		value = append([]byte(nil), dataBlockIter.value...)
	}
	return
}

func (tr *TableReader) Find(key InternalKey) (rKey InternalKey, value []byte, err error) {
	return tr.find(key, false, true)
}

func (tr *TableReader) FindKey(key InternalKey) (rKey InternalKey, err error) {
	rKey, _, err = tr.find(key, true, true)
	return
}

func (tr *TableReader) Get(key InternalKey) (value []byte, err error) {

	rKey, value, err := tr.find(key, false, true)
	if err != nil {
		return
	}

	if bytes.Compare(rKey.ukey(), key.ukey()) != 0 {
		err = ErrNotFound
		return
	}

	return value, nil
}

func (tr *TableReader) NewIterator() (Iterator, error) {
	indexer, err := newIndexIter(tr)
	if err != nil {
		return nil, err
	}
	return newIndexedIterator(indexer), nil
}

type indexIter struct {
	*blockIter
	tr *TableReader
	*BasicReleaser
}

func newIndexIter(tr *TableReader) (*indexIter, error) {
	indexBlock, err := tr.getIndexBlock()
	if err != nil {
		return nil, err
	}
	tr.Ref()
	blockIter := newBlockIter(indexBlock)

	ii := &indexIter{
		blockIter: blockIter,
		tr:        tr,
		BasicReleaser: &BasicReleaser{
			OnClose: func() {
				blockIter.UnRef()
				tr.UnRef()
			},
		},
	}
	return ii, nil
}

func (indexIter *indexIter) Get() Iterator {
	value := indexIter.Value()
	if value == nil {
		return nil
	}

	_, bh := readBH(value)

	dataBlock, err := indexIter.tr.readRawBlock(bh)
	if err != nil {
		indexIter.err = err
		return nil
	}
	defer dataBlock.UnRef()

	return newBlockIter(dataBlock)

}

type filterBlock struct {
	data         []byte
	offsetOffset int
	offsets      []int
	baseLg       uint8
	filterNums   int
}

func (tr *TableReader) readFilterBlock(bh blockHandle) (*filterBlock, error) {

	dataBlock, err := tr.readRawBlock(bh)
	if err != nil {
		return nil, err
	}

	dataLen := len(dataBlock.data)

	baseLg := dataBlock.data[dataLen-1]
	lastOffsetB := dataBlock.data[dataLen-5:]

	lastOffset := int(binary.LittleEndian.Uint32(lastOffsetB))
	nums := (dataLen - lastOffset - 1) / 4

	offsets := make([]int, 0, nums)
	for i := 0; i < nums; i++ {
		offsets = append(offsets, int(binary.LittleEndian.Uint32(dataBlock.data[lastOffset+i*4:])))
	}

	filter := dataBlock.data[:lastOffset]
	return &filterBlock{
		data:         filter,
		offsetOffset: lastOffset,
		offsets:      offsets,
		baseLg:       baseLg,
		filterNums:   nums - 1,
	}, nil
}

func (filterBlock *filterBlock) mayContains(iFilter IFilter, bh blockHandle, ikey InternalKey) bool {

	idx := int(bh.offset / 1 << filterBlock.baseLg)
	if idx+1 > filterBlock.filterNums {
		return false
	}

	offsetN := filterBlock.offsets[idx]
	offsetM := filterBlock.offsets[idx+1]
	filter := filterBlock.data[offsetN:offsetM]
	return iFilter.MayContains(filter, ikey.ukey())
}
