package table

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"leveldb/comparer"
	"leveldb/errors"
	"leveldb/filter"
	"leveldb/ikey"
	"leveldb/iterator"
	"leveldb/storage"
	"leveldb/utils"
	"sort"
)

const blockTailLen = 5
const tableFooterLen = 48

type CompressionType uint8

const (
	compressionTypeNone   CompressionType = 0
	compressionTypeSnappy CompressionType = 1
)

var magicByte = []byte("\x57\xfb\x80\x8b\x24\x75\x47\xdb")

type dataBlock struct {
	*utils.BasicReleaser
	cmp                comparer.BasicComparer
	data               []byte
	restartPointOffset int
	restartPointNums   int
}

func newDataBlock(data []byte, cmp comparer.BasicComparer) (*dataBlock, error) {
	dataLen := len(data)
	if dataLen < 4 {
		return nil, errors.NewErrCorruption("block data corruption")
	}
	restartPointNums := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartPointOffset := len(data) - (restartPointNums+1)*4
	block := &dataBlock{
		data:               data,
		restartPointNums:   restartPointNums,
		restartPointOffset: restartPointOffset,
		cmp:                cmp,
	}
	block.OnClose = block.Close
	block.Ref()
	return block, nil
}

func (br *dataBlock) entry(offset int) (entryLen, shareKeyLen int, unShareKey, value []byte, err error) {
	if offset >= br.restartPointOffset {
		err = errors.ErrIterOutOfBounds
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

func (br *dataBlock) readRestartPoint(restartPoint int) (unShareKey []byte) {
	_, n := binary.Uvarint(br.data[restartPoint:])
	unShareKeyLen, m := binary.Uvarint(br.data[restartPoint+n:])
	_, k := binary.Uvarint(br.data[restartPoint+n+m:])
	unShareKey = br.data[restartPoint+n+m+k : restartPoint+n+m+k+int(unShareKeyLen)]
	return
}

func (br *dataBlock) SeekRestartPoint(key []byte) int {

	n := sort.Search(br.restartPointNums, func(i int) bool {
		restartPoint := binary.LittleEndian.Uint32(br.data[br.restartPointOffset : br.restartPointOffset+i*4])
		unShareKey := br.readRestartPoint(int(restartPoint))
		result := br.cmp.Compare(unShareKey, key)
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
	*utils.BasicReleaser
	ref     int32
	offset  int
	prevKey []byte
	dir     iterator.Direction
	err     error
	ikey    []byte
	value   []byte
	cmp     comparer.BasicComparer
}

func newBlockIter(dataBlock *dataBlock) *blockIter {
	bi := &blockIter{
		dataBlock: dataBlock,
		cmp:       dataBlock.cmp,
	}
	br := &utils.BasicReleaser{
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

func (bi *blockIter) SeekFirst() bool {
	bi.dir = iterator.DirSOI
	bi.offset = 0
	bi.prevKey = bi.prevKey[:0]
	return bi.Next()
}

func (bi *blockIter) Seek(key []byte) bool {

	bi.offset = bi.SeekRestartPoint(key)
	bi.prevKey = bi.prevKey[:0]

	for bi.Next() {
		if bi.Valid() != nil {
			return false
		}
		if bi.cmp.Compare(key, bi.ikey) >= 0 {
			return true
		}
	}
	return false
}

func (bi *blockIter) Next() bool {

	if bi.offset >= bi.dataBlock.restartPointOffset {
		bi.dir = iterator.DirEOI
		return false
	}

	bi.dir = iterator.DirForward
	entryLen, shareKeyLen, unShareKey, value, err := bi.entry(bi.offset)
	if err != nil {
		bi.err = err
		return false
	}

	if len(bi.prevKey) < shareKeyLen {
		bi.err = errors.ErrIterInvalidSharedKey
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
	*utils.BasicReleaser
	r           storage.Reader
	tableSize   int
	filterBlock *filterBlock
	indexBlock  *dataBlock
	indexBH     blockHandle
	metaIndexBH blockHandle
	iFilter     filter.IFilter
	cmp         comparer.BasicComparer
}

func NewTableReader(r storage.Reader, fileSize int, cmp comparer.BasicComparer) (*TableReader, error) {
	footer := make([]byte, tableFooterLen)
	_, err := r.ReadAt(footer, int64(fileSize-tableFooterLen))
	if err != nil {
		return nil, err
	}
	tr := &TableReader{
		r:         r,
		tableSize: fileSize,
		BasicReleaser: &utils.BasicReleaser{
			OnClose: func() {
				_ = r.Close()
			},
		},
		iFilter: filter.DefaultFilter,
		cmp:     cmp,
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

	metaBlock, err := newDataBlock(metaIndexData, cmp)
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
		return errors.NewErrCorruption("footer decode failed")
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
		return nil, errors.NewErrCorruption("too short")
	}

	rawData := data[:n-5]
	checkSum := binary.LittleEndian.Uint32(data[n-5 : n-1])
	compressionType := CompressionType(data[n-1])

	if crc32.ChecksumIEEE(rawData) != checkSum {
		return nil, errors.NewErrCorruption("checksum failed")
	}

	switch compressionType {
	case compressionTypeNone:
	default:
		return nil, errors.ErrUnSupportCompressionType
	}

	block, err := newDataBlock(data[:n], tr.cmp)
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
func (tr *TableReader) find(key []byte, noValue bool, filtered bool) (ikey []byte, value []byte, err error) {
	indexBlock, err := tr.getIndexBlock()
	if err != nil {
		return
	}
	defer indexBlock.UnRef()

	indexBlockIter := newBlockIter(indexBlock)
	defer indexBlockIter.UnRef()

	if !indexBlockIter.Seek(key) {
		err = errors.ErrNotFound
		return
	}

	_, blockHandle := readBH(indexBlockIter.Value())

	if filtered {
		contains := tr.filterBlock.mayContains(tr.iFilter, blockHandle, key)
		if !contains {
			err = errors.ErrNotFound
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
		err = errors.ErrNotFound
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
		err = errors.ErrNotFound
		return
	}

	ikey = dataBlockIter1.Key()
	if !noValue {
		value = append([]byte(nil), dataBlockIter.value...)
	}
	return
}

func (tr *TableReader) Find(key []byte) (rKey []byte, value []byte, err error) {
	return tr.find(key, false, true)
}

func (tr *TableReader) FindKey(key []byte) (rKey []byte, err error) {
	rKey, _, err = tr.find(key, true, true)
	return
}

func (tr *TableReader) Get(key []byte) (value []byte, err error) {

	rKey, value, err := tr.find(key, false, true)
	if err != nil {
		return
	}

	if bytes.Compare(ikey.InternalKey(rKey).UserKey(), ikey.InternalKey(key).UserKey()) != 0 {
		err = errors.ErrNotFound
		return
	}

	return value, nil
}

func (tr *TableReader) NewIterator() (iterator.Iterator, error) {
	indexer, err := newIndexIter(tr)
	if err != nil {
		return nil, err
	}
	return iterator.NewIndexedIterator(indexer), nil
}

type indexIter struct {
	*blockIter
	tr *TableReader
	*utils.BasicReleaser
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
		BasicReleaser: &utils.BasicReleaser{
			OnClose: func() {
				blockIter.UnRef()
				tr.UnRef()
			},
		},
	}
	return ii, nil
}

func (indexIter *indexIter) Get() iterator.Iterator {
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

func (filterBlock *filterBlock) mayContains(iFilter filter.IFilter, bh blockHandle, ikey ikey.InternalKey) bool {

	idx := int(bh.offset / 1 << filterBlock.baseLg)
	if idx+1 > filterBlock.filterNums {
		return false
	}

	offsetN := filterBlock.offsets[idx]
	offsetM := filterBlock.offsets[idx+1]
	filter := filterBlock.data[offsetN:offsetM]
	return iFilter.MayContains(filter, ikey.UserKey())
}
