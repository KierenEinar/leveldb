package table

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"leveldb/cache"
	"leveldb/comparer"
	"leveldb/errors"
	"leveldb/filter"
	"leveldb/iterator"
	"leveldb/options"
	"leveldb/storage"
	"leveldb/utils"
	"sort"
)

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

type blockContent struct {
	data      []byte
	cacheable bool
	poolable  bool
}

type dataBlock struct {
	cmp                comparer.Comparer
	blockContent       blockContent
	restartPointOffset int
	restartPointNums   int
}

func newDataBlock(blockContent blockContent, cmp comparer.Comparer) (*dataBlock, error) {
	data := blockContent.data
	dataLen := len(data)
	if dataLen < 4 {
		return nil, errors.NewErrCorruption("block data corruption")
	}
	restartPointNums := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartPointOffset := len(data) - (restartPointNums+1)*4
	block := &dataBlock{
		blockContent:       blockContent,
		restartPointNums:   restartPointNums,
		restartPointOffset: restartPointOffset,
		cmp:                cmp,
	}
	return block, nil
}

func (block *dataBlock) entry(offset int) (entryLen, shareKeyLen int, unShareKey, value []byte, err error) {
	if offset >= block.restartPointOffset {
		err = errors.ErrIterOutOfBounds
		return
	}
	shareKeyLenU, n := binary.Uvarint(block.blockContent.data[offset:])
	shareKeyLen = int(shareKeyLenU)
	unShareKeyLenU, m := binary.Uvarint(block.blockContent.data[offset+n:])
	unShareKeyLen := int(unShareKeyLenU)
	vLenU, k := binary.Uvarint(block.blockContent.data[offset+n+m:])
	vLen := int(vLenU)
	unShareKey = block.blockContent.data[offset+n+m+k : offset+n+m+k+unShareKeyLen]
	value = block.blockContent.data[offset+n+m+k+unShareKeyLen : offset+n+m+k+unShareKeyLen+vLen]
	entryLen = n + m + k + unShareKeyLen + vLen
	return
}

func (block *dataBlock) readRestartPoint(restartPoint int) (unShareKey []byte) {
	_, n := binary.Uvarint(block.blockContent.data[restartPoint:])
	unShareKeyLen, m := binary.Uvarint(block.blockContent.data[restartPoint+n:])
	_, k := binary.Uvarint(block.blockContent.data[restartPoint+n+m:])
	unShareKey = block.blockContent.data[restartPoint+n+m+k : restartPoint+n+m+k+int(unShareKeyLen)]
	return
}

func (block *dataBlock) seekRestartPoint(key []byte) int {

	n := sort.Search(block.restartPointNums, func(i int) bool {
		restartPoint := binary.LittleEndian.Uint32(block.blockContent.data[block.restartPointOffset : block.restartPointOffset+i*4])
		unShareKey := block.readRestartPoint(int(restartPoint))
		result := block.cmp.Compare(unShareKey, key)
		return result > 0
	})

	if n == 0 {
		return 0
	}

	return n - 1
}

type blockIter struct {
	*dataBlock
	*utils.BasicReleaser
	cmp            comparer.Comparer
	offset         int
	prevKey        []byte
	dir            iterator.Direction
	err            error
	ikey           []byte
	value          []byte
	dummyCleanNode cleanUpNode
}

func newBlockIter(dataBlock *dataBlock, cmp comparer.Comparer) *blockIter {
	bi := &blockIter{
		dataBlock: dataBlock,
		cmp:       cmp,
	}
	br := &utils.BasicReleaser{
		OnClose: func() {
			bi.prevKey = nil
			bi.ikey = nil
			bi.value = nil
			node := bi.dummyCleanNode.next
			for node != nil {
				node.doClean()
				node = node.next
			}
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

	bi.offset = bi.seekRestartPoint(key)
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

type Reader struct {
	*utils.BasicReleaser
	r                 storage.RandomAccessReader
	tableSize         int
	opt               *options.Options
	filterBlockReader *filterBlockReader
	indexBlock        *dataBlock
	indexBH           blockHandle
	metaIndexBH       blockHandle
	cmp               comparer.Comparer
	cacheId           uint64
}

func NewTableReader(opt *options.Options, r storage.RandomAccessReader, fileSize int) (reader *Reader, err error) {
	var (
		footerBlockContent blockContent
		indexBlockContent  blockContent
		footer             = make([]byte, kTableFooterLen)
		footerData         *[]byte
		indexBlock         *dataBlock
	)

	_, err = r.Pread(int64(fileSize-kTableFooterLen), &footerData, &footer)
	if err != nil {
		return
	}

	tr := &Reader{
		r:         r,
		tableSize: fileSize,
		cmp:       opt.InternalComparer,
		opt:       opt,
	}
	tr.BasicReleaser = &utils.BasicReleaser{
		OnClose: tr.Close,
	}

	footerBlockContent.data = *footerData
	err = tr.readFooter(footerBlockContent)
	if err != nil {
		return
	}

	indexBlockContent, err = tr.readBlock(tr.indexBH)
	if err != nil {
		return
	}

	indexBlock, err = newDataBlock(indexBlockContent, tr.cmp)
	if err != nil {
		return
	}

	tr.indexBlock = indexBlock
	err = tr.readMeta()
	return
}

func (tr *Reader) readMeta() (err error) {

	if tr.opt.FilterPolicy == nil {
		return
	}

	var metaContent blockContent

	defer func() {
		if metaContent.poolable {
			utils.PoolPutBytes(&metaContent.data)
		}
	}()

	metaContent, err = tr.readBlock(tr.metaIndexBH)
	if err == nil {
		block, err := newDataBlock(metaContent, comparer.DefaultComparer)
		key := "filterPolicy." + tr.opt.FilterPolicy.Name()
		if err == nil {

			iter := newBlockIter(block, comparer.DefaultComparer)
			if iter.Seek([]byte(key)) && iter.Valid() != nil {
				tr.readFilter(iter.Value())
			}
		}
	}
	return err
}

func (tr *Reader) readFooter(footerContent blockContent) error {

	footerData := footerContent.data
	magic := footerData[40:]
	if bytes.Compare(magic, magicByte) != 0 {
		return errors.NewErrCorruption("footer decode failed")
	}

	bhLen, indexBH := readBH(footerData)
	tr.indexBH = indexBH

	_, tr.metaIndexBH = readBH(footerData[bhLen:])
	return nil
}

func (tr *Reader) readBlock(bh blockHandle) (content blockContent, err error) {

	scratch := utils.PoolGetBytes(int(bh.length) + kBlockTailLen)
	var result *[]byte
	_, err = tr.r.Pread(int64(bh.offset), &result, scratch)
	if err != nil {
		utils.PoolPutBytes(scratch)
		return
	}
	data := *result
	blockContentLen := int(bh.length)

	compressType := data[blockContentLen]
	checkSum := binary.LittleEndian.Uint32(data[blockContentLen+1:])

	if tr.opt.VerifyCheckSum {
		if crc32.ChecksumIEEE(data[:blockContentLen]) != checkSum {
			utils.PoolPutBytes(scratch)
			err = errors.NewErrCorruption("invalid checksum")
			return
		}
	}

	switch compressionType(compressType) {
	case kCompressionTypeNone:
		content.data = data
		if result != scratch { // don't need double cache
			utils.PoolPutBytes(scratch)
		} else {
			content.cacheable = true
			content.poolable = true
		}
	case kCompressionTypeSnappy:

		unCompressedData, unCompressErr := snappyUnCompressed(data)
		if unCompressErr != nil {
			err = unCompressErr
			return
		}
		content.data = unCompressedData
		content.cacheable = true
		content.poolable = true
	default:
		err = errors.NewErrCorruption("data block compression type corrupt")
		utils.PoolPutBytes(scratch)
	}
	return
}

func (tr *Reader) blockReader(blockHandleValue []byte) (iter iterator.Iterator, err error) {
	_, bh := readBH(blockHandleValue)
	var (
		content blockContent
		handle  *cache.LRUHandle
	)
	if tr.opt.BlockCache != nil {
		cacheKey := *utils.PoolGetBytes(16)
		binary.LittleEndian.PutUint64(cacheKey[:8], tr.cacheId)
		binary.LittleEndian.PutUint64(cacheKey[8:], bh.offset)

		handle = tr.opt.BlockCache.Lookup(cacheKey)
		if handle == nil {
			if content, err = tr.readBlock(bh); err != nil {
				return
			}
			if content.cacheable {
				handle = tr.opt.BlockCache.Insert(cacheKey, uint32(len(content.data)), &content, func(key []byte, value interface{}) {
					releaseContent(value)
				})
			}
		}
	} else {
		if content, err = tr.readBlock(bh); err != nil {
			return
		}
	}

	dataBlock, err := newDataBlock(content, tr.opt.InternalComparer)
	if err != nil {
		if content.poolable {
			utils.PoolPutBytes(&content.data)
		}
		return
	}

	blockIter := newBlockIter(dataBlock, tr.cmp)
	if handle != nil {
		registerCleanUp(&blockIter.dummyCleanNode, releaseHandle, tr.opt.BlockCache, handle)
	} else {
		registerCleanUp(&blockIter.dummyCleanNode, releaseContent, &content)
	}

	return
}

// todo used cache
func (tr *Reader) readRawBlock(bh blockHandle) (*dataBlock, error) {
	r := tr.r

	data := make([]byte, bh.length+kBlockTailLen)

	n, err := r.ReadAt(data, int64(bh.offset))
	if err != nil {
		return nil, err
	}

	if n < 5 {
		return nil, errors.NewErrCorruption("too short")
	}

	rawData := data[:n-5]
	checkSum := binary.LittleEndian.Uint32(data[n-5 : n-1])
	compressionType := compressionType(data[n-1])

	if crc32.ChecksumIEEE(rawData) != checkSum {
		return nil, errors.NewErrCorruption("checksum failed")
	}

	switch compressionType {
	case kCompressionTypeNone:
	default:
		return nil, errors.ErrUnSupportCompressionType
	}

	block, err := newDataBlock(data[:n], tr.cmp)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (tr *Reader) getIndexBlock() (b *dataBlock, err error) {

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
func (tr *Reader) find(key []byte, noValue bool, filtered bool) (ikey []byte, value []byte, err error) {
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
		contains := tr.filterBlock.mayContains(tr.filterPolicy, blockHandle, key)
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

func (tr *Reader) Find(key []byte) (rKey []byte, value []byte, err error) {
	return tr.find(key, false, true)
}

func (tr *Reader) FindKey(key []byte) (rKey []byte, err error) {
	rKey, _, err = tr.find(key, true, true)
	return
}

func (tr *Reader) Get(key []byte) (value []byte, err error) {

	rKey, value, err := tr.find(key, false, true)
	if err != nil {
		return
	}

	if tr.cmp.Compare(rKey, key) != 0 {
		err = errors.ErrNotFound
	}

	return value, nil
}

func (tr *Reader) NewIterator() (iterator.Iterator, error) {
	indexer, err := newIndexIter(tr)
	if err != nil {
		return nil, err
	}
	return iterator.NewIndexedIterator(indexer), nil
}

func (tr *Reader) Close() {
	if tr.indexBlock != nil {
		if tr.indexBlock.blockContent.poolable {
			utils.PoolPutBytes(&tr.indexBlock.blockContent.data)
		}
	}
	if tr.filterBlockReader != nil {
		if tr.filterBlockReader.filterData.poolable {
			utils.PoolPutBytes(&tr.filterBlockReader.filterData.data)
		}
	}
}

type indexIter struct {
	*blockIter
	tr *Reader
	*utils.BasicReleaser
}

func newIndexIter(tr *Reader) (*indexIter, error) {
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

type filterBlockReader struct {
	baseLg        uint8
	offsetsOffset uint32
	blockNum      uint32
	filterPolicy  filter.IFilter
	filterData    blockContent
}

func (r *Reader) newFilterBlockReader(bh blockHandle, filterPolicy filter.IFilter) (
	fReader *filterBlockReader, err error) {
	blockContent, err := r.readBlock(bh)
	if err != nil {
		return
	}
	n := len(blockContent.data)
	if n < 5 {
		err = errors.NewErrCorruption("filterPolicy data len less than 5")
		return
	}

	offsetsOffset := binary.LittleEndian.Uint32(blockContent.data[n-5 : n-1])
	if offsetsOffset > uint32(n)-5 {
		err = errors.NewErrCorruption("filterPolicy data offset's offset")
		return
	}

	baseLg := blockContent.data[n-1]
	blockNum := (uint32(n)-1-offsetsOffset)/4 - 1
	fReader = &filterBlockReader{
		baseLg:        baseLg,
		offsetsOffset: offsetsOffset,
		blockNum:      blockNum,
		filterPolicy:  filterPolicy,
		filterData:    blockContent,
	}

	return
}

func (fReader *filterBlockReader) mayContains(key, filter []byte, blockOffset uint32) bool {
	index := blockOffset / (1 << fReader.baseLg)
	start := fReader.offsetsOffset + index*4
	end := fReader.offsetsOffset + (index+1)*4

	i := binary.LittleEndian.Uint32(fReader.filterData.data[start : start+4])
	j := binary.LittleEndian.Uint32(fReader.filterData.data[end : end+4])
	return fReader.filterPolicy.MayContains(key, filter[i:j])
}

func (r *Reader) readFilter(bhValue []byte) {
	_, metaBh := readBH(bhValue)

	filterBlockReader, err := r.newFilterBlockReader(metaBh, r.opt.FilterPolicy)
	if err != nil {
		// todo logger
		// if failed, just return
		return
	}
	r.filterBlockReader = filterBlockReader
	return
}

func snappyUnCompressed(data []byte) (decodeData []byte, err error) {
	panic("implement me...")
}

func releaseHandle(args ...interface{}) {
	utils.Assert(len(args) == 2, "releaseHandle args len not eq 2")
	c, ok := args[0].(cache.Cache)
	utils.Assert(ok, "releaseHandle arg1 convert to Cache failed")
	handle, ok := args[1].(*cache.LRUHandle)
	utils.Assert(ok, "releaseHandle arg2 convert to *LRUHandle failed")
	c.UnRef(handle)
}

func releaseContent(args ...interface{}) {
	utils.Assert(len(args) == 1, "releaseContent args len not eq 1")
	content, ok := args[0].(*blockContent)
	utils.Assert(ok, "releaseContent arg1 convert to *blockContent failed")
	if content.poolable {
		utils.PoolPutBytes(&content.data)
	}
}

type cleanUpNode struct {
	next *cleanUpNode
	f    func(args ...interface{})
	args []interface{}
}

func registerCleanUp(node *cleanUpNode, f func(args ...interface{}), args ...interface{}) {
	nextNode := &cleanUpNode{
		f:    f,
		args: args,
	}
	if node.next != nil {
		nextNode.next = node.next
	}
	node.next = nextNode
	return
}

func (node *cleanUpNode) doClean() {
	n := node
	for n != nil {
		n.f(n.args...)
		n = node.next
	}
}
