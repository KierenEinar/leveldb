package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/KierenEinar/leveldb/cache"
	"github.com/KierenEinar/leveldb/comparer"
	"github.com/KierenEinar/leveldb/errors"
	"github.com/KierenEinar/leveldb/filter"
	"github.com/KierenEinar/leveldb/iterator"
	"github.com/KierenEinar/leveldb/options"
	"github.com/KierenEinar/leveldb/storage"
	"github.com/KierenEinar/leveldb/utils"
)

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
	offsets            []int
}

func newDataBlock(blockContent blockContent, cmp comparer.Comparer) (*dataBlock, error) {
	data := blockContent.data
	dataLen := len(data)
	if dataLen < 4 {
		return nil, errors.NewErrCorruption("block data corruption")
	}
	restartPointNums := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartPointOffset := len(data) - (restartPointNums+1)*4
	offsets := make([]int, restartPointNums)
	for i := 0; i < restartPointNums; i++ {
		offsets[i] = int(binary.LittleEndian.Uint32(data[restartPointOffset+i*4 : restartPointOffset+(i+1)*4]))
	}

	block := &dataBlock{
		blockContent:       blockContent,
		restartPointNums:   restartPointNums,
		restartPointOffset: restartPointOffset,
		cmp:                cmp,
		offsets:            offsets,
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
		restartPoint := binary.LittleEndian.Uint32(block.blockContent.data[block.restartPointOffset+i*4 : block.restartPointOffset+(i+1)*4])
		unShareKey := block.readRestartPoint(int(restartPoint))
		result := block.cmp.Compare(unShareKey, key)
		fmt.Printf("unShareKey=%v, key=%v\n", unShareKey, key)
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
	cmp     comparer.Comparer
	offset  int
	prevKey []byte
	dir     iterator.Direction
	err     error
	key     []byte
	value   []byte
}

func newBlockIter(dataBlock *dataBlock, cmp comparer.Comparer) *blockIter {
	bi := &blockIter{
		dataBlock: dataBlock,
		cmp:       cmp,
	}
	br := &utils.BasicReleaser{
		OnClose: func() {
			bi.prevKey = nil
			bi.key = nil
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

	restartIx := bi.seekRestartPoint(key)
	bi.offset = int(binary.LittleEndian.Uint32(
		bi.dataBlock.blockContent.data[bi.restartPointOffset+restartIx*4 : bi.restartPointOffset+(restartIx+1)*4]))
	bi.prevKey = bi.prevKey[:0]
	for bi.Next() {
		if bi.Valid() != nil {
			return false
		}
		if bi.cmp.Compare(bi.key, key) >= 0 {
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
	bi.key = ikey
	bi.value = value
	bi.prevKey = ikey
	bi.offset = bi.offset + entryLen
	return true
}

func (bi *blockIter) Valid() error {
	return bi.err
}

func (bi *blockIter) Key() []byte {
	return bi.key
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
		footerData         []byte
		indexBlock         *dataBlock
	)

	footerData, err = r.Pread(footer, int64(fileSize-kTableFooterLen))
	if err != nil {
		return
	}

	tr := &Reader{
		r:             r,
		tableSize:     fileSize,
		cmp:           opt.InternalComparer,
		opt:           opt,
		BasicReleaser: &utils.BasicReleaser{},
	}
	tr.Ref()
	tr.RegisterCleanUp(tr.Close)

	footerBlockContent.data = footerData
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

	if err == nil {
		reader = tr
	}

	return
}

func (reader *Reader) NewIterator() iterator.Iterator {
	return iterator.NewIndexedIterator(reader.newIndexIter())
}

func (reader *Reader) Find(key []byte) (rKey []byte, value []byte, err error) {
	return reader.find(key, false, true)
}

func (reader *Reader) FindKey(key []byte) (rKey []byte, err error) {
	rKey, _, err = reader.find(key, true, true)
	return
}

func (reader *Reader) Get(key []byte) (value []byte, err error) {

	rKey, value, err := reader.find(key, false, true)
	if err != nil {
		return
	}

	if reader.cmp.Compare(rKey, key) != 0 {
		err = errors.ErrNotFound
	}

	return value, nil
}

func (reader *Reader) readMeta() (err error) {

	if reader.opt.FilterPolicy == nil {
		return
	}

	var metaContent blockContent

	defer func() {
		if metaContent.poolable {
			utils.PoolPutBytes(metaContent.data)
		}
	}()

	metaContent, err = reader.readBlock(reader.metaIndexBH)
	if err == nil {
		block, err := newDataBlock(metaContent, reader.cmp)
		key := "filter." + reader.opt.FilterPolicy.Name()
		if err == nil {

			iter := newBlockIter(block, reader.cmp)
			if iter.Seek([]byte(key)) && iter.Valid() == nil {
				reader.readFilter(iter.Value())
			}
			iter.UnRef()
		}
	}
	return err
}

func (reader *Reader) readFooter(footerContent blockContent) error {

	footerData := footerContent.data
	magic := footerData[40:]
	if bytes.Compare(magic, magicByte) != 0 {
		return errors.NewErrCorruption("footer decode failed")
	}

	bhLen, indexBH := readBH(footerData)
	reader.indexBH = indexBH

	_, reader.metaIndexBH = readBH(footerData[bhLen:])
	return nil
}

func (reader *Reader) readBlock(bh blockHandle) (content blockContent, err error) {

	scratch := utils.PoolGetBytes(int(bh.length) + kBlockTailLen)
	data, rErr := reader.r.Pread(scratch, int64(bh.offset))
	if rErr != nil {
		utils.PoolPutBytes(scratch)
		return
	}
	blockContentLen := int(bh.length)

	compressType := data[blockContentLen+kBlockTailLen-1]
	checkSum := binary.LittleEndian.Uint32(data[blockContentLen : blockContentLen+kBlockTailLen-1])

	if !reader.opt.NoVerifyCheckSum {
		if crc32.ChecksumIEEE(data[:blockContentLen]) != checkSum {
			utils.PoolPutBytes(scratch)
			err = errors.NewErrCorruption("invalid checksum")
			return
		}
	}

	data = data[:bh.length]

	switch compressionType(compressType) {
	case kCompressionTypeNone:
		content.data = data
		if !bytes.Equal(scratch, data) { // don't need double cache
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

func (reader *Reader) blockReader(bh blockHandle) (iter iterator.Iterator, err error) {
	var (
		content blockContent
		handle  *cache.LRUHandle
	)
	if reader.opt.BlockCache != nil {
		cacheKey := utils.PoolGetBytes(16)
		defer utils.PoolPutBytes(cacheKey)
		binary.LittleEndian.PutUint64(cacheKey[:8], reader.cacheId)
		binary.LittleEndian.PutUint64(cacheKey[8:], bh.offset)

		handle, err = reader.opt.BlockCache.Lookup(cacheKey)
		if err != nil {
			return
		}
		if handle == nil {
			if content, err = reader.readBlock(bh); err != nil {
				return
			}
			if content.cacheable {
				if handle, err = reader.opt.BlockCache.Insert(cacheKey, uint32(len(content.data)), &content, func(key []byte, value interface{}) {
					releaseContent(value)
				}); err != nil {
					return
				}
			}
		}
	} else {
		if content, err = reader.readBlock(bh); err != nil {
			return
		}
	}

	dataBlock, err := newDataBlock(content, reader.opt.InternalComparer)
	if err != nil {
		if content.poolable {
			utils.PoolPutBytes(content.data)
		}
		return
	}

	blockIter := newBlockIter(dataBlock, reader.cmp)
	if handle != nil {
		blockIter.RegisterCleanUp(releaseHandle, reader.opt.BlockCache, handle)
	} else {
		blockIter.RegisterCleanUp(releaseContent, &content)
	}
	iter = blockIter
	return
}

// Seek return gte key
func (reader *Reader) find(key []byte, noValue bool, filtered bool) (ikey []byte, value []byte, err error) {
	indexBlock := reader.indexBlock
	indexBlockIter := newBlockIter(indexBlock, reader.cmp)
	defer indexBlockIter.UnRef()

	if !indexBlockIter.Seek(key) {
		err = errors.ErrNotFound
		return
	}

	_, blockHandle := readBH(indexBlockIter.Value())

	if filtered {
		contains := reader.filterBlockReader.mayContains(key, blockHandle.offset)
		if !contains {
			err = errors.ErrNotFound
			return
		}
	}

	dataBlockIter, err := reader.blockReader(blockHandle)
	if err != nil {
		return
	}
	defer dataBlockIter.UnRef()

	if dataBlockIter.Seek(key) {
		ikey = dataBlockIter.Key()
		if !noValue {
			value = append([]byte(nil), dataBlockIter.Value()...)
		}
		return
	}

	/**
	special case
	0..block last  abcd123
	1..block first abcz124
	so the index block key is abce
	if search abce, so block..0 won't exist abce, should go to the next block
	*/

	if !indexBlockIter.Next() {
		err = errors.ErrNotFound
		return
	}

	_, blockHandle1 := readBH(indexBlockIter.Value())

	dataBlock1Iter, err := reader.blockReader(blockHandle1)
	if err != nil {
		return
	}
	dataBlock1Iter.UnRef()

	if !dataBlock1Iter.Seek(key) {
		err = errors.ErrNotFound
		return
	}

	ikey = dataBlock1Iter.Key()
	if !noValue {
		value = append([]byte(nil), dataBlockIter.Value()...)
	}
	return
}

func (reader *Reader) Close(args ...interface{}) {
	if reader.indexBlock != nil {
		if reader.indexBlock.blockContent.poolable {
			utils.PoolPutBytes(reader.indexBlock.blockContent.data)
		}
	}
	if reader.filterBlockReader != nil {
		if reader.filterBlockReader.filterData.poolable {
			utils.PoolPutBytes(reader.filterBlockReader.filterData.data)
		}
	}

	_ = reader.r.Close()

}

type indexIter struct {
	*blockIter
	tr *Reader
	*utils.BasicReleaser
}

func (reader *Reader) newIndexIter() *indexIter {
	bIter := newBlockIter(reader.indexBlock, reader.cmp)
	ii := &indexIter{
		blockIter: bIter,
		tr:        reader,
		BasicReleaser: &utils.BasicReleaser{
			OnRef: func() {
				reader.Ref()
			},
		},
	}

	ii.RegisterCleanUp(func(args ...interface{}) {
		_reader := args[0].(*Reader)
		_blockIter := args[1].(*blockIter)
		_reader.UnRef()
		_blockIter.UnRef()
	}, reader, bIter)

	ii.Ref()
	return ii
}

func (indexIter *indexIter) Get() iterator.Iterator {
	value := indexIter.Value()
	if value == nil {
		return nil
	}

	_, bh := readBH(value)
	iter, err := indexIter.tr.blockReader(bh)
	if err != nil {
		indexIter.err = err
	}
	return iter
}

type filterBlockReader struct {
	baseLg        uint8
	offsetsOffset uint32
	blockNum      uint32
	filterPolicy  filter.IFilter
	filterData    blockContent
}

func (reader *Reader) newFilterBlockReader(bh blockHandle, filterPolicy filter.IFilter) (
	fReader *filterBlockReader, err error) {
	blockContent, err := reader.readBlock(bh)
	if err != nil {
		return
	}
	n := len(blockContent.data)
	if n < 5 {
		err = errors.NewErrCorruption("filterPolicy data len less than 5")
		return
	}

	offsetsOffset := binary.LittleEndian.Uint32(blockContent.data[n-5 : n-1])
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

func (fReader *filterBlockReader) mayContains(key []byte, blockOffset uint64) bool {
	index := uint32(blockOffset / (1 << fReader.baseLg))
	start := fReader.offsetsOffset + index*4
	end := fReader.offsetsOffset + (index+1)*4

	i := binary.LittleEndian.Uint32(fReader.filterData.data[start : start+4])
	j := binary.LittleEndian.Uint32(fReader.filterData.data[end : end+4])
	return fReader.filterPolicy.MayContains(key, fReader.filterData.data[i:j])
}

func (reader *Reader) readFilter(bhValue []byte) {
	_, metaBh := readBH(bhValue)

	filterBlockReader, err := reader.newFilterBlockReader(metaBh, reader.opt.FilterPolicy)
	if err != nil {
		// todo logger
		// if failed, just return
		return
	}
	reader.filterBlockReader = filterBlockReader
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
		utils.PoolPutBytes(content.data)
	}
}
