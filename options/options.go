package options

import (
	"hash"
	"leveldb/cache"
	"leveldb/comparer"
	"leveldb/filter"
	"leveldb/storage"
)

const KLevelNum = 7
const KLevel0SlowDownTrigger = 8
const KLevel0StopWriteTrigger = 12
const KManifestSizeThreshold = 1 << 26 // 64m
const KMemTableWriteBufferSize = 1 << 22

const KLevel1SizeThreshold = 10 * (1 << 20) //10m
const KWriteBatchSeqSize = 8
const KWriteBatchCountSize = 4
const KWriteBatchHeaderSize = 12 // first 8 bytes represent sequence, last 4 bytes represent batch count
const KTypeValue = 1
const KTypeDel = 2
const KTypeSeek = KTypeValue
const KDefaultCacheFileNums = 1000

func MaxBytesForLevel(level int) uint64 {
	result := uint64(KLevel1SizeThreshold)
	for level > 1 {
		result *= 10
		level--
	}
	return result
}

// Options open db options
type Options struct {

	// if missing current file point to manifest, set true will create a new one
	CreateIfMissingCurrent bool

	// key order comparer, set default InternalKey Comparer
	InternalComparer comparer.Comparer

	// filter keys policy, set default InternalKey Filter
	FilterPolicy filter.IFilter

	// set the storage to support data persist, set default fileStorage
	Storage storage.Storage

	// hash32 used to lru cache hit slot, find and insert. set default fnv32
	Hash32 hash.Hash32

	// max manifest file bytes size, set default 64m
	MaxManifestFileSize int64

	// max open ldb file in cache, set default 1000
	MaxOpenFiles uint32

	// amount of data to build up in memory, set default 4m
	WriteBufferSize uint32

	// cache to store ldb block, set default 8m lrucache
	BlockCache cache.Cache

	// on ldb format, every RestartNums of entry build a restart group, set default 16
	BlockRestartInterval uint8

	// ldb block size, set default 4k
	BlockSize uint32

	// ldb max estimate file size, table size always effect when persist block, set default 2m
	MaxEstimateFileSize uint32

	// VerifyCheckSum verify the ldb data block content check sum, set default true
	VerifyCheckSum bool

	// compaction input's overlapped with grand parent level table count, set default 10
	GPOverlappedLimit int

	// max compaction limit factor, set default 25
	MaxCompactionLimitFactor uint32
}
