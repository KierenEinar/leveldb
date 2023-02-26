package options

import (
	"hash"

	"github.com/KierenEinar/leveldb/cache"
	"github.com/KierenEinar/leveldb/comparer"
	"github.com/KierenEinar/leveldb/filter"
	"github.com/KierenEinar/leveldb/storage"
)

// Options open db options
type Options struct {

	// if missing current file point to manifest, set true will create a new one
	CreateIfMissingCurrent bool

	// key order comparer, set default InternalKey Comparer
	InternalComparer comparer.Comparer

	// filter keys policy, set default InternalKey Filter
	FilterPolicy filter.IFilter

	// filter base lg, set default 8 (1<<8=1024)
	FilterBaseLg uint8

	// set the storage to support data persist, set default fileStorage
	Storage storage.Storage

	// hash32 used to lru cache hit slot, find and insert. set default fnv32
	Hash32 hash.Hash32

	// max manifest file bytes size, set default 64m
	MaxManifestFileSize int64

	// when manifest write to new file failed, allow max failed, set default 1000
	AllowManifestRewriteIgnoreFailed int64

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

	// NoVerifyCheckSum verify the ldb data block content check sum, set default false
	NoVerifyCheckSum bool

	// compaction input's overlapped with grand parent level table count, set default 10
	GPOverlappedLimit int

	// max compaction limit factor, set default 25
	MaxCompactionLimitFactor uint32

	// mem compaction
	// trigger slow write
	Level0SlowDownTrigger int

	// trigger stop write and force level0 compaction
	Level0StopWriteTrigger int

	// when part of journal chunk parse err, drop whole block. set default false, when set true,
	// it will return error when recover log file or recover manifest.
	NoDropWholeBlockOnParseChunkErr bool

	// each write will call sync, set default false, using file page cache
	NoWriteSync bool
}
