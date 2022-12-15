package config

const KLevelNum = 7
const KLevel0SlowDownTrigger = 8
const KLevel0StopWriteTrigger = 12
const KManifestSizeThreshold = 1 << 26 // 64m
const KMemTableWriteBufferSize = 1 << 22

const kLevel1SizeThreshold = 10 * (1 << 20) //10m
const kWriteBatchSeqSize = 8
const kWriteBatchCountSize = 4
const kWriteBatchHeaderSize = 12 // first 8 bytes represent sequence, last 4 bytes represent batch count
const kTypeValue = 1
const kTypeDel = 2
const kTypeSeek = kTypeValue
const kDefaultCacheFileNums = 1000

func maxBytesForLevel(level int) uint64 {
	result := uint64(kLevel1SizeThreshold)
	for level > 1 {
		result *= 10
		level--
	}
	return result
}
