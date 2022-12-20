package config

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
