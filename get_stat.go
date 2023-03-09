package leveldb

type GetStat struct {
	SeekFile      *tFile
	SeekFileLevel int
	MissHint      int
}
