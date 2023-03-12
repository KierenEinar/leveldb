package leveldb

type GetStat struct {
	FirstMissSeekFile      *tFile
	FirstMissSeekFileLevel int
	MissHint               int
}
