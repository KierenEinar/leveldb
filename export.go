package leveldb

type DB interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
}

func Open(dbPath string) (DB, error) {

}

