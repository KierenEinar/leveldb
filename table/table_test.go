package table

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
	"testing"

	"github.com/KierenEinar/leveldb/cache"
	"github.com/KierenEinar/leveldb/storage"

	"github.com/KierenEinar/leveldb/comparer"
	"github.com/KierenEinar/leveldb/filter"

	"github.com/KierenEinar/leveldb/options"
)

var (
	opt *options.Options
	fs  storage.Storage
)

func TestMain(m *testing.M) {
	tmp, _ := ioutil.TempDir(os.TempDir(), "")
	fmt.Print(tmp)
	fs, _ = storage.OpenPath(tmp)
	opt = &options.Options{
		CreateIfMissingCurrent:        false,
		InternalComparer:              comparer.DefaultComparer,
		FilterPolicy:                  filter.DefaultFilter,
		FilterBaseLg:                  12,
		Storage:                       fs,
		Hash32:                        fnv.New32(),
		MaxManifestFileSize:           1 << 26,
		MaxOpenFiles:                  1000,
		WriteBufferSize:               1 << 22, // 4m
		BlockCache:                    cache.NewCache(10, fnv.New32()),
		BlockRestartInterval:          1 << 4,  // 16
		BlockSize:                     1 << 12, // 4k
		MaxEstimateFileSize:           1 << 21, // 2m
		NoVerifyCheckSum:              false,
		GPOverlappedLimit:             10,
		MaxCompactionLimitFactor:      25,
		DropWholeBlockOnParseChunkErr: false,
	}
	m.Run()
	_ = fs.Close()
	_ = os.RemoveAll(tmp)
}

func TestNewWriter(t *testing.T) {
	fd := storage.Fd{
		FileType: storage.KTableFile,
		Num:      rnd.Uint64() & 0xffffffff,
	}
	w, err := fs.NewAppendableFile(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	tableWriter := NewWriter(w, opt)

	kvs := randInputs(10000)
	for _, kv := range kvs {
		if err := tableWriter.Append(kv.key, kv.value); err != nil {
			t.Fatal(err)
		}
	}

	if err := tableWriter.Close(); err != nil {
		t.Fatal(err)
	}

	t.Logf("ffff")
}
