package table

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"io/ioutil"
	"os"
	"testing"

	"github.com/KierenEinar/leveldb/cache"
	"github.com/KierenEinar/leveldb/comparer"
	"github.com/KierenEinar/leveldb/filter"

	"github.com/KierenEinar/leveldb/storage"

	"github.com/KierenEinar/leveldb/options"
)

var (
	opt *options.Options
	fs  storage.Storage
	tmp string
)

func TestMain(m *testing.M) {
	tmp, _ = ioutil.TempDir(os.TempDir(), "abc")
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

	defer fs.Close()
	defer os.RemoveAll(tmp)

	m.Run()

}

func TestNewWriter(t *testing.T) {

	t.Logf("tmp=%s", tmp)

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

	kvs := randInputs(1000, 10000, true)
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

func TestBlockIter_Seek(t *testing.T) {

	bw := newBlockWriter(int(opt.BlockRestartInterval), opt.InternalComparer)
	tmp := make([]byte, 4)
	for i := uint32(1); i <= uint32(opt.BlockRestartInterval)*2; i++ {
		binary.LittleEndian.PutUint32(tmp, i)
		bw.append(tmp, tmp)
	}
	bw.finish()

	bc := blockContent{
		data:      bw.data.Bytes(),
		cacheable: false,
		poolable:  false,
	}

	db, err := newDataBlock(bc, opt.InternalComparer)
	if err != nil {
		t.Fatal(err)
	}
	bi := newBlockIter(db, opt.InternalComparer)
	defer bi.UnRef()

	// seek 16
	seekData := make([]byte, 4)
	binary.LittleEndian.PutUint32(seekData, uint32(16))
	if !bi.Seek(seekData) {
		t.Fatal("seek 16 failed")
	}
	if !bytes.Equal(bi.Key(), seekData) {
		t.Fatal("key not eq")
	}

	if !bytes.Equal(bi.Value(), seekData) {
		t.Fatal("value not eq")
	}

	// seek 17
	binary.LittleEndian.PutUint32(seekData, uint32(17))
	if !bi.Seek(seekData) {
		t.Fatal("seek 17 failed")
	}
	if !bytes.Equal(bi.Key(), seekData) {
		t.Fatal("key not eq")
	}

	if !bytes.Equal(bi.Value(), seekData) {
		t.Fatal("value not eq")
	}

	// seek 32

	binary.LittleEndian.PutUint32(seekData, uint32(32))
	if !bi.Seek(seekData) {
		t.Fatal("seek 32 failed")
	}
	if !bytes.Equal(bi.Key(), seekData) {
		t.Fatal("key not eq")
	}

	if !bytes.Equal(bi.Value(), seekData) {
		t.Fatal("value not eq")
	}

}
