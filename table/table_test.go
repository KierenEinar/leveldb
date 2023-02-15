package table

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/KierenEinar/leveldb/errors"

	"github.com/KierenEinar/leveldb/utils"

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
		if err := tableWriter.Append(kv.k, kv.v); err != nil {
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

	for bi.Next() {
		t.Logf("key=%v, v=%v", bi.Key(), bi.Value())
	}

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

func TestWriter_ApproximateSize(t *testing.T) {

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

	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	for _, c := range letters {
		s := byte(c)
		if err := tableWriter.Append([]byte{s}, []byte{s}); err != nil {
			t.Fatal(err)
		}
	}

	if err := tableWriter.Close(); err != nil {
		t.Fatal(err)
	}

	size := tableWriter.ApproximateSize()

	reader, err := fs.NewSequentialReader(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	data := make([]byte, 1000)

	n, err := reader.Read(data)
	if err != nil {
		t.Fatal(err)
	}

	if n != size {
		t.Fatalf("approsimate size not eq file size, approsimate size=%d, file size=%d", size, n)
	}

}

func TestReader_NewIterator(t *testing.T) {

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

	var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	letters = bytes.Repeat(letters, 1024*10)
	for _, c := range letters {
		if err := tableWriter.Append([]byte{c}, []byte{c}); err != nil {
			t.Fatal(err)
		}
	}

	if err := tableWriter.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := fs.NewRandomAccessReader(fd)
	if err != nil {
		t.Fatal(err)
	}

	tr, err := NewTableReader(opt, r, tableWriter.ApproximateSize(), fd.Num)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.UnRef()

	iter := tr.NewIterator()
	defer iter.UnRef()

	processed := 0
	for iter.Next() {
		if iter.Key()[0] == 'Z' {
			processed++
		}
	}

	if processed != 1024*10 {
		t.Fatalf("processed failed, expected=%d, actual=%d", 1024*10, processed)
	}

}

func TestReader_FindKey(t *testing.T) {
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

	const size = 6
	var inputSize = (1 << 22) / 16 // generate gte 4m input
	inputs := make([][]byte, inputSize)

	for ix := range inputs {
		inputs[ix] = []byte(utils.RandHexByLen(size))
	}

	sort.Slice(inputs, func(i, j int) bool {
		return bytes.Compare(inputs[i], inputs[j]) < 0
	})

	for ix := range inputs {
		if err = tableWriter.Append(inputs[ix], inputs[ix]); err != nil {
			t.Fatal(err)
		}
	}

	if err := tableWriter.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := fs.NewRandomAccessReader(fd)
	if err != nil {
		t.Fatal(err)
	}

	tr, err := NewTableReader(opt, r, tableWriter.ApproximateSize(), fd.Num)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.UnRef()

	for _, input := range inputs {
		_, err := tr.FindKey(input)
		if err != nil {
			t.Fatal(err)
		}
		//t.Logf("findkey=%s, input=%s", rKey, input)
	}

	_, err = tr.FindKey([]byte("a"))
	if err != errors.ErrNotFound {
		t.Fatal("find key err")
	}

	_, err = tr.FindKey([]byte("b"))
	if err != errors.ErrNotFound {
		t.Fatal("find key err")
	}

	_, err = tr.FindKey([]byte("c"))
	if err != errors.ErrNotFound {
		t.Fatal("find key err")
	}

}

func TestDump_Format(t *testing.T) {
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

	const size = 6
	var inputSize = 1024 // generate gte 1m input
	inputs := make([][]byte, inputSize)

	for ix := range inputs {
		inputs[ix] = []byte(utils.RandHexByLen(size))
	}

	sort.Slice(inputs, func(i, j int) bool {
		return bytes.Compare(inputs[i], inputs[j]) < 0
	})

	for ix := range inputs {
		if err = tableWriter.Append(inputs[ix], inputs[ix]); err != nil {
			t.Fatal(err)
		}
	}

	if err := tableWriter.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := fs.NewSequentialReader(fd)
	if err != nil {
		t.Fatal(err)
	}

	defer r.Close()

	dump := NewDump(r, os.Stdout)
	err = dump.Format()
	if err != nil {
		t.Fatal(err)
	}
}

func TestReader_Get(t *testing.T) {

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

	const size = 6
	var inputSize = 1024
	inputs := make([][]byte, inputSize)

	for ix := range inputs {
		inputs[ix] = []byte(utils.RandHexByLen(size))
	}

	sort.Slice(inputs, func(i, j int) bool {
		return bytes.Compare(inputs[i], inputs[j]) < 0
	})

	for ix := range inputs {
		if err = tableWriter.Append(inputs[ix], inputs[ix]); err != nil {
			t.Fatal(err)
		}
	}

	if err := tableWriter.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := fs.NewRandomAccessReader(fd)
	if err != nil {
		t.Fatal(err)
	}

	tr, err := NewTableReader(opt, r, tableWriter.ApproximateSize(), fd.Num)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.UnRef()

	for _, input := range inputs {
		value, err := tr.Get(input)
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(value, input) {
			t.Fatalf("value not eq key")
		}
		t.Logf("key=%s, value=%s", string(input), string(value))
	}

	_, err = tr.Get([]byte("hello world"))
	if err != errors.ErrNotFound {
		t.Fatal(err)
	}

}

func TestReader_Get_Dump(t *testing.T) {
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

	kvPairs := make([]kv, 0)
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			utils.ForeachLetter(1, func(idx int, c rune) {
				if c >= '0' && c <= '9' {
					return
				}
				s := string(c) + strconv.Itoa(i) + strconv.Itoa(j)
				key := []byte(s)
				value := append([]byte(nil), key...)
				kv := kv{
					k: key,
					v: value,
				}
				kvPairs = append(kvPairs, kv)
			})
		}
	}

	sort.Slice(kvPairs, func(i, j int) bool {
		return bytes.Compare(kvPairs[i].k, kvPairs[j].k) < 0
	})

	for _, kv := range kvPairs {
		if err := tableWriter.Append(kv.k, kv.v); err != nil {
			t.Fatal(err)
		}
	}

	if err := tableWriter.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := fs.NewRandomAccessReader(fd)
	if err != nil {
		t.Fatal(err)
	}

	tr, err := NewTableReader(opt, r, tableWriter.ApproximateSize(), fd.Num)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.UnRef()

	for _, kv := range kvPairs {
		v, err := tr.Get(kv.k)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(kv.k, v) != 0 {
			t.Fatal("value not eq")
		}
	}

	seqReader, _ := fs.NewSequentialReader(fd)
	defer seqReader.Close()

	dump := NewDump(seqReader, os.Stdout)

	dump.Format()

}
