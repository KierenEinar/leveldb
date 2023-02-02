package storage

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestFileStorage_WriteAndRead(t *testing.T) {

	tmp := os.TempDir()
	t.Logf("tmpdir=%s", tmp)
	stor, err := OpenPath(tmp)
	if err != nil {
		t.Fatal(err)
	}

	fd := Fd{
		FileType: KDBTempFile,
		Num:      1,
	}

	writer, err := stor.NewWritableFile(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	p := bytes.Repeat([]byte{'x'}, 65535)
	for i := 0; i < 1024; i++ {
		_, err := writer.Write(p)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = writer.Flush()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := stor.NewRandomAccessReader(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	scratch := make([]byte, bytes.MinRead)
	offset := int64(0)
	count := 0
	for {
		value, err := reader.Pread(scratch, offset)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		count += len(value)
		if err == io.EOF {
			break
		}
		offset = int64(count)
	}

	if count != 65535*1024 {
		t.Fatalf("read not eq write, count=%d, expected=%d", count, 65535*1024)
	}

	err = stor.Remove(fd)
	if err != nil {
		t.Fatal(err)
	}
}
