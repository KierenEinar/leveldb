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
	fs, err := OpenPath(tmp)
	if err != nil {
		t.Fatal(err)
	}

	fd := Fd{
		FileType: KDBTempFile,
		Num:      1,
	}

	writer, err := fs.NewWritableFile(fd)
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

	reader, err := fs.NewRandomAccessReader(fd)
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

	err = fs.Remove(fd)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFileStorage_LockFile_UnLockFile(t *testing.T) {

	tmp := os.TempDir()
	t.Logf("tmpdir=%s", tmp)
	fs, err := OpenPath(tmp)
	if err != nil {
		t.Fatal(err)
	}

	fd := FileLockFd()

	fLock, err := fs.LockFile(fd)
	if err != nil {
		t.Fatal(err)
	}

	err = fs.UnLockFile(fLock)
	if err != nil {
		t.Fatal(err)
	}

	_ = fs.Close()

}

func TestFileStorage_SetCurrent_GetCurrent(t *testing.T) {
	tmp := os.TempDir()
	t.Logf("tmpdir=%s", tmp)
	fs, err := OpenPath(tmp)
	if err != nil {
		t.Fatal(err)
	}

	err = fs.SetCurrent(10)
	fd, err := fs.GetCurrent()
	if err != nil {
		t.Fatal(err)
	}

	if fd.Num != 10 {
		t.Fatalf("fd num not eq 10")
	}

	if fd.FileType != KDescriptorFile {
		t.Fatalf("fd filetype not eq KDescriptorFile")
	}

}
