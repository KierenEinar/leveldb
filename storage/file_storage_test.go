package storage

import (
	"bytes"
	"io"
	"io/ioutil"
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
	defer fs.Close()
	fd := FileLockFd()

	fLock, err := fs.LockFile(fd)
	if err != nil {
		t.Fatal(err)
	}

	err = fs.UnLockFile(fLock)
	if err != nil {
		t.Fatal(err)
	}

}

func TestFileStorage_SetCurrent_GetCurrent(t *testing.T) {
	tmp := os.TempDir()
	t.Logf("tmpdir=%s", tmp)
	fs, err := OpenPath(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
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

func TestFileStorage_List(t *testing.T) {

	tmp, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("tmpdir=%s", tmp)
	defer os.Remove(tmp)

	fs, err := OpenPath(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	fds := []Fd{
		{
			FileType: KDescriptorFile,
			Num:      1,
		},
		{
			FileType: KJournalFile,
			Num:      2,
		},
		{
			FileType: KDBTempFile,
			Num:      3,
		},
	}

	fds = append(fds, FileLockFd())
	fds = append(fds, CurrentFd())

	pendings := make(map[string]struct{})

	for _, fd := range fds {
		w, _ := fs.NewWritableFile(fd)
		_ = w.Close()
		pendings[fd.String()] = struct{}{}
	}

	getFds, err := fs.List()
	if err != nil {
		t.Fatal(err)
	}

	for _, fd := range getFds {
		delete(pendings, fd.String())
	}

	if len(pendings) > 0 {
		t.Fatalf("list failed, pending=%v", pendings)
	}

	fs.RemoveDir()

}

func TestFileStorage_NewAppendableFile(t *testing.T) {
	tmp, _ := os.MkdirTemp(os.TempDir(), "")
	defer os.RemoveAll(tmp)
	t.Logf("tmpdir=%s", tmp)
	fs, err := OpenPath(tmp)
	if err != nil {
		t.Fatal(err)
	}

	fd := Fd{
		FileType: KDBTempFile,
		Num:      10855,
	}

	writer, err := fs.NewAppendableFile(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	p := bytes.Repeat([]byte{'x'}, 446)
	_, err = writer.Write(p)

	err = writer.Sync()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := fs.NewSequentialReader(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	data := make([]byte, 1024)
	n, _ := reader.Read(data)

	if n != 446 {
		t.Fatal("writer write failed")
	}

}
