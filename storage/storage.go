package sstable

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"
)

type SequentialWriter interface {
	io.Writer
	io.Closer
	Syncer
}

type SequentialReader interface {
	io.ByteReader
	io.Reader
	io.Closer
}

type ReaderAt interface {
	io.ReaderAt
}

type Reader interface {
	SequentialReader
	ReaderAt
}

type Syncer interface {
	Sync() error
}

type Locker interface {
	UnLock()
}

type Storage interface {
	Lock() (Locker, error)

	// OpenDB ignore error
	OpenDB()

	// Open reader
	Open(fd Fd) (Reader, error)

	// Create Writer, if writer exists, then will truncate it
	Create(fd Fd) (SequentialWriter, error)

	// Remove remove fd
	Remove(fd Fd) error

	// Rename rename fd
	Rename(fd Fd) error

	SetCurrent(num uint64) error

	GetCurrent() (Fd, error)

	List() ([]Fd, error)

	Close() error
}

type FileStorage struct {
	dbPath string
	Storage
	fileLock FileLock
}

type FileLock interface {
	Release()
}

func OpenPath(dbPath string) (Storage, error) {

	err := os.MkdirAll(dbPath, 0644)
	if err != nil {
		return nil, err
	}

	fileLock, err := lockFile(path.Join(dbPath, "LOCK"))
	if err != nil {
		return nil, err
	}

	fs := &FileStorage{
		dbPath:   dbPath,
		fileLock: fileLock,
	}

	runtime.SetFinalizer(fs, (*FileStorage).Close)
	return fs, nil
}

func (fs *FileStorage) Close() error {
	fs.fileLock.Release()
	return nil
}

func (fs *FileStorage) SetCurrent(num uint64) (err error) {

	descriptorFile := path.Join(fs.dbPath, Fd{KDescriptorFile, num}.String())
	dbTmpFile := path.Join(fs.dbPath, Fd{KDBTempFile, num}.String())
	content := descriptorFile + "\n"

	tmp, err := os.OpenFile(dbTmpFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = os.Remove(dbTmpFile)
		}
	}()

	_, err = tmp.Write([]byte(content))
	if err != nil {
		return err
	}

	err = os.Rename(dbTmpFile, descriptorFile)
	if err != nil {
		return err
	}
	return nil
}

// GetCurrent if current path not exists, will return os.ErrNotExist
func (fs *FileStorage) GetCurrent() (fd Fd, err error) {

	current := path.Join(fs.dbPath, "CURRENT")
	fInfo, sErr := os.Stat(current)
	if sErr != nil {
		err = sErr
		return
	}

	if fInfo.IsDir() {
		err = ErrFileIsDir
		return
	}

	file, fErr := os.OpenFile(current, os.O_RDWR, 0644)
	if fErr != nil {
		err = fErr
		return
	}
	defer file.Close()

	content, rErr := ioutil.ReadAll(file)
	if rErr != nil {
		err = rErr
		return
	}

	if len(content) == 0 || !bytes.HasSuffix(content, []byte("\n")) {
		err = NewErrCorruption("invalid current file content")
		return
	}

	currentFd, parseErr := parseFd(string(content))
	if parseErr != nil {
		err = parseErr
		return
	}

	fd = currentFd

	return
}
