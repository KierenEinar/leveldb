//go build: darwin || freebsd || linux
// +build darwin freebsd linux

package storage

import (
	"errors"
	"math"
	"os"
	"runtime"
	"syscall"
	"unsafe"

	"github.com/KierenEinar/leveldb/utils"
)

var gOpenReadOnlyFileLimit = -1

var gOpenMmapFileLimit = -1

type posixMMAPReadableFile struct {
	limiter *Limiter
	data    []byte
	fs      *FileStorage
}

func (r *posixMMAPReadableFile) Close() error {
	_ = r.fs.unRef()
	r.limiter.Release()
	runtime.SetFinalizer(r, nil)
	return syscall.Munmap(r.data)
}

func newPosixMMAPReadableFile(fs *FileStorage, fd int, fileSize int, limiter *Limiter) (*posixMMAPReadableFile, error) {

	if err := fs.ref(); err != nil {
		return nil, err
	}

	data, err := syscall.Mmap(fd, 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	runtime.KeepAlive(data)
	mmapReaderFile := &posixMMAPReadableFile{
		limiter: limiter,
		data:    data,
		fs:      fs,
	}
	runtime.SetFinalizer(mmapReaderFile, (*posixMMAPReadableFile).Close)
	return mmapReaderFile, nil
}

func (mmap *posixMMAPReadableFile) Pread(offset int64, result **[]byte, scratch *[]byte) (n int, err error) {
	if int(offset) > len(mmap.data) {
		err = errors.New("error mmap off position read out of bounds")
		return
	}
	length := len(*scratch)
	data := mmap.data[offset : offset+int64(length)]
	*result = &data
	n = len(data)
	return
}

type posixRandomAccessFileReader struct {
	limiter      *Limiter
	hasPermanent bool
	fd           int
	filePath     string
	fs           *FileStorage
}

func newPosixRandomAccessFileReader(fs *FileStorage, fd int, filePath string, limiter *Limiter) (*posixRandomAccessFileReader, error) {
	p := &posixRandomAccessFileReader{
		limiter:  limiter,
		filePath: filePath,
		fs:       fs,
	}
	if limiter.Acquire() {
		p.hasPermanent = true
		p.fd = fd
		err := fs.ref()
		if err != nil {
			return nil, err
		}
		return p, nil
	}

	p.hasPermanent = false
	p.fd = -1
	runtime.SetFinalizer(p, (*posixRandomAccessFileReader).Close)
	// this means, if resource exhaust then every read will open a new fd.
	return p, syscall.Close(fd)
}

func (r *posixRandomAccessFileReader) Close() error {
	_ = r.fs.unRef()
	if r.hasPermanent {
		utils.Assert(r.fd != -1)
		_ = r.fs.unRef()
		return syscall.Close(r.fd)
	}
	runtime.SetFinalizer(r, nil)
	return nil
}

func (r *posixRandomAccessFileReader) Pread(offset int64, result **[]byte, scratch *[]byte) (n int, err error) {
	fd := r.fd
	if !r.hasPermanent {
		oFd, oErr := syscall.Open(r.filePath, syscall.O_RDONLY, 0644)
		if oErr != nil {
			err = oErr
			return
		}
		fd = oFd
		defer func() {
			_ = syscall.Close(fd)
			r.fd = -1
		}()
	}
	runtime.KeepAlive(*scratch)
	n, err = syscall.Pread(fd, *scratch, offset)
	if err == nil {
		*result = scratch
	}
	return
}

func (fs *FileStorage) newRandomAccessFile(fileDesc Fd) (r RandomAccessReader, err error) {

	filePath := fs.filePath(fileDesc)

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	fd := file.Fd()

	if !fs.mmapLimiter.Acquire() {
		return newPosixRandomAccessFileReader(fs, int(fd), filePath, fs.fdLimiter)
	}

	fInfo, sErr := file.Stat()
	if sErr != nil {
		err = sErr
		fs.mmapLimiter.Release()
		return
	}

	mmapReadableFile, mErr := newPosixMMAPReadableFile(fs, int(fd), int(fInfo.Size()), fs.mmapLimiter)
	if mErr != nil {
		err = mErr
		fs.mmapLimiter.Release()
		return
	}
	r = mmapReadableFile
	return
}

func maxOpenFile() int {
	rlimit := new(syscall.Rlimit)
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, rlimit)
	if err != nil {
		gOpenReadOnlyFileLimit = 50
	} else if int(rlimit.Cur) == syscall.RLIM_INFINITY {
		gOpenReadOnlyFileLimit = math.MaxInt64
	} else {
		gOpenReadOnlyFileLimit = int(rlimit.Cur / 5)
	}
	return gOpenReadOnlyFileLimit
}

func mmapOpenFile() int {
	var p int
	if unsafe.Sizeof(p) == 8 {
		gOpenMmapFileLimit = 1000
	}
	return gOpenMmapFileLimit
}

func (fs *FileStorage) NewRandomAccessReader(fd Fd) (r RandomAccessReader, err error) {
	return fs.newRandomAccessFile(fd)
}
