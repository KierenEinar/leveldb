package storage

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/KierenEinar/leveldb/errors"
	"github.com/KierenEinar/leveldb/utils"
)

type SequentialWriter interface {
	io.Writer
	io.Closer
	Syncer
	Flusher
}

type SequentialReader interface {
	io.Reader
	io.Closer
	io.ByteReader
}

type RandomAccessReader interface {
	// Pread return bytes into value, if bytes.Equal(scratch, value) means file reader,
	// other wise means data from mmap reader
	Pread(scratch []byte, offset int64) (value []byte, err error)
	io.Closer
}

type Flusher interface {
	Flush() error
}

type Syncer interface {
	Sync() error
}

type FileLock interface {
	name() string
	release()
}

type Storage interface {

	// LockFile using file system lock to lock
	LockFile(fd Fd) (FileLock, error)

	// UnLockFile using file system lock to unlock file lock
	UnLockFile(fLock FileLock) error

	NewAppendableFile(fd Fd) (SequentialWriter, error)

	NewWritableFile(fd Fd) (SequentialWriter, error)

	NewSequentialReader(fd Fd) (SequentialReader, error)

	NewRandomAccessReader(fd Fd) (RandomAccessReader, error)

	SetCurrent(num uint64) error

	GetCurrent() (Fd, error)

	// Remove remove fd
	Remove(fd Fd) error

	// Rename rename fd
	Rename(src Fd, target Fd) error

	List() ([]Fd, error)

	RemoveDir() error

	FileSize(fd Fd) (int64, error)

	Close() error
}

type FileStorage struct {
	dbPath    string
	lockTable map[string]FileLock

	mmapLimiter *Limiter
	fdLimiter   *Limiter

	mutex sync.RWMutex
	open  int32
}

func OpenPath(dbPath string) (Storage, error) {
	err := os.MkdirAll(dbPath, 0644)
	if err != nil {
		return nil, err
	}
	fs := &FileStorage{
		dbPath:      dbPath,
		mmapLimiter: NewLimiter(int32(mmapOpenFile())),
		fdLimiter:   NewLimiter(int32(maxOpenFile())),
		lockTable:   make(map[string]FileLock),
	}
	runtime.SetFinalizer(fs, (*FileStorage).Close)
	return fs, nil
}

func (fs *FileStorage) Close() error {

	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	if fs.open == -1 {
		return errors.ErrClosed
	}
	if fs.open > 0 {
		// todo warming log
	}
	fs.open = -1
	runtime.SetFinalizer(fs, nil)
	return nil
}

func (fs *FileStorage) SetCurrent(num uint64) (err error) {

	descriptorFile := Fd{KDescriptorFile, num}.String()
	dbTmpFile := path.Join(fs.dbPath, Fd{KDBTempFile, num}.String())
	content := descriptorFile + "\n"
	current := path.Join(fs.dbPath, "CURRENT")
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

	err = os.Rename(dbTmpFile, current)
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
		err = errors.ErrFileIsDir
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
		err = errors.NewErrCorruption("invalid current file content")
		return
	}

	currentFd, parseErr := ParseFd(string(bytes.TrimRight(content, "\n")))
	if parseErr != nil {
		err = parseErr
		return
	}

	fd = currentFd

	return
}

type Limiter struct {
	allowsAcquired int32
}

func NewLimiter(allowsAcquired int32) *Limiter {
	return &Limiter{allowsAcquired: allowsAcquired}
}

func (l *Limiter) Acquire() bool {
	s := atomic.AddInt32(&l.allowsAcquired, -1)
	if s >= 0 {
		return true
	}
	preIncrement := atomic.AddInt32(&l.allowsAcquired, 1)
	utils.Assert(preIncrement < l.allowsAcquired)
	return false
}

func (l *Limiter) Release() {
	s := atomic.AddInt32(&l.allowsAcquired, 1)
	utils.Assert(s > 0)
}

const kWritableBufferSize = 1 << 16

type WritableFile struct {
	file       *os.File
	buf        [kWritableBufferSize]byte
	pos        int
	isManifest bool
	dbPath     string
	fd         Fd
	fs         *FileStorage
}

func (w *WritableFile) Write(p []byte) (n int, err error) {
	return w.append(p)
}

func (w *WritableFile) Flush() (err error) {
	_, err = w.flushBuffer()
	return
}

func (w *WritableFile) Sync() (err error) {
	err = w.syncDirIfIsManifest()
	if err != nil {
		return
	}

	_, err = w.flushBuffer()
	if err != nil {
		return
	}

	return w.file.Sync()
}

func (w *WritableFile) Close() (err error) {
	_ = w.fs.unRef()
	runtime.SetFinalizer(w, nil)
	return w.file.Close()
}

func (w *WritableFile) syncDirIfIsManifest() (err error) {
	if !w.isManifest {
		return
	}
	dir, err := os.Open(w.dbPath)
	if err != nil {
		return
	}
	defer dir.Close()
	err = dir.Sync()
	return
}

func newWritableFile(fs *FileStorage, file *os.File, dbPath string, fd Fd) *WritableFile {
	w := &WritableFile{
		file:       file,
		pos:        0,
		dbPath:     dbPath,
		fd:         fd,
		isManifest: fd.FileType == KDescriptorFile,
		fs:         fs,
	}

	runtime.SetFinalizer(w, (*WritableFile).Close)

	return w
}

func (w *WritableFile) append(p []byte) (n int, err error) {

	writeSize := len(p)
	n0 := copy(w.buf[w.pos:], p)
	w.pos += n0

	n += n0

	if n0 == writeSize {
		return
	}

	p = p[n:]

	// buf is full and flush it
	_, err = w.flushBuffer()
	if err != nil {
		return
	}

	// small write into buf
	if len(p) <= kWritableBufferSize {
		n1 := copy(w.buf[w.pos:], p)
		w.pos += n1
		n += n1
		return
	}

	// big write into filesystem
	n2, err := w.writeData(p)
	n += n2
	return
}

func (w *WritableFile) flushBuffer() (n int, err error) {
	if w.pos == 0 {
		return
	}
	n, err = w.file.Write(w.buf[:w.pos])
	if err == nil {
		w.pos = 0
	}
	return
}

func (w *WritableFile) writeData(p []byte) (n int, err error) {
	n0, err := w.flushBuffer()
	if err != nil {
		n = n0
		return
	}
	n1, err := w.file.Write(p)
	if err == nil {
		n = n0 + n1
	}
	return
}

type FileWrapper struct {
	*os.File
	fs      *FileStorage
	byteBuf [1]byte
}

func (fw *FileWrapper) Close() error {
	_ = fw.fs.unRef()
	runtime.SetFinalizer(fw, nil)
	return fw.File.Close()
}

func (fs *FileStorage) NewAppendableFile(fd Fd) (w SequentialWriter, err error) {

	if err = fs.ref(); err != nil {
		return
	}

	file, fErr := os.OpenFile(fs.filePath(fd), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if fErr != nil {
		err = fErr
		return
	}
	w = newWritableFile(fs, file, fs.dbPath, fd)
	return
}

func (fs *FileStorage) NewWritableFile(fd Fd) (w SequentialWriter, err error) {
	if err = fs.ref(); err != nil {
		return
	}

	file, fErr := os.OpenFile(fs.filePath(fd), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if fErr != nil {
		err = fErr
		return
	}
	w = newWritableFile(fs, file, fs.dbPath, fd)
	return
}

func (fs *FileStorage) NewSequentialReader(fd Fd) (r SequentialReader, err error) {

	if err = fs.ref(); err != nil {
		return
	}

	file, fErr := os.OpenFile(fs.filePath(fd), os.O_RDONLY, 0644)
	if fErr != nil {
		err = fErr
		return
	}

	r = &FileWrapper{
		File: file,
		fs:   fs,
	}

	runtime.SetFinalizer(r, (*FileWrapper).Close)

	return
}

func (fw *FileWrapper) ReadByte() (b byte, err error) {
	n, err := fw.File.Read(fw.byteBuf[:])
	if err != nil {
		return
	}
	if n == 0 {
		err = io.EOF
		return
	}
	if n > 0 {
		b = fw.byteBuf[0]
	}
	return
}

func (fs *FileStorage) filePath(fd Fd) string {
	return path.Join(fs.dbPath, fd.String())
}

func (fs *FileStorage) Remove(fd Fd) error {
	return os.Remove(fs.filePath(fd))
}

func (fs *FileStorage) Rename(src Fd, target Fd) error {
	srcPath := fs.filePath(src)
	targetPath := fs.filePath(target)
	return os.Rename(srcPath, targetPath)
}

func (fs *FileStorage) List() (fds []Fd, err error) {

	entries, err := os.ReadDir(fs.dbPath)
	if err != nil {
		return
	}

	for _, entry := range entries {
		fd, pErr := ParseFd(entry.Name())
		if pErr != nil {
			continue
		}
		fds = append(fds, fd)
	}

	return
}

func (fs *FileStorage) RemoveDir() (err error) {
	filePath := path.Join(fs.dbPath)
	fInfo, err := os.Stat(filePath)
	if err != nil {
		return
	}
	if !fInfo.IsDir() {
		err = &os.PathError{
			Op:   "RemoveDir",
			Path: filePath,
			Err:  fmt.Errorf("path not dir"),
		}
		return
	}

	return os.RemoveAll(filePath)
}

func (fs *FileStorage) FileSize(fd Fd) (int64, error) {
	filePath := path.Join(fs.dbPath, fd.String())
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}

	if info.IsDir() {
		return 0, &os.PathError{
			Op:   "ApproximateSize",
			Path: filePath,
			Err:  fmt.Errorf("get file size err, is dir"),
		}
	}

	return info.Size(), nil

}

func (fs *FileStorage) LockFile(fd Fd) (FileLock, error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	lockFilePath := path.Join(fs.dbPath, fd.String())

	if _, ok := fs.lockTable[lockFilePath]; ok {
		return nil, errors.ErrLocked
	}
	fLock, err := lockFile(lockFilePath)
	if err == nil {
		fs.lockTable[lockFilePath] = fLock
		fs.open++
		return fLock, nil
	}
	return nil, err
}

func (fs *FileStorage) UnLockFile(fLock FileLock) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	if fLock == nil || fs.lockTable == nil {
		return errors.ErrNotLocked
	}

	lockFilePath := fLock.name()
	if fLock, ok := fs.lockTable[lockFilePath]; !ok {
		return errors.ErrNotLocked
	} else {
		fLock.release()
		fs.open--
		delete(fs.lockTable, lockFilePath)
		return nil
	}
}

func (fs *FileStorage) ref() (err error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	if fs.open < 0 {
		err = errors.ErrClosed
	} else {
		fs.open++
	}
	if err != nil {
		return
	}
	return
}

func (fs *FileStorage) unRef() (err error) {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	if fs.open < 0 {
		err = errors.ErrClosed
	} else {
		fs.open--
	}
	if err != nil {
		return
	}
	return
}
