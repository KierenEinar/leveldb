//go build: darwin || freebsd || linux
// +build darwin freebsd linux

package storage

import (
	"os"
	"runtime"
	"syscall"
)

type UnixFileLock struct {
	*os.File
}

func (fileLock *UnixFileLock) release() {
	setFileLock(fileLock.File, false)
	_ = fileLock.File.Close()
}

func (fileLock *UnixFileLock) name() string {
	return fileLock.Name()
}

func lockFile(path string) (FileLock, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	runtime.KeepAlive(file)
	if ok := setFileLock(file, true); !ok {
		_ = file.Close()
		return nil, err
	}
	fileLock := &UnixFileLock{
		File: file,
	}
	return fileLock, nil
}

func setFileLock(file *os.File, lock bool) bool {
	how := syscall.LOCK_UN
	if lock == true {
		how = syscall.LOCK_EX
	}
	if err := syscall.Flock(int(file.Fd()), how|syscall.LOCK_NB); err != nil {
		return false
	}
	return true
}
