package storage

import (
	"os"
	"path"
)

func mmapOpenFile() int {
	return -1
}

func maxOpenFile() int {
	return -1
}

func (fs *FileStorage) NewRandomAccessReader(fd Fd) (r RandomAccessReader, err error) {

	if err = fs.ref(); err != nil {
		return
	}

	file, fErr := os.OpenFile(path.Join(fs.dbPath, fd.String()), os.O_RDONLY, 0644)
	if fErr != nil {
		err = fErr
		return
	}
	r = file
	return
}
