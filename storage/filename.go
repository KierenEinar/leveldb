package storage

import (
	"errors"
	"fmt"
	"strings"
)

type FileType int

const (
	KDescriptorFile FileType = 1 << iota
	KTableFile
	KJournalFile
	KCurrentFile
	KDBLockFile
	KDBTempFile
)

type Fd struct {
	FileType
	Num uint64
}

func (fd Fd) String() string {

	switch fd.FileType {
	case KDescriptorFile:
		return fmt.Sprintf("MANIFEST-%06d", fd.Num)
	case KJournalFile:
		return fmt.Sprintf("%06d.log", fd.Num)
	case KTableFile:
		return fmt.Sprintf("%06d.ldb", fd.Num)
	case KDBLockFile:
		return fmt.Sprintf("LOCK")
	case KCurrentFile:
		return fmt.Sprintf("CURRENT")
	case KDBTempFile:
		return fmt.Sprintf("%06d.dbtmp", fd.Num)
	default:
		return fmt.Sprintf("%x-%06d", fd.FileType, fd.Num)
	}
}

// ParseFd parse file fd
//  filename format
// CURRENT
// LOCK
// MANIFEST-%06d
// %06d.log
// %06d.ldb
// %06d.dbtmp
func ParseFd(fileName string) (fd Fd, err error) {

	if fileName == "CURRENT" {
		fd.FileType = KCurrentFile
	} else if fileName == "LOCK" {
		fd.FileType = KDBLockFile
	} else if strings.HasPrefix(fileName, "MANIFEST") {
		if _, sErr := fmt.Sscanf("MANIFEST-%06d", fileName, &fd.Num); sErr != nil {
			err = sErr
			return
		}
		fd.FileType = KDescriptorFile
	} else {

		var ft string

		_, sErr := fmt.Sscanf("%06d.%s", fileName, &fd.Num, &ft)
		if sErr != nil {
			err = sErr
			return
		}

		switch ft {
		case "ldb", "sst":
			fd.FileType = KTableFile
		case "log":
			fd.FileType = KJournalFile
		case "dbtmp":
			fd.FileType = KDBTempFile
		default:
			err = errors.New("undefined filetype")
		}
	}
	return
}

func FileLockFd() Fd {
	return Fd{
		FileType: KDBLockFile,
	}
}
