package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io/ioutil"
	"os"
	"testing"

	"github.com/KierenEinar/leveldb/storage"
)

func TestMain(m *testing.M) {

	m.Run()

}

func Test_BasicWrite_Read(t *testing.T) {

	tmpDir, _ := ioutil.TempDir(os.TempDir(), "")
	fs, err := storage.OpenPath(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	defer fs.Close()
	defer os.RemoveAll(tmpDir)

	fd := storage.Fd{
		FileType: storage.KJournalFile,
		Num:      1,
	}

	writer, err := fs.NewWritableFile(fd)
	if err != nil {
		t.Fatal(err)
	}

	jw := NewJournalWriter(writer)
	defer jw.Close()

	t.Run("write full block", func(t *testing.T) {

		reader, err := fs.NewRandomAccessReader(fd)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()

		chunk := bytes.Repeat([]byte{'x'}, kJournalBlockSize-7)
		_, err = jw.Write(chunk)
		if err != nil {
			t.Fatal(err)
		}

		if err := verifyChunk(chunk, reader, 0, kRecordFull); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("write across block", func(t *testing.T) {

		chunk1 := bytes.Repeat([]byte{'x'}, kJournalBlockSize-14)
		_, err = jw.Write(chunk1)
		if err != nil {
			t.Fatal(err)
		}

		reader, err := fs.NewRandomAccessReader(fd)
		if err != nil {
			t.Fatal(err)
		}

		if err := verifyChunk(chunk1, reader, 0, kRecordFull); err != nil {
			t.Fatal(err)
		}

		reader.Close()

		/**
			   chunk1                    chunk2
		/-----------------------/-------------------------/
			   32kb                       32kb
		**/
		writeOffset := kJournalBlockSize
		l := kJournalBlockSize*2 + 1000
		chunk2 := bytes.Repeat([]byte{'x'}, l)

		_, err = jw.Write(chunk2)
		if err != nil {
			t.Fatal(err)
		}

		reader, err = fs.NewRandomAccessReader(fd)
		if err != nil {
			t.Fatal(err)
		}

		defer reader.Close()

		dataOffset := 0
		for idx := 0; l > 0; idx++ {
			writeLen := l
			if l >= kJournalBlockSize-kJournalBlockHeaderLen {
				writeLen = kJournalBlockSize - kJournalBlockHeaderLen
			}
			l -= writeLen
			chunkType := kRecordFirst
			if idx > 0 {
				if l > 0 {
					chunkType = kRecordMiddle
				} else {
					chunkType = kRecordLast
				}
			}

			if err := verifyChunk(chunk2[dataOffset:dataOffset+writeLen], reader, int64(writeOffset), chunkType); err != nil {
				t.Fatal(err)
			}
			dataOffset += writeLen
			writeOffset += kJournalBlockHeaderLen + writeLen
		}

	})

}

func verifyChunk(data []byte, r storage.RandomAccessReader, offset int64, chunkType byte) error {

	p := make([]byte, len(data)+kJournalBlockHeaderLen)

	rData, err := r.Pread(p, offset)
	if err != nil {
		return err
	}

	checkSum1 := crc32.ChecksumIEEE(data)
	checkSum2 := crc32.ChecksumIEEE(rData[kJournalBlockHeaderLen:])

	if checkSum1 != checkSum2 {
		return errors.New("check sum failed")
	}

	dataLen := binary.LittleEndian.Uint16(rData[4:])
	if dataLen != uint16(len(data)) {
		return errors.New("data len")
	}

	if chunkType != rData[6] {
		return errors.New("chunk type")
	}

	if !bytes.Equal(data, rData[kJournalBlockHeaderLen:]) {
		return errors.New("data")
	}

	return nil

}
