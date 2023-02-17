package wal

import (
	"io"
)

type chunkDump struct {
	data []byte
	w    io.Writer
}

type blockDump struct {
	r      io.ReaderAt
	data   [kJournalBlockSize]byte
	offset int
}

func (blockDump *blockDump) readBlock() error {
	r := blockDump.r
	_ = r
	return nil
}
