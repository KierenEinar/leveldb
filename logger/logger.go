package logger

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Settings struct {
	Dir        string
	Name       string
	Ext        string
	ThreshHold int // log file max threshhold bytes
	Closed     <-chan struct{}
}

const (
	Debug = iota
	Info
	Warn
	Error
)

const bufLen = 32 * 1024

var (
	logger  *log.Logger
	buf     = make([]byte, 0, bufLen)
	buffer  *bytes.Buffer
	mu      sync.Mutex
	writesN uint64
	levels  = []string{"Debug", "Info", "Warn", "Error"}
	crlf    = "\r\n"
)

func Setup(settings Settings) {
	name := fmt.Sprintf("%s.%s", settings.Name, settings.Ext)
	filePath := filepath.Join(settings.Dir, name)
	w := mustOpen(filePath)
	mw := io.MultiWriter(os.Stdout, w)
	buffer = bytes.NewBuffer(buf)
	logger = log.New(mw, "", log.LstdFlags|log.Lshortfile)
	go flush(settings.Closed)
}

func SetPrefix(prefix string) {
	mu.Lock()
	defer mu.Unlock()
	logger.SetPrefix(prefix)
}

func print(level int, format string, v ...interface{}) {
	mu.Lock()
	defer mu.Unlock()
	n1, _ := buffer.WriteString("[level-")
	n2, _ := buffer.WriteString(levels[level])
	n3, _ := buffer.WriteString("] ")
	n4, _ := buffer.WriteString(fmt.Sprintf(format, v))
	n5, _ := buffer.WriteString(crlf)
	writesN += uint64(n1 + n2 + n3 + n4 + n5)

}

func Debugf(format string, v ...interface{}) {
	print(Debug, format, v)
}

func Infof(format string, v ...interface{}) {
	print(Info, format, v)
}

func Warnf(format string, v ...interface{}) {
	print(Warn, format, v)
}

func Errorf(format string, v ...interface{}) {
	print(Error, format, v)
}

func flush(closedC <-chan struct{}) {
	ticker := time.NewTicker(time.Second * 1)
	for {
		select {
		case <-ticker.C:
			mu.Lock()
			if buffer.Len() > 0 {
				logger.Print(buffer.String())
			}
			mu.Unlock()
		case <-closedC:
			return
		}
	}
}

func rotate() {

}

func mustOpen(filePath string) *os.File {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		_, _ = os.Stdout.WriteString(err.Error())
		panic(err)
	}
	return file
}
