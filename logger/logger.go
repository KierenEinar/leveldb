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
	PrefixName string
	Ext        string
}

type loggerSettings struct {
	s          Settings
	fileLogger *log.Logger
}

const (
	Debug = iota
	Info
	Warn
	Error
)

var (
	logger       *log.Logger
	levelsPrefix = []string{"[level-Debug] ", "[level-Info] ", "[level-Warn] ", "[level-Error] "}
	logChan      = make(chan *bytes.Buffer, 0)
	closedChan   = make(chan struct{}, 0)
	resetLogChan = make(chan *loggerSettings, 0)
	bytesPool    sync.Pool
	fileLog      *os.File
	crlf         = "\r\n"
	settings     *Settings
	once         sync.Once
)

func init() {
	logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
	bytesPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(nil)
		},
	}
	go asyncFlush()
}

func Close() {
	closedChan <- struct{}{}
}

func Setup(settings Settings) {
	once.Do(func() {
		name := formatLogFileName(settings.PrefixName, time.Now().Format("2006-01-02"), settings.Ext)
		_ = os.MkdirAll(settings.Dir, 0644)
		filePath := filepath.Join(settings.Dir, name)
		fileLog = mustOpen(filePath)
		mw := io.MultiWriter(fileLog, os.Stdout)
		ls := &loggerSettings{
			s:          settings,
			fileLogger: log.New(mw, "", log.LstdFlags|log.Lshortfile),
		}
		resetLogChan <- ls
	})
}

func SetPrefix(prefix string) {
	logger.SetPrefix(prefix)
}

func printf(level int, format string, v ...interface{}) {
	buf := bytesPool.Get().(*bytes.Buffer)
	buf.WriteString(levelsPrefix[level])
	buf.WriteString(fmt.Sprintf(format, v...))
	buf.WriteString(crlf)
	logChan <- buf
}

func Debugf(format string, v ...interface{}) {
	printf(Debug, format, v...)
}

func Infof(format string, v ...interface{}) {
	printf(Info, format, v...)
}

func Warnf(format string, v ...interface{}) {
	printf(Warn, format, v...)
}

func Errorf(format string, v ...interface{}) {
	printf(Error, format, v...)
}

func asyncFlush() {
	nextRotateTime := time.Now().AddDate(0, 0, 1)
	nextRotateTimeZero := time.Date(nextRotateTime.Year(),
		nextRotateTime.Month(),
		nextRotateTime.Day(), 0, 0, 0, 0, time.Local)
	ticker := time.NewTicker(1)
	defer ticker.Stop()
	<-ticker.C
	d := -time.Since(nextRotateTimeZero)
	ticker.Reset(d)

	for {

		select {

		case ls := <-resetLogChan:
			logger = ls.fileLogger
			settings = &ls.s

		case buf := <-logChan:
			ok, err := rotateLogFileIfNeeded(nextRotateTimeZero)
			if err != nil {
				logger.Println(err)
			}
			if ok {
				newNextRotateTimeZero := nextRotateTimeZero.Add(time.Hour * 24)
				ticker.Reset(newNextRotateTimeZero.Sub(nextRotateTimeZero))
				nextRotateTimeZero = newNextRotateTimeZero
			}

			logger.Print(buf.String())
			buf.Reset()
			bytesPool.Put(buf)

		case <-ticker.C:
			ok, err := rotateLogFileIfNeeded(nextRotateTimeZero)
			if err != nil {
				logger.Println(err)
			}
			if ok {
				newNextRotateTimeZero := nextRotateTimeZero.Add(time.Hour * 24)
				ticker.Reset(newNextRotateTimeZero.Sub(nextRotateTimeZero))
				nextRotateTimeZero = newNextRotateTimeZero
			}

		case <-closedChan:
			return
		}
	}

}

func rotateLogFileIfNeeded(nextRotateTimeZero time.Time) (bool, error) {

	if settings == nil {
		return false, nil
	}

	if time.Since(nextRotateTimeZero) < 0 {
		return false, nil
	}

	dir := settings.Dir
	prefix := settings.PrefixName
	ext := settings.Ext

	rName := filepath.Join(dir, formatLogFileName(prefix, nextRotateTimeZero.Format("2006-01-02"), ext))

	newFileLog, err := os.OpenFile(rName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return false, err
	}

	mw := io.MultiWriter(os.Stdout, newFileLog)
	logger = log.New(mw, "", log.LstdFlags|log.Lshortfile)

	_ = fileLog.Close()

	return true, nil
}

func mustOpen(filePath string) *os.File {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		_, _ = os.Stdout.WriteString(err.Error())
		panic(err)
	}
	return file
}

func formatLogFileName(prefix string, timeFormatted string, ext string) string {
	return fmt.Sprintf("%s-%s.%s", prefix,
		timeFormatted,
		ext)
}
