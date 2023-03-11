package logger

import (
	"os"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	t.Run("test info", func(t *testing.T) {
		Infof("test info, args=%s", "hello")
	})

	t.Run("test set up", func(t *testing.T) {
		Setup(Settings{
			Dir:        os.TempDir(),
			PrefixName: "LEVELDB",
			Ext:        "LOG",
		})

		Infof("test info, args=%s", "world")
		time.Sleep(time.Second)
	})

}
