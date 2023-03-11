package logger

import (
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	t.Run("test info", func(t *testing.T) {
		Infof("test info, args=%s", "hello")
		Infof("test info, args=%s", "world")
	})

	t.Run("test set up", func(t *testing.T) {
		Setup(Settings{
			Dir:        os.TempDir(),
			PrefixName: "LEVELDB",
			Ext:        "LOG",
		})

		Infof("test info, args=%s", "world1")
		Infof("test info, args=%s", "world2")
	})

}
