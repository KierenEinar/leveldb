package leveldb

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/KierenEinar/leveldb/collections"
)

func TestVersionEdit_EncodeTo(t *testing.T) {
	vSet := initVersion(t)

	edit := &VersionEdit{}
	edit.setLastSeq(1000)
	edit.setCompareName([]byte("comparer.xx"))
	edit.setNextFile(100)
	edit.setLogNum(99)

	// add table
	for level, tFiles := range vSet.current.levels {
		for idx := range tFiles {
			edit.addTableFile(level, tFiles[idx])
		}
	}

	// del table
	for level := 0; level < KLevelNum; level++ {
		edit.addDelTable(level, uint64(level*10+7))
	}

	edit.addCompactPtr(3, buildInternalKey(nil, []byte(strconv.Itoa(3)+"QZ"), keyTypeValue, 100))

	lk := collections.NewLinkBlockBuffer(0)
	edit.EncodeTo(lk)

	serialEdit := &VersionEdit{}
	serialEdit.DecodeFrom(lk)

	t.Logf("edit=%v", edit)
	t.Logf("serialEdit=%v", serialEdit)
	if !reflect.DeepEqual(edit, serialEdit) {
		t.Fatalf("edit should eq serialEdit")
	}

}
