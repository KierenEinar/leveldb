package leveldb

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/KierenEinar/leveldb/collections"
)

func TestVersionEdit_Encode_Decode(t *testing.T) {
	vSet, lastEdit := recoverVersionSet(t)

	edit := newVersionEdit()
	edit.setLastSeq(lastEdit.lastSeq + 1000)
	edit.setNextFile(lastEdit.nextFileNum + 1000)
	edit.setLogNum(lastEdit.journalNum + 1000)

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

	serialEdit := newVersionEdit()
	serialEdit.DecodeFrom(lk)

	t.Logf("edit=%v", edit)
	t.Logf("serialEdit=%v", serialEdit)
	if !reflect.DeepEqual(edit, serialEdit) {
		t.Fatalf("edit should eq serialEdit")
	}

}
