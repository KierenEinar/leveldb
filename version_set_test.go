package leveldb

import (
	"os"
	"testing"
)

func Test_VersionBuilder(t *testing.T) {

	opt, _ := sanitizeOptions(os.TempDir(), nil)

	t.Run("test add and delete table file", func(t *testing.T) {

		vSet := newVersionSet(opt)

		vb := newBuilder(vSet, nil)

		// add level'0 new table
		edit := VersionEdit{
			addedTables: []addTable{
				{
					level:  0,
					size:   10,
					number: 9,
					imin:   buildInternalKey(nil, []byte("abyu"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte("zxya"), keyTypeValue, 100),
				},
			},
		}

		v := &Version{}

		vb.apply(edit)
		vb.saveTo(v)

		if v.levels[0][0].fd != int(edit.addedTables[0].number) {
			t.Fatalf("level[0][0] should eq ")
		}

		// add level'0 new table
		edit = VersionEdit{
			addedTables: []addTable{
				{
					level:  0,
					size:   10,
					number: 10,
					imin:   buildInternalKey(nil, []byte("oews"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte("yw"), keyTypeValue, 100),
				},
			},
		}

		v2 := &Version{}

		vb = newBuilder(vSet, v)
		vb.apply(edit)
		vb.saveTo(v2)

		if v2.levels[0][1].fd != int(edit.addedTables[0].number) {
			t.Fatalf("level[0][1] should eq ")
		}

		// add level'0 new table

		edit = VersionEdit{
			addedTables: []addTable{
				{
					level:  0,
					size:   10,
					number: 12,
					imin:   buildInternalKey(nil, []byte("rews"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte("uw"), keyTypeValue, 100),
				},
			},
		}

		v3 := &Version{}

		vb = newBuilder(vSet, v2)
		vb.apply(edit)
		vb.saveTo(v3)

		if v3.levels[0][2].fd != int(edit.addedTables[0].number) {
			t.Fatalf("level[0][2] should eq ")
		}

		// add level'0 new table
		edit = VersionEdit{
			addedTables: []addTable{
				{
					level:  0,
					size:   10,
					number: 12,
					imin:   buildInternalKey(nil, []byte("xews"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte("zw"), keyTypeValue, 100),
				},
			},
		}

		v4 := &Version{}

		vb = newBuilder(vSet, v3)
		vb.apply(edit)
		vb.saveTo(v4)

		if v4.levels[0][3].fd != int(edit.addedTables[0].number) {
			t.Fatalf("level[0][2] should eq ")
		}

	})

}
