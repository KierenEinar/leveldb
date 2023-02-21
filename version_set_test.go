package leveldb

import (
	"os"
	"strconv"
	"testing"
)

func Test_VersionBuilder(t *testing.T) {

	opt, _ := sanitizeOptions(os.TempDir(), nil)

	t.Run("test add and delete table file", func(t *testing.T) {

		vSet := newVersionSet(opt)
		fatalInitVersion(t, vSet)

		// del level 1 [1E, 1F], [1G, 1H], [1Q, 1R]
		// add level 1 [1EA, 1FA], [1GA, 1GH], [1GI, 1GZ], [1QA, 1QZ]
		edit := VersionEdit{
			delTables: []delTable{
				{
					level:  1,
					number: 12,
				},
				{
					level:  1,
					number: 13,
				},
				{
					level:  1,
					number: 18,
				},
			},
			addedTables: []addTable{
				{
					level:  1,
					number: 1001,
					imin:   buildInternalKey(nil, []byte("1EA"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte("1FA"), keyTypeValue, 100),
				},
				{
					level:  1,
					number: 1002,
					imin:   buildInternalKey(nil, []byte("1GA"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte("1GH"), keyTypeValue, 100),
				},
				{
					level:  1,
					number: 1003,
					imin:   buildInternalKey(nil, []byte("1GI"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte("1GZ"), keyTypeValue, 100),
				},
				{
					level:  1,
					number: 1004,
					imin:   buildInternalKey(nil, []byte("1QA"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte("1QZ"), keyTypeValue, 100),
				},
			},
		}
		v := &Version{}
		vb := newBuilder(vSet, vSet.current)
		vb.apply(edit)
		vb.saveTo(v)
		vSet.current = v
		for i := 0; i < len(vSet.current.levels[1]); i++ {
			tfile := vSet.current.levels[1][i]
			t.Logf("tFile==>fd[%d], imin[%s], imax[%s]", tfile.fd, tfile.iMin.userKey(), tfile.iMax.userKey())
		}
	})

}

func fatalInitVersion(t *testing.T, vSet *VersionSet) {

	// each level design 10 sstable file
	var lastVer *Version
	for i := 0; i < KLevelNum; i++ {
		c := rune(65)
		step := 1

		for j := 0; j < 10; j++ {
			if i == 0 {
				step = 8
			}
			si := c
			ei := c + rune(step)
			s := strconv.Itoa(i) + string(si)
			e := strconv.Itoa(i) + string(ei)
			imin := buildInternalKey(nil, []byte(s), keyTypeValue, 100)
			imax := buildInternalKey(nil, []byte(e), keyTypeValue, 100)

			if i == 0 {
				c = (si + ei) / 2
			} else {
				c = ei + 1
			}

			t.Logf("level i=%d, min=%s, max=%s, number=%d", i, s, e, i*10+j)

			edit := VersionEdit{
				addedTables: []addTable{
					{
						level:  i,
						size:   10,
						number: uint64(i*10 + j),
						imin:   imin,
						imax:   imax,
					},
				},
			}

			v := &Version{}
			vb := newBuilder(vSet, lastVer)
			vb.apply(edit)
			vb.saveTo(v)
			lastVer = v
			vSet.current = v
			if v.levels[i][j].fd != int(edit.addedTables[0].number) {
				t.Fatalf("level[i][j] should eq ")
			}

		}
	}

}
