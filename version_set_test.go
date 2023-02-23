package leveldb

import (
	"os"
	"strconv"
	"testing"

	"github.com/KierenEinar/leveldb/comparer"
)

func Test_VersionBuilder(t *testing.T) {

	t.Run("test add and delete table file", func(t *testing.T) {

		vSet := initVersion(t)

		// del level 1 [1E, 1F], [1G, 1H], [1Q, 1R]
		// add level 1 [1EA, 1FA], [1GA, 1GH], [1GI, 1GZ], [1QA, 1QZ]
		edit := VersionEdit{}

		delTables := make([]delTable, 0)
		addTables := make([]addTable, 0)
		levelDelFd := [3]uint64{2, 3, 8}

		levelAddFn := func(level int) []addTable {
			return []addTable{
				{
					level:  level,
					number: uint64(level*1000 + 1),
					imin:   buildInternalKey(nil, []byte(strconv.Itoa(level)+"EA"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte(strconv.Itoa(level)+"FA"), keyTypeValue, 100),
				},
				{
					level:  level,
					number: uint64(level*1000 + 2),
					imin:   buildInternalKey(nil, []byte(strconv.Itoa(level)+"GA"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte(strconv.Itoa(level)+"GH"), keyTypeValue, 100),
				},
				{
					level:  level,
					number: uint64(level*1000 + 3),
					imin:   buildInternalKey(nil, []byte(strconv.Itoa(level)+"GI"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte(strconv.Itoa(level)+"GZ"), keyTypeValue, 100),
				},
				{
					level:  level,
					number: uint64(level*1000 + 4),
					imin:   buildInternalKey(nil, []byte(strconv.Itoa(level)+"QA"), keyTypeValue, 100),
					imax:   buildInternalKey(nil, []byte(strconv.Itoa(level)+"QZ"), keyTypeValue, 100),
				},
			}
		}

		for i := 1; i < KLevelNum; i++ {

			for _, fd := range levelDelFd {
				delTables = append(delTables, delTable{
					level:  i,
					number: uint64(i*10) + fd,
				})
			}

			addTables = append(addTables, levelAddFn(i)...)

		}

		edit.addedTables = addTables
		edit.delTables = delTables

		v := &Version{}
		vb := newBuilder(vSet, vSet.current)
		vb.apply(edit)
		vb.saveTo(v)
		vSet.current = v
		for level := 0; level < len(vSet.current.levels); level++ {
			tFiles := vSet.current.levels[level]
			for i := 0; i < len(tFiles); i++ {
				t.Logf("tFile==>fd[%d], imin[%s], imax[%s]",
					tFiles[i].fd, tFiles[i].iMin.userKey(), tFiles[i].iMax.userKey())
			}
		}
	})

}

func initVersion(t *testing.T) *VersionSet {

	opt, _ := sanitizeOptions(os.TempDir(), nil)
	vSet := &VersionSet{
		opt: opt,
	}

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
		}
	}

	return vSet

}

func Test_upperBound(t *testing.T) {
	type args struct {
		s     tFiles
		level int
		tFile *tFile
		cmp   comparer.BasicComparer
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "test level 0, sort by fd",
			args: args{
				s: tFiles{
					{
						fd: 10,
					},
					{
						fd: 8,
					},
					{
						fd: 6,
					},
				},
				level: 0,
				tFile: &tFile{
					fd: 12,
				},
				cmp: IComparer,
			},
			want: 0,
		},
		{
			name: "test level 1, sort by imax",
			args: args{
				s: tFiles{
					{
						iMin: buildInternalKey(nil, []byte("A"), keyTypeSeek, 1000),
						iMax: buildInternalKey(nil, []byte("B"), keyTypeSeek, 1000),
					},
					{
						iMin: buildInternalKey(nil, []byte("E"), keyTypeSeek, 1000),
						iMax: buildInternalKey(nil, []byte("F"), keyTypeSeek, 1000),
					},
					{
						iMin: buildInternalKey(nil, []byte("H"), keyTypeSeek, 1000),
						iMax: buildInternalKey(nil, []byte("J"), keyTypeSeek, 1000),
					},
				},
				level: 1,
				tFile: &tFile{
					iMin: buildInternalKey(nil, []byte("E"), keyTypeSeek, 1000),
					iMax: buildInternalKey(nil, []byte("EA"), keyTypeSeek, 1000),
				},
				cmp: IComparer,
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := upperBound(tt.args.s, tt.args.level, tt.args.tFile, tt.args.cmp); got != tt.want {
				t.Errorf("upperBound() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_vBuilder_maybeAddFile(t *testing.T) {

	opt, _ := sanitizeOptions(os.TempDir(), nil)

	type fields struct {
		vSet      *VersionSet
		base      *Version
		inserted  [KLevelNum]*tFileSortedSet
		deleted   [KLevelNum]*uintSortedSet
		deleteFds []uint64
	}
	type args struct {
		v     *Version
		file  *tFile
		level int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "test delete and add",
			fields: fields{
				vSet: &VersionSet{
					opt: opt,
				},
				deleted:   [7]*uintSortedSet{},
				deleteFds: []uint64{10, 1000},
			},
			args: args{
				v: &Version{},
				file: &tFile{
					fd: 100,
				},
				level: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.deleted[tt.args.level] = newUintSortedSet()
			for _, v := range tt.fields.deleteFds {
				tt.fields.deleted[tt.args.level].add(v)
			}
			builder := &vBuilder{
				vSet:     tt.fields.vSet,
				base:     tt.fields.base,
				inserted: tt.fields.inserted,
				deleted:  tt.fields.deleted,
			}
			builder.maybeAddFile(tt.args.v, tt.args.file, tt.args.level)
		})
	}
}
