package leveldb

import (
	"bytes"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/KierenEinar/leveldb/comparer"

	"github.com/KierenEinar/leveldb/wal"

	"github.com/KierenEinar/leveldb/options"

	"github.com/KierenEinar/leveldb/storage"
)

func Test_VersionBuilder(t *testing.T) {

	t.Run("test add and delete table file", func(t *testing.T) {

		vSet, _ := recoverVersionSet(t)
		defer vSet.opt.Storage.RemoveDir()

		// del level 1 [1E, 1F], [1G, 1H], [1Q, 1R]
		// add level 1 [1EA, 1FA], [1GA, 1GH], [1GI, 1GZ], [1QA, 1QZ]
		edit := newVersionEdit()

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
		baseFd := 100
		for i := 1; i < KLevelNum; i++ {

			for _, fd := range levelDelFd {
				delTables = append(delTables, delTable{
					level:  i,
					number: uint64(baseFd+i*10) + fd,
				})
			}

			addTables = append(addTables, levelAddFn(i)...)

		}

		edit.addedTables = addTables
		edit.delTables = delTables

		v := newVersion(vSet)
		vb := newBuilder(vSet, vSet.current)
		vb.apply(*edit)
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

func TestVersionSet_logAndApply(t *testing.T) {

	vSet, _ := recoverVersionSet(t)
	mu := sync.RWMutex{}
	mu.Lock()
	defer mu.Unlock()
	// delete manifest file
	defer vSet.opt.Storage.Remove(vSet.manifestFd)
	baseFd := 100
	t.Run("test add level 0 file", func(t *testing.T) {

		edit := newVersionEdit()
		tfile := tFile{
			fd:   baseFd + 70,
			iMin: buildInternalKey(nil, []byte("6AA"), keyTypeValue, 100),
			iMax: buildInternalKey(nil, []byte("6DD"), keyTypeValue, 100),
		}
		edit.addTableFile(0, &tfile)
		err := vSet.logAndApply(edit, &mu)
		if err != nil {
			t.Fatalf("add level 0 should not have err")
		}

	})

}

func TestVersionSet_recover(t *testing.T) {

	vSet, edit := recoverVersionSet(t)
	defer vSet.opt.Storage.RemoveDir()

	if vSet.nextFileNum != edit.nextFileNum {
		t.Fatalf("vSet.nextFileNum=%d should eq edit.nextFileNum=%d", vSet.nextFileNum, edit.nextFileNum)
	}

	if vSet.stJournalNum != edit.journalNum {
		t.Fatalf("vSet.stJournalNum=%d should eq edit.journalNum=%d", vSet.stJournalNum, edit.journalNum)
	}

	if vSet.stSeqNum != edit.lastSeq {
		t.Fatalf("vSet.stSeqNum=%d should eq edit.lastSeq=%d", vSet.stSeqNum, edit.lastSeq)
	}

	current := vSet.getCurrent()
	current.Ref()
	defer current.UnRef()

	liveFiles := make(map[uint64]*tFile)
	vSet.addLiveFilesForTest(liveFiles)

	for _, v := range edit.addedTables {
		tf, ok := liveFiles[v.number]
		if ok && bytes.Equal(tf.iMin, v.imin) && bytes.Equal(tf.iMax, v.imax) {
			delete(liveFiles, v.number)
		}
	}

	if len(liveFiles) != 0 {
		t.Fatal("vSet.liveFiles should empty")
	}

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

func initVersionEdit(t *testing.T, opt *options.Options) *VersionEdit {

	baseTableFd := 100

	edit := newVersionEdit()

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
			imin := buildInternalKey(nil, []byte(s), keyTypeValue, sequence(100*i+j))
			imax := buildInternalKey(nil, []byte(e), keyTypeValue, sequence(100*i+j+1))

			if i == 0 {
				c = (si + ei) / 2
			} else {
				c = ei + 1
			}

			tFile := &tFile{
				fd:   baseTableFd,
				iMax: imax,
				iMin: imin,
			}

			t.Logf("level i=%d, min=%s, minseq=%d, max=%s, maxseq=%d, number=%d", i, imin.userKey(),
				imin.seq(), imax.userKey(), imax.seq(), tFile.fd)

			edit.addTableFile(i, tFile)

			if j == 5 {
				edit.addCompactPtr(i, imax)
			}

			baseTableFd = baseTableFd + 1
		}
	}

	lastTable := edit.addedTables[len(edit.addedTables)-1]

	edit.setLastSeq(lastTable.imax.seq())
	edit.setLogNum(99)
	edit.setNextFile(lastTable.number + 1)
	edit.setCompareName(opt.InternalComparer.Name())

	return edit

}

func initOpt(t *testing.T) *options.Options {

	tmpDir, _ := ioutil.TempDir(os.TempDir(), "")
	opt, err := sanitizeOptions(tmpDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	return opt
}

func initManifest(t *testing.T, opt *options.Options) (storage.Fd, *VersionEdit) {

	edit := initVersionEdit(t, opt)

	fd := storage.Fd{
		FileType: storage.KDescriptorFile,
		Num:      98,
	}

	w, err := opt.Storage.NewAppendableFile(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	jw := wal.NewJournalWriter(w, true)
	edit.EncodeTo(jw)
	_ = w.Flush()
	return fd, edit
}

func recoverVersionSet(t *testing.T) (*VersionSet, *VersionEdit) {
	opt := initOpt(t)
	manifestFd, edit := initManifest(t, opt)
	vSet := newVersionSet(opt)
	err := vSet.recover(manifestFd)
	if err != nil {
		t.Fatal(err)
	}
	return vSet, edit
}
