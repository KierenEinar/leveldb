package leveldb

import (
	"os"
	"reflect"
	"testing"

	"github.com/KierenEinar/leveldb/comparer"
)

func Test_tFile_overlapped1(t *testing.T) {
	type fields struct {
		fd         int
		iMax       internalKey
		iMin       internalKey
		size       int
		allowSeeks int
	}
	type args struct {
		imin internalKey
		imax internalKey
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "args in (tfileMin, tFileMax)",
			fields: fields{
				iMin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 10000),
				iMax: buildInternalKey(nil, []byte("zc"), keyTypeSeek, 10500),
			},
			args: args{
				imin: buildInternalKey(nil, []byte("1"), keyTypeSeek, 20000),
				imax: buildInternalKey(nil, []byte("zb"), keyTypeSeek, 30000),
			},
			want: true,
		},
		{
			name: "args in [tfileMin, tFileMax)",
			fields: fields{
				iMin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 10000),
				iMax: buildInternalKey(nil, []byte("zc"), keyTypeSeek, 10500),
			},
			args: args{
				imin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 20000),
				imax: buildInternalKey(nil, []byte("zb"), keyTypeSeek, 30000),
			},
			want: true,
		},
		{
			name: "args in [tfileMin, tFileMax]",
			fields: fields{
				iMin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 10000),
				iMax: buildInternalKey(nil, []byte("zc"), keyTypeSeek, 10500),
			},
			args: args{
				imin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 20000),
				imax: buildInternalKey(nil, []byte("zc"), keyTypeSeek, 30000),
			},
			want: true,
		},
		{
			name: "imin<t.iMin and imax > t.iMax",
			fields: fields{
				iMin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 10000),
				iMax: buildInternalKey(nil, []byte("zc"), keyTypeSeek, 10500),
			},
			args: args{
				imin: buildInternalKey(nil, []byte("0ac"), keyTypeSeek, 20000),
				imax: buildInternalKey(nil, []byte("zca"), keyTypeSeek, 30000),
			},
			want: true,
		},
		{
			name: "imin>t.iMin and imax > t.iMax",
			fields: fields{
				iMin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 10000),
				iMax: buildInternalKey(nil, []byte("zc"), keyTypeSeek, 10500),
			},
			args: args{
				imin: buildInternalKey(nil, []byte("0b"), keyTypeSeek, 20000),
				imax: buildInternalKey(nil, []byte("zca"), keyTypeSeek, 30000),
			},
			want: true,
		},
		{
			name: "imin<t.iMin and imax < t.iMax",
			fields: fields{
				iMin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 10000),
				iMax: buildInternalKey(nil, []byte("zc"), keyTypeSeek, 10500),
			},
			args: args{
				imin: buildInternalKey(nil, []byte("0ay"), keyTypeSeek, 20000),
				imax: buildInternalKey(nil, []byte("zy"), keyTypeSeek, 30000),
			},
			want: true,
		},
		{
			name: "imin<t.iMin and imax = t.iMin",
			fields: fields{
				iMin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 10000),
				iMax: buildInternalKey(nil, []byte("zc"), keyTypeSeek, 10500),
			},
			args: args{
				imin: buildInternalKey(nil, []byte("0"), keyTypeSeek, 20000),
				imax: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 30000),
			},
			want: true,
		},
		{
			name: "imin<t.iMin and imax < t.iMin",
			fields: fields{
				iMin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 10000),
				iMax: buildInternalKey(nil, []byte("zc"), keyTypeSeek, 10500),
			},
			args: args{
				imin: buildInternalKey(nil, []byte("0"), keyTypeSeek, 20000),
				imax: buildInternalKey(nil, []byte("0a"), keyTypeSeek, 30000),
			},
			want: false,
		},
		{
			name: "imin>t.imax",
			fields: fields{
				iMin: buildInternalKey(nil, []byte("0az"), keyTypeSeek, 10000),
				iMax: buildInternalKey(nil, []byte("zc"), keyTypeSeek, 10500),
			},
			args: args{
				imin: buildInternalKey(nil, []byte("zd"), keyTypeSeek, 20000),
				imax: buildInternalKey(nil, []byte("zz"), keyTypeSeek, 30000),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tFile := tFile{
				fd:         tt.fields.fd,
				iMax:       tt.fields.iMax,
				iMin:       tt.fields.iMin,
				size:       tt.fields.size,
				allowSeeks: tt.fields.allowSeeks,
			}
			if got := tFile.overlapped1(tt.args.imin, tt.args.imax); got != tt.want {
				t.Errorf("overlapped1() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_tFiles_getRange1(t *testing.T) {
	type args struct {
		cmp comparer.Comparer
	}
	tests := []struct {
		name     string
		tFiles   tFiles
		args     args
		wantImin internalKey
		wantImax internalKey
	}{
		{
			name: "test a b c d not overlapped",
			tFiles: tFiles{
				{
					iMin: buildInternalKey(nil, []byte("aa"), keyTypeValue, 1000),
					iMax: buildInternalKey(nil, []byte("cc"), keyTypeValue, 1500),
				},
				{
					iMin: buildInternalKey(nil, []byte("dd"), keyTypeValue, 2000),
					iMax: buildInternalKey(nil, []byte("ff"), keyTypeValue, 2500),
				},
				{
					iMin: buildInternalKey(nil, []byte("gg"), keyTypeValue, 3000),
					iMax: buildInternalKey(nil, []byte("hh"), keyTypeValue, 3500),
				},
			},
			args: args{
				cmp: IComparer,
			},
			wantImin: buildInternalKey(nil, []byte("aa"), keyTypeValue, 1000),
			wantImax: buildInternalKey(nil, []byte("hh"), keyTypeValue, 3500),
		},
		{
			name: "test a b c is overlapped, d e f not overlapped",
			tFiles: tFiles{
				{
					iMin: buildInternalKey(nil, []byte("aa"), keyTypeValue, 1000),
					iMax: buildInternalKey(nil, []byte("ff"), keyTypeValue, 1500),
				},
				{
					iMin: buildInternalKey(nil, []byte("dd"), keyTypeValue, 2000),
					iMax: buildInternalKey(nil, []byte("ii"), keyTypeValue, 2500),
				},
				{
					iMin: buildInternalKey(nil, []byte("ee"), keyTypeValue, 3000),
					iMax: buildInternalKey(nil, []byte("zz"), keyTypeValue, 6500),
				},
				{
					iMin: buildInternalKey(nil, []byte("aa"), keyTypeValue, 4000),
					iMax: buildInternalKey(nil, []byte("bb"), keyTypeValue, 4500),
				},
				{
					iMin: buildInternalKey(nil, []byte("jj"), keyTypeValue, 5000),
					iMax: buildInternalKey(nil, []byte("ll"), keyTypeValue, 5500),
				},
				{
					iMin: buildInternalKey(nil, []byte("oo"), keyTypeValue, 800),
					iMax: buildInternalKey(nil, []byte("zz"), keyTypeValue, 950),
				},
			},
			args: args{
				cmp: IComparer,
			},
			wantImin: buildInternalKey(nil, []byte("aa"), keyTypeValue, 4000),
			wantImax: buildInternalKey(nil, []byte("zz"), keyTypeValue, 950),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotImin, gotImax := tt.tFiles.getRange1(tt.args.cmp)
			if !reflect.DeepEqual(gotImin, tt.wantImin) {
				t.Errorf("getRange1() gotImin = %s, want %s", debugInternalKey(gotImin), debugInternalKey(tt.wantImin))
			}
			if !reflect.DeepEqual(gotImax, tt.wantImax) {
				t.Errorf("getRange1() gotImax = %s, want %s", debugInternalKey(gotImax), debugInternalKey(tt.wantImax))
			}
		})
	}
}

func Test_tFiles_getOverlapped1(t *testing.T) {
	type args struct {
		dst        *tFiles
		imin       internalKey
		imax       internalKey
		overlapped bool
	}
	tests := []struct {
		name   string
		tFiles tFiles
		args   args
		want   tFiles
	}{
		{
			name: "level's 0 overlapped tfiles",
			tFiles: tFiles{
				{
					fd:   3,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("er"), keyTypeValue, 50),
					iMax: buildInternalKey(nil, []byte("zaa"), keyTypeValue, 99),
				},
			},
			args: args{
				dst:        &tFiles{},
				imin:       buildInternalKey(nil, []byte("k"), keyTypeValue, 50),
				imax:       buildInternalKey(nil, []byte("z"), keyTypeValue, 1000),
				overlapped: true,
			},
			want: tFiles{
				{
					fd:   3,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("er"), keyTypeValue, 50),
					iMax: buildInternalKey(nil, []byte("zaa"), keyTypeValue, 99),
				},
			},
		},
		{
			name: "level 1 tfiles not ovrlapped",
			tFiles: tFiles{
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
				{
					fd:   3,
					iMin: buildInternalKey(nil, []byte("w"), keyTypeValue, 50),
					iMax: buildInternalKey(nil, []byte("zaa"), keyTypeValue, 99),
				},
			},
			args: args{
				dst:        &tFiles{},
				imin:       buildInternalKey(nil, []byte("a"), keyTypeValue, 50),
				imax:       buildInternalKey(nil, []byte("t"), keyTypeValue, 1000),
				overlapped: false,
			},
			want: tFiles{
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
			},
		},
		{
			name: "level 1 tfiles not ovrlapped, param imin lt tfiles[0].imin, param imax lt tfiles[0].imin",
			tFiles: tFiles{
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
				{
					fd:   3,
					iMin: buildInternalKey(nil, []byte("w"), keyTypeValue, 50),
					iMax: buildInternalKey(nil, []byte("zaa"), keyTypeValue, 99),
				},
			},
			args: args{
				dst:        &tFiles{},
				imin:       buildInternalKey(nil, []byte("a"), keyTypeValue, 50),
				imax:       buildInternalKey(nil, []byte("ab"), keyTypeValue, 1000),
				overlapped: false,
			},
			want: tFiles{},
		},
		{
			name: "level 1 tfiles not ovrlapped, param imin lt tfiles[0].imin, param imax gt tfiles[1].imax but lt tfiles[2].imin",
			tFiles: tFiles{
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
				{
					fd:   3,
					iMin: buildInternalKey(nil, []byte("w"), keyTypeValue, 50),
					iMax: buildInternalKey(nil, []byte("zaa"), keyTypeValue, 99),
				},
			},
			args: args{
				dst:        &tFiles{},
				imin:       buildInternalKey(nil, []byte("a"), keyTypeValue, 50),
				imax:       buildInternalKey(nil, []byte("k"), keyTypeValue, 1000),
				overlapped: false,
			},
			want: tFiles{
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
			},
		},
		{
			name: "level 1 tfiles not ovrlapped, param imin lt tfiles[0].imin, param imax gt tfiles[1].imax but lt tfiles[2].imin",
			tFiles: tFiles{
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
				{
					fd:   3,
					iMin: buildInternalKey(nil, []byte("w"), keyTypeValue, 50),
					iMax: buildInternalKey(nil, []byte("zaa"), keyTypeValue, 99),
				},
			},
			args: args{
				dst:        &tFiles{},
				imin:       buildInternalKey(nil, []byte("a"), keyTypeValue, 50),
				imax:       buildInternalKey(nil, []byte("tt"), keyTypeValue, 1000),
				overlapped: false,
			},
			want: tFiles{
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
			},
		},
		{
			name: "level 1 tfiles not ovrlapped, param imin lt tfiles[0].imin, param imax gt tfiles[2].imin but lt tfiles[2].imax",
			tFiles: tFiles{
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
				{
					fd:   3,
					iMin: buildInternalKey(nil, []byte("w"), keyTypeValue, 50),
					iMax: buildInternalKey(nil, []byte("zaa"), keyTypeValue, 99),
				},
			},
			args: args{
				dst:        &tFiles{},
				imin:       buildInternalKey(nil, []byte("a"), keyTypeValue, 50),
				imax:       buildInternalKey(nil, []byte("wa"), keyTypeValue, 1000),
				overlapped: false,
			},
			want: tFiles{
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
				{
					fd:   3,
					iMin: buildInternalKey(nil, []byte("w"), keyTypeValue, 50),
					iMax: buildInternalKey(nil, []byte("zaa"), keyTypeValue, 99),
				},
			},
		},
		{
			name: "level 1 tfiles not ovrlapped, param imin gt tfiles[2].imax, param imax gt tfiles[2].imax",
			tFiles: tFiles{
				{
					fd:   1,
					iMin: buildInternalKey(nil, []byte("abc"), keyTypeDel, 1000),
					iMax: buildInternalKey(nil, []byte("ifg"), keyTypeValue, 2000),
				},
				{
					fd:   2,
					iMin: buildInternalKey(nil, []byte("kng"), keyTypeValue, 100),
					iMax: buildInternalKey(nil, []byte("t"), keyTypeValue, 200),
				},
				{
					fd:   3,
					iMin: buildInternalKey(nil, []byte("w"), keyTypeValue, 50),
					iMax: buildInternalKey(nil, []byte("zaa"), keyTypeValue, 99),
				},
			},
			args: args{
				dst:        &tFiles{},
				imin:       buildInternalKey(nil, []byte("zb"), keyTypeValue, 50),
				imax:       buildInternalKey(nil, []byte("zzz"), keyTypeValue, 1000),
				overlapped: false,
			},
			want: tFiles{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.tFiles.getOverlapped1(tt.args.dst, tt.args.imin, tt.args.imax, tt.args.overlapped)
			if !reflect.DeepEqual(*tt.args.dst, tt.want) {
				t.Errorf("getOverlapped1() act = %v, want = %v", *tt.args.dst, tt.want)
			}
		})
	}
}

func Test_compaction1_expand(t *testing.T) {

	opt, _ := sanitizeOptions(os.TempDir(), nil)

	t.Run("level'1 won't cause level'0 expand ", func(t *testing.T) {

		c := compaction1{
			inputs: [2]tFiles{
				// level'0, will output [fff, mmmm]
				{
					{
						iMin: buildInternalKey(nil, []byte("hhh"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("mmm"), keyTypeValue, 200),
					},
				},
				// level'1, will output [fffa, ooo]
				{},
			},
			levels: Levels{
				// level 0
				{
					{
						iMin: buildInternalKey(nil, []byte("hhh"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("mmm"), keyTypeValue, 200),
					},
					{
						iMin: buildInternalKey(nil, []byte("mmm"), keyTypeValue, 700),
						iMax: buildInternalKey(nil, []byte("mmmm"), keyTypeValue, 800),
					},
					{
						iMin: buildInternalKey(nil, []byte("a"), keyTypeValue, 500),
						iMax: buildInternalKey(nil, []byte("b"), keyTypeValue, 600),
					},
					{
						iMin: buildInternalKey(nil, []byte("fff"), keyTypeValue, 300),
						iMax: buildInternalKey(nil, []byte("jjj"), keyTypeValue, 400),
					},
				},
				// level 1
				{
					{
						iMin: buildInternalKey(nil, []byte("aaa"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("ff"), keyTypeValue, 200),
					},
					{
						iMin: buildInternalKey(nil, []byte("fffa"), keyTypeValue, 700),
						iMax: buildInternalKey(nil, []byte("mmm"), keyTypeValue, 800),
					},
					{
						iMin: buildInternalKey(nil, []byte("mmma"), keyTypeValue, 500),
						iMax: buildInternalKey(nil, []byte("ooo"), keyTypeValue, 600),
					},
					{
						iMin: buildInternalKey(nil, []byte("y"), keyTypeValue, 500),
						iMax: buildInternalKey(nil, []byte("z"), keyTypeValue, 600),
					},
				},
			},
			sourceLevel: 0,
			opt:         opt,
		}

		c.expand()

		wants0 := tFiles{
			{
				iMin: buildInternalKey(nil, []byte("hhh"), keyTypeValue, 100),
				iMax: buildInternalKey(nil, []byte("mmm"), keyTypeValue, 200),
			},
			{
				iMin: buildInternalKey(nil, []byte("mmm"), keyTypeValue, 700),
				iMax: buildInternalKey(nil, []byte("mmmm"), keyTypeValue, 800),
			},
			{
				iMin: buildInternalKey(nil, []byte("fff"), keyTypeValue, 300),
				iMax: buildInternalKey(nil, []byte("jjj"), keyTypeValue, 400),
			},
		}

		wants1 := tFiles{
			{
				iMin: buildInternalKey(nil, []byte("fffa"), keyTypeValue, 700),
				iMax: buildInternalKey(nil, []byte("mmm"), keyTypeValue, 800),
			},
			{
				iMin: buildInternalKey(nil, []byte("mmma"), keyTypeValue, 500),
				iMax: buildInternalKey(nil, []byte("ooo"), keyTypeValue, 600),
			},
		}

		if !reflect.DeepEqual(c.inputs[0], wants0) {
			t.Errorf("intputs[0] act = %v, want = %v", c.inputs[0], wants0)
		}

		if !reflect.DeepEqual(c.inputs[1], wants1) {
			t.Errorf("intputs[1] act = %v, want = %v", c.inputs[1], wants1)
		}
	})

	t.Run("level'1 will cause level'0 expand ", func(t *testing.T) {

		c := compaction1{
			inputs: [2]tFiles{
				// level'0, will output [fff, mmmm]
				{
					{
						iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("77"), keyTypeValue, 200),
					},
				},
				// level'1, will output [fffa, ooo]
				{},
			},
			levels: Levels{
				// level 0
				{
					{
						iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("77"), keyTypeValue, 200),
					},
					{
						iMin: buildInternalKey(nil, []byte("11"), keyTypeValue, 700),
						iMax: buildInternalKey(nil, []byte("88"), keyTypeValue, 800),
					},
					{
						iMin: buildInternalKey(nil, []byte("88"), keyTypeValue, 500),
						iMax: buildInternalKey(nil, []byte("881"), keyTypeValue, 600),
					},
					{
						iMin: buildInternalKey(nil, []byte("9"), keyTypeValue, 300),
						iMax: buildInternalKey(nil, []byte("9a"), keyTypeValue, 400),
					},
				},
				// level 1
				{
					{
						iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("55"), keyTypeValue, 200),
					},
					{
						iMin: buildInternalKey(nil, []byte("555"), keyTypeValue, 700),
						iMax: buildInternalKey(nil, []byte("666"), keyTypeValue, 800),
					},
					{
						iMin: buildInternalKey(nil, []byte("88"), keyTypeValue, 500),
						iMax: buildInternalKey(nil, []byte("999"), keyTypeValue, 600),
					},
					{
						iMin: buildInternalKey(nil, []byte("y"), keyTypeValue, 500),
						iMax: buildInternalKey(nil, []byte("z"), keyTypeValue, 600),
					},
				},
			},
			sourceLevel: 0,
			opt:         opt,
		}

		c.expand()

		wants0 := tFiles{
			{
				iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
				iMax: buildInternalKey(nil, []byte("77"), keyTypeValue, 200),
			},
			{
				iMin: buildInternalKey(nil, []byte("11"), keyTypeValue, 700),
				iMax: buildInternalKey(nil, []byte("88"), keyTypeValue, 800),
			},
			{
				iMin: buildInternalKey(nil, []byte("88"), keyTypeValue, 500),
				iMax: buildInternalKey(nil, []byte("881"), keyTypeValue, 600),
			},
			{
				iMin: buildInternalKey(nil, []byte("9"), keyTypeValue, 300),
				iMax: buildInternalKey(nil, []byte("9a"), keyTypeValue, 400),
			},
		}

		wants1 := tFiles{
			{
				iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
				iMax: buildInternalKey(nil, []byte("55"), keyTypeValue, 200),
			},
			{
				iMin: buildInternalKey(nil, []byte("555"), keyTypeValue, 700),
				iMax: buildInternalKey(nil, []byte("666"), keyTypeValue, 800),
			},
			{
				iMin: buildInternalKey(nil, []byte("88"), keyTypeValue, 500),
				iMax: buildInternalKey(nil, []byte("999"), keyTypeValue, 600),
			},
		}

		if !reflect.DeepEqual(c.inputs[0], wants0) {
			t.Errorf("intputs[0] act = %v, want = %v", c.inputs[0], wants0)
		}

		if !reflect.DeepEqual(c.inputs[1], wants1) {
			t.Errorf("intputs[1] act = %v, want = %v", c.inputs[1], wants1)
		}
	})

	t.Run("level'1 will cause level'0 expand, but intput[1] change, so level'0 won't expand again", func(t *testing.T) {

		c := compaction1{
			inputs: [2]tFiles{
				// level'0, will output [fff, mmmm]
				{
					{
						iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("77"), keyTypeValue, 200),
					},
				},
				// level'1, will output [fffa, ooo]
				{},
			},
			levels: Levels{
				// level 0
				{
					{
						iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("77"), keyTypeValue, 200),
					},
					{
						iMin: buildInternalKey(nil, []byte("11"), keyTypeValue, 700),
						iMax: buildInternalKey(nil, []byte("88"), keyTypeValue, 800),
					},
					{
						iMin: buildInternalKey(nil, []byte("88"), keyTypeValue, 500),
						iMax: buildInternalKey(nil, []byte("8881"), keyTypeValue, 600),
					},
					{
						iMin: buildInternalKey(nil, []byte("9"), keyTypeValue, 300),
						iMax: buildInternalKey(nil, []byte("9a"), keyTypeValue, 400),
					},
				},
				// level 1
				{
					{
						iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("55"), keyTypeValue, 200),
					},
					{
						iMin: buildInternalKey(nil, []byte("555"), keyTypeValue, 700),
						iMax: buildInternalKey(nil, []byte("666"), keyTypeValue, 800),
					},
					{
						iMin: buildInternalKey(nil, []byte("88"), keyTypeValue, 500),
						iMax: buildInternalKey(nil, []byte("9"), keyTypeValue, 600),
					},
					{
						iMin: buildInternalKey(nil, []byte("91"), keyTypeValue, 500),
						iMax: buildInternalKey(nil, []byte("z"), keyTypeValue, 600),
					},
				},
			},
			sourceLevel: 0,
			opt:         opt,
		}

		c.expand()

		wants0 := tFiles{
			{
				iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
				iMax: buildInternalKey(nil, []byte("77"), keyTypeValue, 200),
			},
			{
				iMin: buildInternalKey(nil, []byte("11"), keyTypeValue, 700),
				iMax: buildInternalKey(nil, []byte("88"), keyTypeValue, 800),
			},
			{
				iMin: buildInternalKey(nil, []byte("88"), keyTypeValue, 500),
				iMax: buildInternalKey(nil, []byte("8881"), keyTypeValue, 600),
			},
		}

		wants1 := tFiles{
			{
				iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
				iMax: buildInternalKey(nil, []byte("55"), keyTypeValue, 200),
			},
			{
				iMin: buildInternalKey(nil, []byte("555"), keyTypeValue, 700),
				iMax: buildInternalKey(nil, []byte("666"), keyTypeValue, 800),
			},
			{
				iMin: buildInternalKey(nil, []byte("88"), keyTypeValue, 500),
				iMax: buildInternalKey(nil, []byte("9"), keyTypeValue, 600),
			},
		}

		if !reflect.DeepEqual(c.inputs[0], wants0) {
			t.Errorf("intputs[0] act = %v, want = %v", c.inputs[0], wants0)
		}

		if !reflect.DeepEqual(c.inputs[1], wants1) {
			t.Errorf("intputs[1] act = %v, want = %v", c.inputs[1], wants1)
		}
	})

	t.Run("input[0] from level 1, input[1] from level 2", func(t *testing.T) {

		c := compaction1{
			inputs: [2]tFiles{
				{
					{
						iMin: buildInternalKey(nil, []byte("00a"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("22c"), keyTypeValue, 200),
					},
				},
				// level'1, will output [fffa, ooo]
				{},
			},
			levels: Levels{
				// level 0
				{},
				// level 1
				{
					{
						iMin: buildInternalKey(nil, []byte("00a"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("22c"), keyTypeValue, 200),
					},
					{
						iMin: buildInternalKey(nil, []byte("33a"), keyTypeValue, 700),
						iMax: buildInternalKey(nil, []byte("44a"), keyTypeValue, 800),
					},
					{
						iMin: buildInternalKey(nil, []byte("55a"), keyTypeValue, 500),
						iMax: buildInternalKey(nil, []byte("66a"), keyTypeValue, 600),
					},
					{
						iMin: buildInternalKey(nil, []byte("77a"), keyTypeValue, 300),
						iMax: buildInternalKey(nil, []byte("88a"), keyTypeValue, 400),
					},
				},
				// level 2
				{
					{
						iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
						iMax: buildInternalKey(nil, []byte("110"), keyTypeValue, 200),
					},
					{
						iMin: buildInternalKey(nil, []byte("11"), keyTypeValue, 700),
						iMax: buildInternalKey(nil, []byte("229"), keyTypeValue, 800),
					},
					{
						iMin: buildInternalKey(nil, []byte("22b"), keyTypeValue, 700),
						iMax: buildInternalKey(nil, []byte("550"), keyTypeValue, 800),
					},
				},
			},
			sourceLevel: 1,
			opt:         opt,
		}

		c.expand()

		wants0 := tFiles{
			{
				iMin: buildInternalKey(nil, []byte("00a"), keyTypeValue, 100),
				iMax: buildInternalKey(nil, []byte("22c"), keyTypeValue, 200),
			},
			{
				iMin: buildInternalKey(nil, []byte("33a"), keyTypeValue, 700),
				iMax: buildInternalKey(nil, []byte("44a"), keyTypeValue, 800),
			},
		}

		wants1 := tFiles{
			{
				iMin: buildInternalKey(nil, []byte("00"), keyTypeValue, 100),
				iMax: buildInternalKey(nil, []byte("110"), keyTypeValue, 200),
			},
			{
				iMin: buildInternalKey(nil, []byte("11"), keyTypeValue, 700),
				iMax: buildInternalKey(nil, []byte("229"), keyTypeValue, 800),
			},
			{
				iMin: buildInternalKey(nil, []byte("22b"), keyTypeValue, 700),
				iMax: buildInternalKey(nil, []byte("550"), keyTypeValue, 800),
			},
		}

		if !reflect.DeepEqual(c.inputs[0], wants0) {
			t.Errorf("intputs[0] act = %v, want = %v", c.inputs[0], wants0)
		}

		if !reflect.DeepEqual(c.inputs[1], wants1) {
			t.Errorf("intputs[1] act = %v, want = %v", c.inputs[1], wants1)
		}
	})

}
