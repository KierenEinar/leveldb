package leveldb

import (
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
			wantImin: buildInternalKey(nil, []byte("aa"), keyTypeValue, 1000),
			wantImax: buildInternalKey(nil, []byte("zz"), keyTypeValue, 6500),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotImin, gotImax := tt.tFiles.getRange1(tt.args.cmp)
			if !reflect.DeepEqual(gotImin, tt.wantImin) {
				t.Errorf("getRange1() gotImin = %s, want %s", string(gotImin), string(tt.wantImin))
			}
			if !reflect.DeepEqual(gotImax, tt.wantImax) {
				t.Errorf("getRange1() gotImax = %s, want %s", string(gotImax), string(tt.wantImax))
			}
		})
	}
}
