package leveldb

import "testing"

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
