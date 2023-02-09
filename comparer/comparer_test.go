package comparer

import (
	"reflect"
	"testing"
)

func TestBytesComparer_Separator(t *testing.T) {
	type args struct {
		a []byte
		b []byte
	}
	tests := []struct {
		name     string
		args     args
		wantDest []byte
	}{
		{
			name: "test a len eq b len, and b last char - a last char > 1",
			args: args{
				a: []byte("abc"),
				b: []byte("abe"),
			},
			wantDest: []byte("abd"),
		},
		{
			name: "test a len eq b len, and b last char - a last char = 1",
			args: args{
				a: []byte("abc"),
				b: []byte("abd"),
			},
			wantDest: []byte("abc"),
		},
		{
			name: "test a is the prefix of b",
			args: args{
				a: []byte("abc"),
				b: []byte("abcd"),
			},
			wantDest: []byte("abc"),
		},
		{
			name: "test a len gt b",
			args: args{
				a: []byte("888888"),
				b: []byte("89"),
			},
			wantDest: []byte("888888"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			by := BytesComparer{}
			if gotDest := by.Separator(tt.args.a, tt.args.b); !reflect.DeepEqual(gotDest, tt.wantDest) {
				t.Errorf("Separator() = %v, want %v", string(gotDest), string(tt.wantDest))
			}
		})
	}
}

func TestBytesComparer_Successor(t *testing.T) {
	type args struct {
		a []byte
	}
	tests := []struct {
		name     string
		args     args
		wantDest []byte
	}{
		{
			name: "",
			args: args{
				a: []byte("abc"),
			},
			wantDest: []byte("b"),
		},
		{
			name: "",
			args: args{
				a: []byte{0xff, 0xff, 'b'},
			},
			wantDest: []byte{0xff, 0xff, 'c'},
		},
		{
			name: "",
			args: args{
				a: []byte{0xff},
			},
			wantDest: []byte{0xff},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BytesComparer{}
			if gotDest := b.Successor(tt.args.a); !reflect.DeepEqual(gotDest, tt.wantDest) {
				t.Errorf("Successor() = %v, want %v", gotDest, tt.wantDest)
			}
		})
	}
}
