package utils

import (
	"sync"
	"sync/atomic"
)

const mutexLocked = 1

func AssertMutexHeld(mutex *sync.RWMutex) {}

func Assert(condition bool, msg ...string) {
	if !condition {
		panic(msg)
	}
}

type Releaser interface {
	Ref() int32
	UnRef() int32
}

type BasicReleaser struct {
	release uint32
	ref     int32
	OnClose func()
	OnRef   func()
	OnUnRef func()
}

func (br *BasicReleaser) Ref() int32 {
	if br.OnRef != nil {
		br.OnRef()
	}
	return atomic.AddInt32(&br.ref, 1)
}

func (br *BasicReleaser) UnRef() int32 {
	newInt32 := atomic.AddInt32(&br.ref, -1)
	if newInt32 < 0 {
		panic("duplicated UnRef")
	}
	if br.OnUnRef != nil {
		br.OnUnRef()
	}
	if newInt32 == 0 {
		if br.OnClose != nil {
			atomic.StoreUint32(&br.release, 1)
			br.OnClose()
		}
	}
	return newInt32
}

func (br *BasicReleaser) Released() bool {
	return atomic.LoadUint32(&br.release) == 1
}

func EnsureBuffer(dst []byte, size int) []byte {
	if len(dst) < size {
		return make([]byte, size)
	}
	return dst[:size]
}
