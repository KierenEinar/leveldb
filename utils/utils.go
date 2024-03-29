package utils

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

const mutexLocked = 1

func AssertMutexHeld(mutex *sync.RWMutex) {}

func Assert(condition bool, msgs ...string) {
	if !condition {
		fetalMsg := bytes.NewBuffer(nil)
		_, file, line, _ := runtime.Caller(2)
		// file
		fetalMsg.WriteString("file:")
		fetalMsg.WriteString(file)
		fetalMsg.WriteString("\n")
		// line
		fetalMsg.WriteString("line:")
		fetalMsg.WriteString(strconv.Itoa(line))
		fetalMsg.WriteString("\n")

		// msgs
		for idx, msg := range msgs {
			fetalMsg.WriteString(msg)
			if idx != len(msgs)-1 {
				fetalMsg.WriteString("\n")
			}
		}
		panic(fetalMsg)
	}
}

type Releaser interface {
	Ref() int32
	UnRef() int32
	RegisterCleanUp(f func(args ...interface{}), args ...interface{})
}

type BasicReleaser struct {
	release          uint32
	ref              int32
	dummyCleanUpNode cleanUpNode
	OnClose          func()
	OnRef            func()
	OnUnRef          func()
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
		atomic.StoreUint32(&br.release, 1)
		if br.OnClose != nil {
			br.OnClose()
		}
		node := br.dummyCleanUpNode.next
		node.doClean()
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

type cleanUpNode struct {
	next *cleanUpNode
	f    func(args ...interface{})
	args []interface{}
}

func (br *BasicReleaser) RegisterCleanUp(f func(args ...interface{}), args ...interface{}) {
	nextNode := &cleanUpNode{
		f:    f,
		args: args,
	}

	nextNode.next = br.dummyCleanUpNode.next
	br.dummyCleanUpNode.next = nextNode
	return
}

func (node *cleanUpNode) doClean() {
	n := node
	for n != nil {
		n.f(n.args...)
		n = n.next
	}
}
