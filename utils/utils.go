package utils

import (
	"sync"
)

const mutexLocked = 1

func AssertMutexHeld(mutex *sync.RWMutex) {}

func Assert(condition bool, msg ...string) {
	if !condition {
		panic(msg)
	}
}