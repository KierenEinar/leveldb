package sstable

import (
	"sync"
)

const mutexLocked = 1

func assertMutexHeld(mutex *sync.RWMutex) {}
