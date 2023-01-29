package cache

import (
	"bytes"
	hash2 "hash"
	"sync"

	"github.com/KierenEinar/leveldb/errors"

	"github.com/KierenEinar/leveldb/utils"
)

const htInitSlots = uint32(1 << 5)

type Cache interface {
	Insert(key []byte, charge uint32, value interface{}, deleter func(key []byte, value interface{})) (*LRUHandle, error)
	Lookup(key []byte) (*LRUHandle, error)
	Erase(key []byte) (*LRUHandle, error)
	Prune()
	Close()
	UnRef(h *LRUHandle)
	//NewId() uint64
}

type LRUHandle struct {
	nextHash *LRUHandle

	next *LRUHandle
	prev *LRUHandle

	hash  uint32
	ref   uint32
	value interface{}
	key   []byte

	inCache bool
	deleter func(key []byte, value interface{})
	charge  uint32
}

func (lh *LRUHandle) Value() interface{} {
	return lh.value
}

type HandleTable struct {
	list  []*LRUHandle
	slots uint32
	size  uint32
}

func NewHandleTable(slots uint32) *HandleTable {
	realSlots := uint32(0)
	for i := htInitSlots; i < 32; i++ {
		if slots < 1<<i {
			realSlots = 1 << i
			break
		}
	}

	return &HandleTable{
		list:  make([]*LRUHandle, realSlots),
		slots: realSlots,
		size:  0,
	}
}

func (ht *HandleTable) Insert(handle *LRUHandle) *LRUHandle {
	ptr := ht.FindPointer(handle.key, handle.hash)
	old := *ptr

	if old == nil {
		*ptr = handle
		ht.size++
		if ht.size > ht.slots {
			ht.Resize(true)
		}
	}

	if old != nil {
		handle.nextHash = old.nextHash
	}

	return old
}

func (ht *HandleTable) Lookup(key []byte, hash uint32) *LRUHandle {
	ptr := ht.FindPointer(key, hash)
	return *ptr
}

func (ht *HandleTable) Erase(key []byte, hash uint32) *LRUHandle {
	ptr := ht.FindPointer(key, hash)
	old := *ptr
	if old != nil {
		ht.size--
		*ptr = old.next
		if ht.size < ht.slots>>1 && ht.slots > htInitSlots {
			ht.Resize(false)
		}
	}
	return old
}

func (ht *HandleTable) FindPointer(key []byte, hash uint32) **LRUHandle {
	slot := hash & (ht.slots - 1)
	ptr := &ht.list[slot]
	for *ptr != nil && (*ptr).hash != hash || bytes.Compare((*ptr).key, key) != 0 {
		ptr = &(*ptr).nextHash
	}
	return ptr
}

func (ht *HandleTable) Resize(growth bool) {

	newSlots := ht.slots
	if growth {
		newSlots = newSlots << 1
	} else {
		newSlots = newSlots >> 1
		utils.Assert(newSlots >= htInitSlots)
	}

	newList := make([]*LRUHandle, newSlots)

	for i := uint32(0); i < ht.slots; i++ {
		ptr := &ht.list[i]
		for *ptr != nil {
			head := &newList[(*ptr).hash&(newSlots-1)]
			next := (*ptr).nextHash
			if *head != nil {
				(*ptr).nextHash = *head
			}
			*head = *ptr
			ptr = &next
		}
	}

	ht.list = newList
	ht.slots = newSlots
}

type LRUCache struct {
	rwMutex sync.RWMutex
	table   *HandleTable

	capacity uint32
	usage    uint32

	// dummy head
	inUse LRUHandle

	// dummy head
	lru LRUHandle

	closed bool
}

func (c *LRUCache) Close() {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	if c.closed {
		return
	}

	c.closed = true

	for next := c.lru.next; next != &c.lru; next = next.next {
		c.finishErase(next)
	}

	for inUse := c.inUse.next; inUse != &c.inUse; inUse = inUse.next {
		c.finishErase(inUse)
	}

}

func newCache(capacity uint32) *LRUCache {
	c := &LRUCache{
		capacity: capacity,
		table:    NewHandleTable(uint32(1 << 8)),
		inUse:    LRUHandle{},
		lru:      LRUHandle{},
	}

	c.inUse.next = &c.inUse
	c.inUse.prev = &c.inUse

	c.lru.next = &c.lru
	c.lru.prev = &c.lru
	return c

}

func (c *LRUCache) Insert(key []byte, hash uint32, charge uint32,
	value interface{}, deleter func(key []byte, value interface{})) (*LRUHandle, error) {

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	if c.closed {
		return nil, errors.ErrClosed
	}

	handle := &LRUHandle{
		hash:    hash,
		ref:     1,
		value:   value,
		key:     append([]byte(nil), key...),
		inCache: true,
		deleter: deleter,
		charge:  charge,
	}

	handle.ref++
	c.usage += charge
	lruAppend(&c.inUse, handle)
	c.finishErase(c.table.Insert(handle))

	for c.usage > c.capacity && c.lru.next != &c.lru {
		c.finishErase(c.table.Erase(c.lru.next.key, c.lru.next.hash))
	}
	return handle, nil

}

func (c *LRUCache) Lookup(key []byte, hash uint32) (*LRUHandle, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	if c.closed {
		return nil, errors.ErrClosed
	}
	h := c.table.Lookup(key, hash)
	if h != nil {
		c.Ref(h)
	}
	return h, nil
}

func (c *LRUCache) Erase(key []byte, hash uint32) (*LRUHandle, error) {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	if c.closed {
		return nil, errors.ErrClosed
	}
	h := c.table.Erase(key, hash)
	c.finishErase(h)
	return h, nil
}

func (c *LRUCache) Prune() {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	if c.closed {
		return
	}

	for next := c.lru.next; next != &c.lru; next = next.next {
		c.finishErase(next)
	}
}

func lruAppend(lru *LRUHandle, h *LRUHandle) {
	h.next = lru
	lru.prev.next = h
	h.prev = lru.prev
	lru.prev = h
}

func lruRemove(h *LRUHandle) {
	h.prev.next = h.next
	h.next.prev = h.prev
}

func (c *LRUCache) finishErase(h *LRUHandle) {
	if h != nil {
		h.inCache = false
		lruRemove(h)
		c.usage -= h.charge
		c.UnRef(h)
	}
}

func (c *LRUCache) UnRef(h *LRUHandle) {

	utils.Assert(h.ref > 0)
	utils.AssertMutexHeld(&c.rwMutex)
	h.ref--
	if h.ref == 0 {
		c.rwMutex.Unlock()
		h.deleter(h.key, h.value) // may cost expensive time
		c.rwMutex.Lock()
	} else if h.ref == 1 && h.inCache {
		lruRemove(h)
		lruAppend(&c.lru, h)
	}
}

func (c *LRUCache) Ref(h *LRUHandle) {
	if h.ref == 1 && h.inCache {
		lruRemove(h)
		lruAppend(&c.inUse, h)
	}
	h.ref++
}

const kNumShardBits = 4

type ShardedLRUCache struct {
	caches [1 << kNumShardBits]*LRUCache
	mutex  sync.Mutex
	hash32 hash2.Hash32
}

func NewCache(capacity uint32, hash32 hash2.Hash32) Cache {

	caches := [1 << kNumShardBits]*LRUCache{}
	for i := 0; i < 1<<kNumShardBits; i++ {
		caches[i] = newCache(capacity)
	}
	c := &ShardedLRUCache{
		caches: caches,
		hash32: hash32,
	}
	return c
}

func (c *ShardedLRUCache) Close() {
	for _, cache := range c.caches {
		go cache.Close()
	}
}

func (c *ShardedLRUCache) Insert(key []byte, charge uint32,
	value interface{}, deleter func(key []byte, value interface{})) (*LRUHandle, error) {
	hash := c.hash(key)
	slot := hash & (1<<kNumShardBits - 1)
	return c.caches[slot].Insert(key, hash, charge, value, deleter)
}

func (c *ShardedLRUCache) Lookup(key []byte) (*LRUHandle, error) {
	hash := c.hash(key)
	slot := hash & (1<<kNumShardBits - 1)
	return c.caches[slot].Lookup(key, hash)
}

func (c *ShardedLRUCache) Erase(key []byte) (*LRUHandle, error) {
	hash := c.hash(key)
	slot := hash & (1<<kNumShardBits - 1)
	return c.caches[slot].Erase(key, hash)
}

func (c *ShardedLRUCache) Prune() {
	for _, cache := range c.caches {
		go cache.Prune()
	}
}

func (c *ShardedLRUCache) hash(key []byte) uint32 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.hash32.Reset()
	_, _ = c.hash32.Write(key)
	return c.hash32.Sum32()
}

func (c *ShardedLRUCache) UnRef(h *LRUHandle) {
	cache := c.caches[h.hash&(1<<kNumShardBits-1)]
	cache.rwMutex.Lock()
	defer cache.rwMutex.Unlock()
	cache.UnRef(h)
}
