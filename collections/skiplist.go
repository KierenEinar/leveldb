package sstable

import (
	"math/rand"
	"sync"
)

const (
	kMaxHeight = 12
	p          = 1 / 4
	kBranching = 4
)

type SkipList struct {
	*BasicReleaser
	level          int8
	rand           *rand.Rand
	seed           int64
	dummyHead      *skipListNode
	tail           *skipListNode
	kvData         []byte
	length         int
	kvSize         int
	rw             sync.RWMutex
	updatesScratch [kMaxHeight]*skipListNode

	BasicComparer
}

func NewSkipList(seed int64, capacity int, cmp BasicComparer) *SkipList {
	skl := &SkipList{
		rand:          rand.New(rand.NewSource(seed)),
		seed:          seed,
		dummyHead:     &skipListNode{},
		kvData:        make([]byte, 0, capacity),
		BasicComparer: cmp,
	}
	return skl
}

func (skl *SkipList) Put(key, value []byte) (err error) {
	if skl.released() {
		err = ErrReleased
		return
	}
	skl.rw.Lock()
	defer skl.rw.Unlock()
	n := skl.dummyHead
	for i := skl.level - 1; i >= 0; i-- {
		for n.next(i) != nil && skl.Compare(n.next(i).key(skl.kvData), key) < 0 {
			n = n.next(i)
		}
		skl.updatesScratch[i] = n
	}

	updates := skl.updatesScratch[:skl.level]

	// if key exists, just update the value
	if updates[0] != nil && skl.Compare(updates[0].next(0).key(skl.kvData), key) == 0 {

		replaceNode := updates[0].next(0)

		// just replace the old value
		if replaceNode.valLen >= len(value) {
			nodeKvData := skl.kvData[replaceNode.kvOffset+replaceNode.keyLen : replaceNode.kvOffset+replaceNode.keyLen+replaceNode.valLen]
			m := copy(nodeKvData[:], value)
			skl.kvSize += replaceNode.valLen - m
			replaceNode.valLen = m
			return
		}

		replaceNode.kvOffset = len(skl.kvData)
		skl.kvData = append(skl.kvData, key...)
		skl.kvData = append(skl.kvData, value...)
		skl.kvSize += len(value) - replaceNode.valLen
		replaceNode.valLen = len(value)
		return
	}

	level := skl.randLevel()

	for i := skl.level; i < level; i++ {
		updates[i] = skl.dummyHead
	}

	if level > skl.level {
		skl.level = level
	}

	newNode := &skipListNode{
		keyLen: len(key),
		valLen: len(value),
		level: skipListNodeLevel{
			maxLevel: level,
			next:     make([]*skipListNode, level),
		},
	}

	for l := 0; l < len(updates); l++ {
		updates[l].setNext(int8(l), newNode)
	}

	// update forward
	updateNextLevel0 := newNode.next(0)
	if updateNextLevel0 != nil {
		updateNextLevel0.backward = newNode
	} else {
		skl.tail = updateNextLevel0
	}

	if updates[0] != skl.dummyHead {
		newNode.backward = updates[0]
	}

	newNode.kvOffset = len(skl.kvData)

	skl.kvData = append(skl.kvData, key...)
	skl.kvData = append(skl.kvData, value...)
	skl.kvSize += len(key) + len(value)
	skl.length++
	return
}

func (skl *SkipList) Del(key []byte) (updated bool, err error) {

	if skl.released() {
		err = ErrReleased
		return
	}

	skl.rw.Lock()
	defer skl.rw.Unlock()

	if skl.tail == nil || skl.dummyHead.next(0) == nil {
		updated = false
		return
	}

	updates := skl.findLT(key)

	if skl.Compare(updates[0].next(0).key(skl.kvData), key) != 0 {
		updated = false
		return
	}

	foundNode := updates[0].next(0)
	for i := foundNode.level.maxLevel - 1; i >= 0; i-- {
		updates[i].setNext(i, foundNode.next(0))
	}

	// update skl level if is empty
	var level = foundNode.level.maxLevel
	for ; skl.dummyHead.next(level-1) == nil; level-- {
	}
	skl.level = level

	// update forward
	prev := foundNode.backward
	next := foundNode.next(0)

	if next != nil {
		if prev != nil {
			next.backward = prev
		}
	} else {
		// foundNode is the last one, so if prev is not null(prev not link dummy head)
		skl.tail = prev
	}

	skl.length--
	skl.kvSize -= foundNode.size()
	updated = true
	return
}

func (skl *SkipList) Get(key []byte) ([]byte, error) {
	if n, found, err := skl.FindGreaterOrEqual(key); err != nil {
		return nil, err
	} else if found == true {
		return n.value(skl.kvData), nil
	}
	return nil, ErrNotFound
}

func (skl *SkipList) FindGreaterOrEqual(key []byte) (*skipListNode, bool, error) {
	if skl.released() {
		return nil, false, ErrReleased
	}

	skl.rw.RLock()
	defer skl.rw.RUnlock()
	n := skl.dummyHead
	var (
		hitLevel int8 = -1
	)
	for i := skl.level - 1; i >= 0; i-- {
		for ; n.next(i) != nil && skl.Compare(n.next(i).key(skl.kvData), key) < 0; n = n.next(i) {
		}
		if n.next(i) != nil && skl.Compare(n.next(i).key(skl.kvData), key) == 0 {
			hitLevel = i
			break
		}
	}
	if hitLevel >= 0 { // case found
		return n.next(hitLevel), true, nil
	}
	next := n.next(0)
	if next != nil {
		return next, false, nil
	}
	return nil, false, nil
}

func (skl *SkipList) Size() int {
	skl.rw.RLock()
	defer skl.rw.RUnlock()
	return skl.kvSize
}

func (skl *SkipList) Capacity() int {
	skl.rw.RLock()
	defer skl.rw.RUnlock()
	return cap(skl.kvData)
}

// NewIterator return an iter
// caller should call UnRef after iterate end
func (skl *SkipList) NewIterator() Iterator {
	skl.Ref()
	sklIter := &SkipListIter{
		skl: skl,
	}
	sklIter.OnClose = func() {
		skl.UnRef()
	}
	return sklIter
}

type SkipListIter struct {
	skl *SkipList
	n   *skipListNode
	dir direction
	Iterator
	*BasicReleaser
	iterErr error
}

func (sklIter *SkipListIter) SeekFirst() bool {
	if sklIter.released() {
		sklIter.iterErr = ErrReleased
		return false
	}

	sklIter.n = sklIter.skl.dummyHead
	sklIter.dir = dirSOI
	return sklIter.Next()
}

func (sklIter *SkipListIter) Next() bool {
	if sklIter.released() {
		sklIter.iterErr = ErrReleased
		return false
	}
	if sklIter.dir == dirSOI {
		return false
	}
	skl := sklIter.skl
	skl.rw.RLock()
	defer skl.rw.RUnlock()

	if sklIter.n == nil {
		return sklIter.SeekFirst()
	}

	n := sklIter.n.next(0)
	if n == nil {
		sklIter.dir = dirEOI
		return false
	}
	sklIter.dir = dirForward
	sklIter.n = n
	return true
}

func (sklIter *SkipListIter) Valid() error {
	if sklIter.released() {
		return ErrReleased
	}
	if sklIter.iterErr != nil {
		return sklIter.iterErr
	}
	return nil
}

func (sklIter *SkipListIter) Seek(key InternalKey) bool {

	if sklIter.released() {
		sklIter.iterErr = ErrReleased
		return false
	}

	skl := sklIter.skl

	sklIter.skl.rw.RLock()
	defer sklIter.skl.rw.RUnlock()

	node, _, err := skl.FindGreaterOrEqual(key)
	if err != nil {
		sklIter.iterErr = err
		return false
	}

	if node == nil {
		sklIter.dir = dirEOI
		return false
	}
	sklIter.n = node
	sklIter.dir = dirForward
	return true
}

func (sklIter *SkipListIter) Key() []byte {
	if sklIter.n == nil {
		return nil
	}
	return sklIter.n.key(sklIter.skl.kvData)
}

func (sklIter *SkipListIter) Value() []byte {
	if sklIter.n == nil {
		return nil
	}
	return sklIter.n.value(sklIter.skl.kvData)
}

func (skl *SkipList) findLT(key []byte) []*skipListNode {

	updates := skl.updatesScratch
	n := skl.dummyHead
	for i := skl.level - 1; i >= 0; i-- {
		for n.next(i) != nil && skl.Compare(n.next(i).key(skl.kvData), key) < 0 {
			n = n.next(i)
		}
		updates[i] = n
	}

	return updates[:skl.level]

}

type skipListNode struct {
	kvOffset int // kvOffset in skipList kvData
	keyLen   int
	valLen   int
	level    skipListNodeLevel
	backward *skipListNode
}

func (node *skipListNode) setNext(i int8, n *skipListNode) {
	assert(i < node.level.maxLevel)
	next := node.level.next[i]
	node.level.next[i] = n
	if n != nil {
		n.level.next[i] = next
	}
}

func (node *skipListNode) next(i int8) *skipListNode {
	assert(i < node.level.maxLevel)
	return node.level.next[i]
}

func (node *skipListNode) size() int {
	return node.keyLen + node.valLen
}

type skipListNodeLevel struct {
	next     []*skipListNode
	maxLevel int8
}

// required mutex held
func (skl *SkipList) randLevel() int8 {
	height := int8(1)
	// n = (1/p)^kMaxHeight, n = 16m, p=1/4 => kMaxHeight=12
	for height < kMaxHeight {
		if skl.rand.Int()%kBranching == 1 {
			height++
		} else {
			break
		}
	}
	assert(height <= kMaxHeight)
	return height
}

func (node *skipListNode) keyValue(kvData []byte) (key []byte, value []byte) {
	assert(node.kvOffset < len(kvData))
	key = kvData[node.kvOffset : node.kvOffset+node.keyLen]
	value = kvData[node.kvOffset+node.keyLen : node.kvOffset+node.keyLen+node.valLen]
	return
}

func (node *skipListNode) key(kvData []byte) (key []byte) {
	assert(node.kvOffset < len(kvData))
	key = kvData[node.kvOffset : node.kvOffset+node.keyLen]
	return
}

func (node *skipListNode) value(kvData []byte) (key []byte) {
	assert(node.kvOffset < len(kvData))
	key = kvData[node.kvOffset+node.keyLen : node.kvOffset+node.keyLen+node.keyLen]
	return
}
