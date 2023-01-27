package collections

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/KierenEinar/leveldb/comparer"
	"github.com/KierenEinar/leveldb/utils"
)

type BTree struct {
	degree int
	root   *BTreeNode
	cmp    comparer.BasicComparer
}

type BTreeNode struct {
	isLeaf   bool
	keys     [][]byte
	values   []interface{}
	siblings []*BTreeNode
	num      int
	degree   int
}

func (node *BTreeNode) isFull() bool {
	return node.num == node.degree*2-1
}

func newNode(degree int, isLeaf bool) *BTreeNode {
	node := &BTreeNode{
		isLeaf:   isLeaf,
		degree:   degree,
		keys:     make([][]byte, degree*2-1),
		values:   make([]interface{}, degree*2-1),
		siblings: make([]*BTreeNode, degree*2),
		num:      0,
	}
	return node
}

func InitBTree(degree int, cmp comparer.BasicComparer) *BTree {
	return &BTree{
		degree: degree,
		cmp:    cmp,
	}
}

func (btree *BTree) Insert(key []byte, value interface{}) {
	root := btree.root
	if root == nil {
		n := newNode(btree.degree, true)
		root = n
	}

	if root.isFull() {
		n := newNode(btree.degree, false)
		k, v := root.keys[btree.degree-1], root.values[btree.degree-1]
		z := root.splitChild()
		n.setKVAndSibling(0, k, v, root, z)
		root = n
	}
	insertNonFull(root, key, value, btree.cmp)
	btree.root = root
}

// must assert idx less than node.num and node must not full
func (node *BTreeNode) setKVAndSibling(idx int, key []byte, value interface{}, left, right *BTreeNode) {
	utils.Assert(!node.isFull())
	copy(node.keys[idx+1:node.num+1], node.keys[idx:node.num])
	copy(node.values[idx+1:node.num+1], node.values[idx:node.num])
	node.keys[idx] = key
	node.values[idx] = value

	copy(node.siblings[idx+2:node.num+2], node.siblings[idx+1:node.num+1])
	node.siblings[idx] = left
	node.siblings[idx+1] = right
	node.num++
}

// must assert node is full
func (node *BTreeNode) splitChild() *BTreeNode {
	utils.Assert(node.isFull())

	t := node.degree
	z := newNode(t, node.isLeaf)

	copy(z.keys, node.keys[t:node.num])
	copy(z.values, node.values[t:node.num])
	if !node.isLeaf {
		// t = 3
		// keys  	0   1    2   3    4
		// sib   0    1    2   3    4    5
		copy(z.siblings, node.siblings[t:node.num+1])
	}
	z.num = t - 1
	node.num = t - 1
	for idx := node.num; idx < len(node.keys); idx++ {
		node.keys[idx] = nil
		node.values[idx] = nil
	}
	return z
}

func insertNonFull(node *BTreeNode, key []byte, value interface{}, cmp comparer.BasicComparer) {

	idx := sort.Search(node.num, func(i int) bool {
		return cmp.Compare(node.keys[i], key) >= 0
	})

	var found bool

	if idx < node.num {
		found = cmp.Compare(node.keys[idx], key) == 0
	}

	if found {
		node.values[idx] = value
		return
	}

	if node.isLeaf {
		copy(node.keys[idx+1:node.num+1], node.keys[idx:node.num])
		copy(node.values[idx+1:node.num+1], node.values[idx:node.num])
		node.num++
		node.keys[idx] = append([]byte(nil), key...)
		node.values[idx] = value
		return
	}

	sibling := node.siblings[idx]

	if !sibling.isFull() {
		insertNonFull(node.siblings[idx], key, value, cmp)
		return
	}

	k, v := sibling.keys[sibling.degree-1], sibling.values[sibling.degree-1]

	z := sibling.splitChild()
	node.setKVAndSibling(idx, k, v, sibling, z)

	if bytes.Compare(k, key) < 0 {
		insertNonFull(z, key, value, cmp)
	} else {
		insertNonFull(sibling, key, value, cmp)
	}
}

func (btree *BTree) Remove(key []byte) bool {

	root := btree.root
	if root == nil {
		return false
	}

	r := remove(root, key, btree.cmp)
	if root.num == 0 {
		if root.isLeaf {
			btree.root = nil
		} else {
			btree.root = root.siblings[0]
		}
	}
	return r
}

// note, caller should follow this rules
// * only root node's num can lt degree if is root
// * other wise node's num should be gte than degree
func remove(node *BTreeNode, key []byte, cmp comparer.BasicComparer) bool {

	idx := sort.Search(node.num, func(i int) bool {
		return cmp.Compare(node.keys[i], key) >= 0
	})

	var found bool

	if idx < node.num {
		found = cmp.Compare(node.keys[idx], key) == 0
	}

	if node.isLeaf && !found {
		return false
	}

	if found {

		if node.isLeaf {
			copy(node.keys[idx:node.num-1], node.keys[idx+1:node.num])
			copy(node.values[idx:node.num-1], node.values[idx+1:node.num])
			node.keys[node.num-1] = nil
			node.values[node.num-1] = nil
			node.num--
			return true
		} else {
			prevSibling := node.siblings[idx]
			nextSibling := node.siblings[idx+1]

			k, v := node.keys[idx], node.values[idx]

			// left sibling is enough
			if prevSibling.num > node.degree-1 {

				mostlyPrevious := prevSibling

				// search mostly previous key
				for !mostlyPrevious.isLeaf {
					mostlyPrevious = mostlyPrevious.siblings[mostlyPrevious.num]
				}

				moveKey := mostlyPrevious.keys[mostlyPrevious.num-1]
				moveValue := mostlyPrevious.values[mostlyPrevious.num-1]
				node.keys[idx] = moveKey
				node.values[idx] = moveValue

				mostlyPrevious.keys[mostlyPrevious.num-1] = k
				mostlyPrevious.values[mostlyPrevious.num-1] = v

				return remove(prevSibling, key, cmp)

			} else if nextSibling.num > node.degree-1 {

				mostLatest := nextSibling
				for !mostLatest.isLeaf {
					mostLatest = mostLatest.siblings[0]
				}

				moveKey := mostLatest.keys[0]
				moveValue := mostLatest.values[0]
				node.keys[idx] = moveKey
				node.values[idx] = moveValue

				mostLatest.keys[0] = k
				mostLatest.values[0] = v

				return remove(nextSibling, key, cmp)

			} else { // merge
				merge(node, idx)
				return remove(node.siblings[idx], key, cmp)
			}

		}
	} else {

		sibling := node.siblings[idx]

		if sibling.num == node.degree-1 {

			var (
				prev *BTreeNode
				next *BTreeNode
			)

			if idx != node.num {
				next = node.siblings[idx+1]
			}

			if idx != 0 {
				prev = node.siblings[idx-1]
			}

			if prev != nil && prev.num > prev.degree-1 {

				nodeKey := node.keys[idx-1]
				nodeVal := node.values[idx-1]

				// sibling borrow prev
				copy(sibling.keys[1:], sibling.keys[:sibling.num])
				copy(sibling.values[1:], sibling.values[:sibling.num])

				node.keys[idx-1] = prev.keys[prev.num-1]
				node.values[idx-1] = prev.values[prev.num-1]

				sibling.keys[0] = nodeKey
				sibling.values[0] = nodeVal

				prev.keys[prev.num-1] = nil
				prev.values[prev.num-1] = nil

				if !sibling.isLeaf {
					copy(sibling.siblings[1:], sibling.siblings[:sibling.num+1])
					sibling.siblings[0] = prev.siblings[prev.num]
					prev.siblings[prev.num] = nil
				}

				sibling.num++
				prev.num--

				return remove(sibling, key, cmp)

			} else if next != nil && next.num > next.degree-1 {
				// sibling borrow next

				nodeKey := node.keys[idx]
				nodeVal := node.values[idx]

				sibling.keys[sibling.num] = nodeKey
				sibling.values[sibling.num] = nodeVal

				node.keys[idx] = next.keys[0]
				node.values[idx] = next.values[0]

				copy(next.keys[0:], next.keys[1:next.num])
				copy(next.values[0:], next.values[1:next.num])

				next.keys[next.num-1] = nil
				next.values[next.num-1] = nil

				if !sibling.isLeaf {
					sibling.siblings[sibling.num+1] = next.siblings[0]
					copy(next.siblings[0:], next.siblings[1:next.num+1])
					next.siblings[next.num] = nil
				}
				sibling.num++
				next.num--

				return remove(sibling, key, cmp)

			} else {

				if prev != nil {
					// merge prev
					merge(node, idx-1)
					return remove(node.siblings[idx-1], key, cmp)
				} else {
					// merge next
					merge(node, idx)
					return remove(node.siblings[idx], key, cmp)
				}
			}

		} else {
			return remove(sibling, key, cmp)
		}

	}
}

func merge(node *BTreeNode, idx int) {
	prevSibling := node.siblings[idx]
	nextSibling := node.siblings[idx+1]

	copy(prevSibling.keys[prevSibling.num+1:], nextSibling.keys[:nextSibling.num])
	copy(prevSibling.values[prevSibling.num+1:], nextSibling.values[:nextSibling.num])

	prevSibling.keys[prevSibling.num] = node.keys[idx]
	prevSibling.values[prevSibling.num] = node.values[idx]

	if !prevSibling.isLeaf {
		copy(prevSibling.siblings[prevSibling.num+1:], nextSibling.siblings[:nextSibling.num+1])
	}

	copy(node.keys[idx:], node.keys[idx+1:node.num])
	copy(node.values[idx:], node.values[idx+1:node.num])
	copy(node.siblings[idx+1:], node.siblings[idx+2:node.num+1])

	node.keys[node.num-1] = nil
	node.values[node.num-1] = nil
	node.siblings[node.num] = nil

	prevSibling.num += nextSibling.num + 1
	node.num -= 1
	nextSibling = nil
}

func (btree *BTree) Get(key []byte) (interface{}, bool) {
	if btree.root == nil {
		return nil, false
	}
	return get(btree.root, key, btree.cmp)
}

func get(node *BTreeNode, key []byte, cmp comparer.BasicComparer) (interface{}, bool) {
	idx := sort.Search(node.num, func(i int) bool {
		return cmp.Compare(node.keys[i], key) >= 0
	})

	var found bool

	if idx < node.num && cmp.Compare(node.keys[idx], key) == 0 {
		found = true
	}

	if found {
		return node.values[idx], true
	}

	if node.isLeaf {
		return nil, false
	}

	return get(node.siblings[idx], key, cmp)

}

func (btree *BTree) Has(key []byte) bool {
	if btree.root == nil {
		return false
	}
	return has(btree.root, key, btree.cmp)
}

func has(node *BTreeNode, key []byte, cmp comparer.BasicComparer) bool {
	idx := sort.Search(node.num, func(i int) bool {
		return cmp.Compare(node.keys[i], key) >= 0
	})

	var found bool

	if idx < node.num && cmp.Compare(node.keys[idx], key) == 0 {
		found = true
	}

	if found {
		return true
	}

	if node.isLeaf {
		return false
	}

	return has(node.siblings[idx], key, cmp)

}

func (tree *BTree) BFS() {
	if tree.root == nil {
		return
	}
	tree.root.bfs()
}

func (node *BTreeNode) bfs() {
	queue := append([]*BTreeNode{}, node)
	curLevelNums := 1
	var nextLevelNums int
	var level int64
	for len(queue) > 0 {
		node := queue[0]
		curLevelNums--
		fmt.Printf("level=%d, keys=%v\n", level, node.keys[:node.num])
		if !node.isLeaf {
			nextLevelNums += node.num + 1
		}
		if curLevelNums == 0 {
			level++
			curLevelNums = nextLevelNums
			nextLevelNums = 0
		}
		queue = queue[1:]
		if !node.isLeaf {
			queue = append(queue, node.siblings[:node.num+1]...)
		}
	}
}

type BTreeIter struct {
	*BTree
	stack []*bTreeCursor
	key   []byte
	value interface{}
}

type bTreeCursor struct {
	node  *BTreeNode
	index int
}

func (tree *BTree) NewIterator() *BTreeIter {
	iter := &BTreeIter{
		BTree: tree,
		stack: make([]*bTreeCursor, 0, tree.depth()),
	}
	return iter
}

func (tree *BTree) depth() int {
	node := tree.root
	depth := 1
	for ; node != nil && !node.isLeaf; depth++ {
		node = node.siblings[0]
	}
	return depth
}

func (iter *BTreeIter) SeekFirst() bool {
	node := iter.root

	// push into stack
	for {
		iter.stack = append(iter.stack, &bTreeCursor{
			node:  node,
			index: 0,
		})
		if node.isLeaf {
			break
		}
		node = node.siblings[0]
	}

	return iter.next()

}

func (iter *BTreeIter) Next() bool {
	if len(iter.stack) > 0 {
		return iter.next()
	}
	return iter.SeekFirst()
}

// Seek seeks a Key that gte than input Key
func (iter *BTreeIter) Seek(key []byte) bool {

	// reset stack
	iter.stack = iter.stack[:0]

	node := iter.root
	cmp := iter.BTree.cmp
	for {

		idx := sort.Search(node.num, func(i int) bool {
			return cmp.Compare(node.keys[i], key) >= 0
		})

		if node.isLeaf && idx == node.num {
			iter.stack = iter.stack[:0] // reset iter
			return false
		}

		iter.stack = append(iter.stack, &bTreeCursor{
			node:  node,
			index: idx,
		})

		// found case
		if idx < node.num && cmp.Compare(node.keys[idx], key) == 0 {
			return iter.next()
		}

		if !node.isLeaf {
			node = node.siblings[idx]
		} else {
			// leaf node and found
			if idx < node.num {
				return iter.next()
			}
			return false // leaf node and not found
		}
	}

}

func (iter *BTreeIter) next() bool {
	for len(iter.stack) > 0 {
		cursor := iter.stack[len(iter.stack)-1]
		if cursor.index < cursor.node.num {
			iter.key = cursor.node.keys[cursor.index]
			iter.value = cursor.node.values[cursor.index]
			cursor.index++
			if !cursor.node.isLeaf {
				iter.stack = append(iter.stack, &bTreeCursor{
					node:  cursor.node.siblings[cursor.index],
					index: 0,
				})
			}
			return true
		} else {
			iter.stack = iter.stack[:len(iter.stack)-1]
		}
	}
	return false
}

func (iter *BTreeIter) Key() []byte {
	if len(iter.stack) > 0 {
		return iter.key
	}
	return nil
}

func (iter *BTreeIter) Value() interface{} {
	if len(iter.stack) > 0 {
		return iter.value
	}
	return nil
}

func (iter *BTreeIter) Reset() {
	iter.key = nil
	iter.value = nil
	iter.stack = iter.stack[:0]
}
