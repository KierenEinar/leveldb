package sstable

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestBTree(t *testing.T) {

	tree := InitBTree(3, DefaultComparer)
	size := uint8(255)
	insertList := make([][]byte, 0, size)

	for i := uint8(0); i < size; i++ {
		insertList = append(insertList, []byte{i})
	}
	rand.Shuffle(int(size), func(i, j int) {
		insertList[i], insertList[j] = insertList[j], insertList[i]
	})

	for i := uint8(0); i < size; i++ {
		fmt.Printf("insert, %d\n", insertList[i])
		tree.Insert(insertList[i], insertList[i])
	}

	tree.BFS()

	for i := uint8(0); i < size; i++ {
		v, ok := tree.Get(insertList[i])
		if !ok {
			t.Fatalf("TestBTree_Get not ok, should be ok, key=%d", insertList[i])
		}
		t.Logf("TestBTree_Get ok, key=%d, value=%d", insertList[i], v)
	}

	for i := uint8(0); i < size; i++ {
		ok := tree.Remove(insertList[i])
		if !ok {
			t.Fatalf("TestBTree_Remove not ok, should be ok, key=%d", insertList[i])
		}

		t.Logf("TestBTree_Remove ok key=%d", insertList[i])
		tree.BFS()
	}

}

func TestBTree_Iterator(t *testing.T) {
	tree := InitBTree(3, DefaultComparer)
	size := uint8(255)
	insertList := make([][]byte, 0, size)

	for i := uint8(0); i < size; i++ {
		insertList = append(insertList, []byte{i})
	}
	rand.Shuffle(int(size), func(i, j int) {
		insertList[i], insertList[j] = insertList[j], insertList[i]
	})

	for i := uint8(0); i < size; i++ {
		fmt.Printf("insert, %d\n", insertList[i])
		tree.Insert(insertList[i], insertList[i])
	}

	iter := tree.NewIterator()
	for iter.Next() {
		fmt.Printf("key=%d,value=%d\n", iter.Key(), iter.Value())
	}

	for i := uint8(0); i < 20; i++ {
		ok := tree.Remove([]byte{i})
		if !ok {
			t.Fatalf("Remove not ok")
		}
	}

	iter.Reset()
	for iter.Next() {
		fmt.Printf("key=%d,value=%d\n", iter.Key(), iter.Value())
	}

	iter.Seek([]byte{100})

	fmt.Printf("key=%d,value=%d\n", iter.Key(), iter.Value())

	for iter.Next() {
		fmt.Printf("key=%d,value=%d\n", iter.Key(), iter.Value())
	}

}
