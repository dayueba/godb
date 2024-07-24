package db

import (
	"bytes"
	"fmt"
	"unsafe"
)

type BTree struct {
	// pointer (a nonzero page number)
	root uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer
	new func(BNode) uint64 // allocate a new page
	del func(uint64)       // deallocate a page
}

// insert a new key or update an existing key
func (tree *BTree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		// create the first node
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil) // 哨兵值，用来解决边缘情况，解决nodeLookupLE找不到key的情况
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

// delete a key and returns whether the key was there
func (tree *BTree) Delete(key []byte) bool {
	assert(len(key) != 0)
	assert(len(key) < BTREE_MAX_KEY_SIZE)

	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated.data) == 0 {
		return false // not found
	}

	tree.del(tree.root)
	// if 1 key in internal node
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		// remove level
		tree.root = updated.getPtr(0) // assign root to 0 pointer
	} else {
		tree.root = tree.new(updated) // assign root to point to updated node
	}
	return true
}

// insert a KV into a node, the result might be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it.
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position.
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return new
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// find index of key to pull key from node
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF: // if leaf
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // key not found
		}
		// delete the key in the leaf
		new := BNode{data: make([]byte, BTREE_PAGE_SIZE)} // allocate empty node
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE: // if internal
		return nodeDelete(tree, node, idx, key)
	default:
		panic("treeDelete: bad node!")
	}
}

// tree container struct
type C struct {
	tree  BTree
	ref   map[string]string // reference map to record each b-tree update
	pages map[uint64]BNode  // hashmap to hold pages in-memory, no disk persistence yet
}

func NewC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert(ok)

				return node
			},
			new: func(node BNode) uint64 {
				assert(node.nbytes() < BTREE_PAGE_SIZE)

				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				assert(pages[key].data == nil)

				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert(ok)

				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) Add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) Del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}

func (c *C) PrintTree() {
	// fmt.Printf("Root page: %d\n", c.pages[c.tree.root])
	fmt.Println("Pages:")
	for pt, node := range c.pages {
		fmt.Println("Pointer:", pt)
		fmt.Println("BNode data:", node.data)
	}
}
