package db

import "testing"
import testify_assert "github.com/stretchr/testify/assert"

func TestNode(t *testing.T) {
	node := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}

	btype := BNODE_LEAF
	var nkeys uint16 = 0
	node.setHeader(btype, nkeys)
	testify_assert.Equal(t, btype, node.btype())
	testify_assert.Equal(t, nkeys, node.nkeys())

}
