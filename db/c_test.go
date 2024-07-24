package db

import "testing"

func TestC_Add(t *testing.T) {
	c := NewC()
	c.Add("key1", "val1")
	c.PrintTree()
}
