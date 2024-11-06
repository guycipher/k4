// Package bstarplustree
// Append only semi B*+Tree variant used for SSTables on K4
// BSD 3-Clause License
//
// Copyright (c) 2024, Alex Gaetano Padula
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its
//     contributors may be used to endorse or promote products derived from
//     this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package bstarplustree

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/guycipher/k4/compressor"
	"github.com/guycipher/k4/pager"
	"os"
	"time"
)

const COMPRESSION_WINDOW_SIZE = 1024 * 32 // The compression window size

// BStarPlusTree is the main struct for the B*+Tree
type BStarPlusTree struct {
	Pager    *pager.Pager // The pager for the bstarplustree
	T        int          // The order of the tree
	Compress bool         // Whether to compress nodes and values
}

// Key is the key struct for the a BStarPlusTree node
type Key struct {
	K   []byte     // The key
	V   []int64    // The values
	TTL *time.Time // Time to live
}

// Node is the node struct for the BStarPlusTree
type Node struct {
	Page     int64   // The page number of the node
	Keys     []*Key  // The keys in node
	Children []int64 // The children of the node
	Leaf     bool    // If the node is a leaf node
	Next     int64   // The next leaf node (for leaf nodes only)
}

// KeyIterator is an iterator for the values of a key
type KeyIterator struct {
	index int            // current index
	key   *Key           // the key
	bspt  *BStarPlusTree // the bstarplustree
}

// Iterator is an iterator for the keys of the BStarPlusTree
type Iterator interface {
	HasNext() bool
	Next() (*Key, error)
	Prev() (*Key, error)
	GetBSPT() *BStarPlusTree
}

// InOrderIterator is an iterator for the keys of the BStarPlusTree in order
type InOrderIterator struct {
	stack []*Node
	index int
	bspt  *BStarPlusTree
}

// Open opens a new or existing BStarPlusTree
func Open(name string, flag, perm int, t int, compression bool) (*BStarPlusTree, error) {
	if t < 2 {
		return nil, errors.New("t must be greater than 1")
	}

	pager, err := pager.OpenPager(name, flag, os.FileMode(perm))
	if err != nil {
		return nil, err
	}

	return &BStarPlusTree{
		T:        t,
		Pager:    pager,
		Compress: compression,
	}, nil
}

// Close closes the BStarPlusTree
func (bspt *BStarPlusTree) Close() error {
	return bspt.Pager.Close()
}

// encodeNode encodes a node into a byte slice
func encodeNode(n *Node, compress bool) ([]byte, error) {
	buff := new(bytes.Buffer)
	enc := gob.NewEncoder(buff)
	err := enc.Encode(n)
	if err != nil {
		return nil, err
	}

	// check if compress is set
	if compress {
		comp, err := compressor.NewCompressor(COMPRESSION_WINDOW_SIZE)
		if err != nil {
			return nil, err
		}

		return comp.Compress(buff.Bytes()), nil

	}

	return buff.Bytes(), nil
}

// newNode creates a new BStarPlusTree node
func (bspt *BStarPlusTree) newNode(leaf bool) (*Node, error) {
	newNode := &Node{
		Leaf: leaf,
		Keys: make([]*Key, 0),
	}

	encodedNode, err := encodeNode(newNode, bspt.Compress)
	if err != nil {
		return nil, err
	}

	newNode.Page, err = bspt.Pager.Write(encodedNode)
	if err != nil {
		return nil, err
	}

	encodedNode, err = encodeNode(newNode, bspt.Compress)
	if err != nil {
		return nil, err
	}

	err = bspt.Pager.WriteTo(newNode.Page, encodedNode)
	if err != nil {
		return nil, err
	}

	return newNode, nil
}

// encodeValue encodes a value into a byte slice
func encodeValue(value []byte) ([]byte, error) {
	buff := new(bytes.Buffer)
	enc := gob.NewEncoder(buff)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// decodeValue decodes a byte slice into a value
func decodeValue(data []byte) ([]byte, error) {
	var value []byte
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// decodeNode decodes a byte slice into a node
func decodeNode(data []byte, decompress bool) (*Node, error) {
	if decompress {
		decomp, err := compressor.NewCompressor(COMPRESSION_WINDOW_SIZE)
		if err != nil {
			return nil, err
		}

		data = decomp.Decompress(data)
	}

	node := &Node{}
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// getRoot returns the root of the BStarPlusTree
func (bspt *BStarPlusTree) getRoot() (*Node, error) {
	root, err := bspt.Pager.GetPage(0)
	if err != nil {
		if err.Error() == "EOF" {
			rootNode := &Node{
				Leaf:     true,
				Page:     0,
				Children: make([]int64, 0),
				Keys:     make([]*Key, 0),
			}

			encodedRoot, err := encodeNode(rootNode, bspt.Compress)
			if err != nil {
				return nil, err
			}

			err = bspt.Pager.WriteTo(0, encodedRoot)
			if err != nil {
				return nil, err
			}

			return rootNode, nil
		} else {
			return nil, err
		}
	}

	rootNode, err := decodeNode(root, bspt.Compress)
	if err != nil {
		return nil, err
	}

	return rootNode, nil
}

// splitRoot splits the root node
func (bspt *BStarPlusTree) splitRoot() error {
	oldRoot, err := bspt.getRoot()
	if err != nil {
		return err
	}

	newOldRoot, err := bspt.newNode(oldRoot.Leaf)
	if err != nil {
		return err
	}

	newOldRoot.Keys = oldRoot.Keys
	newOldRoot.Children = oldRoot.Children

	newRoot := &Node{
		Page:     0,
		Children: []int64{newOldRoot.Page},
	}

	err = bspt.splitChild(newRoot, 0, newOldRoot)
	if err != nil {
		return err
	}

	encodedNewRoot, err := encodeNode(newRoot, bspt.Compress)
	if err != nil {
		return err
	}

	err = bspt.Pager.WriteTo(newRoot.Page, encodedNewRoot)
	if err != nil {
		return err
	}

	encodedNewOldRoot, err := encodeNode(newOldRoot, bspt.Compress)
	if err != nil {
		return err
	}

	err = bspt.Pager.WriteTo(newOldRoot.Page, encodedNewOldRoot)
	if err != nil {
		return err
	}

	return nil
}

// splitChild splits a full child node into two nodes and updates the parent node.
// It creates a new node, redistributes keys and children between the full node and the new node,
// and updates the parent node with the new key and child
func (bspt *BStarPlusTree) splitChild(parent *Node, index int, fullNode *Node) error {
	newNode, err := bspt.newNode(fullNode.Leaf)
	if err != nil {
		return err
	}

	t := bspt.T
	newNode.Keys = append(newNode.Keys, fullNode.Keys[t:]...)
	fullNode.Keys = fullNode.Keys[:t]

	if !fullNode.Leaf {
		newNode.Children = append(newNode.Children, fullNode.Children[t:]...)
		fullNode.Children = fullNode.Children[:t]
	} else {
		newNode.Next = fullNode.Next
		fullNode.Next = newNode.Page
	}

	parent.Keys = append(parent.Keys, nil)
	parent.Children = append(parent.Children, 0)

	for j := len(parent.Keys) - 1; j > index; j-- {
		parent.Keys[j] = parent.Keys[j-1]
	}
	parent.Keys[index] = fullNode.Keys[t-1]

	fullNode.Keys = fullNode.Keys[:t-1]

	for j := len(parent.Children) - 1; j > index+1; j-- {
		parent.Children[j] = parent.Children[j-1]
	}
	parent.Children[index+1] = newNode.Page

	err = bspt.writeNode(fullNode)
	if err != nil {
		return err
	}
	err = bspt.writeNode(newNode)
	if err != nil {
		return err
	}
	return bspt.writeNode(parent)
}

// Put inserts a key into the BStarPlusTree
func (bspt *BStarPlusTree) Put(key, value []byte, ttl *time.Time) error {
	root, err := bspt.getRoot()
	if err != nil {
		return err
	}

	if len(root.Keys) == (2*bspt.T)-1 {
		err = bspt.splitRoot()
		if err != nil {
			return err
		}

		rootBytes, err := bspt.Pager.GetPage(0)
		if err != nil {
			return err
		}

		root, err = decodeNode(rootBytes, bspt.Compress)
		if err != nil {
			return err
		}
	}

	// Encode the value and write it to a new page
	encodedValue, err := encodeValue(value)
	if err != nil {
		return err
	}

	valuePage, err := bspt.Pager.Write(encodedValue)
	if err != nil {
		return err
	}

	err = bspt.insertNonFull(root, key, valuePage, ttl)
	if err != nil {
		return err
	}

	return nil
}

// insertNonFull inserts a key into a node that is not full.
// If the node is a leaf, it inserts the key in the correct position.
// If the node is not a leaf, it finds the correct child to insert the key.
// If the child is full, it handles the split or redistribution before inserting.
// The function ensures that the tree maintains its properties after the insertion
func (bspt *BStarPlusTree) insertNonFull(node *Node, key []byte, valuePage int64, ttl *time.Time) error {
	i := len(node.Keys) - 1

	if node.Leaf {
		// Check if the key already exists
		for j := 0; j <= i; j++ {
			if bytes.Equal(node.Keys[j].K, key) {
				// Append the new value to the existing key
				node.Keys[j].V = append(node.Keys[j].V, valuePage)
				return bspt.writeNode(node)
			}
		}

		// Insert the key in the correct position
		node.Keys = append(node.Keys, nil)
		for i >= 0 && lessThan(key, node.Keys[i].K) {
			node.Keys[i+1] = node.Keys[i]
			i--
		}
		node.Keys[i+1] = &Key{K: key, V: []int64{valuePage}, TTL: ttl}
		return bspt.writeNode(node)
	}

	// Find the child to insert the key
	for i >= 0 && lessThan(key, node.Keys[i].K) {
		i--
	}
	i++

	childBytes, err := bspt.Pager.GetPage(node.Children[i])
	if err != nil {
		return err
	}
	child, err := decodeNode(childBytes, bspt.Compress)
	if err != nil {
		return err
	}

	if len(child.Keys) == 2*bspt.T-1 {
		// Check if the right sibling has space
		if i+1 < len(node.Children) {
			rightSiblingBytes, err := bspt.Pager.GetPage(node.Children[i+1])
			if err != nil {
				return err
			}
			rightSibling, err := decodeNode(rightSiblingBytes, bspt.Compress)
			if err != nil {
				return err
			}

			if len(rightSibling.Keys) < 2*bspt.T-1 {
				err = bspt.redistributeKeys(node, child, rightSibling, i)
				if err != nil {
					return err
				}
			} else {
				err = bspt.splitChild(node, i, child)
				if err != nil {
					return err
				}
			}
		} else {
			err = bspt.splitChild(node, i, child)
			if err != nil {
				return err
			}
		}

		if greaterThan(key, node.Keys[i].K) {
			i++
		}
	}

	childBytes, err = bspt.Pager.GetPage(node.Children[i])
	if err != nil {
		return err
	}
	child, err = decodeNode(childBytes, bspt.Compress)
	if err != nil {
		return err
	}

	return bspt.insertNonFull(child, key, valuePage, ttl)
}

// lessThan compares two values and returns true if a is less than b
func lessThan(a, b []byte) bool {
	return bytes.Compare(a, b) < 0
}

// greaterThan compares two values and returns true if a is greater than b
func greaterThan(a, b []byte) bool {
	return bytes.Compare(a, b) > 0
}

// equal compares two values and returns true if a is equal to b
func equal(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// PrintTree prints the tree (for debugging )
func (bspt *BStarPlusTree) PrintTree() error {
	root, err := bspt.getRoot()
	if err != nil {
		return err
	}
	err = bspt.printTree(root, "", true)
	if err != nil {
		return err
	}
	return nil
}

// printTree prints the tree (for debugging)
func (bspt *BStarPlusTree) printTree(node *Node, indent string, last bool) error {
	fmt.Print(indent)
	if last {
		fmt.Print("└── ")
		indent += "    "
	} else {
		fmt.Print("├── ")
		indent += "│   "
	}

	for _, key := range node.Keys {
		fmt.Printf("%v ", string(key.K))
	}
	fmt.Println()

	for i, child := range node.Children {
		cBytes, err := bspt.Pager.GetPage(child)
		if err != nil {
			return err
		}

		c, err := decodeNode(cBytes, bspt.Compress)
		if err != nil {
			return err
		}

		bspt.printTree(c, indent, i == len(node.Children)-1)
	}

	return nil
}

// HasNext returns true if there are more values in the key
func (it *KeyIterator) HasNext() bool {
	return it.index < len(it.key.V)
}

// Next returns the next value in the key
func (it *KeyIterator) Next() ([]byte, error) {
	if !it.HasNext() {
		return nil, errors.New("no more values")
	}

	valuePage := it.key.V[it.index]

	// read the value from the page
	valueBytes, err := it.bspt.Pager.GetPage(valuePage)
	if err != nil {
		return nil, err
	}

	value, err := decodeValue(valueBytes)

	if err != nil {
		return nil, nil
	}

	it.index++

	return value, nil
}

// Get retrieves a key from the BStarPlusTree
func (bspt *BStarPlusTree) Get(key []byte) (*KeyIterator, error) {
	root, err := bspt.getRoot()
	if err != nil {
		return nil, err
	}

	return bspt.get(root, key)
}

// get retrieves a key from the BStarPlusTree
func (bspt *BStarPlusTree) get(x *Node, key []byte) (*KeyIterator, error) {
	i := 0
	for i < len(x.Keys) && lessThan(x.Keys[i].K, key) {
		i++
	}

	if i < len(x.Keys) && equal(x.Keys[i].K, key) {
		return NewKeyIterator(x.Keys[i], bspt), nil
	} else if x.Leaf {
		return nil, errors.New("key not found")
	} else {
		childBytes, err := bspt.Pager.GetPage(x.Children[i])
		if err != nil {
			return nil, err
		}

		child, err := decodeNode(childBytes, bspt.Compress)
		if err != nil {
			return nil, err
		}

		return bspt.get(child, key)
	}
}

// writeNode encodes and writes a node to the pager
func (bspt *BStarPlusTree) writeNode(n *Node) error {
	encodedNode, err := encodeNode(n, bspt.Compress)
	if err != nil {
		return err
	}
	return bspt.Pager.WriteTo(n.Page, encodedNode)
}

// NewKeyIterator creates a new KeyIterator
func NewKeyIterator(key *Key, bspt *BStarPlusTree) *KeyIterator {
	return &KeyIterator{
		index: 0,
		key:   key,
		bspt:  bspt,
	}
}

// NewInOrderIterator creates a new InOrderIterator
func NewInOrderIterator(bspt *BStarPlusTree) (*InOrderIterator, error) {
	root, err := bspt.getRoot()
	if err != nil {
		return nil, err
	}
	it := &InOrderIterator{
		stack: []*Node{},
		index: -1,
		bspt:  bspt,
	}
	it.pushLeft(root)
	return it, nil
}

// pushLeft pushes all the left children of a node onto the stack
func (it *InOrderIterator) pushLeft(node *Node) {
	for node != nil {
		it.stack = append(it.stack, node)
		if len(node.Children) > 0 {
			childBytes, err := it.bspt.Pager.GetPage(node.Children[0])
			if err != nil {
				return
			}
			child, err := decodeNode(childBytes, it.bspt.Compress)
			if err != nil {
				return
			}
			node = child
		} else {
			node = nil
		}
	}
}

// HasNext returns true if there are more keys in the BStarPlusTree
func (it *InOrderIterator) HasNext() bool {
	return len(it.stack) > 0
}

// GetBSPT returns iterators BSPT instance
func (it *InOrderIterator) GetBSPT() *BStarPlusTree {
	return it.bspt
}

// Next returns the next key in the BStarPlusTree
func (it *InOrderIterator) Next() (*Key, error) {
	if len(it.stack) == 0 {
		return nil, errors.New("no more keys")
	}

	node := it.stack[len(it.stack)-1]
	it.stack = it.stack[:len(it.stack)-1]

	key := node.Keys[it.index+1]
	it.index++

	if it.index < len(node.Keys)-1 {
		it.stack = append(it.stack, node)
	} else {
		it.index = -1
		if len(node.Children) > it.index+2 {
			childBytes, err := it.bspt.Pager.GetPage(node.Children[it.index+2])
			if err != nil {
				return nil, err
			}
			child, err := decodeNode(childBytes, it.bspt.Compress)
			if err != nil {
				return nil, err
			}
			it.pushLeft(child)
		}
	}

	return key, nil
}

// HasPrev returns true if there are previous keys in the BStarPlusTree
func (it *InOrderIterator) HasPrev() bool {
	return len(it.stack) > 0 || it.index > 0
}

// Prev returns the previous key in the BStarPlusTree
func (it *InOrderIterator) Prev() (*Key, error) {
	if len(it.stack) == 0 && it.index <= 0 {
		return nil, errors.New("no previous keys")
	}

	if it.index > 0 {
		it.index--
		node := it.stack[len(it.stack)-1]
		return node.Keys[it.index], nil
	}

	node := it.stack[len(it.stack)-1]
	it.stack = it.stack[:len(it.stack)-1]

	if len(node.Children) > 0 {
		childBytes, err := it.bspt.Pager.GetPage(node.Children[len(node.Children)-1])
		if err != nil {
			return nil, err
		}
		child, err := decodeNode(childBytes, it.bspt.Compress)
		if err != nil {
			return nil, err
		}
		it.pushLeft(child)
	}

	it.index = len(node.Keys) - 1
	return node.Keys[it.index], nil
}

// redistributeKeys redistributes keys between a node and its sibling
func (bspt *BStarPlusTree) redistributeKeys(parent *Node, node *Node, sibling *Node, index int) error {
	isRightSibling := index < len(parent.Keys) && lessThan(parent.Keys[index].K, sibling.Keys[0].K)

	combinedKeys := append(node.Keys, parent.Keys[index])
	combinedKeys = append(combinedKeys, sibling.Keys...)
	var combinedChildren []int64
	if !node.Leaf {
		combinedChildren = append(node.Children, sibling.Children...)
	}

	splitPoint := (len(combinedKeys) + 1) / 2

	if isRightSibling {
		node.Keys = combinedKeys[:splitPoint]
		parent.Keys[index] = combinedKeys[splitPoint]
		sibling.Keys = combinedKeys[splitPoint+1:]
		if !node.Leaf {
			node.Children = combinedChildren[:splitPoint+1]
			sibling.Children = combinedChildren[splitPoint+1:]
		}
	} else {
		sibling.Keys = combinedKeys[:splitPoint]
		parent.Keys[index] = combinedKeys[splitPoint]
		node.Keys = combinedKeys[splitPoint+1:]
		if !node.Leaf {
			sibling.Children = combinedChildren[:splitPoint+1]
			node.Children = combinedChildren[splitPoint+1:]
		}
	}

	if err := bspt.writeNode(node); err != nil {
		return err
	}
	if err := bspt.writeNode(sibling); err != nil {
		return err
	}
	return bspt.writeNode(parent)
}
