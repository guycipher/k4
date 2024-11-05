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
	"github.com/guycipher/k4/pager"
	"os"
	"time"
)

// BStarPlusTree is the main struct for the B*+Tree
type BStarPlusTree struct {
	Pager *pager.Pager // The pager for the bstarplustree
	T     int          // The order of the tree
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
}

// InOrderIterator is an iterator for the keys of the BStarPlusTree in order
type InOrderIterator struct {
	stack []*Node
	index int
	bpt   *BStarPlusTree
}

// Open opens a new or existing BStarPlusTree
func Open(name string, flag, perm int, t int) (*BStarPlusTree, error) {
	if t < 2 {
		return nil, errors.New("t must be greater than 1")
	}

	pager, err := pager.OpenPager(name, flag, os.FileMode(perm))
	if err != nil {
		return nil, err
	}

	return &BStarPlusTree{
		T:     t,
		Pager: pager,
	}, nil
}

// Close closes the BStarPlusTree
func (b *BStarPlusTree) Close() error {
	return b.Pager.Close()
}

// encodeNode encodes a node into a byte slice
func encodeNode(n *Node) ([]byte, error) {
	buff := new(bytes.Buffer)
	enc := gob.NewEncoder(buff)
	err := enc.Encode(n)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// newNode creates a new BStarPlusTree node
func (b *BStarPlusTree) newNode(leaf bool) (*Node, error) {
	newNode := &Node{
		Leaf: leaf,
		Keys: make([]*Key, 0),
	}

	encodedNode, err := encodeNode(newNode)
	if err != nil {
		return nil, err
	}

	newNode.Page, err = b.Pager.Write(encodedNode)
	if err != nil {
		return nil, err
	}

	encodedNode, err = encodeNode(newNode)
	if err != nil {
		return nil, err
	}

	err = b.Pager.WriteTo(newNode.Page, encodedNode)
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
func decodeNode(data []byte) (*Node, error) {
	node := &Node{}
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// getRoot returns the root of the BStarPlusTree
func (b *BStarPlusTree) getRoot() (*Node, error) {
	root, err := b.Pager.GetPage(0)
	if err != nil {
		if err.Error() == "EOF" {
			rootNode := &Node{
				Leaf:     true,
				Page:     0,
				Children: make([]int64, 0),
				Keys:     make([]*Key, 0),
			}

			encodedRoot, err := encodeNode(rootNode)
			if err != nil {
				return nil, err
			}

			err = b.Pager.WriteTo(0, encodedRoot)
			if err != nil {
				return nil, err
			}

			return rootNode, nil
		} else {
			return nil, err
		}
	}

	rootNode, err := decodeNode(root)
	if err != nil {
		return nil, err
	}

	return rootNode, nil
}

// splitRoot splits the root node
func (b *BStarPlusTree) splitRoot() error {
	oldRoot, err := b.getRoot()
	if err != nil {
		return err
	}

	newOldRoot, err := b.newNode(oldRoot.Leaf)
	if err != nil {
		return err
	}

	newOldRoot.Keys = oldRoot.Keys
	newOldRoot.Children = oldRoot.Children

	newRoot := &Node{
		Page:     0,
		Children: []int64{newOldRoot.Page},
	}

	err = b.splitChild(newRoot, 0, newOldRoot)
	if err != nil {
		return err
	}

	encodedNewRoot, err := encodeNode(newRoot)
	if err != nil {
		return err
	}

	err = b.Pager.WriteTo(newRoot.Page, encodedNewRoot)
	if err != nil {
		return err
	}

	encodedNewOldRoot, err := encodeNode(newOldRoot)
	if err != nil {
		return err
	}

	err = b.Pager.WriteTo(newOldRoot.Page, encodedNewOldRoot)
	if err != nil {
		return err
	}

	return nil
}

// splitChild splits a child node of x at index i
func (b *BStarPlusTree) splitChild(x *Node, i int, y *Node) error {
	z, err := b.newNode(y.Leaf)
	if err != nil {
		return err
	}

	z.Keys = append(z.Keys, y.Keys[b.T:]...)
	y.Keys = y.Keys[:b.T]

	if !y.Leaf {
		z.Children = append(z.Children, y.Children[b.T:]...)
		y.Children = y.Children[:b.T]
	} else {
		z.Next = y.Next
		y.Next = z.Page
	}

	x.Keys = append(x.Keys, nil)
	x.Children = append(x.Children, 0)

	for j := len(x.Keys) - 1; j > i; j-- {
		x.Keys[j] = x.Keys[j-1]
	}
	x.Keys[i] = y.Keys[b.T-1]

	y.Keys = y.Keys[:b.T-1]

	for j := len(x.Children) - 1; j > i+1; j-- {
		x.Children[j] = x.Children[j-1]
	}
	x.Children[i+1] = z.Page

	encodedY, err := encodeNode(y)
	if err != nil {
		return err
	}

	err = b.Pager.WriteTo(y.Page, encodedY)
	if err != nil {
		return err
	}

	encodedZ, err := encodeNode(z)
	if err != nil {
		return err
	}

	err = b.Pager.WriteTo(z.Page, encodedZ)
	if err != nil {
		return err
	}

	encodedX, err := encodeNode(x)
	if err != nil {
		return err
	}

	err = b.Pager.WriteTo(x.Page, encodedX)
	if err != nil {
		return err
	}

	return nil
}

// Put inserts a key into the BStarPlusTree
func (b *BStarPlusTree) Put(key, value []byte, ttl *time.Time) error {
	root, err := b.getRoot()
	if err != nil {
		return err
	}

	if len(root.Keys) == (2*b.T)-1 {
		err = b.splitRoot()
		if err != nil {
			return err
		}

		rootBytes, err := b.Pager.GetPage(0)
		if err != nil {
			return err
		}

		root, err = decodeNode(rootBytes)
		if err != nil {
			return err
		}
	}

	// Encode the value and write it to a new page
	encodedValue, err := encodeValue(value)
	if err != nil {
		return err
	}

	valuePage, err := b.Pager.Write(encodedValue)
	if err != nil {
		return err
	}

	err = b.insertNonFull(root, key, valuePage, ttl)
	if err != nil {
		return err
	}

	return nil
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
func (b *BStarPlusTree) PrintTree() error {
	root, err := b.getRoot()
	if err != nil {
		return err
	}
	err = b.printTree(root, "", true)
	if err != nil {
		return err
	}
	return nil
}

// printTree prints the tree (for debugging)
func (b *BStarPlusTree) printTree(node *Node, indent string, last bool) error {
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
		cBytes, err := b.Pager.GetPage(child)
		if err != nil {
			return err
		}

		c, err := decodeNode(cBytes)
		if err != nil {
			return err
		}

		b.printTree(c, indent, i == len(node.Children)-1)
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
func (b *BStarPlusTree) Get(key []byte) (*KeyIterator, error) {
	root, err := b.getRoot()
	if err != nil {
		return nil, err
	}

	return b.get(root, key)
}

// get retrieves a key from the BStarPlusTree
func (b *BStarPlusTree) get(x *Node, key []byte) (*KeyIterator, error) {
	i := 0
	for i < len(x.Keys) && lessThan(x.Keys[i].K, key) {
		i++
	}

	if i < len(x.Keys) && equal(x.Keys[i].K, key) {
		return NewKeyIterator(x.Keys[i], b), nil
	} else if x.Leaf {
		return nil, errors.New("key not found")
	} else {
		childBytes, err := b.Pager.GetPage(x.Children[i])
		if err != nil {
			return nil, err
		}

		child, err := decodeNode(childBytes)
		if err != nil {
			return nil, err
		}

		return b.get(child, key)
	}
}

// redistributeOrSplitChild redistributes keys between a node and its sibling or splits them if both are full
func (b *BStarPlusTree) redistributeOrSplitChild(x *Node, i int, y *Node) error {
	var sibling *Node
	var siblingIndex int

	// Check if the right sibling exists and has space
	if i+1 < len(x.Children) {
		siblingBytes, err := b.Pager.GetPage(x.Children[i+1])
		if err != nil {
			return err
		}
		sibling, err = decodeNode(siblingBytes)
		if err != nil {
			return err
		}
		siblingIndex = i + 1
	}

	// If the right sibling has space, redistribute keys
	if sibling != nil && len(sibling.Keys) < (2*b.T)-1 {
		// Combine keys and redistribute
		keys := append(y.Keys, x.Keys[i])
		keys = append(keys, sibling.Keys...)
		midIndex := (len(keys) + 1) / 2

		y.Keys = keys[:midIndex]
		x.Keys[i] = keys[midIndex]
		sibling.Keys = keys[midIndex+1:]

		// Update children if not leaf
		if !y.Leaf {
			children := append(y.Children, sibling.Children...)
			y.Children = children[:midIndex+1]
			sibling.Children = children[midIndex+1:]
		}

		// Write updated nodes to pager
		if err := b.writeNode(y); err != nil {
			return err
		}
		if err := b.writeNode(sibling); err != nil {
			return err
		}
		if err := b.writeNode(x); err != nil {
			return err
		}
		return nil
	}

	// If the left sibling exists and has space, redistribute keys
	if i > 0 {
		siblingBytes, err := b.Pager.GetPage(x.Children[i-1])
		if err != nil {
			return err
		}
		sibling, err = decodeNode(siblingBytes)
		if err != nil {
			return err
		}
		siblingIndex = i - 1
	}

	// If the left sibling has space, redistribute keys
	if sibling != nil && len(sibling.Keys) < (2*b.T)-1 {
		// Combine keys and redistribute
		keys := append(sibling.Keys, x.Keys[siblingIndex])
		keys = append(keys, y.Keys...)
		midIndex := (len(keys) + 1) / 2

		sibling.Keys = keys[:midIndex]
		x.Keys[siblingIndex] = keys[midIndex]
		y.Keys = keys[midIndex+1:]

		// Update children if not leaf
		if !y.Leaf {
			children := append(sibling.Children, y.Children...)
			sibling.Children = children[:midIndex+1]
			y.Children = children[midIndex+1:]
		}

		// Write updated nodes to pager
		if err := b.writeNode(sibling); err != nil {
			return err
		}
		if err := b.writeNode(y); err != nil {
			return err
		}
		if err := b.writeNode(x); err != nil {
			return err
		}
		return nil
	}

	// If both siblings are full, split into three nodes
	return b.splitChild(x, i, y)
}

// writeNode encodes and writes a node to the pager
func (b *BStarPlusTree) writeNode(n *Node) error {
	encodedNode, err := encodeNode(n)
	if err != nil {
		return err
	}
	return b.Pager.WriteTo(n.Page, encodedNode)
}

// insertNonFull inserts a key into a non-full node
func (b *BStarPlusTree) insertNonFull(x *Node, key []byte, value int64, ttl *time.Time) error {
	i := len(x.Keys) - 1

	if x.Leaf {
		// Find the position to insert the new key
		for i >= 0 && lessThan(key, x.Keys[i].K) {
			i--
		}

		// Check if the key already exists
		if i >= 0 && equal(key, x.Keys[i].K) {
			x.Keys[i].V = append(x.Keys[i].V, value)

			// update ttl
			if ttl != nil {
				x.Keys[i].TTL = ttl
			}

			encodedNode, err := encodeNode(x)
			if err != nil {
				return err
			}

			err = b.Pager.WriteTo(x.Page, encodedNode)
			if err != nil {
				return err
			}

			return nil
		} else {
			// Insert the new key
			x.Keys = append(x.Keys, nil)
			copy(x.Keys[i+2:], x.Keys[i+1:])
			if ttl != nil {
				x.Keys[i+1] = &Key{K: key, V: []int64{value}, TTL: ttl}
			} else {
				x.Keys[i+1] = &Key{K: key, V: []int64{value}}
			}
		}

		encodedNode, err := encodeNode(x)
		if err != nil {
			return err
		}

		err = b.Pager.WriteTo(x.Page, encodedNode)
		if err != nil {
			return err
		}

		return nil
	} else {
		// Find the child to recurse into
		for i >= 0 && lessThan(key, x.Keys[i].K) {
			i--
		}
		i++

		childBytes, err := b.Pager.GetPage(x.Children[i])
		if err != nil {
			return err
		}

		child, err := decodeNode(childBytes)
		if err != nil {
			return err
		}

		// If the child is full, split it
		if len(child.Keys) == (2*b.T)-1 {
			err = b.splitChild(x, i, child)
			if err != nil {
				return err
			}

			if greaterThan(key, x.Keys[i].K) {
				i++
			}
		}

		childBytes, err = b.Pager.GetPage(x.Children[i])
		if err != nil {
			return err
		}

		child, err = decodeNode(childBytes)
		if err != nil {
			return err
		}

		err = b.insertNonFull(child, key, value, ttl)
		if err != nil {
			return err
		}
	}
	return nil
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
func NewInOrderIterator(bpt *BStarPlusTree) (*InOrderIterator, error) {
	root, err := bpt.getRoot()
	if err != nil {
		return nil, err
	}
	it := &InOrderIterator{
		stack: []*Node{},
		index: -1,
		bpt:   bpt,
	}
	it.pushLeft(root)
	return it, nil
}

// pushLeft pushes all the left children of a node onto the stack
func (it *InOrderIterator) pushLeft(node *Node) {
	for node != nil {
		it.stack = append(it.stack, node)
		if len(node.Children) > 0 {
			childBytes, err := it.bpt.Pager.GetPage(node.Children[0])
			if err != nil {
				return
			}
			child, err := decodeNode(childBytes)
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
			childBytes, err := it.bpt.Pager.GetPage(node.Children[it.index+2])
			if err != nil {
				return nil, err
			}
			child, err := decodeNode(childBytes)
			if err != nil {
				return nil, err
			}
			it.pushLeft(child)
		}
	}

	return key, nil
}
