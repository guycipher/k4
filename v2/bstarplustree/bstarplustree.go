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

// splitChild
func (bspt *BStarPlusTree) splitChild(parent *Node, index int, fullNode *Node) error {
	// Create a new node to hold the split keys and children
	newNode, err := bspt.newNode(fullNode.Leaf)
	if err != nil {
		return err
	}

	// Determine the midpoint for splitting
	mid := (len(fullNode.Keys) + 1) / 2

	// Move the keys and children from the full node to the new node
	newNode.Keys = append(newNode.Keys, fullNode.Keys[mid:]...)
	fullNode.Keys = fullNode.Keys[:mid]

	if !fullNode.Leaf {
		newNode.Children = append(newNode.Children, fullNode.Children[mid:]...)
		fullNode.Children = fullNode.Children[:mid]
	}

	// Insert the new node into the parent node
	parent.Children = append(parent.Children[:index+1], append([]int64{newNode.Page}, parent.Children[index+1:]...)...)
	parent.Keys = append(parent.Keys[:index], append([]*Key{fullNode.Keys[mid-1]}, parent.Keys[index:]...)...)
	fullNode.Keys = fullNode.Keys[:mid-1] // Adjust the keys in the full node

	// Write the updated nodes to the pager
	if err := bspt.writeNode(fullNode); err != nil {
		return err
	}
	if err := bspt.writeNode(newNode); err != nil {
		return err
	}
	if err := bspt.writeNode(parent); err != nil {
		return err
	}

	return nil
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

// insertNonFull inserts a key into a non-full node
func (bspt *BStarPlusTree) insertNonFull(node *Node, key []byte, value int64, ttl *time.Time) error {
	i := len(node.Keys) - 1

	if node.Leaf {
		// Check if the key already exists
		for j := 0; j <= i; j++ {
			if equal(key, node.Keys[j].K) {
				node.Keys[j].V = append(node.Keys[j].V, value)
				return bspt.writeNode(node)
			}
		}

		// Insert the key in sorted order
		node.Keys = append(node.Keys, nil) // Make space for new key
		for i >= 0 && lessThan(key, node.Keys[i].K) {
			node.Keys[i+1] = node.Keys[i]
			i--
		}
		node.Keys[i+1] = &Key{K: key, V: []int64{value}, TTL: ttl}
	} else {
		// Find the child to insert the key into
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

		if len(child.Keys) == (2*bspt.T)-1 {
			if i+1 < len(node.Children) {
				rightSiblingBytes, err := bspt.Pager.GetPage(node.Children[i+1])
				if err != nil {
					return err
				}
				rightSibling, err := decodeNode(rightSiblingBytes, bspt.Compress)
				if err != nil {
					return err
				}
				if len(rightSibling.Keys) < (2*bspt.T)-1 {
					err = bspt.redistributeKeys(node, child, rightSibling, i)
					if err != nil {
						return err
					}
				} else if i-1 >= 0 {
					leftSiblingBytes, err := bspt.Pager.GetPage(node.Children[i-1])
					if err != nil {
						return err
					}
					leftSibling, err := decodeNode(leftSiblingBytes, bspt.Compress)
					if err != nil {
						return err
					}
					if len(leftSibling.Keys) < (2*bspt.T)-1 {
						err = bspt.redistributeKeys(node, leftSibling, child, i-1)
						if err != nil {
							return err
						}
					} else {
						err = bspt.splitChild(node, i, child)
						if err != nil {
							return err
						}
						if greaterThan(key, node.Keys[i].K) {
							i++
						}
					}
				} else {
					err = bspt.splitChild(node, i, child)
					if err != nil {
						return err
					}
					if greaterThan(key, node.Keys[i].K) {
						i++
					}
				}
			} else {
				err = bspt.splitChild(node, i, child)
				if err != nil {
					return err
				}
				if greaterThan(key, node.Keys[i].K) {
					i++
				}
			}
		}
		err = bspt.insertNonFull(child, key, value, ttl)
		if err != nil {
			return err
		}
	}

	return bspt.writeNode(node)
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
	if index < len(parent.Keys) && lessThan(parent.Keys[index].K, sibling.Keys[0].K) {
		// Redistribute keys from sibling to node
		node.Keys = append(node.Keys, parent.Keys[index])
		parent.Keys[index] = sibling.Keys[0]
		sibling.Keys = sibling.Keys[1:]
		if !sibling.Leaf {
			node.Children = append(node.Children, sibling.Children[0])
			sibling.Children = sibling.Children[1:]
		}
	} else {
		// Redistribute keys from node to sibling
		sibling.Keys = append([]*Key{parent.Keys[index]}, sibling.Keys...)
		parent.Keys[index] = node.Keys[len(node.Keys)-1]
		node.Keys = node.Keys[:len(node.Keys)-1]
		if !sibling.Leaf {
			sibling.Children = append([]int64{node.Children[len(node.Children)-1]}, sibling.Children...)
			node.Children = node.Children[:len(node.Children)-1]
		}
	}

	// Write the updated nodes to the pager
	if err := bspt.writeNode(node); err != nil {
		return err
	}
	if err := bspt.writeNode(sibling); err != nil {
		return err
	}
	if err := bspt.writeNode(parent); err != nil {
		return err
	}

	return nil
}
