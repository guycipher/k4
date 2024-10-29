// Package skiplist
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
package skiplist

import (
	"bytes"
	"math/rand"
	"time"
)

const TOMBSTONE_VALUE = "$tombstone" // This is specific to k4

// Node represents a node in the skip list
type Node struct {
	key     []byte     // Binary key
	value   []byte     // Binary value
	forward []*Node    // Forward pointers
	ttl     *time.Time // Time to live for the node, can be nil
}

// SkipList represents a skip list
type SkipList struct {
	header   *Node   // Header node
	level    int     // Current level0
	size     int     // in bytes
	maxLevel int     // Maximum level
	p        float64 // Probability
}

// Iterator represents an iterator for the skip list
type Iterator interface {
	Next() bool                // Move to the next node
	Prev() bool                // Move to the previous node
	HasNext() bool             // Check if there is a next node
	HasPrev() bool             // Check if there is a previous node
	Current() ([]byte, []byte) // Get the current key and value
}

// NewNode creates a new node
func NewNode(level int, key, value []byte, ttl *time.Duration) *Node {
	var expiration *time.Time // Expiration time
	if ttl != nil {
		exp := time.Now().Add(*ttl)
		expiration = &exp
	}
	return &Node{
		key:     key,
		value:   value,
		forward: make([]*Node, level+1),
		ttl:     expiration,
	}
}

// Size returns the size of the node in bytes
func (n *Node) Size() int {
	return len(n.key) + len(n.value) + len(n.forward)*8 // assuming 64-bit pointers
}

// IsExpired checks if the node is expired
func (n *Node) IsExpired() bool {
	if n.ttl == nil {
		return false
	}
	return time.Now().After(*n.ttl)
}

// NewSkipList creates a new skip list
func NewSkipList(maxLevel int, p float64) *SkipList {
	rand.Seed(time.Now().UnixNano())
	return &SkipList{
		header:   NewNode(maxLevel, nil, nil, nil),
		level:    0,
		size:     0,
		maxLevel: maxLevel,
		p:        p,
	}
}

// randomLevel generates a random level for the new node
func (sl *SkipList) randomLevel() int {
	level := 0
	for rand.Float64() < sl.p && level < sl.maxLevel {
		level++
	}
	return level
}

// Insert inserts a new key-value pair into the skip list
func (sl *SkipList) Insert(key, value []byte, ttl *time.Duration) {
	update := make([]*Node, sl.maxLevel+1)
	current := sl.header

	// Traverse the skip list to find the position of the key
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && (string(current.forward[i].key) < string(key) || bytes.Equal(current.forward[i].value, []byte(TOMBSTONE_VALUE))) {
			if current.forward[i].IsExpired() || bytes.Equal(current.forward[i].value, []byte(TOMBSTONE_VALUE)) {
				// Skip nodes with tombstone values
				current = current.forward[i]
			} else {
				current = current.forward[i]
			}
		}
		update[i] = current
	}

	// Check if the key exists
	current = current.forward[0]
	if current != nil && string(current.key) == string(key) {
		// Key exists, update the value
		sl.size -= current.Size() // Subtract the size of the old value
		current.value = value
		if ttl != nil {
			exp := time.Now().Add(*ttl)
			current.ttl = &exp
		} else {
			current.ttl = nil
		}
		sl.size += current.Size() // Add the size of the new value
		return
	}

	// Key does not exist, proceed with insertion
	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level + 1; i <= level; i++ {
			update[i] = sl.header
		}
		sl.level = level
	}

	newNode := NewNode(level, key, value, ttl)
	for i := 0; i <= level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.size += newNode.Size()
}

// Delete deletes a key from the skip list
func (sl *SkipList) Delete(key []byte) {
	update := make([]*Node, sl.maxLevel+1)
	current := sl.header

	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && (string(current.forward[i].key) < string(key) || bytes.Equal(current.forward[i].value, []byte(TOMBSTONE_VALUE))) {
			if current.forward[i].IsExpired() || bytes.Equal(current.forward[i].value, []byte(TOMBSTONE_VALUE)) {
				// Skip nodes with tombstone values
				current = current.forward[i]
			} else {
				current = current.forward[i]
			}
		}
		update[i] = current
	}

	current = current.forward[0]
	if current != nil && string(current.key) == string(key) {
		for i := 0; i <= sl.level; i++ {
			if update[i].forward[i] != current {
				break
			}
			update[i].forward[i] = current.forward[i]
		}

		sl.size -= current.Size()

		for sl.level > 0 && sl.header.forward[sl.level] == nil {
			sl.level--
		}
	}
}

// Search searches for a key in the skip list
func (sl *SkipList) Search(key []byte) ([]byte, bool) {
	current := sl.header
	for i := sl.level; i >= 0; i-- {
		for current.forward[i] != nil && (string(current.forward[i].key) < string(key) || bytes.Equal(current.forward[i].value, []byte(TOMBSTONE_VALUE))) {
			if current.forward[i].IsExpired() || bytes.Equal(current.forward[i].value, []byte(TOMBSTONE_VALUE)) {
				// Skip nodes with tombstone values
				current = current.forward[i]
			} else {
				current = current.forward[i]
			}
		}
	}
	current = current.forward[0]
	if current != nil && string(current.key) == string(key) {
		if current.IsExpired() || bytes.Equal(current.value, []byte(TOMBSTONE_VALUE)) {
			return nil, false
		}
		return current.value, true
	}
	return nil, false
}

// SkipListIterator represents an iterator for the skip list
type SkipListIterator struct {
	skipList *SkipList // Skip list
	current  *Node     // Current node
}

// NewIterator creates a new iterator for the skip list
func NewIterator(sl *SkipList) *SkipListIterator {
	return &SkipListIterator{
		skipList: sl,
		current:  sl.header,
	}
}

// Next moves the iterator to the next node
func (it *SkipListIterator) Next() bool {
	for it.current.forward[0] != nil {
		it.current = it.current.forward[0]
		if !it.current.IsExpired() {
			return true
		}
		//it.skipList.Delete(it.current.key)
		//sl.Delete(current.key)
		// We mark as tombstone and delete later
		it.current.ttl = nil

		// Subtraction of the size of the old value
		it.skipList.size -= it.current.Size()
		// Mark as tombstone
		it.current.value = []byte(TOMBSTONE_VALUE)

		// Add the size of the tombstone
		it.skipList.size += it.current.Size()
	}
	return false
}

// Prev moves the iterator to the previous node
func (it *SkipListIterator) Prev() bool {
	// To move backward, we need to traverse from the header to the current node
	if it.current == it.skipList.header {
		return false
	}
	prev := it.skipList.header
	for i := it.skipList.level; i >= 0; i-- {
		for prev.forward[i] != nil && prev.forward[i] != it.current {
			prev = prev.forward[i]
		}
	}
	it.current = prev
	return true
}

// HasNext checks if there is a next node
func (it *SkipListIterator) HasNext() bool {
	next := it.current.forward[0]
	for next != nil && next.IsExpired() {
		it.skipList.Delete(next.key)
		next = it.current.forward[0]
	}
	return next != nil
}

// HasPrev checks if there is a previous node
func (it *SkipListIterator) HasPrev() bool {
	return it.current != it.skipList.header
}

// Current returns the current key and value
func (it *SkipListIterator) Current() ([]byte, []byte) {
	if it.current == it.skipList.header || it.current.IsExpired() {
		return nil, nil
	}
	return it.current.key, it.current.value
}

// Size returns the size of the skip list
func (sl *SkipList) Size() int {
	return sl.size
}

// Copy creates a copy of the skip list
func (sl *SkipList) Copy() *SkipList {
	newSkipList := NewSkipList(sl.maxLevel, sl.p)
	it := NewIterator(sl)
	for it.Next() {
		key, value := it.Current()
		newSkipList.Insert(key, value, nil)
	}
	return newSkipList

}
