// Package bstarplustree tests
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
	"fmt"
	"os"
	"testing"
)

func TestBStarPlusTree_OpenClose(t *testing.T) {
	defer os.Remove("test.db")
	// Create a temporary file for testing
	file, err := os.Create("test.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	// Open the BStarPlusTree
	tree, err := Open(file.Name(), os.O_RDWR|os.O_CREATE, 0644, 4, false)
	if err != nil {
		t.Fatalf("Failed to open BStarPlusTree: %v", err)
	}

	// Close the BStarPlusTree
	err = tree.Close()
	if err != nil {
		t.Fatalf("Failed to close BStarPlusTree: %v", err)
	}
}

func TestBStarPlusTree_InsertRetrieve(t *testing.T) {
	defer os.Remove("test.db")
	// Create a temporary file for testing
	file, err := os.Create("test.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	// Open the BStarPlusTree
	tree, err := Open(file.Name(), os.O_RDWR|os.O_CREATE, 0644, 4, false)
	if err != nil {
		t.Fatalf("Failed to open BStarPlusTree: %v", err)
	}
	defer tree.Close()

	// Insert key-value pairs
	key := []byte("key1")
	value := []byte("value1")
	err = tree.Put(key, value, nil)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}

	// Retrieve the key-value pair
	iter, err := tree.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if !iter.HasNext() {
		t.Fatalf("Expected to find key: %v", key)
	}

	retrievedValue, err := iter.Next()
	if err != nil {
		t.Fatalf("Failed to get next value: %v", err)
	}

	if string(retrievedValue) != string(value) {
		t.Fatalf("Expected value %v, got %v", value, retrievedValue)
	}
}

func TestBStarPlusTree_Reopen(t *testing.T) {
	defer os.Remove("test.db")
	// Create a temporary file for testing
	file, err := os.Create("test.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	// Open the BStarPlusTree
	tree, err := Open(file.Name(), os.O_RDWR|os.O_CREATE, 0644, 4, false)
	if err != nil {
		t.Fatalf("Failed to open BStarPlusTree: %v", err)
	}

	// Insert some key-value pairs
	key1 := []byte("key1")
	value1 := []byte("value1")
	if err := tree.Put(key1, value1, nil); err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}

	key2 := []byte("key2")
	value2 := []byte("value2")
	if err := tree.Put(key2, value2, nil); err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}

	// Close the BStarPlusTree
	if err := tree.Close(); err != nil {
		t.Fatalf("Failed to close BStarPlusTree: %v", err)
	}

	// Reopen the BStarPlusTree
	tree, err = Open(file.Name(), os.O_RDWR, 0644, 4, false)
	if err != nil {
		t.Fatalf("Failed to reopen BStarPlusTree: %v", err)
	}
	defer tree.Close()

	// Verify the key-value pairs are still present
	it, err := tree.Get(key1)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	value, err := it.Next()
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if !bytes.Equal(value, value1) {
		t.Fatalf("Expected value %v, got %v", value1, value)
	}

	it, err = tree.Get(key2)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	value, err = it.Next()
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if !bytes.Equal(value, value2) {
		t.Fatalf("Expected value %v, got %v", value2, value)
	}
}

func TestBStarPlusTree_DuplicateKeys(t *testing.T) {
	defer os.Remove("test.db")
	// Create a temporary file for testing
	file, err := os.Create("test.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	// Open the BStarPlusTree
	tree, err := Open(file.Name(), os.O_RDWR|os.O_CREATE, 0644, 4, false)
	if err != nil {
		t.Fatalf("Failed to open BStarPlusTree: %v", err)
	}
	defer tree.Close()

	// Insert duplicate key-value pairs
	key := []byte("key1")
	value1 := []byte("value1")
	value2 := []byte("value2")
	err = tree.Put(key, value1, nil)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}
	err = tree.Put(key, value2, nil)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}

	// Retrieve the key-value pairs
	iter, err := tree.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	values := [][]byte{}
	for iter.HasNext() {
		v, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to get next value: %v", err)
		}
		values = append(values, v)
	}

	if len(values) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(values))
	}

	if string(values[0]) != string(value1) || string(values[1]) != string(value2) {
		t.Fatalf("Expected values %v and %v, got %v and %v", value1, value2, values[0], values[1])
	}
}

func TestBStarPlusTree_SplitNodes(t *testing.T) {
	defer os.Remove("test.db")
	// Create a temporary file for testing
	file, err := os.Create("test.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	// Open the BStarPlusTree
	tree, err := Open(file.Name(), os.O_RDWR|os.O_CREATE, 0644, 4, false)
	if err != nil {
		t.Fatalf("Failed to open BStarPlusTree: %v", err)
	}
	defer tree.Close()

	// Insert multiple key-value pairs to trigger node splits
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := tree.Put(key, value, nil)
		if err != nil {
			t.Fatalf("Failed to put key-value pair %d: %v", i, err)
		}
	}

	// Verify the structure of the tree
	err = tree.PrintTree()
	if err != nil {
		t.Fatalf("Failed to print tree: %v", err)
	}
}

func TestBStarPlusTree_Iterator(t *testing.T) {
	defer os.Remove("test.db")
	// Create a temporary file for testing
	file, err := os.Create("test.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	// Open the BStarPlusTree
	tree, err := Open(file.Name(), os.O_RDWR|os.O_CREATE, 0644, 4, false)
	if err != nil {
		t.Fatalf("Failed to open BStarPlusTree: %v", err)
	}
	defer tree.Close()

	// Insert multiple key-value pairs
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := tree.Put(key, value, nil)
		if err != nil {
			t.Fatalf("Failed to put key-value pair %d: %v", i, err)
		}
	}

	// Create an in-order iterator
	iter, err := NewInOrderIterator(tree)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}

	// Iterate over the tree and verify the order
	for i := 0; iter.HasNext(); i++ {
		key, err := iter.Next()
		if err != nil {
			t.Fatalf("Failed to get next key: %v", err)
		}

		expectedKey := []byte(fmt.Sprintf("key%d", i))
		if string(key.K) != string(expectedKey) {
			t.Fatalf("Expected key %v, got %v", expectedKey, key.K)
		}
	}

	// Iterate backwards over the tree and verify the order
	for i := 9; iter.HasPrev(); i-- {
		key, err := iter.Prev()
		if err != nil {
			t.Fatalf("Failed to get previous key: %v", err)
		}

		expectedKey := []byte(fmt.Sprintf("key%d", i))
		if string(key.K) != string(expectedKey) {
			t.Fatalf("Expected key %v, got %v", expectedKey, key.K)
		}
	}
}

func TestBStarPlusTree_NonRootNodesFullness(t *testing.T) {
	defer os.Remove("test.db")

	// Create a temporary file for testing
	file, err := os.Create("test.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	// Open the BStarPlusTree
	tree, err := Open(file.Name(), os.O_RDWR|os.O_CREATE, 0644, 4, false)
	if err != nil {
		t.Fatalf("Failed to open BStarPlusTree: %v", err)
	}
	defer tree.Close()

	// Insert multiple key-value pairs to trigger node splits
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err := tree.Put(key, value, nil)
		if err != nil {
			t.Fatalf("Failed to put key-value pair %d: %v", i, err)
		}
	}

	// Verify that non-root nodes are at least 2/3 full
	err = tree.PrintTree()
	if err != nil {
		t.Fatalf("Failed to print tree: %v", err)
	}
}
