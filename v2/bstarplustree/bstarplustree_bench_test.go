// Package bstarplustree bench tests
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
	"os"
	"testing"
)

// BenchmarkPut benchmarks the Put method of BStarPlusTree
func BenchmarkPut(b *testing.B) {
	defer os.Remove("test.db")

	tree, err := Open("test.db", os.O_CREATE|os.O_RDWR, 0644, 3, false)
	if err != nil {
		b.Fatalf("Failed to open BStarPlusTree: %v", err)
	}
	defer tree.Close()

	for i := 0; i < b.N; i++ {
		key := []byte{byte(i % 256)}
		value := []byte{byte(i % 256)}
		if err := tree.Put(key, value, nil); err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}
}

// BenchmarkGet benchmarks the Get method of BStarPlusTree
func BenchmarkGet(b *testing.B) {
	defer os.Remove("test.db")
	tree, err := Open("test.db", os.O_CREATE|os.O_RDWR, 0644, 3, false)
	if err != nil {
		b.Fatalf("Failed to open BStarPlusTree: %v", err)
	}
	defer tree.Close()

	// Preload the tree with some data
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i % 256)}
		value := []byte{byte(i % 256)}
		if err := tree.Put(key, value, nil); err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := []byte{byte(i % 256)}
		_, err := tree.Get(key)
		if err != nil && err.Error() != "key not found" {
			b.Fatalf("Failed to get key: %v", err)
		}
	}
}

// BenchmarkInOrderIterator benchmarks the InOrderIterator of BStarPlusTree
func BenchmarkInOrderIterator(b *testing.B) {
	defer os.Remove("test.db")
	tree, err := Open("test.db", os.O_CREATE|os.O_RDWR, 0644, 3, false)
	if err != nil {
		b.Fatalf("Failed to open BStarPlusTree: %v", err)
	}
	defer tree.Close()

	// Preload the tree with some data
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i % 256)}
		value := []byte{byte(i % 256)}
		if err := tree.Put(key, value, nil); err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		it, err := NewInOrderIterator(tree)
		if err != nil {
			b.Fatalf("Failed to create InOrderIterator: %v", err)
		}

		for it.HasNext() {
			_, err := it.Next()
			if err != nil {
				b.Fatalf("Failed to iterate: %v", err)
			}
		}
	}
}
