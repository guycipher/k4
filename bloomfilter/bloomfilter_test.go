// Package bloomfilter tests
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
package bloomfilter

import (
	"fmt"
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	size := uint(100)
	numHashFuncs := 3
	bf := NewBloomFilter(size, numHashFuncs)

	if len(bf.bitset) != int(size) {
		t.Errorf("Expected bitset size %d, got %d", size, len(bf.bitset))
	}

	if len(bf.hashFuncs) != numHashFuncs {
		t.Errorf("Expected %d hash functions, got %d", numHashFuncs, len(bf.hashFuncs))
	}

	for _, hashFunc := range bf.hashFuncs {
		if hashFunc == nil {
			t.Error("Expected hash function to be initialized, got nil")
		}
	}
}

func TestAdd(t *testing.T) {
	bf := NewBloomFilter(100, 3)
	key := []byte("testkey")

	bf.Add(key)

	for _, hashFunc := range bf.hashFuncs {
		hashFunc.Reset()
		hashFunc.Write(key)
		index := hashFunc.Sum64() % uint64(bf.size)
		if !bf.bitset[index] {
			t.Errorf("Expected bitset[%d] to be true, got false", index)
		}
	}
}

func TestCheck(t *testing.T) {
	bf := NewBloomFilter(100, 3)
	key := []byte("testkey")
	otherKey := []byte("otherkey")

	bf.Add(key)

	if !bf.Check(key) {
		t.Error("Expected key to be present in BloomFilter, got not present")
	}

	if bf.Check(otherKey) {
		t.Error("Expected otherKey to be not present in BloomFilter, got present")
	}
}

func TestFalsePositives(t *testing.T) {
	bf := NewBloomFilter(100, 3)
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	for _, key := range keys {
		bf.Add(key)
	}

	falseKey := []byte("falsekey")
	if bf.Check(falseKey) {
		t.Error("Expected falseKey to be not present in BloomFilter, got present")
	}
}

func TestAddAndCheckMultipleKeys(t *testing.T) {
	size := uint(1000000)
	numHashFuncs := 8
	bf := NewBloomFilter(size, numHashFuncs)

	keys := make([][]byte, 1000000)
	for i := 0; i < 1000000; i++ {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
	}

	// Add each key to the BloomFilter
	for _, key := range keys {
		bf.Add(key)
	}

	// Check if each key is reported as present in the BloomFilter
	for _, key := range keys {
		if !bf.Check(key) {
			t.Errorf("Expected key %s to be present in BloomFilter, got not present", key)
		}
	}
}

func TestSerialize(t *testing.T) {
	bf := NewBloomFilter(100, 3)
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	for _, key := range keys {
		bf.Add(key)
	}

	data, err := bf.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize BloomFilter: %v", err)
	}

	if data == nil {
		t.Error("Expected serialized data to be non-nil")
	}
}

func TestDeserialize(t *testing.T) {
	bf := NewBloomFilter(100, 3)
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}

	for _, key := range keys {
		bf.Add(key)
	}

	data, err := bf.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize BloomFilter: %v", err)
	}

	deserializedBF, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Failed to deserialize BloomFilter: %v", err)
	}

	for _, key := range keys {
		if !deserializedBF.Check(key) {
			t.Errorf("Expected key %s to be present in deserialized BloomFilter, got not present", key)
		}
	}

	// Check a key that was not added
	falseKey := []byte("falsekey")
	if deserializedBF.Check(falseKey) {
		t.Error("Expected falseKey to be not present in deserialized BloomFilter, got present")
	}
}
