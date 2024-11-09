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
	bf := New(size, numHashFuncs)

	if len(bf.bitset) != int(size) {
		t.Errorf("expected bitset size %d, got %d", size, len(bf.bitset))
	}
	if len(bf.hashFuncs) != numHashFuncs {
		t.Errorf("expected number of hash functions %d, got %d", numHashFuncs, len(bf.hashFuncs))
	}
}

func TestAddAndCheck(t *testing.T) {
	bf := New(100, 3)
	key := []byte("test_key")
	value := int64(42)

	bf.Add(key, value)
	exists, retrievedValue := bf.Check(key)

	if !exists {
		t.Errorf("expected key to exist")
	}
	if retrievedValue != value {
		t.Errorf("expected value %d, got %d", value, retrievedValue)
	}
}

func TestResize(t *testing.T) {
	bf := New(10, 3)
	for i := 0; i < 8; i++ {
		key := []byte{byte(i)}
		bf.Add(key, int64(i))
	}

	if len(bf.bitset) != 20 {
		t.Errorf("expected bitset size 20, got %d", len(bf.bitset))
	}

	bf.Add([]byte{9}, 9)
	if len(bf.bitset) <= 10 {
		t.Errorf("expected bitset to grow, got size %d", len(bf.bitset))
	}
}

func TestAddAndCheckMany(t *testing.T) {
	bf := New(100, 8)
	var keys [][]byte
	var values []int64

	for i := 0; i < 10_000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))

		keys = append(keys, key)
		values = append(values, int64(i))

	}

	// Add keys and values to the BloomFilter
	for i, key := range keys {
		bf.Add(key, values[i])
	}

	// Check if each key exists and retrieve its associated value
	for i, key := range keys {
		exists, retrievedValue := bf.Check(key)
		if !exists {
			t.Errorf("expected key %s to exist", key)
		}
		if retrievedValue != values[i] {
			t.Errorf("expected value %d for key %s, got %d", values[i], key, retrievedValue)
		}
	}
}

func TestSerializeDeserialize(t *testing.T) {
	bf := New(100, 3)
	key := []byte("test_key")
	value := int64(42)
	bf.Add(key, value)

	data, err := bf.Serialize()
	if err != nil {
		t.Fatalf("serialization failed: %v", err)
	}

	newBf, err := Deserialize(data)
	if err != nil {
		t.Fatalf("deserialization failed: %v", err)
	}

	exists, retrievedValue := newBf.Check(key)
	if !exists {
		t.Errorf("expected key to exist")
	}
	if retrievedValue != value {
		t.Errorf("expected value %d, got %d", value, retrievedValue)
	}
}

func TestShouldGrow(t *testing.T) {
	bf := New(10, 3)
	for i := 0; i < 7; i++ {
		key := []byte{byte(i)}
		bf.Add(key, int64(i))
	}

	if !bf.shouldGrow() {
		t.Errorf("expected BloomFilter to grow")
	}
}
