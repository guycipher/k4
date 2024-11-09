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
	size := 100
	numHashFuncs := 3
	bf := New(size, numHashFuncs)

	if len(bf.bitArray) != size {
		t.Errorf("expected bit array size %d, got %d", size, len(bf.bitArray))
	}
	if len(bf.hashFuncs) != numHashFuncs {
		t.Errorf("expected number of hash functions %d, got %d", numHashFuncs, len(bf.hashFuncs))
	}
	if bf.count != 0 {
		t.Errorf("expected count 0, got %d", bf.count)
	}
	if bf.threshold != size*2 {
		t.Errorf("expected threshold %d, got %d", size*2, bf.threshold)
	}
}

func TestAddAndCheck(t *testing.T) {
	bf := New(100, 3)
	key := []byte("test_key")
	index := int64(42)

	bf.Add(key, index)
	exists, idx := bf.Check(key)

	if !exists {
		t.Errorf("expected key to exist")
	}
	if idx != index {
		t.Errorf("expected index %d, got %d", index, idx)
	}
}

func TestResize(t *testing.T) {
	bf := New(10, 3)
	for i := 0; i < 80; i++ {
		key := []byte{byte(i)}
		bf.Add(key, int64(i))
	}

	if bf.count != 80 {
		t.Errorf("expected count 80, got %d", bf.count)
	}
	if len(bf.bitArray) != 40 {
		t.Errorf("expected bit array size 40, got %d", len(bf.bitArray))
	}
}

func TestSerializeDeserialize(t *testing.T) {
	bf := New(100, 3)
	key := []byte("test_key")
	index := int64(42)
	bf.Add(key, index)

	data, err := bf.Serialize()
	if err != nil {
		t.Fatalf("serialization failed: %v", err)
	}

	newBf, err := Deserialize(data, 3)
	if err != nil {
		t.Fatalf("deserialization failed: %v", err)
	}

	newBf.hashFuncs = bf.hashFuncs // Re-initialize HashFuncs

	exists, idx := newBf.Check(key)
	if !exists {
		t.Errorf("expected key to exist")
	}
	if idx != index {
		t.Errorf("expected index %d, got %d", index, idx)
	}
}

func TestAddAndCheckMany(t *testing.T) {
	bf := New(10, 3)
	keys := make([][]byte, 200)
	indices := make([]int64, 200)

	for i := 0; i < 200; i++ {
		keys[i] = []byte("key" + fmt.Sprintf("%d", i))
		indices[i] = int64(i)
		bf.Add(keys[i], indices[i])
	}

	// serialize and deserialize
	data, err := bf.Serialize()
	if err != nil {
		t.Fatalf("serialization failed: %v", err)
	}

	newBf, err := Deserialize(data, 3)
	if err != nil {
		t.Fatalf("deserialization failed: %v", err)
	}

	for i, key := range keys {
		exists, idx := newBf.Check(key)
		if !exists {
			t.Errorf("expected key %s to exist", key)
		}
		if idx != indices[i] {
			t.Errorf("expected index %d, got %d", indices[i], idx)
		}
	}
}
