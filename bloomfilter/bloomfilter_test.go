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
	"github.com/guycipher/k4/fuzz"
	"github.com/guycipher/k4/pager"
	"os"
	"testing"
	"time"
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
	size := uint(10)
	numHashFuncs := 8
	bf := NewBloomFilter(size, numHashFuncs)

	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
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

func TestCheck2(t *testing.T) {
	tt := time.Now()
	bf := NewBloomFilter(1000000, 8)

	for i := 0; i < 10000; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		bf.Add(key)
	}

	t.Logf("Time to add 10k keys to bloomfilter: %v", time.Since(tt))

	serialized, err := bf.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize BloomFilter: %v", err)
	}

	bf, err = Deserialize(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize BloomFilter: %v", err)
	}

	tt = time.Now()

	// check all keys
	for i := 0; i < 10000; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		if !bf.Check(key) {
			t.Fatalf("Expected key %s to be present in BloomFilter, got not present", key)
		}
	}

	t.Logf("Time to check 10k keys in bloomfilter: %v", time.Since(tt))
}

func TestCheck3(t *testing.T) {
	tt := time.Now()
	bf := NewBloomFilter(1000000, 8)

	for i := 0; i < 10000; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		bf.Add(key)
	}

	t.Logf("Time to add 10k keys to bloomfilter: %v", time.Since(tt))

	serialized, err := bf.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize BloomFilter: %v", err)
	}

	// We write to file
	f, err := os.OpenFile("bloomfilter.test", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	defer os.Remove("bloomfilter.test")

	f.WriteAt(serialized, 0)

	// We read from file
	serialized = make([]byte, len(serialized))

	_, err = f.ReadAt(serialized, 0)
	if err != nil {
		t.Fatalf("Failed to read from file: %v", err)
	}

	bf, err = Deserialize(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize BloomFilter: %v", err)
	}

	tt = time.Now()

	// check all keys
	for i := 0; i < 10000; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		if !bf.Check(key) {
			t.Fatalf("Expected key %s to be present in BloomFilter, got not present", key)
		}
	}

	t.Logf("Time to check 10k keys in bloomfilter: %v", time.Since(tt))
}

func TestCheck4(t *testing.T) {
	tt := time.Now()
	bf := NewBloomFilter(100000, 8)

	keys := [][]byte{}

	for i := 0; i < 10000; i++ {
		str, _ := fuzz.RandomString(10)
		key := []byte(str)
		keys = append(keys, key)
		bf.Add(key)
	}

	t.Logf("Time to add 10k keys to bloomfilter: %v", time.Since(tt))

	serialized, err := bf.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize BloomFilter: %v", err)
	}

	// We write to file
	p, err := pager.OpenPager("bloomfilter.test", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	defer os.Remove("bloomfilter.test")
	defer p.Close()
	p.Write(serialized)

	// We read from file
	serialized = make([]byte, len(serialized))

	_, err = p.GetPage(0)
	if err != nil {
		t.Fatalf("Failed to read from file: %v", err)
	}

	bf, err = Deserialize(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize BloomFilter: %v", err)
	}

	tt = time.Now()

	// check all keys
	for _, key := range keys {
		if !bf.Check(key) {
			t.Fatalf("Expected key %s to be present in BloomFilter, got not present", key)
		}
	}

	t.Logf("Time to check 10k keys in bloomfilter: %v", time.Since(tt))
}
