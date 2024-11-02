// Package bloomfilter
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
	"bytes"
	"encoding/binary"
	"github.com/guycipher/k4/murmur"
	"math"
)

const GROWTH_FACTOR = 1.5
const SHOULD_GROW_THRESHOLD = 0.7

// BloomFilter represents a Bloom filter.
type BloomFilter struct {
	bitset    []bool                        // Bitset for the Bloom filter
	size      uint                          // Current size of the Bloom filter
	hashFuncs []func([]byte, uint64) uint64 // Hash functions
	keys      [][]byte                      // Original keys, maintained for resizing accuracy
}

// NewBloomFilter initializes a new BloomFilter.
func NewBloomFilter(size uint, numHashFuncs int) *BloomFilter {
	bf := &BloomFilter{
		bitset:    make([]bool, size),
		size:      size,
		hashFuncs: make([]func([]byte, uint64) uint64, numHashFuncs),
		keys:      make([][]byte, 0),
	}

	for i := 0; i < numHashFuncs; i++ {
		bf.hashFuncs[i] = murmur.Hash64
	}

	return bf
}

// Add adds a key to the BloomFilter.
func (bf *BloomFilter) Add(key []byte) {
	if bf.shouldGrow() {
		bf.resize(uint(float64(bf.size) * GROWTH_FACTOR)) // Resize using the growth factor
	}

	for _, hashFunc := range bf.hashFuncs {
		index := hashFunc(key, 0) % uint64(bf.size)
		bf.bitset[index] = true
	}
	bf.keys = append(bf.keys, key)
}

// Check checks if a key is possibly in the BloomFilter.
func (bf *BloomFilter) Check(key []byte) bool {
	for _, hashFunc := range bf.hashFuncs {
		index := hashFunc(key, 0) % uint64(bf.size)
		if !bf.bitset[index] {
			return false
		}
	}
	return true
}

// resize resizes the BloomFilter to a new size.
func (bf *BloomFilter) resize(newSize uint) {
	newBitset := make([]bool, newSize)

	// Calculate the optimal number of hash functions
	numKeys := len(bf.keys)
	newNumHashFuncs := int(math.Ceil(float64(newSize) / float64(numKeys) * math.Ln2))

	// Reinitialize the hash functions
	bf.hashFuncs = make([]func([]byte, uint64) uint64, newNumHashFuncs)
	for i := 0; i < newNumHashFuncs; i++ {
		bf.hashFuncs[i] = murmur.Hash64
	}

	// Re-add all keys
	for _, key := range bf.keys {
		for _, hashFunc := range bf.hashFuncs {
			index := hashFunc(key, 0) % uint64(newSize) // Use the new size
			newBitset[index] = true
		}
	}

	bf.bitset = newBitset
	bf.size = newSize
}

// shouldGrow checks if the BloomFilter should grow.
func (bf *BloomFilter) shouldGrow() bool {
	setBits := 0
	for _, bit := range bf.bitset {
		if bit {
			setBits++
		}
	}
	return setBits > int(float64(bf.size)*SHOULD_GROW_THRESHOLD)
}

// Serialize serializes the BloomFilter to a byte slice
func (bf *BloomFilter) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Write the size of the BloomFilter as uint32
	if err := binary.Write(&buf, binary.LittleEndian, uint32(bf.size)); err != nil {
		return nil, err
	}

	// Write the number of hash functions
	numHashFuncs := int32(len(bf.hashFuncs))
	if err := binary.Write(&buf, binary.LittleEndian, numHashFuncs); err != nil {
		return nil, err
	}

	// Convert bitset to byte slice and write it
	bitsetBytes := make([]byte, (bf.size+7)/8)
	for i, bit := range bf.bitset {
		if bit {
			bitsetBytes[i/8] |= 1 << (i % 8) // Set the i-th bit
		}
	}
	if _, err := buf.Write(bitsetBytes); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Deserialize deserializes a byte slice to a BloomFilter
func Deserialize(data []byte) (*BloomFilter, error) {
	buf := bytes.NewReader(data)

	// Read the size of the BloomFilter as uint32
	var size uint32
	if err := binary.Read(buf, binary.LittleEndian, &size); err != nil {
		return nil, err
	}

	// Read the number of hash functions
	var numHashFuncs int32
	if err := binary.Read(buf, binary.LittleEndian, &numHashFuncs); err != nil {
		return nil, err
	}

	// Read the bitset
	bitsetBytes := make([]byte, (size+7)/8)
	if _, err := buf.Read(bitsetBytes); err != nil {
		return nil, err
	}
	bitset := make([]bool, size)
	for i := range bitset {
		bitset[i] = (bitsetBytes[i/8] & (1 << (i % 8))) != 0 // Check if the i-th bit is set
	}

	// Reinitialize the hash functions
	hashFuncs := make([]func([]byte, uint64) uint64, numHashFuncs)
	for i := 0; i < int(numHashFuncs); i++ {
		hashFuncs[i] = murmur.Hash64 // Use murmur hash function
	}

	return &BloomFilter{
		bitset:    bitset,
		size:      uint(size),
		hashFuncs: hashFuncs,
		keys:      make([][]byte, 0), // Initialize keys slice
	}, nil
}
