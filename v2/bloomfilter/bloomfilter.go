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
)

const LOAD_FACTOR = 0.7

// BloomFilter represents a Bloom filter.
type BloomFilter struct {
	bitset    []uint64                      // Bitset for the Bloom filter
	size      uint                          // Current size of the Bloom filter
	hashFuncs []func([]byte, uint64) uint64 // Hash functions
	capacity  uint                          // Number of elements added
	values    []int64                       // Store actual values here
}

// New initializes a new BloomFilter.
func New(size uint, numHashFuncs int) *BloomFilter {
	bf := &BloomFilter{
		bitset:    make([]uint64, size),                              // Initialize the bitset
		size:      size,                                              // Set the initial size
		hashFuncs: make([]func([]byte, uint64) uint64, numHashFuncs), // Initialize the hash functions
		capacity:  0,                                                 // Initialize capacity
		values:    make([]int64, size),                               // Array to store actual values
	}

	// Initialize the hash functions
	for i := 0; i < numHashFuncs; i++ {
		bf.hashFuncs[i] = murmur.Hash64 // Use murmur hash function
	}

	return bf // Return the BloomFilter
}

// shouldGrow determines if the BloomFilter needs to grow.
func (bf *BloomFilter) shouldGrow() bool {
	return float64(bf.capacity) >= LOAD_FACTOR*float64(bf.size)
}

// grow doubles the size of the BloomFilter and rehashes existing elements.
func (bf *BloomFilter) grow() {
	newSize := bf.size * 2
	newBitset := make([]uint64, newSize)
	newValues := make([]int64, newSize)

	// Rehash existing elements
	for i := uint(0); i < bf.size; i++ {
		if bf.bitset[i]&1 == 1 {
			value := bf.values[i] // Store the associated value
			for _, hashFunc := range bf.hashFuncs {
				index := hashFunc([]byte{byte(i)}, 0) % uint64(newSize)
				newBitset[index] = 1
				newValues[index] = value
			}
		}
	}

	bf.bitset = newBitset
	bf.values = newValues
	bf.size = newSize
}

// Add adds a key and its associated value to the BloomFilter.
func (bf *BloomFilter) Add(key []byte, value int64) {
	if bf.shouldGrow() {
		bf.grow()
	}

	// Add the key and value to the BloomFilter
	for _, hashFunc := range bf.hashFuncs {
		index := hashFunc(key, 0) % uint64(bf.size) // Use the current size
		bf.bitset[index] = 1                        // Set presence bit
		bf.values[index] = value                    // Store the actual value at that index
	}
	bf.capacity++
}

// Check checks if a key is possibly in the BloomFilter and retrieves its associated value.
func (bf *BloomFilter) Check(key []byte) (bool, int64) {
	// Check if the key is possibly in the BloomFilter
	for _, hashFunc := range bf.hashFuncs {
		index := hashFunc(key, 0) % uint64(bf.size)

		// If the presence bit is not set, the key is definitely not in the BloomFilter
		if bf.bitset[index]&1 == 0 {
			return false, 0
		}
	}
	// Retrieve the value from the associated index
	index := bf.hashFuncs[0](key, 0) % uint64(bf.size)
	return true, bf.values[index] // Return the actual value stored at that index
}

// Serialize serializes the BloomFilter to a byte slice
func (bf *BloomFilter) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Write the size of the BloomFilter as uint32
	if err := binary.Write(&buf, binary.LittleEndian, uint32(bf.size)); err != nil {
		return nil, err
	}

	// Write the number of hash functions
	numHashFuncs := int32(len(bf.hashFuncs)) // Get the number of hash functions
	if err := binary.Write(&buf, binary.LittleEndian, numHashFuncs); err != nil {
		return nil, err
	}

	// Write the bitset
	for _, bit := range bf.bitset {
		if err := binary.Write(&buf, binary.LittleEndian, bit); err != nil {
			return nil, err
		}
	}

	// Write the length of the values array
	valuesLen := int32(len(bf.values))
	if err := binary.Write(&buf, binary.LittleEndian, valuesLen); err != nil {
		return nil, err
	}

	// Write the values array
	for _, value := range bf.values {
		if err := binary.Write(&buf, binary.LittleEndian, value); err != nil {
			return nil, err
		}
	}

	// Write the capacity as uint32
	if err := binary.Write(&buf, binary.LittleEndian, uint32(bf.capacity)); err != nil {
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
	bitset := make([]uint64, size)
	for i := range bitset {
		if err := binary.Read(buf, binary.LittleEndian, &bitset[i]); err != nil {
			return nil, err
		}
	}

	// Read the length of the values array
	var valuesLen int32
	if err := binary.Read(buf, binary.LittleEndian, &valuesLen); err != nil {
		return nil, err
	}

	// Read the values array
	values := make([]int64, valuesLen)
	for i := range values {
		if err := binary.Read(buf, binary.LittleEndian, &values[i]); err != nil {
			return nil, err
		}
	}

	// Read the capacity as uint32
	var capacity uint32
	if err := binary.Read(buf, binary.LittleEndian, &capacity); err != nil {
		return nil, err
	}

	// Reinitialize the hash functions
	hashFuncs := make([]func([]byte, uint64) uint64, numHashFuncs)
	for i := 0; i < int(numHashFuncs); i++ {
		hashFuncs[i] = murmur.Hash64 // Use murmur hash function
	}

	return &BloomFilter{
		bitset:    bitset,         // Set the bitset
		size:      uint(size),     // Set the size
		hashFuncs: hashFuncs,      // Set the hash functions
		values:    values,         // Set the values array
		capacity:  uint(capacity), // Set the capacity
	}, nil
}
