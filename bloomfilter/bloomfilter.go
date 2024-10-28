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
	"hash"
	"hash/fnv"
)

// BloomFilter is the data structure that represents a Bloom filter
type BloomFilter struct {
	bitset    []bool        // bitset
	size      uint          // size of the Bloom filter
	hashFuncs []hash.Hash64 // hash functions
}

// NewBloomFilter initializes a new BloomFilter
func NewBloomFilter(size uint, numHashFuncs int) *BloomFilter {
	bf := &BloomFilter{
		bitset:    make([]bool, size),
		size:      size,
		hashFuncs: make([]hash.Hash64, numHashFuncs),
	}

	for i := 0; i < numHashFuncs; i++ {
		bf.hashFuncs[i] = fnv.New64()
	}

	return bf
}

// Add adds a key to the BloomFilter
func (bf *BloomFilter) Add(key []byte) {
	for _, hashFunc := range bf.hashFuncs {
		hashFunc.Reset()
		hashFunc.Write(key)
		index := hashFunc.Sum64() % uint64(bf.size)
		bf.bitset[index] = true
	}
}

// Check checks if a key is possibly in the BloomFilter
func (bf *BloomFilter) Check(key []byte) bool {
	for _, hashFunc := range bf.hashFuncs {
		hashFunc.Reset()
		hashFunc.Write(key)
		index := hashFunc.Sum64() % uint64(bf.size)
		if !bf.bitset[index] {
			return false
		}
	}
	return true
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
			bitsetBytes[i/8] |= 1 << (i % 8)
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
		bitset[i] = (bitsetBytes[i/8] & (1 << (i % 8))) != 0
	}

	// Reinitialize the hash functions
	hashFuncs := make([]hash.Hash64, numHashFuncs)
	for i := 0; i < int(numHashFuncs); i++ {
		hashFuncs[i] = fnv.New64()
	}

	return &BloomFilter{
		bitset:    bitset,
		size:      uint(size),
		hashFuncs: hashFuncs,
	}, nil
}
