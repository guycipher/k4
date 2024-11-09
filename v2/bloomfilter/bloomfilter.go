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

// BloomFilter is the main struct for the bloom filter package
type BloomFilter struct {
	bitArray    []bool                // Bit array to store the bloom filter
	hashFuncs   []func([]byte) uint32 // Hash functions
	keyIndexMap map[string]int64      // Map to store key-index pairs
	count       int                   // Number of keys in the bloom filter
	threshold   int                   // Threshold for resizing the bloom filter
}

// New creates a new BloomFilter with the given size and number of hash functions
func New(size int, numHashFuncs int) *BloomFilter {
	hashFuncs := make([]func([]byte) uint32, numHashFuncs) // Create hash functions

	// Create hash functions
	for i := 0; i < numHashFuncs; i++ {
		seed := uint32(i)
		hashFuncs[i] = func(data []byte) uint32 {
			return murmur.Hash32(data, seed) // Return the hash value
		}
	}
	return &BloomFilter{
		bitArray:    make([]bool, size),
		hashFuncs:   hashFuncs,
		keyIndexMap: make(map[string]int64),
		threshold:   size * 2,
	}
}

// Add adds a key to the bloom filter
func (bf *BloomFilter) Add(key []byte, index int64) {
	// Resize the bloom filter if the count exceeds the threshold
	if bf.count >= bf.threshold {
		bf.resize()
	}
	// Add the key to the bloom filter
	for _, hashFunc := range bf.hashFuncs {
		position := hashFunc(key) % uint32(len(bf.bitArray))
		bf.bitArray[position] = true
	}
	bf.keyIndexMap[string(key)] = index // Add the key to the key-index map
	bf.count++
}

// Check checks if a key exists in the bloom filter
func (bf *BloomFilter) Check(key []byte) (bool, int64) {
	for _, hashFunc := range bf.hashFuncs { // Iterate over the hash functions
		position := hashFunc(key) % uint32(len(bf.bitArray))
		if !bf.bitArray[position] {
			return false, -1
		}
	}
	index, exists := bf.keyIndexMap[string(key)] // Check if the key exists in the key-index map
	return exists, index
}

// resize resizes the bloom filter
func (bf *BloomFilter) resize() {
	newSize := len(bf.bitArray) * 2 // Double the size of the bit array
	newBitArray := make([]bool, newSize)
	// Rehash the keys
	for key := range bf.keyIndexMap {
		for _, hashFunc := range bf.hashFuncs {
			position := hashFunc([]byte(key)) % uint32(newSize)
			newBitArray[position] = true
		}
	}
	bf.bitArray = newBitArray
	bf.threshold = newSize * 2
}

// Serialize serializes the bloom filter
func (bf *BloomFilter) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Serialize BitArray
	bitArraySize := int32(len(bf.bitArray))
	if err := binary.Write(&buf, binary.LittleEndian, bitArraySize); err != nil {
		return nil, err
	}
	for _, bit := range bf.bitArray {
		var b byte
		if bit {
			b = 1
		} else {
			b = 0
		}
		if err := buf.WriteByte(b); err != nil {
			return nil, err
		}
	}

	// Serialize KeyIndexMap
	keyIndexMapSize := int32(len(bf.keyIndexMap))
	if err := binary.Write(&buf, binary.LittleEndian, keyIndexMapSize); err != nil {
		return nil, err
	}
	for key, index := range bf.keyIndexMap {
		keyLen := int32(len(key))
		if err := binary.Write(&buf, binary.LittleEndian, keyLen); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(key); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, index); err != nil {
			return nil, err
		}
	}

	// Serialize Count and Threshold
	if err := binary.Write(&buf, binary.LittleEndian, int32(bf.count)); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, int32(bf.threshold)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Deserialize deserializes the bloom filter
func Deserialize(data []byte) (*BloomFilter, error) {
	buf := bytes.NewBuffer(data)
	bf := &BloomFilter{}

	// Deserialize BitArray
	var bitArraySize int32
	if err := binary.Read(buf, binary.LittleEndian, &bitArraySize); err != nil {
		return nil, err
	}
	bf.bitArray = make([]bool, bitArraySize)
	for i := int32(0); i < bitArraySize; i++ {
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		bf.bitArray[i] = b == 1
	}

	// Deserialize KeyIndexMap
	var keyIndexMapSize int32
	if err := binary.Read(buf, binary.LittleEndian, &keyIndexMapSize); err != nil {
		return nil, err
	}
	bf.keyIndexMap = make(map[string]int64, keyIndexMapSize)
	for i := int32(0); i < keyIndexMapSize; i++ {
		var keyLen int32
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		key := make([]byte, keyLen)
		if _, err := buf.Read(key); err != nil {
			return nil, err
		}
		var index int64
		if err := binary.Read(buf, binary.LittleEndian, &index); err != nil {
			return nil, err
		}
		bf.keyIndexMap[string(key)] = index
	}

	// Deserialize Count and Threshold
	var count, threshold int32
	if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &threshold); err != nil {
		return nil, err
	}
	bf.count = int(count)
	bf.threshold = int(threshold)

	return bf, nil
}
