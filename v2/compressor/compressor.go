// Package compressor
// A simple Lempel-Ziv 1977 inspired compression algorithm
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
package compressor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/guycipher/k4/v2/murmur"
)

// Compressor is the main compression package struct
type Compressor struct {
	windowSize int // defined window size for compression
}

// NewCompressor initiates a new compressor with provided window size spec
func NewCompressor(windowSize int) (*Compressor, error) {
	if windowSize <= 0 {
		return nil, fmt.Errorf("window size must be greater than 0")
	}

	return &Compressor{windowSize: windowSize}, nil
}

// Compress compresses the provided binary array/slice
func (c *Compressor) Compress(data []byte) []byte {
	var compressed bytes.Buffer
	dataLen := len(data)
	i := 0
	hashTable := make(map[uint64]int)

	for i < dataLen {
		matchLength, matchDistance := 0, 0
		if i+2 < dataLen {
			hashKey := murmur.Hash64(data[i:i+3], 0)

			if pos, found := hashTable[hashKey]; found && i-pos <= c.windowSize {
				j := 0
				for j < dataLen-i && data[pos+j] == data[i+j] {
					j++
				}
				matchLength = j
				matchDistance = i - pos
			}
			hashTable[hashKey] = i
		}

		if matchLength > 0 {
			binary.Write(&compressed, binary.BigEndian, uint16(matchDistance))
			compressed.WriteByte(byte(matchLength))
			i += matchLength
		} else {
			binary.Write(&compressed, binary.BigEndian, uint16(0))
			compressed.WriteByte(data[i])
			i++
		}
	}

	return compressed.Bytes()
}

// Decompress decompresses the provided binary array/slice
func (c *Compressor) Decompress(data []byte) []byte {
	var decompressed bytes.Buffer
	dataLen := len(data)
	i := 0
	hashTable := make(map[uint64]int)

	for i < dataLen {
		var matchDistance uint16
		binary.Read(bytes.NewReader(data[i:i+2]), binary.BigEndian, &matchDistance)
		matchLength := int(data[i+2])
		i += 3

		if matchDistance > 0 {
			start := decompressed.Len() - int(matchDistance)
			for j := 0; j < matchLength; j++ {
				decompressed.WriteByte(decompressed.Bytes()[start+j])
			}
		} else {
			decompressed.WriteByte(data[i-1])
		}

		// Update hash table with the new sequence
		if decompressed.Len() >= 3 {
			hashKey := murmur.Hash64(decompressed.Bytes()[decompressed.Len()-3:], 0)
			hashTable[hashKey] = decompressed.Len() - 3
		}
	}

	return decompressed.Bytes()
}
