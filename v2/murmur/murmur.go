// Package murmur
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
package murmur

import (
	"encoding/binary"
)

const (
	// Constants for 64-bit hash
	m64    = 0xff51afd7ed558ccd
	seed64 = 0xc4ceb9fe1a85ec53

	// Constants for 32-bit hash
	m32    = 0xcc9e2d51
	seed32 = 0x1b873593
)

// scramble64 performs the scrambling operation for 64-bit hash
func scramble64(k uint64) uint64 {
	k *= m64
	k = (k << 31) | (k >> 33) // Rotate left by 31 bits
	k *= seed64
	return k
}

// Hash64 computes a 64-bit MurmurHash3 hash for the given key and seed
func Hash64(key []byte, seed uint64) uint64 {
	h := seed // initialize hash with seed
	var k uint64

	// Process the input in 8-byte chunks
	for i := 0; i < len(key)/8; i++ {
		k = binary.LittleEndian.Uint64(key[i*8:])
		h ^= scramble64(k)
		h = (h << 27) | (h >> 37) // Rotate left by 27 bits
		h = h*5 + 0x52dce729
	}

	// Process the remaining bytes
	k = 0
	for i := 0; i < len(key)&7; i++ {
		k <<= 8
		k |= uint64(key[len(key)-1-i])
	}
	h ^= scramble64(k)

	// Finalize the hash
	h ^= uint64(len(key))
	h ^= h >> 33
	h *= m64
	h ^= h >> 33
	h *= seed64
	h ^= h >> 33

	return h // Return the final hash
}

// scramble32 performs the scrambling operation for 32-bit hash
func scramble32(k uint32) uint32 {
	k *= m32
	k = (k << 15) | (k >> 17) // Rotate left by 15 bits
	k *= seed32
	return k
}

// Hash32 computes a 32-bit MurmurHash3 hash for the given key and seed
func Hash32(key []byte, seed uint32) uint32 {
	h := seed // Initialize hash with seed
	var k uint32

	// Process the input in 4-byte chunks
	for i := 0; i < len(key)/4; i++ {
		k = binary.LittleEndian.Uint32(key[i*4:])
		h ^= scramble32(k)
		h = (h << 13) | (h >> 19) // Rotate left by 13 bits
		h = h*5 + 0xe6546b64
	}

	// Process the remaining bytes
	k = 0
	for i := 0; i < len(key)&3; i++ {
		k <<= 8
		k |= uint32(key[len(key)-1-i])
	}
	h ^= scramble32(k)

	// Finalize the hash
	h ^= uint32(len(key))
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16

	return h
}
