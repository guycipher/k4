// Package murmur tests
// murmur3 inspired
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
	"github.com/guycipher/k4/v2/fuzz"
	"testing"
	"time"
)

func TestHash64(t *testing.T) {

	tests := []struct {
		key  []byte
		seed uint64
		want uint64
	}{
		{[]byte("hello"), 0, 0xf369cd39c641eb89},
		{[]byte("world"), 0, 0x96a5312ceeb4b275},
		{[]byte("murmur"), 0, 0xc40377c960d8b391},
		{[]byte("hash"), 0, 0xe7fcedc45a9406da},
	}

	for _, tt := range tests {
		t.Run(string(tt.key), func(t *testing.T) {
			if got := Hash64(tt.key, tt.seed); got != tt.want {
				t.Errorf("Hash64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHash64Many(t *testing.T) {
	// Generate a large number of keys
	keys := make([][]byte, 100000)
	for i := range keys {
		str, _ := fuzz.RandomString(10)
		keys[i] = []byte(str)
	}

	tt := time.Now()
	// Compute the hash of each key
	for _, key := range keys {
		_ = Hash64(key, 0)
	}

	t.Logf("Time taken to hash 100k keys %v", time.Since(tt))

}

func TestHash32(t *testing.T) {

	tests := []struct {
		key  []byte
		seed uint32
		want uint32
	}{
		{[]byte("hello"), 0, 0x248bfa47},
		{[]byte("world"), 0, 0xfb963cfb},
		{[]byte("murmur"), 0, 0x73f313cd},
		{[]byte("hash"), 0, 0x56c454fb},
	}

	for _, tt := range tests {
		t.Run(string(tt.key), func(t *testing.T) {
			if got := Hash32(tt.key, tt.seed); got != tt.want {
				t.Errorf("Hash32() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHash32Many(t *testing.T) {
	// Generate a large number of keys
	keys := make([][]byte, 100000)
	for i := range keys {
		str, _ := fuzz.RandomString(10)
		keys[i] = []byte(str)
	}

	tt := time.Now()
	// Compute the hash of each key
	for _, key := range keys {
		_ = Hash32(key, 0)
	}

	t.Logf("Time taken to hash 100k keys %v", time.Since(tt))

}

func BenchmarkHash64(b *testing.B) {
	key := []byte("benchmarking 64-bit murmur3 hash function")
	seed := uint64(0)

	for i := 0; i < b.N; i++ {
		_ = Hash64(key, seed)
	}
}

func BenchmarkHash32(b *testing.B) {
	key := []byte("benchmarking 32-bit murmur3 hash function")
	seed := uint32(0)

	for i := 0; i < b.N; i++ {
		_ = Hash32(key, seed)
	}
}
