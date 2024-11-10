// Package fuzz tests
// Fuzz generates random byte arrays, strings and key-value pairs for fuzz testing
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
package fuzz

import "testing"

func TestRandomStringUniqueness(t *testing.T) {
	const numGenerations = 1000000
	const strLength = 10

	seen := make(map[string]struct{}, numGenerations)

	for i := 0; i < numGenerations; i++ {
		s, _ := RandomString(strLength)
		if _, exists := seen[s]; exists {
			t.Fatalf("Duplicate string found: %s", s)
		}
		seen[s] = struct{}{}
	}
}

func TestRandomByteArrUniqueness(t *testing.T) {
	const numGenerations = 1000000
	const byteArrLength = 10

	seen := make(map[string]struct{}, numGenerations)

	for i := 0; i < numGenerations; i++ {
		b, _ := RandomByteArr(byteArrLength)
		s := string(b)
		if _, exists := seen[s]; exists {
			t.Fatalf("Duplicate byte array found: %s", s)
		}
		seen[s] = struct{}{}
	}
}

func TestGenerateKeyValuePairs(t *testing.T) {
	const numPairs = 1000000

	pairs := GenerateKeyValuePairs(numPairs)
	if len(pairs) != numPairs {
		t.Fatalf("Expected %d pairs, but got %d", numPairs, len(pairs))
	}

	seenKeys := make(map[string]struct{}, numPairs)
	for key := range pairs {
		if _, exists := seenKeys[key]; exists {
			t.Fatalf("Duplicate key found: %s", key)
		}
		seenKeys[key] = struct{}{}
	}
}
