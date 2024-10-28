// Package k4 benchmarking
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
package k4

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func BenchmarkK4_Put(b *testing.B) {
	// Setup
	dir := "testdata"
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	k4, err := Open(dir, 1024*1024, 60, false)
	if err != nil {
		b.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	// Benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err := k4.Put(key, value, nil)
		if err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}
}

func BenchmarkK4_Get(b *testing.B) {
	// Setup
	dir := "testdata"
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	k4, err := Open(dir, 1024*1024, 60, false)
	if err != nil {
		b.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	// Insert some data
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err := k4.Put(key, value, nil)
		if err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", rand.Intn(1000)))
		_, err := k4.Get(key)
		if err != nil {
			b.Fatalf("Failed to get key: %v", err)
		}
	}
}

func BenchmarkK4_Delete(b *testing.B) {
	// Setup
	dir := "testdata"
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	k4, err := Open(dir, 1024*1024, 60, false)
	if err != nil {
		b.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	// Insert some data
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err := k4.Put(key, value, nil)
		if err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", rand.Intn(1000)))
		err := k4.Delete(key)
		if err != nil {
			b.Fatalf("Failed to delete key: %v", err)
		}
	}
}
