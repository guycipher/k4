// Package cuckoofilter tests
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
package cuckoofilter

import (
	"fmt"
	"testing"
)

func TestNewCuckooFilter(t *testing.T) {
	cf := NewCuckooFilter()

	if len(cf.Buckets) != 400 {
		t.Errorf("expected 400 buckets, got %d", len(cf.Buckets))
	}
	for _, bucket := range cf.Buckets {
		if bucket != 0 {
			t.Errorf("expected all buckets to be empty, got %d", bucket)
		}
	}
}

func TestInsertAndLookup(t *testing.T) {
	cf := NewCuckooFilter()
	key := []byte("testkey")
	prefix := int64(42)

	if !cf.Insert(prefix, key) {
		t.Errorf("expected Insert to return true")
	}

	if p, found := cf.Lookup(key); !found || p != prefix {
		t.Errorf("expected Lookup to find the key with prefix %d, got %d", prefix, p)
	}
}

func TestResize(t *testing.T) {
	cf := NewCuckooFilter()
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i)}
		cf.Insert(int64(i), key)
	}

	fmt.Println(len(cf.Buckets))

	if len(cf.Buckets) < 1000 {
		t.Errorf("expected buckets to be greater than 1000, got %d", len(cf.Buckets))
	}
}

func TestSerializeDeserialize(t *testing.T) {
	cf := NewCuckooFilter()
	key := []byte("testkey")
	prefix := int64(42)
	cf.Insert(prefix, key)

	data, err := cf.Serialize()
	if err != nil {
		t.Fatalf("expected no error during serialization, got %v", err)
	}

	deserializedCF, err := Deserialize(data)
	if err != nil {
		t.Fatalf("expected no error during deserialization, got %v", err)
	}

	if p, found := deserializedCF.Lookup(key); !found || p != prefix {
		t.Errorf("expected Lookup to find the key with prefix %d, got %d", prefix, p)
	}
}

func TestCuckooFilterSizeFor10MillionEntries(t *testing.T) {
	cf := NewCuckooFilter()
	numEntries := 10000000

	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		prefix := int64(i)
		cf.Insert(prefix, key)
	}

	fmt.Printf("Number of buckets: %d\n", len(cf.Buckets))
	fmt.Printf("Number of entries: %d\n", numEntries)

	// serialize and deserialize to check for errors
	data, err := cf.Serialize()
	if err != nil {
		t.Fatalf("expected no error during serialization, got %v", err)
	}

	fmt.Println(len(data))
}
