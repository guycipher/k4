// Package skiplist tests
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
package skiplist

import (
	"testing"
	"time"
)

func TestNewSkipList(t *testing.T) {
	sl := NewSkipList(12, 0.25)
	if sl == nil {
		t.Fatal("Expected new skip list to be created")
	}
	if sl.level != 0 {
		t.Fatalf("Expected level to be 0, got %d", sl.level)
	}
	if sl.size != 0 {
		t.Fatalf("Expected size to be 0, got %d", sl.size)
	}
}

func TestInsertAndSearch(t *testing.T) {
	sl := NewSkipList(12, 0.25)
	key := []byte("key1")
	value := []byte("value1")
	sl.Insert(key, value, nil)

	val, found := sl.Search(key)
	if !found {
		t.Fatal("Expected to find the key")
	}
	if string(val) != string(value) {
		t.Fatalf("Expected value %s, got %s", value, val)
	}
}

func TestInsertWithTTL(t *testing.T) {
	sl := NewSkipList(12, 0.25)
	key := []byte("key1")
	value := []byte("value1")
	ttl := 1 * time.Second
	sl.Insert(key, value, &ttl)

	time.Sleep(2 * time.Second)
	_, found := sl.Search(key)
	if found {
		t.Fatal("Expected key to be expired and not found")
	}
}

func TestDelete(t *testing.T) {
	sl := NewSkipList(12, 0.25)
	key := []byte("key1")
	value := []byte("value1")
	sl.Insert(key, value, nil)

	sl.Delete(key)
	_, found := sl.Search(key)
	if found {
		t.Fatal("Expected key to be deleted")
	}
}

func TestSize(t *testing.T) {
	sl := NewSkipList(12, 0.25)
	key := []byte("key1")
	value := []byte("value1")
	sl.Insert(key, value, nil)

	// Correct the expected size calculation
	expectedSize := len(key) + len(value) + (sl.level+1)*8
	if sl.Size() != expectedSize {
		t.Fatalf("Expected size %d, got %d", expectedSize, sl.Size())
	}
}

func TestIterator(t *testing.T) {
	sl := NewSkipList(12, 0.25)
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")
	sl.Insert(key1, value1, nil)
	sl.Insert(key2, value2, nil)

	it := NewIterator(sl)
	if !it.Next() {
		t.Fatal("Expected iterator to move to the first element")
	}
	k, v := it.Current()
	if string(k) != string(key1) || string(v) != string(value1) {
		t.Fatalf("Expected key %s and value %s, got key %s and value %s", key1, value1, k, v)
	}
	if !it.Next() {
		t.Fatal("Expected iterator to move to the second element")
	}
	k, v = it.Current()
	if string(k) != string(key2) || string(v) != string(value2) {
		t.Fatalf("Expected key %s and value %s, got key %s and value %s", key2, value2, k, v)
	}
	if it.Next() {
		t.Fatal("Expected iterator to be at the end")
	}
}

func TestSearchNil(t *testing.T) {
	sl := NewSkipList(12, 0.25)
	key := []byte("key1")

	_, found := sl.Search(key)
	if found {
		t.Fatal("Expected key to not be found")
	}

	// and again, just in case
	_, found = sl.Search(key)
	if found {
		t.Fatal("Expected key to not be found")
	}
}

func TestInsertTombstone(t *testing.T) {
	sl := NewSkipList(12, 0.25)
	key := []byte("key1")
	value := []byte("$tombstone")

	sl.Insert(key, value, nil)
	_, found := sl.Search(key)
	if found {
		t.Fatal("Expected key to be deleted")
	}

	key = []byte("key2")
	value = []byte("$tombstone")

	sl.Insert(key, value, nil)
	_, found = sl.Search(key)
	if found {
		t.Fatal("Expected key to be deleted")
	}
}
