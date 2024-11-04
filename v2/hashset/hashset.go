// Package hashset
// This hashset is mainly in replacement for the bloom filter implementation in K4
// Bloom filters are more compact yes, but they are not as efficient as a hashset
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
package hashset

import (
	"bytes"
	"encoding/gob"
	"github.com/guycipher/k4/v2/murmur"
)

const initialCapacity = 32
const loadFactorThreshold = 0.7

// HashSet represents a hash set.
type HashSet struct {
	Buckets  [][]interface{}
	Size     int
	Capacity int
}

// NewHashSet creates a new instance of HashSet.
func NewHashSet() *HashSet {
	return &HashSet{
		Buckets:  make([][]interface{}, initialCapacity),
		Capacity: initialCapacity,
	}
}

// Hash function to compute the index for a given value.
func (h *HashSet) hash(value []byte, capacity int) int {
	return int(murmur.Hash64(value, 4) % uint64(capacity))
}

// Add inserts a new element into the set.
func (h *HashSet) Add(value []byte) {
	index := h.hash(value, h.Capacity)
	for _, item := range h.Buckets[index] {
		if bytes.Equal(item.([]byte), value) {
			return // Element already exists
		}
	}
	h.Buckets[index] = append(h.Buckets[index], value)
	h.Size++

	// Resize if the load factor is too high
	if float64(h.Size)/float64(h.Capacity) > loadFactorThreshold {
		h.resize()
	}
}

// Resize increases the capacity of the hash set.
func (h *HashSet) resize() {
	newCapacity := h.Capacity * 2
	newBuckets := make([][]interface{}, newCapacity)

	for _, bucket := range h.Buckets {
		for _, value := range bucket {
			newIndex := h.hash(value.([]byte), newCapacity)
			newBuckets[newIndex] = append(newBuckets[newIndex], value)
		}
	}

	h.Buckets = newBuckets
	h.Capacity = newCapacity
}

// Remove deletes an element from the set.
func (h *HashSet) Remove(value []byte) {
	index := h.hash(value, h.Capacity)
	for i, item := range h.Buckets[index] {
		if bytes.Equal(item.([]byte), value) {
			h.Buckets[index] = append(h.Buckets[index][:i], h.Buckets[index][i+1:]...)
			h.Size--
			return
		}
	}
}

// Contains checks if an element is in the set.
func (h *HashSet) Contains(value []byte) bool {
	index := h.hash(value, h.Capacity)
	for _, item := range h.Buckets[index] {
		if bytes.Equal(item.([]byte), value) {
			return true
		}
	}
	return false
}

// Clear removes all elements from the set.
func (h *HashSet) Clear() {
	h.Buckets = make([][]interface{}, initialCapacity)
	h.Size = 0
	h.Capacity = initialCapacity
}

// Serialize encodes the HashSet into a byte slice.
func (h *HashSet) Serialize() ([]byte, error) {
	// We just use gob to encode the HashSet
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(h)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize decodes the byte slice into a HashSet.
func Deserialize(data []byte) (*HashSet, error) {
	// We just use gob to decode the byte slice
	var h HashSet
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&h)
	if err != nil {
		return nil, err
	}
	return &h, nil
}
