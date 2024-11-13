// Package cuckoofilter implements a custom cuckoo filter data structure that allows for inserting and looking up keys with associated prefixes.
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
	"bytes"
	"encoding/gob"
	"github.com/guycipher/k4/murmur"
)

// Cuckoo Filter Configuration
const (
	initialFilterSize = 1000 // initial number of buckets in the filter
	maxBucketSize     = 8    // max number of elements per bucket
)

/*
	cf := NewCuckooFilter()
	numEntries := 10000000 // 10 million entries

	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		prefix := int64(i)
		cf.Insert(prefix, key)
	}
	...

initialFilterSize = 10000
maxBucketSize     = 4
Number of buckets: 163840000
Number of entries: 10000000
Encoded size 375340330

initialFilterSize = 1000
maxBucketSize     = 4
Number of buckets: 131072000
Number of entries: 10000000
Encoded size 342572346

initialFilterSize = 100
maxBucketSize     = 4
Number of buckets: 209715200
Number of entries: 10000000
Encoded size 421215578

initialFilterSize = 1000
maxBucketSize     = 8
Number of buckets: 65536000
Number of entries: 10000000
277036394 = 264.2 megabytes

*/

// CuckooFilter structure
type CuckooFilter struct {
	Buckets      []uint64
	KeyPrefixMap map[uint64]int64
}

// NewCuckooFilter creates a new cuckoo filter
func NewCuckooFilter() *CuckooFilter {
	return &CuckooFilter{
		Buckets:      make([]uint64, initialFilterSize*maxBucketSize),
		KeyPrefixMap: make(map[uint64]int64),
	}
}

// Hash the key into a single value using murmur
func (cf *CuckooFilter) hashKey(key []byte) uint64 {
	return murmur.Hash64(key, 0) // Using 0 as the seed
}

// Get two possible indices in the cuckoo filter for a hashed key
func (cf *CuckooFilter) getHashIndices(hashedKey uint64) (int, int) {
	filterSize := len(cf.Buckets) / maxBucketSize
	index1 := int(hashedKey % uint64(filterSize))
	index2 := int((hashedKey >> 32) % uint64(filterSize))
	return index1, index2
}

// Resize the cuckoo filter by doubling its size
func (cf *CuckooFilter) resize() {
	newFilterSize := len(cf.Buckets) * 2
	newBuckets := make([]uint64, newFilterSize)

	// Rehash all existing keys into the new buckets
	for i := 0; i < len(cf.Buckets); i++ {
		if cf.Buckets[i] != 0 {
			hashedKey := cf.Buckets[i]
			index1, index2 := cf.getHashIndices(hashedKey)
			inserted := false
			for k := 0; k < maxBucketSize; k++ {
				if newBuckets[index1*maxBucketSize+k] == 0 {
					newBuckets[index1*maxBucketSize+k] = hashedKey
					inserted = true
					break
				}
			}
			if !inserted {
				for k := 0; k < maxBucketSize; k++ {
					if newBuckets[index2*maxBucketSize+k] == 0 {
						newBuckets[index2*maxBucketSize+k] = hashedKey
						break
					}
				}
			}
		}
	}

	cf.Buckets = newBuckets
}

// Insert a key into the cuckoo filter with its prefix
func (cf *CuckooFilter) Insert(prefix int64, key []byte) bool {
	hashedKey := cf.hashKey(key)
	index1, index2 := cf.getHashIndices(hashedKey)

	// Try to insert into the first index
	for i := 0; i < maxBucketSize; i++ {
		if cf.Buckets[index1*maxBucketSize+i] == 0 {
			cf.Buckets[index1*maxBucketSize+i] = hashedKey
			cf.KeyPrefixMap[hashedKey] = prefix
			return true
		}
	}

	// If index1 is full, try to insert into index2
	for i := 0; i < maxBucketSize; i++ {
		if cf.Buckets[index2*maxBucketSize+i] == 0 {
			cf.Buckets[index2*maxBucketSize+i] = hashedKey
			cf.KeyPrefixMap[hashedKey] = prefix
			return true
		}
	}

	// If both buckets are full, resize and retry
	cf.resize()
	return cf.Insert(prefix, key)
}

// Lookup a key in the cuckoo filter and return its prefix if found
func (cf *CuckooFilter) Lookup(key []byte) (int64, bool) {
	hashedKey := cf.hashKey(key)
	index1, index2 := cf.getHashIndices(hashedKey)

	// Check the first index
	for i := 0; i < maxBucketSize; i++ {
		if cf.Buckets[index1*maxBucketSize+i] == hashedKey {
			return cf.KeyPrefixMap[hashedKey], true
		}
	}

	// Check the second index
	for i := 0; i < maxBucketSize; i++ {
		if cf.Buckets[index2*maxBucketSize+i] == hashedKey {
			return cf.KeyPrefixMap[hashedKey], true
		}
	}

	return 0, false
}

// Serialize the cuckoo filter to a byte slice
func (cf *CuckooFilter) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(cf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize the cuckoo filter from a byte slice
func Deserialize(data []byte) (*CuckooFilter, error) {
	var cf CuckooFilter
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&cf)
	if err != nil {
		return nil, err
	}
	return &cf, nil
}
