package hashset

import (
	"bytes"
	"encoding/gob"
	"github.com/guycipher/k4/murmur"
)

const initialCapacity = 128     // initial hashset capacity
const loadFactorThreshold = 0.7 // load factor threshold

// HashSet represents a hash set.
type HashSet struct {
	Buckets  [][]*entry // Buckets to store elements
	Size     int        // Number of elements in the set
	Capacity int        // Capacity of the set
}

type entry struct {
	Value []byte
	Index int64
}

// NewHashSet creates a new instance of HashSet.
func NewHashSet() *HashSet {
	return &HashSet{
		Buckets:  make([][]*entry, initialCapacity), // Initialize buckets
		Capacity: initialCapacity,                   // Set initial capacity
	}
}

// Hash function to compute the index for a given value.
func (h *HashSet) hash(value []byte, capacity int) int {
	return int(murmur.Hash64(value, 4) % uint64(capacity)) // Use murmur hash
}

// Add inserts a new element into the set.
func (h *HashSet) Add(value []byte, index int64) {

	hashIndex := h.hash(value, h.Capacity) // Compute the index

	// Check if the element already exists
	for _, item := range h.Buckets[hashIndex] {
		if bytes.Equal(item.Value, value) {
			return // Element already exists
		}
	}

	// Add the element to the set
	h.Buckets[hashIndex] = append(h.Buckets[hashIndex], &entry{Value: value, Index: index})
	h.Size++ // Increment the size

	// Resize if the load factor is too high
	if float64(h.Size)/float64(h.Capacity) > loadFactorThreshold { // Load factor
		h.resize() // Resize the hash set
	}
}

// Resize increases the capacity of the hash set.
func (h *HashSet) resize() {
	newCapacity := h.Capacity * 2               // new capacity
	newBuckets := make([][]*entry, newCapacity) // new buckets

	for _, bucket := range h.Buckets {
		for _, value := range bucket {
			newIndex := h.hash(value.Value, newCapacity)               // Compute the new index
			newBuckets[newIndex] = append(newBuckets[newIndex], value) // Add the value
		}
	}

	h.Buckets = newBuckets   // Update the buckets
	h.Capacity = newCapacity // Update the capacity
}

// Remove deletes an element from the set.
func (h *HashSet) Remove(value []byte) {
	hashIndex := h.hash(value, h.Capacity) // Compute the index

	// Find the element and remove it
	for i, item := range h.Buckets[hashIndex] {
		if bytes.Equal(item.Value, value) { // Element found
			h.Buckets[hashIndex] = append(h.Buckets[hashIndex][:i], h.Buckets[hashIndex][i+1:]...) // Remove the element
			h.Size--                                                                               // Decrement the size
			return
		}
	}
}

// Contains checks if an element is in the set and returns its index.
func (h *HashSet) Contains(value []byte) (bool, int64) {
	hashIndex := h.hash(value, h.Capacity)      // Compute the index
	for _, item := range h.Buckets[hashIndex] { // Check if the element exists
		if bytes.Equal(item.Value, value) { // Element found
			return true, item.Index // Element exists
		}
	}
	return false, -1
}

// Clear removes all elements from the set.
func (h *HashSet) Clear() {
	h.Buckets = make([][]*entry, initialCapacity) // Reset the buckets
	h.Size = 0                                    // Reset the size
	h.Capacity = initialCapacity                  // Reset the capacity
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
