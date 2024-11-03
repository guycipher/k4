// Package hashset tests
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
	"fmt"
	"testing"
)

func TestHashSet_Add(t *testing.T) {
	set := NewHashSet()
	value := []byte("test")

	set.Add(value)
	if !set.Contains(value) {
		t.Errorf("Expected set to contain %v", value)
	}
}

func TestHashSet_Remove(t *testing.T) {
	set := NewHashSet()
	value := []byte("test")

	set.Add(value)
	set.Remove(value)
	if set.Contains(value) {
		t.Errorf("Expected set to not contain %v", value)
	}
}

func TestHashSet_Contains(t *testing.T) {
	set := NewHashSet()
	value := []byte("test")

	if set.Contains(value) {
		t.Errorf("Expected set to not contain %v", value)
	}

	set.Add(value)
	if !set.Contains(value) {
		t.Errorf("Expected set to contain %v", value)
	}
}

func TestHashSet_Size(t *testing.T) {
	set := NewHashSet()
	value1 := []byte("test1")
	value2 := []byte("test2")

	if set.Size != 0 {
		t.Errorf("Expected size to be 0, got %d", set.Size)
	}

	set.Add(value1)
	if set.Size != 1 {
		t.Errorf("Expected size to be 1, got %d", set.Size)
	}

	set.Add(value2)
	if set.Size != 2 {
		t.Errorf("Expected size to be 2, got %d", set.Size)
	}

	set.Remove(value1)
	if set.Size != 1 {
		t.Errorf("Expected size to be 1, got %d", set.Size)
	}
}

func TestHashSet_Clear(t *testing.T) {
	set := NewHashSet()
	value := []byte("test")

	set.Add(value)
	set.Clear()
	if set.Size != 0 {
		t.Errorf("Expected size to be 0 after clear, got %d", set.Size)
	}
	if set.Contains(value) {
		t.Errorf("Expected set to not contain %v after clear", value)
	}
}

func TestHashSetAddCheckManyValues(t *testing.T) {
	set := NewHashSet()
	for i := 0; i < 10_000; i++ {
		value := []byte("test" + fmt.Sprintf("%d", i))
		set.Add(value)
		if !set.Contains(value) {
			t.Errorf("Expected set to contain %v", value)
		}
	}
}

func TestHashSet_SerializeDeserialize(t *testing.T) {
	set := NewHashSet()
	values := [][]byte{
		[]byte("test1"),
		[]byte("test2"),
		[]byte("test3"),
	}

	for _, value := range values {
		set.Add(value)
	}

	serialized := set.Serialize()
	deserializedSet := Deserialize(serialized)

	for _, value := range values {
		if !deserializedSet.Contains(value) {
			t.Errorf("Expected deserialized set to contain %v", value)
		}
	}

	if deserializedSet.Size != set.Size {
		t.Errorf("Expected deserialized set size to be %d, got %d", set.Size, deserializedSet.Size)
	}
}
