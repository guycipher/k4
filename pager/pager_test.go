// Package pager tests
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
package pager

import (
	"bytes"
	"os"
	"sync"
	"testing"
)

func TestOpenPager(t *testing.T) {
	file, err := os.CreateTemp("", "pager_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	p, err := OpenPager(file.Name(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	if p == nil {
		t.Fatalf("Pager is nil")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Failed to close pager: %v", err)
	}
}

func TestWriteAndGetPage(t *testing.T) {
	file, err := os.CreateTemp("", "pager_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	p, err := OpenPager(file.Name(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	data := []byte("Hello, World!")
	pageID, err := p.Write(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	if pageID != 0 {
		t.Fatalf("Expected pageID 0, got %d", pageID)
	}

	readData, err := p.GetPage(pageID)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	if !bytes.Equal(data, bytes.Trim(readData, "\x00")) {
		t.Fatalf("Expected %s, got %s", data, readData)
	}
}

func TestWriteToMultiplePages(t *testing.T) {
	file, err := os.CreateTemp("", "pager_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	p, err := OpenPager(file.Name(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	data := make([]byte, PAGE_SIZE*2)
	str := "Hello, World!"

	for i := len(data) - len(str); i < len(data); i += len(str) {
		copy(data[i:], str)
	}

	pageID, err := p.Write(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Read back the first page
	readData, err := p.GetPage(pageID)
	if err != nil {
		t.Fatalf("Failed to get first page: %v", err)
	}

	// end of readData should be str
	if !bytes.Equal([]byte(str), readData[len(readData)-len(str):]) {
		t.Fatalf("Expected %s, got %s", str, readData[len(readData)-len(str):])

	}

}

func TestPagerSizeAndCount(t *testing.T) {
	file, err := os.CreateTemp("", "pager_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	p, err := OpenPager(file.Name(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	data := []byte("Hello, World!")
	_, err = p.Write(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	expectedSize := int64(PAGE_SIZE + HEADER_SIZE)
	if p.Size() != expectedSize {
		t.Fatalf("Expected size %d, got %d", expectedSize, p.Size())
	}
	if p.Count() != 1 {
		t.Fatalf("Expected count 1, got %d", p.Count())
	}
}

func TestPagerConcurrency(t *testing.T) {
	file, err := os.CreateTemp("", "pager_test")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	p, err := OpenPager(file.Name(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer p.Close()

	data := []byte("Hello, World!")
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Write(data)
			if err != nil {
				t.Errorf("Failed to write data: %v", err)
			}
		}()
	}

	wg.Wait()
	if p.Count() != 10 {
		t.Fatalf("Expected count 10, got %d", p.Count())
	}
}
