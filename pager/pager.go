// Package pager
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
	"strconv"
	"sync"
)

const PAGE_SIZE = 1024 * 4 // Page size
const HEADER_SIZE = 256    // next (overflowed)

// Pager manages pages in a file
type Pager struct {
	file          *os.File                // file to store pages
	pageLocks     map[int64]*sync.RWMutex // locks for pages
	pageLocksLock *sync.RWMutex           // lock for pagesLocks
	lock          *sync.RWMutex           // lock for the pager
}

// OpenPager opens a file for page management
func OpenPager(filename string, flag int, perm os.FileMode) (*Pager, error) {
	file, err := os.OpenFile(filename, flag, perm)
	if err != nil {
		return nil, err
	}

	pgLocks := make(map[int64]*sync.RWMutex)

	// Read the tree file and create locks for each page
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	for i := int64(0); i < stat.Size()/PAGE_SIZE; i++ {
		pgLocks[i] = &sync.RWMutex{}
	}

	return &Pager{file: file, pageLocks: pgLocks, pageLocksLock: &sync.RWMutex{}, lock: &sync.RWMutex{}}, nil
}

// splitDataIntoChunks splits data into chunks of PAGE_SIZE
func splitDataIntoChunks(data []byte) [][]byte {
	var chunks [][]byte
	for i := 0; i < len(data); i += PAGE_SIZE {
		end := i + PAGE_SIZE

		// Check if end is beyond the length of data
		if end > len(data) {
			end = len(data)
		}

		chunks = append(chunks, data[i:end])
	}
	return chunks
}

// WriteTo writes data to a specific page
func (p *Pager) WriteTo(pageID int64, data []byte) error {
	// lock the page
	p.getPageLock(pageID).Lock()
	defer p.getPageLock(pageID).Unlock()

	// the reason we are doing this is because we are going to write to the page thus having any overflowed pages which are linked to the page may not be needed

	// check if data is larger than the page size
	if len(data) > PAGE_SIZE {
		// create an array [][]byte
		// each element is a page

		chunks := splitDataIntoChunks(data)

		// clear data to free up memory
		data = nil

		headerBuffer := make([]byte, HEADER_SIZE)

		// We need to create pages for each chunk
		// after index 0
		// the next page is the current page + 1

		// index 0 would have the next page of index 1 index 1 would have the next page of index 2

		for i, chunk := range chunks {
			// check if we are at the last chunk
			if i == len(chunks)-1 {
				headerBuffer = make([]byte, HEADER_SIZE)
				nextPage := pageID + 1
				copy(headerBuffer, strconv.FormatInt(nextPage, 10))

				// if chunk is less than PAGE_SIZE, we need to pad it with null bytes
				if len(chunk) < PAGE_SIZE {
					chunk = append(chunk, make([]byte, PAGE_SIZE-len(chunk))...)
				}

				// write the chunk to the file
				_, err := p.file.WriteAt(append(headerBuffer, chunk...), pageID*(PAGE_SIZE+HEADER_SIZE))
				if err != nil {
					return err
				}

			} else {
				// update the header
				headerBuffer = make([]byte, HEADER_SIZE)
				nextPage := pageID + 1
				copy(headerBuffer, strconv.FormatInt(nextPage, 10))

				if len(chunk) < PAGE_SIZE {
					chunk = append(chunk, make([]byte, PAGE_SIZE-len(chunk))...)
				}

				// write the chunk to the file
				_, err := p.file.WriteAt(append(headerBuffer, chunk...), pageID*(PAGE_SIZE+HEADER_SIZE))
				if err != nil {
					return err
				}

				// update the pageID
				pageID = nextPage

			}
		}

	} else {
		// create a buffer to store the header
		headerBuffer := make([]byte, HEADER_SIZE)

		// set the next page to -1
		copy(headerBuffer, "-1")

		// if data is less than PAGE_SIZE, we need to pad it with null bytes
		if len(data) < PAGE_SIZE {
			data = append(data, make([]byte, PAGE_SIZE-len(data))...)
		}

		// write the data to the file
		_, err := p.file.WriteAt(append(headerBuffer, data...), (PAGE_SIZE+HEADER_SIZE)*pageID)
		if err != nil {
			return err
		}

	}

	return nil
}

// getPageLock gets the lock for a page
func (p *Pager) getPageLock(pageID int64) *sync.RWMutex {
	// Lock the mutex that protects the PageLocks map
	p.pageLocksLock.Lock()
	defer p.pageLocksLock.Unlock()

	// Used for page level locking
	// This is decent for concurrent reads and writes
	if lock, ok := p.pageLocks[pageID]; ok {
		return lock
	} else {
		// Create a new lock
		p.pageLocks[pageID] = &sync.RWMutex{}
		return p.pageLocks[pageID]
	}
}

// Write writes data to the next available page
func (p *Pager) Write(data []byte) (int64, error) {
	// lock the pager
	p.lock.Lock()
	defer p.lock.Unlock()

	// get the current file size
	fileInfo, err := p.file.Stat()
	if err != nil {
		return -1, err
	}

	if fileInfo.Size() == 0 {

		err = p.WriteTo(0, data)
		if err != nil {
			return -1, err
		}

		return 0, nil
	}

	// create a new page
	pageId := fileInfo.Size() / (PAGE_SIZE + HEADER_SIZE)

	err = p.WriteTo(pageId, data)
	if err != nil {
		return -1, err
	}

	return pageId, nil

}

// Close closes the file
func (p *Pager) Close() error {
	if p != nil {
		return p.file.Close()
	}

	return nil
}

// GetPage gets a page and returns the data
// Will gather all the pages that are linked together
func (p *Pager) GetPage(pageID int64) ([]byte, error) {

	// lock the page
	p.getPageLock(pageID).Lock()
	defer p.getPageLock(pageID).Unlock()

	result := make([]byte, 0)

	// get the page
	dataPHeader := make([]byte, PAGE_SIZE+HEADER_SIZE)

	if pageID == 0 {

		_, err := p.file.ReadAt(dataPHeader, 0)
		if err != nil {
			return nil, err
		}
	} else {

		_, err := p.file.ReadAt(dataPHeader, pageID*(PAGE_SIZE+HEADER_SIZE))
		if err != nil {
			return nil, err
		}
	}

	// get header
	header := dataPHeader[:HEADER_SIZE]
	data := dataPHeader[HEADER_SIZE:]

	// remove the null bytes
	header = bytes.Trim(header, "\x00")
	//data = bytes.Trim(data, "\x00")

	// append the data to the result
	result = append(result, data...)

	// get the next page
	nextPage, err := strconv.ParseInt(string(header), 10, 64)
	if err != nil {
		return nil, err
	}

	if nextPage == -1 {
		return result, nil

	}

	for {

		dataPHeader = make([]byte, PAGE_SIZE+HEADER_SIZE)

		_, err := p.file.ReadAt(dataPHeader, nextPage*(PAGE_SIZE+HEADER_SIZE))
		if err != nil {
			break
		}

		// get header
		header = dataPHeader[:HEADER_SIZE]
		data = dataPHeader[HEADER_SIZE:]

		// remove the null bytes
		header = bytes.Trim(header, "\x00")
		//data = bytes.Trim(data, "\x00")

		// append the data to the result
		result = append(result, data...)

		// get the next page
		nextPage, err = strconv.ParseInt(string(header), 10, 64)
		if err != nil || nextPage == -1 {
			break
		}

	}

	return result, nil
}

// Size returns the size of the file
func (p *Pager) Size() int64 {
	if p == nil {
		return 0
	}

	stat, _ := p.file.Stat()
	return stat.Size()
}

// Count returns the number of pages
func (p *Pager) Count() int64 {
	return p.Size() / (PAGE_SIZE + HEADER_SIZE)
}

// FileName returns the name of the file
func (p *Pager) FileName() string {
	return p.file.Name()
}
