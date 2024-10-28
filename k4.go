// Package k4
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
	"k4/pager"
	"k4/skiplist"
	"os"
	"sync"
	"time"
)

const SSTABLE_EXTENSION = ".sst"     // The SSTable file extension
const LOG_EXTENSION = ".log"         // The log file extension
const WAL_EXTENSION = ".wal"         // The write ahead log file extension
const TOMBSTONE_VALUE = "$tombstone" // The tombstone value

// k4 is the main structure for the k4 database
type K4 struct {
	sstables               []*SSTable         // in memory sstables.  We just keep the opened file descriptors
	sstablesLock           *sync.RWMutex      // read write lock for sstables
	memtable               *skiplist.SkipList // in memory memtable (skip list)
	memtableLock           *sync.RWMutex      // read write lock for memtable
	memtableFlushThreshold int                // in bytes
	compactionInterval     int                // in seconds, pairs up sstables and merges them
	directory              string             // the directory where the database files are stored
	lastCompaction         time.Time          // the last time a compaction was run
	transactions           []*Transaction     // in memory transactions
	transactionsLock       *sync.RWMutex      // read write lock for transactions
	logging                bool               // whether or not to log to the log file
	logFile                *os.File           // the log file
	wal                    *pager.Pager       // the write ahead log
	wg                     *sync.WaitGroup    // wait group for the wal
	walQueue               []*Operation       // the write ahead log queue
	walQueueLock           *sync.Mutex        // mutex for the wal queue
	exit                   chan struct{}      // channel to signal the background wal routine to exit
}

// SSTable is the structure for the SSTable files
type SSTable struct {
	pager *pager.Pager  // the pager for the sstable file
	lock  *sync.RWMutex // read write lock for the sstable
}

// Transaction is the structure for the transactions
type Transaction struct {
	id  int64        // Unique identifier for the transaction
	ops []*Operation // List of operations in the transaction
}

// Operation Used for transaction operations and WAL
type Operation struct {
	op       OPR_CODE   // Operation code
	key      []byte     // Key for the operation
	value    []byte     // Value for the operation
	rollback *Operation // Pointer to the operation that will undo this operation
}

type OPR_CODE int // Operation code

const (
	PUT OPR_CODE = iota
	DELETE
	GET
)

// SSTableIterator is the structure for the SSTable iterator
type SSTableIterator struct {
	pager       *pager.Pager // the pager for the sstable file
	currentPage int          // the current page
	lastPage    int          // the last page in the sstable
}

// WALIterator is the structure for the WAL iterator
type WALIterator struct {
	pager       *pager.Pager // the pager for the wal file
	currentPage int          // the current page
	lastPage    int          // the last page in the wal
}
