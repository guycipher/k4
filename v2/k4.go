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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/guycipher/k4/compressor"
	"github.com/guycipher/k4/v2/cuckoofilter"
	"github.com/guycipher/k4/v2/pager"
	"github.com/guycipher/k4/v2/skiplist"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

const SSTABLE_EXTENSION = ".sst"                 // The SSTable file extension
const LOG_EXTENSION = ".log"                     // The log file extension
const WAL_EXTENSION = ".wal"                     // The write ahead log file extension
const TOMBSTONE_VALUE = "$tombstone"             // The tombstone value
const COMPRESSION_WINDOW_SIZE = 1024 * 32        // The compression window size
const BACKGROUND_OP_SLEEP = 5 * time.Microsecond // The background sleep time for the background operations

// K4 is the main structure for the k4 database
type K4 struct {
	sstables               []*SSTable           // in memory sstables.  We just keep the opened file descriptors
	sstablesLock           *sync.RWMutex        // read write lock for sstables
	memtable               *skiplist.SkipList   // in memory memtable (skip list)
	memtableLock           *sync.RWMutex        // read write lock for memtable
	memtableFlushThreshold int                  // in bytes
	memtableMaxLevel       int                  // the maximum level of the memtable (default 12)
	memtableP              float64              // the probability of the memtable (default 0.25)
	compactionInterval     int                  // in seconds, pairs up sstables and merges them
	directory              string               // the directory where the database files are stored
	lastCompaction         time.Time            // the last time a compaction was run
	transactions           []*Transaction       // in memory transactions
	transactionsLock       *sync.RWMutex        // read write lock for transactions
	logging                bool                 // whether or not to log to the log file
	logFile                *os.File             // the log file
	wal                    *pager.Pager         // the write ahead log
	wg                     *sync.WaitGroup      // wait group for the wal
	walQueue               []*Operation         // the write ahead log queue
	walQueueLock           *sync.Mutex          // mutex for the wal queue
	exit                   chan struct{}        // channel to signal the background operations to exit
	compress               bool                 // whether to compress the keys and their values
	flushQueue             []*skiplist.SkipList // queue for flushing memtables to disk
	flushQueueLock         *sync.Mutex          // mutex for the flush queue
}

// SSTable is the structure for the SSTable files
type SSTable struct {
	pager      *pager.Pager  // the pager for the sstable file
	lock       *sync.RWMutex // read write lock for the sstable
	compressed bool          // whether the sstable is compressed; this gets set when the sstable is created, the configuration is passed from K4
}

// Transaction is the structure for the transactions
type Transaction struct {
	id   int64         // Unique identifier for the transaction
	ops  []*Operation  // List of operations in the transaction
	lock *sync.RWMutex // The lock for the transaction
}

// Operation Used for transaction operations and WAL
type Operation struct {
	Op       OPR_CODE   // Operation code
	Key      []byte     // Key for the operation
	Value    []byte     // Value for the operation
	Rollback *Operation // Pointer to the operation that will undo this operation
} // fields must be exported for gob

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
	compressed  bool         // whether the sstable is compressed
}

// WALIterator is the structure for the WAL iterator
type WALIterator struct {
	pager       *pager.Pager // the pager for the wal file
	currentPage int          // the current page
	lastPage    int          // the last page in the wal
	compressed  bool         // whether the wal is compressed; this gets set when the sstable is created, the configuration is passed from K4
}

// KV mainly used for storage of key value pairs on disk pages
// we code the KV into a binary format before writing to disk
type KV struct {
	Key   []byte     // Binary array of key
	Value []byte     // Binary array of keys value
	TTL   *time.Time // Time to live
}

// KeyValueArray type to hold a slice of KeyValue's
type KeyValueArray []*KV

// Iterator is a structure for an iterator which goes through
// memtable and sstables.  First it goes through the memtable, then once exhausted goes through the sstables from newest to oldest
type Iterator struct {
	instance     *K4                        // the instance of K4
	memtableIter *skiplist.SkipListIterator // memtable iterator
	sstablesIter []*SSTableIterator         // an iterator for each sstable
	currentKey   []byte                     // the current key
	currentValue []byte                     // the current value
	sstIterIndex int                        // the current sstable iterator index
	prevStarted  bool                       // whether the previous function was called
}

// Open opens a new K4 instance at the specified directory.
// will reopen the database if it already exists
// directory - the directory where the database files are stored
// memtableFlushThreshold - the threshold in bytes for flushing the memtable to disk
// compactionInterval - the interval in seconds for running compactions
// logging - whether or not to log to the log file
func Open(directory string, memtableFlushThreshold int, compactionInterval int, logging, compress bool, args ...interface{}) (*K4, error) {
	// Create directory if it doesn't exist
	err := os.MkdirAll(directory, 0755) // MkdirAll does nothing if directory exists..
	if err != nil {
		return nil, err
	}

	// Register *time.Time with gob
	gob.Register(&time.Time{})

	// Initialize K4
	k4 := &K4{
		memtableLock:           &sync.RWMutex{},
		directory:              directory,
		memtableFlushThreshold: memtableFlushThreshold,
		compactionInterval:     compactionInterval,
		sstables:               make([]*SSTable, 0),
		sstablesLock:           &sync.RWMutex{},
		lastCompaction:         time.Now(),
		transactions:           make([]*Transaction, 0),
		transactionsLock:       &sync.RWMutex{},
		logging:                logging,
		wg:                     &sync.WaitGroup{},
		walQueue:               make([]*Operation, 0),
		walQueueLock:           &sync.Mutex{},
		exit:                   make(chan struct{}),
		compress:               compress,
		flushQueue:             make([]*skiplist.SkipList, 0),
		flushQueueLock:         &sync.Mutex{},
	}

	// Check for max level and probability for memtable (skiplist)
	// this is optional
	if len(args) > 0 { // if there are arguments

		// First argument should be max level
		if maxLevel, ok := args[0].(int); ok {
			k4.memtableMaxLevel = maxLevel
		} else { // if not provided, default to 12
			k4.memtableMaxLevel = 12
		}

		// Check for p
		if len(args) > 1 { // if there are more arguments
			// the argument after max level should be a probability

			if p, ok := args[1].(float64); ok {
				k4.memtableP = p
			} else { // if not provided, default to 0.25
				k4.memtableP = 0.25
			}
		}

	} else { // If no optional memtable arguments, set defaults
		k4.memtableMaxLevel = 12
		k4.memtableP = 0.25
	}

	k4.memtable = skiplist.NewSkipList(k4.memtableMaxLevel, k4.memtableP) // Set the memtable

	// Load SSTables
	// We open sstable files in the configured directory
	k4.loadSSTables()

	// If logging is set we will open a logging file, so we can write to it
	if logging {
		// Create log file
		logFile, err := os.OpenFile(directory+string(os.PathSeparator)+LOG_EXTENSION, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}

		// Set log output to the log file
		log.SetOutput(logFile)

		// Set log file in K4
		k4.logFile = logFile
	}

	// open the write ahead log
	wal, err := pager.OpenPager(directory+string(os.PathSeparator)+WAL_EXTENSION, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Set wal in K4
	k4.wal = wal

	// Start the background wal writer
	k4.wg.Add(1)
	go k4.backgroundWalWriter() // start the background wal writer
	k4.printLog("Background WAL writer started")

	// Start the background flusher
	k4.wg.Add(1)
	go k4.backgroundFlusher() // start the background flusher
	k4.printLog("Background flusher started")

	// Start the background compactor
	k4.wg.Add(1)
	go k4.backgroundCompactor() // start the background compactor
	k4.printLog("Background compactor started")

	k4.printLog("K4 opened successfully")

	return k4, nil
}

// Close closes the K4
func (k4 *K4) Close() error {

	k4.printLog("Closing up")

	// when there is anything in the memtable we flush it to disk
	if k4.memtable.Size() > 0 {
		k4.printLog(fmt.Sprintf("Memtable is of size %d bytes and is being flushed to disk", k4.memtable.Size()))
		k4.appendMemtableToFlushQueue()
	}

	// signal the background operations to exit
	close(k4.exit)

	k4.printLog("Waiting for background operations to finish")

	// wait for the background operations to finish
	k4.wg.Wait()

	k4.printLog("Background operations finished")

	k4.printLog("Closing SSTables")

	// Close SSTables
	for _, sstable := range k4.sstables {
		err := sstable.pager.Close()
		if err != nil {
			return err
		}
	}

	k4.printLog("SSTables closed")

	// Close WAL
	if k4.wal != nil {
		k4.printLog("Closing WAL")
		err := k4.wal.Close()
		if err != nil {
			return err
		}
		k4.printLog("WAL closed")
	}

	if k4.logging {
		// Close log file
		err := k4.logFile.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// printLog prints a log message to the log file or stdout
// takes a string message
func (k4 *K4) printLog(msg string) {
	if k4.logging {
		log.Println(msg) // will log to the log file
	}
}

// backgroundWalWriter writes operations to the write ahead log
// This function runs in the background and pops operations from the wal queue and writes
// to write ahead log.  The reason we do this is to optimize write speed
func (k4 *K4) backgroundWalWriter() {

	defer k4.wg.Done() // Defer completion of routine

	for {
		select {
		case <-k4.exit:
			// Escalate what hasn't been written to the wal
			k4.walQueueLock.Lock() // lock the wal queue
			for _, op := range k4.walQueue {
				data := encodeOp(op.Op, op.Key, op.Value)
				_, err := k4.wal.Write(data)
				if err != nil {
					k4.printLog(fmt.Sprintf("Failed to write to WAL: %v", err))
				}
			}

			k4.walQueueLock.Unlock() // unlock the wal queue

			// break out
			return

		default:
			k4.walQueueLock.Lock() // lock up the wal queue

			if len(k4.walQueue) > 0 { // Check if there are operations in the wal queue
				op := k4.walQueue[0]          // Get the first operation
				k4.walQueue = k4.walQueue[1:] // Remove the first operation
				k4.walQueueLock.Unlock()      // Unlock the wal queue

				// Encode operation
				data := encodeOp(op.Op, op.Key, op.Value)

				// Write to WAL
				_, err := k4.wal.Write(data)
				if err != nil {
					k4.printLog(fmt.Sprintf("Failed to write to WAL: %v", err)) // Log error
				}
			} else {
				k4.walQueueLock.Unlock()        // Unlock the wal queue
				time.Sleep(BACKGROUND_OP_SLEEP) // If you have a speedy loop your cpu will be cycled greatly
				// What we do here is sleep for a tiny bit of time each iteration if no work is to be done
			}
		}
	}
}

// encodeOp encodes an operation into a binary format
func encodeOp(op OPR_CODE, key, value []byte) []byte {

	// create an operation struct and initialize it
	operation := Operation{
		Op:    op,
		Key:   key,
		Value: value,
	}

	var buf bytes.Buffer
	// Encode Op as an int32 (4 bytes)
	if err := binary.Write(&buf, binary.LittleEndian, int32(operation.Op)); err != nil {
		return nil
	}

	// Encode the length of the Key and the Key itself
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(operation.Key))); err != nil {
		return nil
	}
	if err := binary.Write(&buf, binary.LittleEndian, operation.Key); err != nil {
		return nil
	}

	// Encode the length of the Value and the Value itself
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(operation.Value))); err != nil {
		return nil
	}
	if err := binary.Write(&buf, binary.LittleEndian, operation.Value); err != nil {
		return nil
	}

	if operation.Rollback != nil {
		if err := binary.Write(&buf, binary.LittleEndian, byte(1)); err != nil {
			return nil
		}
		// Recursively encode the rollback operation
		rollbackData := encodeOp(operation.Rollback.Op, operation.Rollback.Key, operation.Rollback.Value)
		buf.Write(rollbackData)
	} else {
		// No rollback operation
		if err := binary.Write(&buf, binary.LittleEndian, byte(0)); err != nil {
			return nil
		}
	}

	return buf.Bytes()

}

// decodeOp decodes an encoded operation
func decodeOp(data []byte) (OPR_CODE, []byte, []byte, error) {
	buf := bytes.NewReader(data)
	var op Operation

	// Decode Op (int32)
	var opCode int32
	if err := binary.Read(buf, binary.LittleEndian, &opCode); err != nil {
		return 0, nil, nil, err
	}
	op.Op = OPR_CODE(opCode)

	// Decode Key length and the Key
	var keyLen int32
	if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
		return 0, nil, nil, err
	}
	op.Key = make([]byte, keyLen)
	if err := binary.Read(buf, binary.LittleEndian, &op.Key); err != nil {
		return 0, nil, nil, err
	}

	// Decode Value length and the Value
	var valueLen int32
	if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
		return 0, nil, nil, err
	}
	op.Value = make([]byte, valueLen)
	if err := binary.Read(buf, binary.LittleEndian, &op.Value); err != nil {
		return 0, nil, nil, err
	}

	// Decode Rollback pointer:
	var hasRollback byte
	if err := binary.Read(buf, binary.LittleEndian, &hasRollback); err != nil {
		return 0, nil, nil, err
	}
	if hasRollback == 1 {
		// Recursively decode the rollback operation
		rollbackData := make([]byte, buf.Len())
		if _, err := buf.Read(rollbackData); err != nil {
			return 0, nil, nil, err
		}
		rollbackOp, rollbackKey, rollbackValue, err := decodeOp(rollbackData)
		if err != nil {
			return 0, nil, nil, err
		}

		op.Rollback = &Operation{
			Op:    rollbackOp,
			Key:   rollbackKey,
			Value: rollbackValue,
		}

	}

	return op.Op, op.Key, op.Value, nil
}

// encodeKv encodes a key-value pair into a binary format
func encodeKv(key, value []byte, ttl *time.Time) []byte {
	// create a key value pair struct
	kv := KV{
		Key:   key,
		Value: value,
		TTL:   ttl,
	}

	var buf bytes.Buffer

	// Encode the length of the Key and the Key itself
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(kv.Key))); err != nil {
		return nil
	}
	if err := binary.Write(&buf, binary.LittleEndian, kv.Key); err != nil {
		return nil
	}

	// Encode the length of the Value and the Value itself
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(kv.Value))); err != nil {
		return nil
	}
	if err := binary.Write(&buf, binary.LittleEndian, kv.Value); err != nil {
		return nil
	}

	// Encode whether TTL is present
	if kv.TTL != nil {
		if err := binary.Write(&buf, binary.LittleEndian, byte(1)); err != nil {
			return nil
		}
		// Encode the TTL value
		if err := binary.Write(&buf, binary.LittleEndian, kv.TTL.UnixNano()); err != nil {
			return nil
		}
	} else {
		if err := binary.Write(&buf, binary.LittleEndian, byte(0)); err != nil {
			return nil
		}
	}

	return buf.Bytes()
}

// decodeKV decodes a key-value pair
func decodeKV(data []byte) (key, value []byte, ttl *time.Time, err error) {
	buf := bytes.NewReader(data)
	var kv KV

	// Decode the length of the Key and the Key itself
	var keyLen int32
	if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
		return nil, nil, nil, err
	}
	if keyLen < 0 || keyLen > int32(len(data)) {
		return nil, nil, nil, fmt.Errorf("invalid key length: %d", keyLen)
	}
	kv.Key = make([]byte, keyLen)
	if err := binary.Read(buf, binary.LittleEndian, &kv.Key); err != nil {
		return nil, nil, nil, err
	}

	// Decode the length of the Value and the Value itself
	var valueLen int32
	if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
		return nil, nil, nil, err
	}
	if valueLen < 0 || valueLen > int32(len(data)) {
		return nil, nil, nil, fmt.Errorf("invalid value length: %d", valueLen)
	}
	kv.Value = make([]byte, valueLen)
	if err := binary.Read(buf, binary.LittleEndian, &kv.Value); err != nil {
		return nil, nil, nil, err
	}

	// Decode whether TTL is present
	var hasTTL byte
	if err := binary.Read(buf, binary.LittleEndian, &hasTTL); err != nil {
		return nil, nil, nil, err
	}
	if hasTTL == 1 {
		// Decode the TTL value
		var ttl int64
		if err := binary.Read(buf, binary.LittleEndian, &ttl); err != nil {
			return nil, nil, nil, err
		}
		t := time.Unix(0, ttl)
		kv.TTL = &t
	}

	return kv.Key, kv.Value, kv.TTL, nil
}

// loadSSTables loads SSTables from the directory
func (k4 *K4) loadSSTables() {

	// Open configured K4 directory
	dir, err := os.Open(k4.directory)
	if err != nil {
		k4.printLog(fmt.Sprintf("Failed to open directory: %v", err))
		return
	}

	defer dir.Close() // defer closing the directory

	// Read directory
	files, err := dir.Readdir(-1)
	if err != nil {
		return
	}

	// Filter and sort files by modification time
	var sstableFiles []os.FileInfo
	for _, file := range files {
		if !file.IsDir() && len(file.Name()) > len(SSTABLE_EXTENSION) && file.Name()[len(file.Name())-len(SSTABLE_EXTENSION):] == SSTABLE_EXTENSION {
			sstableFiles = append(sstableFiles, file)
		}
	}
	sort.Slice(sstableFiles, func(i, j int) bool {
		return sstableFiles[i].ModTime().Before(sstableFiles[j].ModTime())
	}) // sort the sstable files by modification time

	// Open and append SSTables
	for _, file := range sstableFiles {
		sstablePager, err := pager.OpenPager(k4.directory+string(os.PathSeparator)+file.Name(), os.O_RDWR, 0644)
		if err != nil {
			// could possibly handle this better
			k4.printLog(fmt.Sprintf("Failed to open sstable: %v", err))
			continue
		}

		k4.sstables = append(k4.sstables, &SSTable{
			pager:      sstablePager,
			lock:       &sync.RWMutex{},
			compressed: k4.compress,
		}) // append the sstable to the list of sstables
	}
}

// appendMemtableToFlushQueue appends the memtable to the flush queue clearing the memtable
// This opens up the memtable for new writes
func (k4 *K4) appendMemtableToFlushQueue() {
	k4.flushQueueLock.Lock()         // lock the flush queue
	defer k4.flushQueueLock.Unlock() // unlock flush queue on defer

	copyOfMemtable := k4.memtable.Copy() // copy the memtable

	k4.flushQueue = append(k4.flushQueue, copyOfMemtable) // append the copy of the memtable to the flush queue

	k4.memtable = skiplist.NewSkipList(k4.memtableMaxLevel, k4.memtableP) // clear the instance memtable to welcome to new writes

}

// flushMemtable flushes the memtable into an SSTable
func (k4 *K4) flushMemtable(memtable *skiplist.SkipList) error {
	k4.printLog("Flushing memtable off flush queue")

	// Create SSTable
	sstable, err := k4.createSSTable()
	if err != nil {
		return err
	}

	// Create a new skiplist iterator
	it := skiplist.NewIterator(memtable)

	// create a cuckoo filter
	cf := cuckoofilter.NewCuckooFilter()

	// We create another iterator to write the key value pairs to the sstable
	it = skiplist.NewIterator(memtable)

	// Iterate through the memtable
	for it.Next() {
		key, value, ttl := it.Current()
		if bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
			continue // skip tombstones
		}

		var beforeCompressionKey []byte

		// Check for compression
		if k4.compress {
			beforeCompressionKey = key

			key, value, err = compressKeyValue(key, value) // compress key and value
			if err != nil {
				return err
			}
		}

		// Encode key-value pair
		var data []byte

		if ttl != nil {
			expiry := time.Now().Add(*ttl)
			data = encodeKv(key, value, &expiry)
		} else {

			data = encodeKv(key, value, nil)
		}

		// Write to SSTable
		pgN, err := sstable.pager.Write(data)
		if err != nil {
			return err
		}

		if k4.compress {
			cf.Insert(pgN, beforeCompressionKey) // add key, and page index to cuckoo filter
		} else {
			cf.Insert(pgN, key) // add key, and page index to cuckoo filter
		}

	}

	// serialize the cuckoo filter
	cfData, err := cf.Serialize()
	if err != nil {
		return err
	}

	// Write the cuckoo filter to the final pages of SSTable
	_, err = sstable.pager.Write(cfData)
	if err != nil {
		return err
	}

	// We only lock sstables array when we are appending a new sstable
	// this is because we don't want to block reads while we are flushing the memtable only when we are appending a new sstable
	k4.sstablesLock.Lock() // lock the sstables
	// Append SSTable to list of SSTables
	k4.sstables = append(k4.sstables, sstable)
	k4.sstablesLock.Unlock() // unlock the sstables

	k4.printLog("Flushed memtable")

	return nil
}

// createSSTable creates an SSTable
// creates an sstable in directory, opens file and returns the sstable
func (k4 *K4) createSSTable() (*SSTable, error) {
	k4.sstablesLock.RLock()         // read lock
	defer k4.sstablesLock.RUnlock() // unlock on defer

	// Create SSTable file
	sstablePager, err := pager.OpenPager(k4.directory+string(os.PathSeparator)+sstableFilename(len(k4.sstables)), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Create SSTable
	return &SSTable{
		pager:      sstablePager,
		lock:       &sync.RWMutex{},
		compressed: k4.compress,
	}, nil
}

// createSSTableNoLock creates an SSTable without locking ssTables slice
// (used mainly for functions that lock the ssTables slice prior to calling this function)
func (k4 *K4) createSSTableNoLock() (*SSTable, error) {

	// Create SSTable file
	sstablePager, err := pager.OpenPager(k4.directory+string(os.PathSeparator)+sstableFilename(len(k4.sstables)), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Create SSTable
	return &SSTable{
		pager:      sstablePager,
		lock:       &sync.RWMutex{},
		compressed: k4.compress,
	}, nil
}

// sstableFilename returns the filename for an SSTable
func sstableFilename(index int) string {
	return "sstable_" + fmt.Sprintf("%d", index) + SSTABLE_EXTENSION
}

// newSSTableIterator creates a new SSTable iterator
func newSSTableIterator(pager *pager.Pager, compressed bool) *SSTableIterator {
	return &SSTableIterator{
		pager:       pager,                  // the pager for the sstable file
		currentPage: -1,                     // start at initial page
		lastPage:    int(pager.Count() - 1), // the last page in the sstable
		compressed:  compressed,             // whether the sstable is compressed
	}
}

// next returns true if there is another key-value pair in the SSTable
func (it *SSTableIterator) next() bool {
	// We check if the current page is greater than the last page
	// if so we return false
	if it.currentPage > it.lastPage {
		return false
	}

	it.currentPage++ // increment the current page
	return true
}

// current returns the current key-value pair in the SSTable
func (it *SSTableIterator) current() ([]byte, []byte, *time.Time) {
	// Get the current page
	data, err := it.pager.GetPage(int64(it.currentPage))
	if err != nil {
		return nil, nil, nil
	}

	// DEcode key-value pair
	key, value, ttl, err := decodeKV(data)
	if err != nil {

		return nil, nil, ttl
	}

	// Check if key value has TTL set, if so we check if it has expired
	if ttl != nil {
		if time.Now().After(*ttl) {
			// skip and go to next
			if it.next() {
				return it.current()
			} else {
				return nil, nil, nil
			}
		}
	}

	// Check for compression
	if it.compressed {
		// If so we decompress the key and value
		key, value, err = decompressKeyValue(key, value)
		if err != nil {
			return nil, nil, nil
		}
	}

	return key, value, ttl
}

// currentKey returns the current key in the SSTable
func (it *SSTableIterator) currentKey() []byte {
	// Get the current page
	data, err := it.pager.GetPage(int64(it.currentPage))
	if err != nil {
		return nil
	}

	// Decode key-value pair
	key, value, _, err := decodeKV(data)
	if err != nil {
		return nil
	}

	// Check for compression
	if it.compressed {
		// If so we decompress the key
		key, _, err = decompressKeyValue(key, value)
		if err != nil {
			return nil
		}

	}

	return key
}

// prev returns true if there is a previous key-value pair in the SSTable
func (it *SSTableIterator) prev() bool {
	// We check if the current page is less than 0
	// if so we return false
	if it.currentPage < 0 {
		return false
	}

	it.currentPage-- // decrement the current page
	return true
}

// newWALIterator creates a new WAL iterator
func newWALIterator(pager *pager.Pager, compressed bool) *WALIterator {

	return &WALIterator{
		pager:       pager,
		currentPage: -1,
		lastPage:    int(pager.Count() - 1),
		compressed:  compressed,
	}
}

// next returns true if there is another operation in the WAL
func (it *WALIterator) next() bool {
	it.currentPage++
	return it.currentPage <= it.lastPage
}

// current returns the current operation in the WAL
func (it *WALIterator) current() (OPR_CODE, []byte, []byte) {
	data, err := it.pager.GetPage(int64(it.currentPage))
	if err != nil {
		return -1, nil, nil
	}

	// Decode operation
	op, key, value, err := decodeOp(data)
	if err != nil {
		return -1, nil, nil
	}

	if it.compressed {
		key, value, err = decompressKeyValue(key, value)
		if err != nil {
			return -1, nil, nil
		}

	}

	return op, key, value
}

// compact compacts K4's sstables by pairing and merging them in parallel
func (k4 *K4) compact() error {
	k4.sstablesLock.Lock()         // lock up the sstables to prevent reads while we are compacting
	defer k4.sstablesLock.Unlock() // defer unlocking the sstables

	// if only 2 sstables, no need to compact
	if len(k4.sstables) == 2 {
		return nil
	}

	k4.printLog("Starting compaction")

	pairs := len(k4.sstables) / 2      // determine the number of pairs
	var wg sync.WaitGroup              // create a wait group
	newSStables := make([]*SSTable, 0) // create a new slice of sstables
	sstablesToRemove := make([]int, 0) // create a new slice of sstables to remove
	routinesLock := &sync.Mutex{}      // create a new mutex for the routines

	// iterate over the pairs of sstables
	for i := 0; i < pairs*2; i += 2 {

		// if we are at the end of the sstables we break
		if i+1 >= len(k4.sstables) {
			break
		}

		// increment the wait group
		wg.Add(1)

		// start a goroutine to compact the pair of sstables into a new sstable
		go func(i int, sstablesToRemove *[]int, newSStables *[]*SSTable, routinesLock *sync.Mutex) {
			defer wg.Done() // defer completion of goroutine

			cf := cuckoofilter.NewCuckooFilter() // create a new cuckoo filter

			// create a new sstable
			newSstable, err := k4.createSSTableNoLock()
			if err != nil {
				k4.printLog(fmt.Sprintf("Failed to create SSTable: %v", err))
				return
			}

			// set sst1 and sst2 to the sstables at index i and i+1
			sstable1 := k4.sstables[i]
			sstable2 := k4.sstables[i+1]

			// create a new iterator for sstable1 to add entries to the new sstable
			it := newSSTableIterator(sstable1.pager, k4.compress)
			for it.next() {
				key, value, ttl := it.current()
				if ttl != nil && time.Now().After(*ttl) { // if the key has expired we skip it
					continue
				}

				var beforeCompressionKey []byte

				// Check for compression
				if k4.compress {
					beforeCompressionKey = key
					key, value, err = compressKeyValue(key, value) // compress key and value
					if err != nil {
						k4.printLog(fmt.Sprintf("Failed to compress key-value: %v", err))
						return
					}
				}

				// Check ttl
				if ttl != nil {
					// check if ttl is expired
					if time.Now().After(*ttl) {
						// skip and go to next
						continue
					}
				}

				data := encodeKv(key, value, ttl)
				pgN, err := newSstable.pager.Write(data)
				if err != nil {
					k4.printLog(fmt.Sprintf("Failed to write key-value to SSTable: %v", err))
					return
				}

				if k4.compress {
					cf.Insert(pgN, beforeCompressionKey) // add key, and page index to cuckoo filter
				} else {
					cf.Insert(pgN, key) // add key, and page index to cuckoo filter
				}
			}

			// create a new iterator for sstable2 to add entries to the new sstable
			it = newSSTableIterator(sstable2.pager, k4.compress)
			for it.next() {
				key, value, ttl := it.current()
				if ttl != nil && time.Now().After(*ttl) { // if the key has expired we skip it
					continue
				}

				var beforeCompressionKey []byte

				// Check for compression
				if k4.compress {
					beforeCompressionKey = key
					key, value, err = compressKeyValue(key, value) // compress key and value
					if err != nil {
						k4.printLog(fmt.Sprintf("Failed to compress key-value: %v", err))
						return
					}
				}

				// Check ttl
				if ttl != nil {
					// check if ttl is expired
					if time.Now().After(*ttl) {
						// skip and go to next
						continue
					}
				}

				data := encodeKv(key, value, ttl)        // encode key-value pair into byte array
				pgN, err := newSstable.pager.Write(data) // write to new sstable
				if err != nil {
					k4.printLog(fmt.Sprintf("Failed to write key-value to SSTable: %v", err))
					return
				}

				if k4.compress {
					cf.Insert(pgN, beforeCompressionKey) // add key, and page index to cuckoo filter
				} else {
					cf.Insert(pgN, key) // add key, and page index to cuckoo filter
				}
			}

			// serialize the cuckoo filter
			cfData, err := cf.Serialize()
			if err != nil {
				k4.printLog(fmt.Sprintf("Failed to serialize cuckoo filter: %v", err))
				return
			}

			// write cuckoo filter to final sstable pages
			_, err = newSstable.pager.Write(cfData)
			if err != nil {
				k4.printLog(fmt.Sprintf("Failed to write cuckoo filter to SSTable: %v", err))
				return
			}

			// close sstable1 and sstable2
			err = sstable1.pager.Close()
			if err != nil {
				k4.printLog(fmt.Sprintf("Failed to close SSTable1: %v", err))
				return
			}

			err = sstable2.pager.Close()
			if err != nil {
				k4.printLog(fmt.Sprintf("Failed to close SSTable2: %v", err))
				return
			}

			routinesLock.Lock()
			*sstablesToRemove = append(*sstablesToRemove, i)
			*newSStables = append(*newSStables, newSstable)
			routinesLock.Unlock()

			err = os.Remove(k4.directory + string(os.PathSeparator) + sstableFilename(i))
			if err != nil {
				k4.printLog(fmt.Sprintf("Failed to remove SSTable1 file: %v", err))
				return
			}

			err = os.Remove(k4.directory + string(os.PathSeparator) + sstableFilename(i+1))
			if err != nil {
				k4.printLog(fmt.Sprintf("Failed to remove SSTable2 file: %v", err))
				return
			}
		}(i, &sstablesToRemove, &newSStables, routinesLock)
	}

	wg.Wait()

	// remove paired sstables
	for _, index := range sstablesToRemove {
		if index >= len(k4.sstables) {
			continue
		}
		k4.sstables = append(k4.sstables[:index], k4.sstables[index+2:]...)
	}

	// append new sstables
	k4.sstables = append(k4.sstables, newSStables...)

	k4.printLog("Compaction completed")

	return nil
}

// RecoverFromWAL recovers K4 from a write ahead log
func (k4 *K4) RecoverFromWAL() error {
	k4.printLog("Starting to recover from write ahead log")

	// Iterate over the write ahead log
	it := newWALIterator(k4.wal, k4.compress)
	for it.next() {
		op, key, value := it.current()

		switch op {
		case PUT:
			err := k4.Put(key, value, nil)
			if err != nil {
				return err
			}
		case DELETE:
			err := k4.Delete(key)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid operation")
		}
	}

	k4.printLog("Recovery from write ahead log completed")

	return nil

}

// compressKeyValue compresses a key and value
func compressKeyValue(key, value []byte) ([]byte, []byte, error) {
	// compress the key and value

	// create new compressor for key
	keyCompressor, err := compressor.NewCompressor(COMPRESSION_WINDOW_SIZE)
	if err != nil {
		return nil, nil, err
	}

	// create new compressor for value
	valueCompressor, err := compressor.NewCompressor(COMPRESSION_WINDOW_SIZE)
	if err != nil {
		return nil, nil, err
	}

	// compress the key and value and return
	return keyCompressor.Compress(key), valueCompressor.Compress(value), nil
}

// decompressKeyValue decompresses a key and value
func decompressKeyValue(key, value []byte) ([]byte, []byte, error) {
	// decompress the key and value

	// create new decompressor for key
	keyCompressor, err := compressor.NewCompressor(COMPRESSION_WINDOW_SIZE)
	if err != nil {
		return nil, nil, err
	}

	// create new decompressor for value
	valueCompressor, err := compressor.NewCompressor(COMPRESSION_WINDOW_SIZE)
	if err != nil {
		return nil, nil, err
	}

	// decompress the key and value and return
	return keyCompressor.Decompress(key), valueCompressor.Decompress(value), nil
}

// appendToWALQueue appends an operation to the write ahead log queue
func (k4 *K4) appendToWALQueue(op OPR_CODE, key, value []byte) error {
	operation := &Operation{
		Op:    op,
		Key:   key,
		Value: value,
	}

	// If compression is configured we compress the key and value
	if k4.compress {
		var err error
		operation.Key, operation.Value, err = compressKeyValue(key, value)
		if err != nil {
			return err
		}
	}

	// Lock the wal queue
	k4.walQueueLock.Lock()
	defer k4.walQueueLock.Unlock() // unlock on defer

	k4.walQueue = append(k4.walQueue, operation) // Append operation to the wal queue

	return nil
}

// BeginTransaction begins a new transaction
func (k4 *K4) BeginTransaction() *Transaction {

	// Lock the transactions list
	k4.transactionsLock.Lock()
	defer k4.transactionsLock.Unlock() // Unlock the transactions list on defer

	// Create a new transaction
	transaction := &Transaction{
		id:   int64(len(k4.transactions)) + 1,
		ops:  make([]*Operation, 0),
		lock: &sync.RWMutex{},
	}

	k4.transactions = append(k4.transactions, transaction) // Append transaction to list of transactions

	return transaction

}

// AddOperation adds an operation to a transaction
func (txn *Transaction) AddOperation(op OPR_CODE, key, value []byte) {

	// Lock up the transaction
	txn.lock.Lock()
	defer txn.lock.Unlock() // Unlock the transaction on defer

	// If GET we should not add to the transaction
	if op == GET {
		return
	}

	// Initialize the operation
	operation := &Operation{
		Op:    op,
		Key:   key,
		Value: value,
	}

	// Based on the operation, we can determine the rollback operation
	switch op {
	case PUT:
		// On PUT operation, the rollback operation is DELETE
		operation.Rollback = &Operation{
			Op:    DELETE,
			Key:   key,
			Value: nil,
		}
	case DELETE:
		// On DELETE operation we can put back the key value pair
		operation.Rollback = &Operation{
			Op:    PUT,
			Key:   key,
			Value: value,
		}
	case GET:
		operation.Rollback = nil // GET operations are read-only
	}

	txn.ops = append(txn.ops, operation)
}

// Commit commits a transaction
func (txn *Transaction) Commit(k4 *K4) error {
	k4.memtableLock.Lock() // Makes the transaction atomic and serializable
	defer k4.memtableLock.Unlock()

	// Lock the transaction
	txn.lock.Lock()
	defer txn.lock.Unlock()

	// Apply operations to memtable
	for _, op := range txn.ops {
		switch op.Op {
		case PUT:
			// Append operation to WAL queue
			err := k4.appendToWALQueue(PUT, op.Key, op.Value)
			if err != nil {
				return err
			}

			k4.memtable.Insert(op.Key, op.Value, nil) // we don't use put, we use insert
		case DELETE:
			err := k4.appendToWALQueue(DELETE, op.Key, nil)
			if err != nil {
				return err
			}

			k4.memtable.Insert(op.Key, []byte(TOMBSTONE_VALUE), nil)
		// GET operations are read-only

		default:
			// Rollback transaction
			err := txn.Rollback(k4)
			if err != nil {
				return err
			}
			return fmt.Errorf("invalid operation")
		}
	}

	// Check if memtable needs to be flushed
	if k4.memtable.Size() >= k4.memtableFlushThreshold {
		k4.appendMemtableToFlushQueue() // Append memtable to flush queue
	}

	return nil
}

// Rollback rolls back a transaction (after a commit)
func (txn *Transaction) Rollback(k4 *K4) error {
	// Lock the transaction
	txn.lock.Lock()
	defer txn.lock.Unlock()

	// Lock memtable
	k4.memtableLock.Lock()
	defer k4.memtableLock.Unlock()

	// Apply rollback operations to memtable
	for i := len(txn.ops) - 1; i >= 0; i-- {

		op := txn.ops[i]
		switch op.Op {
		case PUT:
			err := k4.appendToWALQueue(PUT, op.Key, []byte(TOMBSTONE_VALUE))
			if err != nil {
				return err
			}
			k4.memtable.Insert(op.Key, []byte(TOMBSTONE_VALUE), nil)
		case DELETE:
			err := k4.appendToWALQueue(PUT, op.Key, nil)
			if err != nil {
				return err
			}
			k4.memtable.Insert(op.Key, op.Value, nil)
		default:
			return fmt.Errorf("invalid operation")
		}
	}

	return nil
}

// Remove removes a transaction from the list of transactions in K4
func (txn *Transaction) Remove(k4 *K4) {
	// Clear transaction operations
	txn.ops = make([]*Operation, 0)

	// Find and remove transaction from list of transactions
	k4.transactionsLock.Lock() // Lock the transactions list
	for i, t := range k4.transactions {
		if t == txn {
			k4.transactions = append(k4.transactions[:i], k4.transactions[i+1:]...)
			break
		}
	}

	k4.transactionsLock.Unlock() // Unlock the transactions list
}

// Get gets a key from K4
func (k4 *K4) Get(key []byte) ([]byte, error) {

	// read lock the memtable
	k4.memtableLock.RLock()

	defer k4.memtableLock.RUnlock() // unlock the memtable on defer

	// Check memtable
	value, found := k4.memtable.Search(key)
	if found {
		// Check if the value is a tombstone
		if bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
			return nil, nil
		}

		return value, nil
	}

	// We will check the sstables in reverse order
	// We copy the sstables to avoid locking the sstables slice for the below looped reads
	k4.sstablesLock.RLock()
	if len(k4.sstables) == 0 {
		k4.sstablesLock.RUnlock()
		return nil, nil
	}

	sstablesCopy := make([]*SSTable, len(k4.sstables))
	copy(sstablesCopy, k4.sstables)
	k4.sstablesLock.RUnlock()

	// Check SSTables
	for i := len(sstablesCopy) - 1; i >= 0; i-- {
		sstable := sstablesCopy[i]
		value, err := sstable.get(key, -1)
		if err != nil {
			return nil, err
		}
		if value != nil {
			if bytes.Equal(value, []byte(TOMBSTONE_VALUE)) { // Check if the value is a tombstone
				return nil, nil
			}

			return value, nil
		}
	}

	return nil, nil
}

// get gets a key from the SSTable
func (sstable *SSTable) get(key []byte, lastPage int64) ([]byte, error) {
	// SStable pages are locked on read so no need to lock general sstable

	// Determine the last page if not provided
	if lastPage == -1 {
		lastPage = sstable.pager.Count() - 1 // Get last page for cuckoo filter
	}

	var cf *cuckoofilter.CuckooFilter
	var err error

	// Try to decode the cuckoo filter from the final pages
	for lastPage >= 0 {
		cfData, err := sstable.pager.GetPage(lastPage)
		if err != nil {
			return nil, err
		}

		cf, err = cuckoofilter.Deserialize(cfData)
		if err == nil {
			break
		}

		lastPage--
	}

	if err != nil {
		return nil, err
	}

	if cf == nil {
		return nil, nil
	}

	// Check if the key exists in the cuckoo filter, if it does we get the page index
	pg, exists := cf.Lookup(key)
	if !exists {
		return nil, nil
	}

	// Get the key value pair
	data, err := sstable.pager.GetPage(pg)
	if err != nil {
		return nil, err
	}

	// Decode the key value pair
	k, v, ttl, err := decodeKV(data)
	if err != nil {
		return nil, err
	}

	// Decompress the key and value if the sstable is compressed
	if sstable.compressed {
		_, v, err = decompressKeyValue(k, v)
		if err != nil {
			return nil, err
		}

	}

	if ttl != nil && time.Now().After(*ttl) {
		return nil, nil
	}

	// Check for tombstone value
	if bytes.Equal(v, []byte(TOMBSTONE_VALUE)) {
		return nil, nil
	}

	return v, nil
}

// Put puts a key-value pair into K4
func (k4 *K4) Put(key, value []byte, ttl *time.Duration) error {
	// Check if key or value is nil
	if key == nil || value == nil {
		return fmt.Errorf("key or value cannot be nil")
	}

	// Value cannot be a tombstone
	if bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
		return fmt.Errorf("value cannot be a tombstone")
	}

	// Lock memtable
	k4.memtableLock.Lock()
	defer k4.memtableLock.Unlock()

	// Append operation to WAL queue
	err := k4.appendToWALQueue(PUT, key, value)
	if err != nil {
		return err
	}

	k4.memtable.Insert(key, value, ttl) // insert the key value pair into the memtable

	// Check if memtable needs to be flushed
	if k4.memtable.Size() >= k4.memtableFlushThreshold {
		k4.appendMemtableToFlushQueue()
	}

	return nil
}

// Delete deletes a key from K4
func (k4 *K4) Delete(key []byte) error {
	// Check if key is nil
	if key == nil {
		return fmt.Errorf("key cannot be nil")
	}

	// Lock memtable
	k4.memtableLock.Lock()
	defer k4.memtableLock.Unlock()

	// Append operation to WAL queue
	err := k4.appendToWALQueue(DELETE, key, nil)
	if err != nil {
		return err
	}

	// We simply put a tombstone value for the key
	k4.memtable.Insert(key, []byte(TOMBSTONE_VALUE), nil)

	return nil
}

// NGet gets a key from K4 and returns a map of key-value pairs
func (k4 *K4) NGet(key []byte) (*KeyValueArray, error) {
	// Check if key is nil
	if key == nil {
		return nil, fmt.Errorf("key cannot be nil")
	}

	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		k, value, _ := it.Current()
		if !bytes.Equal(k, key) && !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {

			result.append(&KV{
				Key:   k,
				Value: value,
			})
		}
	}

	// We will check the sstables in reverse order
	// We copy the sstables to avoid locking the sstables slice for the below looped reads
	k4.sstablesLock.RLock()
	sstablesCopy := make([]*SSTable, len(k4.sstables))
	copy(sstablesCopy, k4.sstables)
	k4.sstablesLock.RUnlock()

	// Check SSTables
	for i := len(sstablesCopy) - 1; i >= 0; i-- {
		sstable := sstablesCopy[i]
		it := newSSTableIterator(sstable.pager, k4.compress)
		for it.next() {
			k, value, ttl := it.current()
			if !bytes.Equal(k, key) && !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
				// check ttl
				if ttl != nil {
					if time.Now().After(*ttl) {
						continue
					}
				}

				if _, exists := result.binarySearch(key); !exists {

					result.append(&KV{
						Key:   k,
						Value: value,
					})
				}
			}
		}
	}

	return result, nil
}

// GreaterThan gets all keys greater than a key from K4 and returns a map of key-value pairs
func (k4 *K4) GreaterThan(key []byte) (*KeyValueArray, error) {
	// Check if key is nil
	if key == nil {
		return nil, fmt.Errorf("key cannot be nil")
	}

	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		k, value, _ := it.Current()
		if greaterThan(k, key) && !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
			result.append(&KV{
				Key:   k,
				Value: value,
			})
		}
	}

	// We will check the sstables in reverse order
	// We copy the sstables to avoid locking the sstables slice for the below looped reads
	k4.sstablesLock.RLock()
	sstablesCopy := make([]*SSTable, len(k4.sstables))
	copy(sstablesCopy, k4.sstables)
	k4.sstablesLock.RUnlock()

	// Check SSTables
	for i := len(sstablesCopy) - 1; i >= 0; i-- {
		sstable := sstablesCopy[i]
		it := newSSTableIterator(sstable.pager, k4.compress)
		for it.next() {
			k, value, ttl := it.current()
			if greaterThan(k, key) && !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
				// check ttl
				if ttl != nil {
					if time.Now().After(*ttl) {
						continue
					}
				}

				if _, exists := result.binarySearch(k); !exists {
					result.append(&KV{
						Key:   k,
						Value: value,
					})
				}
			}
		}
	}

	return result, nil
}

// GreaterThanEq queries keys greater than or equal to a key from K4
func (k4 *K4) GreaterThanEq(key []byte) (*KeyValueArray, error) {
	// Check if key is nil
	if key == nil {
		return nil, fmt.Errorf("key cannot be nil")
	}

	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		k, value, _ := it.Current()
		if (greaterThan(k, key) || bytes.Equal(k, key)) && !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
			result.append(&KV{
				Key:   k,
				Value: value,
			})
		}
	}

	// We will check the sstables in reverse order
	// We copy the sstables to avoid locking the sstables slice for the below looped reads
	k4.sstablesLock.RLock()
	sstablesCopy := make([]*SSTable, len(k4.sstables))
	copy(sstablesCopy, k4.sstables)
	k4.sstablesLock.RUnlock()

	// Check SSTables
	for i := len(sstablesCopy) - 1; i >= 0; i-- {
		sstable := sstablesCopy[i]
		it := newSSTableIterator(sstable.pager, k4.compress)
		for it.next() {
			k, value, ttl := it.current()
			if (greaterThan(k, key) || bytes.Equal(k, key)) && !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
				// check ttl
				if ttl != nil {
					if time.Now().After(*ttl) {
						continue
					}
				}

				if _, exists := result.binarySearch(k); !exists {
					result.append(&KV{
						Key:   k,
						Value: value,
					})
				}
			}
		}
	}

	return result, nil
}

// LessThan gets all keys less than a key from K4 and returns a map of key-value pairs
func (k4 *K4) LessThan(key []byte) (*KeyValueArray, error) {
	// Check if key is nil
	if key == nil {
		return nil, fmt.Errorf("key cannot be nil")
	}

	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		k, value, _ := it.Current()
		if lessThan(k, key) && !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
			result.append(&KV{
				Key:   k,
				Value: value,
			})
		}
	}

	// We will check the sstables in reverse order
	// We copy the sstables to avoid locking the sstables slice for the below looped reads
	k4.sstablesLock.RLock()
	sstablesCopy := make([]*SSTable, len(k4.sstables))
	copy(sstablesCopy, k4.sstables)
	k4.sstablesLock.RUnlock()

	// Check SSTables
	for i := len(sstablesCopy) - 1; i >= 0; i-- {
		sstable := sstablesCopy[i]
		it := newSSTableIterator(sstable.pager, k4.compress)
		for it.next() {
			k, value, ttl := it.current()
			if bytes.Compare(k, key) < 0 && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
				// check ttl
				if ttl != nil {
					if time.Now().After(*ttl) {
						continue
					}
				}

				if _, exists := result.binarySearch(k); !exists {
					result.append(&KV{
						Key:   k,
						Value: value,
					})
				}
			}
		}
	}

	return result, nil
}

// LessThanEq queries keys less than or equal to a key from K4
func (k4 *K4) LessThanEq(key []byte) (*KeyValueArray, error) {
	// Check if key is nil
	if key == nil {
		return nil, fmt.Errorf("key cannot be nil")
	}

	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		k, value, _ := it.Current()
		if (lessThan(k, key) || bytes.Equal(k, key)) && !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
			result.append(&KV{
				Key:   k,
				Value: value,
			})
		}
	}

	// We will check the sstables in reverse order
	// We copy the sstables to avoid locking the sstables slice for the below looped reads
	k4.sstablesLock.RLock()
	sstablesCopy := make([]*SSTable, len(k4.sstables))
	copy(sstablesCopy, k4.sstables)
	k4.sstablesLock.RUnlock()

	// Check SSTables
	for i := len(sstablesCopy) - 1; i >= 0; i-- {
		sstable := sstablesCopy[i]
		it := newSSTableIterator(sstable.pager, k4.compress)
		for it.next() {
			k, value, ttl := it.current()
			if (lessThan(k, key) || bytes.Equal(k, key)) && !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
				// check ttl
				if ttl != nil {
					if time.Now().After(*ttl) {
						continue
					}
				}

				if _, exists := result.binarySearch(k); !exists {
					result.append(&KV{
						Key:   k,
						Value: value,
					})
				}
			}
		}
	}

	return result, nil
}

// Range queries keys in a range from K4
func (k4 *K4) Range(startKey, endKey []byte) (*KeyValueArray, error) {
	// Check if startKey or endKey is nil
	if startKey == nil || endKey == nil {
		return nil, fmt.Errorf("startKey and or endKey cannot be nil")
	}

	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		key, value, _ := it.Current()
		if (greaterThan(key, startKey) || bytes.Equal(key, startKey)) && (lessThan(key, endKey) || bytes.Equal(key, endKey)) {
			if !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
				result.append(&KV{
					Key:   key,
					Value: value,
				})
			}
		}
	}
	k4.memtableLock.RUnlock()

	// Check SSTables
	// We will check the sstables in reverse order
	// We copy the sstables to avoid locking the sstables slice for the below looped reads
	k4.sstablesLock.RLock()
	sstablesCopy := make([]*SSTable, len(k4.sstables))
	copy(sstablesCopy, k4.sstables)
	k4.sstablesLock.RUnlock()

	for i := len(sstablesCopy) - 1; i >= 0; i-- {
		sstable := sstablesCopy[i]
		it := newSSTableIterator(sstable.pager, k4.compress)
		for it.next() {
			key, value, ttl := it.current()
			if (greaterThan(key, startKey) || bytes.Equal(key, startKey)) && (lessThan(key, endKey) || bytes.Equal(key, endKey)) {
				if !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
					// check ttl
					if ttl != nil {
						if time.Now().After(*ttl) {
							continue
						}
					}

					if _, exists := result.binarySearch(key); !exists {
						result.append(&KV{
							Key:   key,
							Value: value,
						})
					}
				}
			}
		}
	}

	return result, nil
}

// NRange returns key value pairs not in provided range
func (k4 *K4) NRange(startKey, endKey []byte) (*KeyValueArray, error) {
	// Check if startKey or endKey is nil
	if startKey == nil || endKey == nil {
		return nil, fmt.Errorf("startKey and or endKey cannot be nil")
	}

	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		key, value, _ := it.Current()
		if !(greaterThan(key, startKey) || bytes.Equal(key, startKey)) || !(lessThan(key, endKey) || bytes.Equal(key, endKey)) {
			if !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
				result.append(&KV{
					Key:   key,
					Value: value,
				})
			}
		}
	}
	k4.memtableLock.RUnlock()

	// Check SSTables
	k4.sstablesLock.RLock()
	sstablesCopy := make([]*SSTable, len(k4.sstables))
	copy(sstablesCopy, k4.sstables)
	k4.sstablesLock.RUnlock()

	for i := len(sstablesCopy) - 1; i >= 0; i-- {
		sstable := sstablesCopy[i]
		it := newSSTableIterator(sstable.pager, k4.compress)
		for it.next() {
			key, value, ttl := it.current()
			if !(greaterThan(key, startKey) || bytes.Equal(key, startKey)) || !(lessThan(key, endKey) || bytes.Equal(key, endKey)) {
				if !bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
					// check ttl
					if ttl != nil {
						if time.Now().After(*ttl) {
							continue
						}
					}

					if _, exists := result.binarySearch(key); !exists {
						result.append(&KV{
							Key:   key,
							Value: value,
						})
					}
				}
			}
		}
	}

	return result, nil
}

// append method to add a new KeyValue to the array
func (kva *KeyValueArray) append(kv *KV) {
	*kva = append(*kva, kv) // Append the KeyValue to the array
}

// binarySearch method to find a KeyValue by key using binary search
func (kva KeyValueArray) binarySearch(key []byte) (*KV, bool) {
	// returns true if the key at index i is greater than or equal to the search key
	index := sort.Search(len(kva), func(i int) bool {
		return bytes.Compare(kva[i].Key, key) >= 0
	})

	// check if the index is within the bounds of the array and if the key at the
	// found index matches the search key
	if index < len(kva) && bytes.Equal(kva[index].Key, key) {
		// If the key is found, return the KeyValue pair and true.
		return kva[index], true
	}

	// if the key is not found, return nil and false
	return nil, false
}

// backgroundFlusher
// is a function that runs in the background.  When there is a memtable in the flush queue
// we pop it and flush it to a new SSTable
func (k4 *K4) backgroundFlusher() {
	defer k4.wg.Done() // We defer the wait group done to signal the wait group that we are done, so on return the done signal is sent

	for {
		select {
		case <-k4.exit:
			// Escalate the remaining memtables in the flush queue
			k4.flushQueueLock.Lock() // Lock flush queue
			for _, memtable := range k4.flushQueue {
				err := k4.flushMemtable(memtable) // We flush every memtable in the queue to disk in the escalation
				if err != nil {
					k4.flushQueueLock.Unlock()
					k4.printLog("Error flushing memtable: " + err.Error())
				}

			}

			k4.flushQueue = nil        // nil it up
			k4.flushQueueLock.Unlock() // unlock the flush queue
			return
		default:
			// Lock flush queue
			k4.flushQueueLock.Lock()

			if len(k4.flushQueue) > 0 { // Check if there is any memtable in the flush queue
				memtable := k4.flushQueue[0]      // pop a memtable from the flush queue
				k4.flushQueue = k4.flushQueue[1:] // shift 1
				k4.flushQueueLock.Unlock()        // unlock flush queue

				// Flush memtable
				err := k4.flushMemtable(memtable)
				if err != nil {
					k4.printLog("Error flushing memtable: " + err.Error())
				}

			} else {
				k4.flushQueueLock.Unlock()      // unlock flush queue
				time.Sleep(BACKGROUND_OP_SLEEP) // If you have a speedy loop your cpu will be cycled greatly
				// What we do here is sleep for a tiny bit of time each iteration if no work is to be done
			}
		}
	}
}

// backgroundCompactor
// is a function that runs in the background. At configured intervals, it will compact the sstables
// by pairing and merging them
func (k4 *K4) backgroundCompactor() {
	k4.printLog("Background compactor started")

	defer k4.wg.Done() // We defer the wait group done to signal the wait group that we are done, so on return the done signal is sent

	for {
		select {
		case <-k4.exit: // If we get a signal to exit we break out of the loop
			return
		default:
			if time.Since(k4.lastCompaction).Seconds() > float64(k4.compactionInterval) { // We check if it is time to compact
				err := k4.compact() // We compact the heck out of them sstables
				if err != nil {
					k4.printLog("Error compacting sstables: " + err.Error())
				}

				k4.lastCompaction = time.Now() // We set the last compaction time too now to reset the timer
			} else {
				time.Sleep(BACKGROUND_OP_SLEEP) // If you have a speedy loop your cpu will be cycled greatly
				// What we do here is sleep for a tiny bit of time each iteration if no work is to be done
			}
		}
	}
}

// lessThan returns true if a is less than b.
func lessThan(a, b []byte) bool {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return true
		} else if a[i] > b[i] {
			return false
		}
	}

	return len(a) < len(b)
}

// greaterThan returns true if a is greater than b.
func greaterThan(a, b []byte) bool {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] > b[i] {
			return true
		} else if a[i] < b[i] {
			return false
		}
	}

	return len(a) > len(b)
}

// NewIterator creates a new Iterator
func NewIterator(instance *K4) *Iterator {
	instance.sstablesLock.RLock() // Lock the sstables as we are gonna check how many sstables we have
	defer instance.sstablesLock.RUnlock()

	if len(instance.sstables) == 0 { // If we have no sstables we return nil
		return nil
	}

	sstablesIter := make([]*SSTableIterator, len(instance.sstables)) // We create an array of sstable iterators

	// We create an iterator for each sstable
	for i, sstable := range instance.sstables {
		sstablesIter[i] = newSSTableIterator(sstable.pager, sstable.compressed)
	}

	return &Iterator{
		instance:     instance,
		memtableIter: skiplist.NewIterator(instance.memtable),
		sstablesIter: sstablesIter,
		sstIterIndex: len(instance.sstables) - 1, // we should start at the latest sstable
	}
}

// Next moves the iterator to the next key-value pair
func (it *Iterator) Next() ([]byte, []byte) {
	// Check memtable
	if it.memtableIter.Next() {
		k, v, _ := it.memtableIter.Current()
		if k != nil {
			if bytes.Equal(v, []byte(TOMBSTONE_VALUE)) {
				return it.Next() // If the value is a tombstone we skip it
			}
		}
		return k, v
	}

	// Iterate through SSTables
	for it.sstIterIndex >= 0 {
		if it.sstablesIter[it.sstIterIndex].next() {
			key, value, ttl := it.sstablesIter[it.sstIterIndex].current()
			if key != nil {
				if bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
					continue
				}

				// check ttl
				if ttl != nil {
					if time.Now().After(*ttl) {
						continue
					}
				}

				return key, value
			}
		} else {
			it.sstIterIndex-- // If we have no more keys in the sstable we move to the next sstable
		}
	}

	// If no more sstables to check, return nil
	return nil, nil
}

// Prev moves the iterator to the previous key-value pair
func (it *Iterator) Prev() ([]byte, []byte) {
	if !it.prevStarted {
		if k, v, _ := it.memtableIter.Current(); k != nil {
			it.prevStarted = true // We set the prevStarted to true to indicate we have started the prev iteration
			// We do this to get the last element in the memtable

			if bytes.Equal(v, []byte(TOMBSTONE_VALUE)) {
				return it.Prev() // If the value is a tombstone we skip it
			}

			return k, v
		}
	}

	// Check memtable
	if it.memtableIter.Prev() {
		k, v, _ := it.memtableIter.Current()
		if k != nil {
			if bytes.Equal(v, []byte(TOMBSTONE_VALUE)) {
				return it.Prev() // If the value is a tombstone we skip it
			}
		}

		return k, v
	}

	if it.sstIterIndex == -1 {
		// set to 0
		it.sstIterIndex = 0
	}

	// Iterate through SSTables
	for it.sstIterIndex < len(it.sstablesIter) {

		if it.sstablesIter[it.sstIterIndex].prev() {
			key, value, ttl := it.sstablesIter[it.sstIterIndex].current()
			if key != nil {
				if bytes.Equal(value, []byte(TOMBSTONE_VALUE)) {
					continue
				}

				if ttl != nil {
					if time.Now().After(*ttl) {
						continue
					}
				}

				return key, value
			}
		} else {
			it.sstIterIndex++ // If we have no more keys in the sstable we move to the next sstable

		}
	}

	// If no more sstables to check, return nil
	return nil, nil
}

// Reset resets the sstable iterator
func (it *Iterator) Reset() {
	it.memtableIter = skiplist.NewIterator(it.instance.memtable) // We reset the memtable iterator
	it.sstIterIndex = len(it.instance.sstables) - 1              // We reset the sstable iterator index

	// We reset the sstable iterators
	for i := 0; i < len(it.sstablesIter); i++ {
		it.sstablesIter[i] = newSSTableIterator(it.instance.sstables[i].pager, it.instance.sstables[i].compressed) // Create new iterator for sstable
	}

	it.prevStarted = false // We reset the prevStarted to false

}

// EscalateFlush is a public method to force flush memtable to queue
func (k4 *K4) EscalateFlush() error {
	// Lock the memtable
	k4.memtableLock.Lock()
	defer k4.memtableLock.Unlock()

	// Append memtable to flush queue
	k4.appendMemtableToFlushQueue()

	return nil
}

// EscalateCompaction is a public method to force compaction
func (k4 *K4) EscalateCompaction() error {
	return k4.compact()
}
