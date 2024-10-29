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
	"encoding/gob"
	"fmt"
	"github.com/guycipher/k4/bloomfilter"
	"github.com/guycipher/k4/pager"
	"github.com/guycipher/k4/skiplist"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

const SSTABLE_EXTENSION = ".sst"     // The SSTable file extension
const LOG_EXTENSION = ".log"         // The log file extension
const WAL_EXTENSION = ".wal"         // The write ahead log file extension
const TOMBSTONE_VALUE = "$tombstone" // The tombstone value

// K4 is the main structure for the k4 database
type K4 struct {
	sstables               []*SSTable         // in memory sstables.  We just keep the opened file descriptors
	sstablesLock           *sync.RWMutex      // read write lock for sstables
	memtable               *skiplist.SkipList // in memory memtable (skip list)
	memtableLock           *sync.RWMutex      // read write lock for memtable
	memtableFlushThreshold int                // in bytes
	memtableMaxLevel       int                // the maximum level of the memtable (default 12)
	memtableP              float64            // the probability of the memtable (default 0.25)
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
	id   int64        // Unique identifier for the transaction
	ops  []*Operation // List of operations in the transaction
	lock *sync.RWMutex
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
}

// WALIterator is the structure for the WAL iterator
type WALIterator struct {
	pager       *pager.Pager // the pager for the wal file
	currentPage int          // the current page
	lastPage    int          // the last page in the wal
}

// KV mainly used for serialization
type KV struct {
	Key   []byte // Binary array of key
	Value []byte // Binary array of keys value
}

// KeyValueArray type to hold a slice of KeyValue's
type KeyValueArray []*KV

// Open opens a new K4 instance at the specified directory.
// will reopen the database if it already exists
// directory - the directory where the database files are stored
// memtableFlushThreshold - the threshold in bytes for flushing the memtable to disk
// compactionInterval - the interval in seconds for running compactions
// logging - whether or not to log to the log file
func Open(directory string, memtableFlushThreshold int, compactionInterval int, logging bool, args ...interface{}) (*K4, error) {
	// Create directory if it doesn't exist
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return nil, err
	}

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

	// @todo start backgroundFlusher
	// @todo start backgroundCompactor
	// above will be implemented in the future to minimize blocking on reads and writes when these operations occur

	return k4, nil
}

// Close closes the K4
func (k4 *K4) Close() error {

	k4.printLog("Closing up")

	// wait for the wal writer to finish
	close(k4.exit)

	// wait for the background wal writer to finish
	k4.wg.Wait()

	if k4.memtable.Size() > 0 {
		k4.printLog(fmt.Sprintf("Memtable is of size %d bytes and must be flushed to disk", k4.memtable.Size()))
		err := k4.flushMemtable()
		if err != nil {
			return err
		}
	}

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
func (k4 *K4) printLog(msg string) {
	if k4.logging {
		log.Println(msg)
	}
}

// backgroundWalWriter writes operations to the write ahead log
func (k4 *K4) backgroundWalWriter() {
	defer k4.wg.Done()
	for {
		k4.walQueueLock.Lock()
		if len(k4.walQueue) > 0 {
			op := k4.walQueue[0]
			k4.walQueue = k4.walQueue[1:]
			k4.walQueueLock.Unlock()

			// Serialize operation
			data := serializeOp(op.Op, op.Key, op.Value)

			// Write to WAL
			_, err := k4.wal.Write(data)
			if err != nil {
				k4.printLog(fmt.Sprintf("Failed to write to WAL: %v", err))
			}
		} else {
			k4.walQueueLock.Unlock()
		}

		select {
		case <-k4.exit:
			return
		default:
			continue
		}
	}
}

// serializeOp serializes an operation
func serializeOp(op OPR_CODE, key, value []byte) []byte {
	var buf bytes.Buffer // create a buffer

	enc := gob.NewEncoder(&buf) // create a new encoder with the buffer

	// create an operation struct and initialize it
	operation := Operation{
		Op:    op,
		Key:   key,
		Value: value,
	}

	// encode the operation
	err := enc.Encode(&operation)
	if err != nil {
		return nil
	}

	return buf.Bytes()

}

// deserializeOp deserializes an operation
func deserializeOp(data []byte) (OPR_CODE, []byte, []byte, error) {

	operation := Operation{} // The operation to be deserialized

	dec := gob.NewDecoder(bytes.NewReader(data)) // Create a new decoder

	err := dec.Decode(&operation) // Decode the operation

	if err != nil {
		return 0, nil, nil, err
	}

	return operation.Op, operation.Key, operation.Value, nil
}

// serializeKv serializes a key-value pair
func serializeKv(key, value []byte) []byte {
	var buf bytes.Buffer // create a buffer

	enc := gob.NewEncoder(&buf) // create a new encoder with the buffer

	// create a key value pair struct
	kv := KV{
		Key:   key,
		Value: value,
	}

	// encode the key value pair
	err := enc.Encode(kv)
	if err != nil {
		return nil
	}

	return buf.Bytes() // return the bytes
}

// deserializeKv deserializes a key-value pair
func deserializeKv(data []byte) (key, value []byte, err error) {
	kv := KV{} // The key value pair to be deserialized

	dec := gob.NewDecoder(bytes.NewReader(data)) // Create a new decoder

	err = dec.Decode(&kv) // Decode the key value pair
	if err != nil {
		return nil, nil, err
	}

	return kv.Key, kv.Value, nil

}

// loadSSTables loads SSTables from the directory
func (k4 *K4) loadSSTables() {
	// Open configured directory
	dir, err := os.Open(k4.directory)
	if err != nil {
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
			continue
		}

		k4.sstables = append(k4.sstables, &SSTable{
			pager: sstablePager,
			lock:  &sync.RWMutex{},
		}) // append the sstable to the list of sstables
	}
}

// flushMemtable flushes the memtable into an SSTable
func (k4 *K4) flushMemtable() error {
	k4.printLog("Flushing memtable")
	// Create SSTable
	sstable, err := k4.createSSTable()
	if err != nil {
		return err
	}

	// Iterate over memtable and write to SSTable

	it := skiplist.NewIterator(k4.memtable)
	// first we will create a bloom filter which will be on initial pages of sstable
	// we will add all the keys to the bloom filter
	// then we will add the key value pairs to the sstable

	// create a bloom filter
	bf := bloomfilter.NewBloomFilter(1000000, 8)

	// add all the keys to the bloom filter
	for it.Next() {
		key, _ := it.Current()
		bf.Add(key)
	}

	// serialize the bloom filter
	bfData, err := bf.Serialize()
	if err != nil {
		return err
	}

	// Write the bloom filter to the SSTable
	_, err = sstable.pager.Write(bfData)
	if err != nil {
		return err
	}

	it = skiplist.NewIterator(k4.memtable)
	for it.Next() {
		key, value := it.Current()
		if bytes.Compare(value, []byte(TOMBSTONE_VALUE)) == 0 {
			continue
		}

		// Serialize key-value pair
		data := serializeKv(key, value)

		// Write to SSTable
		_, err := sstable.pager.Write(data)
		if err != nil {
			return err
		}

	}

	// Append SSTable to list of SSTables
	k4.sstables = append(k4.sstables, sstable)

	// Clear memtable
	skiplist.NewSkipList(k4.memtableMaxLevel, k4.memtableP)

	k4.printLog("Flushed memtable")

	if time.Since(k4.lastCompaction).Seconds() > float64(k4.compactionInterval) {
		err = k4.compact()
		if err != nil {
			return err
		}
		k4.lastCompaction = time.Now()

	}

	return nil
}

// createSSTable creates an SSTable
func (k4 *K4) createSSTable() (*SSTable, error) {
	// Create SSTable file
	sstablePager, err := pager.OpenPager(k4.directory+string(os.PathSeparator)+sstableFilename(len(k4.sstables)), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Create SSTable
	return &SSTable{
		pager: sstablePager,
		lock:  &sync.RWMutex{},
	}, nil
}

// sstableFilename returns the filename for an SSTable
func sstableFilename(index int) string {
	return "sstable_" + fmt.Sprintf("%d", index) + SSTABLE_EXTENSION
}

// newSSTableIterator creates a new SSTable iterator
func newSSTableIterator(pager *pager.Pager) *SSTableIterator {
	return &SSTableIterator{
		pager:       pager,
		currentPage: 1, // skip the first page which is the bloom filter
		lastPage:    int(pager.Count() - 1),
	}
}

// next returns true if there is another key-value pair in the SSTable
func (it *SSTableIterator) next() bool {
	if it.currentPage > it.lastPage {
		return false
	}

	it.currentPage++
	return true
}

// current returns the current key-value pair in the SSTable
func (it *SSTableIterator) current() ([]byte, []byte) {
	data, err := it.pager.GetPage(int64(it.currentPage))
	if err != nil {
		return nil, nil
	}

	key, value, err := deserializeKv(data)
	if err != nil {
		return nil, nil
	}

	return key, value
}

// currentKey returns the current key in the SSTable
func (it *SSTableIterator) currentKey() []byte {
	data, err := it.pager.GetPage(int64(it.currentPage))
	if err != nil {
		return nil
	}
	key, _, err := deserializeKv(data)
	if err != nil {
		return nil
	}
	return key
}

// newWALIterator creates a new WAL iterator
func newWALIterator(pager *pager.Pager) *WALIterator {

	return &WALIterator{
		pager:       pager,
		currentPage: -1,
		lastPage:    int(pager.Count() - 1),
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

	// Deserialize operation
	op, key, value, err := deserializeOp(data)
	if err != nil {
		return -1, nil, nil
	}

	return op, key, value
}

// compact compacts K4's sstables by pairing and merging
func (k4 *K4) compact() error {
	k4.sstablesLock.Lock()
	defer k4.sstablesLock.Unlock()

	k4.printLog("Starting compaction")

	// we merge the first sstable with the second sstable and so on
	// each merge creates a new sstable, removing the former sstables

	// we will figure out how many pairs we can make
	pairs := len(k4.sstables) / 2

	// we start from oldest sstables
	for i := 0; i < pairs; i++ {
		// we will merge the ith sstable with the (i+1)th sstable
		// we will create a new sstable and write the merged data to it
		// then we will remove the ith and (i+1)th sstable
		// then we will add the new sstable to the list of sstables

		// we will create a bloom filter which will be on initial pages of sstable
		// we will add all the keys to the bloom filter
		// then we will add the key value pairs to the sstable

		// create a bloom filter
		bf := bloomfilter.NewBloomFilter(1000000, 8)

		// create a new sstable
		newSstable, err := k4.createSSTable()
		if err != nil {
			return err
		}

		// get the ith and (i+1)th sstable
		sstable1 := k4.sstables[i]
		sstable2 := k4.sstables[i+1]

		// add all the keys to the bloom filter
		it := newSSTableIterator(sstable1.pager)
		for it.next() {
			key := it.currentKey()
			bf.Add(key)
		}

		it = newSSTableIterator(sstable2.pager)
		for it.next() {
			key := it.currentKey()
			bf.Add(key)
		}

		// serialize the bloom filter
		bfData, err := bf.Serialize()
		if err != nil {
			return err
		}

		// Write the bloom filter to the SSTable
		_, err = newSstable.pager.Write(bfData)
		if err != nil {
			return err
		}

		// iterate over the ith and (i+1)th sstable
		it = newSSTableIterator(sstable1.pager)
		for it.next() {
			key, value := it.current()

			// Serialize key-value pair
			data := serializeKv(key, value)

			// Write to SSTable
			_, err := newSstable.pager.Write(data)
			if err != nil {
				return err
			}
		}

		it = newSSTableIterator(sstable2.pager)

		for it.next() {
			key, value := it.current()

			// Serialize key-value pair
			data := serializeKv(key, value)

			// Write to SSTable
			_, err := newSstable.pager.Write(data)
			if err != nil {
				return err
			}
		}

		// Remove the ith and (i+1)th sstable
		err = sstable1.pager.Close()
		if err != nil {
			return err
		}

		err = sstable2.pager.Close()
		if err != nil {
			return err
		}

		// remove sstables from the list
		k4.sstables = append(k4.sstables[:i], k4.sstables[i+2:]...)

		// Append SSTable to list of SSTables
		k4.sstables = append(k4.sstables, newSstable)

		// remove the paired sstables from the directory
		err = os.Remove(k4.directory + string(os.PathSeparator) + sstableFilename(i))
		if err != nil {
			return err
		}

		err = os.Remove(k4.directory + string(os.PathSeparator) + sstableFilename(i+1))
		if err != nil {
			return err
		}

	}

	k4.printLog("Compaction completed")

	return nil
}

// RecoverFromWAL recovers K4 from a write ahead log
func (k4 *K4) RecoverFromWAL() error {
	// Iterate over the write ahead log
	it := newWALIterator(k4.wal)
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

	return nil

}

// appendToWALQueue appends an operation to the write ahead log queue
func (k4 *K4) appendToWALQueue(op OPR_CODE, key, value []byte) error {
	operation := &Operation{
		Op:    op,
		Key:   key,
		Value: value,
	}

	k4.walQueueLock.Lock()
	defer k4.walQueueLock.Unlock()

	k4.walQueue = append(k4.walQueue, operation)

	return nil
}

// BeginTransaction begins a new transaction
func (k4 *K4) BeginTransaction() *Transaction {
	k4.transactionsLock.Lock()
	defer k4.transactionsLock.Unlock()

	// Create a new transaction
	transaction := &Transaction{
		id:   int64(len(k4.transactions)) + 1,
		ops:  make([]*Operation, 0),
		lock: &sync.RWMutex{},
	}

	k4.transactions = append(k4.transactions, transaction)

	return transaction

}

// AddOperation adds an operation to a transaction
func (txn *Transaction) AddOperation(op OPR_CODE, key, value []byte) {

	// Lock up the transaction
	txn.lock.Lock()
	defer txn.lock.Unlock()

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

			k4.memtable.Insert(op.Key, op.Value, nil)
		case DELETE:
			err := k4.appendToWALQueue(DELETE, op.Key, nil)
			if err != nil {
				return err
			}

			k4.memtable.Insert(op.Key, []byte(TOMBSTONE_VALUE), nil)
		// GET operations are read-only

		default:
			return fmt.Errorf("invalid operation")
		}
	}

	// Check if memtable needs to be flushed
	if k4.memtable.Size() > k4.memtableFlushThreshold {
		err := k4.flushMemtable()
		if err != nil {
			// Rollback transaction
			err = txn.Rollback(k4)
			if err != nil {
				return err
			}
			return err
		}
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
	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()

	value, found := k4.memtable.Search(key)
	if found {
		if bytes.Compare(value, []byte(TOMBSTONE_VALUE)) == 0 { // Check if the value is a tombstone
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
		value, err := sstable.get(key)
		if err != nil {
			return nil, err
		}
		if value != nil {
			return value, nil
		}
	}

	return nil, nil
}

// get gets a key from the SSTable
func (sstable *SSTable) get(key []byte) ([]byte, error) {
	// SStable pages are locked on read so no need to lock general sstable

	// Read the bloom filter
	bfData, err := sstable.pager.GetPage(0)
	if err != nil {
		return nil, err
	}

	bf, err := bloomfilter.Deserialize(bfData)
	if err != nil {
		return nil, err
	}

	// Check if the key exists in the bloom filter
	if !bf.Check(key) {
		return nil, nil
	}

	// Iterate over SSTable
	it := newSSTableIterator(sstable.pager)
	for it.next() {
		k, v := it.current()

		if bytes.Compare(k, key) == 0 {
			return v, nil
		}
	}

	return nil, nil
}

// Put puts a key-value pair into K4
func (k4 *K4) Put(key, value []byte, ttl *time.Duration) error {

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
	if k4.memtable.Size() > k4.memtableFlushThreshold {
		err := k4.flushMemtable()
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete deletes a key from K4
func (k4 *K4) Delete(key []byte) error {
	// We simply put a tombstone value for the key
	return k4.Put(key, []byte(TOMBSTONE_VALUE), nil)
}

// NGet gets a key from K4 and returns a map of key-value pairs
func (k4 *K4) NGet(key []byte) (*KeyValueArray, error) {
	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		k, value := it.Current()
		if !bytes.Equal(k, key) && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
		it := newSSTableIterator(sstable.pager)
		for it.next() {
			k, value := it.current()
			if !bytes.Equal(k, key) && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		k, value := it.Current()
		if bytes.Compare(k, key) > 0 && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
		it := newSSTableIterator(sstable.pager)
		for it.next() {
			k, value := it.current()
			if bytes.Compare(k, key) > 0 && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		k, value := it.Current()
		if bytes.Compare(k, key) >= 0 && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
		it := newSSTableIterator(sstable.pager)
		for it.next() {
			k, value := it.current()
			if bytes.Compare(k, key) >= 0 && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		k, value := it.Current()
		if bytes.Compare(k, key) < 0 && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
		it := newSSTableIterator(sstable.pager)
		for it.next() {
			k, value := it.current()
			if bytes.Compare(k, key) < 0 && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		k, value := it.Current()
		if bytes.Compare(k, key) <= 0 && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
		it := newSSTableIterator(sstable.pager)
		for it.next() {
			k, value := it.current()
			if bytes.Compare(k, key) <= 0 && bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		key, value := it.Current()
		if bytes.Compare(key, startKey) >= 0 && bytes.Compare(key, endKey) <= 0 {
			if bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
		it := newSSTableIterator(sstable.pager)
		for it.next() {
			key, value := it.current()
			if bytes.Compare(key, startKey) >= 0 && bytes.Compare(key, endKey) <= 0 {
				if bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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

// NRange queries keys in a range from K4 and returns a map of key-value pairs
func (k4 *K4) NRange(startKey, endKey []byte) (*KeyValueArray, error) {
	result := &KeyValueArray{}

	// Check memtable
	k4.memtableLock.RLock()
	it := skiplist.NewIterator(k4.memtable)
	for it.Next() {
		key, value := it.Current()
		if bytes.Compare(key, startKey) < 0 || bytes.Compare(key, endKey) > 0 {
			if bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
		it := newSSTableIterator(sstable.pager)
		for it.next() {
			key, value := it.current()
			if bytes.Compare(key, startKey) < 0 || bytes.Compare(key, endKey) > 0 {
				if bytes.Compare(value, []byte(TOMBSTONE_VALUE)) != 0 {
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
	*kva = append(*kva, kv)
}

// binarySearch method to find a KeyValue by key using binary search
func (kva KeyValueArray) binarySearch(key []byte) (*KV, bool) {
	index := sort.Search(len(kva), func(i int) bool {
		return bytes.Compare(kva[i].Key, key) >= 0
	})
	if index < len(kva) && bytes.Equal(kva[index].Key, key) {
		return kva[index], true
	}
	return nil, false
}
