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

// KV mainly used for serialization
type KV struct {
	Key   []byte
	Value []byte
}

// Open opens a new K4 instance at the specified directory.
// will reopen the database if it already exists
// directory - the directory where the database files are stored
// memtableFlushThreshold - the threshold in bytes for flushing the memtable to disk
// compactionInterval - the interval in seconds for running compactions
// logging - whether or not to log to the log file
func Open(directory string, memtableFlushThreshold int, compactionInterval int, logging bool) (*K4, error) {
	// Create directory if it doesn't exist
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return nil, err
	}

	// Initialize K4
	k4 := &K4{
		memtable:               skiplist.NewSkipList(12, 0.25),
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

	// Load SSTables
	k4.loadSSTables()

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
	go k4.backgroundWalWriter()

	return k4, nil
}

// Close closes the K4
func (k4 *K4) Close() error {

	k4.printLog("Closing up")

	// wait for the wal writer to finish
	close(k4.exit)

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
		select {
		case <-k4.exit:
			return
		default:
			k4.walQueueLock.Lock()
			if len(k4.walQueue) > 0 {
				op := k4.walQueue[0]
				k4.walQueue = k4.walQueue[1:]
				k4.walQueueLock.Unlock()

				err := k4.WriteToWAL(op.op, op.key, op.value)
				if err != nil {
					k4.printLog(fmt.Sprintf("Failed to write to WAL: %v", err))
				}

			} else {
				k4.walQueueLock.Unlock()
			}
		}
	}
}

// SerializeOp serializes an operation
func SerializeOp(op OPR_CODE, key, value []byte) []byte {
	var buf bytes.Buffer

	// use gob

	enc := gob.NewEncoder(&buf)

	operation := Operation{
		op:    op,
		key:   key,
		value: value,
	}

	err := enc.Encode(operation)
	if err != nil {
		return nil
	}

	return buf.Bytes()

}

// DeserializeOp deserializes an operation
func DeserializeOp(data []byte) (OPR_CODE, []byte, []byte, error) {

	operation := Operation{}

	dec := gob.NewDecoder(bytes.NewReader(data))

	err := dec.Decode(&operation)

	if err != nil {
		return 0, nil, nil, err
	}

	return operation.op, operation.key, operation.value, nil
}

// SerializeKv serializes a key-value pair
func SerializeKv(key, value []byte) []byte {
	var buf bytes.Buffer

	// use gob

	enc := gob.NewEncoder(&buf)

	kv := KV{
		Key:   key,
		Value: value,
	}

	err := enc.Encode(kv)
	if err != nil {
		return nil
	}

	return buf.Bytes()
}

// DeserializeKv deserializes a key-value pair
func DeserializeKv(data []byte) (key, value []byte, err error) {

	kv := KV{}

	dec := gob.NewDecoder(bytes.NewReader(data))

	err = dec.Decode(&kv)

	if err != nil {
		return nil, nil, err
	}

	return kv.Key, kv.Value, nil

}

// loadSSTables loads SSTables from the directory
func (k4 *K4) loadSSTables() {
	// Open directory
	dir, err := os.Open(k4.directory)
	if err != nil {
		return
	}
	defer dir.Close()

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
	})

	// Open and append SSTables
	for _, file := range sstableFiles {
		sstablePager, err := pager.OpenPager(k4.directory+string(os.PathSeparator)+file.Name(), os.O_RDWR, 0644)
		if err != nil {
			continue
		}

		k4.sstables = append(k4.sstables, &SSTable{
			pager: sstablePager,
			lock:  &sync.RWMutex{},
		})
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
		data := SerializeKv(key, value)

		// Write to SSTable
		_, err := sstable.pager.Write(data)
		if err != nil {
			return err
		}

	}

	// Append SSTable to list of SSTables
	k4.sstables = append(k4.sstables, sstable)

	// Clear memtable
	k4.memtable = skiplist.NewSkipList(12, 0.25)

	k4.printLog("Flushed memtable")

	if time.Since(k4.lastCompaction).Seconds() > float64(k4.compactionInterval) {
		k4.compact()
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

func NewSSTableIterator(pager *pager.Pager) *SSTableIterator {
	return &SSTableIterator{
		pager:       pager,
		currentPage: 1,
		lastPage:    int(pager.Count() - 1),
	}
}

// Next returns true if there is another key-value pair in the SSTable
func (it *SSTableIterator) Next() bool {
	if it.currentPage > it.lastPage {
		return false
	}

	it.currentPage++
	return true
}

// Current returns the current key-value pair in the SSTable
func (it *SSTableIterator) Current() ([]byte, []byte) {
	data, err := it.pager.GetPage(int64(it.currentPage))
	if err != nil {
		return nil, nil
	}

	key, value, err := DeserializeKv(data)
	if err != nil {
		return nil, nil
	}

	return key, value
}

// CurrentKey returns the current key in the SSTable
func (it *SSTableIterator) CurrentKey() []byte {
	data, err := it.pager.GetPage(int64(it.currentPage))
	if err != nil {
		return nil
	}
	key, _, err := DeserializeKv(data)
	if err != nil {
		return nil
	}
	return key
}

// NewWALIterator creates a new WAL iterator
func NewWALIterator(pager *pager.Pager) *WALIterator {
	return &WALIterator{
		pager:       pager,
		currentPage: 1,
		lastPage:    int(pager.Count() - 1),
	}
}

// Next returns true if there is another operation in the WAL
func (it *WALIterator) Next() bool {
	if it.currentPage > it.lastPage {
		return false
	}

	it.currentPage++
	return true
}

// Current returns the current operation in the WAL
func (it *WALIterator) Current() (OPR_CODE, []byte, []byte) {
	data, err := it.pager.GetPage(int64(it.currentPage))
	if err != nil {
		return -1, nil, nil
	}

	// Deserialize operation
	op, key, value, err := DeserializeOp(data)
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

	// we do a pairwise merge of the sstables
	// what this means is that we will merge the first sstable with the second sstable and so on
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
		it := NewSSTableIterator(sstable1.pager)
		for it.Next() {
			key := it.CurrentKey()
			bf.Add(key)
		}

		it = NewSSTableIterator(sstable2.pager)
		for it.Next() {
			key := it.CurrentKey()
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
		it = NewSSTableIterator(sstable1.pager)
		for it.Next() {
			key, value := it.Current()

			// Serialize key-value pair
			data := SerializeKv(key, value)

			// Write to SSTable
			_, err := newSstable.pager.Write(data)
			if err != nil {
				return err
			}
		}

		it = NewSSTableIterator(sstable2.pager)

		for it.Next() {
			key, value := it.Current()

			// Serialize key-value pair
			data := SerializeKv(key, value)

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
	}

	k4.printLog("Compaction completed")

	return nil
}

// RecoverFromWAL recovers K4 from a write ahead log
func (k4 *K4) RecoverFromWAL() error {
	// Iterate over the write ahead log
	it := NewWALIterator(k4.wal)
	for it.Next() {
		op, key, value := it.Current()
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
		}
	}

	return nil

}

// WriteToWAL writes an operation to the write ahead log
func (k4 *K4) WriteToWAL(op OPR_CODE, key, value []byte) error {
	operation := &Operation{
		op:    op,
		key:   key,
		value: value,
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
		id:  int64(len(k4.transactions)) + 1,
		ops: make([]*Operation, 0),
	}

	k4.transactions = append(k4.transactions, transaction)

	return transaction

}

// AddOperation adds an operation to a transaction
func (txn *Transaction) AddOperation(op OPR_CODE, key, value []byte) {
	operation := &Operation{
		op:    op,
		key:   key,
		value: value,
	}

	// Based on the operation, we can determine the rollback operation
	switch op {
	case PUT:
		operation.rollback = &Operation{
			op:    DELETE,
			key:   key,
			value: nil,
		}
	case DELETE:
		operation.rollback = &Operation{
			op:    PUT,
			key:   key,
			value: value,
		}
	}

	txn.ops = append(txn.ops, operation)
}

// Commit commits a transaction
func (txn *Transaction) Commit(k4 *K4) error {
	k4.memtableLock.Lock() // Makes the transaction atomic and serializable
	defer k4.memtableLock.Unlock()

	// Apply operations to memtable
	for _, op := range txn.ops {
		switch op.op {
		case PUT:
			k4.memtable.Insert(op.key, op.value, nil)
		case DELETE:
			k4.memtable.Insert(op.key, []byte(TOMBSTONE_VALUE), nil)
		}
	}

	// Check if memtable needs to be flushed
	if k4.memtable.Size() > k4.memtableFlushThreshold {
		err := k4.flushMemtable()
		if err != nil {
			return err
		}
	}

	return nil
}

// Rollback rolls back a transaction (after a commit)
func (txn *Transaction) Rollback(k4 *K4) error {

	// Apply rollback operations to memtable
	for i := len(txn.ops) - 1; i >= 0; i-- {
		op := txn.ops[i]
		switch op.op {
		case PUT:
			k4.Delete(op.key)
		case DELETE:
			k4.Put(op.key, op.value, nil)
		}
	}

	return nil
}

// Remove removes a transaction from the list of transactions in K4
func (txn *Transaction) Remove(k4 *K4) {
	// Clear operations
	txn.ops = make([]*Operation, 0)

	// Find and remove transaction
	for i, t := range k4.transactions {
		if t == txn {
			k4.transactions = append(k4.transactions[:i], k4.transactions[i+1:]...)
			break
		}
	}
}

// Get gets a key from K4
func (k4 *K4) Get(key []byte) ([]byte, error) {
	// Check memtable
	k4.memtableLock.RLock()
	defer k4.memtableLock.RUnlock()
	value, found := k4.memtable.Search(key)
	if found {
		if bytes.Compare(value, []byte(TOMBSTONE_VALUE)) == 0 {
			return nil, nil
		}

		return value, nil
	}

	// Check SSTables
	k4.sstablesLock.RLock()
	defer k4.sstablesLock.RUnlock()
	for i := len(k4.sstables) - 1; i >= 0; i-- {
		sstable := k4.sstables[i]
		value, err := sstable.Get(key)
		if err != nil {
			return nil, err
		}
		if value != nil {
			return value, nil
		}
	}

	return nil, nil
}

// Get gets a key from the SSTable
func (sstable *SSTable) Get(key []byte) ([]byte, error) {
	// Read from SSTable
	sstable.lock.RLock()
	defer sstable.lock.RUnlock()

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
	it := NewSSTableIterator(sstable.pager)
	for it.Next() {
		k, v := it.Current()

		if bytes.Compare(k, key) == 0 {
			return v, nil
		}
	}

	return nil, nil
}

// Put puts a key-value pair into K4
func (k4 *K4) Put(key, value []byte, ttl *time.Duration) error {
	// Put into memtable
	k4.memtableLock.Lock()
	defer k4.memtableLock.Unlock()

	// Write to WAL
	err := k4.WriteToWAL(PUT, key, value)
	if err != nil {
		return err
	}

	k4.memtable.Insert(key, value, ttl)

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

	// Put tombstone into memtable
	return k4.Put(key, []byte(TOMBSTONE_VALUE), nil)
}
