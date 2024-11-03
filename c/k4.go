// C Library for K4
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
package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"github.com/guycipher/k4"
	"time"
)

var (
	globalDB   *k4.K4          // The global database instance
	currentTxn *k4.Transaction // The current transaction
	iter       *k4.Iterator
	// What differs in the C library as you can have 1 global database instance and 1 current transaction at a time
)

const (
	//export OPR_PUT is the operation code for setting a key-value pair
	OPR_PUT = 0
	//export OPR_DEL is the operation code for deleting a key-value pair
	OPR_DEL = 1
	// Above does not export the constants to C for some odd reason
)

//export db_open
func db_open(directory *C.char, memtableFlushThreshold C.int, compactionInterval C.int, logging C.int, compress C.int) C.int {
	db, err := k4.Open(C.GoString(directory), int(memtableFlushThreshold), int(compactionInterval), logging != 0, compress != 0)
	if err != nil {
		return -1
	}

	globalDB = db

	return 0
}

//export db_close
func db_close() C.int {
	err := globalDB.Close()
	if err != nil {
		return -1
	}

	return 0
}

//export db_put
func db_put(key *C.char, value *C.char, ttl C.int64_t) C.int {

	if ttl == -1 {
		err := globalDB.Put([]byte(C.GoString(key)), []byte(C.GoString(value)), nil)
		if err != nil {
			return -1
		}
		return 0
	}

	ttlDuration := time.Duration(ttl)

	err := globalDB.Put([]byte(C.GoString(key)), []byte(C.GoString(value)), &ttlDuration)
	if err != nil {
		return -1
	}
	return 0
}

//export db_get
func db_get(key *C.char) *C.char {
	value, err := globalDB.Get([]byte(C.GoString(key)))
	if err != nil {
		return nil
	}
	return C.CString(string(value))
}

//export db_delete
func db_delete(key *C.char) C.int {
	err := globalDB.Delete([]byte(C.GoString(key)))
	if err != nil {
		return -1
	}
	return 0
}

//export begin_transaction
func begin_transaction() C.int {
	currentTxn = globalDB.BeginTransaction()
	return 0
}

//export add_operation
func add_operation(op C.int, key *C.char, value *C.char) C.int {
	if currentTxn == nil {
		return -1
	}

	currentTxn.AddOperation(k4.OPR_CODE(op), []byte(C.GoString(key)), []byte(C.GoString(value)))

	return 0
}

//export remove_transaction
func remove_transaction() C.int {

	currentTxn.Remove(globalDB)

	currentTxn = nil

	return 0
}

//export rollback_transaction
func rollback_transaction() C.int {

	err := currentTxn.Rollback(globalDB)
	if err != nil {
		return -1
	}
	return 0
}

//export commit_transaction
func commit_transaction() C.int {

	err := currentTxn.Commit(globalDB)
	if err != nil {
		return -1
	}
	return 0
}

//export recover_from_wal
func recover_from_wal() C.int {
	if globalDB == nil {
		return -1
	}

	err := globalDB.RecoverFromWAL()
	if err != nil {
		return -1
	}
	return 0
}

//export new_iterator
func new_iterator() C.int {
	iter = k4.NewIterator(globalDB)
	return 0
}

//export iter_next
func iter_next() (key *C.char, value *C.char) {
	k, v := iter.Next()
	if k == nil {
		return nil, nil
	}
	return C.CString(string(k)), C.CString(string(v))
}

//export iter_prev
func iter_prev() (key *C.char, value *C.char) {
	k, v := iter.Prev()
	if k == nil {
		return nil, nil
	}
	return C.CString(string(k)), C.CString(string(v))
}

func main() {}
