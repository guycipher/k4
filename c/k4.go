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
	"unsafe"
)

const (
	//export OPR_PUT is the operation code for setting a key-value pair
	OPR_PUT = 0
	//export OPR_DEL is the operation code for deleting a key-value pair
	OPR_DEL = 1
	// Above does not export the constants to C for some odd reason
)

//export db_open
func db_open(directory *C.char, memtableFlushThreshold C.int, compactionInterval C.int, logging C.int, compress C.int) unsafe.Pointer {
	db, err := k4.Open(C.GoString(directory), int(memtableFlushThreshold), int(compactionInterval), logging != 0, compress != 0)
	if err != nil {
		return nil
	}

	return unsafe.Pointer(db)
}

//export db_close
func db_close(dbPtr unsafe.Pointer) C.int {
	db := (*k4.K4)(dbPtr)
	err := db.Close()
	if err != nil {
		return -1
	}

	return 0
}

//export db_put
func db_put(dbPtr unsafe.Pointer, key *C.char, keyLen C.int, value *C.char, valueLen C.int, ttl C.int64_t) C.int {
	db := (*k4.K4)(dbPtr)
	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	valueBytes := C.GoBytes(unsafe.Pointer(value), valueLen)

	if ttl == -1 {
		err := db.Put(keyBytes, valueBytes, nil)
		if err != nil {
			return -1
		}
		return 0
	}

	ttlDuration := time.Duration(ttl)
	err := db.Put(keyBytes, valueBytes, &ttlDuration)
	if err != nil {
		return -1
	}
	return 0
}

//export db_get
func db_get(dbPtr unsafe.Pointer, key *C.char, keyLen C.int) *C.char {
	db := (*k4.K4)(dbPtr)
	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	value, err := db.Get(keyBytes)
	if err != nil {
		return nil
	}
	return C.CString(string(value))
}

//export db_delete
func db_delete(dbPtr unsafe.Pointer, key *C.char, keyLen C.int) C.int {
	db := (*k4.K4)(dbPtr)
	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	err := db.Delete(keyBytes)
	if err != nil {
		return -1
	}
	return 0
}

//export begin_transaction
func begin_transaction(dbPtr unsafe.Pointer) unsafe.Pointer {
	db := (*k4.K4)(dbPtr)
	tx := db.BeginTransaction()
	return unsafe.Pointer(tx)
}

//export add_operation
func add_operation(txPtr unsafe.Pointer, operation C.int, key *C.char, keyLen C.int, value *C.char, valueLen C.int) C.int {
	txn := (*k4.Transaction)(txPtr)
	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	valueBytes := C.GoBytes(unsafe.Pointer(value), valueLen)

	txn.AddOperation(k4.OPR_CODE(operation), []byte(keyBytes), []byte(valueBytes))

	return 0
}

//export remove_transaction
func remove_transaction(dbPtr unsafe.Pointer, txPtr unsafe.Pointer) {
	db := (*k4.K4)(dbPtr)
	txn := (*k4.Transaction)(txPtr)
	txn.Remove(db)

}

//export commit_transaction
func commit_transaction(txPtr unsafe.Pointer, dbPtr unsafe.Pointer) C.int {
	txn := (*k4.Transaction)(txPtr)
	db := (*k4.K4)(dbPtr)
	err := txn.Commit(db)
	if err != nil {
		return -1
	}
	return 0
}

//export rollback_transaction
func rollback_transaction(txPtr unsafe.Pointer, dbPtr unsafe.Pointer) C.int {
	txn := (*k4.Transaction)(txPtr)
	db := (*k4.K4)(dbPtr)
	err := txn.Rollback(db)
	if err != nil {
		return -1
	}
	return 0
}

//export recover_from_wal
func recover_from_wal(dbPtr unsafe.Pointer) C.int {
	db := (*k4.K4)(dbPtr)
	err := db.RecoverFromWAL()
	if err != nil {
		return -1
	}
	return 0
}

//export greater_than
func greater_than(dbPtr unsafe.Pointer, key *C.char, keyLen C.int) ([]*C.char, []*C.char) {
	db := (*k4.K4)(dbPtr)
	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	keysValuePairs, err := db.GreaterThan(keyBytes)
	if err != nil {
		return nil, nil
	}

	var cKeys []*C.char
	var cValues []*C.char

	for _, kv := range *keysValuePairs {
		cKeys = append(cKeys, C.CString(string(kv.Key)))
		cValues = append(cValues, C.CString(string(kv.Value)))

	}

	return cKeys, cValues

}

//export less_than
func less_than(dbPtr unsafe.Pointer, key *C.char, keyLen C.int) ([]*C.char, []*C.char) {
	db := (*k4.K4)(dbPtr)
	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	keysValuePairs, err := db.LessThan(keyBytes)
	if err != nil {
		return nil, nil
	}

	var cKeys []*C.char
	var cValues []*C.char

	for _, kv := range *keysValuePairs {
		cKeys = append(cKeys, C.CString(string(kv.Key)))
		cValues = append(cValues, C.CString(string(kv.Value)))

	}

	return cKeys, cValues
}

//export nget
func nget(dbPtr unsafe.Pointer, key *C.char, keyLen C.int, n C.int) ([]*C.char, []*C.char) {
	db := (*k4.K4)(dbPtr)
	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	keysValuePairs, err := db.NGet(keyBytes)
	if err != nil {
		return nil, nil
	}

	var cKeys []*C.char
	var cValues []*C.char

	for _, kv := range *keysValuePairs {
		cKeys = append(cKeys, C.CString(string(kv.Key)))
		cValues = append(cValues, C.CString(string(kv.Value)))

	}

	return cKeys, cValues
}

//export greater_than_eq
func greater_than_eq(dbPtr unsafe.Pointer, key *C.char, keyLen C.int) ([]*C.char, []*C.char) {
	db := (*k4.K4)(dbPtr)
	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	keysValuePairs, err := db.GreaterThanEq(keyBytes)
	if err != nil {
		return nil, nil
	}

	var cKeys []*C.char
	var cValues []*C.char

	for _, kv := range *keysValuePairs {
		cKeys = append(cKeys, C.CString(string(kv.Key)))
		cValues = append(cValues, C.CString(string(kv.Value)))

	}

	return cKeys, cValues
}

//export less_than_eq
func less_than_eq(dbPtr unsafe.Pointer, key *C.char, keyLen C.int) ([]*C.char, []*C.char) {
	db := (*k4.K4)(dbPtr)
	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	keysValuePairs, err := db.LessThanEq(keyBytes)
	if err != nil {
		return nil, nil
	}

	var cKeys []*C.char
	var cValues []*C.char

	for _, kv := range *keysValuePairs {
		cKeys = append(cKeys, C.CString(string(kv.Key)))
		cValues = append(cValues, C.CString(string(kv.Value)))

	}

	return cKeys, cValues
}

//export range_
func range_(dbPtr unsafe.Pointer, start *C.char, startLen C.int, end *C.char, endLen C.int) ([]*C.char, []*C.char) {
	db := (*k4.K4)(dbPtr)
	startBytes := C.GoBytes(unsafe.Pointer(start), startLen)
	endBytes := C.GoBytes(unsafe.Pointer(end), endLen)
	keysValuePairs, err := db.Range(startBytes, endBytes)
	if err != nil {
		return nil, nil
	}

	var cKeys []*C.char
	var cValues []*C.char

	for _, kv := range *keysValuePairs {
		cKeys = append(cKeys, C.CString(string(kv.Key)))
		cValues = append(cValues, C.CString(string(kv.Value)))

	}

	return cKeys, cValues
}

//export nrange
func nrange(dbPtr unsafe.Pointer, start *C.char, startLen C.int, end *C.char, endLen C.int, n C.int) ([]*C.char, []*C.char) {
	db := (*k4.K4)(dbPtr)
	startBytes := C.GoBytes(unsafe.Pointer(start), startLen)
	endBytes := C.GoBytes(unsafe.Pointer(end), endLen)
	keysValuePairs, err := db.NRange(startBytes, endBytes)
	if err != nil {
		return nil, nil
	}

	var cKeys []*C.char
	var cValues []*C.char

	for _, kv := range *keysValuePairs {
		cKeys = append(cKeys, C.CString(string(kv.Key)))
		cValues = append(cValues, C.CString(string(kv.Value)))

	}

	return cKeys, cValues
}

//export new_iterator
func new_iterator(dbPtr unsafe.Pointer) unsafe.Pointer {
	db := (*k4.K4)(dbPtr)
	iter := k4.NewIterator(db)
	return unsafe.Pointer(iter)
}

//export iter_next
func iter_next(iterPtr unsafe.Pointer) (*C.char, *C.char) {
	iter := (*k4.Iterator)(iterPtr)
	key, value := iter.Next()
	if key == nil {
		return nil, nil
	}

	return C.CString(string(key)), C.CString(string(value))
}

//export iter_prev
func iter_prev(iterPtr unsafe.Pointer) (*C.char, *C.char) {
	iter := (*k4.Iterator)(iterPtr)
	key, value := iter.Prev()
	if key == nil {
		return nil, nil
	}

	return C.CString(string(key)), C.CString(string(value))
}

//export iter_reset
func iter_reset(iterPtr unsafe.Pointer) {
	iter := (*k4.Iterator)(iterPtr)
	iter.Reset()
}

func main() {}
