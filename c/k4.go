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
struct KeyValuePair {
    char* key;
    char* value;
};

struct KeyValuePairArray {
    struct KeyValuePair* pairs;
    int numPairs;
};
*/
import "C"
import (
	"github.com/guycipher/k4"
	"runtime/cgo"
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

	// The handle is valid until the program calls Delete on it.
	// The handle uses resources, and this package assumes that C code may hold on to the handle,
	// so a program must explicitly call Delete when the handle is no longer needed.
	handle := cgo.NewHandle(db)

	return unsafe.Pointer(handle)
}

//export db_close
func db_close(dbPtr unsafe.Pointer) C.int {

	handle := cgo.Handle(dbPtr)
	db := handle.Value().(*k4.K4)
	err := db.Close()
	if err != nil {
		return -1
	}

	// Delete invalidates a handle. The handle is no longer valid after calling Delete.
	handle.Delete()

	return 0
}

//export db_put
func db_put(dbPtr unsafe.Pointer, key *C.char, keyLen C.int, value *C.char, valueLen C.int, ttl C.int64_t) C.int {
	handle := cgo.Handle(dbPtr)
	db := handle.Value().(*k4.K4)

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
	handle := cgo.Handle(dbPtr)
	db := handle.Value().(*k4.K4)

	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	value, err := db.Get(keyBytes)
	if err != nil {
		return nil
	}
	return C.CString(string(value))
}

//export db_delete
func db_delete(dbPtr unsafe.Pointer, key *C.char, keyLen C.int) C.int {
	handle := cgo.Handle(dbPtr)
	db := handle.Value().(*k4.K4)

	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	err := db.Delete(keyBytes)
	if err != nil {
		return -1
	}
	return 0
}

//export begin_transaction
func begin_transaction(dbPtr unsafe.Pointer) unsafe.Pointer {
	handle := cgo.Handle(dbPtr)
	db := handle.Value().(*k4.K4)

	tx := db.BeginTransaction()

	txnHandle := cgo.NewHandle(tx)

	return unsafe.Pointer(txnHandle)
}

//export add_operation
func add_operation(txPtr unsafe.Pointer, operation C.int, key *C.char, keyLen C.int, value *C.char, valueLen C.int) C.int {
	txnHandle := cgo.Handle(txPtr)
	txn := txnHandle.Value().(*k4.Transaction)

	keyBytes := C.GoBytes(unsafe.Pointer(key), keyLen)
	valueBytes := C.GoBytes(unsafe.Pointer(value), valueLen)

	txn.AddOperation(k4.OPR_CODE(operation), []byte(keyBytes), []byte(valueBytes))

	return 0
}

//export remove_transaction
func remove_transaction(dbPtr unsafe.Pointer, txPtr unsafe.Pointer) {
	handle := cgo.Handle(dbPtr)
	db := handle.Value().(*k4.K4)

	txnHandle := cgo.Handle(txPtr)
	txn := txnHandle.Value().(*k4.Transaction)

	txn.Remove(db)

	txnHandle.Delete()

}

//export commit_transaction
func commit_transaction(txPtr unsafe.Pointer, dbPtr unsafe.Pointer) C.int {
	handle := cgo.Handle(dbPtr)
	db := handle.Value().(*k4.K4)

	txnHandle := cgo.Handle(txPtr)
	txn := txnHandle.Value().(*k4.Transaction)

	err := txn.Commit(db)
	if err != nil {
		return -1
	}
	return 0
}

//export rollback_transaction
func rollback_transaction(txPtr unsafe.Pointer, dbPtr unsafe.Pointer) C.int {
	txnHandle := cgo.Handle(txPtr)
	txn := txnHandle.Value().(*k4.Transaction)

	handle := cgo.Handle(dbPtr)
	db := handle.Value().(*k4.K4)

	err := txn.Rollback(db)
	if err != nil {
		return -1
	}
	return 0
}

//export recover_from_wal
func recover_from_wal(dbPtr unsafe.Pointer) C.int {
	handle := cgo.Handle(dbPtr)
	db := handle.Value().(*k4.K4)

	err := db.RecoverFromWAL()
	if err != nil {
		return -1
	}
	return 0
}

//export range_
func range_(dbPtr unsafe.Pointer, start *C.char, startLen C.int, end *C.char, endLen C.int) C.struct_KeyValuePairArray {
	handle := cgo.Handle(dbPtr)
	db := handle.Value().(*k4.K4)

	startBytes := C.GoBytes(unsafe.Pointer(start), startLen)
	endBytes := C.GoBytes(unsafe.Pointer(end), endLen)
	keysValuePairs, err := db.Range(startBytes, endBytes)
	if err != nil {
		return C.struct_KeyValuePairArray{pairs: nil, numPairs: 0}
	}

	// Convert keysValuePairs from *k4.KeyValueArray to a slice
	keysValuePairsSlice := *keysValuePairs

	// Allocate memory for the array of KeyValuePair structs
	cArray := C.malloc(C.size_t(len(keysValuePairsSlice)) * C.size_t(unsafe.Sizeof(C.struct_KeyValuePair{})))
	cKeyValuePairs := (*[1 << 30]C.struct_KeyValuePair)(cArray)[:len(keysValuePairsSlice):len(keysValuePairsSlice)]

	// Populate the array with key-value pairs
	for i, kv := range keysValuePairsSlice {
		cKeyValuePairs[i].key = C.CString(string(kv.Key))
		cKeyValuePairs[i].value = C.CString(string(kv.Value))
	}

	return C.struct_KeyValuePairArray{pairs: (*C.struct_KeyValuePair)(cArray), numPairs: C.int(len(keysValuePairsSlice))}
}

func main() {}
