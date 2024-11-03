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

//export Open
func Open(directory *C.char, memtableFlushThreshold C.int, compactionInterval C.int, logging C.int, compress C.int) unsafe.Pointer {
	db, err := k4.Open(C.GoString(directory), int(memtableFlushThreshold), int(compactionInterval), logging != 0, compress != 0)
	if err != nil {
		return nil
	}
	return unsafe.Pointer(db)
}

//export Close
func Close(db unsafe.Pointer) {
	k4db := (*k4.K4)(db)
	k4db.Close()
}

//export Put
func Put(db unsafe.Pointer, key *C.char, value *C.char, ttl C.int64_t) C.int {
	k4db := (*k4.K4)(db)
	ttlDuration := time.Duration(ttl)

	err := k4db.Put([]byte(C.GoString(key)), []byte(C.GoString(value)), &ttlDuration)
	if err != nil {
		return -1
	}
	return 0
}

//export Get
func Get(db unsafe.Pointer, key *C.char) *C.char {
	k4db := (*k4.K4)(db)
	value, err := k4db.Get([]byte(C.GoString(key)))
	if err != nil {
		return nil
	}
	return C.CString(string(value))
}

//export Delete
func Delete(db unsafe.Pointer, key *C.char) C.int {
	k4db := (*k4.K4)(db)
	err := k4db.Delete([]byte(C.GoString(key)))
	if err != nil {
		return -1
	}
	return 0
}

//export BeginTransaction
func BeginTransaction(db unsafe.Pointer) unsafe.Pointer {
	k4db := (*k4.K4)(db)
	return unsafe.Pointer(k4db.BeginTransaction())
}

//export AddOperation
func AddOperation(txn unsafe.Pointer, op C.int, key *C.char, value *C.char) {
	transaction := (*k4.Transaction)(txn)
	transaction.AddOperation(k4.OPR_CODE(op), []byte(C.GoString(key)), []byte(C.GoString(value)))
}

//export RemoveTransaction
func RemoveTransaction(txn unsafe.Pointer, db unsafe.Pointer) {
	transaction := (*k4.Transaction)(txn)
	k4db := (*k4.K4)(db)
	transaction.Remove(k4db)
}

//export RollbackTransaction
func RollbackTransaction(txn unsafe.Pointer, db unsafe.Pointer) C.int {
	transaction := (*k4.Transaction)(txn)
	k4db := (*k4.K4)(db)
	err := transaction.Rollback(k4db)
	if err != nil {
		return -1
	}
	return 0
}

//export CommitTransaction
func CommitTransaction(txn unsafe.Pointer, db unsafe.Pointer) C.int {
	transaction := (*k4.Transaction)(txn)
	k4db := (*k4.K4)(db)
	err := transaction.Commit(k4db)
	if err != nil {
		return -1
	}
	return 0
}

//export RecoverFromWAL
func RecoverFromWAL(db unsafe.Pointer) C.int {
	k4db := (*k4.K4)(db)
	err := k4db.RecoverFromWAL()
	if err != nil {
		return -1
	}
	return 0
}

func main() {}
