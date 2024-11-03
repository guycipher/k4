// k4.c - C Library for K4
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
#include "k4.h"
#include <stdlib.h>
#include <string.h>
#include "_cgo_export.h"

K4* k4_open(const char* directory, int memtableFlushThreshold, int compactionInterval, int logging, int compress) {
    return (K4*)Open(directory, memtableFlushThreshold, compactionInterval, logging, compress);
}

void k4_close(K4* db) {
    Close((void*)db);
}

int k4_put(K4* db, const char* key, const char* value, int64_t ttl) {
    if (ttl == -1) {
        return Put((void*)db, key, value, NULL);
    }
    return Put((void*)db, key, value, ttl);
}

char* k4_get(K4* db, const char* key) {
    char* value = Get((void*)db, key);
    if (value == NULL) {
        return NULL;
    }
    char* result = (char*)malloc(strlen(value) + 1);
    strcpy(result, value);
    return result;
}

int k4_delete(K4* db, const char* key) {
    return Delete((void*)db, key);
}

Transaction* k4_begin_transaction(K4* db) {
    return (Transaction*)BeginTransaction((void*)db);
}

void k4_add_operation(Transaction* txn, int op, const char* key, const char* value) {
    AddOperation((void*)txn, op, key, value);
}

void k4_remove_transaction(Transaction* txn, K4* db) {
    RemoveTransaction((void*)txn, (void*)db);
}

int k4_rollback_transaction(Transaction* txn, K4* db) {
    return RollbackTransaction((void*)txn, (void*)db);
}

int k4_commit_transaction(Transaction* txn, K4* db) {
    return CommitTransaction((void*)txn, (void*)db);
}

int k4_recover_from_wal(K4* db) {
    return RecoverFromWAL((void*)db);
}