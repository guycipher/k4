// K4 Rust FFI
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
extern crate libc;

use libc::{c_char, c_int, c_void, int64_t};
use std::ffi::CStr;
use std::ptr;

#[repr(C)]
pub struct KeyValuePair {
    key: *mut c_char,
    value: *mut c_char,
}

#[repr(C)]
pub struct KeyValuePairArray {
    pairs: *mut KeyValuePair,
    numPairs: c_int,
}

#[repr(C)]
pub struct IterNextReturn {
    r0: *mut c_char,
    r1: *mut c_char,
}

#[repr(C)]
pub struct IterPrevReturn {
    r0: *mut c_char,
    r1: *mut c_char,
}

#[link(name = "libk4")] // Link to K4 C library
extern "C" {
    pub fn db_open(directory: *const c_char, memtableFlushThreshold: c_int, compactionInterval: c_int, logging: c_int, compress: c_int) -> *mut c_void;
    pub fn db_close(dbPtr: *mut c_void) -> c_int;
    pub fn db_put(dbPtr: *mut c_void, key: *const c_char, keyLen: c_int, value: *const c_char, valueLen: c_int, ttl: int64_t) -> c_int;
    pub fn db_get(dbPtr: *mut c_void, key: *const c_char, keyLen: c_int) -> *mut c_char;
    pub fn db_delete(dbPtr: *mut c_void, key: *const c_char, keyLen: c_int) -> c_int;
    pub fn begin_transaction(dbPtr: *mut c_void) -> *mut c_void;
    pub fn add_operation(txPtr: *mut c_void, operation: c_int, key: *const c_char, keyLen: c_int, value: *const c_char, valueLen: c_int) -> c_int;
    pub fn remove_transaction(dbPtr: *mut c_void, txPtr: *mut c_void);
    pub fn commit_transaction(txPtr: *mut c_void, dbPtr: *mut c_void) -> c_int;
    pub fn rollback_transaction(txPtr: *mut c_void, dbPtr: *mut c_void) -> c_int;
    pub fn recover_from_wal(dbPtr: *mut c_void) -> c_int;
    pub fn range_(dbPtr: *mut c_void, start: *const c_char, startLen: c_int, end: *const c_char, endLen: c_int) -> KeyValuePairArray;
    pub fn nrange(dbPtr: *mut c_void, start: *const c_char, startLen: c_int, end: *const c_char, endLen: c_int) -> KeyValuePairArray;
    pub fn greater_than(dbPtr: *mut c_void, key: *const c_char, keyLen: c_int) -> KeyValuePairArray;
    pub fn less_than(dbPtr: *mut c_void, key: *const c_char, keyLen: c_int) -> KeyValuePairArray;
    pub fn nget(dbPtr: *mut c_void, key: *const c_char, keyLen: c_int) -> KeyValuePairArray;
    pub fn greater_than_eq(dbPtr: *mut c_void, key: *const c_char, keyLen: c_int) -> KeyValuePairArray;
    pub fn less_than_eq(dbPtr: *mut c_void, key: *const c_char, keyLen: c_int) -> KeyValuePairArray;
    pub fn new_iterator(dbPtr: *mut c_void) -> *mut c_void;
    pub fn iter_next(iterPtr: *mut c_void) -> IterNextReturn;
    pub fn iter_prev(iterPtr: *mut c_void) -> IterPrevReturn;
    pub fn iter_reset(iterPtr: *mut c_void);
    pub fn iter_close(iterPtr: *mut c_void);
}