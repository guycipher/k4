// K4 Node.JS FFI
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
const ffi = require('ffi-napi');
const ref = require('ref-napi');

// KeyValuePair is a struct with two fields: key and value
const KeyValuePair = new ffi.StructType({
    key: 'string',
    value: 'string'
});

// KeyValuePairArray is an array of KeyValuePair structs
const KeyValuePairArray = new ffi.StructType({
    pairs: KeyValuePair,
    numPairs: 'int'
});

// IterNextReturn is the return type of the iter_next function
const IterNextReturn = new ffi.StructType({
    r0: 'string',
    r1: 'string'
});

// IterPrevReturn is the return type of the iter_prev function
const IterPrevReturn = new ffi.StructType({
    r0: 'string',
    r1: 'string'
});

// Load the K4 library
// note you must have the library installed in your system
const k4 = ffi.Library('libk4', {
    'db_open': ['pointer', ['string', 'int', 'int', 'int', 'int']],
    'db_close': ['int', ['pointer']],
    'db_put': ['int', ['pointer', 'string', 'int', 'string', 'int', 'int64']],
    'db_get': ['string', ['pointer', 'string', 'int']],
    'db_delete': ['int', ['pointer', 'string', 'int']],
    'begin_transaction': ['pointer', ['pointer']],
    'add_operation': ['int', ['pointer', 'int', 'string', 'int', 'string', 'int']],
    'remove_transaction': ['void', ['pointer', 'pointer']],
    'commit_transaction': ['int', ['pointer', 'pointer']],
    'rollback_transaction': ['int', ['pointer', 'pointer']],
    'recover_from_wal': ['int', ['pointer']],
    'range_': [KeyValuePairArray, ['pointer', 'string', 'int', 'string', 'int']],
    'nrange': [KeyValuePairArray, ['pointer', 'string', 'int', 'string', 'int']],
    'greater_than': [KeyValuePairArray, ['pointer', 'string', 'int']],
    'less_than': [KeyValuePairArray, ['pointer', 'string', 'int']],
    'nget': [KeyValuePairArray, ['pointer', 'string', 'int']],
    'greater_than_eq': [KeyValuePairArray, ['pointer', 'string', 'int']],
    'less_than_eq': [KeyValuePairArray, ['pointer', 'string', 'int']],
    'new_iterator': ['pointer', ['pointer']],
    'iter_next': [IterNextReturn, ['pointer']],
    'iter_prev': [IterPrevReturn, ['pointer']],
    'iter_reset': ['void', ['pointer']],
    'iter_close': ['void', ['pointer']]
});