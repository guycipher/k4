-- K4 Lua FFI
-- BSD 3-Clause License
--
-- Copyright (c) 2024, Alex Gaetano Padula
-- All rights reserved.
--
-- Redistribution and use in source and binary forms, with or without
-- modification, are permitted provided that the following conditions are met:
--
--  1. Redistributions of source code must retain the above copyright notice, this
--     list of conditions and the following disclaimer.
--
--  2. Redistributions in binary form must reproduce the above copyright notice,
--     this list of conditions and the following disclaimer in the documentation
--     and/or other materials provided with the distribution.
--
--  3. Neither the name of the copyright holder nor the names of its
--     contributors may be used to endorse or promote products derived from
--     this software without specific prior written permission.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
-- AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
-- IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
-- DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
-- FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
-- DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
-- SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
-- CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
-- OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
-- OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

local ffi = require("ffi")

-- Load the shared library
local k4 = ffi.load("libk4.so") -- you gotta specify the path to the shared library

-- KeyValuePair is a structure that holds a key-value pair
ffi.cdef[[
typedef struct {
    char* key;
    char* value;
} KeyValuePair;
]]

-- KeyValuePairArray is a structure that holds an array of KeyValuePair structures
ffi.cdef[[
typedef struct {
    KeyValuePair* pairs;
    int numPairs;
} KeyValuePairArray;
]]

-- Define the iter_next_return structure
ffi.cdef[[
typedef struct {
    char* r0;
    char* r1;
} IterNextReturn;
]]

-- Define the iter_prev_return structure
ffi.cdef[[
typedef struct {
    char* r0;
    char* r1;
} IterPrevReturn;
]]

-- Define the function prototypes
ffi.cdef[[
void* db_open(const char* directory, int memtableFlushThreshold, int compactionInterval, int logging, int compress);
int db_close(void* dbPtr);
int db_put(void* dbPtr, const char* key, int keyLen, const char* value, int valueLen, int64_t ttl);
char* db_get(void* dbPtr, const char* key, int keyLen);
int db_delete(void* dbPtr, const char* key, int keyLen);
void* begin_transaction(void* dbPtr);
int add_operation(void* txPtr, int operation, const char* key, int keyLen, const char* value, int valueLen);
void remove_transaction(void* dbPtr, void* txPtr);
int commit_transaction(void* txPtr, void* dbPtr);
int rollback_transaction(void* txPtr, void* dbPtr);
int recover_from_wal(void* dbPtr);
KeyValuePairArray range_(void* dbPtr, const char* start, int startLen, const char* end, int endLen);
KeyValuePairArray nrange(void* dbPtr, const char* start, int startLen, const char* end, int endLen);
KeyValuePairArray greater_than(void* dbPtr, const char* key, int keyLen);
KeyValuePairArray less_than(void* dbPtr, const char* key, int keyLen);
KeyValuePairArray nget(void* dbPtr, const char* key, int keyLen);
KeyValuePairArray greater_than_eq(void* dbPtr, const char* key, int keyLen);
KeyValuePairArray less_than_eq(void* dbPtr, const char* key, int keyLen);
void* new_iterator(void* dbPtr);
IterNextReturn iter_next(void* iterPtr);
IterPrevReturn iter_prev(void* iterPtr);
void iter_reset(void* iterPtr);
void iter_close(void* iterPtr);
int escalate_flush(void* dbPtr);
int escalate_compaction(void* dbPtr);
]]