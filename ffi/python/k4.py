"""
k4 - Python bindings for the k4 library
BSD 3-Clause License

Copyright (c) 2024, Alex Gaetano Padula
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
   contributors may be used to endorse or promote products derived from
   this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
from ._c_interface import lib

# db_open opens/reopens a database at the given directory
def db_open(directory, memtable_flush_threshold, compaction_interval, logging, compress):
    return lib.db_open(directory.encode('utf-8'), memtable_flush_threshold, compaction_interval, logging, compress)

# db_close closes the database gracefully
def db_close():
    return lib.db_close()

# db_put inserts a key-value pair into the database.  Inserting same key multiple times will overwrite the value.
def db_put(key, value, ttl):
    return lib.db_put(key.encode('utf-8'), value.encode('utf-8'), ttl)

# db_get retrieves the value of a key from the database
def db_get(key):
    result = lib.db_get(key.encode('utf-8'))
    return result.decode('utf-8') if result else None

# db_delete deletes a key-value pair from the database
def db_delete(key):
    return lib.db_delete(key.encode('utf-8'))

# begin_transaction starts a new transaction
def begin_transaction():
    return lib.begin_transaction()

# add_operation adds an operation to the current transaction, 0 for put, 1 for delete
def add_operation(op, key, value):
    return lib.add_operation(op, key.encode('utf-8'), value.encode('utf-8'))

# remove_transaction removes the current transaction
def remove_transaction():
    return lib.remove_transaction()

# rollback_transaction rolls back the current transaction
def rollback_transaction():
    return lib.rollback_transaction()

# commit_transaction commits the current transaction
def commit_transaction():
    return lib.commit_transaction()

# recover_from_wal recovers a database from the write-ahead log
def recover_from_wal():
    return lib.recover_from_wal()

# greater_than returns key value pairs greater than the given key
def greater_than(key):
    result = lib.greater_than(key.encode('utf-8'))
    return result.r0, result.r1

# less_than returns key value pairs less than the given key
def less_than(key):
    result = lib.less_than(key.encode('utf-8'))
    return result.r0, result.r1

# nget returns key value pairs not equal to the given key
def nget(key):
    result = lib.nget(key.encode('utf-8'))
    return result.r0, result.r1

# greater_than_eq returns key value pairs greater than or equal to the given key
def greater_than_eq(key):
    result = lib.greater_than_eq(key.encode('utf-8'))
    return result.r0, result.r1

# range_ returns key value pairs in the given range
def range_(start, end):
    result = lib.range_(start.encode('utf-8'), end.encode('utf-8'))
    return result.r0, result.r1

# nrange returns key value pairs not in the given range
def nrange(start, end):
    result = lib.nrange(start.encode('utf-8'), end.encode('utf-8'))
    return result.r0, result.r1

# new_iterator creates a new iterator
def new_iterator():
    return lib.new_iterator()

# iter_next moves the iterator to the next key value pair
def iter_next():
    result = lib.iter_next()
    return result.r0.decode('utf-8'), result.r1.decode('utf-8')

# iter_prev moves the iterator to the previous key value pair
def iter_prev():
    result = lib.iter_prev()
    return result.r0.decode('utf-8'), result.r1.decode('utf-8')

# iter_reset resets the iterator
def iter_reset():
    lib.iter_reset()