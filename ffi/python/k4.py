# K4 Python FFI
# BSD 3-Clause License
#
# Copyright (c) 2024, Alex Gaetano Padula
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#  1. Redistributions of source code must retain the above copyright notice, this
#     list of conditions and the following disclaimer.
#
#  2. Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#  3. Neither the name of the copyright holder nor the names of its
#     contributors may be used to endorse or promote products derived from
#     this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import ctypes
from ctypes import c_char_p, c_int, c_void_p, c_int64, Structure, POINTER

# Load the shared library
k4 = ctypes.CDLL('libk4.so') # you gotta specify the path to the shared library

# KeyValuePair is a structure that holds a key-value pair
class KeyValuePair(Structure):
    _fields_ = [("key", c_char_p),
                ("value", c_char_p)]

# KeyValuePairArray is a structure that holds an array of KeyValuePair's
class KeyValuePairArray(Structure):
    _fields_ = [("pairs", POINTER(KeyValuePair)),
                ("numPairs", c_int)]

# Define the iter_next_return structure
class IterNextReturn(Structure):
    _fields_ = [("r0", c_char_p),
                ("r1", c_char_p)]

# Define the iter_prev_return structure
class IterPrevReturn(Structure):
    _fields_ = [("r0", c_char_p),
                ("r1", c_char_p)]

# Define the K4 function prototypes
k4.db_open.argtypes = [c_char_p, c_int, c_int, c_int, c_int]
k4.db_open.restype = c_void_p

k4.db_close.argtypes = [c_void_p]
k4.db_close.restype = c_int

k4.db_put.argtypes = [c_void_p, c_char_p, c_int, c_char_p, c_int, c_int64]
k4.db_put.restype = c_int

k4.db_get.argtypes = [c_void_p, c_char_p, c_int]
k4.db_get.restype = c_char_p

k4.db_delete.argtypes = [c_void_p, c_char_p, c_int]
k4.db_delete.restype = c_int

k4.begin_transaction.argtypes = [c_void_p]
k4.begin_transaction.restype = c_void_p

k4.add_operation.argtypes = [c_void_p, c_int, c_char_p, c_int, c_char_p, c_int]
k4.add_operation.restype = c_int

k4.remove_transaction.argtypes = [c_void_p, c_void_p]

k4.commit_transaction.argtypes = [c_void_p, c_void_p]
k4.commit_transaction.restype = c_int

k4.rollback_transaction.argtypes = [c_void_p, c_void_p]
k4.rollback_transaction.restype = c_int

k4.recover_from_wal.argtypes = [c_void_p]
k4.recover_from_wal.restype = c_int

k4.range_.argtypes = [c_void_p, c_char_p, c_int, c_char_p, c_int]
k4.range_.restype = KeyValuePairArray

k4.nrange.argtypes = [c_void_p, c_char_p, c_int, c_char_p, c_int]
k4.nrange.restype = KeyValuePairArray

k4.greater_than.argtypes = [c_void_p, c_char_p, c_int]
k4.greater_than.restype = KeyValuePairArray

k4.less_than.argtypes = [c_void_p, c_char_p, c_int]
k4.less_than.restype = KeyValuePairArray

k4.nget.argtypes = [c_void_p, c_char_p, c_int]
k4.nget.restype = KeyValuePairArray

k4.greater_than_eq.argtypes = [c_void_p, c_char_p, c_int]
k4.greater_than_eq.restype = KeyValuePairArray

k4.less_than_eq.argtypes = [c_void_p, c_char_p, c_int]
k4.less_than_eq.restype = KeyValuePairArray

k4.new_iterator.argtypes = [c_void_p]
k4.new_iterator.restype = c_void_p

k4.iter_next.argtypes = [c_void_p]
k4.iter_next.restype = IterNextReturn

k4.iter_prev.argtypes = [c_void_p]
k4.iter_prev.restype = IterPrevReturn

k4.iter_reset.argtypes = [c_void_p]

k4.iter_close.argtypes = [c_void_p]

k4.escalate_flush.argtypes = [c_void_p]
k4.escalate_flush.restype = c_int

k4.escalate_compaction.argtypes = [c_void_p]
k4.escalate_compaction.restype = c_int