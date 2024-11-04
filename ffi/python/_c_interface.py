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
import ctypes
from ctypes import c_char_p, c_int, c_int64, Structure, POINTER

# Load the shared library
# You must build the shared library before running, obviously
lib = ctypes.CDLL('../../c/libk4.so')

# Define GoSlice structure
class GoSlice(Structure):
    _fields_ = [("data", ctypes.c_void_p), ("len", ctypes.c_int), ("cap", ctypes.c_int)]

# Define return structures
class GreaterThanReturn(Structure):
    _fields_ = [("r0", GoSlice), ("r1", GoSlice)]

class LessThanReturn(Structure):
    _fields_ = [("r0", GoSlice), ("r1", GoSlice)]

class NGetReturn(Structure):
    _fields_ = [("r0", GoSlice), ("r1", GoSlice)]

class GreaterThanEqReturn(Structure):
    _fields_ = [("r0", GoSlice), ("r1", GoSlice)]

class RangeReturn(Structure):
    _fields_ = [("r0", GoSlice), ("r1", GoSlice)]

class NRangeReturn(Structure):
    _fields_ = [("r0", GoSlice), ("r1", GoSlice)]

class IterNextReturn(Structure):
    _fields_ = [("r0", c_char_p), ("r1", c_char_p)]

class IterPrevReturn(Structure):
    _fields_ = [("r0", c_char_p), ("r1", c_char_p)]

# Define function prototypes
lib.db_open.argtypes = [c_char_p, c_int, c_int, c_int, c_int]
lib.db_open.restype = c_int

lib.db_close.restype = c_int

lib.db_put.argtypes = [c_char_p, c_char_p, c_int64]
lib.db_put.restype = c_int

lib.db_get.argtypes = [c_char_p]
lib.db_get.restype = c_char_p

lib.db_delete.argtypes = [c_char_p]
lib.db_delete.restype = c_int

lib.begin_transaction.restype = c_int

lib.add_operation.argtypes = [c_int, c_char_p, c_char_p]
lib.add_operation.restype = c_int

lib.remove_transaction.restype = c_int

lib.rollback_transaction.restype = c_int

lib.commit_transaction.restype = c_int

lib.recover_from_wal.restype = c_int

lib.greater_than.argtypes = [c_char_p]
lib.greater_than.restype = GreaterThanReturn

lib.less_than.argtypes = [c_char_p]
lib.less_than.restype = LessThanReturn

lib.nget.argtypes = [c_char_p]
lib.nget.restype = NGetReturn

lib.greater_than_eq.argtypes = [c_char_p]
lib.greater_than_eq.restype = GreaterThanEqReturn

lib.range_.argtypes = [c_char_p, c_char_p]
lib.range_.restype = RangeReturn

lib.nrange.argtypes = [c_char_p, c_char_p]
lib.nrange.restype = NRangeReturn

lib.new_iterator.restype = c_int

lib.iter_next.restype = IterNextReturn

lib.iter_prev.restype = IterPrevReturn

lib.iter_reset.restype = None