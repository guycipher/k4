=begin
K4 Ruby FFI
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
=end
require 'ffi'

module K4
  extend FFI::Library
  ffi_lib 'libk4.so' # specify the path to the shared library

  class KeyValuePair < FFI::Struct
    layout :key, :pointer,
           :value, :pointer
  end

  class KeyValuePairArray < FFI::Struct
    layout :pairs, :pointer,
           :numPairs, :int
  end

  class IterNextReturn < FFI::Struct
    layout :r0, :pointer,
           :r1, :pointer
  end

  class IterPrevReturn < FFI::Struct
    layout :r0, :pointer,
           :r1, :pointer
  end

  attach_function :db_open, [:string, :int, :int, :int, :int], :pointer
  attach_function :db_close, [:pointer], :int
  attach_function :db_put, [:pointer, :string, :int, :string, :int, :int64], :int
  attach_function :db_get, [:pointer, :string, :int], :string
  attach_function :db_delete, [:pointer, :string, :int], :int
  attach_function :begin_transaction, [:pointer], :pointer
  attach_function :add_operation, [:pointer, :int, :string, :int, :string, :int], :int
  attach_function :remove_transaction, [:pointer, :pointer], :void
  attach_function :commit_transaction, [:pointer, :pointer], :int
  attach_function :rollback_transaction, [:pointer, :pointer], :int
  attach_function :recover_from_wal, [:pointer], :int
  attach_function :range_, [:pointer, :string, :int, :string, :int], KeyValuePairArray.by_value
  attach_function :nrange, [:pointer, :string, :int, :string, :int], KeyValuePairArray.by_value
  attach_function :greater_than, [:pointer, :string, :int], KeyValuePairArray.by_value
  attach_function :less_than, [:pointer, :string, :int], KeyValuePairArray.by_value
  attach_function :nget, [:pointer, :string, :int], KeyValuePairArray.by_value
  attach_function :greater_than_eq, [:pointer, :string, :int], KeyValuePairArray.by_value
  attach_function :less_than_eq, [:pointer, :string, :int], KeyValuePairArray.by_value
  attach_function :new_iterator, [:pointer], :pointer
  attach_function :iter_next, [:pointer], IterNextReturn.by_value
  attach_function :iter_prev, [:pointer], IterPrevReturn.by_value
  attach_function :iter_reset, [:pointer], :void
  attach_function :iter_close, [:pointer], :void
  attach_function :escalate_flush, [:pointer], :int
  attach_function :escalate_compaction, [:pointer], :int
end