// K4 Java FFI
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
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface K4 extends Library {
    K4 INSTANCE = Native.load("libk4", K4.class); // Load the shared library

    Pointer db_open(String directory, int memtableFlushThreshold, int compactionInterval, int logging, int compress);
    int db_close(Pointer dbPtr);
    int db_put(Pointer dbPtr, String key, int keyLen, String value, int valueLen, long ttl);
    Pointer db_get(Pointer dbPtr, String key, int keyLen);
    int db_delete(Pointer dbPtr, String key, int keyLen);
    Pointer begin_transaction(Pointer dbPtr);
    int add_operation(Pointer txPtr, int operation, String key, int keyLen, String value, int valueLen);
    void remove_transaction(Pointer dbPtr, Pointer txPtr);
    int commit_transaction(Pointer txPtr, Pointer dbPtr);
    int rollback_transaction(Pointer txPtr, Pointer dbPtr);
    int recover_from_wal(Pointer dbPtr);
    KeyValuePairArray.ByValue range_(Pointer dbPtr, String start, int startLen, String end, int endLen);
    KeyValuePairArray.ByValue nrange(Pointer dbPtr, String start, int startLen, String end, int endLen);
    KeyValuePairArray.ByValue greater_than(Pointer dbPtr, String key, int keyLen);
    KeyValuePairArray.ByValue less_than(Pointer dbPtr, String key, int keyLen);
    KeyValuePairArray.ByValue nget(Pointer dbPtr, String key, int keyLen);
    KeyValuePairArray.ByValue greater_than_eq(Pointer dbPtr, String key, int keyLen);
    KeyValuePairArray.ByValue less_than_eq(Pointer dbPtr, String key, int keyLen);
    Pointer new_iterator(Pointer dbPtr);
    K4.iter_next_return iter_next(Pointer iterPtr);
    K4.iter_prev_return iter_prev(Pointer iterPtr);
    void iter_reset(Pointer iterPtr);
    void iter_close(Pointer iterPtr);

    class iter_next_return extends Structure {
        public Pointer r0;
        public Pointer r1;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("r0", "r1");
        }

        public static class ByReference extends iter_next_return implements Structure.ByReference {}
        public static class ByValue extends iter_next_return implements Structure.ByValue {}
    }

    class iter_prev_return extends Structure {
        public Pointer r0;
        public Pointer r1;

        @Override
        protected List<String> getFieldOrder() {
            return Arrays.asList("r0", "r1");
        }

        public static class ByReference extends iter_prev_return implements Structure.ByReference {}
        public static class ByValue extends iter_prev_return implements Structure.ByValue {}
    }
}