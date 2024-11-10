// K4 C# FFI
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
using System;
using System.Runtime.InteropServices;

public static class K4
{
    private const string DllName = "libk4.so";

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr db_open(string directory, int memtableFlushThreshold, int compactionInterval, int logging, int compress);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int db_close(IntPtr dbPtr);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int db_put(IntPtr dbPtr, string key, int keyLen, string value, int valueLen, long ttl);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr db_get(IntPtr dbPtr, string key, int keyLen);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int db_delete(IntPtr dbPtr, string key, int keyLen);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr begin_transaction(IntPtr dbPtr);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int add_operation(IntPtr txPtr, int operation, string key, int keyLen, string value, int valueLen);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern void remove_transaction(IntPtr dbPtr, IntPtr txPtr);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int commit_transaction(IntPtr txPtr, IntPtr dbPtr);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int rollback_transaction(IntPtr txPtr, IntPtr dbPtr);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int recover_from_wal(IntPtr dbPtr);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern KeyValuePairArray range_(IntPtr dbPtr, string start, int startLen, string end, int endLen);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern KeyValuePairArray nrange(IntPtr dbPtr, string start, int startLen, string end, int endLen);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern KeyValuePairArray greater_than(IntPtr dbPtr, string key, int keyLen);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern KeyValuePairArray less_than(IntPtr dbPtr, string key, int keyLen);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern KeyValuePairArray nget(IntPtr dbPtr, string key, int keyLen);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern KeyValuePairArray greater_than_eq(IntPtr dbPtr, string key, int keyLen);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern KeyValuePairArray less_than_eq(IntPtr dbPtr, string key, int keyLen);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr new_iterator(IntPtr dbPtr);

    [StructLayout(LayoutKind.Sequential)]
    public struct IterNextReturn
    {
        public IntPtr r0;
        public IntPtr r1;
    }

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IterNextReturn iter_next(IntPtr iterPtr);

    [StructLayout(LayoutKind.Sequential)]
    public struct IterPrevReturn
    {
        public IntPtr r0;
        public IntPtr r1;
    }

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern IterPrevReturn iter_prev(IntPtr iterPtr);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern void iter_reset(IntPtr iterPtr);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern void iter_close(IntPtr iterPtr);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int escalate_flush(IntPtr dbPtr);

    [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
    public static extern int escalate_compaction(IntPtr dbPtr);
}