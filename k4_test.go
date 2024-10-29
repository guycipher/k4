// Package k4 tests
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
package k4

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func setup(t *testing.T) string {
	dir, err := ioutil.TempDir(".", "k4_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

func teardown(dir string) {
	os.RemoveAll(dir)
}

func TestOpenClose(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}

	err = k4.Close()
	if err != nil {
		t.Fatalf("Failed to close K4: %v", err)
	}
}

func TestPutGet(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	key := []byte("key1")
	value := []byte("value1")

	err = k4.Put(key, value, nil)
	if err != nil {
		t.Fatalf("Failed to put key-value: %v", err)
	}

	got, err := k4.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Fatalf("Expected value %s, got %s", value, got)
	}
}

func TestDelete(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	key := []byte("key1")
	value := []byte("value1")

	err = k4.Put(key, value, nil)
	if err != nil {
		t.Fatalf("Failed to put key-value: %v", err)
	}

	err = k4.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	got, err := k4.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if got != nil {
		t.Fatalf("Expected nil, got %s", got)
	}
}

func TestMemtableFlush(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 2764/2, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		value := []byte("value" + fmt.Sprintf("%d", i))

		err = k4.Put(key, value, nil)
		if err != nil {
			k4.Close()
			t.Fatalf("Failed to put key-value: %v", err)
		}
	}

	k4.Close()

	k4, err = Open(dir, 1024*1024, 2, false)
	if err != nil {
		t.Fatalf("Failed to reopen K4: %v", err)
	}
	defer k4.Close()

	// get all keys
	for i := 0; i < 100; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		value := []byte("value" + fmt.Sprintf("%d", i))

		got, err := k4.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key: %v", err)
		}

		if !bytes.Equal(got, value) {
			t.Fatalf("Expected value %s, got %s", value, got)
		}
	}
}

func TestCompaction(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 2764/4, 1, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		value := []byte("value" + fmt.Sprintf("%d", i))

		err = k4.Put(key, value, nil)
		if err != nil {
			k4.Close()
			t.Fatalf("Failed to put key-value: %v", err)
		}

	}

	k4.Close()

	k4, err = Open(dir, 1024*1024, 2, false)
	if err != nil {
		t.Fatalf("Failed to reopen K4: %v", err)
	}
	defer k4.Close()

	// get all keys
	for i := 0; i < 100; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		value := []byte("value" + fmt.Sprintf("%d", i))

		got, err := k4.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key: %v", err)
		}

		if !bytes.Equal(got, value) {
			t.Fatalf("Expected value %s, got %s", value, got)
		}
	}
}

func TestTransactionCommit(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	txn := k4.BeginTransaction()
	txn.AddOperation(PUT, []byte("key1"), []byte("value1"))
	txn.AddOperation(PUT, []byte("key2"), []byte("value2"))

	err = txn.Commit(k4)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	value, err := k4.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if !bytes.Equal(value, []byte("value1")) {
		t.Fatalf("Expected value1, got %s", value)
	}

	value, err = k4.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get key2: %v", err)
	}
	if !bytes.Equal(value, []byte("value2")) {
		t.Fatalf("Expected value2, got %s", value)
	}
}

func TestTransactionRollback(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	txn := k4.BeginTransaction()
	txn.AddOperation(PUT, []byte("key1"), []byte("value1"))
	txn.AddOperation(PUT, []byte("key2"), []byte("value2"))

	err = txn.Commit(k4)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	err = txn.Rollback(k4)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	txn.Remove(k4)

	value, err := k4.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if value != nil {
		t.Fatalf("Expected nil, got %s", value)
	}

	value, err = k4.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get key2: %v", err)
	}
	if value != nil {
		t.Fatalf("Expected nil, got %s", value)
	}

}

func TestConcurrentTransactions(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	txn1 := k4.BeginTransaction()
	txn2 := k4.BeginTransaction()

	txn1.AddOperation(PUT, []byte("key1"), []byte("value1"))
	txn2.AddOperation(PUT, []byte("key2"), []byte("value2"))

	err = txn1.Commit(k4)
	if err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	err = txn2.Commit(k4)
	if err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	value, err := k4.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if !bytes.Equal(value, []byte("value1")) {
		t.Fatalf("Expected value1, got %s", value)
	}

	value, err = k4.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get key2: %v", err)
	}
	if !bytes.Equal(value, []byte("value2")) {
		t.Fatalf("Expected value2, got %s", value)
	}
}

func TestPutGet2(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, (1024*1024)*512, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	tt := time.Now()

	for i := 0; i < 1000000; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		value := []byte("value" + fmt.Sprintf("%d", i))

		err = k4.Put(key, value, nil)
		if err != nil {
			t.Fatalf("Failed to put key-value: %v", err)
		}
	}

	fmt.Println("Put time: ", time.Since(tt))

	got, err := k4.Get([]byte(fmt.Sprintf("key%d", 999999)))
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if !bytes.Equal([]byte(fmt.Sprintf("value%d", 999999)), got) {
		t.Fatalf("Expected value %s, got %s", fmt.Sprintf("value%d", 999999), got)
	}
}

func TestWALRecovery(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}

	key := []byte("key1")
	value := []byte("value1")

	err = k4.Put(key, value, nil)
	if err != nil {
		k4.Close()
		t.Fatalf("Failed to put key-value: %v", err)
	}

	k4.Close()

	// Closing flushes sstables, lets delete them

	// open directory and delete all files that end with SSTable extension
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}

	for _, file := range files {

		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(file.Name(), ".sst") {
			err = os.Remove(dir + string(os.PathSeparator) + file.Name())

		}

	}

	k4, err = Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to reopen K4: %v", err)
	}

	err = k4.RecoverFromWAL()
	if err != nil {
		k4.Close()
		t.Fatalf("Failed to recover from WAL: %v", err)
	}

	got, err := k4.Get(key)
	if err != nil {
		k4.Close()
		t.Fatalf("Failed to get key: %v", err)
	}

	if !bytes.Equal(got, value) {
		k4.Close()
		t.Fatalf("Expected value %s, got %s", value, got)
	}

	k4.Close()
}

func TestNGet(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	// Insert key-value pairs
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")
	key3 := []byte("key3")
	value3 := []byte("value3")

	err = k4.Put(key1, value1, nil)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	err = k4.Put(key2, value2, nil)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	err = k4.Put(key3, value3, nil)
	if err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}

	// Test NGet
	result, err := k4.NGet(key2)
	if err != nil {
		t.Fatalf("Failed to NGet: %v", err)
	}

	// Check the result
	if len(*result) != 2 {
		t.Fatalf("Expected 2 key-value pairs, got %d", len(*result))
	}

	expected := map[string][]byte{
		"key1": value1,
		"key3": value3,
	}

	for _, kv := range *result {
		if !bytes.Equal(kv.Value, expected[string(kv.Key)]) {
			t.Fatalf("Expected value %s for key %s, got %s", expected[string(kv.Key)], kv.Key, kv.Value)
		}
	}
}

func TestGreaterThan(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	// Insert key-value pairs
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")
	key3 := []byte("key3")
	value3 := []byte("value3")
	key4 := []byte("key4")
	value4 := []byte("value4")

	err = k4.Put(key1, value1, nil)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	err = k4.Put(key2, value2, nil)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	err = k4.Put(key3, value3, nil)
	if err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}
	err = k4.Put(key4, value4, nil)
	if err != nil {
		t.Fatalf("Failed to put key4: %v", err)
	}

	// Test GreaterThan
	result, err := k4.GreaterThan(key2)
	if err != nil {
		t.Fatalf("Failed to GreaterThan: %v", err)
	}

	// Check the result
	if len(*result) != 2 {
		t.Fatalf("Expected 2 key-value pairs, got %d", len(*result))
	}

	expected := map[string][]byte{
		"key3": value3,
		"key4": value4,
	}

	for _, kv := range *result {
		if !bytes.Equal(kv.Value, expected[string(kv.Key)]) {
			t.Fatalf("Expected value %s for key %s, got %s", expected[string(kv.Key)], kv.Key, kv.Value)
		}
	}
}

func TestGreaterThanEq(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	// Insert key-value pairs
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")
	key3 := []byte("key3")
	value3 := []byte("value3")
	key4 := []byte("key4")
	value4 := []byte("value4")

	err = k4.Put(key1, value1, nil)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	err = k4.Put(key2, value2, nil)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	err = k4.Put(key3, value3, nil)
	if err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}
	err = k4.Put(key4, value4, nil)
	if err != nil {
		t.Fatalf("Failed to put key4: %v", err)
	}

	// Test GreaterThanEq
	result, err := k4.GreaterThanEq(key2)
	if err != nil {
		t.Fatalf("Failed to GreaterThanEq: %v", err)
	}

	// Check the result
	if len(*result) != 3 {
		t.Fatalf("Expected 3 key-value pairs, got %d", len(*result))
	}

	expected := map[string][]byte{
		"key2": value2,
		"key3": value3,
		"key4": value4,
	}

	for _, kv := range *result {
		if !bytes.Equal(kv.Value, expected[string(kv.Key)]) {
			t.Fatalf("Expected value %s for key %s, got %s", expected[string(kv.Key)], kv.Key, kv.Value)
		}
	}
}

func TestLessThan(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	// Insert key-value pairs
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")
	key3 := []byte("key3")
	value3 := []byte("value3")
	key4 := []byte("key4")
	value4 := []byte("value4")

	err = k4.Put(key1, value1, nil)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	err = k4.Put(key2, value2, nil)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	err = k4.Put(key3, value3, nil)
	if err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}
	err = k4.Put(key4, value4, nil)
	if err != nil {
		t.Fatalf("Failed to put key4: %v", err)
	}

	// Test LessThan
	result, err := k4.LessThan(key3)
	if err != nil {
		t.Fatalf("Failed to LessThan: %v", err)
	}

	// Check the result
	if len(*result) != 2 {
		t.Fatalf("Expected 2 key-value pairs, got %d", len(*result))
	}

	expected := map[string][]byte{
		"key1": value1,
		"key2": value2,
	}

	for _, kv := range *result {
		if !bytes.Equal(kv.Value, expected[string(kv.Key)]) {
			t.Fatalf("Expected value %s for key %s, got %s", expected[string(kv.Key)], kv.Key, kv.Value)
		}
	}
}

func TestLessThanEq(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	// Insert key-value pairs
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")
	key3 := []byte("key3")
	value3 := []byte("value3")
	key4 := []byte("key4")
	value4 := []byte("value4")

	err = k4.Put(key1, value1, nil)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	err = k4.Put(key2, value2, nil)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	err = k4.Put(key3, value3, nil)
	if err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}
	err = k4.Put(key4, value4, nil)
	if err != nil {
		t.Fatalf("Failed to put key4: %v", err)
	}

	// Test LessThanEq
	result, err := k4.LessThanEq(key3)
	if err != nil {
		t.Fatalf("Failed to LessThanEq: %v", err)
	}

	// Check the result
	if len(*result) != 3 {
		t.Fatalf("Expected 3 key-value pairs, got %d", len(*result))
	}

	expected := map[string][]byte{
		"key1": value1,
		"key2": value2,
		"key3": value3,
	}

	for _, kv := range *result {
		if !bytes.Equal(kv.Value, expected[string(kv.Key)]) {
			t.Fatalf("Expected value %s for key %s, got %s", expected[string(kv.Key)], kv.Key, kv.Value)
		}
	}
}

func TestRange(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	// Insert key-value pairs
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")
	key3 := []byte("key3")
	value3 := []byte("value3")
	key4 := []byte("key4")
	value4 := []byte("value4")

	err = k4.Put(key1, value1, nil)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	err = k4.Put(key2, value2, nil)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	err = k4.Put(key3, value3, nil)
	if err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}
	err = k4.Put(key4, value4, nil)
	if err != nil {
		t.Fatalf("Failed to put key4: %v", err)
	}

	// Test Range
	result, err := k4.Range(key2, key4)
	if err != nil {
		t.Fatalf("Failed to Range: %v", err)
	}

	// Check the result
	if len(*result) != 3 {
		t.Fatalf("Expected 3 key-value pairs, got %d", len(*result))
	}

	expected := map[string][]byte{
		"key2": value2,
		"key3": value3,
		"key4": value4,
	}

	for _, kv := range *result {
		if !bytes.Equal(kv.Value, expected[string(kv.Key)]) {
			t.Fatalf("Expected value %s for key %s, got %s", expected[string(kv.Key)], kv.Key, kv.Value)
		}
	}
}

func TestNRange(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}
	defer k4.Close()

	// Insert key-value pairs
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")
	key3 := []byte("key3")
	value3 := []byte("value3")
	key4 := []byte("key4")
	value4 := []byte("value4")

	err = k4.Put(key1, value1, nil)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	err = k4.Put(key2, value2, nil)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	err = k4.Put(key3, value3, nil)
	if err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}
	err = k4.Put(key4, value4, nil)
	if err != nil {
		t.Fatalf("Failed to put key4: %v", err)
	}

	// Test NRange
	result, err := k4.NRange(key2, key3)
	if err != nil {
		t.Fatalf("Failed to NRange: %v", err)
	}

	// Check the result
	if len(*result) != 2 {
		t.Fatalf("Expected 2 key-value pairs, got %d", len(*result))
	}

	expected := map[string][]byte{
		"key1": value1,
		"key4": value4,
	}

	for _, kv := range *result {
		if !bytes.Equal(kv.Value, expected[string(kv.Key)]) {
			t.Fatalf("Expected value %s for key %s, got %s", expected[string(kv.Key)], kv.Key, kv.Value)
		}
	}
}

func TestReopen(t *testing.T) {
	dir := setup(t)
	defer teardown(dir)

	k4, err := Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to open K4: %v", err)
	}

	key := []byte("key1")
	value := []byte("value1")

	err = k4.Put(key, value, nil)
	if err != nil {
		k4.Close()
		t.Fatalf("Failed to put key-value: %v", err)
	}

	k4.Close()

	k4, err = Open(dir, 1024, 60, false)
	if err != nil {
		t.Fatalf("Failed to reopen K4: %v", err)
	}
	defer k4.Close()

	got, err := k4.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if !bytes.Equal(got, value) {
		t.Fatalf("Expected value %s, got %s", value, got)
	}
}
