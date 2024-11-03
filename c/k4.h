// k4.h - C Library for K4
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
#ifndef K4_H
#define K4_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Forward declaration of the K4 structure
typedef struct K4 K4;

// Forward declaration of the Transaction structure
typedef struct Transaction Transaction;

enum Operation {
    PUT,
    DELETE
};

/**
 * \brief Opens a new K4 database instance.
 *
 * \param directory The directory where the database files are stored.
 * \param memtableFlushThreshold The threshold for flushing the memtable to disk.
 * \param compactionInterval The interval for running compaction.
 * \param logging Enable or disable logging (1 to enable, 0 to disable).
 * \param compress Enable or disable compression (1 to enable, 0 to disable).
 * \return A pointer to the K4 database instance.
 */
K4* k4_open(const char* directory, int memtableFlushThreshold, int compactionInterval, int logging, int compress);

/**
 * \brief Closes the K4 database instance.
 *
 * \param db A pointer to the K4 database instance.
 */
void k4_close(K4* db);

/**
 * \brief Puts a key-value pair into the database.
 *
 * \param db A pointer to the K4 database instance.
 * \param key The key to be inserted.
 * \param value The value to be associated with the key.
 * \param ttl The time-to-live for the key-value pair.
 * \return 0 on success, non-zero on failure.
 */
int k4_put(K4* db, const char* key, const char* value, int64_t ttl);

/**
 * \brief Gets a value from the database.
 *
 * \param db A pointer to the K4 database instance.
 * \param key The key whose value is to be retrieved.
 * \return The value associated with the key, or NULL if the key does not exist.
 */
char* k4_get(K4* db, const char* key);

/**
 * \brief Deletes a key-value pair from the database.
 *
 * \param db A pointer to the K4 database instance.
 * \param key The key to be deleted.
 * \return 0 on success, non-zero on failure.
 */
int k4_delete(K4* db, const char* key);

/**
 * \brief Begins a new transaction.
 *
 * \param db A pointer to the K4 database instance.
 * \return A pointer to the new Transaction instance.
 */
Transaction* k4_begin_transaction(K4* db);

/**
 * \brief Adds an operation to the transaction.
 *
 * \param txn A pointer to the Transaction instance.
 * \param op The operation code (e.g., PUT or DELETE).
 * \param key The key to be operated on.
 * \param value The value to be associated with the key (for PUT operations).
 */
void k4_add_operation(Transaction* txn, int op, const char* key, const char* value);

/**
 * \brief Removes a transaction after it has been committed.
 *
 * \param txn A pointer to the Transaction instance.
 * \param db A pointer to the K4 database instance.
 */
void k4_remove_transaction(Transaction* txn, K4* db);

/**
 * \brief Rolls back a transaction.
 *
 * \param txn A pointer to the Transaction instance.
 * \param db A pointer to the K4 database instance.
 * \return 0 on success, non-zero on failure.
 */
int k4_rollback_transaction(Transaction* txn, K4* db);

/**
 * \brief Commits a transaction.
 *
 * \param txn A pointer to the Transaction instance.
 * \param db A pointer to the K4 database instance.
 * \return 0 on success, non-zero on failure.
 */
int k4_commit_transaction(Transaction* txn, K4* db);

/**
 * \brief Recovers the database from the Write-Ahead Log (WAL) file.
 *
 * \param db A pointer to the K4 database instance.
 * \return 0 on success, non-zero on failure.
 */
int k4_recover_from_wal(K4* db);

#ifdef __cplusplus
}
#endif

#endif // K4_H