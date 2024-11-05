/*
* Example using K4 shared storage engine library
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libk4.h>

int main() {
    // Open database
    void* db = db_open("data", 1024, 60, 1, 1);
    if (db == NULL) {
        printf("Failed to open database\n");
        return 1;
    }

    // Put key-value pair
    char* key = "key1";
    char* value = "value1";
    if (db_put(db, key, strlen(key), value, strlen(value), -1) != 0) {
        printf("Failed to put key-value pair\n");
        db_close(db);
        return 1;
    }

    // Get value by key
    char* retrieved_value = db_get(db, key, strlen(key));
    if (retrieved_value == NULL) {
        printf("Failed to get value\n");
        db_close(db);
        return 1;
    }
    printf("Retrieved value: %s\n", retrieved_value);
    free(retrieved_value);

    // Delete key-value pair
    if (db_delete(db, key, strlen(key)) != 0) {
        printf("Failed to delete key-value pair\n");
        db_close(db);
        return 1;
    }

    // Begin transaction
    void* txn = begin_transaction(db);
    if (txn == NULL) {
        printf("Failed to begin transaction\n");
        db_close(db);
        return 1;
    }

    // Add operation to transaction
    char* key2 = "key2";
    char* value2 = "value2";
    if (add_operation(txn, 0, key2, strlen(key2), value2, strlen(value2)) != 0) {
        printf("Failed to add operation to transaction\n");
        rollback_transaction(txn, db);
        db_close(db);
        return 1;
    }

    printf("commiting txn\n");

    // Commit transaction
    if (commit_transaction(txn, db) != 0) {
        printf("Failed to commit transaction\n");
        rollback_transaction(txn, db);
        db_close(db);
        return 1;
    }

    printf("txn committed\n");

    // Define start and end keys
    char* startKey = "key1";
    int startLen = strlen(startKey);
    char* endKey = "key3";
    int endLen = strlen(endKey);

    // Call range_ function
    struct KeyValuePairArray result = range_(db, startKey, startLen, endKey, endLen);
    if (result.pairs == NULL) {
        printf("Failed to get range\n");
        db_close(db);
        return 1;
    }

    // Process the result
    for (int i = 0; i < result.numPairs; i++) {
        printf("Key: %s, Value: %s\n", result.pairs[i].key, result.pairs[i].value);
        free(result.pairs[i].key);
        free(result.pairs[i].value);
    }

    // Free the allocated memory for the result array
    free(result.pairs);

    // Close database
    if (db_close(db) != 0) {
        printf("Failed to close database\n");
        return 1;
    }

    return 0;
}