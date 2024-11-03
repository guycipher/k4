#include <stdio.h>
#include <stdlib.h>
#include "k4.h"

int main() {
    // Open the K4 database
    K4* db = k4_open("data", 1024, 60, 1, 1);
    if (db == NULL) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }

    // Put a key-value pair into the database
    if (k4_put(db, "key1", "value1", 3600) != 0) {
        fprintf(stderr, "Failed to put key-value pair\n");
        k4_close(db);
        return 1;
    }

    // Get the value for a key from the database
    char* value = k4_get(db, "key1");
    if (value == NULL) {
        fprintf(stderr, "Failed to get value for key\n");
        k4_close(db);
        return 1;
    }
    printf("Got value: %s\n", value);
    free(value);

    // Delete a key-value pair from the database
    if (k4_delete(db, "key1") != 0) {
        fprintf(stderr, "Failed to delete key-value pair\n");
        k4_close(db);
        return 1;
    }

    // Begin a transaction
    Transaction* txn = k4_begin_transaction(db);
    if (txn == NULL) {
        fprintf(stderr, "Failed to begin transaction\n");
        k4_close(db);
        return 1;
    }

    // Add operations to the transaction
    k4_add_operation(txn, PUT, "key2", "value2");
    k4_add_operation(txn, DELETE, "key2", NULL);

    // Commit the transaction
    if (k4_commit_transaction(txn, db) != 0) {
        fprintf(stderr, "Failed to commit transaction\n");
        k4_rollback_transaction(txn, db);
        k4_close(db);
        return 1;
    }

    // Remove the transaction
    k4_remove_transaction(txn, db);

    // Close the K4 database
    k4_close(db);

    return 0;
}