#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libk4.h>

int main() {
    // Open database
    void* db = db_open("testdb", 1024, 60, 1, 1);
    if (db == NULL) {
        printf("Failed to open database\n");
        return 1;
    }

//    // Put key-value pair
//    char* key = "key1";
//    char* value = "value1";
//    if (db_put(db, key, strlen(key), value, strlen(value), -1) != 0) {
//        printf("Failed to put key-value pair\n");
//        db_close(db);
//        return 1;
//    }
//
//    // Get value by key
//    char* retrieved_value = db_get(db, key, strlen(key));
//    if (retrieved_value == NULL) {
//        printf("Failed to get value\n");
//        db_close(db);
//        return 1;
//    }
//    printf("Retrieved value: %s\n", retrieved_value);
//    free(retrieved_value);
//
//    // Delete key-value pair
//    if (db_delete(db, key, strlen(key)) != 0) {
//        printf("Failed to delete key-value pair\n");
//        db_close(db);
//        return 1;
//    }
//
//    // Begin transaction
//    void* txn = begin_transaction(db);
//    if (txn == NULL) {
//        printf("Failed to begin transaction\n");
//        db_close(db);
//        return 1;
//    }
//
//    // Add operation to transaction
//    char* key2 = "key2";
//    char* value2 = "value2";
//    if (add_operation(txn, 1, key2, strlen(key2), value2, strlen(value2)) != 0) {
//        printf("Failed to add operation to transaction\n");
//        rollback_transaction(txn, db);
//        db_close(db);
//        return 1;
//    }
//
//    // Commit transaction
//    if (commit_transaction(txn, db) != 0) {
//        printf("Failed to commit transaction\n");
//        rollback_transaction(txn, db);
//        db_close(db);
//        return 1;
//    }
//
//    // Range query
//    struct range__return range_result = range_(db, "key0", strlen("key0"), "key3", strlen("key3"));
//    for (int i = 0; i < range_result.r0.len; i++) {
//        printf("Range key: %s, value: %s\n", ((char**)range_result.r0.data)[i], ((char**)range_result.r1.data)[i]);
//    }
//
//    // Iterator
//    void* iter = new_iterator(db);
//    struct iter_next_return iter_result;
//    while ((iter_result = iter_next(iter)).r0 != NULL) {
//        printf("Iterator key: %s, value: %s\n", iter_result.r0, iter_result.r1);
//        free(iter_result.r0);
//        free(iter_result.r1);
//    }
//    iter_reset(iter);

    // Close database
    if (db_close(db) != 0) {
        printf("Failed to close database\n");
        return 1;
    }

    return 0;
}