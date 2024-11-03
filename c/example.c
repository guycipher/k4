#include <stdio.h>
#include <stdlib.h>
#include <libk4.h>

int main() {
    // Open a database
    if (db_open("data", 1024, 60, 1, 0) != 0) {
        printf("Failed to open database\n");
        return -1;
    }

    // Put a key-value pair
    if (db_put("key1", "value1", 1000000000 * 3000) != 0) { // 1000000000 * 3000 = 3000 seconds or -1 for no expiration
        printf("Failed to put key-value pair\n");
        db_close();
        return -1;
    }

    // Get the value for the key
    char* value = db_get("key1");
    if (value == NULL) {
        printf("Failed to get value for key\n");
        db_close();
        return -1;
    }
    printf("Got value: %s\n", value);
    free(value);

    // Delete the key-value pair
    if (db_delete("key1") != 0) {
        printf("Failed to delete key-value pair\n");
        db_close();
        return -1;
    }

    // Begin a transaction
    if (begin_transaction() != 0) {
        printf("Failed to begin transaction\n");
        db_close();
        return -1;
    }

    // Add an operation to the transaction
    if (add_operation(0, "key2", "value2") != 0) {
        printf("Failed to add operation to transaction\n");
        rollback_transaction();
        db_close();
        return -1;
    }

    // Commit the transaction
    if (commit_transaction() != 0) {
        printf("Failed to commit transaction\n");
        db_close();
        return -1;
    }

    // Close the database
    if (db_close() != 0) {
        printf("Failed to close database\n");
        return -1;
    }

    return 0;
}