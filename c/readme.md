## C library for K4

Building C library
```
go build -o libk4.so -buildmode=c-shared k4.go
```

Once run you should get
```
libk4.so
libk4.h
```

On unix copy the files to /usr/local/lib and /usr/local/include
```
sudo cp libk4.so /usr/local/lib/
sudo cp libk4.h /usr/local/include/
```

On windows copy the files to C:\Program Files\K4
```
copy libk4.so C:\Program Files\K4
copy libk4.h C:\Program Files\K4
```

On unix update the library cache
```
sudo ldconfig
```

Verify install
```
ldconfig -p | grep libk4
```

Should see
```
libk4.so (libc6,x86-64) => /usr/local/lib/libk4.so
```

Now you should be able to compile example
```
cc -o example example.c -L/usr/local/lib -lk4 -I/usr/local/include
```


### API
```c
void* db_open(char* directory, int memtableFlushThreshold, int compactionInterval, int logging, int compress);
int db_close(void* dbPtr);
int db_put(void* dbPtr, char* key, int keyLen, char* value, int valueLen, int64_t ttl);
char* db_get(void* dbPtr, char* key, int keyLen);
int db_delete(void* dbPtr, char* key, int keyLen);
void* begin_transaction(void* dbPtr);
int add_operation(void* txPtr, int operation, char* key, int keyLen, char* value, int valueLen);
void remove_transaction(void* dbPtr, void* txPtr);
int commit_transaction(void* txPtr, void* dbPtr);
int rollback_transaction(void* txPtr, void* dbPtr);
int recover_from_wal(void* dbPtr);
struct KeyValuePairArray range_(void* dbPtr, char* start, int startLen, char* end, int endLen);
struct KeyValuePairArray nrange(void* dbPtr, char* start, int startLen, char* end, int endLen);
struct KeyValuePairArray greater_than(void* dbPtr, char* key, int keyLen);
struct KeyValuePairArray less_than(void* dbPtr, char* key, int keyLen);
struct KeyValuePairArray nget(void* dbPtr, char* key, int keyLen);
struct KeyValuePairArray greater_than_eq(void* dbPtr, char* key, int keyLen);
struct KeyValuePairArray less_than_eq(void* dbPtr, char* key, int keyLen);
void* new_iterator(void* dbPtr);

/* Return type for iter_next */
struct iter_next_return {
    char* r0;
    char* r1;
};

struct iter_next_return iter_next(void* iterPtr);

/* Return type for iter_prev */
struct iter_prev_return {
    char* r0;
    char* r1;
};

struct iter_prev_return iter_prev(void* iterPtr);
void iter_reset(void* iterPtr);
void iter_close(void* iterPtr);

```

#### Iterator example
```c
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

    // Create a new iterator
    void* iter = new_iterator(db);
    if (iter == NULL) {
        printf("Failed to create iterator\n");
        db_close(db);
        return 1;
    }

    // Iterate forward through the database
    struct iter_next_return next;

    while ((next = iter_next(iter)).r0 != NULL) {
        printf("Key: %s, Value: %s\n", next.r0, next.r1);
        free(next.r0);
        free(next.r1);
    }

    // Reset the iterator
    iter_reset(iter);

    // Iterate backward through the database
    struct iter_prev_return prev;

    while ((prev = iter_prev(iter)).r0 != NULL) {
        printf("Key: %s, Value: %s\n", prev.r0, prev.r1);
        free(prev.r0);
        free(prev.r1);
    }

    // Close the iterator
    iter_close(iter);

    // Close database
    if (db_close(db) != 0) {
        printf("Failed to close database\n");
        return 1;
    }

    return 0;
}
```