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

#### Database Operations
```c
int db_open(char* directory, int memtableFlushThreshold, int compactionInterval, int logging, int compress);
int db_close();
int db_put(char* key, char* value, int64_t ttl);
char* db_get(char* key);
int db_delete(char* key);
```

#### Transaction Management
```c
int begin_transaction();
int add_operation(int op, char* key, char* value);
int remove_transaction();
int rollback_transaction();
int commit_transaction();
int recover_from_wal();
```

#### Query Operations
```c
struct greater_than_return greater_than(char* key);
struct less_than_return less_than(char* key);
struct nget_return nget(char* key);
struct greater_than_eq_return greater_than_eq(char* key);
struct less_than_eq_return less_than_eq(char* key);
struct range__return range_(char* start, char* end);
struct nrange_return nrange(char* start, char* end);
```

#### Iterator Functions
```c
int new_iterator();
struct iter_next_return iter_next();
struct iter_prev_return iter_prev();
void iter_reset();
```