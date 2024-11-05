<div>
    <h1 align="left"><img width="88" src="graphics/k4-v2.png"></h1>
</div>

K4 is an open-source, high-performance, transactional, and durable storage engine based on an LSM (Log-Structured Merge) tree architecture. This design optimizes high-speed writes, making it ideal for modern data-intensive applications.

### Benchmarks
```
goos: linux
goarch: amd64
pkg: github.com/guycipher/k4
cpu: 11th Gen Intel(R) Core(TM) i7-11700K @ 3.60GHz
BenchmarkK4_Put
BenchmarkK4_Put-16    	  158104	      6862 ns/op # 145,000 ops/s

RocksDB vs K4
+=+=+=+=+=+=+=+
Both engines were used with default settings and similar configurations.
**RocksDB v7.8.3**      1 million writes sequential key-value pairs default settings = 2.9s-3.1s
**K4      v1.9.0**      1 million writes sequential key-value pairs default settings = 174.67ms-190.00ms
```

More benchmarks coming comparing against other storage engines.

### Features
- High speed writes.  Reads are also fast but writes are the primary focus.
- Durability
- Optimized for RAM and flash storage (SSD)
- Variable length binary keys and values.  Keys and their values can be any length
- Write-Ahead Logging (WAL).  System writes PUT and DELETE operations to a log file before applying them to K4.
- Atomic transactions.  Multiple PUT and DELETE operations can be grouped together and applied atomically to K4.
- Multi-threaded parallel paired compaction.  SSTables are paired up during compaction and merged into a single SSTable(s).  This reduces the number of SSTables and minimizes disk I/O for read operations.
- Memtable implemented as a skip list.
- Configurable memtable flush threshold
- Configurable compaction interval (in seconds)
- Configurable logging
- Configurable skip list (max level and probability)
- Optimized hashset for faster lookups.  SSTable initial pages contain a hashset.  The system uses the hashset to determine if a key is in the SSTable before scanning the SSTable.
- Recovery from WAL
- Granular page locking (sstables on scan are locked granularly)
- Thread-safe (multiple readers, single writer)
- TTL support (time to live).  Keys can be set to expire after a certain time duration.
- Murmur3 inspired hashing on compression and hash set
- Optional compression support (Simple lightweight and optimized Lempel-Ziv 1977 inspired compression algorithm)
- Background flushing and compaction operations for less blocking on read and write operations
- Easy intuitive API(Get, Put, Delete, Range, NRange, GreaterThan, GreaterThanEq, LessThan, LessThanEq, NGet)
- Iterator for iterating over key-value pairs in memtable and sstables with Next and Prev methods
- No dependencies

### C Library and FFIs
K4 has a C library that can be used with FFIs (Foreign Function Interfaces) in other languages.  The C library is built using the Go toolchain and can be found in the c directory.

#### FFIs
- [ ] [Python]()
- [ ] [Ruby]()
- [ ] [Lua]()
- [ ] [Java]()
- [ ] [Rust]()
- [ ] [Haskell]()
- [ ] [Perl]()
- [ ] [C#]()
- [ ] [Scala]()
- [ ] [D]()
- [ ] [Swift]()
- [ ] [Kotlin]()
- [ ] [Elixir]()
- [ ] [OCaml]()
- [ ] [PHP]()
- [ ] [F#]()
- [ ] [Ada]()
- [ ] [R]()
- [x] [Node.JS]()


### Example usage
This is GO code that demonstrates how to use K4.  The code is simple and demonstrates PUT, GET, and DELETE operations.

```go
import (
    "github.com/guycipher/k4"
    "log"
)

func main() {
    var err error
    directory := "./data"
    memtableFlushThreshold := 1024 * 1024 // 1MB
    compactionInterval := 3600 // 1 hour
    logging := true
    compression := false

    db, err := k4.Open(directory, memtableFlushThreshold, compactionInterval, logging, compression)
    if err != nil {
        log.Fatalf("Failed to open K4: %v", err)
    }

    defer db.Close()


    // Put
    // Putting the same key will update the value
    key := []byte("key")
    value := []byte("value")
    err = db.Put(key, value, nil)
    if err != nil {
        log.Fatalf("Failed to put key: %v", err)
    }

    // Get
    value, err = db.Get(key)
    if err != nil {
        log.Fatalf("Failed to get key: %v", err)
    }

    // Delete
    err = db.Delete(key)
    if err != nil {
        log.Fatalf("Failed to get key: %v", err)
    }
}
```

### Iteration
To iterate over key-value pairs you can use an Iterator.
Will iterate over key-value pairs in memtable then sstables.

```go

it := NewIterator(db)

for  {
    key, value := it.Next()
    if key == nil {
        break
    }

    // .. You can also go back if you want
    key, value = it.Prev()
    if key == nil {
        break
    }
}

```

### Transactions
Transactions are atomic and can be used to group multiple PUT and DELETE operations together.  Transactions are committed atomically to K4.
Transactions can be rolled back after their commited but before they are removed.
Commits are first come first serve and are applied to K4 in the order they were committed.

```go
txn := db.BeginTransaction()

txn.AddOperation(k4.PUT, key, value)
txn.AddOperation(k4.DELETE, key, nil)

err = txn.Commit()

// Once committed you can rollback before the transaction is removed
// On commit error the transaction is automatically rolled back
txn.Rollback()

// Remove the transaction after commit or rollback
txn.Remove() // txn now no longer usable nor existent

```

### Recovery
If you have a populated WAL file in the data directory but no data files aka sstables you can use `RecoverFromWAL()` which will replay the WAL file and populate K4.

#### Example
```go
func main() {
    directory := "./data"
    memtableFlushThreshold := 1024 * 1024 // 1MB
    compactionInterval := 3600 // 1 hour
    logging := true
    compression := false

    db, err := k4.Open(directory, memtableFlushThreshold, compactionInterval, logging, compression)
    if err != nil {
        log.Fatalf("Failed to open K4: %v", err)
    }

    defer db.Close()

    err := db.RecoverFromWAL()
    if err != nil {
        ..
    }

    // Continue as normal
}
```

### TTL
TTL (time to live) when putting a key-value pair you can specify a time duration after which the key-value pair will be deleted.
The system will mark the key with a tombstone and delete it during compaction and or flush operations.
```go
key := []byte("key")
value := []byte("value")
ttl :=  6 * time.Second

err = db.Put(key, value, ttl)
if err != nil {
    ..
..

```

### Configuration recommendations

It is advisable to configure the memtable flush threshold and compaction interval in accordance with your application's specific requirements. A minimum memtable flush size of 50-256 MB is suggested.

Regarding compaction, a compaction interval of 1-6 hours is recommended, contingent upon storage usage and activity patterns.

### Compression
Compression is optional and can be enabled or disabled when opening the K4 instance.
Memtable keys and their values are not compressed.  What is compressed is WAL entries and SSTable pages.
Compression could save disk space and reduce disk I/O but it could also increase CPU usage and slow down read and write operations.


### API
```go

// Open a new K4 instance
// you can pass extra arguments to configure memtable such as
// args[0] = max level, must be an int
// args[1] = probability, must be a float64
func Open(directory string, memtableFlushThreshold int, compactionInterval int, logging, compress bool, args ...interface{}) (*K4, error)

// Close the K4 instance gracefully
func (k4 *K4) Close() error

// Put a key-value pair into the db
func (k4 *K4) Put(key, value []byte, ttl *time.Duration) error

// Get a value from the db
func (k4 *K4) Get(key []byte) ([]byte, error)

// Get all key-value pairs not equal to the key
func (k4 *K4) NGet(key []byte) (*KeyValueArray, error)

// Get all key-value pairs greater than the key
func (k4 *K4) GreaterThan(key []byte) (*KeyValueArray, error)

// Get all key-value pairs greater than or equal to the key
func (k4 *K4) GreaterThanEq(key []byte) (*KeyValueArray, error)

// Get all key-value pairs less than the key
func (k4 *K4) LessThan(key []byte) (*KeyValueArray, error)

// Get all key-value pairs less than or equal to the key
func (k4 *K4) LessThanEq(key []byte) (*KeyValueArray, error)

// Get all key-value pairs in the range of startKey and endKey
func (k4 *K4) Range(startKey, endKey []byte) (*KeyValueArray, error)

// Get all key-value pairs not in the range of startKey and endKey
func (k4 *K4) NRange(startKey, endKey []byte) (*KeyValueArray, error)

// Delete a key-value pair from the db
func (k4 *K4) Delete(key []byte) error

// Begin a transaction
func (k4 *K4) BeginTransaction() *Transaction

// Add a key-value pair to the transaction OPR_CODE can be PUT or DELETE
func (txn *Transaction) AddOperation(op OPR_CODE, key, value []byte)

// Remove a transaction after it has been committed
func (txn *Transaction) Remove(k4 *K4)

// Rollback a transaction (only after commit or error)
func (txn *Transaction) Rollback(k4 *K4) error

// Commit a transaction
func (txn *Transaction) Commit(k4 *K4) error

// Recover from WAL file
// WAL file must be placed in the data directory
func (k4 *K4) RecoverFromWAL() error
```

### Reporting bugs
If you find a bug with K4 create an issue on this repository but please do not include any sensitive information in the issue.  If you have a security concern please follow SECURITY.md.

### Contributing
This repository for the K4 project welcomes contributions.  Before submitting a pull request (PR), please ensure you have reviewed and adhered to our guidelines outlined in SECURITY.md and CODE_OF_CONDUCT.md.
Following these documents is essential for maintaining a safe and respectful environment for all contributors. We encourage you to create well-structured PRs that address specific issues or enhancements, and to include relevant details in your description. Your contributions help improve K4, and we appreciate your commitment to fostering a collaborative and secure development process. Thank you for being part of the K4 project.

### License
BSD 3-Clause License (https://opensource.org/licenses/BSD-3-Clause)