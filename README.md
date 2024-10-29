# k4
High performance transactional, durable embedded storage engine. Designed to deliver low-latency, optimized read and write performance.

### Benchmarks
```
goos: linux
goarch: amd64
pkg: github.com/guycipher/k4
cpu: 11th Gen Intel(R) Core(TM) i7-11700K @ 3.60GHz
BenchmarkK4_Put
BenchmarkK4_Put-16    	  131302	      8762 ns/op

RocksDB vs K4
+=+=+=+=+=+=+=+=
**RocksDB v7.8.3** 1 million writes sequential key-value pairs default settings = 2.9s-3.1s
**K4      v1.0.0**      1 million writes sequential key-value pairs default settings = 1.7s-1.9s
```

### Features
- High speed writes and reads
- Durability
- Variable length binary keys and values
- Write-Ahead Logging (WAL)
- Atomic transactions
- Paired compaction
- Memtable implemented as a skip list
- Disk-based storage
- Configurable memtable flush threshold
- Configurable compaction interval (in seconds)
- Configurable logging
- Bloom filter for faster lookups
- Recovery from WAL
- Granular page locking
- Thread-safe
- TTL support
- No dependencies


### Example usage
Importing
```go
import("github.com/guycipher/k4")
```

```go
func main() {
    var err error
    directory := "./data"
    memtableFlushThreshold := 1024 * 1024 // 1MB
    compactionInterval := 3600 // 1 hour
    logging := true

    db, err := k4.Open(directory, memtableFlushThreshold, compactionInterval, logging)
    if err != nil {
        log.Fatalf("Failed to open K4: %v", err)
    }

    defer db.Close()


    // Put
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

### Transactions
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
If you have a populated WAL file in the data directory but no data you can use `RecoverFromWAL()` which will replay the WAL file and populate the LSM tree.

#### Example
```go
func main() {
    directory := "./data"
    memtableFlushThreshold := 1024 * 1024 // 1MB
    compactionInterval := 3600 // 1 hour
    logging := true

    db, err := k4.Open(directory, memtableFlushThreshold, compactionInterval, logging)
    if err != nil {
        log.Fatalf("Failed to open LSMTree: %v", err)
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
The system will mark the key with a tombstone and delete it during compaction.
```go
key := []byte("key")
value := []byte("value")
ttl :=  6 * time.Second

err = db.Put(key, value, ttl)
if err != nil {
    ..
..

```

### API
```go

// Open a new K4 instance
// you can pass extra arguments to configure memtable such as
// args[0] = max level, must be an int
// args[1] = probability, must be a float64
func Open(directory string, memtableFlushThreshold int, compactionInterval int, logging bool, args ...interface{}) (*K4, error)

// Close the K4 instance gracefully
func (k4 *K4) Close() error

// Put a key-value pair into the db
func (k4 *K4) Put(key, value []byte, ttl *time.Duration) error

// Get a value from the db
func (k4 *K4) Get(key []byte) ([]byte, error)

// Get all key-value pairs not equal to the key
func (k4 *K4) NGet(key []byte) (map[string][]byte, error)

// Get all key-value pairs greater than the key
func (k4 *K4) GreaterThan(key []byte) (map[string][]byte, error)

// Get all key-value pairs greater than or equal to the key
func (k4 *K4) GreaterThanEq(key []byte) (map[string][]byte, error)

// Get all key-value pairs less than the key
func (k4 *K4) LessThan(key []byte) (map[string][]byte, error)

// Get all key-value pairs less than or equal to the key
func (k4 *K4) LessThanEq(key []byte) (map[string][]byte, error)

// Get all key-value pairs in the range of startKey and endKey
func (k4 *K4) Range(startKey, endKey []byte) (map[string][]byte, error)

// Get all key-value pairs not in the range of startKey and endKey
func (k4 *K4) NRange(startKey, endKey []byte) (map[string][]byte, error)

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



