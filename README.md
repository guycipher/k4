# k4
High performance transactional, durable embedded key value store. Designed to deliver low-latency, optimized read and write performance.

### Benchmarks
```
goos: linux
goarch: amd64
pkg: github.com/guycipher/k4
cpu: 11th Gen Intel(R) Core(TM) i7-11700K @ 3.60GHz
BenchmarkK4_Put
BenchmarkK4_Put-16    	  131302	      8762 ns/op
```

### Features
- Variable length binary keys and values
- Write-Ahead Logging (WAL)
- Atomic transactions
- Paired Compaction
- Memtable implemented as a skip list
- Disk-based storage
- Configurable memtable flush threshold
- Configurable compaction interval (in seconds)
- Configurable logging
- Bloom filter for faster lookups
- Recovery from WAL
- Thread-safe
- Memtable TTL support
- No dependencies


### Example usage
```go
func main() {
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

    // Get
    value, err := db.Get(key)

    // Delete
    err = db.Delete(key)
}
```

### Transactions
```go
txn := db.BeginTransaction()

txn.AddOperation(k4.PUT, key, value)
txn.AddOperation(k4.DELETE, key, nil)

err = txn.Commit()

// Once committed you can rollback before the transaction is cleared
txn.Rollback()

// Clear the transaction
txn.Clear() // txn now no longer usable nor existent

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
```go
key := []byte("key")
value := []byte("value")
ttl :=  6 * time.Second

err = db.Put(key, value, ttl)
if err != nil {
    ..
..

```


