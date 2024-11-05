package main

import (
	"fmt"
	"github.com/guycipher/k4"
	"log"
	"os"
	"time"
)

const (
	DB_PATH = "testdb"
	NUM_OPS = 10000
)

func benchmarkK4() {
	db, err := k4.Open(DB_PATH, (1024*1024)*256, 3600, false, false)
	if err != nil {
		log.Fatalf("Error opening K4 database: %v", err)
	}
	defer db.Close()

	key := make([]byte, 20)
	value := make([]byte, 20)

	// Benchmark Put
	start := time.Now()
	for i := 0; i < NUM_OPS; i++ {
		key = []byte(fmt.Sprintf("key%d", i))
		value = []byte(fmt.Sprintf("value%d", i))
		if err := db.Put(key, value, nil); err != nil {
			log.Fatalf("Error putting key: %v", err)
		}
	}
	cpuTimeUsed := time.Since(start).Seconds()
	fmt.Printf("K4 Put: %f seconds\n", cpuTimeUsed)

	// Benchmark Get
	start = time.Now()
	for i := 0; i < NUM_OPS; i++ {
		key = []byte(fmt.Sprintf("key%d", i))
		if _, err := db.Get(key); err != nil {
			log.Fatalf("Error getting key: %v", err)
		}
	}
	cpuTimeUsed = time.Since(start).Seconds()
	fmt.Printf("K4 Get: %f seconds\n", cpuTimeUsed)

	// Benchmark Delete
	start = time.Now()
	for i := 0; i < NUM_OPS; i++ {
		key = []byte(fmt.Sprintf("key%d", i))
		if err := db.Delete(key); err != nil {
			log.Fatalf("Error deleting key: %v", err)
		}
	}
	cpuTimeUsed = time.Since(start).Seconds()
	fmt.Printf("K4 Delete: %f seconds\n", cpuTimeUsed)

	os.RemoveAll(DB_PATH)
}

func main() {
	benchmarkK4()
}