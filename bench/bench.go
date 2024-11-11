package main

import (
	"fmt"
	"github.com/guycipher/k4/v2"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	DB_PATH     = "testdb"
	NUM_OPS     = 10000
	NUM_THREADS = 4
)

func benchmarkK4(thread int) {

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
	fmt.Printf("K4 Put(%d): %f seconds\n", thread, cpuTimeUsed)

	// Benchmark Get
	start = time.Now()
	for i := 0; i < NUM_OPS; i++ {
		key = []byte(fmt.Sprintf("key%d", i))
		if _, err := db.Get(key); err != nil {
			log.Fatalf("Error getting key: %v", err)
		}
	}
	cpuTimeUsed = time.Since(start).Seconds()
	fmt.Printf("K4 Get(%d): %f seconds\n", thread, cpuTimeUsed)

	// Benchmark Delete
	start = time.Now()
	for i := 0; i < NUM_OPS; i++ {
		key = []byte(fmt.Sprintf("key%d", i))
		if err := db.Delete(key); err != nil {
			log.Fatalf("Error deleting key: %v", err)
		}
	}
	cpuTimeUsed = time.Since(start).Seconds()
	fmt.Printf("K4 Delete (%d): %f seconds\n", thread, cpuTimeUsed)

	os.RemoveAll(DB_PATH)
}

func benchmarkK4Random() {
	db, err := k4.Open(DB_PATH, (1024*1024)*256, 3600, false, false)
	if err != nil {
		log.Fatalf("Error opening K4 database: %v", err)
	}
	defer db.Close()

	key := make([]byte, 20)
	value := make([]byte, 20)

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Benchmark Put
	start := time.Now()
	for i := 0; i < NUM_OPS; i++ {
		key = []byte(fmt.Sprintf("key%d", rand.Intn(NUM_OPS)))
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
		key = []byte(fmt.Sprintf("key%d", rand.Intn(NUM_OPS)))
		if _, err := db.Get(key); err != nil {
			log.Fatalf("Error getting key: %v", err)
		}
	}
	cpuTimeUsed = time.Since(start).Seconds()
	fmt.Printf("K4 Get: %f seconds\n", cpuTimeUsed)

	// Benchmark Delete
	start = time.Now()
	for i := 0; i < NUM_OPS; i++ {
		key = []byte(fmt.Sprintf("key%d", rand.Intn(NUM_OPS)))
		if err := db.Delete(key); err != nil {
			log.Fatalf("Error deleting key: %v", err)
		}
	}
	cpuTimeUsed = time.Since(start).Seconds()
	fmt.Printf("K4 Delete: %f seconds\n", cpuTimeUsed)

	os.RemoveAll(DB_PATH)
}

func benchmarkK4Concurrent() {
	var wg sync.WaitGroup
	wg.Add(NUM_THREADS)

	for i := 0; i < NUM_THREADS; i++ {
		go func() {
			defer wg.Done()

			benchmarkK4(i)
		}()
	}

	wg.Wait()
}

func main() {

	fmt.Println("Benchmarker started with the set parameters:")
	fmt.Printf("Number of operations: %d\n", NUM_OPS)
	fmt.Printf("Number of threads: %d\n", NUM_THREADS)

	fmt.Println()

	fmt.Println("Benchmarking K4 non concurrent operations")
	benchmarkK4(-1)
	fmt.Println()
	fmt.Println("Benchmarking K4 random operations")
	benchmarkK4Random()
	fmt.Println()
	fmt.Println("Benchmarking K4 concurrent operations")
	benchmarkK4Concurrent()
}
