package main

import (
	"flag"
	"fmt"
	"github.com/guycipher/k4/v2"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	DB_PATH = "testdb"
)

func benchmarkK4(thread int, numOps int) {

	db, err := k4.Open(DB_PATH, (1024*1024)*256, 3600, false, false)
	if err != nil {
		log.Fatalf("Error opening K4 database: %v", err)
	}
	defer db.Close()

	key := make([]byte, 20)
	value := make([]byte, 20)

	// Benchmark Put
	start := time.Now()
	for i := 0; i < numOps; i++ {
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
	for i := 0; i < numOps; i++ {
		key = []byte(fmt.Sprintf("key%d", i))
		if _, err := db.Get(key); err != nil {
			log.Fatalf("Error getting key: %v", err)
		}
	}
	cpuTimeUsed = time.Since(start).Seconds()
	fmt.Printf("K4 Get(%d): %f seconds\n", thread, cpuTimeUsed)

	// Benchmark Delete
	start = time.Now()
	for i := 0; i < numOps; i++ {
		key = []byte(fmt.Sprintf("key%d", i))
		if err := db.Delete(key); err != nil {
			log.Fatalf("Error deleting key: %v", err)
		}
	}
	cpuTimeUsed = time.Since(start).Seconds()
	fmt.Printf("K4 Delete (%d): %f seconds\n", thread, cpuTimeUsed)

	os.RemoveAll(DB_PATH)
}

func benchmarkK4Random(numOps int) {
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
	for i := 0; i < numOps; i++ {
		key = []byte(fmt.Sprintf("key%d", rand.Intn(numOps)))
		value = []byte(fmt.Sprintf("value%d", i))
		if err := db.Put(key, value, nil); err != nil {
			log.Fatalf("Error putting key: %v", err)
		}
	}
	cpuTimeUsed := time.Since(start).Seconds()
	fmt.Printf("K4 Put: %f seconds\n", cpuTimeUsed)

	// Benchmark Get
	start = time.Now()
	for i := 0; i < numOps; i++ {
		key = []byte(fmt.Sprintf("key%d", rand.Intn(numOps)))
		if _, err := db.Get(key); err != nil {
			log.Fatalf("Error getting key: %v", err)
		}
	}
	cpuTimeUsed = time.Since(start).Seconds()
	fmt.Printf("K4 Get: %f seconds\n", cpuTimeUsed)

	// Benchmark Delete
	start = time.Now()
	for i := 0; i < numOps; i++ {
		key = []byte(fmt.Sprintf("key%d", rand.Intn(numOps)))
		if err := db.Delete(key); err != nil {
			log.Fatalf("Error deleting key: %v", err)
		}
	}
	cpuTimeUsed = time.Since(start).Seconds()
	fmt.Printf("K4 Delete: %f seconds\n", cpuTimeUsed)

	os.RemoveAll(DB_PATH)
}

func benchmarkK4Concurrent(numOps, numThreads int) {
	var wg sync.WaitGroup
	wg.Add(numThreads)

	for i := 0; i < numThreads; i++ {
		go func() {
			defer wg.Done()

			benchmarkK4(i, numOps)
		}()
	}

	wg.Wait()
}

func main() {
	numOps := flag.Int("num_ops", 10000, "number of operations")
	numThreads := flag.Int("num_threads", 4, "number of threads for concurrent operations")

	flag.Parse() // parse the flags

	fmt.Println("Benchmarker started with the set parameters:")
	fmt.Printf("Number of operations: %d\n", *numOps)
	fmt.Printf("Number of threads: %d\n", *numThreads)

	fmt.Println()

	fmt.Println("Benchmarking K4 non concurrent operations")
	benchmarkK4(-1, *numOps)
	fmt.Println()
	fmt.Println("Benchmarking K4 random operations")
	benchmarkK4Random(*numOps)
	fmt.Println()
	fmt.Println("Benchmarking K4 concurrent operations")
	benchmarkK4Concurrent(*numOps, *numThreads)
}
