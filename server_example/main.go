package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/guycipher/k4/v2"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

// handleConnection reads commands from the client and executes them on the K4 database.
func handleConnection(conn net.Conn, db *k4.K4) {
	defer conn.Close() // defer closing the connection

	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadBytes('\n') // Read up to the next newline
		if err != nil {
			log.Println("Error reading from client:", err)
			return
		}

		message = bytes.TrimSpace(message)
		parts := bytes.SplitN(message, []byte(" "), 3)
		if len(parts) < 2 {
			conn.Write([]byte("Invalid command\n"))
			continue
		}

		command := bytes.ToUpper(parts[0])
		key := []byte(parts[1])
		var response string

		switch {
		case bytes.HasPrefix(command, []byte("PUT")):
			// i.e. PUT key value
			if len(parts) < 3 {
				response = "PUT command requires a value\n"
			} else {
				value := []byte(parts[2])
				err := db.Put(key, value, nil)
				if err != nil {
					response = fmt.Sprintf("Error putting key: %v\n", err)
				} else {
					response = "OK\n"
				}
			}
		case bytes.HasPrefix(command, []byte("GET")):
			// i.e. GET key
			value, err := db.Get(key)
			if err != nil {
				response = fmt.Sprintf("Error getting key: %v\n", err)
			} else {
				response = fmt.Sprintf("Value: %s\n", value)
			}
		case bytes.HasPrefix(command, []byte("DELETE")):
			// i.e. DELETE key
			err := db.Delete(key)
			if err != nil {
				response = fmt.Sprintf("Error deleting key: %v\n", err)
			} else {
				response = "OK\n"
			}
		default:
			response = "Unknown command\n"
		}

		conn.Write([]byte(response))
	}
}

// startServer starts a TCP server that listens for incoming connections and handles them
// using the handleConnection function
func startServer(ctx context.Context, address string, db *k4.K4) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	defer listener.Close()

	log.Printf("Server started on %s\n", address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				log.Println("Server shutting down...")
				return
			default:
				log.Println("Error accepting connection:", err)
				continue
			}
		}
		go handleConnection(conn, db)
	}
}

func main() {
	directory := "./data"
	memtableFlushThreshold := 1024 * 1024 // 1MB
	compactionInterval := 3600            // 1 hour
	logging := true
	compression := false

	db, err := k4.Open(directory, memtableFlushThreshold, compactionInterval, logging, compression)
	if err != nil {
		log.Fatalf("Failed to open K4: %v", err)
	}

	// Create a context that is canceled on SIGINT or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go startServer(ctx, ":8000", db)

	// Wait for the context to be canceled
	<-ctx.Done()

	db.Close() // Close the database

	log.Println("Server stopped")
}
