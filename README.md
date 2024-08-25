# LSM Tree Implementation

## Project Overview

This repository contains a Go implementation of a Log-Structured Merge-Tree (LSM-Tree), a data structure used for persistent storage that prioritizes write performance over read performance. LSM-Trees are particularly useful for scenarios where write operations are frequent and read operations can tolerate some latency. 

## Features

- **Memtable:** An in-memory data store that efficiently handles incoming write requests.
- **Sorted String Tables (SSTables):** Persistent storage of data on disk, sorted by key, providing efficient retrieval and merging operations.
- **LSM Tree Orchestration:** Implementation of the core LSM-Tree logic, managing the memtable, SSTables, compaction, and crash recovery.
- **Read/Write Access Patterns:** Optimized read and write access patterns for improved efficiency and performance.
- **Performance Benchmarks:**  A collection of benchmarks to measure the performance of various LSM-Tree operations.
- **Serialization Protocol:** A custom serialization protocol for efficiently storing data in SSTables.
- **Architecture Decision Records (ADRs):** A documented record of architectural decisions made during the development process.


## Installation

**Prerequisites:**

- Go 1.18 or later

**Installation Steps:**

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/lsm-tree.git
   ```

2. **Navigate to the project directory:**
   ```bash
   cd lsm-tree
   ```

3. **Build the project:**
   ```bash
   go build
   ```

## Usage

```go
package main

import (
	"fmt"

	"github.com/your-username/lsm-tree"
)

func main() {
	// Create a new LSM-Tree instance
	db := lsm.NewLSMTree("data")

	// Insert key-value pairs into the LSM-Tree
	err := db.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		fmt.Println(err)
		return
	}
	err = db.Put([]byte("key2"), []byte("value2"))
	if err != nil {
		fmt.Println(err)
		return
	}

	// Retrieve a value by its key
	value, err := db.Get([]byte("key1"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Value for key1:", string(value))

	// Close the LSM-Tree instance
	db.Close()
}

```

This example demonstrates basic operations, including inserting key-value pairs and retrieving values.  

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

This project is licensed under the MIT License. 
