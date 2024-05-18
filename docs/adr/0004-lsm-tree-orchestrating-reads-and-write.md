# 4. lsm-tree-orchestrating-reads-and-write

## Author: Danish

Date: 2024-05-18

## Status: Accepted

Supercedes [4. lsm-tree-orchestrating-reads-and-write](0004-lsm-tree-orchestrating-reads-and-write.md)


## Context

To support high-throughput writes and efficient, consistent reads, we need to integrate the Memtable and SSTable components into a cohesive storage engine. The Memtable functions as a high-speed, in-memory buffer for both reads and writes, while the SSTable provides a persistent, on-disk structure to ensure data durability. We aim to handle the complexities involved in managing the Memtable's flushing process and achieve consistent reads between the in-memory Memtables and on-disk SSTables.

## Decision
We decided to implement a concurrent Memtable flushing mechanism with an optimized read path. This involves:

* Memtable Flushing Queue: Maintaining a distinct queue for filled and frozen Memtables awaiting disk flush. This allows the system to continue accepting new writes into a new Memtable.

* Background Flushing Process: Running a background process to handle the flushing of frozen Memtables to disk asynchronously, ensuring write operations are not interrupted.

* Tiered Read Path: Ensuring reads can access the most recent data by sequentially searching through the active Memtable, frozen Memtables in the flushing queue, and on-disk SSTables.

* Multi-Level Locking: Implementing a hierarchical locking mechanism to manage concurrency and avoid deadlocks, categorizing locks into three levels for different components.

## Consequences

### Positive
* Continuous Write Operations: The system can handle high-throughput writes without significant pauses, as new writes can continue in a new Memtable while the old one is being flushed.
* Efficient Reads: The read operations can access the most recent data quickly by searching through the active and frozen Memtables before querying the on-disk SSTables.
* Data Durability: Data is persistently stored on disk, ensuring durability and recovery in case of failures.
* Concurrency Management: The multi-level locking mechanism effectively prevents deadlocks and allows safe concurrent read and write operations.
### Negative
* Complexity in Flushing: Managing the background flushing process introduces additional complexity to the system.
* Locking Overhead: The hierarchical locking mechanism can become complex, especially with concurrent operations, potentially affecting performance.
* Heavy Range Scans: Range scan operations can lock the entire database, reducing concurrency and performance during these scans.
