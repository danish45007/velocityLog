# 2. use-memtable-as-in-memory-buffer

Date: 2024-04-06

## Status

Accepted

Supercedes [1. Record architecture decisions](0001-record-architecture-decisions.md)

## Context

In our system architecture, we are building a write-optimized database engine based on Log-Structured Merge Trees (LSM Trees). To achieve high write-throughput, we need an efficient in-memory buffer for temporarily storing writes before they are persisted to disk.

## Decision

We have decided to use a memtable as the in-memory buffer for our LSM Tree implementation. The memtable will serve as a sorted structure for buffering writes in RAM before they are flushed to disk. We have chosen this approach for its efficiency in handling write operations and its ability to maintain data in sorted order, which facilitates efficient range scans.

## Consequences

* Easier Write Operations: Utilizing a memtable simplifies the handling of write operations by providing a fast, in-memory buffer for write ingestion.

* Improved Write Performance: Memtables enable high write-throughput by allowing quick writes to memory before eventual disk flush.

* Efficient Range Scans: By maintaining data in sorted order, memtables facilitate efficient range scans, which are essential for various query operations.

* Increased Memory Usage: Storing data in memory incurs memory overhead, which needs to be monitored and managed to prevent resource exhaustion.

* Potential Data Loss Risk: Since data in the memtable is only temporarily stored in memory, there is a risk of data loss in the event of a system failure or crash before the data is flushed to disk. Proper error handling and recovery mechanisms are necessary to mitigate this risk.
