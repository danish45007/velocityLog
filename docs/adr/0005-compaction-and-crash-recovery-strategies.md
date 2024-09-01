# 5. compaction-and-crash-recovery-strategies

## Author: Danish

Date: 2024-04-07

## Status: Accepted

Supercedes [4. lsm-tree-orchestrating-reads-and-write](0004-lsm-tree-orchestrating-reads-and-write.md)

## Context

Our storage engine, designed to efficiently handle reads, writes, updates, and deletions, is facing challenges related to write and read amplification due to the continuous influx of data. These challenges stem from the nature of the log-structured merge (LSM) tree, where updates and deletions are recorded as new entries or tombstones, respectively, leading to increased disk space usage and slower read performance as more SSTable files accumulate over time. Additionally, ensuring data durability in the event of application crashes is crucial for maintaining system reliability.

## Decision

We have decided to implement a Tiered Compaction strategy in our LSM Tree-based storage engine to address the issues of write and read amplification. This strategy involves:

* Tiered Compaction: SSTables are organized into levels, with compaction triggered when the number of SSTables at a level exceeds a predefined threshold. The compaction process merges these SSTables into a single, consolidated SSTable, which is then moved to the next higher level.

* Background Compaction Process: A background process will be implemented to continuously monitor the levels and trigger compaction when necessary. This process runs as a separate goroutine to minimize the impact on the main operations of the storage engine.

* Minimizing Lock Contention: The compaction process is designed to minimize lock contention by holding locks only for short durations during the initial checks and setup phases, allowing other operations to continue concurrently.

* Write-Ahead Log (WAL) Integration: To ensure data durability, a WAL is used to log all mutations (inserts, updates, deletes) before they are applied to the Memtable. The WAL ensures that in the event of a crash, any unflushed data can be recovered.

## Consequences

* Improved Disk Space Utilization: By regularly compacting SSTables and removing redundant or deleted entries, disk space usage is optimized, directly addressing the issue of write amplification.

* Enhanced Read Performance: Compaction reduces the number of SSTables that must be scanned during read operations, particularly for range scans, thereby improving read latency and overall system performance.

* Efficient Crash Recovery: The integration of WAL with checkpoints ensures that in the event of a crash, the system can recover to its last consistent state, maintaining data durability and reducing downtime.

* Increased System Complexity: While the benefits of compaction and WAL are clear, the implementation adds complexity to the system, requiring careful management of concurrency, memory, and disk I/O resources.

* Resource Overhead: The background compaction process introduces additional resource overhead, which must be managed to avoid impacting the primary operations of the storage engine.
