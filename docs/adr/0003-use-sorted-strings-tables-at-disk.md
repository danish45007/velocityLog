# 3. use-sorted-strings-tables-at-disk

## Author: Danish

Date: 2024-04-07

## Status: Accepted

Supercedes [3. use-sorted-strings-tables-at-disk](0003-use-sorted-strings-tables-at-disk.md)

## Context

In the process of building a write-optimized database engine, we have explored various components, including the Memtable and the overall architecture. Now, we are focusing on implementing an on-disk representation of data to ensure efficient reads and writes. In this context, we are discussing the usage of Sorted String Tables (SSTables) and the auxiliary structures required to work with them effectively.

## Decision

* We have decided to adopt Sorted String Tables (SSTables) as the on-disk representation of data in our database engine. SSTables provide an efficient mechanism for storing and retrieving data while optimizing both read and write operations. Key elements of our decision include:

* Storage Mechanism: SSTables store data in sorted order on disk, facilitating efficient binary search-based lookups.

* Flush Strategy: Data from the Memtable is flushed to disk to create immutable SSTable files when specific flush conditions are met, typically when the Memtable size exceeds a predetermined threshold.

* Metadata Management: Each SSTable contains essential metadata structures such as a Bloom filter and an index to support efficient data access and reduce disk I/O.

* Sparse Indexing: To mitigate the memory overhead associated with large SSTables, we may implement sparse indexing, which indexes only a subset of keys, reducing the index size while maintaining reasonable read performance.

* Bloom Filters: Bloom filters are used to quickly determine whether a key is likely present in an SSTable, reducing unnecessary disk I/O for non-existent keys.

* SSTable Iterator: We will develop an SSTable iterator to facilitate memory-efficient traversal of SSTable entries during compaction operations.

## Consequences

By adopting SSTables as the on-disk storage mechanism, we anticipate the following consequences:

* Improved Performance: SSTables facilitate efficient read and write operations, leading to overall performance improvements in the database engine.

* Reduced Disk I/O: The use of auxiliary structures such as Bloom filters and indexes minimizes disk I/O by quickly identifying relevant data entries.

* Memory Efficiency: Sparse indexing and memory-efficient SSTable iterators mitigate memory overhead, enabling the handling of large datasets without excessive memory consumption.

* Complexity: The implementation and management of SSTables, including auxiliary structures and compaction processes, add complexity to the system architecture.

* Trade-offs: Sparse indexing and probabilistic data structures like Bloom filters introduce trade-offs between memory usage, query performance, and false positive rates.

Overall, the decision to use SSTables aligns with our goal of building a write-optimized database engine capable of handling large volumes of data with efficient read and write operations.
