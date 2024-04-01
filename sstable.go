package velocitylog

import "os"

// EntrySize is the size of an entry in the log.
type EntrySize int64

type SSTable struct {
	bloomFilter *BloomFilter // Bloom filter for the SSTable.
	index       *Index       // Index for the SSTable.
	file        *os.File     // File handle for on-disk ssTable file storage.
	dataOffset  EntrySize    // Offset from where the actual entries start in the file.

}

type SSTableIterator struct {
	sst   *SSTable  // pointer to the associated SSTable.
	file  *os.File  // file handle for the SSTable file.
	Value *LSMEntry // current entry.

}
