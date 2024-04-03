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

// SSTableIterator is an iterator for SSTable
type SSTableIterator struct {
	sst   *SSTable  // pointer to the associated SSTable.
	file  *os.File  // file handle for the SSTable file.
	Value *LSMEntry // current entry.

}

/*
Writes a list of MemtableKeyValue entires to file in SSTable format.
The format of the SSTable file is as follows:
1. Bloom filter size (OffsetSize)
2. Bloom filter data (BloomFilter ProtoBuf)
3. Index size (OffsetSize)
4. Index data (Index ProtoBuf)
5. Data entries

The data entries are written in the following format:
1. Size of the entry (OffsetSize)
2. Entry data (LSMEntry ProtoBuf)
*/
