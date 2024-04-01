package velocitylog

// EntrySize is the size of an entry in the log.
type EntrySize int64

type SSTable struct {
	bloomFilter *BloomFilter // Bloom filter for the SSTable.

}
