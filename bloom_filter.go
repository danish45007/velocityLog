package velocitylog

import (
	"github.com/spaolacci/murmur3"
)

func newBloomFilter(size int64) *BloomFilter {
	return &BloomFilter{
		Bitset: make([]bool, size), // Bitset slice of size with all values set to false.
		Size:   size,               // Size of the Bitset.
	}
}

// Add adds a key to the Bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	// generate 3 hash values for the key.
	hash1 := murmur3.Sum64(key)
	hash2 := murmur3.Sum64WithSeed(key, 1)
	hash3 := murmur3.Sum64WithSeed(key, 2)

	// set the corresponding bits in the Bitset to true.
	bf.Bitset[hash1%uint64(bf.Size)] = true
	bf.Bitset[hash2%uint64(bf.Size)] = true
	bf.Bitset[hash3%uint64(bf.Size)] = true
}

// Contains checks if a key is present in the Bloom filter.
func (bf *BloomFilter) Contains(key []byte) bool {
	// generate 3 hash values for the key.
	hash1 := murmur3.Sum64(key)
	hash2 := murmur3.Sum64WithSeed(key, 1)
	hash3 := murmur3.Sum64WithSeed(key, 2)

	// check if the corresponding bits in the Bitset are true.
	return bf.Bitset[hash1%uint64(bf.Size)] && bf.Bitset[hash2%uint64(bf.Size)] && bf.Bitset[hash3%uint64(bf.Size)]
}
