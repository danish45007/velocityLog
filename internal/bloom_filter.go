package velocitylog

import (
	pb "github.com/danish45007/velocitylog/proto"
	"github.com/spaolacci/murmur3"
)

// Create a new BloomFilter
func newBloomFilter(size int64) *pb.BloomFilter {
	return &pb.BloomFilter{
		Bitset: make([]bool, size),
		Size:   size,
	}
}

// Add key to the BloomFilter
func Add(bloomFilter *pb.BloomFilter, key []byte) {
	hash1 := murmur3.Sum64(key)
	hash2 := murmur3.Sum64WithSeed(key, 1)
	hash3 := murmur3.Sum64WithSeed(key, 2)

	bloomFilter.Bitset[hash1%uint64(bloomFilter.Size)] = true
	bloomFilter.Bitset[hash2%uint64(bloomFilter.Size)] = true
	bloomFilter.Bitset[hash3%uint64(bloomFilter.Size)] = true
}

// Check if key is in the BloomFilter
func Contains(bloomFilter *pb.BloomFilter, key []byte) bool {
	hash1 := murmur3.Sum64(key)
	hash2 := murmur3.Sum64WithSeed(key, 1)
	hash3 := murmur3.Sum64WithSeed(key, 2)

	return bloomFilter.Bitset[hash1%uint64(bloomFilter.Size)] &&
		bloomFilter.Bitset[hash2%uint64(bloomFilter.Size)] &&
		bloomFilter.Bitset[hash3%uint64(bloomFilter.Size)]
}
