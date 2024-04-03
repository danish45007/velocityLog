package velocitylog

import (
	"bytes"
	"encoding/binary"
)

// Generate a bloom filter, index and entries buffer for the SSTable from list of MemtableKeyValue entries.
func generateMetaDataAndEntriesBuffer(messages []*LSMEntry) (*BloomFilter, *Index, *bytes.Buffer) {
	var (
		bloomFilter   *BloomFilter = newBloomFilter(BloomFilterSize)
		index         []*IndexEntry
		currentOffset EntrySize     = InitialOffset
		entriesBuffer *bytes.Buffer = new(bytes.Buffer) // Buffer to store the entries.
	)

	for _, message := range messages {
		// marshal the message to bytes.
		marshalEntry := MarshalEntry(message)
		entrySize := EntrySize(len(marshalEntry))

		// add the entry to index and bloom filter.
		index = append(index, &IndexEntry{
			Key:    message.Key,
			Offset: int64(currentOffset),
		})
		bloomFilter.Add([]byte(message.Key))

		// write the entry size and entry data to the buffer.
		// entry size is written as a 64-bit integer in little-endian format.
		binary.Write(entriesBuffer, binary.LittleEndian, EntrySize(entrySize))

		// write the entry data to the buffer.
		entriesBuffer.Write(marshalEntry)

		// update the current offset.
		// currentOffset is updated by adding the size of the entry size and the entry data.
		currentOffset += binary.Size(int64(entrySize)) + EntrySize(entrySize)

	}
	return bloomFilter, &Index{
		Index: index,
	}, entriesBuffer
}
