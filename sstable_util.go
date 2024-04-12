package velocitylog

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

// Generate a bloom filter, index and entries buffer for the SSTable from list of MemtableKeyValue entries.
func generateMetaDataAndEntriesBuffer(messages []*LSMEntry) (*BloomFilter, *Index, *bytes.Buffer, error) {
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
		currentOffset += EntrySize(binary.Size(entrySize)) + entrySize

	}
	return bloomFilter, &Index{
		Index: index,
	}, entriesBuffer, nil
}

func WriteToSSTable(filename string, bloomFilterData []byte, indexData []byte, entriesData *bytes.Buffer) (EntrySize, error) {
	file, err := os.Create(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	var dataOffset EntrySize = InitialOffset

	// write the bloom filter size to the file.
	if err := binary.Write(file, binary.LittleEndian, EntrySize(len(bloomFilterData))); err != nil {
		return 0, err
	}
	// update the data offset with the size of the bloom filter size.
	dataOffset += EntrySize(binary.Size(EntrySize(len(bloomFilterData))))
	// write the bloom filter data.
	if _, err := file.Write(bloomFilterData); err != nil {
		return 0, err
	}
	// update the data offset with the size of the bloom filter data.
	dataOffset += EntrySize(len(bloomFilterData))

	// write the index size to the file.
	if err := binary.Write(file, binary.LittleEndian, EntrySize(len(indexData))); err != nil {
		return 0, err
	}
	// update the data offset with the size of the index size.
	dataOffset += EntrySize(binary.Size(EntrySize(len(indexData))))
	// write the index data.
	if _, err := file.Write(indexData); err != nil {
		return 0, err
	}
	// update the data offset with the size of the index data.
	dataOffset += EntrySize(len(indexData))

	// write the entries data to the file.
	if _, err := io.Copy(file, entriesData); err != nil {
		return 0, err
	}
	return dataOffset, nil
}

// readDataSize reads the size of the data from the file.
func readDataSize(file *os.File) (EntrySize, error) {
	var size EntrySize
	if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
		return 0, err
	}
	return size, nil
}

// readSSTableMetadata reads the bloom filter, index and data offset from the SSTable file.
func readSSTableMetadata(file *os.File) (*BloomFilter, *Index, EntrySize, error) {
	var dataOffset EntrySize = InitialOffset

	// read the bloom filter size from the file.
	bloomFilterSize, err := readDataSize(file)
	if err != nil {
		return nil, nil, 0, err
	}
	// update the data offset with the size of the bloom filter size.
	dataOffset += EntrySize(binary.Size(bloomFilterSize))

	// read the bloom filter data from the file.
	bloomFilterData := make([]byte, bloomFilterSize)
	if _, err := file.Read(bloomFilterData); err != nil {
		return nil, nil, 0, err
	}
	// update the data offset with the size of the bloom filter data.
	dataOffset += EntrySize(len(bloomFilterData))

	// read the index size from the file.
	indexSize, err := readDataSize(file)
	if err != nil {
		return nil, nil, 0, err
	}
	// update the data offset with the size of the index size.
	dataOffset += EntrySize(binary.Size(indexSize))

	// read the index data from the file.
	indexData := make([]byte, indexSize)
	if _, err := file.Read(indexData); err != nil {
		return nil, nil, 0, err
	}
	// update the data offset with the size of the index data.
	dataOffset += EntrySize(len(indexData))

	// unmarshal the bloom filter and index data.
	bloomFilter := &BloomFilter{}
	UnmarshalEntry(bloomFilterData, bloomFilter)
	index := &Index{}
	UnmarshalEntry(indexData, index)

	return bloomFilter, index, dataOffset, nil

}
