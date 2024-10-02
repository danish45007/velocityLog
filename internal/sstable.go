package velocitylog

import (
	"io"
	"os"

	pb "github.com/danish45007/velocitylog/proto"
)

// EntrySize is the size of an entry in the log.
type EntrySize int64

type SSTable struct {
	bloomFilter *pb.BloomFilter // Bloom filter for the SSTable.
	index       *pb.Index       // Index for the SSTable.
	file        *os.File        // File handle for on-disk ssTable file storage.
	dataOffset  EntrySize       // Offset from where the actual entries start in the file.

}

// SSTableIterator is an iterator for SSTable
type SSTableIterator struct {
	sst   *SSTable     // pointer to the associated SSTable.
	file  *os.File     // file handle for the SSTable file.
	Value *pb.LSMEntry // current entry.

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
func SerializeToSSTable(messages []*pb.LSMEntry, filename string) (*SSTable, error) {
	bloomFilter, index, buffEntries, err := generateMetaDataAndEntriesBuffer(messages)
	if err != nil {
		return nil, err
	}
	indexData := MarshalEntry(index)
	bloomFilterData := MarshalEntry(bloomFilter)

	dataOffset, err := WriteToSSTable(filename, bloomFilterData, indexData, buffEntries)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return &SSTable{
		bloomFilter: bloomFilter,
		index:       index,
		file:        file,
		dataOffset:  dataOffset,
	}, nil

}

// OpenSSTable opens an SSTable file and returns an SSTable object for reading.
func OpenSSTable(filename string) (*SSTable, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	bloomFilterData, indexData, dataOffset, err := readSSTableMetadata(file)
	if err != nil {
		return nil, err
	}

	return &SSTable{
		bloomFilter: bloomFilterData,
		index:       indexData,
		file:        file,
		dataOffset:  dataOffset,
	}, nil

}

// Close closes the SSTable file.
func (s *SSTable) Close() error {
	return s.file.Close()
}

// Get returns the value/ for the given key from the SSTable.
// Returns nil if the key is not found.
func (s *SSTable) Get(key string) (*pb.LSMEntry, error) {
	// check if the key is present in the bloom filter.
	if !Contains(s.bloomFilter, []byte(key)) {
		return nil, nil
	}
	// get the offset of the key from the index.
	offset, found := findOffsetForKey(key, s.index.Index)
	if !found {
		return nil, nil
	}

	// seek to the offset of entry in the file. The offset is relative to the start of the data entries.
	// so we need to add the data offset to the offset of the entry.
	_, err := s.file.Seek(int64(s.dataOffset+EntrySize(offset)), io.SeekStart)
	if err != nil {
		return nil, err
	}
	// read the size of the entry.
	size, _ := readDataSize(s.file)

	// read the entry data.
	data, err := readEntryFromFile(s.file, size)
	if err != nil {
		return nil, err
	}
	entry := &pb.LSMEntry{}
	UnmarshalEntry(data, entry)
	return entry, nil

}

// rangeScan returns all the entries in the SSTable that have keys in the range [startKey, endKey) inclusive.
func (s *SSTable) RangeScan(startKey, endKey string) ([]*pb.LSMEntry, error) { // Change here
	startOffsetKey, found := findOffsetForKey(startKey, s.index.Index)
	if !found {
		return nil, nil
	}
	_, err := s.file.Seek(int64(s.dataOffset+EntrySize(startOffsetKey)), io.SeekStart)
	if err != nil {
		return nil, err
	}
	var results []*pb.LSMEntry
	for {
		size, err := readDataSize(s.file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		data, err := readEntryFromFile(s.file, size)
		if err != nil {
			return nil, err
		}
		entry := &pb.LSMEntry{}
		UnmarshalEntry(data, entry)
		if entry.Key > endKey {
			break
		}
		results = append(results, entry)
	}
	return results, nil
}

// GetEntries returns all the entries in the SSTable.
func (s *SSTable) GetEntries() ([]*pb.LSMEntry, error) {
	// seek to the start of the data entries in the file.
	_, err := s.file.Seek(int64(s.dataOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	var results []*pb.LSMEntry
	for {
		size, err := readDataSize(s.file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		data, err := readEntryFromFile(s.file, size)
		if err != nil {
			return nil, err
		}
		entry := &pb.LSMEntry{}
		UnmarshalEntry(data, entry)
		results = append(results, entry)
	}
	return results, nil
}

// Front returns an Iterator for the SSTable.
// The iterator is positioned at the first entry in the SSTable.
func (s *SSTable) Front() *SSTableIterator {
	// open a new file for the iterator.
	file, err := os.Open(s.file.Name())
	if err != nil {
		return nil
	}
	iterator := &SSTableIterator{
		sst:   s,
		file:  file,
		Value: &pb.LSMEntry{},
	}
	// seek to the start of the data entries in the file.
	_, err = iterator.file.Seek(int64(iterator.sst.dataOffset), io.SeekStart)
	if err != nil {
		panic(err)
	}
	// read the size of the file
	size, err := readDataSize(iterator.file)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}
	data, err := readEntryFromFile(iterator.file, size)
	if err != nil {
		panic(err)
	}
	UnmarshalEntry(data, iterator.Value)
	return iterator

}

// Next returns the next entry in the SSTable.
// Returns nil if there are no more entries.
func (it *SSTableIterator) Next() *SSTableIterator {
	size, err := readDataSize(it.file)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}
	data, err := readEntryFromFile(it.file, size)
	if err != nil {
		panic(err)
	}
	it.Value = &pb.LSMEntry{}
	UnmarshalEntry(data, it.Value)
	return it
}

// Closes the iterator.
func (it *SSTableIterator) Close() error {
	return it.file.Close()
}
