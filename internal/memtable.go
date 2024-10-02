package velocitylog

import (
	"time"

	pb "github.com/danish45007/velocitylog/proto"
	"github.com/huandu/skiplist"
)

// Memtable is a memory table that supports fast writes, reads, deletes, and range scans.
// It uses a skip list as the underlying data structure to store key-value pairs.
type Memtable struct {
	data skiplist.SkipList // Skip list to store key-value pairs.
	size int64             // Size of the memtable in bytes.
}

// NewMemtable creates a new memtable.
func NewMemtable() *Memtable {
	return &Memtable{
		data: *skiplist.New(skiplist.String), // Create a new skip list with string keys.
		size: 0,                              // Initialize the size to 0.
	}
}

// Put inserts a key-value pair into the memtable, Not Thread-Safe Implementation.
func (m *Memtable) Put(key string, value []byte) {
	// Calculate the size of the value.
	sizeChange := int64(len(value))
	// check if the key already exists in the memtable
	existingValue := m.data.Get(key)
	if existingValue != nil {
		// if the key already exists, update the size of the memtable
		// by subtracting the size of the existing value
		m.size -= int64(len(existingValue.Value.(*pb.LSMEntry).Value))
	} else {
		// if the key does not exist, update the size of the memtable
		// by adding the size of the key
		sizeChange += int64(len(key))
	}

	// update with new entry
	entry := getLSMEntry(key, &value, pb.Command_PUT)
	m.data.Set(key, entry)
	// update the size of the memtable by adding the size of the value
	m.size += sizeChange
}

// Delete deletes a key from the memtable, Not Thread-Safe Implementation.
func (m *Memtable) Delete(key string) {
	existingEntry := m.data.Get(key)
	if existingEntry != nil {
		// if the key exists, update the size of the memtable
		// by subtracting the size of the value
		m.size -= int64(len(existingEntry.Value.(*pb.LSMEntry).Value))
	} else {
		// if the key does not exist, update the size of the memtable
		// by adding the size of the key
		m.size += int64(len(key))
	}
	// update with new entry
	entry := getLSMEntry(key, nil, pb.Command_DELETE)
	m.data.Set(key, entry)
}

// Get retrieves a value for a given key from the memtable, Not Thread-Safe Implementation.
func (m *Memtable) Get(key string) *pb.LSMEntry {
	entry := m.data.Get(key)

	if entry == nil {
		return nil
	}

	// We need to include the tombstones in the range scan.
	// The caller need checks the command field in the LSMEntry to determine
	// if the entry is a tombstone or not.
	return entry.Value.(*pb.LSMEntry)
}

// RangeScan returns all key-value pairs in the memtable within the given key range, Not Thread-Safe Implementation.
// The startKey is inclusive, and the endKey is inclusive.
func (m *Memtable) RangeScan(startKey, endKey string) []*pb.LSMEntry {
	var results []*pb.LSMEntry
	// Find the first entry that is greater than or equal to the start key.
	// and use the Next method to iterate through the entries.
	iterator := m.data.Find(startKey)

	for iterator != nil {
		// If the key is greater than the end key, break the loop.
		if iterator.Element().Key().(string) > endKey {
			break
		}
		// We need to include the tombstones in the range scan.
		// The caller need checks the command field in the LSMEntry to determine
		// if the entry is a tombstone or not.
		results = append(results, iterator.Value.(*pb.LSMEntry))
		iterator = iterator.Next()
	}
	return results
}

// SizeInBytes returns the size of the memtable in bytes. Not Thread-Safe Implementation.
func (m *Memtable) SizeInBytes() int64 {
	return m.size
}

// Clear resets the memtable to an empty state. Not Thread-Safe Implementation.
func (m *Memtable) Clear() {
	m.data.Init() // Initialize the skip list.
	m.size = 0    // Reset the size to 0.
}

func (m *Memtable) Len() int {
	return m.data.Len()
}

// GenerateEntries returns a serializable list of memtable entries in sorted order. Not Thread-Safe Implementation.
func (m *Memtable) GenerateEntries() []*pb.LSMEntry {
	var results []*pb.LSMEntry
	// start from the first entry in the skip list
	iterator := m.data.Front()
	for iterator != nil {
		results = append(results, iterator.Value.(*pb.LSMEntry))
		iterator = iterator.Next()
	}
	return results
}

// GetLSMEntry return a new LSMEntry with the given key, value, and command.
func getLSMEntry(key string, value *[]byte, command pb.Command) *pb.LSMEntry {
	lsmEntry := &pb.LSMEntry{
		Key:       key,
		Command:   command,
		Timestamp: time.Now().UnixNano(),
	}
	if value != nil {
		lsmEntry.Value = *value
	}
	return lsmEntry
}
