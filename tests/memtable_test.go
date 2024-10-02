package tests

import (
	"fmt"
	"testing"

	velocitylog "github.com/danish45007/velocitylog/internal"
	pb "github.com/danish45007/velocitylog/proto"
	"github.com/stretchr/testify/assert"
)

func TestMemtablePutGetDelete(t *testing.T) {
	t.Parallel()
	memtable := velocitylog.NewMemtable()
	// test put and get operations
	memtable.Put("key1", []byte("value1"))
	assert.Equal(t, []byte("value1"), memtable.Get("key1").Value, "memtable.Get(key1) should return value1")
	// test timestamp to be not nil for the key
	assert.NotNil(t, memtable.Get("key1").Timestamp, "memtable.Get(key1).Timestamp should not be nil")

	// test delete operation
	memtable.Delete("key1")
	assert.Nil(t, memtable.Get("key1").Value, "memtable.Get(key1) should return nil after delete")

	// get operation for non-existing key
	assert.Nil(t, memtable.Get("key11"), "memtable.Get(key1) should return nil for non-existing key")
}

// test the RangeScan() method of memtable
func TestMemtableScan(t *testing.T) {
	t.Parallel()

	memtable := velocitylog.NewMemtable()

	// Test Scan() with no results.
	results := memtable.RangeScan("foo", "foo")
	assert.Empty(t, results, "memtable.Scan(\"foo\", \"foo\") should return an empty slice")

	// Test Scan() with one result.
	memtable.Put("foo", []byte("bar"))
	results = memtable.RangeScan("foo", "foo")
	assert.Len(t, results, 1, "memtable.Scan(\"foo\", \"foo\") should return a slice with length 1")
	assert.Equal(t, []byte("bar"), results[0].Value, "memtable.Scan(\"foo\", \"foo\") should return [\"bar\"]")

	// Test Scan() with multiple results.
	memtable.Put("foo", []byte("bar0"))
	memtable.Put("foo8", []byte("bar8"))
	memtable.Put("foo1", []byte("bar1"))
	memtable.Put("foo7", []byte("bar7"))
	memtable.Put("foo3", []byte("bar3"))
	memtable.Put("foo9", []byte("bar9"))
	memtable.Put("foo6", []byte("bar6"))
	memtable.Put("foo2", []byte("bar2"))
	memtable.Put("foo4", []byte("bar4"))
	memtable.Put("foo5", []byte("bar5"))
	results = memtable.RangeScan("foo", "foo9")
	assert.Len(t, results, 10, "memtable.Scan(\"foo\", \"foo9\") should return a slice with length 10")
	for i := 0; i < 10; i++ {
		assert.Equal(t, []byte(fmt.Sprintf("bar%v", i)), results[i].Value, "memtable.Scan(\"foo\", \"foo9\") should return [\"bar0\", \"bar1\", ..., \"bar9\"]")
	}

	// Scan another range
	results = memtable.RangeScan("foo2", "foo7")
	assert.Len(t, results, 6, "memtable.Scan(\"foo2\", \"foo7\") should return a slice with length 6")
	for i := 2; i < 8; i++ {
		assert.Equal(t, []byte(fmt.Sprintf("bar%v", i)), results[i-2].Value, "memtable.Scan(\"foo2\", \"foo7\") should return [\"bar2\", \"bar3\", ..., \"bar7\"]")
	}

	// Scan another range with no results
	results = memtable.RangeScan("foo2", "foo1")
	assert.Empty(t, results, "memtable.Scan(\"foo2\", \"foo1\") should return an empty slice")

	// Scan another range with one result
	results = memtable.RangeScan("foo2", "foo2")
	assert.Len(t, results, 1, "memtable.Scan(\"foo2\", \"foo2\") should return a slice with length 1")
	assert.Equal(t, []byte("bar2"), results[0].Value, "memtable.Scan(\"foo2\", \"foo2\") should return [\"bar2\"]")

	// Scan another range with non-exact start and end keys
	results = memtable.RangeScan("foo2", "fooz")
	assert.Len(t, results, 8, "memtable.Scan(\"foo2\", \"fooz\") should return a slice with length 8")
	for i := 2; i < 10; i++ {
		assert.Equal(t, []byte(fmt.Sprintf("bar%v", i)), results[i-2].Value, "memtable.Scan(\"foo2\", \"fooz\") should return [\"bar2\", \"bar3\", ..., \"bar9\"]")
	}

	// Scan another range with non-exact start and end keys
	results = memtable.RangeScan("a", "foo3")
	assert.Len(t, results, 4, "memtable.Scan(\"fo\", \"foo3\") should return a slice with length 4")
	for i := 0; i < 4; i++ {
		assert.Equal(t, []byte(fmt.Sprintf("bar%v", i)), results[i].Value, "memtable.Scan(\"fo\", \"foo3\") should return [\"bar0\", \"bar1\", \"bar2\", \"bar3\"]")
	}
}

func TestMemtableScanUpdateEntries(t *testing.T) {
	t.Parallel()
	memtable := velocitylog.NewMemtable()

	// populate the memtable with large number of entries
	for i := 0; i < 26; i++ {
		key := fmt.Sprintf("%c", 'a'+i)
		value := []byte(fmt.Sprintf("%c", 'a'+i))
		memtable.Put(key, value)
	}
	// update some entries
	for i := 0; i < 26; i++ {
		key := fmt.Sprintf("%c", 'a'+i)
		value := []byte(fmt.Sprintf("%c%c", 'a'+i, 'a'+i))
		memtable.Put(key, value)
	}

	// scan the memtable
	results := memtable.RangeScan("a", "z")

	// validate the results
	assert.Equal(t, 26, len(results), "memtable.Scan(\"a\", \"z\") should return 26 entries")
	for i := 0; i < 26; i++ {
		key := fmt.Sprintf("%c", 'a'+i)
		value := fmt.Sprintf("%c%c", 'a'+i, 'a'+i)
		assert.Equal(t, []byte(value), results[i].Value, fmt.Sprintf("memtable.Scan(\"a\", \"z\") should return %v:%v", key, value))
	}

}

// test the SizeInBytes() method of memtable
func TestMemtableSizeInBytes(t *testing.T) {
	t.Parallel()
	memtable := velocitylog.NewMemtable()

	// Test SizeInBytes() with no entries.
	assert.Equal(t, int64(0), memtable.SizeInBytes(), "memtable.SizeInBytes() should return 0 with no entries")

	// Test SizeInBytes() with one entry.
	memtable.Put("foo", []byte("bar"))
	assert.Equal(t, int64(6), memtable.SizeInBytes(), "memtable.SizeInBytes() should return 6 with one entry")

	// Test SizeInBytes() with multiple entries.
	memtable.Put("foo", []byte("bar0"))
	memtable.Put("foo8", []byte("bar8"))
	memtable.Put("foo1", []byte("bar1"))
	memtable.Put("foo7", []byte("bar7"))
	memtable.Put("foo3", []byte("bar3"))
	memtable.Put("foo9", []byte("bar9"))
	memtable.Put("foo6", []byte("bar6"))
	memtable.Put("foo2", []byte("bar2"))
	memtable.Put("foo4", []byte("bar4"))
	memtable.Put("foo5", []byte("bar5"))
	assert.Equal(t, int64(79), memtable.SizeInBytes(), "memtable.SizeInBytes() should return 64 with 10 entries")

	// Test SizeInBytes() after deleting an entry.
	memtable.Delete("foo")
	// we don't have to subtract the size of the key, because the key remains in the memtable with a tombstone marker.
	assert.Equal(t, int64(75), memtable.SizeInBytes(), "memtable.SizeInBytes() should return 64 after deleting an entry")

}

// Test GenerateEntries method of memtable. Performs a list of Put and Delete operations.
// then calls GenerateEntries() and check if the return entries are correct
// the list of entries should be in sorted order. Delete entries should be present with the tombstone marker.
func TestMemtableGenerateEntries(t *testing.T) {
	t.Parallel()

	memtable := velocitylog.NewMemtable()

	// Test GetSerializableEntries() with no entries.
	entries := memtable.GenerateEntries()
	assert.Equal(t, 0, len(entries), "memtable.GetSerializableEntries() should return 0 with no entries")

	// Test GetSerializableEntries() with one entry.
	memtable.Put("foo0", []byte("bar"))
	entries = memtable.GenerateEntries()
	assert.Equal(t, 1, len(entries), "memtable.GetSerializableEntries() should return 1 with one entry")
	assert.Equal(t, "foo0", entries[0].Key, "memtable.GetSerializableEntries() should return [\"foo\"] with one entry")
	assert.Equal(t, "bar", string(entries[0].Value), "memtable.GetSerializableEntries() should return [\"bar\"] with one entry")
	assert.Equal(t, pb.Command_PUT, entries[0].Command, "memtable.GetSerializableEntries() should return [PUT] with one entry")

	// Test GetSerializableEntries() with multiple entries.
	memtable.Put("foo0", []byte("bar0"))
	memtable.Put("foo8", []byte("bar8"))
	memtable.Put("foo1", []byte("bar1"))
	memtable.Put("foo7", []byte("bar7"))
	memtable.Put("foo3", []byte("bar3"))
	memtable.Put("foo9", []byte("bar9"))
	memtable.Put("foo6", []byte("bar6"))
	memtable.Put("foo2", []byte("bar2"))
	memtable.Put("foo4", []byte("bar4"))
	memtable.Put("foo5", []byte("bar5"))
	entries = memtable.GenerateEntries()
	assert.Equal(t, 10, len(entries), "memtable.GetSerializableEntries() should return 10 with multiple entries")
	for i := 0; i < 10; i++ {
		assert.Equal(t, fmt.Sprintf("foo%d", i), entries[i].Key, "memtable.GetSerializableEntries() should return [\"foo0\", \"foo1\", ..., \"foo9\"] with multiple entries")
		assert.Equal(t, []byte(fmt.Sprintf("bar%d", i)), entries[i].Value, "memtable.GetSerializableEntries() should return [\"bar0\", \"bar1\", ..., \"bar9\"] with multiple entries")
		assert.Equal(t, pb.Command_PUT, entries[i].Command, "memtable.GetSerializableEntries() should return [PUT] with multiple entries")
	}

	// Test GetSerializableEntries() with a deleted entry.
	memtable.Delete("foo0")
	memtable.Delete("foo8")
	memtable.Delete("foo1")
	// Delete entry not present in memtable.
	memtable.Delete("z")

	entries = memtable.GenerateEntries()
	assert.Equal(t, 11, len(entries), "memtable.GetSerializableEntries() should return 10 with a deleted entry")
	for i := 0; i < 10; i++ {
		assert.Equal(t, fmt.Sprintf("foo%d", i), entries[i].Key, "memtable.GetSerializableEntries() should return [\"foo0\", \"foo1\", ..., \"foo9\"] with a deleted entry")
		if i == 0 || i == 1 || i == 8 {
			assert.Equal(t, pb.Command_DELETE, entries[i].Command, "memtable.GetSerializableEntries() should return [DELETE] with a deleted entry")
			// Value should be nil.
			assert.Nil(t, entries[i].Value, "memtable.GetSerializableEntries() should return nil with a deleted entry")
		} else {
			assert.Equal(t, pb.Command_PUT, entries[i].Command, "memtable.GetSerializableEntries() should return [PUT] with a deleted entry")
			assert.Equal(t, []byte(fmt.Sprintf("bar%d", i)), entries[i].Value, "memtable.GetSerializableEntries() should return [\"bar0\", \"bar1\", ..., \"bar9\"] with a deleted entry")
		}
	}

	// Check last entry.
	assert.Equal(t, "z", entries[10].Key, "memtable.GetSerializableEntries() should return [\"z\"] with a deleted entry")
}

// Test Clear() method of memtable performs a list of put and delete operations and then
// calls the Clear() method. The memtable should be empty after calling the Clear() method.
func TestMemtableClear(t *testing.T) {
	t.Parallel()

	memtable := velocitylog.NewMemtable()

	// Test Clear() with no entries.
	memtable.Clear()
	assert.Equal(t, int(0), memtable.Len(), "memtable.Len() should return 0 with no entries")
	assert.Equal(t, int64(0), memtable.SizeInBytes(), "memtable.SizeInBytes() should return 0 with no entries")

	// Test Clear() with one entry.
	memtable.Put("foo", []byte("bar"))
	memtable.Clear()
	assert.Equal(t, int(0), memtable.Len(), "memtable.Len() should return 0 with no entries")
	assert.Equal(t, int64(0), memtable.SizeInBytes(), "memtable.SizeInBytes() should return 0 with one entry")

	// Test Clear() with multiple entries.
	memtable.Put("foo0", []byte("bar0"))
	memtable.Put("foo8", []byte("bar8"))
	memtable.Put("foo1", []byte("bar1"))
	memtable.Put("foo7", []byte("bar7"))
	memtable.Put("foo3", []byte("bar3"))
	memtable.Put("foo9", []byte("bar9"))
	memtable.Put("foo6", []byte("bar6"))
	memtable.Put("foo2", []byte("bar2"))
	memtable.Put("foo4", []byte("bar4"))
	memtable.Put("foo5", []byte("bar5"))
	memtable.Clear()
	assert.Equal(t, int(0), memtable.Len(), "memtable.Len() should return 0 with no entries")
	assert.Equal(t, int64(0), memtable.SizeInBytes(), "memtable.SizeInBytes() should return 0 with multiple entries")

	// Test Clear() with a deleted entry.
	memtable.Delete("foo")
	memtable.Clear()
	assert.Equal(t, int(0), memtable.Len(), "memtable.Len() should return 0 with no entries")
	assert.Equal(t, int64(0), memtable.SizeInBytes(), "memtable.SizeInBytes() should return 0 with a deleted entry")
}
