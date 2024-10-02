package tests

import (
	"os"
	"testing"

	velocitylog "github.com/danish45007/velocitylog/internal"
	pb "github.com/danish45007/velocitylog/proto"
	"github.com/stretchr/testify/assert"
)

func TestSSTable(t *testing.T) {
	t.Parallel()

	var reopenFile bool = true

	for i := 0; i < 2; i++ {
		testFileName := "TestSStable.sst"
		// create a new memtable
		memtable := velocitylog.NewMemtable()

		populateMemtableWithTestData(memtable)

		// verify the contents of the tree.
		assert.Equal(t, []byte("value1"), memtable.Get("key1").Value)
		assert.Equal(t, []byte("value3"), memtable.Get("key3").Value)
		assert.Equal(t, []byte("value5"), memtable.Get("key5").Value)
		assert.Equal(t, []byte(nil), memtable.Get("key2").Value)
		assert.Equal(t, []byte(nil), memtable.Get("key4").Value)

		// write the memtable to an SSTable
		sstable, err := velocitylog.SerializeToSSTable(memtable.GenerateEntries(), testFileName)

		assert.Nil(t, err)    // check if there was an error while writing to the SSTable
		defer sstable.Close() // close the SSTable file after the test completes

		// read the SSTable and verify the contents
		entry, err := sstable.Get("key1")
		assert.Nil(t, err)
		assert.Equal(t, []byte("value1"), entry.Value)
		assert.NotEqual(t, entry.Timestamp, nil, "Timestamp should not be nil")

		entry, err = sstable.Get("key3")
		assert.Nil(t, err)
		assert.Equal(t, []byte("value3"), entry.Value)
		assert.NotEqual(t, entry.Timestamp, nil, "Timestamp should not be nil")

		if reopenFile {
			if err := sstable.Close(); err != nil {
				t.Fatal(err)
			}
			sstable, err = velocitylog.OpenSSTable(testFileName)
			assert.Nil(t, err)
		}
		// read deleted entry
		entry, err = sstable.Get("key2")
		assert.Nil(t, err)
		assert.Equal(t, []byte(nil), entry.Value)

		// read the non-existent key
		entry, err = sstable.Get("key6")
		assert.Nil(t, err)
		assert.Nil(t, entry)

		reopenFile = !reopenFile
		os.Remove(testFileName)
	}
}

// Test RangeScan on an SSTable
func TestSSTableRangeScan(t *testing.T) {
	t.Parallel()

	var reopenFile bool = true

	for i := 0; i < 2; i++ {
		testFileName := "TestSStableRangeScan.sst"
		// create a new memtable
		memtable := velocitylog.NewMemtable()

		populateMemtableWithTestData(memtable)

		// Write the memtable to an SSTable
		sstable, err := velocitylog.SerializeToSSTable(memtable.GenerateEntries(), testFileName)
		assert.Nil(t, err)
		defer sstable.Close()

		if reopenFile {
			if err := sstable.Close(); err != nil {
				t.Fatal(err)
			}
			sstable, err = velocitylog.OpenSSTable(testFileName)
			assert.Nil(t, err)
		}

		// Range scan the SSTable
		entries, err := sstable.RangeScan("key1", "key5")
		assert.Nil(t, err)
		assert.Equal(t, 5, len(entries))

		assert.Equal(t, []byte("value1"), entries[0].Value)
		assert.NotNil(t, entries[0].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, pb.Command_DELETE, entries[1].Command)
		assert.NotNil(t, entries[1].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, []byte("value3"), entries[2].Value)
		assert.NotNil(t, entries[2].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, pb.Command_DELETE, entries[3].Command)
		assert.NotNil(t, entries[3].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, []byte("value5"), entries[4].Value)
		assert.NotNil(t, entries[4].Timestamp, "Timestamp should not be nil")

		// cleanup
		os.Remove(testFileName)
		reopenFile = !reopenFile

	}
}

// Test RangeScan on an SSTable with a non-existent Range
func TestSSTableRangeScanNonExistentRange(t *testing.T) {
	t.Parallel()

	var reopenFile bool = true

	for i := 0; i < 2; i++ {
		testFileName := "TestSStableRangeScanNonExistentRange.sst"
		// create a new memtable
		memtable := velocitylog.NewMemtable()

		populateMemtableWithTestData(memtable)

		// Write the memtable to an SSTable
		sstable, err := velocitylog.SerializeToSSTable(memtable.GenerateEntries(), testFileName)
		assert.Nil(t, err)
		defer sstable.Close()

		if reopenFile {
			// close the file and reopen it
			if err := sstable.Close(); err != nil {
				t.Fatal(err)
			}
			sstable, err = velocitylog.OpenSSTable(testFileName)
			assert.Nil(t, err)
		}

		// Range scan the SSTable
		entries, err := sstable.RangeScan("key6", "key10")
		assert.Nil(t, err)
		assert.Nil(t, entries)

		reopenFile = !reopenFile
		// cleanup
		os.Remove(testFileName)
	}
}

// Test RangeScan on an SSTable with non-exact Range.
// func TestSSTableRangeScanNonExactRange(t *testing.T) {
// 	t.Parallel()
// 	var reopenFile bool = true
// 	for i := 0; i < 2; i++ {
// 		testFileName := "TestSStableRangeScanNonExactRange1.sst"
// 		// Create a new LSM memtable.
// 		memtable := velocitylog.NewMemtable()

// 		populateMemtableWithTestData(memtable)

// 		// Write the memtable to an SSTable.
// 		sstable, err := velocitylog.SerializeToSSTable(memtable.GenerateEntries(), testFileName)
// 		assert.Nil(t, err)
// 		defer sstable.Close()

// 		if reopenFile {
// 			if err := sstable.Close(); err != nil {
// 				t.Fatal(err)
// 			}
// 			// Open the SSTable for reading.
// 			sstable, err = velocitylog.OpenSSTable(testFileName)
// 			assert.Nil(t, err)
// 		}

// 		// Range scan the SSTable.
// 		entries, err := sstable.RangeScan("a", "z")
// 		assert.Nil(t, err)
// 		assert.Equal(t, 5, len(entries))

// 		assert.Equal(t, []byte("value1"), entries[0].Value)
// 		assert.NotNil(t, entries[0].Timestamp, "Timestamp should not be nil")

// 		assert.Equal(t, pb.Command_DELETE, entries[1].Command)
// 		assert.NotNil(t, entries[1].Timestamp, "Timestamp should not be nil")

// 		assert.Equal(t, []byte("value3"), entries[2].Value)
// 		assert.NotNil(t, entries[2].Timestamp, "Timestamp should not be nil")

// 		assert.Equal(t, pb.Command_DELETE, entries[3].Command)
// 		assert.NotNil(t, entries[3].Timestamp, "Timestamp should not be nil")

// 		assert.Equal(t, []byte("value5"), entries[4].Value)
// 		assert.NotNil(t, entries[4].Timestamp, "Timestamp should not be nil")

// 		reopenFile = !reopenFile
// 		os.Remove(testFileName)
// 	}
// }

// Test RangeScan with updated entries.
func TestRangeScanWithUpdatedEntries(t *testing.T) {
	t.Parallel()

	var reopenFile bool = true

	for i := 0; i < 2; i++ {
		testFileName := "TestRangeScanWithUpdatedEntries.sst"
		// Create a new LSM memtable.
		memtable := velocitylog.NewMemtable()

		populateMemtableWithTestData(memtable)

		// Update the memtable.
		memtable.Put("key1", []byte("value1-updated"))
		memtable.Put("key3", []byte("value3-updated"))
		memtable.Put("key5", []byte("value5-updated"))

		// Write the memtable to an SSTable.
		sstable, err := velocitylog.SerializeToSSTable(memtable.GenerateEntries(), testFileName)
		assert.Nil(t, err)
		defer sstable.Close()

		if reopenFile {
			if err := sstable.Close(); err != nil {
				t.Fatal(err)
			}
			// Open the SSTable for reading.
			sstable, err = velocitylog.OpenSSTable(testFileName)
			assert.Nil(t, err)
		}

		// Range scan the SSTable.
		entries, err := sstable.RangeScan("key1", "key5")
		assert.Nil(t, err)
		assert.Equal(t, 5, len(entries))

		assert.Equal(t, []byte("value1-updated"), entries[0].Value)
		assert.NotNil(t, entries[0].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, pb.Command_DELETE, entries[1].Command)
		assert.NotNil(t, entries[1].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, []byte("value3-updated"), entries[2].Value)
		assert.NotNil(t, entries[2].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, pb.Command_DELETE, entries[3].Command)
		assert.NotNil(t, entries[3].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, []byte("value5-updated"), entries[4].Value)
		assert.NotNil(t, entries[4].Timestamp, "Timestamp should not be nil")

		reopenFile = !reopenFile
		os.Remove(testFileName)
	}
}

// Test RangeScan on an SSTable with non-exact Range.
func TestRangeScanNonExactRange2(t *testing.T) {
	t.Parallel()

	var reopenFile bool = true

	for i := 0; i < 2; i++ {
		testFileName := "TestRangeScanNonExactRange2.sst"

		// Create a new LSM memtable.
		memtable := velocitylog.NewMemtable()

		populateMemtableWithTestData(memtable)

		// Write the memtable to an SSTable.
		sstable, err := velocitylog.SerializeToSSTable(memtable.GenerateEntries(), testFileName)
		assert.Nil(t, err)
		defer sstable.Close()

		if reopenFile {
			if err := sstable.Close(); err != nil {
				t.Fatal(err)
			}
			// Open the SSTable for reading.
			sstable, err = velocitylog.OpenSSTable(testFileName)
			assert.Nil(t, err)
		}

		// Range scan the SSTable.
		entries, err := sstable.RangeScan("z", "za")
		assert.Nil(t, err)
		assert.Equal(t, 0, len(entries))

		reopenFile = !reopenFile
		os.Remove(testFileName)
	}
}

// Test ReadAll on an SSTable.
func TestGetEntries(t *testing.T) {
	t.Parallel()

	var reopenFile bool = true

	for i := 0; i < 2; i++ {
		testFileName := "TestGetEntries.sst"

		// Create a new LSM memtable.
		memtable := velocitylog.NewMemtable()

		populateMemtableWithTestData(memtable)

		// Write the memtable to an SSTable.
		sstable, err := velocitylog.SerializeToSSTable(memtable.GenerateEntries(), testFileName)
		assert.Nil(t, err)
		defer sstable.Close()

		if reopenFile {
			if err := sstable.Close(); err != nil {
				t.Fatal(err)
			}
			// Open the SSTable for reading.
			sstable, err = velocitylog.OpenSSTable(testFileName)
			assert.Nil(t, err)
		}

		// Read all entries from the SSTable.
		entries, err := sstable.GetEntries()
		assert.Nil(t, err)
		assert.Equal(t, 5, len(entries))

		assert.Equal(t, []byte("value1"), entries[0].Value)
		assert.NotNil(t, entries[0].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, pb.Command_DELETE, entries[1].Command)
		assert.NotNil(t, entries[1].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, []byte("value3"), entries[2].Value)
		assert.NotNil(t, entries[2].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, pb.Command_DELETE, entries[3].Command)
		assert.NotNil(t, entries[3].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, []byte("value5"), entries[4].Value)
		assert.NotNil(t, entries[4].Timestamp, "Timestamp should not be nil")

		reopenFile = !reopenFile
		os.Remove(testFileName)
	}
}

func TestSSTableIterator(t *testing.T) {
	t.Parallel()

	var reopenFile bool = true

	for i := 0; i < 2; i++ {
		testFileName := "TestSSTableIterator.sst"

		// Create a new LSM memtable.
		memtable := velocitylog.NewMemtable()

		populateMemtableWithTestData(memtable)

		// Write the memtable to an SSTable.
		sstable, err := velocitylog.SerializeToSSTable(memtable.GenerateEntries(), testFileName)
		assert.Nil(t, err)
		defer sstable.Close()

		if reopenFile {
			if err := sstable.Close(); err != nil {
				t.Fatal(err)
			}
			// Open the SSTable for reading.
			sstable, err = velocitylog.OpenSSTable(testFileName)
			assert.Nil(t, err)
		}

		// Iterate over the SSTable.
		iter := sstable.Front()
		defer iter.Close()

		// Read all entries from the SSTable.
		entries := make([]*pb.LSMEntry, 0)
		for iter != nil {
			entries = append(entries, iter.Value)
			iter = iter.Next()
		}
		assert.Equal(t, 5, len(entries))

		assert.Equal(t, "value1", string(entries[0].Value))
		assert.NotNil(t, entries[0].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, pb.Command_DELETE, entries[1].Command)
		assert.NotNil(t, entries[1].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, "value3", string(entries[2].Value))
		assert.NotNil(t, entries[2].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, pb.Command_DELETE, entries[3].Command)
		assert.NotNil(t, entries[3].Timestamp, "Timestamp should not be nil")

		assert.Equal(t, "value5", string(entries[4].Value))
		assert.NotNil(t, entries[4].Timestamp, "Timestamp should not be nil")

		reopenFile = !reopenFile
		os.Remove(testFileName)
	}
}

func populateMemtableWithTestData(memtable *velocitylog.Memtable) {
	memtable.Put("key1", []byte("value1"))
	memtable.Put("key2", []byte("value2"))
	memtable.Put("key3", []byte("value3"))
	memtable.Put("key4", []byte("value4"))
	memtable.Put("key5", []byte("value5"))

	memtable.Delete("key2")
	memtable.Delete("key4")
}
