package tests

import (
	"testing"

	velocitylog "github.com/danish45007/velocitylog"
	"github.com/stretchr/testify/assert"
)

func TestMemtablePutGetDelete(t *testing.T) {
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
