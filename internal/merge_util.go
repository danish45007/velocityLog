package velocitylog

import (
	"container/heap"

	pb "github.com/danish45007/velocitylog/proto"
	"github.com/huandu/skiplist"
)

// heap entry for k-way merge algorithm
type heapEntry struct {
	entry     *pb.LSMEntry
	listIndex int              //index of entry source
	index     int              //index of entry in the list
	iterator  *SSTableIterator //iterator for the entry
}

// heap implementation for k-way merge algorithm
type mergeHeap []heapEntry

//////////////////////////////////////////////////////////////////////
// Helper functions for the heap implementation
//////////////////////////////////////////////////////////////////////
// Len returns the length of the heap
func (h mergeHeap) Len() int {
	return len(h)
}

// minHeap based on the timestamp of the entries
// the smallest timestamp will be at the top of the heap
// Less, returns true if the timestamp of the entry at index i is less than the timestamp of the entry at index j
func (h mergeHeap) Less(i, j int) bool {
	return h[i].entry.Timestamp < h[j].entry.Timestamp
}

// Swap two entries in the heap
func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push an entry into the heap
func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(heapEntry))

}

// Pop the min entry from the heap
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	*h = old[0 : n-1]
	return entry
}

// mergeRanges, performs k-way merge on the list of possible overlapping ranges and merge
// them into a single sorted list of key-value pairs without duplicates.
// Deduplication is done by keeping track of the most recent entry for a key and discarding
// the older entries using the timestamp.
func mergeRanges(ranges [][]*pb.LSMEntry) []KVPair {
	// create a min heap
	minHeap := &mergeHeap{}
	// initialize the heap
	heap.Init(minHeap)

	var results []KVPair

	// keep track of the most recent entry for each key, in sorted order of keys
	seen := skiplist.New(skiplist.String)

	// add the first element from each range to the heap
	for i, rangeEntries := range ranges {
		if len(rangeEntries) > 0 {
			heap.Push(minHeap, heapEntry{entry: rangeEntries[0], listIndex: i, index: 0})
		}
	}

	// check if there are entries in the heap
	for minHeap.Len() > 0 {
		// pop the min entry from the heap
		minEntry := heap.Pop(minHeap).(heapEntry)
		previousEntry := seen.Get(minEntry.entry.Key)

		// check if the key has been seen before
		if previousEntry != nil {
			// if the previous entry has a smaller timestamp, then replace
			// the previous entry with the current entry
			if previousEntry.Value.(heapEntry).entry.Timestamp < minEntry.entry.Timestamp {
				seen.Set(minEntry.entry.Key, minEntry.entry)
			}
		} else {
			// add the entry to the seen list
			seen.Set(minEntry.entry.Key, minEntry)
		}
		// add the next element from the same list to the heap
		if minEntry.index+1 < len(ranges[minEntry.listIndex]) {
			heap.Push(minHeap, heapEntry{entry: ranges[minEntry.listIndex][minEntry.index+1], listIndex: minEntry.listIndex, index: minEntry.index + 1})
		}
	}
	// iterate through the seen list and add the entries to the results
	iter := seen.Front()
	for iter != nil {
		entry := iter.Value.(heapEntry)
		// check the command field in the LSMEntry to determine if the entry is a tombstone or not.
		if entry.entry.Command == pb.Command_DELETE {
			iter = iter.Next()
			continue
		}
		results = append(results, KVPair{Key: entry.entry.Key, Value: entry.entry.Value})
		iter = iter.Next()
	}
	return results
}

// Performs a k-way merge on SSTable iterators of possibly overlapping ranges
// and merges them into a single range without any duplicate entries.
// Deduplication is done by keeping track of the most recent entry for each key
// and discarding the older ones using the timestamp.
func mergeIterators(iterators []*SSTableIterator) []*pb.LSMEntry {
	minHeap := &mergeHeap{}
	heap.Init(minHeap)

	var results []*pb.LSMEntry

	// Keep track of the most recent entry for each key, in sorted order of keys.
	seen := skiplist.New(skiplist.String)

	// Add the iterators to the heap.
	for _, iterator := range iterators {
		if iterator == nil {
			continue
		}
		heap.Push(minHeap, heapEntry{entry: iterator.Value, iterator: iterator})
	}

	for minHeap.Len() > 0 {
		// Pop the min entry from the heap.
		minEntry := heap.Pop(minHeap).(heapEntry)
		previousValue := seen.Get(minEntry.entry.Key)

		// Check if this key has been seen before.
		if previousValue != nil {
			// If the previous entry has a smaller timestamp, then we need to
			// replace it with the more recent entry.
			if previousValue.Value.(heapEntry).entry.Timestamp < minEntry.entry.Timestamp {
				seen.Set(minEntry.entry.Key, minEntry)
			}
		} else {
			// Add the entry to the seen list.
			seen.Set(minEntry.entry.Key, minEntry)
		}

		// Add the next element from the same list to the heap
		if minEntry.iterator.Next() != nil {
			nextEntry := minEntry.iterator.Value
			heap.Push(minHeap, heapEntry{entry: nextEntry, iterator: minEntry.iterator})
		}
	}

	// Iterate through the seen list and add the values to the results.
	iter := seen.Front()
	for iter != nil {
		entry := iter.Value.(heapEntry)
		if entry.entry.Command == pb.Command_DELETE {
			iter = iter.Next()
			continue
		}
		results = append(results, entry.entry)
		iter = iter.Next()
	}

	return results
}
