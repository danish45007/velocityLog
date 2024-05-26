package velocitylog

import (
	"context"
	sync "sync"
	"time"

	wal "github.com/danish45007/GoLogMatrix"
)

// level represents a level in the LSM tree.
type level struct {
	sstablesLock sync.RWMutex // Lock to protect the sstables.
	sstables     []*SSTable   // List of SSTables in the level.
}

// KVPair struct represents a key-value pair for upstream applications.
type KVPair struct {
	Key   string
	Value []byte
}
type LSMTree struct {
	memLock         sync.RWMutex // Lock to protect the memtable.
	memtable        *Memtable
	maxMemtableSize int64              // Maximum size of the memtable in bytes before flushing to ssTable.
	directory       string             // Directory to store the SSTable files.
	wal             *wal.WAL           // Write-ahead log for the LSM tree.
	inRecovery      bool               // Flag to indicate if the LSM tree is recovering entries from the WAL.
	levels          []*level           // List of levels in the LSM tree.
	currentSSTSeq   uint64             // Next sequence number for the SSTable.
	compactionChan  chan int           // Channel for triggering compaction at a level.
	flushingLock    sync.RWMutex       // Lock to protect the flushing queue.
	flushingQueue   []*Memtable        // Queue of memtables to be flushed to SSTable. Used to serve reads while flushing.
	flushingChan    chan *Memtable     // Channel for triggering flushing of memtables to SSTables.
	ctx             context.Context    // Context for the LSM tree.
	cancel          context.CancelFunc // Cancel function for the context.
	wg              sync.WaitGroup     // WaitGroup for the LSM tree.
}

///////////////////////////////////////////////////////////////////////////////////
// Public API for LSM Tree
///////////////////////////////////////////////////////////////////////////////////

// Opens a LSMTree. if the directory does not exist, it will be created.
// directory:  is used to store the SSTable files.
// maxMemtableSize: is the maximum size of the memtable in bytes before flushing to SSTable.
// recoveryFromWAL: is a flag to indicate if the LSM tree should recover entries from the WAL on startup.
// On Startup, the LSM tree will load all SSTables handles (if any) from disk into memory.

func Open(directory string, maxMemtableSize int64, recoveryFromWAL bool) (*LSMTree, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// setup the WAL for LSM tree.
	wal, err := wal.OpenWAL(directory+WALDirectorySuffix, true, WALMaxFileSize, WALMaxSegments)

	if err != nil {
		cancel()
		return nil, err
	}

	// create levels for the LSM tree.
	levels := make([]*level, MaxLevels)
	// initialize the levels with empty SSTables.
	for i := 0; i < MaxLevels; i++ {
		levels[i] = &level{
			sstables: make([]*SSTable, 0),
		}
	}

	lsm := &LSMTree{
		memtable:        NewMemtable(),
		maxMemtableSize: maxMemtableSize,
		directory:       directory,
		wal:             wal,
		inRecovery:      false,
		currentSSTSeq:   0,
		levels:          levels,
		compactionChan:  make(chan int, 100),
		flushingQueue:   make([]*Memtable, 0),
		flushingChan:    make(chan *Memtable, 100),
		ctx:             ctx,
		cancel:          cancel,
	}
	// load the SSTables from disk into memory.
	if err := lsm.loadSSTables(); err != nil {
		return nil, err
	}
	lsm.wg.Add(2)
	// start the background processes for compaction and memtable flushing.
	go lsm.backgroundCompaction()
	go lsm.backgroundMemtableFlushing()

	// recover any entries that were written into WAL but not flushed to SSTable.
	if recoveryFromWAL {
		if err := lsm.recoverFromWAL(); err != nil {
			return nil, err
		}
	}
	return lsm, nil
}

func (l *LSMTree) Close() error {
	// acquire the lock to protect the memtable.
	l.memLock.Lock()
	// acquire the lock to protect the flushing queue.
	l.flushingLock.Lock()
	// add the current memtable to the flushing queue.
	l.flushingQueue = append(l.flushingQueue, l.memtable)
	//
	l.flushingLock.Unlock()
	// signal the background memtable flushing process to flush the memtable.
	l.flushingChan <- l.memtable
	// initialize a new memtable.
	l.memtable = NewMemtable()
	// release the lock to protect the memtable.
	l.memLock.Unlock()

	// cancel the context.
	l.cancel()
	// wait for the background processes to finish.
	l.wg.Wait()
	// close the WAL.
	if err := l.wal.Close(); err != nil {
		return err
	}
	// close te channels
	close(l.flushingChan)
	close(l.compactionChan)
	return nil
}

// Put, Insert a key-value pair into the LSM tree.
func (l *LSMTree) Put(key string, value []byte) error {
	// acquire the lock to protect the memtable
	l.memLock.Lock()
	defer l.memLock.Unlock()

	// Write to WAL before to memtable. And we don't need to write entries to WAL if we are recovering from WAL.
	if !l.inRecovery {
		marshalEntry := MarshalEntry(&WALEntry{
			Key:       key,
			Command:   Command_PUT,
			Value:     value,
			Timestamp: time.Now().UnixNano(),
		})
		l.wal.WriteEntity(marshalEntry)
	}
	// insert the key-value pair into the memtable.
	l.memtable.Put(key, value)

	// check if the memtable has reached the maximum size and need to be flushed to SSTable.
	if l.memtable.SizeInBytes() > l.maxMemtableSize {
		// acquire the lock to protect the flushing queue.
		l.flushingLock.Lock()
		// add the current memtable to the flushing queue.
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		// release the lock to protect the flushing queue.
		l.flushingLock.Unlock()
		// signal the background memtable flushing process to flush the memtable.
		l.flushingChan <- l.memtable
		// initialize a new memtable.
		l.memtable = NewMemtable()
	}
	return nil
}

// Delete, Delete a key from the LSM tree.
func (l *LSMTree) Delete(key string) error {
	// acquire the lock to protect the memtable.
	l.memLock.Lock()
	defer l.memLock.Unlock()

	// Write to WAL before to memtable. And we don't need to write entries to WAL if we are recovering from WAL.
	if !l.inRecovery {
		marshalEntry := MarshalEntry(&WALEntry{
			Key:       key,
			Command:   Command_DELETE,
			Timestamp: time.Now().UnixNano(),
		})
		l.wal.WriteEntity(marshalEntry)
	}
	// insert a tombstone for the key in the memtable.
	l.memtable.Delete(key)

	// check if the memtable has reached the maximum size and need to be flushed to SSTable.
	if l.memtable.SizeInBytes() > l.maxMemtableSize {
		// acquire the lock to protect the flushing queue.
		l.flushingLock.Lock()
		// add the current memtable to the flushing queue.
		l.flushingQueue = append(l.flushingQueue, l.memtable)
		// release the lock to protect the flushing queue.
		l.flushingLock.Unlock()
		// signal the background memtable flushing process to flush the memtable.
		l.flushingChan <- l.memtable
		// initialize a new memtable.
		l.memtable = NewMemtable()
	}
	return nil
}

// Get, Retrieve a value for a given key from the LSM tree.
// if the key is not found returns nil, otherwise returns the value.
// it will search the memtable first, then search the SSTables.

func (l *LSMTree) Get(key string) ([]byte, error) {
	// acquire a read lock on memtable.
	l.memLock.RLock()
	// search the memtable for the key.
	value := l.memtable.Get(key)
	if value != nil {
		l.memLock.RUnlock()
		return processAndReturnEntry(value)
	}
	l.memLock.RUnlock()

	// search in the flush queue.
	l.flushingLock.RLock()
	// search in reverse order to look for the most recent memtable.
	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		value = l.flushingQueue[i].Get(key)
		if value != nil {
			l.flushingLock.RUnlock()
			return processAndReturnEntry(value)
		}
	}
	l.flushingLock.RUnlock()

	// search in the SSTables.
	// iterate through all the levels in the LSM tree.
	for level := range l.levels {
		// acquire a read lock on the level.
		l.levels[level].sstablesLock.RLock()
		// iterate through all the SSTables in the level in reverse order.
		for i := len(l.levels[level].sstables) - 1; i >= 0; i-- {
			// search for the key in the SSTable.
			value, err := l.levels[level].sstables[i].Get(key)
			if err != nil {
				l.levels[level].sstablesLock.RUnlock()
				return nil, err
			}
			if value != nil {
				l.levels[level].sstablesLock.RUnlock()
				return processAndReturnEntry(value)
			}
		}
		l.levels[level].sstablesLock.RUnlock()
	}
	return nil, nil
}

// RangeScan, returns all key-value pairs in the LSM tree within the given key range [startKey, endKey]
// the entries are returned in sorted order

func (l *LSMTree) RangeScan(startKey string, endKey string) ([]KVPair, error) {
	ranges := [][]*LSMEntry{}
	// acquire all the locks together to ensure a consistent view of the LSM tree for the range scan.

	// acquire a read lock on the memtable.
	l.memLock.RLock()
	defer l.memLock.RUnlock()

	// acquire a read lock on all the levels in the LSM tree.
	for _, level := range l.levels {
		level.sstablesLock.RLock()
		defer level.sstablesLock.RUnlock()
	}

	// acquire a read lock on the flushing queue.
	l.flushingLock.RLock()
	defer l.flushingLock.RUnlock()

	// search in the memtable.
	entries := l.memtable.RangeScan(startKey, endKey)
	// add the entries to the ranges.
	if len(entries) > 0 {
		ranges = append(ranges, entries)
	}

	// search in the flushing queue.
	for i := len(l.flushingQueue) - 1; i >= 0; i-- {
		entries = l.flushingQueue[i].RangeScan(startKey, endKey)
		if len(entries) > 0 {
			ranges = append(ranges, entries)
		}
	}

	// search in the SSTables.
	// iterate through all the levels in the LSM tree.
	for _, level := range l.levels {
		// iterate through all the SSTables in the level in reverse order.
		for i := len(level.sstables) - 1; i >= 0; i-- {
			// search for the key in the SSTable.
			entries, err := level.sstables[i].RangeScan(startKey, endKey)
			if err != nil {
				return nil, err
			}
			if len(entries) > 0 {
				ranges = append(ranges, entries)
			}
		}
	}
	return mergeRanges(ranges), nil
}
