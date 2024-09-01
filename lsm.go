package velocitylog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	sync "sync"
	"sync/atomic"
	"time"

	wal "github.com/danish45007/GoLogMatrix"
)

// maxSSTableInLevel is the maximum number of SSTables in each level of the LSM tree.
// factor of 2 is used to increase the number of SSTables in each level.
// Initially, the number of SSTables in each level is 4 and in total there are 6 levels.
var maxSSTableInLevel = map[int]int{
	0: 4,
	1: 8,
	2: 16,
	3: 32,
	4: 64,
	5: 128,
	6: 256,
}

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

///////////////////////////////////////////////////////////////////////////////////
// Utility functions for LSM Tree                                                 /
///////////////////////////////////////////////////////////////////////////////////

//backgroundMemtableFlushing, is a background process that continuously listens on the flushing channel
// for memtables that need to be flushed to SSTable until the context is Done or canceled.
func (l *LSMTree) backgroundMemtableFlushing() error {
	// defer the wait group to signal the completion of the background process.
	defer l.wg.Done()
	for {
		select {
		// check if the context is done.
		case <-l.ctx.Done():
			// check if there are any memtables in the flushing c
			if len(l.flushingChan) == 0 {
				return nil
			}
		// wait for a memtable to be flushed.
		case memtable := <-l.flushingChan:
			// flush the memtable
			l.flushMemtable(memtable)
		}
	}

}

// backgroundCompaction, is a background process that continuously listens on the compaction channel
// for levels that need to be compacted until the context is Done or canceled.
// It uses Tiered compaction strategy to compact the levels in the LSM tree.

func (l *LSMTree) backgroundCompaction() error {
	// defer the wait group to signal the completion of the background process.
	defer l.wg.Done()
	for {
		select {
		case <-l.ctx.Done():
			// check if there are any levels that need to be compacted.
			if readyToExit := l.checkAndTriggerCompaction(); readyToExit {
				return nil
			}
		case compactionCandidate := <-l.compactionChan:
			// compact the level.
			l.compactLevel(compactionCandidate)
		}
	}
}

// checkAndTriggerCompaction, check of all the levels have less than the maximum number of SSTables.
// if any level has more than the maximum number of SSTables, trigger compaction for that level.
// returns true if all the levels are ready to exit, otherwise false.
func (l *LSMTree) checkAndTriggerCompaction() bool {
	readyToExit := true
	// iterate through all the levels in the LSM tree.
	for i, level := range l.levels {
		// acquire a read lock on the level
		level.sstablesLock.RLock()
		// check if the level has more than the maximum number of .
		if len(level.sstables) > maxSSTableInLevel[i] {
			// trigger compaction for the level.SSTables
			l.compactionChan <- i
			readyToExit = false
		}
		level.sstablesLock.RUnlock()
	}
	return readyToExit
}

// compactLevel, compacts the SSTables in the level using the Tiered compaction strategy.
func (l *LSMTree) compactLevel(compactionCandidate int) error {
	// check if reached the maximum number of levels in the LSM tree.
	if compactionCandidate == MaxLevels-1 {
		return nil
	}

	// lock the level while we check we need to compact.
	l.levels[compactionCandidate].sstablesLock.Lock()

	// check if the number of SSTables in the level is less than the maximum number of SSTables.
	if len(l.levels[compactionCandidate].sstables) <= maxSSTableInLevel[compactionCandidate] {
		l.levels[compactionCandidate].sstablesLock.Unlock()
		return nil
	}

	// get the iterator for the SSTables in this level to compact.
	_, iterators := l.getSSTablesAtLevel(compactionCandidate)

	// we are free to release the lock on the level now while we process the SSTables.
	//This is safe because these SSTables are immutable and will be deleted by this method
	// which is single-threaded and will not be accessed by any other goroutine.

	l.levels[compactionCandidate].sstablesLock.Unlock()

	// merge all SSTables into a single SSTable.
	mergedSSTable, err := l.mergeSSTables(iterators, compactionCandidate+1)
	if err != nil {
		return err
	}
	// acquire a write lock on the level and next level.
	l.levels[compactionCandidate].sstablesLock.Lock()
	l.levels[compactionCandidate+1].sstablesLock.Lock()

	// delete the old SSTables from the level.
	l.deleteSSTableAtLevel(compactionCandidate, iterators)

	// add new SSTable to the next level.
	l.addSSTableAtLevel(mergedSSTable, compactionCandidate+1)

	// release the lock on the level and next level.
	l.levels[compactionCandidate].sstablesLock.Unlock()
	l.levels[compactionCandidate+1].sstablesLock.Unlock()

	return nil
}

// loadSSTables, loads all the SSTables from disk into memory.
// sorts the SSTables based on the sequence number, method is called on startup.
func (l *LSMTree) loadSSTables() error {
	if err := os.Mkdir(l.directory, 0755); err != nil {
		return err
	}
	if err := l.loadSSTablesFromDisk(); err != nil {
		return err
	}
	l.sortSSTablesBySequenceNumber()
	l.initializeCurrentSequenceNumber()
	return nil
}

// loadSSTablesFromDisk, loads all the all the files from disk into memory.
// that have the SSTableFilePrefix.
func (l *LSMTree) loadSSTablesFromDisk() error {
	files, err := os.ReadDir(l.directory)
	if err != nil {
		return err
	}
	for _, file := range files {
		// check if the file is a directory or not an SSTable file.
		if file.IsDir() || !isSSTableFile(file.Name()) {
			continue
		}
		sstable, err := OpenSSTable(l.directory + "/" + file.Name())
		if err != nil {
			return err
		}
		level := l.getLevelFromSSTableFilename(sstable.file.Name())
		l.levels[level].sstables = append(l.levels[level].sstables, sstable)
	}
	return nil
}

// recoverFromWAL, reads all the entries from WAL, after the last checkpoint
// It will give us all the entries that were written to the memtable but not flushed to SSTable.
func (l *LSMTree) recoverFromWAL() error {
	l.inRecovery = true
	defer func() {
		l.inRecovery = false
	}()
	// read all entries from WAL, after the last checkpoint.
	entries, err := l.readEntriesFromWAL()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		// process the entry
		if err := l.processWALEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

// mergeSSTables, runs merge on the iterator and create a new SSTables with the merged entries.
func (l *LSMTree) mergeSSTables(iterators []*SSTableIterator, targetLevel int) (*SSTable, error) {
	mergedEntries := mergeIterators(iterators)

	sstableFileName := l.getSSTableFilename(targetLevel)

	sst, err := SerializeToSSTable(mergedEntries, sstableFileName)
	if err != nil {
		return nil, err
	}
	return sst, nil
}

// getSSTablesAtLevel, returns the sstables and its iterators at the given level. (Not Thread-Safe Implementation)
func (l *LSMTree) getSSTablesAtLevel(level int) ([]*SSTable, []*SSTableIterator) {
	// get the sstables at the level
	sstables := l.levels[level].sstables
	// create an iterator for each SSTable.
	iterators := make([]*SSTableIterator, len(sstables))
	// iterate through all the SSTables in the level.
	for i, sstable := range sstables {
		iterators[i] = sstable.Front()
	}
	return sstables, iterators

}

// deleteSSTableAtLevel, deletes the SSTables from the level. (Not Thread-Safe Implementation)
func (l *LSMTree) deleteSSTableAtLevel(level int, iterators []*SSTableIterator) {
	// remove the SSTables from the level.
	l.levels[level].sstables = l.levels[level].sstables[len(iterators):]
	// delete the SSTables from disk.
	for _, iterator := range iterators {
		// delete the file pointed by the iterator.
		if err := os.Remove(iterator.sst.file.Name()); err != nil {
			panic(err)
		}
	}
}

// addSSTableAtLevel, adds the SSTable to the level. (Not Thread-Safe Implementation)
func (l *LSMTree) addSSTableAtLevel(sstable *SSTable, level int) {
	l.levels[level].sstables = append(l.levels[level].sstables, sstable)
	// send a signal on the compaction channel that a new sstable has been added to the level.
	l.compactionChan <- level
}

// flushMemtable, flushes the memtable to an on-disk SSTable.
func (l *LSMTree) flushMemtable(memtable *Memtable) {
	if memtable.size == 0 {
		return
	}
	// increment the sequence number with atomic operation.
	// the sequence number is shared across multiple goroutines.
	// and we need to ensure that the sequence number is unique.
	atomic.AddUint64(&l.currentSSTSeq, 1)
	// get the filename for the SSTable.
	sstableFileName := l.getSSTableFilename(0)
	// serialize the memtable to an SSTable.
	sst, err := SerializeToSSTable(memtable.GenerateEntries(), sstableFileName)
	if err != nil {
		panic(err)
	}
	// acquire a write lock on the level.
	l.levels[0].sstablesLock.Lock()
	// acquire lock on flush queue.
	l.flushingLock.Lock()

	// create a wal checkpoint for the SSTable.
	l.wal.CreateCheckPoint(
		MarshalEntry(&WALEntry{
			Key:       sstableFileName,
			Command:   Command_WRITE_SST,
			Timestamp: time.Now().UnixNano(),
		}))

	// add the SSTable to the level.
	l.levels[0].sstables = append(l.levels[0].sstables, sst)
	// remove the memtable from the flushing queue.
	l.flushingQueue = l.flushingQueue[1:] // remove the first element. (FIFO)
	// release the lock on the flush queue.
	l.flushingLock.Unlock()
	// release the lock on the level.
	l.levels[0].sstablesLock.Unlock()
	// send a signal on the compaction channel that a new sstable has been added to the level.
	l.compactionChan <- 0
}

// readEntriesFromWAL, reads all the entries from WAL after the last checkpoint.
// returns all the entries that were written to the memtable but not flushed to SSTable.
func (l *LSMTree) readEntriesFromWAL() ([]*wal.WAL_Entry, error) {
	entries, err := l.wal.ReadAllFromOffset(-1, true)
	if err != nil {
		// attempt to repair the WAL if it is corrupted.
		_, err := l.wal.Repair()
		if err != nil {
			return nil, err
		}
		// read all entries from WAL after the last checkpoint.
		entries, err = l.wal.ReadAllFromOffset(-1, true)
		if err != nil {
			return nil, err
		}
	}
	return entries, nil
}

// processWALEntry, process the WAL entry, it is used to recover the entries from WAL.
// it reads the wal entries and performs the corresponding operation on the LSM tree.
func (l *LSMTree) processWALEntry(entry *wal.WAL_Entry) error {
	// check if the entry is a checkpoint and skip it.
	if entry.GetIsCheckPoint() {
		// NOTE: we may use this checkpoint entry to recover in more sophisticated scenarios.
		return nil
	}
	walEntry := WALEntry{}
	// unmarshal the entry.
	UnmarshalEntry(entry.GetData(), &walEntry)

	// process the entry based on the command.
	switch walEntry.Command {
	case Command_PUT:
		return l.Put(walEntry.Key, walEntry.Value)
	case Command_DELETE:
		return l.Delete(walEntry.Key)
	case Command_WRITE_SST:
		return errors.New("unexpected SSTable write entry in WAL")
	default:
		return errors.New("unknown command in WAL entry")
	}
}

// sortSSTablesBySequenceNumber, sorts the SSTables in each level based on the sequence number.
func (l *LSMTree) sortSSTablesBySequenceNumber() {
	for _, level := range l.levels {
		sort.Slice(level.sstables, func(i, j int) bool {
			iSequence := l.getSequenceNumber(level.sstables[i].file.Name())
			jSequence := l.getSequenceNumber(level.sstables[j].file.Name())
			return iSequence < jSequence
		})
	}
}

// returns the sequence number from the sstable filename.
// example: sstable_0_123 -> 123
func (l *LSMTree) getSequenceNumber(filename string) uint64 {
	// directory + "/" + sstable_ + level (single digit) + _ + 1
	sequenceStr := filename[len(l.directory)+1+2+len(SSTableFilePrefix):]
	sequence, err := strconv.ParseUint(sequenceStr, 10, 64)
	if err != nil {
		panic(err)
	}
	return sequence
}

// Get the level from the SSTable filename.
// Example: sstable_0_123 -> 0
func (l *LSMTree) getLevelFromSSTableFilename(filename string) int {
	// directory + "/" + sstable_ + level (single digit) + _ + 1
	levelStr := filename[len(l.directory)+1+len(SSTableFilePrefix) : len(l.directory)+2+len(SSTableFilePrefix)]
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		panic(err)
	}
	return level
}

// Set the current_sst_sequence to the maximum sequence number found in any of
// the SSTables.
func (l *LSMTree) initializeCurrentSequenceNumber() error {
	var maxSequence uint64
	for _, level := range l.levels {
		if len(level.sstables) > 0 {
			lastSSTable := level.sstables[len(level.sstables)-1]
			sequence := l.getSequenceNumber(lastSSTable.file.Name())
			if sequence > maxSequence {
				maxSequence = sequence
			}
		}
	}

	atomic.StoreUint64(&l.currentSSTSeq, maxSequence)

	return nil
}

// Get the filename for the next SSTable.
func (l *LSMTree) getSSTableFilename(level int) string {
	return fmt.Sprintf("%s/%s%d_%d", l.directory, SSTableFilePrefix, level, atomic.LoadUint64(&l.currentSSTSeq))
}
