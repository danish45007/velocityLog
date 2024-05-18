package velocitylog

import (
	"context"
	sync "sync"

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
	flushingLock    sync.Mutex         // Lock to protect the flushing queue.
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
