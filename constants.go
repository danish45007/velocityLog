package velocitylog

const (
	BloomFilterSize    = 1000000    // 1 million bits.
	InitialOffset      = 0          // Initial offset for the data entries.
	SSTableFilePrefix  = "sstable_" // Prefix for SSTable files.
	WALDirectorySuffix = "_wal"     // Suffix for WAL directory.
	MaxLevels          = 6          // Maximum number of levels in the LSM tree.
	WALMaxFileSize     = 128000     // 128 KB
	WALMaxSegments     = 1000       // Maximum number of WAL segments.
)
