package velocitylog

// Checks if the given entry is a tombstone if the command is DELETE return nil otherwise return the entry.
func processAndReturnEntry(entry *LSMEntry) ([]byte, error) {
	// Check if the entry is a tombstone.
	if entry.Command == Command_DELETE {
		return nil, nil
	}
	return entry.Value, nil
}

// Checks if the given filename is an SSTable file.
func isSSTableFile(filename string) bool {
	return filename[:len(SSTableFilePrefix)] == SSTableFilePrefix
}
