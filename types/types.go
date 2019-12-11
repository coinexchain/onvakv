package types

type Entry struct {
	Key        []byte
	Value      []byte
	NextKey    []byte
	Height     int64
	LastHeight int64
	SerialNum  int64
}

type Iterator interface {
	Domain() (start []byte, end []byte)
	Valid() bool
	Next()
	Key() []byte
	Value() uint64
	Close()
}

type IndexTree interface {
	// initialize the internal data structure
	Init(dirname string, repFn func(string)) error
	// begin the write phase, during which no reading is permitted
	BeginWrite()
	// end the write phase, and mark the corresponding height
	EndWrite(height int64)
	// Iterator over a domain of keys in ascending order. End is exclusive.
	// Start must be less than end, or the Iterator is invalid.
	// Iterator must be closed by caller.
	// To iterate over entire domain, use store.Iterator(nil, nil)
	// Can NOT be used in in write phase
	Iterator(start, end []byte) Iterator
	// Iterator over a domain of keys in descending order. End is exclusive.
	// Start must be less than end, or the Iterator is invalid.
	// Iterator must be closed by caller.
	// Can NOT be used in in write phase
	ReverseIterator(start, end []byte) Iterator
	// Query the KV-pair, when it is NOT in write phase. Panics on nil key.
	// Get can be invoked from many goroutines concurrently
	Get(k []byte) (uint64, bool)
	// Set sets the key. Panics on nil key.
	// Set and Delete can be invoked from only one goroutine
	Set(k []byte, v uint64)
	// Delete deletes the key. Panics on nil key.
	Delete(k []byte)
}

type DataTree interface {
	DeactiviateEntry(sn int64)
	AppendEntry(entry *Entry) uint64
	ReadEntry(pos uint64) *Entry
	GetActiveBit(sn int64) bool
	DeleteActiveTwig(twigID int64)
	GetActiveEntriesInTwig(twigID int64) []*Entry
	TwigCanBePruned(twigID int64) bool
	PruneTwigs(startID, endID int64) []byte
	GetFileSizes() (int64, int64)
	EndBlock()
}

type MetaDB interface {
	Commit()
	SetCurrHeight(h int64)
	GetCurrHeight() int64

	SetTwigMtFileSize(size int64)
	GetTwigMtFileSize() int64

	SetEntryFileSize(size int64)
	GetEntryFileSize() int64

	GetTwigHeight(twigID int64) int64
	DeleteTwigHeight(twigID int64)

	SetLastPrunedTwig(twigID int64)
	GetLastPrunedTwig() int64

	GetEdgeNodes() []byte
	SetEdgeNodes(bz []byte)

	// MaxSerialNum is the maximum serial num among all the entries
	GetMaxSerialNum() int64
	IncrMaxSerialNum() // It should call setTwigHeight(twigID int64, height int64)

	// the count of all the active entries, increased in AppendEntry, decreased in DeactiviateEntry
	GetActiveEntryCount() int64
	IncrActiveEntryCount()
	DecrActiveEntryCount()

	// the ID of the oldest active twig, increased by ReapOldestActiveTwig
	GetOldestActiveTwigID() int64
	IncrOldestActiveTwigID()
}

