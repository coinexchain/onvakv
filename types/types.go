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

type UpdateTask struct {
	TaskKind  int
	PrevEntry *Entry
	CurrEntry *Entry
	Key       []byte
	Value     []byte
}


type IndexTree interface {
	Init(dirname string, repFn func(string)) error
	BeginWrite(height int64)
	EndWrite()
	Iterator(start, end []byte) Iterator
	ReverseIterator(start, end []byte) Iterator
	Get(k []byte) (uint64, bool)
	GetAtHeight(k []byte, height uint64) (uint64, bool)
	Set(k []byte, v uint64)
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
	EndBlock() []byte
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

