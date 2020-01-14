package types

type StoreKey interface {
	Name() string
	String() string
}

type Serializable interface {
	ToBytes() []byte
	FromBytes([]byte)
}

type ObjIterator interface {
	Domain() (start []byte, end []byte)
	Valid() bool
	Next()
	Key() (key []byte)
	Value() (value []byte)
	ObjValue(ptr *Serializable)
	Close()
}

type CacheStatus int
const (
	//nolint
	Missed CacheStatus = 0
	Hit CacheStatus = 1
	JustDeleted CacheStatus = -1
)

type KObjStore interface {
	Get(key []byte) []byte
	GetObj(key []byte, ptr *Serializable)
	GetReadOnlyObj(key []byte, ptr *Serializable)
	Has(key []byte) bool
	Iterator(start, end []byte) ObjIterator
	ReverseIterator(start, end []byte) ObjIterator

	Set(key, value []byte)
	SetObj(key []byte, obj Serializable)
	Delete(key []byte)
}

type BaseStore interface {
	Get(key []byte) []byte
	GetObj(key []byte, ptr *Serializable)
	GetObjForOverlay(key []byte, ptr *Serializable)
	GetReadOnlyObj(key []byte, ptr *Serializable)
	Has(key []byte) bool
	Iterator(start, end []byte) ObjIterator
	ReverseIterator(start, end []byte) ObjIterator

	SetAsync(key, value []byte)
	SetObjAsync(key []byte, obj Serializable)
	DeleteAsync(key []byte)
	Flush()
}

type MultiStore interface {
	BaseStore
	SubStore(StoreKey) KObjStore
	Cached() MultiStore
}

type RootStore interface {
	BaseStore
	Cached() MultiStore
	SetHeight(int)

	GetRootHash() []byte

	//SetPruning(PruningOptions)
	// Mount a store of type using the given db.
	// If db == nil, the new store will use the CommitMultiStore db.
	//MountStoreWithDB(key StoreKey, typ StoreType, db dbm.DB)
	// Load the latest persisted version.  Called once after all
	// calls to Mount*Store() are complete.
	//LoadLatestVersion() error
}

