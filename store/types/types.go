package types

type StoreKey interface {
	Name() string
	Prefix() string
}

type Serializable interface {
	ToBytes() []byte
	FromBytes([]byte)
	DeepCopy() interface{}
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
	Missed      CacheStatus = 0
	Hit         CacheStatus = 1
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

