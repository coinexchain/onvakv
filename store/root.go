package store

import (
	"reflect"

	dbm "github.com/tendermint/tm-db"

	"github.com/coinexchain/onvakv/store/types"
	"github.com/coinexchain/onvakv"
	onvatypes "github.com/coinexchain/onvakv/types"
)

// TODO: add guard kv pairs

const CacheSizeLimit = 1024*1024

type OnvaRootStore struct {
	cache     map[string]types.Serializable
	okv       *onvakv.OnvaKV
	tasks     []onvatypes.UpdateTask
	height    int64
	storeKeys map[types.StoreKey]struct{}
}

var _ types.BaseStore = (*OnvaRootStore)(nil)

func (root *OnvaRootStore) SetHeight(h int64) {
	root.height = h
}

func (root *OnvaRootStore) Get(key []byte) []byte {
	return root.okv.Get(key)
}
func (root *OnvaRootStore) GetObjForOverlay(key []byte, ptr *types.Serializable) {
	root.GetObj(key, ptr)
}
func (root *OnvaRootStore) GetObj(key []byte, ptr *types.Serializable) {
	obj, ok := root.cache[string(key)]
	if ok {
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(obj))
		root.cache[string(key)] = obj.DeepCopy().(types.Serializable)
	} else if bz := root.okv.Get(key); bz != nil {
		(*ptr).FromBytes(bz)
	} else {
		*ptr = nil
	}
}
func (root *OnvaRootStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
	obj, ok := root.cache[string(key)]
	if ok {
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(obj))
	} else if bz := root.okv.Get(key); bz != nil {
		(*ptr).FromBytes(bz)
	} else {
		*ptr = nil
	}
}
func (root *OnvaRootStore) Has(key []byte) bool {
	return root.okv.Get(key) != nil
}
func (root *OnvaRootStore) Iterator(start, end []byte) types.ObjIterator {
	return &RootStoreIterator{root: root, iter: root.okv.Iterator(start, end)}
}
func (root *OnvaRootStore) ReverseIterator(start, end []byte) types.ObjIterator {
	return &RootStoreIterator{root: root, iter: root.okv.ReverseIterator(start, end)}
}
func (root *OnvaRootStore) SetAsync(key, value []byte) {
	root.tasks = append(root.tasks, onvakv.NewSetTask(key, value))
}
func (root *OnvaRootStore) SetObjAsync(key []byte, obj types.Serializable) {
	root.tasks = append(root.tasks, onvakv.NewSetTask(key, obj.ToBytes()))
}
func (root *OnvaRootStore) addToCache(key []byte, obj types.Serializable) {
	if len(root.cache) > CacheSizeLimit {
		for k := range root.cache {
			delete(root.cache, k) //remove a random entry
			break
		}
	}
	root.cache[string(key)] = obj
}
func (root *OnvaRootStore) DeleteAsync(key []byte) {
	root.tasks = append(root.tasks, onvakv.NewDeleteTask(key))
}
func (root *OnvaRootStore) Flush() {
	root.okv.EndBlock(root.tasks, root.height)
	root.tasks = root.tasks[:0]
}
func (root *OnvaRootStore) Cached() types.MultiStore {
	return &OverlayedMultiStore {
		cache:     NewCacheStore(),
		parent:    root,
		storeKeys: root.storeKeys,
	}
}
func (root *OnvaRootStore) GetRootHash() []byte {
	return root.okv.GetRootHash()
}

type RootStoreIterator struct {
	root *OnvaRootStore
	iter dbm.Iterator
}

func (iter *RootStoreIterator) Domain() (start []byte, end []byte) {
	return iter.iter.Domain()
}
func (iter *RootStoreIterator) Valid() bool {
	return iter.iter.Valid()
}
func (iter *RootStoreIterator) Next() {
	iter.iter.Next()
}
func (iter *RootStoreIterator) Key() (key []byte) {
	return iter.iter.Key()
}
func (iter *RootStoreIterator) Value() (value []byte) {
	return iter.iter.Value()
}
func (iter *RootStoreIterator) ObjValue(ptr *types.Serializable) {
	if obj, ok := iter.root.cache[string(iter.iter.Key())]; ok {
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(obj))
	}
	(*ptr).FromBytes(iter.iter.Value())
}
func (iter *RootStoreIterator) Close() {
	iter.iter.Close()
}

	//SetPruning(PruningOptions)
	// Mount a store of type using the given db.
	// If db == nil, the new store will use the CommitMultiStore db.
	//MountStoreWithDB(key StoreKey, typ StoreType, db dbm.DB)
	// Load the latest persisted version.  Called once after all
	// calls to Mount*Store() are complete.
	//LoadLatestVersion() error
