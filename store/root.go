package store

import (
	"reflect"

	dbm "github.com/tendermint/tm-db"

	"github.com/coinexchain/onvakv"
	"github.com/coinexchain/onvakv/store/types"
)

const CacheSizeLimit = 1024 * 1024

type RootStore struct {
	cache          map[string]types.Serializable
	isCacheableKey func(k []byte) bool
	okv            *onvakv.OnvaKV
	height         int64
	storeKeys      map[types.StoreKey]struct{}
}

var _ types.RootStoreI = &RootStore{}

func NewRootStore(okv *onvakv.OnvaKV, storeKeys map[types.StoreKey]struct{}, isCacheableKey func(k []byte) bool) *RootStore {
	return &RootStore{
		cache:          make(map[string]types.Serializable),
		isCacheableKey: isCacheableKey,
		okv:            okv,
		height:         -1,
		storeKeys:      storeKeys,
	}
}

func (root *RootStore) SetHeight(h int64) {
	root.height = h
}

func (root *RootStore) Get(key []byte) []byte { //TODO should check root.cache
	e := root.okv.GetEntry(key)
	if e == nil {
		return nil
	}
	return e.Value
}
func (root *RootStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
	ok := false
	var obj types.Serializable
	if root.isCacheableKey(key) {
		obj, ok = root.cache[string(key)]
	}
	if ok {
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(obj))
	} else if bz := root.Get(key); bz != nil {
		(*ptr).FromBytes(bz)
		root.addToCache(key, *ptr)
	} else {
		*ptr = nil
	}
}
func (root *RootStore) GetObjCopy(key []byte, ptr *types.Serializable) {
	ok := false
	var obj types.Serializable
	if root.isCacheableKey(key) {
		obj, ok = root.cache[string(key)]
	}
	if ok {
		newObj := obj.DeepCopy().(types.Serializable)
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(newObj))
	} else if bz := root.Get(key); bz != nil {
		(*ptr).FromBytes(bz)
	} else {
		*ptr = nil
	}
}

func (root *RootStore) Has(key []byte) bool {
	return root.okv.GetEntry(key) != nil
}

func (root *RootStore) PrepareForUpdate(key []byte) {
	root.okv.PrepareForUpdate(key)
}

func (root *RootStore) PrepareForDeletion(key []byte) {
	root.okv.PrepareForDeletion(key)
}

func (root *RootStore) Iterator(start, end []byte) types.ObjIterator {
	return &RootStoreIterator{root: root, iter: root.okv.Iterator(start, end)}
}
func (root *RootStore) ReverseIterator(start, end []byte) types.ObjIterator {
	return &RootStoreIterator{root: root, iter: root.okv.ReverseIterator(start, end)}
}

func (root *RootStore) BeginWrite() {
	if root.height < 0 {
		panic("Height is not initialized")
	}
	root.okv.BeginWrite(root.height)
}

func (root *RootStore) Set(key, value []byte) {
	root.okv.Set(key, value)
}

func (root *RootStore) SetObj(key []byte, obj types.Serializable) {
	root.okv.Set(key, obj.ToBytes())
	root.addToCache(key, obj)
}

func (root *RootStore) Delete(key []byte) {
	root.okv.Delete(key)
	delete(root.cache, string(key))
}

func (root *RootStore) EndWrite() {
	root.okv.EndWrite()
}

func (root *RootStore) CheckConsistency() {
	root.okv.CheckConsistency()
}

func (root *RootStore) addToCache(key []byte, obj types.Serializable) {
	if !root.isCacheableKey(key) {
		return
	}
	if len(root.cache) > CacheSizeLimit {
		for k := range root.cache {
			delete(root.cache, k) //remove a random entry
			break
		}
	}
	root.cache[string(key)] = obj //.DeepCopy().(types.Serializable) // maybe we do not need deepcopy
}

func (root *RootStore) GetTrunkStore() interface{} {
	return &TrunkStore{
		cache:     NewCacheStore(),
		root:      root,
		storeKeys: root.storeKeys,
		isWriting: 0,
	}
}

func (root *RootStore) GetRootHash() []byte {
	return root.okv.GetRootHash()
}

type RootStoreIterator struct {
	root *RootStore
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
	key := iter.iter.Key()
	ok := false
	var obj types.Serializable
	if iter.root.isCacheableKey(key) {
		obj, ok = iter.root.cache[string(key)]
	}
	if ok {
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(obj)) // Client must use this obj as readonly
	} else {
		(*ptr).FromBytes(iter.iter.Value())
	}
}
func (iter *RootStoreIterator) Close() {
	iter.iter.Close()
}

