package store

import (
	"fmt"
	"reflect"
	"sync"

	dbm "github.com/tendermint/tm-db"

	"github.com/coinexchain/onvakv"
	"github.com/coinexchain/onvakv/store/types"
)

const CacheSizeLimit = 1024 * 1024

type RootStore struct {
	cache          map[string]types.Serializable
	cacheBuf       *sync.Map
	isCacheableKey func(k []byte) bool
	okv            *onvakv.OnvaKV
	height         int64
	storeKeys      map[types.StoreKey]struct{}
}

var _ types.RootStoreI = &RootStore{}

func NewRootStore(okv *onvakv.OnvaKV, storeKeys map[types.StoreKey]struct{}, isCacheableKey func(k []byte) bool) *RootStore {
	return &RootStore{
		cache:          make(map[string]types.Serializable),
		cacheBuf:       &sync.Map{},
		isCacheableKey: isCacheableKey,
		okv:            okv,
		height:         -1,
		storeKeys:      storeKeys,
	}
}

func (root *RootStore) SetHeight(h int64) {
	root.height = h
}

func (root *RootStore) Get(key []byte) []byte {
	ok := false
	var obj types.Serializable
	if root.isCacheableKey != nil && root.isCacheableKey(key) {
		obj, ok = root.cache[string(key)]
	}
	if ok {
		return obj.ToBytes()
	} else {
		return root.get(key)
	}
}

func (root *RootStore) get(key []byte) []byte {
	e := root.okv.GetEntry(key)
	if e == nil {
		return nil
	}
	return e.Value
}

func (root *RootStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
	ok := false
	var obj types.Serializable
	if root.isCacheableKey != nil && root.isCacheableKey(key) {
		obj, ok = root.cache[string(key)]
	}
	if ok {
		//fmt.Printf("HIT on %#v : %#v\n", key, obj.ToBytes())
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(obj))
	} else if bz := root.get(key); bz != nil {
		(*ptr).FromBytes(bz)
		if root.isCacheableKey(key) {
			root.cacheBuf.Store(string(key), *ptr)
		}
	} else {
		*ptr = nil
	}
}

func (root *RootStore) GetObjCopy(key []byte, ptr *types.Serializable) {
	ok := false
	var obj types.Serializable
	if root.isCacheableKey != nil && root.isCacheableKey(key) {
		obj, ok = root.cache[string(key)]
	}
	if ok {
		//fmt.Printf("HIT on %#v : %#v\n", key, obj.ToBytes())
		newObj := obj.DeepCopy().(types.Serializable)
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(newObj))
	} else if bz := root.get(key); bz != nil {
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
		panic(fmt.Sprintf("Height is not initialized: %", root.height))
	}
	root.okv.BeginWrite(root.height)
	root.cacheBuf.Range(func(key, value interface{}) bool {
		root.addToCache([]byte(key.(string)), value.(types.Serializable))
		return true
	})
	root.cacheBuf = nil
}

func (root *RootStore) Set(key, value []byte) {
	root.okv.Set(key, value)
	if root.isCacheableKey != nil && root.isCacheableKey(key) {
		_, ok := root.cache[string(key)]
		if ok {
			//fmt.Printf("CACHE-UPDATE on %#v : %#v\n", key, value)
			root.cache[string(key)].FromBytes(value)
		}
	}
}

func (root *RootStore) SetObj(key []byte, obj types.Serializable) {
	root.okv.Set(key, obj.ToBytes())
	if root.isCacheableKey != nil && root.isCacheableKey(key) {
		root.addToCache(key, obj)
	}
}

func (root *RootStore) Delete(key []byte) {
	root.okv.Delete(key)
	delete(root.cache, string(key))
}

func (root *RootStore) EndWrite() {
	root.okv.EndWrite()
	root.cacheBuf = &sync.Map{}
}

func (root *RootStore) CheckConsistency() {
	root.okv.CheckConsistency()
}

func (root *RootStore) addToCache(key []byte, obj types.Serializable) {
	if len(root.cache) > CacheSizeLimit {
		for k := range root.cache {
			delete(root.cache, k) //remove a random entry
			break
		}
	}
	//fmt.Printf("CACHE-INSERT on %#v : %#v\n", key, obj.ToBytes())
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

func (root *RootStore) Close() {
	root.okv.Close()
	root.cache = nil
}

func (root *RootStore) ActiveCount() int {
	return root.okv.ActiveCount()
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
	if iter.root.isCacheableKey != nil && iter.root.isCacheableKey(key) {
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

