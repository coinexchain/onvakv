package store

import (
	"github.com/coinexchain/onvakv/store/types"
)

type MockRootStore struct {
	cacheStore          *CacheStore
	preparedForUpdate   map[string]struct{}
	preparedForDeletion map[string]struct{}
	isWritting          bool
}

var _ types.RootStoreI = &MockRootStore{}

func NewMockRootStore() *MockRootStore {
	return &MockRootStore {
		cacheStore:          NewCacheStore(),
		preparedForUpdate:   make(map[string]struct{}),
		preparedForDeletion: make(map[string]struct{}),
	}
}

func (rs *MockRootStore) GetTrunkStore() interface{} {
	return &TrunkStore{
		cache:     NewCacheStore(),
		root:      rs,
		isWriting: 0,
	}
}

func (rs *MockRootStore) SetHeight(h int64) {
}

func (rs *MockRootStore) Get(key []byte) []byte {
	if rs.isWritting {panic("isWritting")}
	v, status := rs.cacheStore.Get(key)
	if status == types.Missed {
		return nil
	}
	return v
}

func (rs *MockRootStore) GetObjCopy(key []byte, ptr *types.Serializable) {
	if rs.isWritting {panic("isWritting")}
	rs.cacheStore.GetObjCopy(key, ptr)
}

func (rs *MockRootStore) GetObj(key []byte, ptr *types.Serializable) {
	if rs.isWritting {panic("isWritting")}
	rs.cacheStore.GetObj(key, ptr)
}

func (rs *MockRootStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
	if rs.isWritting {panic("isWritting")}
	rs.cacheStore.GetReadOnlyObj(key, ptr)
}

func (rs *MockRootStore) Has(key []byte) bool {
	if rs.isWritting {panic("isWritting")}
	_, status := rs.cacheStore.Get(key)
	return status == types.Hit
}

func (rs *MockRootStore) PrepareForUpdate(key []byte) {
	if rs.isWritting {panic("isWritting")}
	rs.preparedForUpdate[string(key)] = struct{}{}
}

func (rs *MockRootStore) PrepareForDeletion(key []byte) {
	if rs.isWritting {panic("isWritting")}
	rs.preparedForDeletion[string(key)] = struct{}{}
}

func (rs *MockRootStore) Iterator(start, end []byte) types.ObjIterator {
	return rs.cacheStore.Iterator(start, end)
}

func (rs *MockRootStore) ReverseIterator(start, end []byte) types.ObjIterator {
	return rs.cacheStore.ReverseIterator(start, end)
}

func (rs *MockRootStore) BeginWrite() {
	rs.isWritting = true
}

func (rs *MockRootStore) Set(key, value []byte) {
	if !rs.isWritting {panic("notWritting")}
	if _, ok := rs.preparedForUpdate[string(key)]; !ok {
		panic("not prepared")
	}
	rs.cacheStore.Set(key, value)
}

func (rs *MockRootStore) SetObj(key []byte, obj types.Serializable) {
	if !rs.isWritting {panic("notWritting")}
	if _, ok := rs.preparedForUpdate[string(key)]; !ok {
		panic("not prepared")
	}
	rs.cacheStore.SetObj(key, obj)
}

func (rs *MockRootStore) Delete(key []byte) {
	if !rs.isWritting {panic("notWritting")}
	if _, ok := rs.preparedForDeletion[string(key)]; !ok {
		panic("not prepared")
	}
	rs.cacheStore.Delete(key)
}

func (rs *MockRootStore) EndWrite() {
	rs.isWritting = false
}

