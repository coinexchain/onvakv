package fuzzaboveroot

import (
	"github.com/coinexchain/onvakv/store"
	storetypes "github.com/coinexchain/onvakv/store/types"
)

type RefStore struct {
	cs *store.CacheStore
}

func NewRefStore() *RefStore {
	return &RefStore{
		cs: store.NewCacheStore(),
	}
}

func (rs *RefStore) Close() {
	rs.cs.Close()
}

func (rs *RefStore) Get(key []byte) []byte {
	v, _ := rs.cs.Get(key)
	return v
}

func (rs *RefStore) Has(key []byte) bool {
	_, status := rs.cs.Get(key)
	return status != storetypes.Missed
}

func (rs *RefStore) Set(key, value []byte) {
	rs.cs.Set(key, value)
}

func (rs *RefStore) Delete(key []byte) {
	rs.cs.RealDelete(key)
}

func (rs *RefStore) Iterator(start, end []byte) storetypes.ObjIterator {
	return rs.cs.Iterator(start, end)
}

func (rs *RefStore) ReverseIterator(start, end []byte) storetypes.ObjIterator {
	return rs.cs.ReverseIterator(start, end)
}
