package fuzz

import (
	"github.com/coinexchain/onvakv/store"
	storetypes "github.com/coinexchain/onvakv/store/types"
)

type UndoOp struct {
	oldStatus  storetypes.CacheStatus
	key, value []byte
}

type RefStore struct {
	cs      *store.CacheStore
	tobeDel map[string]struct{}
	justAdd map[string]struct{}
}

func NewRefStore() *RefStore {
	return &RefStore{
		cs:      store.NewCacheStore(),
		tobeDel: make(map[string]struct{}),
		justAdd: make(map[string]struct{}),
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

func (rs *RefStore) MarkSet(key []byte) {
	rs.justAdd[string(key)] = struct{}{}
}

func (rs *RefStore) RealSet(key, value []byte) {
	rs.cs.Set(key, value)
}

func (rs *RefStore) Set(key, value []byte) UndoOp {
	v, status := rs.cs.Get(key)
	rs.cs.Set(key, value)
	return UndoOp {
		oldStatus: status,
		key:       key,
		value:     v,
	}
}

func (rs *RefStore) IsChangedInSameEpoch(key []byte) bool {
	_, ok1 := rs.justAdd[string(key)]
	_, ok2 := rs.tobeDel[string(key)]
	return ok1 || ok2
}

func (rs *RefStore) RealDelete(key []byte) {
	rs.cs.RealDelete(key)
}

func (rs *RefStore) Delete(key []byte) UndoOp {
	v, status := rs.cs.Get(key)
	rs.cs.Delete(key)
	return UndoOp {
		oldStatus: status,
		key:       key,
		value:     v,
	}
}

func (rs *RefStore) MarkDelete(key []byte) {
	rs.tobeDel[string(key)] = struct{}{}
}

func (rs *RefStore) SwitchEpoch() {
	for key := range rs.tobeDel {
		if _, status := rs.cs.Get([]byte(key)); status == storetypes.JustDeleted {
			rs.cs.RealDelete([]byte(key))
		}
	}
	rs.tobeDel = make(map[string]struct{})
	rs.justAdd = make(map[string]struct{})
}

func (rs *RefStore) Iterator(start, end []byte) storetypes.ObjIterator {
	return rs.cs.Iterator(start, end)
}

func (rs *RefStore) ReverseIterator(start, end []byte) storetypes.ObjIterator {
	return rs.cs.ReverseIterator(start, end)
}
