package store

import (
	"sync/atomic"

	"github.com/coinexchain/onvakv/store/types"
)

// We use a new TrunkStore for every block
type TrunkStore struct {
	cache     *CacheStore
	root      *RootStore
	storeKeys map[types.StoreKey]struct{}
	isWriting int64
}

func (ts *TrunkStore) Cached() *MultiStore {
	return &MultiStore{
		cache:     NewCacheStore(),
		parent:    ts,
		storeKeys: ts.storeKeys,
	}
}

func (ts *TrunkStore) Has(key []byte) bool {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	_, status := ts.cache.Get(key)
	switch status {
	case types.JustDeleted:
		return false
	case types.Hit:
		return true
	case types.Missed:
		return ts.root.Has(key)
	default:
		panic("Invalid Status")
	}
}

func (ts *TrunkStore) Get(key []byte) []byte {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	res, status := ts.cache.Get(key)
	switch status {
	case types.JustDeleted:
		return nil
	case types.Hit:
		return res
	case types.Missed:
		return ts.root.Get(key)
	default:
		panic("Invalid Status")
	}
}

func (ts *TrunkStore) GetObjCopy(key []byte, ptr *types.Serializable) {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	status := ts.cache.GetObjCopy(key, ptr)
	switch status {
	case types.JustDeleted:
		ptr = nil
	case types.Missed:
		ts.root.GetObjCopy(key, ptr)
	}
}

func (ts *TrunkStore) GetObj(key []byte, ptr *types.Serializable) {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	status := ts.cache.GetObj(key, ptr)
	switch status {
	case types.JustDeleted:
		ptr = nil
	case types.Missed:
		ts.root.GetObjCopy(key, ptr)
	}
}

func (ts *TrunkStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	status := ts.cache.GetReadOnlyObj(key, ptr)
	switch status {
	case types.JustDeleted:
		ptr = nil
	case types.Missed:
		ts.root.GetReadOnlyObj(key, ptr)
	}
}

func (ts *TrunkStore) PrepareForUpdate(key []byte) {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	ts.root.PrepareForUpdate(key)
}

func (ts *TrunkStore) PrepareForDeletion(key []byte) {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	ts.root.PrepareForDeletion(key)
}

func (ts *TrunkStore) Update(updator func(cache *CacheStore)) {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	updator(ts.cache)
}

func (ts *TrunkStore) Iterator(start, end []byte) types.ObjIterator {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	return newCacheMergeIterator(ts.root.Iterator(start, end), ts.cache.Iterator(start, end), true)
}

func (ts *TrunkStore) ReverseIterator(start, end []byte) types.ObjIterator {
	if atomic.LoadInt64(&ts.isWriting) != 0 {
		panic("Is Writing")
	}
	return newCacheMergeIterator(ts.root.ReverseIterator(start, end), ts.cache.ReverseIterator(start, end), false)
}

func (ts *TrunkStore) writeBack() {
	if atomic.AddInt64(&ts.isWriting, 1) != 1 {
		panic("Conflict During Writing")
	}
	ts.root.BeginWrite()
	ts.cache.ScanAllEntries(func(key []byte, obj interface{}, isDeleted bool) {
		if isDeleted {
			ts.root.Delete(key)
		} else {
			if sobj, ok := obj.(types.Serializable); ok {
				ts.root.SetObj(key, sobj)
			} else {
				ts.root.Set(key, obj.([]byte))
			}
		}
	})
	ts.root.EndWrite()
	if atomic.AddInt64(&ts.isWriting, -1) != 0 {
		panic("Conflict During Writing")
	}
}

func (ts *TrunkStore) Close(writeBack bool) {
	if writeBack {
		ts.writeBack()
	}
	ts.cache.Close()
	ts.cache = nil
	ts.root = nil
	ts.storeKeys = nil
}

