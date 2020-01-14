package store

import (
	"github.com/coinexchain/onvakv/store/types"
)

type OverlayedMultiStore struct {
	cache     *CacheStore
	parent    types.BaseStore
	storeKeys map[types.StoreKey]struct{}
}

var _ types.MultiStore = (*OverlayedMultiStore)(nil)

func (ms *OverlayedMultiStore) SubStore(storeKey types.StoreKey) types.KObjStore {
	if _, ok := ms.storeKeys[storeKey]; !ok {
		panic("Invalid StoreKey")
	}
	prefix := []byte(storeKey.String())
	if len(prefix) < 2 {
		panic("Prefix is too short")
	}
	if prefix[0] == 0 && prefix[1] == 0 {
		panic("Prefix conflicts with guarding kv pair")
	}
	if prefix[0] == 255 && prefix[1] == 255 {
		panic("Prefix conflicts with guarding kv pair")
	}
	return NewPrefixedStore(ms, prefix)
}

func (ms *OverlayedMultiStore) Cached() types.MultiStore {
	return &OverlayedMultiStore {
		cache:     NewCacheStore(),
		parent:    ms,
		storeKeys: ms.storeKeys,
	}
}

func (ms *OverlayedMultiStore) Get(key []byte) []byte {
	res, status := ms.cache.Get(key)
	switch status {
	case types.JustDeleted:
		return nil
	case types.Hit:
		return res
	case types.Missed:
		return ms.parent.Get(key)
	default:
		panic("Invalid Status")
	}
}

func (ms *OverlayedMultiStore) GetObjForOverlay(key []byte, ptr *types.Serializable) {
	status := ms.cache.GetObjForOverlay(key, ptr)
	switch status {
	case types.JustDeleted:
		ptr = nil
	case types.Missed:
		ms.parent.GetObjForOverlay(key, ptr)
	}
}

func (ms *OverlayedMultiStore) GetObj(key []byte, ptr *types.Serializable) {
	status := ms.cache.GetObj(key, ptr)
	switch status {
	case types.JustDeleted:
		ptr = nil
	case types.Missed:
		ms.parent.GetObjForOverlay(key, ptr)
	}
}

func (ms *OverlayedMultiStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
	status := ms.cache.GetReadOnlyObj(key, ptr)
	switch status {
	case types.JustDeleted:
		ptr = nil
	case types.Missed:
		ms.parent.GetReadOnlyObj(key, ptr)
	}
}

func (ms *OverlayedMultiStore) Has(key []byte) bool {
	return ms.Get(key) != nil
}

func (ms *OverlayedMultiStore) SetAsync(key, value []byte) {
	ms.cache.Set(key, value)
}

func (ms *OverlayedMultiStore) SetObjAsync(key []byte, obj types.Serializable) {
	ms.cache.SetObj(key, obj)
}

func (ms *OverlayedMultiStore) DeleteAsync(key []byte) {
	ms.cache.Delete(key)
}

func (ms *OverlayedMultiStore) Flush() {
	ms.cache.ScanAllEntries(func(key []byte, obj interface{}, isDeleted bool) {
		if isDeleted {
			ms.parent.DeleteAsync(key)
		} else {
			if sobj, ok := obj.(types.Serializable); ok {
				ms.parent.SetObjAsync(key, sobj)
			} else {
				ms.parent.SetAsync(key, obj.([]byte))
			}
		}
		ms.cache.Close()
		ms.cache = NewCacheStore()
	})
}

func (ms *OverlayedMultiStore) Iterator(start, end []byte) types.ObjIterator {
	return newCacheMergeIterator(ms.parent.Iterator(start, end), ms.cache.Iterator(start, end), true)
}

func (ms *OverlayedMultiStore) ReverseIterator(start, end []byte) types.ObjIterator {
	return newCacheMergeIterator(ms.parent.ReverseIterator(start, end), ms.cache.ReverseIterator(start, end), false)
}

