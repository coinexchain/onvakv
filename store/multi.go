package store

import (
	"github.com/coinexchain/onvakv/store/types"
)

// We use a new TrunkStore for transaction
type MultiStore struct {
	cache     *CacheStore
	parent    *TrunkStore
	storeKeys map[types.StoreKey]struct{}
}

func (ms *MultiStore) SubStore(storeKey types.StoreKey) types.KObjStore {
	if storeKey == nil {
		return ms
	}
	if _, ok := ms.storeKeys[storeKey]; !ok {
		panic("Invalid StoreKey")
	}
	prefix := []byte(storeKey.Prefix())
	return NewPrefixedStore(ms, prefix)
}

func (ms *MultiStore) Has(key []byte) bool {
	_, status := ms.cache.Get(key)
	switch status {
	case types.JustDeleted:
		return false
	case types.Hit:
		return true
	case types.Missed:
		return ms.parent.Has(key)
	default:
		panic("Invalid Status")
	}
}

func (ms *MultiStore) Get(key []byte) []byte {
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

func (ms *MultiStore) GetObj(key []byte, ptr *types.Serializable) {
	status := ms.cache.GetObj(key, ptr)
	switch status {
	case types.JustDeleted:
		*ptr = nil
	case types.Missed:
		ms.parent.GetObjCopy(key, ptr)
	}
}

func (ms *MultiStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
	status := ms.cache.GetReadOnlyObj(key, ptr)
	switch status {
	case types.JustDeleted:
		*ptr = nil
	case types.Missed:
		ms.parent.GetReadOnlyObj(key, ptr)
	}
}

func (ms *MultiStore) Set(key, value []byte) {
	ms.cache.Set(key, value)
	ms.parent.PrepareForUpdate(key)
}

func (ms *MultiStore) SetObj(key []byte, obj types.Serializable) {
	ms.cache.SetObj(key, obj)
	ms.parent.PrepareForUpdate(key)
}

func (ms *MultiStore) Delete(key []byte) {
	ms.cache.Delete(key)
	ms.parent.PrepareForDeletion(key)
}

func (ms *MultiStore) Close(writeBack bool) {
	if writeBack {
		ms.writeBack()
	}
	ms.cache = nil
	ms.parent = nil
	ms.storeKeys = nil
}

func (ms *MultiStore) writeBack() {
	ms.parent.Update(func(cache *CacheStore) {
		ms.cache.ScanAllEntries(func(key []byte, obj interface{}, isDeleted bool) {
			if isDeleted {
				cache.Delete(key)
			} else {
				if sobj, ok := obj.(types.Serializable); ok {
					cache.SetObj(key, sobj)
				} else {
					cache.Set(key, obj.([]byte))
				}
			}
		})
	})
}

func (ms *MultiStore) Iterator(start, end []byte) types.ObjIterator {
	return newCacheMergeIterator(ms.parent.Iterator(start, end), ms.cache.Iterator(start, end), true)
}

func (ms *MultiStore) ReverseIterator(start, end []byte) types.ObjIterator {
	return newCacheMergeIterator(ms.parent.ReverseIterator(start, end), ms.cache.ReverseIterator(start, end), false)
}
