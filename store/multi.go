package store

import (
	"github.com/coinexchain/onvakv/store/types"
)

// We use a new TrunkStore for transaction
type MultiStore struct {
	cache     *CacheStore
	trunk     *TrunkStore
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
		return ms.trunk.Has(key)
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
		return ms.trunk.Get(key)
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
		ms.trunk.GetObjCopy(key, ptr)
	}
}

func (ms *MultiStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
	status := ms.cache.GetReadOnlyObj(key, ptr)
	switch status {
	case types.JustDeleted:
		*ptr = nil
	case types.Missed:
		ms.trunk.GetReadOnlyObj(key, ptr)
	}
}

func (ms *MultiStore) Set(key, value []byte) {
	ms.cache.Set(key, value)
	ms.trunk.PrepareForUpdate(key)
}

func (ms *MultiStore) SetObj(key []byte, obj types.Serializable) {
	ms.cache.SetObj(key, obj)
	ms.trunk.PrepareForUpdate(key)
}

func (ms *MultiStore) Delete(key []byte) {
	ms.cache.Delete(key)
	ms.trunk.PrepareForDeletion(key)
}

func (ms *MultiStore) Close(writeBack bool) {
	if writeBack {
		ms.writeBack()
	}
	ms.cache = nil
	ms.trunk = nil
	ms.storeKeys = nil
}

func (ms *MultiStore) writeBack() {
	ms.trunk.Update(func(cache *CacheStore) {
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
	return newCacheMergeIterator(ms.trunk.Iterator(start, end), ms.cache.Iterator(start, end), true)
}

func (ms *MultiStore) ReverseIterator(start, end []byte) types.ObjIterator {
	return newCacheMergeIterator(ms.trunk.ReverseIterator(start, end), ms.cache.ReverseIterator(start, end), false)
}
