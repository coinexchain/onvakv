package rabbit

import (
	"fmt"

	"github.com/coinexchain/onvakv/store"
	"github.com/coinexchain/onvakv/store/types"
)

// We use a new TrunkStore for transaction
type SimpleMultiStore struct {
	cache     *SimpleCacheStore
	trunk     *store.TrunkStore
}

//var WatchedKey = []uint8{0x47, 0x60, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0}
//var WatchedShortKey = []uint8{0xe3, 0x7e}

func (sms *SimpleMultiStore) GetCachedValue(key [KeySize]byte) *CachedValue {
	cv, status := sms.cache.GetValue(key)
	switch status {
	case types.JustDeleted:
		return nil
	case types.Missed:
		bz := sms.trunk.Get(key[:])
		if bz == nil {
			return nil
		}
		cv := BytesToCachedValue(bz)
		sms.cache.SetValue(key, cv)
		return cv
	case types.Hit:
		return cv
	default:
		panic(fmt.Sprintf("Invalid Status %d", status))
	}
}

func (sms *SimpleMultiStore) MustGetCachedValue(key [KeySize]byte) *CachedValue {
	cv, status := sms.cache.GetValue(key)
	if status != types.Hit {
		panic("Failed to get cached value")
	}
	return cv
}

func (sms *SimpleMultiStore) SetCachedValue(key [KeySize]byte, cv *CachedValue) {
	cv.isDirty = true
	sms.cache.SetValue(key, cv)
	if cv.isDeleted {
		sms.trunk.PrepareForDeletion(key[:])
	} else {
		sms.trunk.PrepareForUpdate(key[:])
	}
}

func (sms *SimpleMultiStore) Close(writeBack bool) {
	if writeBack {
		sms.writeBack()
	}
}

func (sms *SimpleMultiStore) writeBack() {
	sms.trunk.Update(func(cache *store.CacheStore) {
		sms.cache.ScanAllEntries(func(key, value []byte, isDeleted bool) {
			if isDeleted {
				cache.Delete(key)
			} else {
				cache.Set(key, value)
			}
		})
	})
}

