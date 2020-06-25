package rabbit

import (
	"fmt"
	"encoding/binary"

	"github.com/coinexchain/onvakv/store/types"
)

const (
	EmptyMarkerIndex = 0
	PassbyNumIndex = 1
	KeyLenStart = PassbyNumIndex+8
	KeyStart = KeyLenStart+4
)

type CachedValue struct {
	passbyNum uint64
	key       []byte
	obj       interface{}
	isEmpty   bool
	isDeleted bool
	isDirty   bool
}

func (v *CachedValue) ToBytes() []byte {
	var buf, value []byte
	if v.isEmpty {
		buf = make([]byte, 1+8+4, 1+8+4+len(v.key))
		buf[EmptyMarkerIndex] = 1
	} else {
		buf = make([]byte, 1+8+4, 1+8+4+len(v.key)+len(value))
		buf[EmptyMarkerIndex] = 0
		if bz, ok := v.obj.([]byte); ok {
			value = bz
		} else {
			value = v.obj.(types.Serializable).ToBytes()
		}
	}
	binary.LittleEndian.PutUint64(buf[PassbyNumIndex:PassbyNumIndex+8], v.passbyNum)
	binary.LittleEndian.PutUint32(buf[KeyLenStart:KeyStart], uint32(len(v.key)))
	buf = append(buf, v.key...)
	if !v.isEmpty {
		buf = append(buf, value...)
	}
	return buf
}

func BytesToCachedValue(buf []byte) *CachedValue {
	keyLen := int(binary.LittleEndian.Uint32(buf[KeyLenStart:KeyStart]))
	res := &CachedValue {
		passbyNum: binary.LittleEndian.Uint64(buf[PassbyNumIndex:PassbyNumIndex+8]),
		key:       buf[KeyStart:KeyStart+keyLen],
	}
	if buf[EmptyMarkerIndex] != 0 {
		res.isEmpty = true
		res.obj = nil
	} else {
		res.isEmpty = false
		res.obj = buf[KeyStart+keyLen:]
	}
	return res
}


type SimpleCacheStore struct {
	m map[[KeySize]byte]*CachedValue
}

func NewSimpleCacheStore() *SimpleCacheStore {
	return &SimpleCacheStore{
		m: make(map[[KeySize]byte]*CachedValue),
	}
}

func (scs *SimpleCacheStore) ScanAllEntries(fn func(key, value []byte, isDeleted bool)) {
	for key, cv := range scs.m {
		if cv.obj == nil && !cv.isDeleted && !cv.isEmpty {
			panic(fmt.Sprintf("Dangling Cache Entry for %s(%v) %#v", string(key[:]), key, cv))
		}
		if !cv.isDirty {
			continue
		}
		fn(key[:], cv.ToBytes(), cv.isDeleted)
	}
}

func (scs *SimpleCacheStore) GetValue(key [KeySize]byte) (value *CachedValue, status types.CacheStatus) {
	v, ok := scs.m[key]
	if !ok {
		status = types.Missed
	} else if v.isDeleted {
		status = types.JustDeleted
	} else {
		value = v
		status = types.Hit
	}
	return
}

func (scs *SimpleCacheStore) SetValue(key [KeySize]byte, value *CachedValue) {
	scs.m[key] = value
}

