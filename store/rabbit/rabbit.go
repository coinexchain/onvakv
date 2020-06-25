package rabbit

import (
	"bytes"
	"fmt"

	sha256 "github.com/minio/sha256-simd"

	"github.com/coinexchain/onvakv/store"
	"github.com/coinexchain/onvakv/store/types"
)

const (
	KeySize = 2 // 2 for fuzz, 8 for production

	NotFount = 0
	EmptySlot = 1
	Exists = 2

	MaxFindDepth = 100
)

type RabbitStore struct {
	sms SimpleMultiStore
}

func NewRabbitStore(trunk *store.TrunkStore) (rabbit RabbitStore) {
	rabbit.sms = SimpleMultiStore{
		cache: NewSimpleCacheStore(),
		trunk: trunk,
	}
	return
}

var _ types.MultiStoreI = &RabbitStore{}

func (rabbit RabbitStore) Has(key []byte) bool {
	_, _, status := rabbit.find(key, true)
	return status == Exists
}

func (rabbit RabbitStore) Get(key []byte) []byte {
	cv, _, status := rabbit.find(key, true)
	if status != Exists {
		return nil
	}
	if bz, ok := cv.obj.([]byte); ok {
		return append([]byte{}, bz...)
	} else {
		return cv.obj.(types.Serializable).ToBytes()
	}
}

func (rabbit RabbitStore) GetObj(key []byte, ptr *types.Serializable) {
	rabbit.getObjHelper(false, key, ptr)
}

func (rabbit RabbitStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
	rabbit.getObjHelper(true, key, ptr)
}

func (rabbit RabbitStore) getObjHelper(readonly bool, key []byte, ptr *types.Serializable) {
	cv, _, status := rabbit.find(key, true)
	if status != Exists {
		*ptr = nil
		return
	}
	if cv.obj == nil {
		panic(fmt.Sprintf("Reading a dangling value %#v\n", cv))
	}
	if bz, ok := cv.obj.([]byte); ok {
		(*ptr).FromBytes(bz)
	} else {
		*ptr = cv.obj.(types.Serializable)
	}
	if !readonly {
		cv.obj = nil
	}
}

func (rabbit RabbitStore) find(key []byte, earlyExit bool) (cv *CachedValue, path [][KeySize]byte, status int) {
	var k [KeySize]byte
	hash := sha256.Sum256(key)
	status = NotFount
	for i := 0; i < MaxFindDepth; i++ {
		copy(k[:], hash[:])
		k[0] = k[0] | 0x1 // force the MSB to 1
		path = append(path, k)
		cv = rabbit.sms.GetCachedValue(k)
		if cv == nil {
			return
		}
		if bytes.Equal(cv.key, key) {
			status = Exists
			if cv.isEmpty {
				status = EmptySlot
			}
			return
		} else if earlyExit && cv.passbyNum == 0 {
			return
		} else {
			hash = sha256.Sum256(hash[:])
		}
	}
	panic(fmt.Sprintf("MaxFindDepth(%d) reached!", MaxFindDepth))
}

func (rabbit RabbitStore) Set(key []byte, bz []byte) {
	rabbit.setHelper(key, bz)
}

func (rabbit RabbitStore) SetObj(key []byte, obj types.Serializable) {
	rabbit.setHelper(key, obj)
}

func (rabbit RabbitStore) setHelper(key []byte, obj interface{}) {
	_, path, status := rabbit.find(key, false)
	if status == Exists { //change
		cv := rabbit.sms.MustGetCachedValue(path[len(path)-1])
		cv.obj = obj
		rabbit.sms.SetCachedValue(path[len(path)-1], cv)
		return
	}
	if status == EmptySlot { //overwrite
		cv := rabbit.sms.MustGetCachedValue(path[len(path)-1])
		cv.key = append([]byte{}, key...) //TODO
		cv.obj = obj
		cv.isEmpty = false
		rabbit.sms.SetCachedValue(path[len(path)-1], cv)
	} else { //insert
		rabbit.sms.SetCachedValue(path[len(path)-1], &CachedValue{
			key:       append([]byte{}, key...),
			obj:       obj,
			passbyNum: 0,
			isEmpty:   false,
		})
	}
	// incr passbyNum
	for _, k := range path[:len(path)-1] {
		cv := rabbit.sms.MustGetCachedValue(k)
		cv.passbyNum++
		rabbit.sms.SetCachedValue(k, cv)
	}
}

func (rabbit RabbitStore) Delete(key []byte) {
	_, path, status := rabbit.find(key, true)
	if status != Exists {
		return
	}
	cv := rabbit.sms.MustGetCachedValue(path[len(path)-1])
	if cv.passbyNum == 0 { // can delete it
		cv.isDeleted = true
	} else { // can not delete it, just mark it as deleted
		cv.isEmpty = true
	}
	rabbit.sms.SetCachedValue(path[len(path)-1], cv)
	for _, k := range path[:len(path)-1] {
		cv := rabbit.sms.MustGetCachedValue(k)
		cv.passbyNum--
		if cv.passbyNum == 0 && cv.isEmpty {
			cv.isDeleted = true
		}
		rabbit.sms.SetCachedValue(k, cv)
	}
}

func (rabbit RabbitStore) Close(writeBack bool) {
	rabbit.sms.Close(writeBack)
}

func (rabbit RabbitStore) ActiveCount() int {
	return rabbit.sms.trunk.ActiveCount()
}

func (rabbit RabbitStore) Iterator(start, end []byte) types.ObjIterator {
	panic("Not Implemented")
}

func (rabbit RabbitStore) ReverseIterator(start, end []byte) types.ObjIterator {
	panic("Not Implemented")
}

