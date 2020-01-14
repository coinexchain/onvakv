package store

import (
	"bytes"
	"reflect"
	"io"

	"github.com/coinexchain/onvakv/store/types"
	"github.com/coinexchain/onvakv/store/b"
)

type CacheStore struct {
	bt *b.Tree
}

func NewCacheStore() *CacheStore {
	return &CacheStore {
		bt: b.TreeNew(bytes.Compare),
	}
}

func (cs *CacheStore) Close() {
	cs.bt.Close()
}

func (cs *CacheStore) ScanAllEntries(fn func(key []byte, obj interface{}, isDeleted bool)) {
	e, err := cs.bt.SeekFirst()
	if err != nil {
		return
	}
	defer e.Close()
	key, value, err := e.Next()
	for err != nil {
		if value.GetObj() == nil {
			panic("Dangling Cache Entry")
		}
		fn(key, value.GetObj(), value.IsDeleted())
		key, value, err = e.Next()
	}
}

func (cs *CacheStore) Get(key []byte) (res []byte, status types.CacheStatus) {
	v, ok := cs.bt.Get(key)
	if !ok {
		status = types.Missed
	} else if v.IsDeleted() {
		status = types.JustDeleted
	} else {
		obj := v.GetObj()
		bz, ok := obj.([]byte)
		if ok {
			res = bz
		} else {
			res = obj.(types.Serializable).ToBytes()
		}
	}
	return
}

func (cs *CacheStore) GetObjForOverlay(key []byte, ptr *types.Serializable) (status types.CacheStatus) {
	cs.bt.Put(key, func(oldV b.Value, exists bool) (newV b.Value, write bool) {
		if exists {
			if oldV.IsDeleted() {
				status = types.JustDeleted
			} else {
				bz, isRawBytes := oldV.GetObj().([]byte)
				if isRawBytes {
					(*ptr).FromBytes(bz)
				} else {
					obj := oldV.GetObj().(types.Serializable)
					newV = b.NewValue(obj.DeepCopy())
					reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(obj))
					write = true
				}
				status = types.Hit
			}
		} else {
			status = types.Missed
		}
		return
	})
	return
}

func (cs *CacheStore) GetObj(key []byte, ptr *types.Serializable) (status types.CacheStatus) {
	cs.bt.Put(key, func(oldV b.Value, exists bool) (newV b.Value, write bool) {
		if exists {
			if oldV.IsDeleted() {
				status = types.JustDeleted
			} else {
				bz, isRawBytes := oldV.GetObj().([]byte)
				if isRawBytes {
					(*ptr).FromBytes(bz)
				} else {
					reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(oldV.GetObj()))
					newV = b.NilValue()
					write = true
				}
				status = types.Hit
			}
		} else {
			status = types.Missed
		}
		return
	})
	return
}

func (cs *CacheStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) (status types.CacheStatus) {
	v, ok := cs.bt.Get(key)
	if !ok {
		status = types.Missed
	} else if v.IsDeleted() {
		status = types.JustDeleted
	} else {
		obj := v.GetObj()
		bz, isRawBytes := obj.([]byte)
		if isRawBytes {
			(*ptr).FromBytes([]byte(bz))
		} else {
			reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(obj))
		}
		status = types.Hit
	}
	return
}

func (cs *CacheStore) Set(key, value []byte) {
	v := b.NewValue(value)
	cs.bt.Set(append([]byte{}, key...), v)
}

func (cs *CacheStore) SetObj(key []byte, obj types.Serializable) {
	v := b.NewValue(obj)
	cs.bt.Set(append([]byte{}, key...), v)
}

func (cs *CacheStore) Delete(key []byte) {
	v := b.DeletedValue()
	cs.bt.Set(append([]byte{}, key...), v)
}


type ForwardIter struct {
	enumerator *b.Enumerator
	start      []byte
	end        []byte
	key        []byte
	value      b.Value
	err        error
}
type BackwardIter struct {
	enumerator *b.Enumerator
	start      []byte
	end        []byte
	key        []byte
	value      b.Value
	err        error
}
var _ types.ObjIterator = (*ForwardIter)(nil)
var _ types.ObjIterator = (*BackwardIter)(nil)

func (iter *ForwardIter) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}
func (iter *ForwardIter) Valid() bool {
	return iter.err == nil && !iter.value.IsDeleted()
}
func (iter *ForwardIter) Next() {
	iter.key, iter.value, iter.err = iter.enumerator.Next()
	if bytes.Compare(iter.key, iter.end) >= 0 {
		iter.err = io.EOF
	}
}
func (iter *ForwardIter) Key() []byte {
	return iter.key
}
func (iter *ForwardIter) Value() []byte {
	if iter.value.IsDeleted() {
		return nil
	}
	obj := iter.value.GetObj()
	bz, isRawBytes := obj.([]byte)
	if isRawBytes {
		return bz
	} else {
		return obj.(types.Serializable).ToBytes()
	}
}
func (iter *ForwardIter) ObjValue(ptr *types.Serializable) {
	if iter.value.IsDeleted() {
		*ptr = nil
	}
	obj := iter.value.GetObj()
	bz, isRawBytes := obj.([]byte)
	if isRawBytes {
		(*ptr).FromBytes([]byte(bz))
	} else {
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(obj))
	}
}
func (iter *ForwardIter) Close() {
	iter.enumerator.Close()
}

func (iter *BackwardIter) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}
func (iter *BackwardIter) Valid() bool {
	return iter.err == nil && !iter.value.IsDeleted()
}
func (iter *BackwardIter) Next() {
	iter.key, iter.value, iter.err = iter.enumerator.Prev()
	if bytes.Compare(iter.key, iter.start) < 0 {
		iter.err = io.EOF
	}
}
func (iter *BackwardIter) Key() []byte {
	return iter.key
}
func (iter *BackwardIter) Value() []byte {
	if iter.value.IsDeleted() {
		return nil
	}
	obj := iter.value.GetObj()
	bz, isRawBytes := obj.([]byte)
	if isRawBytes {
		return bz
	} else {
		return obj.(types.Serializable).ToBytes()
	}
}
func (iter *BackwardIter) ObjValue(ptr *types.Serializable) {
	if iter.value.IsDeleted() {
		*ptr = nil
	}
	obj := iter.value.GetObj()
	bz, isRawBytes := obj.([]byte)
	if isRawBytes {
		(*ptr).FromBytes([]byte(bz))
	} else {
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(obj))
	}
}
func (iter *BackwardIter) Close() {
	iter.enumerator.Close()
}

func (cs *CacheStore) Iterator(start, end []byte) types.ObjIterator {
	iter := &ForwardIter{start:start, end:end}
	if bytes.Compare(start, end) >= 0 {
		iter.err = io.EOF
		return iter
	}
	iter.enumerator, _ = cs.bt.Seek(start)
	iter.Next() //fill key, value, err
	return iter
}

func (cs *CacheStore) ReverseIterator(start, end []byte) types.ObjIterator {
	iter := &BackwardIter{start:start, end:end}
	if bytes.Compare(start, end) >= 0 {
		iter.err = io.EOF
		return iter
	}
	iter.enumerator, _ = cs.bt.Seek(end)
	//now iter.enumerator >= k, we want end is exclusive
	iter.enumerator.Prev()
	iter.Next() //fill key, value, err
	return iter
}

