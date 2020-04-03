package store

import (
	"bytes"

	"github.com/coinexchain/onvakv/store/types"
)

type PrefixedStore struct {
	parent *MultiStore
	prefix []byte
}

var _ types.KObjStore = PrefixedStore{}

func NewPrefixedStore(parent *MultiStore, prefix []byte) PrefixedStore {
	return PrefixedStore{
		parent: parent,
		prefix: prefix,
	}
}

func cloneAppend(bz []byte, tail []byte) (res []byte) {
	res = make([]byte, len(bz)+len(tail))
	copy(res, bz)
	copy(res[len(bz):], tail)
	return
}

func (s PrefixedStore) key(key []byte) (res []byte) {
	if key == nil {
		panic("nil key on Store")
	}
	res = cloneAppend(s.prefix, key)
	return
}

// Implements KObjStore
func (s PrefixedStore) Get(key []byte) []byte {
	res := s.parent.Get(s.key(key))
	return res
}

// Implements KObjStore
func (s PrefixedStore) GetObj(key []byte, ptr *types.Serializable) {
	s.parent.GetObj(s.key(key), ptr)
}

// Implements KObjStore
func (s PrefixedStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
	s.parent.GetReadOnlyObj(s.key(key), ptr)
}

// Implements KObjStore
func (s PrefixedStore) Has(key []byte) bool {
	return s.parent.Has(s.key(key))
}

// Implements KObjStore
func (s PrefixedStore) Set(key, value []byte) {
	if value == nil {
		panic("value can not be nil")
	}
	s.parent.Set(s.key(key), value)
}

// Implements KObjStore
func (s PrefixedStore) SetObj(key []byte, obj types.Serializable) {
	if obj == nil {
		panic("value can not be nil")
	}
	s.parent.SetObj(s.key(key), obj)
}

// Implements KObjStore
func (s PrefixedStore) Delete(key []byte) {
	s.parent.Delete(s.key(key))
}

// Implements KObjStore
func (s PrefixedStore) Iterator(start, end []byte) types.ObjIterator {
	if start == nil || end == nil {
		panic("OnvaKV does not support nil slices for iterator")
	}

	newstart := cloneAppend(s.prefix, start)

	newend := cloneAppend(s.prefix, end)

	iter := s.parent.Iterator(newstart, newend)

	return newPrefixIterator(s.prefix, start, end, iter)
}

// Implements KObjStore
func (s PrefixedStore) ReverseIterator(start, end []byte) types.ObjIterator {
	if start == nil || end == nil {
		panic("OnvaKV does not support nil slices for iterator")
	}
	newstart := cloneAppend(s.prefix, start)

	newend := cloneAppend(s.prefix, end)

	iter := s.parent.ReverseIterator(newstart, newend)

	return newPrefixIterator(s.prefix, start, end, iter)
}

var _ types.ObjIterator = (*prefixIterator)(nil)

type prefixIterator struct {
	prefix     []byte
	start, end []byte
	iter       types.ObjIterator
	valid      bool
}

func newPrefixIterator(prefix, start, end []byte, parent types.ObjIterator) *prefixIterator {
	return &prefixIterator{
		prefix: prefix,
		start:  start,
		end:    end,
		iter:   parent,
		valid:  parent.Valid() && bytes.HasPrefix(parent.Key(), prefix),
	}
}

// Implements ObjIterator
func (iter *prefixIterator) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}

// Implements ObjIterator
func (iter *prefixIterator) Valid() bool {
	return iter.valid && iter.iter.Valid()
}

// Implements ObjIterator
func (iter *prefixIterator) Next() {
	if !iter.valid {
		panic("prefixIterator invalid, cannot call Next()")
	}
	iter.iter.Next()
	if !iter.iter.Valid() || !bytes.HasPrefix(iter.iter.Key(), iter.prefix) {
		iter.valid = false
	}
}

// Implements ObjIterator
func (iter *prefixIterator) Key() (key []byte) {
	if !iter.valid {
		panic("prefixIterator invalid, cannot call Key()")
	}
	key = iter.iter.Key()
	key = stripPrefix(key, iter.prefix)
	return
}

// Implements ObjIterator
func (iter *prefixIterator) Value() []byte {
	if !iter.valid {
		panic("prefixIterator invalid, cannot call Value()")
	}
	return iter.iter.Value()
}

// Implements ObjIterator
func (iter *prefixIterator) ObjValue(ptr *types.Serializable) {
	if !iter.valid {
		panic("prefixIterator invalid, cannot call Value()")
	}
	iter.iter.ObjValue(ptr)
}

// Implements ObjIterator
func (iter *prefixIterator) Close() {
	iter.iter.Close()
}

// copied from github.com/tendermint/tendermint/libs/db/prefix_db.go
func stripPrefix(key []byte, prefix []byte) []byte {
	if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
		panic("should not happen")
	}
	return key[len(prefix):]
}

