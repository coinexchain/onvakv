package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/coinexchain/onvakv"
	"github.com/coinexchain/onvakv/store/types"
)

func TestPrefix(t *testing.T) {
	okv := onvakv.NewOnvaKV4Mock()
	first := []byte{0}
	last := []byte{255,255,255,255,255,255}
	okv.InitGuards(first, last)

	storeKeys := make(map[types.StoreKey]struct{})
	keyAll := types.NewStrStoreKey("storekey-all","")
	storeKeys[keyAll] = struct{}{}
	keyA := types.NewStrStoreKey("storekey-a","a")
	storeKeys[keyA] = struct{}{}
	keyB := types.NewStrStoreKey("storekey-b","b")
	storeKeys[keyB] = struct{}{}
	//keyC := types.NewStrStoreKey("storekey-c","c")
	//storeKeys[keyC] = struct{}{}
	root := NewRootStore(okv, storeKeys, func(k []byte) bool {
		return false // no cache at all
	})
	root.SetHeight(1)
	ts := root.GetTrunkStore()

	tx1Store := ts.Cached()
	tx2Store := ts.Cached()
	tx1StoreA := tx1Store.SubStore(keyA)
	tx1StoreB := tx1Store.SubStore(keyB)
	tx2StoreA := tx2Store.SubStore(keyA)
	tx2StoreB := tx2Store.SubStore(keyB)

	tx1StoreA.Set([]byte("A1-22"), []byte{2,0,0,0,2,0,0,0})
	tx1StoreA.Set([]byte("A1-23"), []byte{2,0,0,0,3,0,0,0})
	tx1StoreB.Set([]byte("B1-88"), []byte{8,0,0,0,8,0,0,0})
	tx1StoreB.SetObj([]byte("B1-89"), &Coord{x:8, y:9})

	tx2StoreA.Set([]byte("A2-22"), []byte{22,0,0,0,22,0,0,0})
	tx2StoreA.Set([]byte("A2-23"), []byte{22,0,0,0,33,0,0,0})
	tx2StoreB.Set([]byte("B2-88"), []byte{88,0,0,0,88,0,0,0})
	tx2StoreB.SetObj([]byte("B2-89"), &Coord{x:88, y:99})

	assert.Equal(t, true, tx1StoreA.Has([]byte("A1-22")))
	assert.Equal(t, false, tx2StoreA.Has([]byte("A1-22")))
	assert.Equal(t, []byte{2,0,0,0,2,0,0,0}, tx1StoreA.Get([]byte("A1-22")))
	assert.Equal(t, []byte{88,0,0,0,99,0,0,0}, tx2StoreB.Get([]byte("B2-89")))
	var ptr types.Serializable
	ptr = &Coord{}
	tx1StoreA.GetObj([]byte("A1-23"), &ptr)
	assert.Equal(t, &Coord{x:2, y:3}, ptr)
	ptr = &Coord{}
	tx2StoreB.GetReadOnlyObj([]byte("B2-88"), &ptr)
	assert.Equal(t, &Coord{x:88, y:88}, ptr)

	var checkIterA = func(iter types.ObjIterator) {
		assert.Equal(t, []byte("A1-22"), iter.Key())
		assert.Equal(t, []byte{2,0,0,0,2,0,0,0}, iter.Value())
		iter.Next()
		assert.Equal(t, []byte("A1-23"), iter.Key())
		var ptr types.Serializable
		ptr = &Coord{}
		iter.ObjValue(&ptr)
		assert.Equal(t, &Coord{x:2, y:3}, ptr)
		iter.Next()
	}
	var checkIterB = func(iter types.ObjIterator) {
		assert.Equal(t, []byte("B2-89"), iter.Key())
		assert.Equal(t, []byte{88,0,0,0,99,0,0,0}, iter.Value())
		iter.Next()
		assert.Equal(t, []byte("B2-88"), iter.Key())
		var ptr types.Serializable
		ptr = &Coord{}
		iter.ObjValue(&ptr)
		assert.Equal(t, &Coord{x:88, y:88}, ptr)
		iter.Next()
		assert.Equal(t, false, iter.Valid())
		iter.Close()
	}
	iter := tx1StoreA.Iterator([]byte("A1-21"), []byte("A1-24"))
	start, end := iter.Domain()
	assert.Equal(t, []byte("A1-21"), start)
	assert.Equal(t, []byte("A1-24"), end)
	checkIterA(iter)
	assert.Equal(t, false, iter.Valid())
	iter.Close()
	iter = tx2StoreB.ReverseIterator([]byte("B2-87"), []byte("B2-899"))
	start, end = iter.Domain()
	assert.Equal(t, []byte("B2-87"), start)
	assert.Equal(t, []byte("B2-899"), end)
	checkIterB(iter)

	tx1Store.Close(true)
	tx2Store.Close(true)
	tx1Store = ts.Cached()
	tx2Store = ts.Cached()
	tx1StoreA = tx1Store.SubStore(keyA)
	tx1StoreB = tx1Store.SubStore(keyB)
	tx2StoreA = tx2Store.SubStore(keyA)
	tx2StoreB = tx2Store.SubStore(keyB)

	assert.Equal(t, true, tx2StoreA.Has([]byte("A1-22")))
	assert.Equal(t, true, tx1StoreA.Has([]byte("A2-22")))
	assert.Equal(t, []byte{2,0,0,0,2,0,0,0}, tx2StoreA.Get([]byte("A1-22")))
	assert.Equal(t, []byte{88,0,0,0,99,0,0,0}, tx1StoreB.Get([]byte("B2-89")))
	tx2StoreA.GetObj([]byte("A1-23"), &ptr)
	assert.Equal(t, &Coord{x:2, y:3}, ptr)
	ptr = &Coord{}
	tx1StoreB.GetReadOnlyObj([]byte("B2-88"), &ptr)
	assert.Equal(t, &Coord{x:88, y:88}, ptr)

	tx1StoreA.Delete([]byte("A2-22"))
	tx2StoreA.Delete([]byte("A1-22")) // will not write back
	assert.Nil(t, tx1StoreA.Get([]byte("A2-22")))
	assert.Equal(t, false, tx2StoreA.Has([]byte("A1-22")))
	ptr = &Coord{}
	tx2StoreA.GetReadOnlyObj([]byte("A1-22"), &ptr)
	assert.Nil(t, ptr)
	tx2StoreA.GetObj([]byte("A1-22"), &ptr)
	assert.Nil(t, ptr)

	tx1Store.Close(true)
	tx2Store.Close(false)
	tx1Store = ts.Cached()
	tx2Store = ts.Cached()
	tx1StoreA = tx1Store.SubStore(keyA)
	tx1StoreB = tx1Store.SubStore(keyB)
	tx2StoreA = tx2Store.SubStore(keyA)
	tx2StoreB = tx2Store.SubStore(keyB)

	assert.Equal(t, []byte{2,0,0,0,2,0,0,0}, tx2StoreA.Get([]byte("A1-22")))
	assert.Nil(t, tx2StoreA.Get([]byte("A2-22")))

	tx2StoreAll := tx2Store.SubStore(keyAll)
	assert.Equal(t, []byte{2,0,0,0,2,0,0,0}, tx2StoreAll.Get([]byte("aA1-22")))
	assert.Nil(t, tx2StoreAll.Get([]byte("aA2-22")))
	iter = tx2StoreAll.Iterator([]byte("aA2-22"), []byte("bB1-888"), )
	assert.Equal(t, []byte("aA2-23"), iter.Key()) //aA2-22 was deleted
	iter.Next()
	assert.Equal(t, []byte("bB1-88"), iter.Key())
	iter.Next()
	assert.Equal(t, false, iter.Valid())
	iter.Close()

	txStoreAll := tx2Store.SubStore(nil)
	assert.Equal(t, []byte{2,0,0,0,2,0,0,0}, txStoreAll.Get([]byte("aA1-22")))
	assert.Nil(t, txStoreAll.Get([]byte("aA2-22")))
	iter = txStoreAll.Iterator([]byte("aA2-22"), []byte("bB1-888"), )
	assert.Equal(t, []byte("aA2-23"), iter.Key()) //aA2-22 was deleted
	iter.Next()
	assert.Equal(t, []byte("bB1-88"), iter.Key())
	iter.Next()
	assert.Equal(t, false, iter.Valid())
	iter.Close()
}


//func (s PrefixedStore) Get(key []byte) []byte {
//func (s PrefixedStore) GetObj(key []byte, ptr *types.Serializable) {
//func (s PrefixedStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
//func (s PrefixedStore) Has(key []byte) bool {
//func (s PrefixedStore) Set(key, value []byte) {
//func (s PrefixedStore) SetObj(key []byte, obj types.Serializable) {
//func (s PrefixedStore) Delete(key []byte) {
//func (s PrefixedStore) Iterator(start, end []byte) types.ObjIterator {
//func (s PrefixedStore) ReverseIterator(start, end []byte) types.ObjIterator {
//
//func (iter *prefixIterator) Domain() ([]byte, []byte) {
//func (iter *prefixIterator) Valid() bool {
//func (iter *prefixIterator) Next() {
//func (iter *prefixIterator) Key() (key []byte) {
//func (iter *prefixIterator) Value() []byte {
//func (iter *prefixIterator) ObjValue(ptr *types.Serializable) {
//func (iter *prefixIterator) Close() {

