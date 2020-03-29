package store

import (
	"fmt"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/coinexchain/onvakv/store/types"
)

type Coord struct {
	x, y uint32
}
func (coord *Coord) ToBytes() []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint32(buf[:4], coord.x)
	binary.LittleEndian.PutUint32(buf[4:], coord.y)
	return buf[:]
}

func (coord *Coord) FromBytes(buf []byte) {
	if len(buf) != 8 {
		panic("length is not 8")
	}
	coord.x = binary.LittleEndian.Uint32(buf[:4])
	coord.y = binary.LittleEndian.Uint32(buf[4:])
}

func (coord *Coord) DeepCopy() interface{} {
	return &Coord{
		x: coord.x,
		y: coord.y,
	}
}

func TestCacheBasic(t *testing.T) {
	cs := NewCacheStore()
	coord1 := &Coord{x: 1, y: 2}
	coord2 := &Coord{x:11, y: 0}
	coord3 := &Coord{x:99, y: 9}
	coord4 := &Coord{x: 8, y:44}

	assert.NotPanics(t, func() {
		cs.ScanAllEntries(func(key []byte, obj interface{}, isDeleted bool) {
			panic("Should not reach here")
		})
	})

	// Add 4 values
	cs.Set([]byte("0102"), coord1.ToBytes())
	cs.Set([]byte("1100"), coord2.ToBytes())
	cs.SetObj([]byte("9909"), coord3)
	cs.SetObj([]byte("0844"), coord4)

	res, status := cs.Get([]byte("0102")) // bytes to bytes
	assert.Equal(t, types.Hit, status)
	assert.Equal(t, []byte{1, 0, 0, 0, 2, 0, 0, 0}, res)
	fmt.Printf("%#v\n", res)
	_, status = cs.Get([]byte("0201")) // no such key
	assert.Equal(t, types.Missed, status)
	res, status = cs.Get([]byte("0844")) // bytes to object
	assert.Equal(t, types.Hit, status)
	assert.Equal(t, []byte{8, 0, 0, 0, 44, 0, 0, 0}, res)

	var coord5 Coord
	var ptr types.Serializable
	ptr = &coord5

	status = cs.GetReadOnlyObj([]byte("0201"), &ptr) // no such key
	assert.Equal(t, types.Missed, status)

	status = cs.GetObjCopy([]byte("1100"), &ptr) // bytes to object
	assert.Equal(t, types.Hit, status)
	assert.Equal(t, coord2, ptr)
	assert.NotPanics(t, func() { // should not panic because it was copied
		cs.ScanAllEntries(func(key []byte, obj interface{}, isDeleted bool) { })
	})
	status = cs.GetObj([]byte("1100"), &ptr) // bytes to object
	assert.Equal(t, types.Hit, status)
	assert.Equal(t, coord2, ptr)
	obj := ptr.(*Coord)
	obj.y++
	assert.NotPanics(t, func() { // NotPanic because this entry contains bytes
		cs.ScanAllEntries(func(key []byte, obj interface{}, isDeleted bool) { })
	})
	cs.SetObj([]byte("1100"), obj)
	assert.NotPanics(t, func() { // NotPanic because the obj is returned
		cs.ScanAllEntries(func(key []byte, obj interface{}, isDeleted bool) { })
	})
	res, status = cs.Get([]byte("1100"))
	assert.Equal(t, types.Hit, status)
	assert.Equal(t, []byte{11, 0, 0, 0, 1, 0, 0, 0}, res) // y was increased to 1

	status = cs.GetObjCopy([]byte("0844"), &ptr) // obj to obj
	assert.Equal(t, types.Hit, status)
	assert.Equal(t, coord4, ptr)
	status = cs.GetObj([]byte("0844"), &ptr) // obj to obj
	assert.Equal(t, types.Hit, status)
	assert.Equal(t, coord4, ptr)
	assert.Panics(t, func() { // Panic because the obj is moved out
		cs.ScanAllEntries(func(key []byte, obj interface{}, isDeleted bool) { })
	})
	cs.SetObj([]byte("0844"), ptr.(*Coord))
	refList := []string{
		"0102([]byte{0x30, 0x31, 0x30, 0x32}) obj: []byte{0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0} isDel: false",
		"0844([]byte{0x30, 0x38, 0x34, 0x34}) obj: &store.Coord{x:0x8, y:0x2c} isDel: false",
		"1100([]byte{0x31, 0x31, 0x30, 0x30}) obj: &store.Coord{x:0xb, y:0x1} isDel: false",
		"9909([]byte{0x39, 0x39, 0x30, 0x39}) obj: &store.Coord{x:0x63, y:0x9} isDel: false",
	}
	impList := make([]string, 0, 4)
	cs.ScanAllEntries(func(key []byte, obj interface{}, isDeleted bool) {
		s := fmt.Sprintf("%s(%#v) obj: %#v isDel: %v", string(key), key, obj, isDeleted)
		impList = append(impList, s)
	})
	assert.Equal(t, refList, impList) // check all the contents

	status = cs.GetReadOnlyObj([]byte("0102"), &ptr) //bytes to obj
	assert.Equal(t, coord1, ptr)
	status = cs.GetReadOnlyObj([]byte("9909"), &ptr) //obj to obj
	assert.Equal(t, coord3, ptr)
	status = cs.GetObjCopy([]byte("9909"), &ptr) //obj to obj
	assert.Equal(t, coord3, ptr)

	status = cs.GetObj([]byte("0800"), &ptr) // no such key
	assert.Equal(t, types.Missed, status)

	status = cs.GetObjCopy([]byte("8888"), &ptr) // no such key
	assert.Equal(t, types.Missed, status)


	cs.Delete([]byte("1100")) // delete an entry
	_, status = cs.Get([]byte("1100"))
	assert.Equal(t, types.JustDeleted, status)
	status = cs.GetObj([]byte("1100"), &ptr)
	assert.Equal(t, types.JustDeleted, status)
	status = cs.GetObjCopy([]byte("1100"), &ptr)
	assert.Equal(t, types.JustDeleted, status)
	status = cs.GetReadOnlyObj([]byte("1100"), &ptr)
	assert.Equal(t, types.JustDeleted, status)

	cs.Close()
}

func TestCacheIter(t *testing.T) {
	cs := NewCacheStore()
	coord1 := &Coord{x: 1, y: 1}
	coord2 := &Coord{x: 2, y: 2}
	coord3 := &Coord{x: 3, y: 3}
	coord4 := &Coord{x: 4, y: 4}
	coord5 := &Coord{x: 5, y: 5}
	coord6 := &Coord{x: 6, y: 6}

	// Add 6 values
	cs.Set([]byte("1101"), coord1.ToBytes())
	cs.Set([]byte("1102"), coord2.ToBytes())
	cs.SetObj([]byte("1103"), coord3)
	cs.SetObj([]byte("1104"), coord4)
	cs.SetObj([]byte("1105"), coord5)
	cs.SetObj([]byte("1106"), coord6)

	var coordX Coord
	var ptr types.Serializable
	ptr = &coordX

	iter := cs.ReverseIterator([]byte("2105"), []byte("1106"))
	assert.Equal(t, false, iter.Valid())
	iter.Close()
	iter = cs.Iterator([]byte("2105"), []byte("1106"))
	assert.Equal(t, false, iter.Valid())
	iter.Close()

	iter = cs.ReverseIterator([]byte("1105"), []byte("1106"))
	assert.Equal(t, "1105", string(iter.Key()))
	assert.Equal(t, []byte{5, 0, 0, 0, 5, 0, 0, 0}, iter.Value())
	iter.ObjValue(&ptr)
	assert.Equal(t, coord5, ptr)
	iter.Next()
	assert.Equal(t, false, iter.Valid())
	assert.Nil(t, iter.Value())
	iter.ObjValue(&ptr)
	assert.Nil(t, ptr)
	iter.Close()

	iter = cs.Iterator([]byte("1101"), []byte("1102"))
	assert.Equal(t, "1101", string(iter.Key()))
	assert.Equal(t, []byte{1, 0, 0, 0, 1, 0, 0, 0}, iter.Value())
	ptr = &coordX
	iter.ObjValue(&ptr)
	assert.Equal(t, coord1, ptr)
	iter.Next()
	assert.Equal(t, false, iter.Valid())
	assert.Nil(t, iter.Value())
	iter.ObjValue(&ptr)
	assert.Nil(t, ptr)
	start, end := iter.Domain()
	assert.Equal(t, "1101", string(start))
	assert.Equal(t, "1102", string(end))
	iter.Close()

	iter = cs.ReverseIterator([]byte("11011"), []byte("11022"))
	assert.Equal(t, "1102", string(iter.Key()))
	assert.Equal(t, []byte{2, 0, 0, 0, 2, 0, 0, 0}, iter.Value())
	ptr = &coordX
	iter.ObjValue(&ptr)
	assert.Equal(t, coord2, ptr)
	start, end = iter.Domain()
	assert.Equal(t, "11011", string(start))
	assert.Equal(t, "11022", string(end))
	iter.Close()

	iter = cs.Iterator([]byte("1104"), []byte("11054"))
	assert.Equal(t, "1104", string(iter.Key()))
	assert.Equal(t, []byte{4, 0, 0, 0, 4, 0, 0, 0}, iter.Value())
	iter.ObjValue(&ptr)
	assert.Equal(t, coord4, ptr)
	iter.Next()
	assert.Equal(t, "1105", string(iter.Key()))
	iter.Close()

	cs.Delete([]byte("1105"))
	cs.Delete([]byte("1104"))
	cs.Delete([]byte("1102"))
	iter = cs.ReverseIterator([]byte("11033"), []byte("11066"))
	assert.Equal(t, "1106", string(iter.Key()))
	assert.Equal(t, []byte{6, 0, 0, 0, 6, 0, 0, 0}, iter.Value())
	iter.Next()
	assert.Equal(t, false, iter.Valid())
	iter.Close()
	iter = cs.Iterator([]byte("1101"), []byte("11033"))
	assert.Equal(t, "1101", string(iter.Key()))
	iter.Next()
	assert.Equal(t, "1103", string(iter.Key()))
	assert.Equal(t, true, iter.Valid())
	iter.Next()
	assert.Equal(t, false, iter.Valid())
	iter.Close()
}

//func (iter *BackwardIter) Domain() ([]byte, []byte) {
//func (iter *BackwardIter) Valid() bool {
//func (iter *BackwardIter) Next() {
//func (iter *BackwardIter) Key() []byte {
//func (iter *BackwardIter) Value() []byte {
//func (iter *BackwardIter) ObjValue(ptr *types.Serializable) {
//func (iter *BackwardIter) Close() {
//func (cs *CacheStore) Iterator(start, end []byte) types.ObjIterator {
//func (cs *CacheStore) ReverseIterator(start, end []byte) types.ObjIterator {

