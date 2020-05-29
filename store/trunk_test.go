package store

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/coinexchain/onvakv"
	"github.com/coinexchain/onvakv/store/types"
)

type TestOp struct {
	isDel  bool
	ignore bool
	key    []byte
	value  []byte
	obj    *Coord
}

func runList(ts *TrunkStore, opList []TestOp) {
	for _, op := range opList {
		if op.isDel {
			ts.PrepareForDeletion(op.key)
		} else {
			ts.PrepareForUpdate(op.key)
		}
	}
	ts.Update(func(cache *CacheStore) {
		for _, op := range opList {
			if op.ignore {
				continue
			}
			if op.isDel {
				cache.Delete(op.key)
			} else if op.obj != nil {
				cache.SetObj(op.key, op.obj)
			} else {
				cache.Set(op.key, op.value)
			}
		}
	})
}

func getListAdd() []TestOp {
	return []TestOp{
		{isDel: false, ignore: false, key: []byte("03210"), value: []byte("00"), obj: nil},
		{isDel: false, ignore: false, key: []byte("03211"), value: []byte("10"), obj: nil},
		{isDel: false, ignore: false, key: []byte("03212"), value: []byte("20"), obj: nil},
		{isDel: false, ignore: false, key: []byte("03213"), value: []byte{3,0,0,0,1,0,0,0}, obj: nil},
		{isDel: false, ignore: false, key: []byte("03214"), value: nil, obj: &Coord{x:4, y:4}},
		{isDel: false, ignore: false, key: []byte("03215"), value: []byte("50"), obj: nil},
		{isDel: false, ignore: false, key: []byte("03216"), value: []byte("60"), obj: nil},
		{isDel: false, ignore: false, key: []byte("03217"), value: []byte("70"), obj: nil},
		{isDel: false, ignore: false, key: []byte("43218"), value: []byte("80"), obj: nil},
		{isDel: false, ignore: false, key: []byte("43219"), value: []byte("90"), obj: nil},
		{isDel: false, ignore: false, key: []byte("4321a"), value: []byte{1,0,0,0,3,0,0,0}, obj: nil},
		{isDel: false, ignore: false, key: []byte("4321b"), value: nil, obj: &Coord{x:8, y:8}},
		{isDel: false, ignore: false, key: []byte("4321c"), value: []byte("c0"), obj: nil},
		{isDel: false, ignore: false, key: []byte("4321d"), value: []byte("d0"), obj: nil},
		{isDel: false, ignore: false, key: []byte("4321e"), value: nil, obj: &Coord{x:3, y:3}},
		{isDel: false, ignore: false, key: []byte("4321f"), value: []byte("f0"), obj: nil},
	}
}

func getListDel() []TestOp {
	return []TestOp{
		{isDel: true, ignore: true, key: []byte("03210"), value: []byte("00"), obj: nil},
		{isDel: true, ignore: false, key: []byte("03211"), value: []byte("10"), obj: nil},
		{isDel: true, ignore: false, key: []byte("03212"), value: []byte("20"), obj: nil},
		{isDel: true, ignore: false, key: []byte("03217"), value: []byte("70"), obj: nil},
		{isDel: false, ignore: false, key: []byte("43218"), value: []byte{88,0,0,0,88,0,0,0}, obj: nil},
		{isDel: true, ignore: true, key: []byte("4321b"), value: nil, obj: &Coord{x:8, y:8}},
		{isDel: true, ignore: false, key: []byte("4321e"), value: nil, obj: nil},
	}
}

func TestTrunk(t *testing.T) {
	okv := onvakv.NewOnvaKV4Mock()
	first := []byte{0}
	last := []byte{255,255,255,255,255,255}
	okv.InitGuards(first, last)

	root := NewRootStore(okv, nil, func(k []byte) bool {
		return k[0] != byte('0')
	})
	root.SetHeight(1)
	ts := root.GetTrunkStore()

	list1 := getListAdd()
	runList(ts, list1)

	var check1 = func() {
		var ptr types.Serializable
		ptr = &Coord{}
		assert.Equal(t, true, ts.Has([]byte("03210")))
		assert.Equal(t, false, ts.Has([]byte("0321x")))
		assert.Equal(t, []byte("00"), ts.Get([]byte("03210")))
		assert.Equal(t, []byte("10"), ts.Get([]byte("03211")))
		assert.Equal(t, []byte("20"), ts.Get([]byte("03212")))
		ts.GetObj([]byte("03213"), &ptr)
		assert.Equal(t, &Coord{x:3, y:1}, ptr)
		ts.GetObjCopy([]byte("03213"), &ptr)
		assert.Equal(t, &Coord{x:3, y:1}, ptr)
		ts.GetReadOnlyObj([]byte("03213"), &ptr)
		assert.Equal(t, &Coord{x:3, y:1}, ptr)
		assert.Equal(t, []byte{4,0,0,0,4,0,0,0}, ts.Get([]byte("03214")))
		assert.Equal(t, []byte("50"), ts.Get([]byte("03215")))
		assert.Equal(t, []byte("60"), ts.Get([]byte("03216")))
		assert.Equal(t, []byte("70"), ts.Get([]byte("03217")))
		assert.Equal(t, []byte("80"), ts.Get([]byte("43218")))
		assert.Equal(t, []byte("90"), ts.Get([]byte("43219")))
		ts.GetObj([]byte("4321a"), &ptr)
		assert.Equal(t, &Coord{x:1, y:3}, ptr)
		ts.GetObjCopy([]byte("4321a"), &ptr)
		assert.Equal(t, &Coord{x:1, y:3}, ptr)
		assert.Equal(t, []byte{8,0,0,0,8,0,0,0}, ts.Get([]byte("4321b")))
		assert.Equal(t, []byte("c0"), ts.Get([]byte("4321c")))
		assert.Equal(t, []byte("d0"), ts.Get([]byte("4321d")))
		ts.GetReadOnlyObj([]byte("4321e"), &ptr)
		assert.Equal(t, &Coord{x:3, y:3}, ptr)
		assert.Equal(t, []byte("f0"), ts.Get([]byte("4321f")))

		//{isDel: false, ignore: false, key: []byte("03212"), value: []byte("20"), obj: nil},
		//{isDel: false, ignore: false, key: []byte("03213"), value: []byte{3,0,0,0,1,0,0,0}, obj: nil},
		//{isDel: false, ignore: false, key: []byte("03214"), value: nil, obj: &Coord{x:4, y:4}},
		iter := ts.Iterator([]byte("03212"), []byte("032144"))
		start, end := iter.Domain()
		assert.Equal(t, []byte("03212"), start)
		assert.Equal(t, []byte("032144"), end)
		assert.Equal(t, true, iter.Valid())
		assert.Equal(t, []byte("20"), iter.Value())
		iter.Next()
		assert.Equal(t, []byte("03213"), iter.Key())
		ptr = &Coord{}
		iter.ObjValue(&ptr)
		assert.Equal(t, &Coord{x:3, y:1}, ptr)
		iter.Next()
		assert.Equal(t, []byte("03214"), iter.Key())
		ptr = &Coord{}
		iter.ObjValue(&ptr)
		assert.Equal(t, &Coord{x:4, y:4}, ptr)
		iter.Close()

		//{isDel: fale, ignore: false, key: []byte("43219"), value: []byte("90"), obj: nil},
		//{isDel: false, ignore: false, key: []byte("4321a"), value: []byte{1,0,0,0,3,0,0,0}, obj: nil},
		//{isDel: false, ignore: false, key: []byte("4321b"), value: nil, obj: &Coord{x:8, y:8}},
		assert.Equal(t, []byte{8,0,0,0,8,0,0,0}, ts.Get([]byte("4321b")))
		iter = ts.ReverseIterator([]byte("432190"), []byte("4321b0"))
		assert.Equal(t, []byte("4321b"), iter.Key())
		fmt.Printf("----------- 4321b\n")
		ptr = &Coord{}
		iter.ObjValue(&ptr)
		assert.Equal(t, &Coord{x:8, y:8}, ptr)
		iter.Next()
		assert.Equal(t, []byte{1,0,0,0,3,0,0,0}, iter.Value())
		iter.Next()
		assert.Equal(t, false, iter.Valid())
		iter.Close()
	}

	check1()
	ts.Close(true)

	root.SetHeight(2)
	ts = root.GetTrunkStore()
	check1()

	//=========
	list2 := getListDel()
	runList(ts, list2)

	var check2 = func() {
		var ptr types.Serializable
		ptr = &Coord{}
		assert.Equal(t, []byte("00"), ts.Get([]byte("03210")))
		ts.GetObj([]byte("03211"), &ptr)
		assert.Nil(t, ptr)
		ts.GetObjCopy([]byte("03212"), &ptr)
		assert.Nil(t, ptr)
		assert.Equal(t, false, ts.Has([]byte("03212")))
		ptr = &Coord{}
		ts.GetReadOnlyObj([]byte("4321b"), &ptr)
		assert.Equal(t, &Coord{x:8, y:8}, ptr)
		ts.GetReadOnlyObj([]byte("03217"), &ptr)
		assert.Nil(t, ptr)
		assert.Nil(t, ts.Get([]byte("03217")))
		assert.Equal(t, []byte{88,0,0,0,88,0,0,0}, ts.Get([]byte("43218")))
		ts.GetObjCopy([]byte("0321b"), &ptr)
		assert.Nil(t, ptr)
		ts.GetReadOnlyObj([]byte("4321e"), &ptr)
		assert.Nil(t, ptr)

		//{isDel: false, ignore: false, key: []byte("03210"), value: []byte("00"), obj: nil},
		//{isDel: false, ignore: false, key: []byte("03213"), value: []byte{3,0,0,0,1,0,0,0}, obj: nil},
		fmt.Printf("--------------03210 Iter\n")
		iter := ts.Iterator([]byte("03210"), []byte("03214"))
		fmt.Printf("--------------03210\n")
		assert.Equal(t, []byte("03210"), iter.Key())
		iter.Next()
		assert.Equal(t, []byte("03213"), iter.Key())
		iter.Next()
		assert.Equal(t, false, iter.Valid())
		iter.Close()

		//{isDel: false, ignore: false, key: []byte("4321d"), value: []byte("d0"), obj: nil},
		//{isDel: false, ignore: false, key: []byte("4321f"), value: []byte("f0"), obj: nil},
		iter = ts.ReverseIterator([]byte("4321d0"), []byte("4321f0"))
		assert.Equal(t, []byte("4321f"), iter.Key())
		assert.Equal(t, []byte("f0"), iter.Value())
		iter.Next()
		assert.Equal(t, false, iter.Valid())
		iter.Close()
	}

	check2()
	ts.Close(true)

	root.SetHeight(3)
	ts = root.GetTrunkStore()
	check2()

	ts.Close(false)

	okv.Close()
	os.RemoveAll("./rocksdb.db")
}

//func (ts *TrunkStore) Has(key []byte) bool {
//func (ts *TrunkStore) Get(key []byte) []byte {
//func (ts *TrunkStore) GetObjCopy(key []byte, ptr *types.Serializable) {
//func (ts *TrunkStore) GetObj(key []byte, ptr *types.Serializable) {
//func (ts *TrunkStore) GetReadOnlyObj(key []byte, ptr *types.Serializable) {
//func (ts *TrunkStore) Iterator(start, end []byte) types.ObjIterator {
//func (ts *TrunkStore) ReverseIterator(start, end []byte) types.ObjIterator {
//func (ts *TrunkStore) Close(writeBack bool) {


