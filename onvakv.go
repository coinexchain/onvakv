package onvakv

import (
	"bytes"
	//"fmt"
	"os"
	"math"
	"sort"
	"sync"

	dbm "github.com/tendermint/tm-db"

	"github.com/coinexchain/onvakv/datatree"
	"github.com/coinexchain/onvakv/indextree"
	"github.com/coinexchain/onvakv/metadb"
	"github.com/coinexchain/onvakv/types"
)

const (
	defaultFileSize = 1024*1024*1024
	StartReapThres int64 = 1000 * 1000
	KeptEntriesToActiveEntriesRation = 3
)

type OnvaKV struct {
	meta          types.MetaDB
	idxTree       types.IndexTree
	datTree       types.DataTree
	rocksdb       *indextree.RocksDB
	rootHash      []byte
	k2eCache      *sync.Map
	cachedEntries []*EntryX
}

func NewOnvaKV4Mock() *OnvaKV {
	okv := &OnvaKV{k2eCache: &sync.Map{}}

	okv.datTree = datatree.NewMockDataTree()
	okv.idxTree = indextree.NewMockIndexTree()

	var err error
	okv.rocksdb, err = indextree.NewRocksDB("rocksdb", "./")
	if err != nil {
		panic(err)
	}

	okv.meta = metadb.NewMetaDB(okv.rocksdb)
	okv.rocksdb.OpenNewBatch()
	return okv
}

func NewOnvaKV(dirName string, queryHistory bool) (*OnvaKV, error) {
	_, err := os.Stat(dirName)
	dirNotExists := os.IsNotExist(err)
	okv := &OnvaKV{
		k2eCache:      &sync.Map{},
		cachedEntries: make([]*EntryX, 0, 2000),
	}
	if dirNotExists {
		os.Mkdir(dirName, 0700)
	}

	okv.rocksdb, err = indextree.NewRocksDB("rocksdb", dirName)
	if err != nil {
		panic(err)
	}
	okv.meta = metadb.NewMetaDB(okv.rocksdb)

	if dirNotExists { // Create a new database in this dir
		okv.datTree = datatree.NewEmptyTree(defaultFileSize, dirName)
		okv.meta.Init()
	} else if okv.meta.GetIsRunning() { // OnvaKV is *NOT* closed properly
		oldestActiveTwigID := okv.meta.GetOldestActiveTwigID()
		youngestTwigID := okv.meta.GetMaxSerialNum() >> datatree.TwigShift
		bz := okv.meta.GetEdgeNodes()
		edgeNodes := datatree.BytesToEdgeNodes(bz)
		okv.datTree = datatree.RecoverTree(defaultFileSize, dirName, edgeNodes,
			okv.meta.GetLastPrunedTwig(), oldestActiveTwigID, youngestTwigID)
	} else { // OnvaKV is closed properly
		okv.datTree = datatree.LoadTree(defaultFileSize, dirName)
	}

	if queryHistory { // use rocksdb to keep the historical index
		okv.idxTree = indextree.NewNVTreeMem(okv.rocksdb)
		err = okv.idxTree.Init(nil)
		if err != nil {
			return nil, err
		}
	} else { // only latest index, no historical index at all
		okv.idxTree = indextree.NewNVTreeMem(nil)
		oldestActiveTwigID := okv.meta.GetOldestActiveTwigID()
		okv.datTree.ScanEntries(oldestActiveTwigID, func(pos int64, entry *Entry, _ []int64) {
			okv.idxTree.Set(entry.Key, uint64(pos))
		})
	}

	okv.meta.SetIsRunning(true)
	return okv, nil
}

func (okv *OnvaKV) Close() {
	okv.meta.SetIsRunning(false)
	okv.idxTree.Close()
	okv.rocksdb.Close()
	okv.datTree.Sync()
	okv.datTree.Close()
	okv.meta.Close()
	okv.idxTree = nil
	okv.rocksdb = nil
	okv.datTree = nil
	okv.meta = nil
	okv.k2eCache = nil
}

type Entry = types.Entry
type EntryX = types.EntryX

func (okv *OnvaKV) GetRootHash() []byte {
	return append([]byte{}, okv.rootHash...)
}

func (okv *OnvaKV) GetEntry(k []byte) *Entry {
	pos, ok := okv.idxTree.Get(k)
	if !ok {
		return nil
	}
	return okv.datTree.ReadEntry(int64(pos))
}

func (okv *OnvaKV) PrepareForUpdate(k []byte) {
	//fmt.Printf("In PrepareForUpdate we see: %s\n", string(k))
	pos, findIt := okv.idxTree.Get(k)
	if findIt { // The case of Change
		//fmt.Printf("In PrepareForUpdate we update\n")
		entry := okv.datTree.ReadEntry(int64(pos))
		v, ok  := okv.k2eCache.Load(string(k))
		if !ok || v == nil {
			//fmt.Printf("Now we add entry to k2e(findIt): %s(%#v)\n", string(k), k)
			okv.k2eCache.Store(string(k), &EntryX{
				EntryPtr:  entry,
				Operation: types.OpNone,
			})
		}
		return
	}

	// The case of Insert
	//fmt.Printf("Now we add entry to k2e(not-findIt): %s(%#v)\n", string(k), k)
	okv.k2eCache.Store(string(k), &EntryX{
		EntryPtr: &Entry{
			Key:        k,
			Value:      nil,
			NextKey:    nil,
			Height:     0,
			LastHeight: 0,
			SerialNum:  -1, //inserted entries has negative SerialNum
		},
		Operation: types.OpNone,
	})

	//fmt.Printf("In PrepareForUpdate we insert\n")
	prevEntry := okv.getPrevEntry(k)
	//fmt.Printf("prevEntry(%#v): %#v\n", k, prevEntry)
	kStr := string(prevEntry.Key)
	v, ok  := okv.k2eCache.Load(kStr)
	if !ok || v == nil {
		//fmt.Printf("Now we add entry to k2e(prevEntry.Key): %s(%#v)\n", kStr, prevEntry.Key)
		okv.k2eCache.Store(kStr, &EntryX{
			EntryPtr:  prevEntry,
			Operation: types.OpNone,
		})
	}

	kStr = string(prevEntry.NextKey)
	_, ok = okv.k2eCache.Load(kStr)
	if !ok {
		//fmt.Printf("Now we add entry to k2e(prevEntry.NextKey): %s(%#v)\n", kStr, prevEntry.NextKey)
		okv.k2eCache.Store(kStr, nil) // we do not need next entry's value, so here we store nil
	} else {
		//fmt.Printf("Now we hit k2eCache: %s(%#v)\n", kStr, prevEntry.NextKey)
	}
}

func (okv *OnvaKV) PrepareForDeletion(k []byte) (findIt bool) {
	//fmt.Printf("In PrepareForDeletion we see: %s\n", string(k))
	pos, findIt := okv.idxTree.Get(k)
	if !findIt {
		return
	}

	entry := okv.datTree.ReadEntry(int64(pos))
	kStr := string(entry.Key)
	v, ok := okv.k2eCache.Load(kStr)
	if !ok || v == nil {
		okv.k2eCache.Store(kStr, &EntryX{
			EntryPtr:  entry,
			Operation: types.OpNone,
		})
	}

	prevEntry := okv.getPrevEntry(k)
	kStr = string(prevEntry.Key)
	v, ok = okv.k2eCache.Load(kStr)
	if !ok || v == nil {
		okv.k2eCache.Store(kStr, &EntryX{
			EntryPtr:  prevEntry,
			Operation: types.OpNone,
		})
	}

	kStr = string(entry.NextKey)
	_, ok = okv.k2eCache.Load(kStr)
	if !ok {
		okv.k2eCache.Store(kStr, nil) // we do not need next entry's value, so here we store nil
	}
	return
}

func makeFakeEntryX(key string) *EntryX {
	return &EntryX {
		EntryPtr: &Entry{
			Key:        []byte(key),
			Value:      nil,
			NextKey:    nil,
			Height:     -1,
			LastHeight: -1,
			SerialNum:  math.MaxInt64, // fake entry has largest possible SerialNum
		},
		Operation: types.OpNone,
	}
}

func (okv *OnvaKV) getPrevEntry(k []byte) *Entry {
	iter := okv.idxTree.ReverseIterator([]byte{}, k)
	if !iter.Valid() {
		panic("The iterator is invalid! Missing a guard node?")
	}
	pos := iter.Value()
	//fmt.Printf("In getPrevEntry: %#v %d\n", iter.Key(), iter.Value())
	return okv.datTree.ReadEntry(int64(pos))
}


const (
	MinimumTasksInGoroutine = 10
	MaximumGoroutines       = 128
)

func (okv *OnvaKV) numOfKeptEntries() int64 {
	return okv.meta.GetMaxSerialNum() - okv.meta.GetOldestActiveTwigID()*datatree.LeafCountInTwig
}

func (okv *OnvaKV) BeginWrite(height int64) {
	okv.rocksdb.OpenNewBatch()
	okv.idxTree.BeginWrite(height)
	okv.meta.SetCurrHeight(height)
}

func (okv *OnvaKV) Set(key, value []byte) {
	//fmt.Printf("In Set we see: %s %s\n", string(key), string(value))
	v, ok := okv.k2eCache.Load(string(key))
	if !ok {
		panic("Can not find entry in cache")
	}
	if v == nil {
		panic("Can not change or insert at a fake entry")
	}
	entry := v.(*EntryX)
	entry.EntryPtr.Value = value
	entry.Operation = types.OpInsertOrChange
}

func (okv *OnvaKV) Delete(key []byte) {
	//fmt.Printf("In Delete we see: %s(%#v)\n", string(key), key)
	v, ok := okv.k2eCache.Load(string(key))
	if !ok {
		panic("Can not find entry in cache")
	}
	if v == nil {
		panic("Can not delete a fake entry")
	}
	entry := v.(*EntryX)
	entry.Operation = types.OpDelete
}

func getPrev(cachedEntries []*EntryX, i int) int {
	var j int
	for j = i-1; j >= 0; j-- {
		if cachedEntries[j].Operation != types.OpDelete {
			break
		}
	}
	if j < 0 {
		panic("Can not find previous entry")
	}
	return j
}

func getNext(cachedEntries []*EntryX, i int) int {
	var j int
	for j = i+1; j < len(cachedEntries); j++ {
		if cachedEntries[j].Operation != types.OpDelete {
			break
		}
	}
	if j >= len(cachedEntries) {
		panic("Can not find previous entry")
	}
	return j
}

func (okv *OnvaKV) update() {
	okv.k2eCache.Range(func(key, value interface{}) bool {
		if value == nil {
			keyStr := key.(string)
		        okv.cachedEntries = append(okv.cachedEntries, makeFakeEntryX(keyStr))
		} else {
			entryX := value.(*EntryX)
		        okv.cachedEntries = append(okv.cachedEntries, entryX)
		}
		return true
	})
	sort.Slice(okv.cachedEntries, func(i,j int) bool {
		return bytes.Compare(okv.cachedEntries[i].EntryPtr.Key, okv.cachedEntries[j].EntryPtr.Key) < 0
	})
	// set NextKey to correct values and mark IsModified
	for i, entryX := range okv.cachedEntries {
		if entryX.Operation != types.OpNone && entryX.EntryPtr.SerialNum == math.MaxInt64 {
			panic("Operate on a fake entry")
		}
		if entryX.Operation == types.OpDelete {
			entryX.IsModified = true
			next := getNext(okv.cachedEntries, i)
			nextKey := okv.cachedEntries[next].EntryPtr.Key
			prev := getPrev(okv.cachedEntries, i)
			okv.cachedEntries[prev].EntryPtr.NextKey = nextKey
			okv.cachedEntries[prev].IsModified = true
		} else if entryX.Operation == types.OpInsertOrChange {
			entryX.IsModified = true
			next := getNext(okv.cachedEntries, i)
			entryX.EntryPtr.NextKey = okv.cachedEntries[next].EntryPtr.Key
			prev := getPrev(okv.cachedEntries, i)
			okv.cachedEntries[prev].EntryPtr.NextKey = entryX.EntryPtr.Key
			okv.cachedEntries[prev].IsModified = true
			//fmt.Printf("this: %s(%#v) prev %d: %s(%#v) next %d: %s(%#v)\n", entryX.EntryPtr.Key, entryX.EntryPtr.Key,
			//	prev, okv.cachedEntries[prev].EntryPtr.Key, okv.cachedEntries[prev].EntryPtr.Key,
			//	next,  okv.cachedEntries[next].EntryPtr.Key, okv.cachedEntries[next].EntryPtr.Key)
		}
	}
	// update stored data
	for _, entryX := range okv.cachedEntries {
		if !entryX.IsModified {
			continue
		}
		ptr := entryX.EntryPtr
		if entryX.Operation == types.OpDelete {
			//fmt.Printf("Now we deactive %d for deletion\n", ptr.SerialNum)
			okv.idxTree.Delete(ptr.Key)
			okv.DeactiviateEntry(ptr.SerialNum)
		} else {
			if ptr.SerialNum >= 0 { // if this entry already exists
				okv.DeactiviateEntry(ptr.SerialNum)
			}
			ptr.LastHeight = ptr.Height
			ptr.Height = okv.meta.GetCurrHeight()
			ptr.SerialNum = okv.meta.GetMaxSerialNum()
			//fmt.Printf("Now SerialNum = %d for %s(%#v)\n", ptr.SerialNum, string(ptr.Key), ptr.Key)
			okv.meta.IncrMaxSerialNum()
			pos := okv.datTree.AppendEntry(ptr)
			okv.idxTree.Set(ptr.Key, uint64(pos))
		}
	}
}

func (okv *OnvaKV) DeactiviateEntry(sn int64) {
	pendingDeactCount := okv.datTree.DeactiviateEntry(sn)
	if pendingDeactCount > datatree.DeactivedSNListMaxLen {
		sn := okv.meta.GetMaxSerialNum()
		okv.meta.IncrMaxSerialNum()
		entry := datatree.DummyEntry(sn)
		okv.datTree.AppendEntry(entry)
		okv.datTree.DeactiviateEntry(sn)
	}
}

func (okv *OnvaKV) EndWrite() {
	okv.update()
	for okv.numOfKeptEntries() > okv.meta.GetActiveEntryCount()*KeptEntriesToActiveEntriesRation &&
		okv.meta.GetActiveEntryCount() > StartReapThres {
		twigID := okv.meta.GetOldestActiveTwigID()
		entries := okv.datTree.GetActiveEntriesInTwig(twigID)
		for _, e := range entries {
			okv.DeactiviateEntry(e.SerialNum)
			e.SerialNum = okv.meta.GetMaxSerialNum()
			okv.meta.IncrMaxSerialNum()
			pos := okv.datTree.AppendEntry(e)
			okv.idxTree.Set(e.Key, uint64(pos))
		}
		okv.datTree.EvictTwig(twigID)
		okv.meta.IncrOldestActiveTwigID()
	}
	root := okv.datTree.EndBlock()
	okv.rootHash = root
	okv.k2eCache = &sync.Map{} // clear content
	okv.cachedEntries = okv.cachedEntries[:0] // clear content

	eS, tS := okv.datTree.GetFileSizes()
	okv.meta.SetEntryFileSize(eS)
	okv.meta.SetTwigMtFileSize(tS)
	okv.meta.Commit()
	okv.idxTree.EndWrite()
	okv.rocksdb.CloseOldBatch()
}

func (okv *OnvaKV) InitGuards(startKey, endKey []byte) {
	okv.idxTree.BeginWrite(-1)
	okv.meta.SetCurrHeight(-1)

	pos := okv.datTree.AppendEntry(&Entry{
		Key:        startKey,
		Value:      []byte{},
		NextKey:    endKey,
		Height:     -1,
		LastHeight: -1,
		SerialNum:  okv.meta.GetMaxSerialNum(),
	})
	okv.meta.IncrMaxSerialNum()
	okv.idxTree.Set(startKey, uint64(pos))

	pos = okv.datTree.AppendEntry(&Entry{
		Key:        endKey,
		Value:      []byte{},
		NextKey:    endKey,
		Height:     -1,
		LastHeight: -1,
		SerialNum:  okv.meta.GetMaxSerialNum(),
	})
	okv.meta.IncrMaxSerialNum()
	okv.idxTree.Set(endKey, uint64(pos))

	okv.idxTree.EndWrite()
	okv.rootHash = okv.datTree.EndBlock()
	okv.meta.Commit()
}

func (okv *OnvaKV) PruneBeforeHeight(height int64) {
	start := okv.meta.GetLastPrunedTwig() + 1
	end := start + 1
	endHeight := okv.meta.GetTwigHeight(end)
	if endHeight < 0 {
		return
	}
	for endHeight < height && okv.datTree.TwigCanBePruned(end) {
		end++
		endHeight = okv.meta.GetTwigHeight(end)
		if endHeight < 0 {
			return
		}
	}
	end--
	if end > start {
		edgeNodesBytes := okv.datTree.PruneTwigs(start, end)
		okv.meta.SetEdgeNodes(edgeNodesBytes)
		for i := start; i < end; i++ {
			okv.meta.DeleteTwigHeight(i)
		}
		okv.meta.SetLastPrunedTwig(end-1)
	}
	okv.rocksdb.SetPruneHeight(uint64(height))
}

type OnvaIterator struct {
	okv  *OnvaKV
	iter types.Iterator
}

var _ dbm.Iterator = (*OnvaIterator)(nil)

func (iter *OnvaIterator) Domain() (start []byte, end []byte) {
	return iter.iter.Domain()
}
func (iter *OnvaIterator) Valid() bool {
	return iter.iter.Valid()
}
func (iter *OnvaIterator) Next() {
	iter.iter.Next()
}
func (iter *OnvaIterator) Key() []byte {
	return iter.iter.Key()
}
func (iter *OnvaIterator) Value() []byte {
	if !iter.Valid() {
		return nil
	}
	pos := iter.iter.Value()
	//fmt.Printf("pos = %d %#v\n", pos, iter.okv.datTree.ReadEntry(int64(pos)))
	return iter.okv.datTree.ReadEntry(int64(pos)).Value
}
func (iter *OnvaIterator) Close() {
	iter.iter.Close()
}

func (okv *OnvaKV) Iterator(start, end []byte) dbm.Iterator {
	return &OnvaIterator{okv: okv, iter: okv.idxTree.Iterator(start, end)}
}

func (okv *OnvaKV) ReverseIterator(start, end []byte) dbm.Iterator {
	return &OnvaIterator{okv: okv, iter: okv.idxTree.ReverseIterator(start, end)}
}
