package onvakv

import (
	"bytes"
	"fmt"
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
	StartReapThres int64 = 10000 // 1000 * 1000
	KeptEntriesToActiveEntriesRatio = 3
)

type OnvaKV struct {
	meta          types.MetaDB
	idxTree       types.IndexTree
	datTree       types.DataTree
	rocksdb       *indextree.RocksDB
	rootHash      []byte
	k2heMap       *sync.Map // key-to-hot-entry map
	k2nkMap       *sync.Map // key-to-next-key map
	cachedEntries []*HotEntry
	startKey      []byte
	endKey        []byte
}

func NewOnvaKV4Mock(startEndKeys [][]byte) *OnvaKV {
	okv := &OnvaKV{k2heMap: &sync.Map{}, k2nkMap: &sync.Map{}}

	okv.datTree = datatree.NewMockDataTree()
	okv.idxTree = indextree.NewMockIndexTree()

	var err error
	okv.rocksdb, err = indextree.NewRocksDB("rocksdb", "./")
	if err != nil {
		panic(err)
	}

	okv.meta = metadb.NewMetaDB(okv.rocksdb)
	okv.rocksdb.OpenNewBatch()
	okv.InitGuards(startEndKeys[0], startEndKeys[1])
	return okv
}

func NewOnvaKV(dirName string, queryHistory bool, startEndKeys [][]byte) (*OnvaKV, error) {
	_, err := os.Stat(dirName)
	dirNotExists := os.IsNotExist(err)
	okv := &OnvaKV{
		k2heMap:      &sync.Map{},
		k2nkMap:      &sync.Map{},
		cachedEntries: make([]*HotEntry, 0, 2000),
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
		if queryHistory {
			okv.idxTree = indextree.NewNVTreeMem(okv.rocksdb)
		} else {
			okv.idxTree = indextree.NewNVTreeMem(nil)
		}
		okv.rocksdb.OpenNewBatch()
		okv.meta.Init()
		for i := 0; i < 2048; i++ {
			sn := okv.meta.GetMaxSerialNum()
			okv.meta.IncrMaxSerialNum()
			entry := datatree.DummyEntry(sn)
			okv.datTree.AppendEntry(entry)
			okv.datTree.DeactiviateEntry(sn)
		}
		okv.InitGuards(startEndKeys[0], startEndKeys[1])
		okv.rocksdb.CloseOldBatch()
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

	if dirNotExists {
		//do nothing
	} else if queryHistory { // use rocksdb to keep the historical index
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
	okv.k2heMap = nil
	okv.k2nkMap = nil
}

type Entry = types.Entry
type HotEntry = types.HotEntry

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

func isFakeInserted(hotEntry *HotEntry) bool {
	return hotEntry.EntryPtr.SerialNum == -1 && hotEntry.EntryPtr.Value == nil
}

func isInserted(hotEntry *HotEntry) bool {
	return hotEntry.Operation == types.OpInsertOrChange && hotEntry.EntryPtr.SerialNum == -1
}

func isModified(hotEntry *HotEntry) bool {
	return hotEntry.Operation == types.OpInsertOrChange && hotEntry.EntryPtr.SerialNum >= 0
}

func (okv *OnvaKV) PrepareForUpdate(k []byte) {
	//fmt.Printf("In PrepareForUpdate we see: %s\n", string(k))
	pos, findIt := okv.idxTree.Get(k)
	if findIt { // The case of Change
		//fmt.Printf("In PrepareForUpdate we update\n")
		entry := okv.datTree.ReadEntry(int64(pos))
		//fmt.Printf("Now we add entry to k2e(findIt): %s(%#v)\n", string(k), k)
		okv.k2heMap.Store(string(k), &HotEntry{
			EntryPtr:  entry,
			Operation: types.OpNone,
		})
		return
	}
	prevEntry := okv.getPrevEntry(k)

	// The case of Insert
	//fmt.Printf("Now we add entry to k2e(not-findIt): %s(%#v)\n", string(k), k)
	okv.k2heMap.Store(string(k), &HotEntry{
		EntryPtr: &Entry{
			Key:        append([]byte{}, k...),
			Value:      nil,
			NextKey:    nil,
			Height:     0,
			LastHeight: 0,
			SerialNum:  -1, //inserted entries has negative SerialNum
		},
		Operation: types.OpNone,
	})

	//fmt.Printf("In PrepareForUpdate we insert\n")
	//fmt.Printf("prevEntry(%#v): %#v\n", k, prevEntry)
	//fmt.Printf("Now we add entry to k2e(prevEntry.Key): %s(%#v)\n", kStr, prevEntry.Key)
	okv.k2heMap.Store(string(prevEntry.Key), &HotEntry{
		EntryPtr:  prevEntry,
		Operation: types.OpNone,
	})

	okv.k2nkMap.Store(string(prevEntry.NextKey), nil)
}

func (okv *OnvaKV) PrepareForDeletion(k []byte) (findIt bool) {
	//fmt.Printf("In PrepareForDeletion we see: %#v\n", k)
	pos, findIt := okv.idxTree.Get(k)
	if !findIt {
		return
	}

	entry := okv.datTree.ReadEntry(int64(pos))
	prevEntry := okv.getPrevEntry(k)

	//fmt.Printf("In PrepareForDeletion we read: %#v\n", entry)
	okv.k2heMap.Store(string(entry.Key), &HotEntry{
		EntryPtr:  entry,
		Operation: types.OpNone,
	})

	okv.k2heMap.Store(string(prevEntry.Key), &HotEntry{
		EntryPtr:  prevEntry,
		Operation: types.OpNone,
	})

	okv.k2nkMap.Store(string(entry.NextKey), nil) // we do not need next entry's value, so here we store nil
	return
}

func isHintHotEntry(hotEntry *HotEntry) bool {
	return hotEntry.EntryPtr.SerialNum == math.MinInt64
}

func makeHintHotEntry(key string) *HotEntry {
	return &HotEntry {
		EntryPtr: &Entry{
			Key:        []byte(key),
			Value:      nil,
			NextKey:    nil,
			Height:     -1,
			LastHeight: -1,
			SerialNum:  math.MinInt64, // hint entry has smallest possible SerialNum
		},
		Operation: types.OpNone,
	}
}

func (okv *OnvaKV) getPrevEntry(k []byte) *Entry {
	iter := okv.idxTree.ReverseIterator([]byte{}, k)
	defer iter.Close()
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
	v, ok := okv.k2heMap.Load(string(key))
	if !ok {
		panic("Can not find entry in cache")
	}
	if v == nil {
		panic("Can not change or insert at a fake entry")
	}
	//fmt.Printf("In Set we see: %#v %#v\n", key, value)
	hotEntry := v.(*HotEntry)
	hotEntry.EntryPtr.Value = value
	hotEntry.Operation = types.OpInsertOrChange
}

func (okv *OnvaKV) Delete(key []byte) {
	//fmt.Printf("In Delete we see: %s(%#v)\n", string(key), key)
	v, ok := okv.k2heMap.Load(string(key))
	if !ok {
		return // delete a non-exist kv pair
	}
	if v == nil {
		return // delete a non-exist kv pair
	}
	hotEntry := v.(*HotEntry)
	hotEntry.Operation = types.OpDelete
}

func getPrev(cachedEntries []*HotEntry, i int) int {
	var j int
	for j = i-1; j >= 0; j-- {
		if cachedEntries[j].Operation != types.OpDelete && !isFakeInserted(cachedEntries[j]) {
			break
		}
	}
	if j < 0 {
		//for j = i; j >= 0; j-- {
		//	fmt.Printf("Debug j %d hotEntry %#v Entry %#v\n", j, cachedEntries[j], cachedEntries[j].EntryPtr)
		//}
		panic("Can not find previous entry")
	}
	return j
}

func getNext(cachedEntries []*HotEntry, i int) int {
	var j int
	for j = i+1; j < len(cachedEntries); j++ {
		if cachedEntries[j].Operation != types.OpDelete && !isFakeInserted(cachedEntries[j]) {
			break
		}
	}
	if j >= len(cachedEntries) {
		//for j = i; j < len(cachedEntries); j++ {
		//	fmt.Printf("Debug j %d hotEntry %#v Entry %#v\n", j, cachedEntries[j], cachedEntries[j].EntryPtr)
		//}
		panic("Can not find next entry")
	}
	return j
}

func (okv *OnvaKV) update() {
	okv.k2heMap.Range(func(key, value interface{}) bool {
		hotEntry := value.(*HotEntry)
		//fmt.Printf("HERE key: %#v HotEntry: %#v Entry: %#v\n", []byte(key.(string)), hotEntry, *(hotEntry.EntryPtr))
		okv.cachedEntries = append(okv.cachedEntries, hotEntry)
		return true
	})
	okv.k2nkMap.Range(func(key, value interface{}) bool {
		kStr := key.(string)
		if _, ok := okv.k2heMap.Load(kStr); !ok {
			keyStr := kStr
		        okv.cachedEntries = append(okv.cachedEntries, makeHintHotEntry(keyStr))
		}
		return true
	})
	sort.Slice(okv.cachedEntries, func(i,j int) bool {
		return bytes.Compare(okv.cachedEntries[i].EntryPtr.Key, okv.cachedEntries[j].EntryPtr.Key) < 0
	})
	// set NextKey to correct values and mark IsModified
	for i, hotEntry := range okv.cachedEntries {
		if hotEntry.Operation != types.OpNone && isHintHotEntry(hotEntry) {
			panic("Operate on a hint entry")
		}
		if isFakeInserted(hotEntry) {
			continue
		}
		if hotEntry.Operation == types.OpDelete {
			hotEntry.IsModified = true
			next := getNext(okv.cachedEntries, i)
			nextKey := okv.cachedEntries[next].EntryPtr.Key
			prev := getPrev(okv.cachedEntries, i)
			okv.cachedEntries[prev].EntryPtr.NextKey = nextKey
			okv.cachedEntries[prev].IsTouchedByNext = true
		} else if isInserted(hotEntry) {
			hotEntry.IsModified = true
			//fmt.Printf("THERE key: %#v HotEntry: %#v Entry: %#v\n", hotEntry.EntryPtr.Key, hotEntry, *(hotEntry.EntryPtr))
			next := getNext(okv.cachedEntries, i)
			hotEntry.EntryPtr.NextKey = okv.cachedEntries[next].EntryPtr.Key
			prev := getPrev(okv.cachedEntries, i)
			okv.cachedEntries[prev].EntryPtr.NextKey = hotEntry.EntryPtr.Key
			okv.cachedEntries[prev].IsTouchedByNext = true
			//fmt.Printf("this: %s(%#v) prev %d: %s(%#v) next %d: %s(%#v)\n", hotEntry.EntryPtr.Key, hotEntry.EntryPtr.Key,
			//	prev, okv.cachedEntries[prev].EntryPtr.Key, okv.cachedEntries[prev].EntryPtr.Key,
			//	next,  okv.cachedEntries[next].EntryPtr.Key, okv.cachedEntries[next].EntryPtr.Key)
		} else if isModified(hotEntry) {
			hotEntry.IsModified = true
		}
	}
	// update stored data
	for _, hotEntry := range okv.cachedEntries {
		if !(hotEntry.IsModified || hotEntry.IsTouchedByNext) {
			continue
		}
		ptr := hotEntry.EntryPtr
		if hotEntry.Operation == types.OpDelete && ptr.SerialNum >= 0 {
			// if ptr.SerialNum==-1, then we are deleting a just-inserted value, so ignore it.
			//fmt.Printf("Now we deactive %d for deletion %#v\n", ptr.SerialNum, ptr)
			okv.idxTree.Delete(ptr.Key)
			//okv.meta.DecrActiveEntryCount()
			okv.DeactiviateEntry(ptr.SerialNum)
		} else if hotEntry.Operation != types.OpNone || hotEntry.IsTouchedByNext {
			if ptr.SerialNum >= 0 { // if this entry already exists
				//fmt.Printf("Now we deactive %d for refresh %#v\n", ptr.SerialNum, ptr)
				//okv.meta.DecrActiveEntryCount()
				okv.DeactiviateEntry(ptr.SerialNum)
			}
			ptr.LastHeight = ptr.Height
			ptr.Height = okv.meta.GetCurrHeight()
			ptr.SerialNum = okv.meta.GetMaxSerialNum()
			//fmt.Printf("Now SerialNum = %d for %s(%#v) %#v Entry %#v\n", ptr.SerialNum, string(ptr.Key), ptr.Key, hotEntry, *ptr)
			okv.meta.IncrMaxSerialNum()
			pos := okv.datTree.AppendEntry(ptr)
			//okv.meta.IncrActiveEntryCount()
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

func (okv *OnvaKV) CheckConsistency() {
	iter := okv.idxTree.ReverseIterator([]byte{}, okv.endKey)
	defer iter.Close()
	nextKey := okv.endKey
	for iter.Valid() && !bytes.Equal(iter.Key(), okv.startKey) {
		pos := iter.Value()
		entry := okv.datTree.ReadEntry(int64(pos))
		if !bytes.Equal(entry.NextKey, nextKey) {
			panic(fmt.Sprintf("Invalid NextKey for %#v, datTree %#v, idxTree %#v\n",
				iter.Key(), entry.NextKey, nextKey))
		}
		nextKey = iter.Key()
		iter.Next()
	}
}

func (okv *OnvaKV) ActiveCount() int {
	return okv.idxTree.ActiveCount()
}

func (okv *OnvaKV) EndWrite() {
	okv.update()
	//if okv.meta.GetActiveEntryCount() != int64(okv.idxTree.ActiveCount()) - 2 {
	//	panic(fmt.Sprintf("Fuck meta.GetActiveEntryCount %d okv.idxTree.ActiveCount %d\n", okv.meta.GetActiveEntryCount(), okv.idxTree.ActiveCount()))
	//}
	//fmt.Printf("numOfKeptEntries %d ActiveCount %d x3 %d\n", okv.numOfKeptEntries(), okv.idxTree.ActiveCount(), okv.idxTree.ActiveCount()*3)
	for okv.numOfKeptEntries() > int64(okv.idxTree.ActiveCount())*KeptEntriesToActiveEntriesRatio &&
		int64(okv.idxTree.ActiveCount()) > StartReapThres {
		twigID := okv.meta.GetOldestActiveTwigID()
		entries := okv.datTree.GetActiveEntriesInTwig(twigID)
		for _, e := range entries {
			if string(e.Key) == "dummy" {panic(fmt.Sprintf("an active entry is dummy %#v", e))}
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
	okv.k2heMap = &sync.Map{} // clear content
	okv.k2nkMap = &sync.Map{} // clear content
	okv.cachedEntries = okv.cachedEntries[:0] // clear content

	eS, tS := okv.datTree.GetFileSizes()
	okv.meta.SetEntryFileSize(eS)
	okv.meta.SetTwigMtFileSize(tS)
	okv.meta.Commit()
	okv.idxTree.EndWrite()
	okv.rocksdb.CloseOldBatch()
}

func (okv *OnvaKV) InitGuards(startKey, endKey []byte) {
	okv.startKey = append([]byte{}, startKey...)
	okv.endKey = append([]byte{}, endKey...)
	okv.idxTree.BeginWrite(-1)
	okv.meta.SetCurrHeight(-1)

	entry := &Entry{
		Key:        startKey,
		Value:      []byte{},
		NextKey:    endKey,
		Height:     -1,
		LastHeight: -1,
		SerialNum:  okv.meta.GetMaxSerialNum(),
	}
	pos := okv.datTree.AppendEntry(entry)
	okv.meta.IncrMaxSerialNum()
	okv.idxTree.Set(startKey, uint64(pos))

	entry = &Entry{
		Key:        endKey,
		Value:      []byte{},
		NextKey:    endKey,
		Height:     -1,
		LastHeight: -1,
		SerialNum:  okv.meta.GetMaxSerialNum(),
	}
	pos = okv.datTree.AppendEntry(entry)
	okv.meta.IncrMaxSerialNum()
	okv.idxTree.Set(endKey, uint64(pos))

	okv.idxTree.EndWrite()
	okv.rootHash = okv.datTree.EndBlock()
	okv.meta.Commit()
	okv.rocksdb.CloseOldBatch()
	okv.rocksdb.OpenNewBatch()
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
