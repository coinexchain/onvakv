package onvakv

import (
	"sync"

	dbm "github.com/tendermint/tm-db"

	"github.com/coinexchain/onvakv/types"
	"github.com/coinexchain/onvakv/datatree"
)

const StartReapThres int64 = 1000 * 1000
const ActiveEntriesToKeptEntriesRation = 3

func (okv *OnvaKV) PruneBeforeHeight(height int64) {
	start := okv.meta.GetLastPrunedTwig()+1
	end := start
	endHeight := okv.meta.GetTwigHeight(end)
	for endHeight < height && okv.datTree.TwigCanBePruned(end) {
		end++
		endHeight = okv.meta.GetTwigHeight(end)
	}
	if end > start {
		edgeNodes := okv.datTree.PruneTwigs(start, end)
		okv.meta.SetEdgeNodes(edgeNodes)
		for i:=start; i<=end; i++ {
			okv.meta.DeleteTwigHeight(i)
		}
		okv.meta.SetLastPrunedTwig(end)
	}
}

type OnvaKV struct {
	meta     types.MetaDB
	idxTree  types.IndexTree
	datTree  types.DataTree
	rootHash []byte
}

const (
	SET = 1
	CHANGE = 2
	INSERT = 3
	DELETE = 4
	NOP = 0
)

type Entry = types.Entry

func NewSetTask(k,v []byte) types.UpdateTask {
	return types.UpdateTask{
		TaskKind: SET,
		Key:      k,
		Value:    v,
	}
}

func NewDeleteTask(k []byte) types.UpdateTask {
	return types.UpdateTask{
		TaskKind: DELETE,
		Key:      k,
	}
}

func (okv *OnvaKV) GetRootHash() []byte {
	return append([]byte{}, okv.rootHash...)
}

func (okv *OnvaKV) Get(k []byte) []byte {
	pos, ok := okv.idxTree.Get(k)
	if !ok {
		return nil
	}
	return okv.datTree.ReadEntry(pos).Value
}

func (okv *OnvaKV) getEntry(k []byte) *Entry {
	pos, ok := okv.idxTree.Get(k)
	if !ok {
		return nil
	}
	return okv.datTree.ReadEntry(pos)
}

func (okv *OnvaKV) getPrevEntry(k []byte) *Entry {
	iter := okv.idxTree.ReverseIterator([]byte{}, k)
	if !iter.Valid() {
		panic("The iterator is invalid! Missing a guard node?")
	}
	pos := iter.Value()
	return okv.datTree.ReadEntry(pos)
}

func (okv *OnvaKV) changeEntry(e *Entry, v []byte) {
	okv.datTree.DeactiviateEntry(e.SerialNum)
	e.LastHeight = e.Height
	e.Height = okv.meta.GetCurrHeight()
	e.SerialNum = okv.meta.GetMaxSerialNum()
	okv.meta.IncrMaxSerialNum()
	pos := okv.datTree.AppendEntry(e)
	okv.idxTree.Set(e.Key, pos)
}

func (okv *OnvaKV) insertEntry(prev *Entry, k []byte, v []byte) {
	curr := &Entry{
		Key:        k,
		Value:      v,
		NextKey:    prev.NextKey,
		Height:     okv.meta.GetCurrHeight(),
		LastHeight: -1,
		SerialNum:  okv.meta.GetMaxSerialNum(),
	}
	okv.meta.IncrMaxSerialNum()
	pos := okv.datTree.AppendEntry(prev)
	okv.idxTree.Set(curr.Key, pos)

	okv.datTree.DeactiviateEntry(prev.SerialNum)
	prev.LastHeight = prev.Height
	prev.Height = okv.meta.GetCurrHeight()
	prev.SerialNum = okv.meta.GetMaxSerialNum()
	prev.NextKey = k
	okv.meta.IncrMaxSerialNum()
	pos = okv.datTree.AppendEntry(prev)
	okv.idxTree.Set(prev.Key, pos)

	okv.meta.IncrActiveEntryCount()
}

func (okv *OnvaKV) deleteEntry(prev *Entry, curr *Entry) {
	okv.datTree.DeactiviateEntry(curr.SerialNum)
	okv.datTree.DeactiviateEntry(prev.SerialNum)
	prev.LastHeight = prev.Height
	prev.Height = okv.meta.GetCurrHeight()
	prev.SerialNum = okv.meta.GetMaxSerialNum()
	prev.NextKey = curr.NextKey
	okv.meta.IncrMaxSerialNum()
	pos := okv.datTree.AppendEntry(prev)
	okv.idxTree.Set(prev.Key, pos)

	okv.meta.DecrActiveEntryCount()
}

func (okv *OnvaKV) runTask(task types.UpdateTask) {
	if task.TaskKind == INSERT {
		okv.insertEntry(task.PrevEntry, task.Key, task.Value)
	} else if task.TaskKind == CHANGE {
		okv.changeEntry(task.CurrEntry, task.Value)
	} else if task.TaskKind == DELETE {
		okv.deleteEntry(task.PrevEntry, task.CurrEntry)
	} else {
		panic("Invalid Task Kind")
	}
}

const (
	MinimumTasksInGoroutine = 10
	MaximumGoroutines = 128
)

func (okv *OnvaKV) prepareTask(task *types.UpdateTask) {
	if task.TaskKind == SET {
		task.CurrEntry = okv.getEntry(task.Key)
		if task.CurrEntry == nil {
			task.TaskKind = INSERT
			task.PrevEntry = okv.getPrevEntry(task.Key)
		} else {
			task.TaskKind = CHANGE
		}
	} else if task.TaskKind == DELETE {
		task.CurrEntry = okv.getEntry(task.Key)
		if task.CurrEntry == nil {
			task.TaskKind = NOP
		} else {
			task.PrevEntry = okv.getPrevEntry(task.Key)
		}
	} else {
		panic("Invalid Task Kind")
	}
}

func (okv *OnvaKV) prepareTasks(tasks []types.UpdateTask) {
	stripe := MinimumTasksInGoroutine
	if stripe * MaximumGoroutines < len(tasks) {
		stripe = len(tasks)/MaximumGoroutines
		if len(tasks)%MaximumGoroutines != 0 {
			stripe++
		}
	}
	var wg sync.WaitGroup
	for start:=0; start<len(tasks); start+=stripe {
		end := start+stripe
		if end > len(tasks) {
			end = len(tasks)
		}
		wg.Add(1)
		go func() {
			for i:=start; i<end; i++ {
				okv.prepareTask(&tasks[i])
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (okv *OnvaKV) numOfKeptEntries() int64 {
	return okv.meta.GetMaxSerialNum() - okv.meta.GetOldestActiveTwigID() * datatree.LeafCountInTwig
}

func (okv *OnvaKV) EndBlock(tasks []types.UpdateTask, height int64) {
	okv.prepareTasks(tasks)
	okv.idxTree.BeginWrite(height)
	for _, task := range tasks {
		okv.runTask(task)
	}
	okv.meta.SetCurrHeight(height)
	for okv.numOfKeptEntries() > okv.meta.GetActiveEntryCount()*ActiveEntriesToKeptEntriesRation &&
	okv.meta.GetActiveEntryCount() > StartReapThres {
		twigID := okv.meta.GetOldestActiveTwigID()
		entries := okv.datTree.GetActiveEntriesInTwig(twigID)
		for _, e := range entries {
			okv.datTree.DeactiviateEntry(e.SerialNum)
			e.SerialNum = okv.meta.GetMaxSerialNum()
			okv.meta.IncrMaxSerialNum()
			pos := okv.datTree.AppendEntry(e)
			okv.idxTree.Set(e.Key, pos)
		}
		okv.datTree.DeleteActiveTwig(twigID)
		okv.meta.IncrOldestActiveTwigID()
	}
	okv.idxTree.EndWrite()
	okv.rootHash = okv.datTree.EndBlock()

	eS, tS := okv.datTree.GetFileSizes()
	okv.meta.SetEntryFileSize(eS)
	okv.meta.SetTwigMtFileSize(tS)
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
	pos := iter.iter.Value()
	return iter.okv.datTree.ReadEntry(pos).Value
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



