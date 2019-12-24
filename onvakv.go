package onvakv

import (
	"sync"

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
	meta    types.MetaDB
	idxTree types.IndexTree
	datTree types.DataTree
}

const (
	SET = 1
	CHANGE = 2
	INSERT = 3
	DELETE = 4
	NOP = 0
)

type Entry = types.Entry

type UpdateTask struct {
	taskKind  int
	prevEntry *Entry
	currEntry *Entry
	key       []byte
	value     []byte
}

func NewSetTask(k,v []byte) UpdateTask {
	return UpdateTask{
		taskKind: SET,
		key:      k,
		value:    v,
	}
}

func NewDeleteTask(k []byte) UpdateTask {
	return UpdateTask{
		taskKind: DELETE,
		key:      k,
	}
}

func (okv *OnvaKV) Get(k []byte) ([]byte, bool) {
	pos, ok := okv.idxTree.Get(k)
	if !ok {
		return nil, false
	}
	return okv.datTree.ReadEntry(pos).Value, true
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

func (okv *OnvaKV) runTask(task UpdateTask) {
	if task.taskKind == INSERT {
		okv.insertEntry(task.prevEntry, task.key, task.value)
	} else if task.taskKind == CHANGE {
		okv.changeEntry(task.currEntry, task.value)
	} else if task.taskKind == DELETE {
		okv.deleteEntry(task.prevEntry, task.currEntry)
	} else {
		panic("Invalid Task Kind")
	}
}

const (
	MinimumTasksInGoroutine = 10
	MaximumGoroutines = 128
)

func (okv *OnvaKV) prepareTask(task *UpdateTask) {
	if task.taskKind == SET {
		task.currEntry = okv.getEntry(task.key)
		if task.currEntry == nil {
			task.taskKind = INSERT
			task.prevEntry = okv.getPrevEntry(task.key)
		} else {
			task.taskKind = CHANGE
		}
	} else if task.taskKind == DELETE {
		task.currEntry = okv.getEntry(task.key)
		if task.currEntry == nil {
			task.taskKind = NOP
		} else {
			task.prevEntry = okv.getPrevEntry(task.key)
		}
	} else {
		panic("Invalid Task Kind")
	}
}

func (okv *OnvaKV) prepareTasks(tasks []UpdateTask) {
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

func (okv *OnvaKV) EndBlock(tasks []UpdateTask, height int64) {
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
	okv.datTree.EndBlock()

	eS, tS := okv.datTree.GetFileSizes()
	okv.meta.SetEntryFileSize(eS)
	okv.meta.SetTwigMtFileSize(tS)
}

