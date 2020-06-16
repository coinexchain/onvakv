package fuzz

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"strconv"

	"github.com/coinexchain/randsrc"
	"github.com/coinexchain/onvakv/store"
	storetypes "github.com/coinexchain/onvakv/store/types"
)


const (
	OpRead = 8
	OpIterate = 6
	OpWrite = 1
	OpDelete = 0
)

var DBG bool

var EndKey = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255}

type FuzzConfig struct {
	MaxReadCountInTx     uint32
	MaxIterCountInTx     uint32
	MaxWriteCountInTx    uint32
	MaxDeleteCountInTx   uint32
	MaxTxCountInEpoch    uint32
	MaxEpochCountInBlock uint32
	EffectiveBits        uint32
	MaxIterDistance      uint32
	TxSucceedRatio       float32
	EpochSucceedRatio    float32
}

type Pair struct {
	Key, Value []byte
}

type Operation struct {
	opType  int
	key     [8]byte
	keyEnd  [8]byte
	value   []byte
	results []Pair
}

type Tx struct {
	OpList  []Operation
	Succeed bool
}

type Epoch struct {
	TxList []*Tx
	Succeed bool
}

type Block struct {
	EpochList []Epoch
	Succeed bool
}

func getRand8Bytes(rs randsrc.RandSrc, cfg *FuzzConfig, touchedKeys map[uint64]struct{}) (res [8]byte) {
	sh := 62 - cfg.EffectiveBits
	if touchedKeys == nil {
		i := rs.GetUint64()
		i = ((i<<sh)>>sh)|3
		binary.LittleEndian.PutUint64(res[:], i)
		return
	}
	for {
		i := rs.GetUint64()
		i = ((i<<sh)>>sh)|3
		if _, ok := touchedKeys[i]; ok {
			continue
		} else {
			binary.LittleEndian.PutUint64(res[:], i)
			break
		}
	}
	return
}

func UpdateRefStoreWithTx(ref *RefStore, tx *Tx) {
	for _, op := range tx.OpList {
		if op.opType == OpWrite {
			ref.Set(op.key[:], op.value[:])
		}
		if op.opType == OpDelete {
			ref.Delete(op.key[:])
		}
	}
}

func RecheckIter(ref *RefStore, rs randsrc.RandSrc, cfg *FuzzConfig, tx *Tx) {
	for i, op := range tx.OpList {
		if op.opType != OpIterate {
			continue
		}
		var iter storetypes.ObjIterator
		if bytes.Compare(op.key[:], op.keyEnd[:]) < 0 {
			iter = ref.Iterator(op.key[:], op.keyEnd[:])
		} else {
			iter = ref.ReverseIterator(op.keyEnd[:], op.key[:])
		}

		iterOK := true
		for _, pair := range op.results {
			if !iter.Valid() {
				iterOK = false
				break
			}
			if !bytes.Equal(iter.Key(), pair.Key) {
				iterOK = false
				break
			}
			if !bytes.Equal(iter.Value(), pair.Value) {
				iterOK = false
				break
			}
			iter.Next()
		}
		if iter.Valid() && len(op.results) < int(cfg.MaxIterDistance) {
			iterOK = false
		}
		if !iterOK {
			tx.OpList[i].value = nil //nil-value marks it as invalid
		}

		iter.Close()
	}
}

func GenerateRandTx(ref *RefStore, rs randsrc.RandSrc, cfg *FuzzConfig, touchedKeys map[uint64]struct{}, epochSuc bool) *Tx {
	readCount, iterCount, writeCount, deleteCount := uint32(0), uint32(0), uint32(0), uint32(0)
	maxReadCount := rs.GetUint32()%cfg.MaxReadCountInTx
	maxIterCount := rs.GetUint32()%cfg.MaxIterCountInTx
	maxWriteCount := rs.GetUint32()%cfg.MaxWriteCountInTx
	maxDeleteCount := rs.GetUint32()%cfg.MaxDeleteCountInTx
	tx := Tx{
		OpList: make([]Operation, 0, maxReadCount+maxWriteCount+maxDeleteCount),
		Succeed: float32(rs.GetUint32()%0x10000)/float32(0x10000) < cfg.TxSucceedRatio,
	}
	var undoList []UndoOp
	succeed := tx.Succeed && epochSuc
	if !succeed {
		undoList = make([]UndoOp, 0, maxWriteCount + maxDeleteCount)
	}
	for readCount!=maxReadCount || iterCount!=maxIterCount || writeCount!=maxWriteCount || deleteCount!=maxDeleteCount {
		if rs.GetUint32()%4 == 0 && readCount < maxReadCount {
			key := getRand8Bytes(rs, cfg, touchedKeys)
			tx.OpList = append(tx.OpList, Operation{
				opType: OpRead,
				key:    key,
				value:  ref.Get(key[:]),
			})
			readCount++
		}
		if rs.GetUint32()%4 == 0 && iterCount < maxIterCount {
			op := Operation{
				opType: OpIterate,
				key:    getRand8Bytes(rs, cfg, nil),
				keyEnd: getRand8Bytes(rs, cfg, nil),
				value:  []byte{1}, //make its non-nil, which marks this op as valid
			}
			var iter storetypes.ObjIterator
			if bytes.Compare(op.key[:], op.keyEnd[:]) < 0 {
				iter = ref.Iterator(op.key[:], op.keyEnd[:])
			} else {
				iter = ref.ReverseIterator(op.keyEnd[:], op.key[:])
			}
			iterSucceed := true
			if iter.Valid() {
				for len(op.results) < int(cfg.MaxIterDistance) {
					if !iter.Valid() {break}
					changed := ref.IsChangedInSameEpoch(iter.Key())
					if DBG {fmt.Printf("IsChangedInSameEpoch %v %#v\n", changed, iter.Key())}
					if changed {
						iterSucceed = false
						break
					}
					if iter.Value() == nil {
						if DBG {fmt.Printf("skipping a pair deleted in same transaction %#v\n", iter.Key())}
						iter.Next()
						continue
					}
					op.results = append(op.results, Pair{
						Key:   append([]byte{}, iter.Key()...),
						Value: append([]byte{}, iter.Value()...),
					})
					iter.Next()
				}
			}
			if iterSucceed {
				tx.OpList = append(tx.OpList, op)
			}
			iter.Close()
			iterCount++
		}
		if rs.GetUint32()%4 == 0 && writeCount < maxWriteCount {
			v := getRand8Bytes(rs, cfg, nil)
			op := Operation{
				opType: OpWrite,
				key:    getRand8Bytes(rs, cfg, touchedKeys),
				value:  v[:],
			}
			undo := ref.Set(op.key[:], op.value[:])
			if succeed {
				if DBG {fmt.Printf("MarkSet %#v\n", op.key[:])}
				ref.MarkSet(op.key[:])
			} else {
				undoList = append(undoList, undo)
			}
			tx.OpList = append(tx.OpList, op)
			writeCount++
		}
		if rs.GetUint32()%4 == 0 && deleteCount < maxDeleteCount {
			op := Operation{
				opType: OpDelete,
				key:    getRand8Bytes(rs, cfg, touchedKeys),
			}
			undo := ref.Delete(op.key[:])
			if succeed {
				if DBG {fmt.Printf("MarkDelete %#v\n", op.key[:])}
				ref.MarkDelete(op.key[:])
			} else {
				undoList = append(undoList, undo)
			}
			tx.OpList = append(tx.OpList, op)
			deleteCount++
		}
	}
	if succeed { // to prevent inter-tx dependency
		for _, op := range tx.OpList {
			if op.opType == OpRead || op.opType == OpWrite || op.opType == OpDelete {
				touchedKeys[binary.LittleEndian.Uint64(op.key[:])] = struct{}{}
			}
		}
	} else { // to recovery old state
		for i := len(undoList)-1; i >= 0; i-- {
			undoOp := undoList[i]
			if undoOp.oldStatus == storetypes.Missed {
				ref.RealDelete(undoOp.key)
			} else if undoOp.oldStatus == storetypes.JustDeleted {
				ref.Delete(undoOp.key)
			} else {
				ref.RealSet(undoOp.key, undoOp.value)
			}
		}
	}
	return &tx
}

func GenerateRandEpoch(height, epochNum int, ref *RefStore, rs randsrc.RandSrc, cfg *FuzzConfig) Epoch {
	keyCountEstimated := cfg.MaxTxCountInEpoch*(cfg.MaxReadCountInTx+cfg.MaxIterCountInTx*cfg.MaxIterDistance*2+
		cfg.MaxWriteCountInTx+cfg.MaxDeleteCountInTx)/2
	touchedKeys := make(map[uint64]struct{}, keyCountEstimated)
	txCount := rs.GetUint32()%cfg.MaxTxCountInEpoch
	epoch := Epoch{TxList: make([]*Tx, int(txCount))}
	epoch.Succeed = float32(rs.GetUint32()%0x10000)/float32(0x10000) < cfg.EpochSucceedRatio
	for i := range epoch.TxList {
		tx := GenerateRandTx(ref, rs, cfg, touchedKeys, epoch.Succeed)
		if DBG {
			fmt.Printf("FinishGeneration h:%d epoch %d (%v) tx %d (%v) of %d\n", height, epochNum, epoch.Succeed, i, tx.Succeed, txCount)
			for j, op := range epoch.TxList[i].OpList {
				fmt.Printf("See operation %d of %d\n", j, len(tx.OpList))
				fmt.Printf("%#v\n", op)
			}
		}
		epoch.TxList[i] = tx
	}
	ref.SwitchEpoch()
	if epoch.Succeed {
		for _, tx := range epoch.TxList {
			RecheckIter(ref, rs, cfg, tx)
		}
	}

	iter := ref.Iterator([]byte{}, EndKey)
	defer iter.Close()
	for iter.Valid() {
		if DBG {fmt.Printf("GEN.AT %d-%d key: %#v Value:%#v\n", height, epochNum, iter.Key(), iter.Value())}
		iter.Next()
	}

	return epoch
}

func GenerateRandBlock(height int, ref *RefStore, rs randsrc.RandSrc, cfg *FuzzConfig) Block {
	epochCount := rs.GetUint32()%cfg.MaxEpochCountInBlock
	block := Block{EpochList: make([]Epoch, epochCount)}
	for i := range block.EpochList {
		if DBG {fmt.Printf("Generating h:%d epoch %d of %d\n", height, i, epochCount)}
		block.EpochList[i] = GenerateRandEpoch(height, i, ref, rs, cfg)
	}
	return block
}

func CheckTx(height, epochNum, txNum int, multi *store.MultiStore, tx *Tx, cfg *FuzzConfig, epochSuc bool) {
	for i, op := range tx.OpList {
		if DBG {
			fmt.Printf("Check %d-%d (%v) tx %d (%v) operation %d of %d\n", height, epochNum, epochSuc, txNum,  tx.Succeed, i, len(tx.OpList))
			fmt.Printf("%#v\n", op)
		}
		if op.opType == OpRead {
			if !bytes.Equal(op.value[:], multi.Get(op.key[:])) {
				panic(fmt.Sprintf("Error in Get tx#%d", i))
			}
		}
		if op.opType == OpIterate && len(op.value) != 0 {
			var iter storetypes.ObjIterator
			if bytes.Compare(op.key[:], op.keyEnd[:]) < 0 {
				iter = multi.Iterator(op.key[:], op.keyEnd[:])
			} else {
				iter = multi.ReverseIterator(op.keyEnd[:], op.key[:])
			}
			panicReason := ""
			for _, pair := range op.results {
				if !iter.Valid() {
					panicReason = "Iterator Invalid"
					break
				}
				if !bytes.Equal(iter.Key(), pair.Key) {
					panicReason = fmt.Sprintf("Key mismatch real %#v expect %#v", iter.Key(), pair.Key)
					break
				}
				if !bytes.Equal(iter.Value(), pair.Value) {
					panicReason = fmt.Sprintf("Value mismatch real %#v expect %#v", iter.Value(), pair.Value)
					break
				}
				if DBG {fmt.Printf("Key match real %#v expect %#v\n", iter.Key(), pair.Key)}
				iter.Next()
			}
			if len(panicReason) == 0 && iter.Valid() && len(op.results) < int(cfg.MaxIterDistance) {
				panicReason = "Iterator Should be Invalid"
			}
			if len(panicReason) != 0 {
				fmt.Printf("Remaining (at most 10):\n")
				for i := 0; i < 10 && iter.Valid(); i++ {
					fmt.Printf("key: %#v  value: %#v\n", iter.Key(), iter.Value())
					iter.Next()
				}
				panic(panicReason)
			}
			iter.Close()
		}
		if op.opType == OpWrite {
			multi.Set(op.key[:], op.value[:])
		}
		if op.opType == OpDelete {
			multi.Delete(op.key[:])
		}
	}
}

func ExecuteBlock(height int, root storetypes.RootStoreI, block *Block, cfg *FuzzConfig, inParallel bool) {
	showRoot := func(epochNum int, succeed bool) {
		iter := root.Iterator([]byte{}, EndKey)
		defer iter.Close()
		for iter.Valid() {
			fmt.Printf("CHECK.AT %d-%d (%v) key: %#v Value:%#v\n", height, epochNum, succeed, iter.Key(), iter.Value())
			iter.Next()
		}
	}
	//showTrunk := func(trunk *store.TrunkStore, epochNum, txNum int, epochSuc, txSuc bool) {
	//	fmt.Printf("Dumping\n")
	//	iter := trunk.Iterator([]byte{}, EndKey)
	//	defer iter.Close()
	//	for iter.Valid() {
	//		fmt.Printf("CHECK.AT %d-%d (%v) tx %d (%v) key: %#v Value:%#v\n", height, epochNum, epochSuc, txNum, txSuc, iter.Key(), iter.Value())
	//		iter.Next()
	//	}
	//}
	for i, epoch := range block.EpochList {
		if DBG {fmt.Printf("Check h:%d epoch %d (%v) of %d\n", height, i, epoch.Succeed, len(block.EpochList))}
		trunk := root.GetTrunkStore().(*store.TrunkStore)
		dbList := make([]*store.MultiStore, len(epoch.TxList))
		var wg sync.WaitGroup
		for j, tx := range epoch.TxList {
			dbList[j] = trunk.Cached()
			if DBG {fmt.Printf("Check h:%d epoch %d (%v) tx %d (%v) of %d\n", height, i, epoch.Succeed, j, tx.Succeed, len(epoch.TxList))}
			if inParallel {
				wg.Add(1)
				go func(tx *Tx, j int) {
					CheckTx(height, i, j, dbList[j], tx, cfg, epoch.Succeed)
					wg.Done()
				}(tx, j)
			} else {
				CheckTx(height, i, j, dbList[j], tx, cfg, epoch.Succeed)
			}

		}
		if inParallel {wg.Wait()}
		for j, tx := range epoch.TxList {
			if DBG {fmt.Printf("WriteBack %d-%d tx %d : %v\n", height, i, j, tx.Succeed)}
			dbList[j].Close(tx.Succeed)
			//showTrunk(trunk, i, j, tx.Succeed, epoch.Succeed)
		}
		trunk.Close(epoch.Succeed)

		if DBG {showRoot(i, epoch.Succeed)}
	}
}

func runTest() {
	randFilename := os.Getenv("RANDFILE")
	roundCount, err := strconv.Atoi(os.Getenv("RANDCOUNT"))
	if err != nil {
		panic(err)
	}

	rs := randsrc.NewRandSrcFromFileWithSeed(randFilename, []byte{0})
	root := store.NewMockRootStore()
	ref := NewRefStore()
	fmt.Printf("Initialized\n")
	cfg := &FuzzConfig {
		MaxReadCountInTx:     10,
		MaxIterCountInTx:     5,
		MaxWriteCountInTx:    10,
		MaxDeleteCountInTx:   10,
		MaxTxCountInEpoch:    100,
		MaxEpochCountInBlock: 5,
		EffectiveBits:        16,
		MaxIterDistance:      16,
		TxSucceedRatio:       0.85,
		EpochSucceedRatio:    0.95,
	}

	for i := 0; i< roundCount; i++ {
		fmt.Printf("Block %d\n", i)
		block := GenerateRandBlock(i, ref, rs, cfg)
		//ExecuteBlock(i, root, &block, cfg, false) //not in parrallel
		ExecuteBlock(i, root, &block, cfg, true) //in parrallel
	}
}

