package fuzzaboveroot

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
	OpRead = 0
	OpWrite = 1
	OpDelete = 2
	OpIterate = 3
)

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
}

//TODO cached writes can be read out

type Pair struct {
	Key, Value []byte
}

type Operation struct {
	op      int
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
	TxList []Tx
}

type Block struct {
	EpochList []Epoch
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
			touchedKeys[i] = struct{}{}
			break
		}
	}
	return
}

func UpdateRefStoreWithTx(ref *RefStore, tx *Tx) {
	for _, op := range tx.OpList {
		if op.op == OpWrite {
			ref.Set(op.key[:], op.value[:])
		}
		if op.op == OpDelete {
			ref.Delete(op.key[:])
		}
	}
}

func GenerateRandTx(ref *RefStore, rs randsrc.RandSrc, cfg *FuzzConfig, touchedKeys map[uint64]struct{}) Tx {
	readCount, iterCount, writeCount, deleteCount := uint32(0), uint32(0), uint32(0), uint32(0)
	maxReadCount := rs.GetUint32()%cfg.MaxReadCountInTx
	maxIterCount := rs.GetUint32()%cfg.MaxIterCountInTx
	maxWriteCount := rs.GetUint32()%cfg.MaxWriteCountInTx
	maxDeleteCount := rs.GetUint32()%cfg.MaxDeleteCountInTx
	tx := Tx{OpList: make([]Operation, 0, maxReadCount+maxWriteCount+maxDeleteCount)}
	for readCount!=maxReadCount || iterCount!=maxIterCount ||
		writeCount!=maxWriteCount || deleteCount!=maxDeleteCount {
		if rs.GetUint32()%4 == 0 && readCount < maxReadCount {
			key := getRand8Bytes(rs, cfg, touchedKeys)
			tx.OpList = append(tx.OpList, Operation{
				op:    OpRead,
				key:   key,
				value: ref.Get(key[:]),
			})
			readCount++
		}
		if rs.GetUint32()%4 == 0 && iterCount < maxIterCount {
			key := getRand8Bytes(rs, cfg, touchedKeys)
			keyEnd := getRand8Bytes(rs, cfg, touchedKeys)
			var iter storetypes.ObjIterator
			var op Operation
			if bytes.Compare(key[:], keyEnd[:]) < 0 {
				iter = ref.Iterator(key[:], keyEnd[:])
				op = Operation{
					op:     OpIterate,
					key:    key,
					keyEnd: keyEnd,
				}
			} else {
				iter = ref.ReverseIterator(keyEnd[:], key[:])
				op = Operation{
					op:     OpIterate,
					key:    key,
					keyEnd: keyEnd,
				}
			}
			iterSucceed := true
			if iter.Valid() {
				var key uint64
				for i := 0; i < int(cfg.MaxIterDistance); i++ {
					if !iter.Valid() {break}
					key = binary.LittleEndian.Uint64(iter.Key())
					if _, ok := touchedKeys[key]; !ok {
						iterSucceed = false
						break
					}
					touchedKeys[key] = struct{}{}
					op.results = append(op.results, Pair{
						Key:   append([]byte{}, iter.Key()...),
						Value: append([]byte{}, iter.Value()...),
					})
					iter.Next()
				}
				if iterSucceed {
					tx.OpList = append(tx.OpList, op)
				}
			} else {
				tx.OpList = append(tx.OpList, op)
			}
			iterCount++
			iter.Close()
		}
		if rs.GetUint32()%4 == 0 && writeCount < maxWriteCount {
			v := getRand8Bytes(rs, cfg, nil)
			tx.OpList = append(tx.OpList, Operation{
				op:    OpWrite,
				key:   getRand8Bytes(rs, cfg, touchedKeys),
				value: v[:],
			})
			writeCount++
		}
		if rs.GetUint32()%4 == 0 && deleteCount < maxDeleteCount {
			tx.OpList = append(tx.OpList, Operation{
				op:  OpDelete,
				key: getRand8Bytes(rs, cfg, touchedKeys),
			})
			deleteCount++
		}
	}
	tx.Succeed = float32(rs.GetUint32()%0x10000)/float32(0x10000) < cfg.TxSucceedRatio
	return tx
}

func GenerateRandEpoch(ref *RefStore, rs randsrc.RandSrc, cfg *FuzzConfig) Epoch {
	keyCountEstimated := cfg.MaxTxCountInEpoch*(cfg.MaxReadCountInTx+cfg.MaxWriteCountInTx+cfg.MaxDeleteCountInTx)/2
	touchedKeys := make(map[uint64]struct{}, keyCountEstimated)
	txCount := rs.GetUint32()%cfg.MaxTxCountInEpoch
	epoch := Epoch{TxList: make([]Tx, int(txCount))}
	for i := range epoch.TxList {
		epoch.TxList[i] = GenerateRandTx(ref, rs, cfg, touchedKeys)
	}
	for _, tx := range epoch.TxList {
		if !tx.Succeed {continue}
		for _, op := range tx.OpList {
			if op.op == OpWrite {
				ref.Set(op.key[:], op.value[:])
			}
			if op.op == OpDelete {
				ref.Delete(op.key[:])
			}
		}
	}
	return epoch
}

func GenerateRandBlock(ref *RefStore, rs randsrc.RandSrc, cfg *FuzzConfig) Block {
	epochCount := rs.GetUint32()%cfg.MaxEpochCountInBlock
	block := Block{EpochList: make([]Epoch, epochCount)}
	for i := range block.EpochList {
		block.EpochList[i] = GenerateRandEpoch(ref, rs, cfg)
	}
	return block
}

func CheckTx(multi *store.MultiStore, tx *Tx) {
	for _, op := range tx.OpList {
		if op.op == OpRead {
			if !bytes.Equal(op.value[:], multi.Get(op.key[:])) {
				panic("Error in Get")
			}
		}
		if op.op == OpIterate {
			var iter storetypes.ObjIterator
			if bytes.Compare(op.key[:], op.keyEnd[:]) < 0 {
				iter = multi.Iterator(op.key[:], op.keyEnd[:])
			} else {
				iter = multi.ReverseIterator(op.keyEnd[:], op.key[:])
			}
			for _, pair := range op.results {
				if !iter.Valid() {
					panic("Iterator Invalid")
				}
				if !bytes.Equal(iter.Key(), pair.Key) {
					panic("Key mismatch")
				}
				if !bytes.Equal(iter.Value(), pair.Value) {
					panic("Value mismatch")
				}
				iter.Next()
			}
			if iter.Valid() {
				panic("Iterator Should be Invalid")
			}
		}
		if op.op == OpWrite && tx.Succeed {
			multi.Set(op.key[:], op.value[:])
		}
		if op.op == OpDelete && tx.Succeed {
			multi.Delete(op.key[:])
		}
	}
}

func ExecuteBlock(root storetypes.RootStoreI, block *Block) {
	for _, epoch := range block.EpochList {
		trunk := root.GetTrunkStore().(*store.TrunkStore)
		var wg sync.WaitGroup
		for _, tx := range epoch.TxList {
			wg.Add(1)
			go func(tx *Tx) {
				multi := trunk.Cached()
				CheckTx(multi, tx)
				wg.Done()
			}(&tx)
		}
		wg.Wait()
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
		TxSucceedRatio:       0.9,
	}

	for i := 0; i< roundCount; i++ {
		block := GenerateRandBlock(ref, rs, cfg)
		ExecuteBlock(root, &block)
	}
}

