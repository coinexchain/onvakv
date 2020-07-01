package main

import (
	"fmt"
	"encoding/binary"
	"math/big"
	"os"
	"sync"

	"github.com/coinexchain/onvakv"
	"github.com/coinexchain/onvakv/store"
	"github.com/coinexchain/onvakv/store/rabbit"
)

const (
	NumTxPerWorker = 16
	NumTxInEpoch = 1024
	NumWorkers = NumTxInEpoch / NumTxPerWorker
	NumEpochInBlock = 32
)

type Epoch struct {
	jobList [NumWorkers]Job
}

type AccountAndNum struct {
	acc Account
	num uint64
}

type Job struct {
	txList         [NumTxPerWorker]Tx
	changedAccList []AccountAndNum
}

func ReadEpoch(fin *os.File) (epoch Epoch) {
	for i := 0; i < NumWorkers; i++ {
		for j := 0; j < NumTxPerWorker; j++ {
			var bz [TxLen]byte
			_, err := fin.Read(bz[:])
			if err != nil {
				panic(err)
			}
			epoch.jobList[i].txList[j] = ParseTx(bz)
		}
		epoch.jobList[i].changedAccList = make([]AccountAndNum, 0, 2*NumTxPerWorker)
	}
	return
}

func (epoch Epoch) Check() bool {
	touchedNum := make(map[uint64]struct{}, 2*NumTxInEpoch)
	for i := 0; i < NumWorkers; i++ {
		for j := 0; j < NumTxPerWorker; j++ {
			tx := epoch.jobList[i].txList[j]
			_, fromConflict := touchedNum[tx.FromNum]
			_, toConflict := touchedNum[tx.ToNum]
			if fromConflict || toConflict {
				return false
			}
			touchedNum[tx.FromNum] = struct{}{}
			touchedNum[tx.ToNum] = struct{}{}
		}
	}
	return true
}

func (epoch Epoch) Run(root *store.RootStore) {
	var wg sync.WaitGroup
	isValid := true
	wg.Add(1+NumWorkers)
	go func() {
		isValid = epoch.Check()
		wg.Done()
	}()
	for i := range epoch.jobList {
		go func(i int) {
			epoch.jobList[i].Run(root)
			wg.Done()
		}(i)
	}
	wg.Wait()
	if !isValid {
		fmt.Printf("Found an invalid epoch!")
		return
	}
	for i := 0; i < NumWorkers; i++ {
		for _, accAndNum := range epoch.jobList[i].changedAccList {
			k := getShortKey(accAndNum.num)
			v := accAndNum.acc.ToBytes()
			root.Set(k, v)
		}
	}
}

func getShortKey(n uint64) []byte {
	var shortKey [rabbit.KeySize]byte
	binary.LittleEndian.PutUint64(shortKey[:], n)
	return shortKey[:]
}

func (job *Job) Run(root *store.RootStore) {
	for _, tx := range job.txList {
		job.executeTx(root, tx)
	}
}

func (job *Job) executeTx(root *store.RootStore, tx Tx) {
	var fromAcc, toAcc Account
	fromShortKey := getShortKey(tx.FromNum)
	toShortKey := getShortKey(tx.ToNum)
	root.PrepareForUpdate(fromShortKey)
	fromAccBz := root.Get(fromShortKey)
	if len(fromAccBz) == 0 {
		fmt.Printf("Cannot find from-account")
		return
	}
	root.PrepareForUpdate(toShortKey)
	toAccBz := root.Get(toShortKey)
	if len(toAccBz) == 0 {
		fmt.Printf("Cannot find to-account")
		return
	}
	fromAcc.FromBytes(fromAccBz)
	toAcc.FromBytes(toAccBz)
	fromIdx := fromAcc.Find(tx.CoinID)
	if fromIdx < 0 {
		fmt.Printf("Cannot find the token in from-account")
		return
	}
	toIdx := toAcc.Find(tx.CoinID)
	if toIdx < 0 {
		fmt.Printf("Cannot find the token in to-account")
		return
	}
	amount := int64(tx.Amount)
	if amount < 0 {
		amount = -amount
	}
	nativeTokenAmount, fromAmount, toAmount, toNewAmount := &big.Int{}, &big.Int{}, &big.Int{}, &big.Int{}
	fromAmount.SetBytes(fromAcc.GetTokenAmount(fromIdx))
	toAmount.SetBytes(toAcc.GetTokenAmount(toIdx))
	amountInt := big.NewInt(amount)
	if fromAmount.Cmp(amountInt) < 0 { // not enough tokens
		fmt.Printf("Not enough token")
		return // fail
	}
	fromAmount.Sub(fromAmount, amountInt)
	toNewAmount.Add(toAmount, amountInt)
	if toNewAmount.Cmp(toAmount) < 0 { //overflow
		fmt.Printf("token overflow")
		return // fail
	}
	fromAcc.SetTokenAmount(fromIdx, BigIntToBytes(fromAmount))
	toAcc.SetTokenAmount(toIdx, BigIntToBytes(toAmount))
	nativeTokenAmount.SetBytes(fromAcc.GetNativeAmount())
	gas := big.NewInt(10)
	if nativeTokenAmount.Cmp(gas) < 0 { //overflow
		fmt.Printf("not enough native token for gas")
		return // fail
	}
	nativeTokenAmount.Sub(nativeTokenAmount, gas)
	fromAcc.SetNativeAmount(BigIntToBytes(nativeTokenAmount))
	fromAcc.SetSequence(fromAcc.GetSequence()+1)
	job.changedAccList = append(job.changedAccList, AccountAndNum{fromAcc, tx.FromNum})
	job.changedAccList = append(job.changedAccList, AccountAndNum{toAcc, tx.ToNum})
}

func RunTx(numEpoch int, txFile string) {
	fin, err := os.Open(txFile)
	if err != nil {
		panic(err)
	}

	okv, err := onvakv.NewOnvaKV("./onvakv4test", false, [][]byte{GuardStart, GuardEnd})
	if err != nil {
		panic(err)
	}
	for i := 0; i < numEpoch / NumEpochInBlock; i++ {
		root := store.NewRootStore(okv, nil, nil)
		for j := 0; j < NumEpochInBlock; j++ {
			epoch := ReadEpoch(fin)
			epoch.Run(root)
		}
		root.Close()
	}
}

