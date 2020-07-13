package fuzz

import (
	"testing"
)

// go test -tags cppbtree -c -coverpkg github.com/coinexchain/onvakv/store .
// RANDFILE=~/Downloads/goland-2019.1.3.dmg RANDCOUNT=1000 ./fuzz.test 

func Test1(t *testing.T) {
	cfg1 := &FuzzConfig {
		MaxReadCountInTx:     10,
		MaxIterCountInTx:     5,
		MaxWriteCountInTx:    10,
		MaxDeleteCountInTx:   10,
		MaxTxCountInEpoch:    100,
		MaxEpochCountInBlock: 5,
		EffectiveBits:        16,
		MaxIterDistance:      16,
		MaxActiveCount:       -1,
		TxSucceedRatio:       0.85,
		BlockSucceedRatio:    0.95,
		DelAfterIterRatio:    0.05,
		RootType:             "Real",
		TestRabbit:           false,
	}
	runTest(cfg1)

	//cfg2 := &FuzzConfig {
	//	MaxReadCountInTx:     10,
	//	MaxIterCountInTx:     0,
	//	MaxWriteCountInTx:    10,
	//	MaxDeleteCountInTx:   10,
	//	MaxTxCountInEpoch:    1, // For rabbit, we cannot avoid inter-tx dependency
	//	MaxEpochCountInBlock: 500,
	//	EffectiveBits:        16,
	//	MaxIterDistance:      16,
	//	MaxActiveCount:       256*256/16,
	//	TxSucceedRatio:       1,
	//	BlockSucceedRatio:    0.95,
	//	DelAfterIterRatio:    0.05,
	//	RootType:             "MockRoot",
	//	TestRabbit:           true,
	//}
	//runTest(cfg2)

}
