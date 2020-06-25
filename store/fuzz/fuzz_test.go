package fuzz

import (
	"testing"
)

// go test -c -coverpkg github.com/coinexchain/onvakv/store .

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
		TxSucceedRatio:       0.85,
		BlockSucceedRatio:    0.95,
		DelAfterIterRatio:    0.05,
		RootType:             "Real",
		TestRabbit:           false,
	}

	//cfg2 := &FuzzConfig {
	//	MaxReadCountInTx:     10,
	//	MaxIterCountInTx:     0,
	//	MaxWriteCountInTx:    10,
	//	MaxDeleteCountInTx:   10,
	//	MaxTxCountInEpoch:    100,
	//	MaxEpochCountInBlock: 5,
	//	EffectiveBits:        16,
	//	MaxIterDistance:      16,
	//	TxSucceedRatio:       0.85,
	//	BlockSucceedRatio:    0.95,
	//	DelAfterIterRatio:    0.00,
	//	RootType:             "MockRoot",
	//	TestRabbit:           true,
	//}

	runTest(cfg1)
}
