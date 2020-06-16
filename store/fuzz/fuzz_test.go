package fuzz

import (
	"testing"
)

// go test -c -coverpkg github.com/coinexchain/onvakv/store .

// RANDFILE=~/Downloads/goland-2019.1.3.dmg RANDCOUNT=1000 ./fuzz.test 

func Test1(t *testing.T) {
	runTest()
}
