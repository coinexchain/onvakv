package main

import (
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"os"

	sha256 "github.com/minio/sha256-simd"
	"github.com/coinexchain/randsrc"
)

type Tx struct {
	FromNum uint64
	ToNum   uint64
	CoinID  [AddrLen]byte
	Amount  uint64
}

func (tx Tx) ToBytes() []byte {
	res := make([]byte, 24, 24+AddrLen)
	binary.LittleEndian.PutUint64(res[0:8], tx.Amount)
	binary.LittleEndian.PutUint64(res[8:16], tx.FromNum)
	binary.LittleEndian.PutUint64(res[16:24], tx.ToNum)
	return append(res, tx.CoinID[:]...)
}

func ParseTx(bz [24+AddrLen]byte) (tx Tx) {
	tx.Amount = binary.LittleEndian.Uint64(bz[0:8])
	tx.FromNum = binary.LittleEndian.Uint64(bz[8:16])
	tx.ToNum = binary.LittleEndian.Uint64(bz[16:24])
	copy(tx.CoinID[:], bz[24:])
	return
}

func getNum(addr2num map[[AddrLen]byte]uint64, addr [AddrLen]byte) uint64 {
	if num, ok := addr2num[addr]; ok {
		return num
	}
	hash := sha256.Sum256(addr[:])
	num := binary.LittleEndian.Uint64(hash[:8])
	num = num | 0x1
	return num
}

func GenerateTx(accNum int, addr2num map[[AddrLen]byte]uint64, rs randsrc.RandSrc) *Tx {
	fromSN := int(rs.GetUint64()) % accNum
	fromCoinList := GetCoinList(int64(fromSN))
	fromCoinMap := make(map[uint8]struct{}, len(fromCoinList))
	for _, coinType := range fromCoinList {
		fromCoinMap[coinType] = struct{}{}
	}
	tx := Tx{FromNum: getNum(addr2num, SNToAddr(int64(fromSN)))}
	toSN := int(rs.GetUint64()) % accNum
	for {
		toCoinList := GetCoinList(int64(toSN))
		coinTypeOfBoth := -1
		for _, coinType := range toCoinList {
			if _, ok := fromCoinMap[coinType]; ok {
				coinTypeOfBoth = int(coinType)
				break
			}
		}
		if coinTypeOfBoth > 0 {
			tx.ToNum = getNum(addr2num, SNToAddr(int64(toSN)))
			tx.CoinID = CoinIDList[coinTypeOfBoth]
			break
		} else {
			toSN = int(rs.GetUint64()) % accNum
		}
	}
	tx.Amount = rs.GetUint64()
	return &tx
}

func RunGenerateTxFile(epochCount, accNum int, jsonFilename, randFilename, outFilename string) {
	jsonFile, err := os.Open(jsonFilename)
	if err != nil {
		panic(err)
	}
	bz, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		panic(err)
	}
	addr2num := make(map[[AddrLen]byte]uint64)
	err = json.Unmarshal(bz, addr2num)
	if err != nil {
		panic(err)
	}
	rs := randsrc.NewRandSrcFromFile(randFilename)
	out, err := os.OpenFile(outFilename, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	for i := 0; i < epochCount; i++ {
		txCount := 0
		touchedNum := make(map[uint64]struct{}, 2*txCount)
		for {
			tx := GenerateTx(accNum, addr2num, rs)
			_, fromConflict := touchedNum[tx.FromNum]
			_, toConflict := touchedNum[tx.ToNum]
			if fromConflict || toConflict {
				continue
			}
			touchedNum[tx.FromNum] = struct{}{}
			touchedNum[tx.ToNum] = struct{}{}
			_, err := out.Write(tx.ToBytes())
			if err != nil {
				panic(err)
			}
			txCount++
			if txCount == NumTxInEpoch {
				break
			}
		}
	}
}

