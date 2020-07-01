package main

import (
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"os"

	sha256 "github.com/minio/sha256-simd"
	"github.com/coinexchain/randsrc"
)

const (
	TxLen = 32
)

type Tx struct {
	FromNum uint64
	ToNum   uint64
	Amount  uint64
	CoinID  [ShortIDLen]byte
}

func (tx Tx) ToBytes() []byte {
	res := make([]byte, TxLen)
	binary.LittleEndian.PutUint64(res[0:8], tx.Amount)
	binary.LittleEndian.PutUint64(res[8:16], tx.FromNum)
	binary.LittleEndian.PutUint64(res[16:24], tx.ToNum)
	copy(res[24:32], tx.CoinID[:])
	return res
}

func ParseTx(bz [TxLen]byte) (tx Tx) {
	tx.Amount = binary.LittleEndian.Uint64(bz[0:8])
	tx.FromNum = binary.LittleEndian.Uint64(bz[8:16])
	tx.ToNum = binary.LittleEndian.Uint64(bz[16:24])
	copy(tx.CoinID[:], bz[24:32])
	return
}

// Given an account's address, calculate its "card number"
func getNum(addr2num map[[AddrLen]byte]uint64, addr [AddrLen]byte) uint64 {
	if num, ok := addr2num[addr]; ok {
		// if the card number was recorded, use it
		return num
	}
	// otherwise we re-compute the card number, with one hop
	hash := sha256.Sum256(addr[:])
	num := binary.LittleEndian.Uint64(hash[:8])
	num = num | 0x1
	return num
}

// generate a random Tx, whose from-account and to-account have at least one coin in common
func GenerateTx(totalAccounts int, addr2num map[[AddrLen]byte]uint64, rs randsrc.RandSrc) *Tx {
	fromSN := int(rs.GetUint64()) % totalAccounts
	fromCoinList := GetCoinList(int64(fromSN))
	fromCoinMap := make(map[uint8]struct{}, len(fromCoinList))
	for _, coinType := range fromCoinList {
		fromCoinMap[coinType] = struct{}{}
	}
	tx := Tx{FromNum: getNum(addr2num, SNToAddr(int64(fromSN)))}
	toSN := int(rs.GetUint64()) % totalAccounts
	for {
		toCoinList := GetCoinList(int64(toSN))
		coinTypeOfBoth := -1
		for _, coinType := range toCoinList {
			if _, ok := fromCoinMap[coinType]; ok {
				coinTypeOfBoth = int(coinType)
				break
			}
		}
		if coinTypeOfBoth > 0 { // both of them have one same coin
			tx.ToNum = getNum(addr2num, SNToAddr(int64(toSN)))
			tx.CoinID = CoinIDList[coinTypeOfBoth]
			break
		} else { //try another to-account
			toSN = int(rs.GetUint64()) % totalAccounts
		}
	}
	tx.Amount = rs.GetUint64()
	return &tx
}

func RunGenerateTxFile(epochCount, totalAccounts int, jsonFilename, randFilename, outFilename string) {
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
	// one account can just occur once in an epoch, otherwise there will be conflicts
	// we must filter away the TXs which conflict with existing TXs in the epoch
	for i := 0; i < epochCount; i++ {
		txCount := 0
		touchedNum := make(map[uint64]struct{}, 2*txCount)
		for {
			tx := GenerateTx(totalAccounts, addr2num, rs)
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

