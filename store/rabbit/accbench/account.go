package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sort"

	sha256 "github.com/minio/sha256-simd"
	"github.com/coinexchain/randsrc"

	"github.com/coinexchain/onvakv"
	"github.com/coinexchain/onvakv/store"
	"github.com/coinexchain/onvakv/store/rabbit"
)

var (
	GuardStart = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	GuardEnd = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
)

const (
	MaxCoinCount = 20
	NumCoinType = 100

	AddrLen = 20
	AmountLen = 32
	EntryLen = AddrLen + AmountLen
	AddressOffset = 0
	SequenceOffset = AddressOffset + AddrLen
	NativeTokenAmountOffset = SequenceOffset + 8
	ERC20TokenOffset = NativeTokenAmountOffset + AmountLen
)

var CoinIDList [NumCoinType][AddrLen]byte

func CoinTypeToCoinID(i int) (res [AddrLen]byte) {
	hash := sha256.Sum256([]byte(fmt.Sprintf("coin%d", i)))
	copy(res[:], hash[:])
	return
}

func init() {
	for i := range CoinIDList {
		CoinIDList[i] = CoinTypeToCoinID(i)
	}
}

type Coin struct {
	ID     [AddrLen]byte
	Amount [AmountLen]byte
}

type Account struct {
	bz        []byte
	coinCount int
}

func (acc *Account) ToBytes() []byte {
	return acc.bz
}

func (acc *Account) FromBytes(bz []byte) {
	if len(bz) < ERC20TokenOffset || len(bz[ERC20TokenOffset:]) % EntryLen != 0 {
		acc = nil
	}
	acc.bz = bz
	acc.coinCount = len(bz[ERC20TokenOffset:]) / EntryLen
}

func (acc *Account) DeepCopy() interface{} {
	return &Account{
		bz:        append([]byte{}, acc.bz...),
		coinCount: acc.coinCount,
	}
}

func NewAccount(addr [AddrLen]byte, sequence int64, nativeTokenAmount [AmountLen]byte, coins []Coin) Account {
	bz := make([]byte, AddrLen+8+AmountLen+len(coins)*(AddrLen+AmountLen))
	copy(bz[AddressOffset:], addr[:])
	binary.LittleEndian.PutUint64(bz[SequenceOffset:], uint64(sequence))
	copy(bz[NativeTokenAmountOffset:], nativeTokenAmount[:])
	start := ERC20TokenOffset
	for _, coin := range coins {
		copy(bz[start:], coin.ID[:])
		copy(bz[start+AddrLen:], coin.Amount[:])
		start += EntryLen
	}
	return Account{bz: bz, coinCount: len(coins)}
}

func (acc Account) Address() []byte {
	return acc.bz[AddressOffset:AddressOffset+AddrLen]
}

func (acc Account) GetSequence() int64 {
	return int64(binary.LittleEndian.Uint64(acc.bz[SequenceOffset:SequenceOffset+8]))
}

func (acc Account) SetSequence(seq int64) {
	binary.LittleEndian.PutUint64(acc.bz[SequenceOffset:SequenceOffset+8], uint64(seq))
}

func (acc Account) GetNativeAmount() []byte {
	return acc.bz[NativeTokenAmountOffset:NativeTokenAmountOffset+AmountLen]
}

func (acc Account) SetNativeAmount(amount [AmountLen]byte) {
	copy(acc.bz[NativeTokenAmountOffset:], amount[:])
}

func (acc Account) GetTokenID(i int) []byte {
	start := ERC20TokenOffset + i*EntryLen
	return acc.bz[start:start+AddrLen]
}

func (acc Account) GetTokenAmount(i int) []byte {
	start := ERC20TokenOffset + i*EntryLen + AddrLen
	return acc.bz[start:start+AmountLen]
}

func (acc Account) SetTokenAmount(i int, amount [AmountLen]byte) {
	start := ERC20TokenOffset + i*EntryLen + AddrLen
	copy(acc.bz[start:], amount[:])
}

func (acc Account) Find(tokenID [AddrLen]byte) int {
	i := sort.Search(acc.coinCount, func(i int) bool {
		return bytes.Compare(acc.GetTokenID(i), tokenID[:]) >= 0
	})
	if i < acc.coinCount && bytes.Equal(acc.GetTokenID(i), tokenID[:]) { //present
		return i
	} else { // not present
		return -1
	}
}

func GetCoinList(accountSN int64) []uint8 {
	hash := sha256.Sum256([]byte(fmt.Sprintf("cointypelist%d", accountSN)))
	coinCount := 1 + hash[0]%MaxCoinCount
	res := make([]uint8, coinCount)
	for i := range res {
		res[i] = hash[i+1] % NumCoinType
	}
	return res
}

func BigIntToBytes(i *big.Int) (res [AmountLen]byte) {
	bz := i.Bytes()
	if len(bz) > AmountLen {
		panic("Too large")
	}
	startingZeros := len(res)-len(bz)
	copy(res[startingZeros:], bz)
	return res
}

func GetRandAmount(rs randsrc.RandSrc) [AmountLen]byte {
	i := big.NewInt(int64(rs.GetUint32()))
	i.Lsh(i, 128)
	return BigIntToBytes(i)
}

func SNToAddr(accountSN int64) (addr [AddrLen]byte) {
	hash := sha256.Sum256([]byte(fmt.Sprintf("address%d", accountSN)))
	copy(addr[:], hash[:])
	return
}

func GenerateAccount(accountSN int64, rs randsrc.RandSrc) Account {
	nativeTokenAmount := GetRandAmount(rs)
	coinList := GetCoinList(accountSN)
	coins := make([]Coin, len(coinList))
	for i := range coins {
		coinType := int(coinList[i])
		coins[i].ID = CoinIDList[coinType]
		coins[i].Amount = GetRandAmount(rs)
	}
	sort.Slice(coins, func(i, j int) bool {
		return bytes.Compare(coins[i].ID[:], coins[j].ID[:]) < 0
	})
	return NewAccount(SNToAddr(accountSN), 0, nativeTokenAmount, coins)
}

func RunGenerateAccounts(numAccounts int, randFilename string, jsonFile string) {
	addr2num := make(map[[AddrLen]byte]uint64)
	rs := randsrc.NewRandSrcFromFile(randFilename)
	okv, err := onvakv.NewOnvaKV("./onvakv4test", false, [][]byte{GuardStart, GuardEnd})
	if err != nil {
		panic(err)
	}
	root := store.NewRootStore(okv, nil, nil)

	numBlocks := numAccounts / 1000 //create 1000 accounts every block
	for i := 0; i < numBlocks; i++ {
		trunk := root.GetTrunkStore().(*store.TrunkStore)
		rbt := rabbit.NewRabbitStore(trunk)
		for j := 0; j < 1000; j++ {
			acc := GenerateAccount(int64(i*1000+j), rs)
			rbt.SetObj(acc.Address(), &acc)
			path, ok := rbt.GetShortKeyPath(acc.Address())
			if !ok {
				panic("Cannot get the object which was just set")
			}
			if len(path) > 1 {
				var addr [AddrLen]byte
				copy(addr[:], acc.Address())
				n := binary.LittleEndian.Uint64(path[len(path)-1][:])
				addr2num[addr] = n
			}
		}
		rbt.Close(true)
		trunk.Close(true)
	}

	b, err := json.Marshal(addr2num)
	out, err := os.OpenFile(jsonFile, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		panic(err)
	}
	out.Write(b)
	out.Close()

	root.Close()
}

