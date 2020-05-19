package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	sha256 "github.com/minio/sha256-simd"
	"github.com/coinexchain/randsrc"
)

const (
	ReadCount = 100*10000
	PageSize = 4096
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <rand-source-file> <page-count>\n", os.Args[0])
		return
	}
	randFilename := os.Args[1]
	pageCount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	RandWriteFile(pageCount, randFilename)

	//var wg sync.WaitGroup
	//for i := 0; i < 64; i++ {
	//	wg.Add(1)
	//	go RandRead(pageCount, randFilename, byte(i), &wg)
	//}
	//wg.Wait()
}

func RandWriteFile(pageCount int, randFilename string) {
	datFile, err := os.OpenFile("a.dat", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0700)
	if err != nil {
		panic(err)
	}
	defer datFile.Close()
	rs := randsrc.NewRandSrcFromFile(randFilename)
	var buf [32]byte
	for i := 0; i < pageCount; i++ {
		copy(buf[:], rs.GetBytes(32))
		for j := 0; j < PageSize/32; j++ {
			buf = sha256.Sum256(buf[:])
			datFile.Write(buf[:])
		}
	}
	datFile.Sync()
}

func RandRead(pageCount int, randFilename string, seed byte, wg *sync.WaitGroup) (res byte) {
	rs := randsrc.NewRandSrcFromFileWithSeed(randFilename, []byte{seed})
	datFile, err := os.Open("a.dat")
	if err != nil {
		panic(err)
	}
	var buf [4096]byte
	for i := 0; i < ReadCount; i++ {
		n := int(rs.GetUint64())%pageCount
		datFile.ReadAt(buf[:], int64(n)*int64(PageSize))
		for _, b := range buf[:] {
			res += b
		}
	}
	wg.Done()
	return res
}
