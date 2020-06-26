package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"sync"
	"bufio"
	"strings"

	"github.com/coinexchain/randsrc"
	"github.com/coinexchain/onvakv"
	"github.com/coinexchain/onvakv/store"
	"github.com/coinexchain/onvakv/store/rabbit"
)

const (
	BatchSize = 1000
	SamplePos = 99

	Stripe = 64
	ReadBatchSize = 64*Stripe
)

type KVPair struct {
	Key, Value []byte
}

func ReadSamples(fname string, kvCount int, batchHandler func(batch []KVPair)) int {
	file, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var readBatch [ReadBatchSize]KVPair
	idx := 0
	totalRun := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, " ")
		if len(fields) != 3 || fields[0] != "SAMPLE" {
			panic("Invalid line: "+line)
		}
		k, err := base64.StdEncoding.DecodeString(fields[1])
		if err != nil {
			panic(err)
		}
		v, err := base64.StdEncoding.DecodeString(fields[2])
		if err != nil {
			panic(err)
		}
		readBatch[idx] = KVPair{k, v}
		idx++
		if idx == ReadBatchSize {
			idx = 0
			batchHandler(readBatch[:])
			totalRun++
			if totalRun*ReadBatchSize >= kvCount {break}
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
	return totalRun
}


var (
	GuardStart = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	GuardEnd = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255}
)

func main() {
	if len(os.Args) != 4 || (os.Args[1] != "rp" && os.Args[1] != "rs" && os.Args[1] != "w") {
		fmt.Printf("Usage: %s w <rand-source-file> <kv-count>\n", os.Args[0])
		fmt.Printf("Usage: %s rp <sample-file> <kv-count>\n", os.Args[0])
		fmt.Printf("Usage: %s rs <sample-file> <kv-count>\n", os.Args[0])
		return
	}
	kvCount, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}

	okv, err := onvakv.NewOnvaKV("./onvakv4test", false, [][]byte{GuardStart, GuardEnd})
	if err != nil {
		panic(err)
	}
	root := store.NewRootStore(okv, nil, func(k []byte) bool {return false})

	if os.Args[1] == "w" {
		randFilename := os.Args[2]
		rs := randsrc.NewRandSrcFromFile(randFilename)
		RandomWrite(root, rs, kvCount)
		root.Close()
		return
	}

	sampleFilename := os.Args[2]
	var totalRun int
	trunk := root.GetTrunkStore().(*store.TrunkStore)
	if os.Args[1] == "rp" {
		totalRun = ReadSamples(sampleFilename, kvCount, func(batch []KVPair) {
			rbt := rabbit.NewRabbitStore(trunk)
			checkPar(rbt, batch)
		})
	}
	if os.Args[1] == "rs" {
		totalRun = ReadSamples(sampleFilename, kvCount, func(batch []KVPair) {
			rbt := rabbit.NewRabbitStore(trunk)
			checkSer(rbt, batch)
		})
	}
	fmt.Printf("totalRun: %d\n", totalRun)
}

func RandomWrite(root *store.RootStore, rs randsrc.RandSrc, count int) {
	file, err := os.OpenFile("./sample.txt", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	numBatch := count/BatchSize
	for i := 0; i < numBatch; i++ {
		root.SetHeight(int64(i))
		trunk := root.GetTrunkStore().(*store.TrunkStore)
		rbt := rabbit.NewRabbitStore(trunk)
		if i % 100 == 0 {
			fmt.Printf("Now %d of %d\n", i, numBatch)
		}
		for j := 0; j < BatchSize; j++ {
			k := rs.GetBytes(32)
			v := rs.GetBytes(32)
			if j == SamplePos {
				s := fmt.Sprintf("SAMPLE %s %s\n", base64.StdEncoding.EncodeToString(k),
					base64.StdEncoding.EncodeToString(v))
				_, err := file.Write([]byte(s))
				if err != nil {
					panic(err)
				}
			}
			rbt.Set(k, v)
		}
		rbt.Close(true)
		trunk.Close(true)
	}
}

func checkPar(rbt rabbit.RabbitStore, batch []KVPair) {
	if len(batch) != ReadBatchSize {
		panic(fmt.Sprintf("invalid size %d %d", len(batch), ReadBatchSize))
	}
	var wg sync.WaitGroup
	for i := 0; i < ReadBatchSize/Stripe; i++ {
		wg.Add(1)
		go func(start, end int) {
			for _, pair := range batch[start:end] {
				v := rbt.Get(pair.Key)
				if !bytes.Equal(v, pair.Value) {
					fmt.Printf("Not Equal for %v: ref: %v actual: %v\n", pair.Key, pair.Value, v)
				}
			}
			wg.Done()
		}(i*Stripe, (i+1)*Stripe)
	}
	wg.Wait()
}

func checkSer(rbt rabbit.RabbitStore, batch []KVPair) {
	if len(batch) != ReadBatchSize {
		panic("invalid size")
	}
	for _, pair := range batch {
		v := rbt.Get(pair.Key)
		if !bytes.Equal(v, pair.Value) {
			fmt.Printf("Not Equal for %v: ref: %v actual: %v\n", pair.Key, pair.Value, v)
		}
	}
}


