package nvtree

import (
	"bytes"
	"os"
	"io"
	"io/ioutil"
	"fmt"
	"math"
	"strings"
	"strconv"
	"sort"
	"sync"
	"encoding/binary"
	"encoding/hex"

	"github.com/mmcloughlin/meow"

	"github.com/coinexchain/onvakv/nvtreemap/b"
)

const MaxEntryCount int64 = 1024*1024

type ForwardIter interface {
	Close()
	Next() (key []byte, v []byte, err error)
}
type BackwardIter interface {
	Close()
	Prev() (key []byte, v []byte, err error)
}

type NVTree interface {
	Init(dirname string) error
	BeginWrite()
	EndWrite(height int64)
	GetHeight() int64
	ForwardIterFrom(k []byte) ForwardIter
	BackwardIterFrom(k []byte) BackwardIter
	Set(k, v []byte)
	Get(k []byte) ([]byte, bool)
	Delete(k []byte)
}

const (
	DUPLOG byte = 0
	SET byte = 1
	DEL byte = 2
	HEIGHT byte = 3
)

type KVLog interface {
	Init(dirname string, runFn func(op byte, key, value []byte)) error
	Sync()
	IncrRound()
	WriteHeight(height int64)
	GetHeight() int64
	Set(key, value []byte)
	DupLog(key, value []byte)
	Delete(key []byte)
}

// ============================

type ForwardIterMem struct {
	enumerator *b.Enumerator
}
type BackwardIterMem struct {
	enumerator *b.Enumerator
}
var _ ForwardIter = (*ForwardIterMem)(nil)
var _ BackwardIter = (*BackwardIterMem)(nil)

type NVTreeMem struct {
	mtx        sync.RWMutex
	bt         *b.Tree
	isWriting  bool
	kvlog      KVLog
	lastLogKey []byte
	numUpdate  int64
}
var _ NVTree = (*NVTreeMem)(nil)

func (iter *ForwardIterMem) Close() {
	iter.enumerator.Close()
}
func (iter *ForwardIterMem) Next() (key []byte, v []byte, err error) {
	return iter.enumerator.Next()
}
func (iter *BackwardIterMem) Close() {
	iter.enumerator.Close()
}
func (iter *BackwardIterMem) Prev() (key []byte, v []byte, err error) {
	return iter.enumerator.Next()
}

func NewNVTreeMem() NVTree {
	btree := b.TreeNew(bytes.Compare)
	return &NVTreeMem {
		kvlog: &KVFileLog{},
		bt:    btree,
	}
}

func (tree *NVTreeMem) Init(dirname string) error {
	return tree.kvlog.Init(dirname, func(op byte, key, value []byte) {
		switch op {
		case DUPLOG:
			tree.lastLogKey = key
			tree.bt.Set(key, value)
		case SET:
			tree.bt.Set(key, value)
		case DEL:
			tree.bt.Delete(key)
		default:
			panic("Invalid Op")
		}
	})
}

func (tree *NVTreeMem) BeginWrite() {
	tree.mtx.Lock()
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	tree.isWriting = true
}

func (tree *NVTreeMem) EndWrite(height int64) {
	tree.isWriting = false
	tree.mtx.Unlock()
	tree.mtx.RLock()
	tree.kvlog.WriteHeight(height)
	go tree.writeDupLog()
}

func (tree *NVTreeMem) GetHeight() int64 {
	return tree.kvlog.GetHeight()
}

func (tree *NVTreeMem) writeDupLog() {
	defer tree.mtx.RUnlock()
	defer tree.kvlog.Sync()
	enumerator, _ := tree.bt.Seek(tree.lastLogKey)
	key, value, err := enumerator.Next()
	for err != io.EOF && tree.numUpdate != 0 {
		tree.kvlog.DupLog(key, value)
		key, value, err = enumerator.Next()
		tree.numUpdate--
	}
	if tree.numUpdate == 0 {
		return
	}
	tree.kvlog.IncrRound()
	enumerator, _ = tree.bt.SeekFirst()
	for err != io.EOF && tree.numUpdate != 0 {
		tree.kvlog.DupLog(key, value)
		key, value, err = enumerator.Next()
		tree.numUpdate--
	}
}

func (tree *NVTreeMem) Set(k, v []byte) {
	if !tree.isWriting {
		panic("tree.isWriting must be true! bug here...")
	}
	tree.bt.Set(k, v)
	tree.numUpdate++
}

func (tree *NVTreeMem) Get(k []byte) ([]byte, bool) {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	return tree.bt.Get(k)
}

func (tree *NVTreeMem) Delete(k []byte) {
	if !tree.isWriting {
		panic("tree.isWriting must be true! bug here...")
	}
	tree.bt.Delete(k)
}

func (tree *NVTreeMem) ForwardIterFrom(k []byte) ForwardIter {
	iter := &ForwardIterMem{}
	iter.enumerator, _ = tree.bt.Seek(k)
	return iter
}

func (tree *NVTreeMem) BackwardIterFrom(k []byte) BackwardIter {
	var ok bool
	iter := &BackwardIterMem{}
	iter.enumerator, ok = tree.bt.Seek(k)
	if !ok { //now iter.enumerator > k
		iter.enumerator.Next()
	}
	return iter
}

// ===============================================================

type logFileInfo struct {
	round   int64
	endKey []byte
	name    string
}

func parseLogFileInfo(name string) (logfn logFileInfo, err error) {
	twoParts := strings.Split(name, "-")
	if len(twoParts) != 2 {
		err = fmt.Errorf("%s does not match the pattern 'RoundNumber-PrevKey'",name)
		return
	}
	logfn.name = name
	logfn.round, err = strconv.ParseInt(twoParts[0], 10, 63)
	if err != nil {
		return
	}
	logfn.endKey, err = hex.DecodeString(twoParts[1])
	return
}


func getFileInfosInDir(dirname string) ([]logFileInfo, error) {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return nil, err
	}
	res := make([]logFileInfo, 0, len(files))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		logfn, err := parseLogFileInfo(file.Name())
		if err != nil {
			return nil, err
		}
		res = append(res, logfn)
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].round < res[j].round {
			return true
		}
		if bytes.Compare(res[i].endKey, res[j].endKey) < 0 {
			return true
		}
		return false
	})
	return res, nil
}

type KVFileLog struct {
	dirname        string
	currRound      int64
	currFile       *os.File
	currEntryCount int64
	height         int64
	buf            []byte
	lastLogKey     []byte
}

var _ KVLog = (*KVFileLog)(nil)

func (kvlog *KVFileLog) execFile(filename string, runFn func(op byte, key, value []byte)) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	kvlog.currEntryCount = 0
	for {
		h := meow.New32(0)
		kvlog.buf = kvlog.buf[:1]
		n, err := file.Read(kvlog.buf)
		h.Write(kvlog.buf)
		if n == 0 && err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		op := kvlog.buf[0]
		kvlog.buf = kvlog.buf[:8]
		_, err = file.Read(kvlog.buf)
		h.Write(kvlog.buf)
		if err != nil {
			return err
		}
		if op == HEIGHT {
			kvlog.height = int64(binary.LittleEndian.Uint64(kvlog.buf[:]))
			continue
		}
		kLen := binary.LittleEndian.Uint32(kvlog.buf[0:4])
		vLen := binary.LittleEndian.Uint32(kvlog.buf[4:8])
		if len(kvlog.buf) < int(kLen+vLen) {
			kvlog.buf = make([]byte, kLen+vLen)
		} else {
			kvlog.buf = kvlog.buf[:kLen+vLen]
		}
		_, err = file.Read(kvlog.buf)
		h.Write(kvlog.buf)
		if err != nil {
			return err
		}
		runFn(op, kvlog.buf[0:kLen], kvlog.buf[kLen:kLen+vLen])
		kvlog.currEntryCount++

		kvlog.buf = kvlog.buf[:4]
		_, err = file.Read(kvlog.buf)
		if !bytes.Equal(kvlog.buf, h.Sum(nil)) {
			panic("Checksum Error")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (kvlog *KVFileLog) Init(dirname string,  runFn func(op byte, key, value []byte)) error {
	fileInfos, err := getFileInfosInDir(dirname)
	if err != nil {
		return err
	}
	if len(fileInfos) == 0 {
		return fmt.Errorf("No files are found in %s", dirname)
	}
	kvlog.buf = make([]byte, 4)
	for _, fileInfo := range fileInfos {
		filename := dirname + "/" + fileInfo.name
		err = kvlog.execFile(filename, runFn)
		if err != nil {
			return err
		}
	}
	last := fileInfos[len(fileInfos)-1]
	kvlog.currRound = last.round
	filename := dirname + "/" + last.name
	kvlog.currFile, err = os.OpenFile(filename, os.O_APPEND, 0644)
	return err
}

func (kvlog *KVFileLog) IncrRound() {
	kvlog.currRound++
}

func (kvlog *KVFileLog) Sync() {
	kvlog.currFile.Sync()
	if kvlog.currEntryCount > MaxEntryCount {
		kvlog.openNewFile()
		kvlog.removeOldFile()
	}
}

func (kvlog *KVFileLog) openNewFile() {
	kvlog.currFile.Sync()
	kvlog.currFile.Close()
	hexStr := hex.EncodeToString(kvlog.lastLogKey)
	filename := fmt.Sprintf("%d-%s", kvlog.currRound, hexStr)
	var err error
	kvlog.currFile, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE, 0644)
	if err!=nil {
		panic(err)
	}
}

func (kvlog *KVFileLog) removeOldFile() {
	fileInfos, err := getFileInfosInDir(kvlog.dirname)
	if err != nil {
		panic(err)
	}
	for _, fileInfo := range fileInfos {
		if fileInfo.round > kvlog.currRound {
			panic("Invalid round number")
		}
		if fileInfo.round == kvlog.currRound {
			break
		}
		if fileInfo.round == kvlog.currRound - 1 && bytes.Compare(fileInfo.endKey, kvlog.lastLogKey) > 0 {
			break
		}
		os.Remove(kvlog.dirname + "/" + fileInfo.name)
	}
}

func (kvlog *KVFileLog) GetHeight() int64 {
	return kvlog.height
}

func (kvlog *KVFileLog) writeLog(op byte, key, value []byte) {
	kvlog.currEntryCount++
	h := meow.New32(0)
	kvlog.currFile.Write([]byte{op})
	h.Write([]byte{op})
	var kBuf [4]byte
	var vBuf [4]byte
	if len(key) > math.MaxUint32 || len(value) > math.MaxUint32 {
		panic("Length of key or value is too large")
	}
	binary.LittleEndian.PutUint32(kBuf[:], uint32(len(key)))
	binary.LittleEndian.PutUint32(vBuf[:], uint32(len(value)))
	for _, content := range [][]byte{kBuf[:], vBuf[:], key, value} {
		_, err := kvlog.currFile.Write(content)
		h.Write(content)
		if err != nil {
			panic(err)
		}
	}
	_, err := kvlog.currFile.Write(h.Sum(nil))
	if err != nil {
		panic(err)
	}
}

func (kvlog *KVFileLog) WriteHeight(height int64) {
	kvlog.height = height
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(height))
	kvlog.writeLog(HEIGHT, buf[:], []byte{})
}
func (kvlog *KVFileLog) Set(key, value []byte) {
	kvlog.writeLog(SET, key, value)
}
func (kvlog *KVFileLog) DupLog(key, value []byte) {
	kvlog.writeLog(DUPLOG, key, value)
	kvlog.lastLogKey = key
}
func (kvlog *KVFileLog) Delete(key []byte) {
	kvlog.writeLog(DEL, key, []byte{})
}


