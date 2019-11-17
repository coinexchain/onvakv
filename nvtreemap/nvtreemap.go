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
	dbm "github.com/tendermint/tm-db"

	"github.com/coinexchain/onvakv/nvtreemap/b"
)

// Operations to KVFileLog
const (
	DUPLOG byte = 0
	SET byte = 1
	DEL byte = 2
	HEIGHT byte = 3
)

type Iterator interface {
    Valid() bool
    Next()
    Key() []byte
    Value() uint64
    Close()
}

// Non-volatile tree
type NVTree interface {
	// initialize the internal data structure
	Init(dirname string, repFn func(string)) error
	// begin the write phase, during which no reading is permitted
	BeginWrite()
	// end the write phase, and mark the corresponding height
	EndWrite(height int64)
	// get forward/backward iterators, when it is NOT in write phase
	Iterator(start []byte) Iterator
	ReverseIterator(start []byte) Iterator
	// query the KV-pair, when it is NOT in write phase
	// Get can be invoked from many goroutines concurrently
	Get(k []byte) uint64
	// update the KV store, when it is in write phase
	// Set and Delete can be invoked from only one goroutine
	Set(k []byte, v uint64)
	Delete(k []byte)
}

// ============================
// Here we implement NVTree with dbm.GoLevelDB

type NVTreeLevelDB struct {
	db        *dbm.GoLevelDB
	batch     dbm.Batch
	mtx       sync.Mutex
	isWriting bool
}

type NVTreeLevelDBIter struct {
	iter      dbm.Iterator
}
func (iter *NVTreeLevelDBIter) Valid() bool {
	return iter.iter.Valid()
}
func (iter *NVTreeLevelDBIter) Next() {
	iter.iter.Next()
}
func (iter *NVTreeLevelDBIter) Key() []byte {
	return iter.iter.Key()
}
func (iter *NVTreeLevelDBIter) Value() uint64 {
	return binary.LittleEndian.Uint64(iter.iter.Value())
}
func (iter *NVTreeLevelDBIter) Close() {
	iter.iter.Close()
}

var _ NVTree = (*NVTreeLevelDB)(nil)

func (tree *NVTreeLevelDB) Init(dirname string, _ func(string)) (err error) {
	tree.db, err = dbm.NewGoLevelDB("tree", dirname)
	if err != nil {
		tree.batch = tree.db.NewBatch()
	}
	return err
}

func (tree *NVTreeLevelDB) BeginWrite() {
	tree.mtx.Lock()
	tree.isWriting = true
}

func (tree *NVTreeLevelDB) EndWrite(_ int64) {
	tree.batch.WriteSync()
	tree.batch.Close()
	tree.batch = tree.db.NewBatch()
	tree.isWriting = false
	tree.mtx.Unlock()
}

func (tree *NVTreeLevelDB) Iterator(start []byte) Iterator {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	// nil is the largest key
	return &NVTreeLevelDBIter{iter:tree.db.Iterator(start, nil)}
}

func (tree *NVTreeLevelDB) ReverseIterator(start []byte) Iterator {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	// []byte{} is the largest key
	return &NVTreeLevelDBIter{iter:tree.db.ReverseIterator([]byte{}, start)}
}

func (tree *NVTreeLevelDB) Get(k []byte) uint64 {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	return binary.LittleEndian.Uint64(tree.db.Get(k))
}

func (tree *NVTreeLevelDB) Set(k []byte, v uint64) {
	if !tree.isWriting {
		panic("tree.isWriting must be true! bug here...")
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	tree.batch.Set(k, buf[:])
}

func (tree *NVTreeLevelDB) Delete(k []byte) {
	if !tree.isWriting {
		panic("tree.isWriting must be true! bug here...")
	}
	tree.batch.Delete(k)
}


// ============================
// Here we implement NVTree with an in-memory B-Tree and a file log

type ForwardIterMem struct {
	enumerator *b.Enumerator
	key        []byte
	value      uint64
	err        error
}
type BackwardIterMem struct {
	enumerator *b.Enumerator
	key        []byte
	value      uint64
	err        error
}
var _ Iterator = (*ForwardIterMem)(nil)
var _ Iterator = (*BackwardIterMem)(nil)

func (iter *ForwardIterMem) Valid() bool {
	return iter.err == nil
}
func (iter *ForwardIterMem) Next() {
	iter.key, iter.value, iter.err = iter.enumerator.Next()
}
func (iter *ForwardIterMem) Key() []byte {
	return iter.key
}
func (iter *ForwardIterMem) Value() uint64 {
	return iter.value
}
func (iter *ForwardIterMem) Close() {
	iter.enumerator.Close()
}

func (iter *BackwardIterMem) Valid() bool {
	return iter.err == nil
}
func (iter *BackwardIterMem) Next() {
	iter.key, iter.value, iter.err = iter.enumerator.Prev()
}
func (iter *BackwardIterMem) Key() []byte {
	return iter.key
}
func (iter *BackwardIterMem) Value() uint64 {
	return iter.value
}
func (iter *BackwardIterMem) Close() {
	iter.enumerator.Close()
}

//------------

type NVTreeMem struct {
	mtx        sync.RWMutex
	bt         *b.Tree
	isWriting  bool
	kvlog      *KVFileLog
	prevEndKey []byte // The last key of DUPLOG which was written out
	numUpdate  int64
}
var _ NVTree = (*NVTreeMem)(nil)

func NewNVTreeMem(entryCountLimit int) NVTree {
	btree := b.TreeNew(bytes.Compare)
	return &NVTreeMem {
		kvlog: &KVFileLog{entryCountLimit: entryCountLimit},
		bt:    btree,
	}
}

type LogEntry struct {
	op    byte
	key   []byte
	value []byte
}

func (tree *NVTreeMem) Init(dirname string, repFn func(string)) error {
	return tree.kvlog.Init(dirname, repFn, func(e LogEntry) {
		switch e.op {
		case DUPLOG: //duplicate log for old items
			tree.prevEndKey = e.key
			tree.bt.Set(e.key, binary.LittleEndian.Uint64(e.value))
		case SET: // set new items
			tree.bt.Set(e.key, binary.LittleEndian.Uint64(e.value))
		case DEL: // new deletions
			tree.bt.Delete(e.key)
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
	if !tree.isWriting {
		panic("tree.isWriting cannot be false! bug here...")
	}
	tree.isWriting = false
	tree.kvlog.WriteHeight(height)
	tree.mtx.Unlock()
	tree.mtx.RLock()
	// writeDupLog may overlap with queries from outside
	// But writeDupLog will NOT overlap with next BeginWrite
	go tree.writeDupLog()
}

func (tree *NVTreeMem) GetHeight() int64 {
	return tree.kvlog.GetHeight()
}

// With dup-log, we can ensure every KV pair is logged withing a round
func (tree *NVTreeMem) writeDupLog() {
	defer tree.mtx.RUnlock()
	defer tree.kvlog.Sync()
	enumerator, _ := tree.bt.Seek(tree.prevEndKey)
	key, value, err := enumerator.Next()
	// scan from prevEndKey to the end
	for err != io.EOF && tree.numUpdate != 0 {
		tree.kvlog.DupLog(key, value)
		tree.prevEndKey = key
		key, value, err = enumerator.Next()
		tree.numUpdate--
	}
	if tree.numUpdate == 0 {
		return
	}
	// Wrap to the first KV pair and start a new round
	tree.kvlog.IncrRound()
	enumerator, _ = tree.bt.SeekFirst()
	key, value, err = enumerator.Next()
	for err != io.EOF && tree.numUpdate != 0 {
		tree.kvlog.DupLog(key, value)
		tree.prevEndKey = key
		key, value, err = enumerator.Next()
		tree.numUpdate--
	}
}

func (tree *NVTreeMem) Set(k []byte, v uint64) {
	if !tree.isWriting {
		panic("tree.isWriting must be true! bug here...")
	}
	tree.bt.Set(k, v)
	tree.numUpdate++
}

func (tree *NVTreeMem) Get(k []byte) uint64 {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	v, ok := tree.bt.Get(k)
	if !ok {
		return 0
	}
	return v
}

func (tree *NVTreeMem) Delete(k []byte) {
	if !tree.isWriting {
		panic("tree.isWriting must be true! bug here...")
	}
	tree.bt.Delete(k)
}

func (tree *NVTreeMem) Iterator(start []byte) Iterator {
	iter := &ForwardIterMem{}
	iter.enumerator, _ = tree.bt.Seek(start)
	iter.Next()
	return iter
}

func (tree *NVTreeMem) ReverseIterator(start []byte) Iterator {
	var ok bool
	iter := &BackwardIterMem{}
	iter.enumerator, ok = tree.bt.Seek(start)
	if !ok { //now iter.enumerator > k
		iter.enumerator.Prev()
	}
	iter.Next()
	return iter
}

// ===============================================================

// The name of a log file has two parts: round number and the previous log file's ending key
// lastEndPos is the latest HEIGHT entry's ending position of this log file,
// when lastEndPos==0, there is no HEIGHT entry in current log file.
type logFileInfo struct {
	name       string
	round      int64
	prevEndKey []byte
	lastEndPos int64
}

// The content after the latest HEIGHT entry is useless.
// So, when a log file is useful, it must have a HEIGHT entry
func (lfi *logFileInfo) isUseful() bool {
	return lfi.lastEndPos > 0
}

// Given the name of a file, returns the parsed logFileInfo
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
	logfn.prevEndKey, err = hex.DecodeString(twoParts[1])
	return
}

// Scan the file names in a directory, return a sorted slice of logFileInfo
func getFileInfosInDir(dirname string) ([]*logFileInfo, error) {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		return nil, err
	}
	res := make([]*logFileInfo, 0, len(files))
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		logfn, err := parseLogFileInfo(file.Name())
		if err != nil {
			return nil, err
		}
		res = append(res, &logfn)
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].round < res[j].round {
			return true
		}
		if bytes.Compare(res[i].prevEndKey, res[j].prevEndKey) < 0 {
			return true
		}
		return false
	})
	return res, nil
}

type KVFileLog struct {
	dirname         string
	currRound       int64
	currWrFile      *os.File
	// the count of the entries in currWrFile
	entryCount      int64
	// the limit for entryCount. When it is reached, we create a new file
	entryCountLimit int
	// the latest marked height
	height          int64
	// A temporery buffer used in execFile
	buf             []byte
	// This varialbe will be used as the "previous end key" part when creating a new file
	prevEndKey      []byte
}

// Parse a file and feed the log entries to the execFn (execute function)
func (kvlog *KVFileLog) execFile(dirname string, fileInfo *logFileInfo, execFn func(LogEntry)) error {
	filename := dirname + "/" + fileInfo.name
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	kvlog.entryCount = 0
	currPos := 0
	entries := make([]LogEntry, 0, 1000)
	for {
		h := meow.New32(0)
		// Read one byte of OP
		buf := kvlog.buf[:1]
		n, err := file.Read(buf)
		if n == 0 && err == io.EOF {
			break // No new log entry is available
		}
		if err != nil {
			return err
		}
		h.Write(buf)
		currPos += n
		op := buf[0]
		// Read 4 bytes of key length
		buf = kvlog.buf[:4]
		n, err = file.Read(buf)
		currPos += n
		h.Write(buf)
		if err != nil {
			return err
		}
		kLen := binary.LittleEndian.Uint32(buf)
		// Read key and value
		if len(kvlog.buf) < int(kLen)+8 {
			kvlog.buf = make([]byte, kLen+8)
			buf = kvlog.buf
		} else {
			buf = kvlog.buf[:kLen+8]
		}
		n, err = file.Read(buf)
		currPos += n
		h.Write(buf)
		if err != nil {
			return err
		}
		// for DUPLOG, SET and DEL, just buffer the entries for later execution
		// for HEIGHT, we perform the real execution
		if op == HEIGHT {
			kvlog.height = int64(binary.LittleEndian.Uint64(buf[kLen:kLen+8]))
			kvlog.entryCount += int64(len(entries))
			fileInfo.lastEndPos = int64(currPos)
			for _, e := range entries {
				execFn(e)
			}
			entries = entries[:0]
		} else {
			entries = append(entries, LogEntry{op:op, key:buf[0:kLen], value:buf[kLen:kLen+8]})
		}

		// Read checksum
		buf = kvlog.buf[:4]
		n, err = file.Read(buf)
		currPos += n
		if err != nil {
			return err
		}
		if !bytes.Equal(buf, h.Sum(nil)) {
			panic("Checksum Error")
		}
	}
	return nil
}

func (kvlog *KVFileLog) Init(dirname string, repFn func(string), execFn func(e LogEntry)) error {
	fileInfos, err := getFileInfosInDir(dirname)
	if err != nil {
		return err
	}
	if len(fileInfos) == 0 {
		return fmt.Errorf("No files are found in %s", dirname)
	}
	kvlog.buf = make([]byte, 4096)
	// execute the log entries in the files under dirname
	for _, fileInfo := range fileInfos {
		repFn(fileInfo.name)
		err = kvlog.execFile(dirname, fileInfo, execFn)
		if err != nil {
			return err
		}
	}
	// scan the files in reverse order and delete the useless files
	lastUsefulFileIdx := -1
	for i:=len(fileInfos)-1; i>=0; i-- {
		fileInfo := fileInfos[i]
		if fileInfo.isUseful() {
			lastUsefulFileIdx = i
			break
		}
		err = os.Remove(dirname + "/" + fileInfo.name)
		if err != nil {
			return err
		}
	}
	if lastUsefulFileIdx == -1 {
		panic("Bug Here!")
	}

	// Re-open the last useful file for write, after truncating its useless tail (if any).
	lastUsefulFile := fileInfos[lastUsefulFileIdx]
	kvlog.currRound = lastUsefulFile.round
	filename := dirname + "/" + lastUsefulFile.name
	kvlog.currWrFile, err = os.OpenFile(filename, os.O_WRONLY, 0644)
	kvlog.currWrFile.Truncate(lastUsefulFile.lastEndPos)
	kvlog.currWrFile.Seek(lastUsefulFile.lastEndPos, 0)
	return err
}

func (kvlog *KVFileLog) IncrRound() {
	kvlog.currRound++
}

// Sync log entries to disk
func (kvlog *KVFileLog) Sync() {
	kvlog.currWrFile.Sync()
	if kvlog.entryCount > int64(kvlog.entryCountLimit) {
		kvlog.openNewFile()
		kvlog.pruneOldFiles()
	}
}

// Close the old currWrFile and open a new currWrFile
func (kvlog *KVFileLog) openNewFile() {
	kvlog.currWrFile.Close()
	hexStr := hex.EncodeToString(kvlog.prevEndKey)
	filename := fmt.Sprintf("%d-%s", kvlog.currRound, hexStr)
	var err error
	kvlog.currWrFile, err = os.OpenFile(filename, os.O_CREATE, 0644)
	if err!=nil {
		panic(err)
	}
}

// Prune the old files which are no longer needed to rebuild the treemap
func (kvlog *KVFileLog) pruneOldFiles() {
	fileInfos, err := getFileInfosInDir(kvlog.dirname)
	if err != nil {
		panic(err)
	}
	for _, fileInfo := range fileInfos {
		if fileInfo.round > kvlog.currRound {
			panic("Invalid round number")
		}
		// last round, but prevEndKey is large enough
		if fileInfo.round == kvlog.currRound - 1 && bytes.Compare(fileInfo.prevEndKey, kvlog.prevEndKey) > 0 {
			break
		}
		// current round
		if fileInfo.round == kvlog.currRound {
			break
		}
		os.Remove(kvlog.dirname + "/" + fileInfo.name)
	}
}

func (kvlog *KVFileLog) GetHeight() int64 {
	return kvlog.height
}

// For different kinds of log entries, we use the same format: one-byte-op 4-byte-key-length key 8-byte-value 4-byte-checksum
func (kvlog *KVFileLog) writeLog(op byte, key []byte, value uint64) {
	kvlog.entryCount++
	h := meow.New32(0)
	kvlog.currWrFile.Write([]byte{op})
	h.Write([]byte{op})
	var kBuf [4]byte
	var vBuf [8]byte
	if len(key) > math.MaxUint32 {
		panic("Length of key is too large")
	}
	binary.LittleEndian.PutUint32(kBuf[:], uint32(len(key)))
	binary.LittleEndian.PutUint64(vBuf[:], value)
	for _, content := range [][]byte{kBuf[:], key, vBuf[:]} {
		if len(content) == 0 {
			continue
		}
		_, err := kvlog.currWrFile.Write(content)
		h.Write(content)
		if err != nil {
			panic(err)
		}
	}
	_, err := kvlog.currWrFile.Write(h.Sum(nil))
	if err != nil {
		panic(err)
	}
}

func (kvlog *KVFileLog) WriteHeight(height int64) {
	kvlog.height = height
	kvlog.writeLog(HEIGHT, []byte{}, uint64(height))
}
func (kvlog *KVFileLog) Set(key []byte, value uint64) {
	kvlog.writeLog(SET, key, value)
}
func (kvlog *KVFileLog) DupLog(key []byte, value uint64) {
	kvlog.writeLog(DUPLOG, key, value)
	kvlog.prevEndKey = key
}
func (kvlog *KVFileLog) Delete(key []byte) {
	kvlog.writeLog(DEL, key, 0)
}

