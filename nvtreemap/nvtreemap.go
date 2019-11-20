package nvtreemap

import (
	"bytes"
	"os"
	"io"
	"io/ioutil"
	"fmt"
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

	MaxKeyLength = 8192
)

func getOpStr(op byte) string {
	switch op {
	case DUPLOG:
		return "DUPLOG"
	case SET:
		return "SET"
	case DEL:
		return "DEL"
	case HEIGHT:
		return "HEIGHT"
	default:
		return "Unknown"
	}
}

type Iterator interface {
	Domain() (start []byte, end []byte)
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
	// Iterator over a domain of keys in ascending order. End is exclusive.
	// Start must be less than end, or the Iterator is invalid.
	// Iterator must be closed by caller.
	// To iterate over entire domain, use store.Iterator(nil, nil)
	// Can NOT be used in in write phase
	Iterator(start, end []byte) Iterator
	// Iterator over a domain of keys in descending order. End is exclusive.
	// Start must be less than end, or the Iterator is invalid.
	// Iterator must be closed by caller.
	// Can NOT be used in in write phase
	ReverseIterator(start, end []byte) Iterator
	// Query the KV-pair, when it is NOT in write phase. Panics on nil key.
	// Get can be invoked from many goroutines concurrently
	Get(k []byte) uint64
	// Set sets the key. Panics on nil key.
	// Set and Delete can be invoked from only one goroutine
	Set(k []byte, v uint64)
	// Delete deletes the key. Panics on nil key.
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
func (iter *NVTreeLevelDBIter) Domain() (start []byte, end []byte) {
	start, end = iter.iter.Domain()
	return
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
	if tree.batch == nil {
		tree.batch = tree.db.NewBatch()
	}
	tree.isWriting = true
}

func (tree *NVTreeLevelDB) EndWrite(_ int64) {
	tree.batch.WriteSync()
	tree.batch.Close()
	tree.batch = tree.db.NewBatch()
	tree.isWriting = false
	tree.mtx.Unlock()
}

func (tree *NVTreeLevelDB) Iterator(start, end []byte) Iterator {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	return &NVTreeLevelDBIter{iter:tree.db.Iterator(start, end)}
}

func (tree *NVTreeLevelDB) ReverseIterator(start, end []byte) Iterator {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	return &NVTreeLevelDBIter{iter:tree.db.ReverseIterator(start, end)}
}

func (tree *NVTreeLevelDB) Get(k []byte) uint64 {
	if tree.isWriting {
		panic("tree.isWriting cannot be true! bug here...")
	}
	res := tree.db.Get(k)
	if len(res) == 0 {
		return 0
	}
	return binary.LittleEndian.Uint64(res)
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
	start      []byte
	end        []byte
	key        []byte
	value      uint64
	err        error
}
type BackwardIterMem struct {
	enumerator *b.Enumerator
	start      []byte
	end        []byte
	key        []byte
	value      uint64
	err        error
}
var _ Iterator = (*ForwardIterMem)(nil)
var _ Iterator = (*BackwardIterMem)(nil)

func (iter *ForwardIterMem) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}
func (iter *ForwardIterMem) Valid() bool {
	return iter.err == nil
}
func (iter *ForwardIterMem) Next() {
	if iter.err == nil {
		iter.key, iter.value, iter.err = iter.enumerator.Next()
		if bytes.Compare(iter.key, iter.end) >= 0 {
			iter.err = io.EOF
		}
	}
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

func (iter *BackwardIterMem) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}
func (iter *BackwardIterMem) Valid() bool {
	return iter.err == nil
}
func (iter *BackwardIterMem) Next() {
	if iter.err == nil {
		iter.key, iter.value, iter.err = iter.enumerator.Prev()
		if bytes.Compare(iter.key, iter.start) < 0 {
			iter.err = io.EOF
		}
	}
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

func NewNVTreeMem(entryCountLimit int) *NVTreeMem {
	btree := b.TreeNew(bytes.Compare)
	return &NVTreeMem {
		kvlog:            &KVFileLog{entryCountLimit: entryCountLimit},
		bt:               btree,
		prevEndKey:       []byte{},
	}
}

type LogEntry struct {
	op    byte
	key   []byte
	value uint64
}

func (tree *NVTreeMem) Init(dirname string, repFn func(string)) error {
	var prevEndKey []byte
	err := tree.kvlog.Init(dirname, repFn, func(e LogEntry) {
		switch e.op {
		case DUPLOG: //duplicate log for old items
			//fmt.Printf("Now Init DUPLOG Set K %v V %d\n", e.key, e.value)
			prevEndKey = e.key
			tree.bt.Set(e.key, e.value)
		case SET: // set new items
			//fmt.Printf("Now Init Set K %v V %d\n", e.key, e.value)
			tree.bt.Set(e.key, e.value)
		case DEL: // new deletions
			tree.bt.Delete(e.key)
		default:
			panic("Invalid Op")
		}
	})
	tree.prevEndKey = append([]byte{}, prevEndKey...) // make a copy for safe
	return err
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
	tree.kvlog.Sync()
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
	//fmt.Printf("Now start writeDupLog from: %v, numUpdate: %d\n", tree.prevEndKey, tree.numUpdate)
	enumerator, _ := tree.bt.Seek(tree.prevEndKey)
	key, value, err := enumerator.Next()
	// scan from prevEndKey to the end
	for err != io.EOF && tree.numUpdate != 0 {
		tree.kvlog.DupLog(key, value)
		key, value, err = enumerator.Next()
		tree.prevEndKey = key
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
		key, value, err = enumerator.Next()
		tree.prevEndKey = key
		tree.numUpdate--
	}
}

func (tree *NVTreeMem) Set(k []byte, v uint64) {
	if !tree.isWriting {
		panic("tree.isWriting must be true! bug here...")
	}
	tree.bt.Set(k, v)
	tree.kvlog.Set(k, v)
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
	tree.kvlog.Delete(k)
}

func (tree *NVTreeMem) Iterator(start, end []byte) Iterator {
	iter := &ForwardIterMem{start:start, end:end}
	if bytes.Compare(start, end) >= 0 {
		iter.err = io.EOF
		return iter
	}
	iter.enumerator, _ = tree.bt.Seek(start)
	iter.Next() //fill key, value, err
	return iter
}

func (tree *NVTreeMem) ReverseIterator(start, end []byte) Iterator {
	iter := &BackwardIterMem{start:start, end:end}
	if bytes.Compare(start, end) >= 0 {
		iter.err = io.EOF
		return iter
	}
	iter.enumerator, _ = tree.bt.Seek(end)
	//now iter.enumerator >= k, we want end is exclusive
	iter.enumerator.Prev()
	iter.Next() //fill key, value, err
	return iter
}

// ===============================================================

// The name of a log file has three parts: sid, round number and the previous log file's ending key
// lastEndPos is the latest HEIGHT entry's ending position of this log file,
// when lastEndPos==0, there is no HEIGHT entry in current log file.
type logFileInfo struct {
	name       string
	sid        int64
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
	threeParts := strings.Split(name, "-")
	if len(threeParts) != 3 {
		err = fmt.Errorf("%s does not match the pattern 'SerialID-RoundNumber-PrevKey'",name)
		return
	}
	logfn.name = name
	logfn.sid, err = strconv.ParseInt(threeParts[0], 10, 63)
	if err != nil {
		return
	}
	logfn.round, err = strconv.ParseInt(threeParts[1], 10, 63)
	if err != nil {
		return
	}
	logfn.prevEndKey, err = hex.DecodeString(threeParts[2])
	return
}

// Scan the file names in a directory, return a sorted slice of logFileInfo
func getFileInfosInDir(dirname string) ([]*logFileInfo, error) {
	files, err := ioutil.ReadDir(dirname)
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
		} else if res[i].round > res[j].round {
			return false
		} else {
			return bytes.Compare(res[i].prevEndKey, res[j].prevEndKey) < 0
		}
	})
	return res, nil
}

type KVFileLog struct {
	dirname         string
	currSID         int64
	currRound       int64
	currWrFile      *os.File
	currWrFileName  string
	// the count of the entries in currWrFile
	entryCount      int64
	// the limit for entryCount. When it is reached, we create a new file
	entryCountLimit int
	// the latest marked height
	height          int64
	// This varialbe will be used as the "previous end key" part when creating a new file
	prevEndKey      []byte
}

func scanFileForEntries(filename string, handler func(currPos int64, height int64, entries []LogEntry)) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	currPos := 0
	entries := make([]LogEntry, 0, 1000)
	for {
		h := meow.New32(0)
		// Read one byte of OP
		var buf4 [4]byte
		n, err := file.Read(buf4[:1])
		if n == 0 && err == io.EOF {
			break // No new log entry is available
		}
		if err != nil {
			return err
		}
		h.Write(buf4[:1])
		currPos += n
		op := buf4[0]
		// Read 4 bytes of key length
		n, err = file.Read(buf4[:])
		currPos += n
		h.Write(buf4[:])
		if err != nil {
			return err
		}
		kLen := binary.LittleEndian.Uint32(buf4[:])
		// Read key
		key := make([]byte, kLen) //allocate new slice
		n, err = file.Read(key)
		currPos += n
		h.Write(key)
		if err != nil {
			return err
		}
		// Read value
		var buf8 [8]byte
		n, err = file.Read(buf8[:])
		currPos += n
		h.Write(buf8[:])
		if err != nil {
			return err
		}
		// Read checksum
		n, err = file.Read(buf4[:])
		currPos += n
		if err != nil {
			return err
		}
		if !bytes.Equal(buf4[:], h.Sum(nil)) {
			panic("Checksum Error")
		}
		// Update!
		////fmt.Printf("Now Read %s K %d %v V %v Pos %d\n", getOpStr(op), kLen, key, buf8, currPos)
		// for DUPLOG, SET and DEL, just buffer the entries for later execution
		// for HEIGHT, we perform the real execution
		if op == HEIGHT {
			height := int64(binary.LittleEndian.Uint64(buf8[:]))
			handler(int64(currPos), height, entries)
		} else {
			v := binary.LittleEndian.Uint64(buf8[:])
			entries = append(entries, LogEntry{op:op, key:key, value:v})
		}
	}
	return nil
}

// Parse a file and feed the log entries to the execFn (execute function)
func (kvlog *KVFileLog) execFile(fileInfo *logFileInfo, execFn func(LogEntry)) error {
	filename := kvlog.dirname + "/" + fileInfo.name
	kvlog.entryCount = 0

	return scanFileForEntries(filename, func(currPos int64, height int64, entries []LogEntry) {
		kvlog.height = height
		kvlog.entryCount += int64(len(entries))
		fileInfo.lastEndPos = currPos
		for _, e := range entries {
			execFn(e)
		}
		entries = entries[:0]
	})
}

func (kvlog *KVFileLog) Init(dirname string, repFn func(string), execFn func(e LogEntry)) error {
	kvlog.dirname = dirname
	fileInfos, err := getFileInfosInDir(dirname)
	if err != nil {
		return err
	}
	// an empty dir, which is not initialized
	if len(fileInfos) == 0 {
		kvlog.currSID = -1
		kvlog.openNewFile()
		kvlog.WriteHeight(-1)
		repFn("<init>")
		return nil
	}
	// execute the log entries in the files under dirname
	for _, fileInfo := range fileInfos {
		repFn(fileInfo.name)
		err = kvlog.execFile(fileInfo, execFn)
		if err != nil {
			return err
		}
		if !fileInfo.isUseful() {
			panic("A useless file without HEIGHT was found! Bug here...")
		}
	}

	// Re-open the last useful file for write, after truncating its useless tail (if any).
	lastUsefulFile := fileInfos[len(fileInfos)-1]
	kvlog.currSID = lastUsefulFile.sid
	kvlog.currRound = lastUsefulFile.round
	filename := dirname + "/" + lastUsefulFile.name
	kvlog.currWrFileName = filename
	//fmt.Printf("Now Open %s, Truncate %d\n", kvlog.currWrFileName, lastUsefulFile.lastEndPos)
	kvlog.currWrFile, err = os.OpenFile(filename, os.O_WRONLY, 0700)
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
		kvlog.pruneOldFiles()
		kvlog.openNewFile()
		kvlog.entryCount = 0
	}
}

// Close the old currWrFile and open a new currWrFile
func (kvlog *KVFileLog) openNewFile() {
	if kvlog.currWrFile != nil {
		kvlog.currWrFile.Close()
	}
	hexStr := hex.EncodeToString(kvlog.prevEndKey)
	kvlog.currSID++
	filename := fmt.Sprintf("%d-%d-%s", kvlog.currSID, kvlog.currRound, hexStr)
	var err error
	kvlog.currWrFileName = kvlog.dirname + "/" + filename
	//fmt.Printf("Now Open %s\n", kvlog.currWrFileName)
	kvlog.currWrFile, err = os.OpenFile(kvlog.currWrFileName, os.O_CREATE|os.O_WRONLY, 0644)
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
	latest := fileInfos[len(fileInfos)-1]
	toBeDel := ""
	for _, fileInfo := range fileInfos[:len(fileInfos)-1] {
		// initial case
		if fileInfo.round == 0 && latest.round == 1 {
			break
		}
		// last round, and prevEndKey are not overlapped totally
		if fileInfo.round == latest.round - 1 && bytes.Compare(fileInfo.prevEndKey, latest.prevEndKey) > 0 {
			break
		}
		// current round
		if fileInfo.round == latest.round {
			break
		}
		if len(toBeDel) != 0 {
			//fmt.Printf("Now Remove %s\n", toBeDel)
			os.Remove(toBeDel)
		}
		//fmt.Printf("Now To Remove %s latestRound %d latest.prevEndKey: %v\n", dirname + "/" + fileInfo.name, latest.round, latest.prevEndKey)
		toBeDel = kvlog.dirname + "/" + fileInfo.name
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
	if len(key) > MaxKeyLength {
		panic("Length of key is too large")
	}
	binary.LittleEndian.PutUint32(kBuf[:], uint32(len(key)))
	binary.LittleEndian.PutUint64(vBuf[:], value)
	//fmt.Printf("Now Write OP %s K %v %v V %v @%s\n", getOpStr(op), kBuf[:], key, vBuf[:], kvlog.currWrFileName)
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

