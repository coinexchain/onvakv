package indextree

import (
	"fmt"
	"os"
	"testing"
	"strings"

	"github.com/stretchr/testify/assert"
)

var Content string

func showFileContent(currPos int64, height int64, entries []LogEntry) {
	var str strings.Builder
	str.WriteString("\n")
	for _, e := range entries {
		str.WriteString(fmt.Sprintf("%s %v => %v\n", getOpStr(e.op), e.key, e.value))
	}
	str.WriteString(fmt.Sprintf("======= height: %d currPos: %d ====\n", height, currPos))
	Content = str.String()
}

func commonCheck(t *testing.T, tree *NVTreeMem) {
	assert.Equal(t, uint64(0),  tree.Get([]byte{128, 0}))
	assert.Equal(t, uint64(2),  tree.Get([]byte{128, 2}))
	assert.Equal(t, uint64(4),  tree.Get([]byte{128, 4}))
	assert.Equal(t, uint64(6),  tree.Get([]byte{128, 6}))
	assert.Equal(t, uint64(8),  tree.Get([]byte{128, 8}))
	assert.Equal(t, uint64(10), tree.Get([]byte{128, 10}))

	assert.Equal(t, uint64(100),  tree.Get([]byte{127,  200}))
	assert.Equal(t, uint64(102),  tree.Get([]byte{127,  202}))
	assert.Equal(t, uint64(100),  tree.Get([]byte{127,  204}))
	assert.Equal(t, uint64(106),  tree.Get([]byte{127,  206}))
	assert.Equal(t,   uint64(0),  tree.Get([]byte{127,  208}))
	assert.Equal(t, uint64(212),  tree.Get([]byte{127,  212}))

	assert.Equal(t, uint64(202),  tree.Get([]byte{126,  202}))
	assert.Equal(t, uint64(200),  tree.Get([]byte{126,  204}))
	assert.Equal(t, uint64(206),  tree.Get([]byte{126,  206}))
	assert.Equal(t, uint64(208),  tree.Get([]byte{126,  208}))
	assert.Equal(t, uint64(210),  tree.Get([]byte{126,  210}))
}

func TestMemTree(t *testing.T) {
	dirname := "./tmp4test"
	err := os.Mkdir(dirname, 0700)
	if err != nil {
		panic(err)
	}
	entryCountLimit := 4
	tree := NewNVTreeMem(entryCountLimit)
	err = tree.Init(dirname, func(fname string) {
		fmt.Printf("Now load log file %s.\n", fname)
	})
	if err != nil {
		panic(err)
	}

	tree.BeginWrite()
	tree.Set([]byte{128, 0}, 0)
	tree.Set([]byte{128, 2}, 2)
	tree.Set([]byte{128, 4}, 0)
	tree.Set([]byte{128, 6}, 6)
	tree.Delete([]byte{128, 4})
	tree.Set([]byte{128, 4}, 4)
	tree.Set([]byte{128, 8}, 7)
	tree.Set([]byte{128, 10}, 10)
	tree.EndWrite(0)
	tree.BeginWrite()
	correctContent := `
SET [128 0] => 0
SET [128 2] => 2
SET [128 4] => 0
SET [128 6] => 6
DEL [128 4] => 0
SET [128 4] => 4
SET [128 8] => 7
SET [128 10] => 10
======= height: 0 currPos: 186 ====
`
	for _, filename := range []string{"0-0-"} {
		fmt.Printf("____________[ %s ]____________\n", filename)
		scanFileForEntries("./tmp4test/"+filename, showFileContent)
		fmt.Print(Content)
		assert.Equal(t, correctContent, Content)
		fmt.Printf("^^^^^^^^^^^^[ %s ]^^^^^^^^^^^^\n", filename)
	}
	tree.Set([]byte{128, 8}, 8)
	tree.Set([]byte{127, 200}, 200)
	tree.Set([]byte{127, 202}, 202)
	tree.Set([]byte{127, 204}, 200)
	tree.Set([]byte{127, 206}, 206)
	tree.Set([]byte{127, 208}, 208)
	tree.Set([]byte{127, 210}, 210)
	tree.EndWrite(1)
	tree.BeginWrite()
	correctContent = `
DUPLOG [128 0] => 0
DUPLOG [128 2] => 2
DUPLOG [128 4] => 4
DUPLOG [128 6] => 6
DUPLOG [128 8] => 7
DUPLOG [128 10] => 10
DUPLOG [128 0] => 0
SET [128 8] => 8
SET [127 200] => 200
SET [127 202] => 202
SET [127 204] => 200
SET [127 206] => 206
SET [127 208] => 208
SET [127 210] => 210
======= height: 1 currPos: 283 ====
`
	for _, filename := range []string{"1-0-"} {
		fmt.Printf("____________[ %s ]____________\n", filename)
		scanFileForEntries("./tmp4test/"+filename, showFileContent)
		fmt.Print(Content)
		assert.Equal(t, correctContent, Content)
		fmt.Printf("^^^^^^^^^^^^[ %s ]^^^^^^^^^^^^\n", filename)
	}
	tree.Set([]byte{127, 212}, 212)
	tree.EndWrite(2)
	tree.BeginWrite()
	correctContent = `
DUPLOG [128 2] => 2
DUPLOG [128 4] => 4
DUPLOG [128 6] => 6
DUPLOG [128 8] => 8
DUPLOG [128 10] => 10
DUPLOG [127 200] => 200
DUPLOG [127 202] => 202
SET [127 212] => 212
======= height: 2 currPos: 169 ====
`
	for _, filename := range []string{"2-1-8000"} {
		fmt.Printf("____________[ %s ]____________\n", filename)
		scanFileForEntries("./tmp4test/"+filename, showFileContent)
		fmt.Print(Content)
		assert.Equal(t, correctContent, Content)
		fmt.Printf("^^^^^^^^^^^^[ %s ]^^^^^^^^^^^^\n", filename)
	}
	tree.Set([]byte{127, 200}, 200)
	tree.Set([]byte{126, 202}, 202)
	tree.Set([]byte{126, 204}, 200)
	tree.Set([]byte{126, 206}, 206)
	tree.Set([]byte{126, 208}, 208)
	tree.Set([]byte{126, 210}, 0)
	tree.EndWrite(3)
	tree.BeginWrite()
	correctContent = `
DUPLOG [127 204] => 200
SET [127 200] => 200
SET [126 202] => 202
SET [126 204] => 200
SET [126 206] => 206
SET [126 208] => 208
SET [126 210] => 0
======= height: 3 currPos: 150 ====
`
	for _, filename := range []string{"3-2-7fca"} {
		fmt.Printf("____________[ %s ]____________\n", filename)
		scanFileForEntries("./tmp4test/"+filename, showFileContent)
		fmt.Print(Content)
		assert.Equal(t, correctContent, Content)
		fmt.Printf("^^^^^^^^^^^^[ %s ]^^^^^^^^^^^^\n", filename)
	}
	tree.Set([]byte{126, 210}, 210)
	tree.Set([]byte{127, 200}, 100)
	tree.Set([]byte{127, 202}, 102)
	tree.Set([]byte{127, 204}, 100)
	tree.Set([]byte{127, 206}, 106)
	tree.Set([]byte{127, 208}, 108)
	tree.Set([]byte{127, 210}, 110)
	tree.EndWrite(4)
	tree.BeginWrite()
	correctContent = `
DUPLOG [127 206] => 206
DUPLOG [127 208] => 208
DUPLOG [127 210] => 210
DUPLOG [127 212] => 212
DUPLOG [128 0] => 0
DUPLOG [128 2] => 2
SET [126 210] => 210
SET [127 200] => 100
SET [127 202] => 102
SET [127 204] => 100
SET [127 206] => 106
SET [127 208] => 108
SET [127 210] => 110
======= height: 4 currPos: 264 ====
`
	for _, filename := range []string{"4-2-7fcc"} {
		fmt.Printf("____________[ %s ]____________\n", filename)
		scanFileForEntries("./tmp4test/"+filename, showFileContent)
		fmt.Print(Content)
		assert.Equal(t, correctContent, Content)
		fmt.Printf("^^^^^^^^^^^^[ %s ]^^^^^^^^^^^^\n", filename)
	}
	tree.Set([]byte{127, 210}, 210)
	tree.EndWrite(5)
	tree.BeginWrite()
	correctContent = `
DUPLOG [128 4] => 4
DUPLOG [128 6] => 6
DUPLOG [128 8] => 8
DUPLOG [128 10] => 10
DUPLOG [126 202] => 202
DUPLOG [126 204] => 200
DUPLOG [126 206] => 206
SET [127 210] => 210
======= height: 5 currPos: 169 ====
`
	for _, filename := range []string{"5-2-8002"} {
		fmt.Printf("____________[ %s ]____________\n", filename)
		scanFileForEntries("./tmp4test/"+filename, showFileContent)
		fmt.Print(Content)
		assert.Equal(t, correctContent, Content)
		fmt.Printf("^^^^^^^^^^^^[ %s ]^^^^^^^^^^^^\n", filename)
	}
	tree.Delete([]byte{127, 208})
	tree.EndWrite(6)
	tree.BeginWrite()
	tree.Set([]byte{129, 204}, 100)
	tree.Set([]byte{129, 206}, 106)
	tree.Set([]byte{129, 208}, 108)
	tree.Delete([]byte{127, 210})
	tree.kvlog.currWrFile.Sync() //simulate program crash without writing HEIGHT
	tree.kvlog.currWrFile.Close()
	fmt.Printf("currWrFileName: %s\n", tree.kvlog.currWrFileName)
	correctContent = `
DUPLOG [126 208] => 208
DEL [127 208] => 0
======= height: 6 currPos: 55 ====
`
	tree.isWriting = false

	commonCheck(t, tree)
	assert.Equal(t, uint64(0),  tree.Get([]byte{127,  210}))
	assert.Equal(t, uint64(100),  tree.Get([]byte{129,  204}))
	assert.Equal(t, uint64(106),  tree.Get([]byte{129,  206}))
	assert.Equal(t, uint64(108),  tree.Get([]byte{129,  208}))

	//assert.Equal(t, uint64(210),  tree.Get([]byte{127,  210}))

	for _, filename := range []string{"6-3-7ece"} {
		fmt.Printf("____________[ %s ]____________\n", filename)
		scanFileForEntries("./tmp4test/"+filename, showFileContent)
		fmt.Print(Content)
		assert.Equal(t, correctContent, Content)
		fmt.Printf("^^^^^^^^^^^^[ %s ]^^^^^^^^^^^^\n", filename)
	}

	fmt.Printf("-------------------------------------------------\n")
	fmt.Printf("-------------------------------------------------\n")
	fmt.Printf("-------------------------------------------------\n")
	tree = NewNVTreeMem(entryCountLimit)
	err = tree.Init(dirname, func(fname string) {
		fmt.Printf("Now load log file %s.\n", fname)
	})

	commonCheck(t, tree)
	assert.Equal(t, uint64(210),  tree.Get([]byte{127,  210}))
	assert.Equal(t, uint64(0),  tree.Get([]byte{129,  204}))
	assert.Equal(t, uint64(0),  tree.Get([]byte{129,  206}))
	assert.Equal(t, uint64(0),  tree.Get([]byte{129,  208}))

	testIter(t, tree)

	err = os.RemoveAll(dirname)
	if err != nil {
		panic(err)
	}
}

func testIter(t *testing.T, tree NVTree) {
	var str strings.Builder
	iter := tree.Iterator([]byte{127, 206}, []byte{128,4})
	for iter.Valid() {
		str.WriteString(fmt.Sprintf("%v %d\n", iter.Key(), iter.Value()))
		iter.Next()
	}
correctContent := `[127 206] 106
[127 210] 210
[127 212] 212
[128 0] 0
[128 2] 2
`
	assert.Equal(t, correctContent, str.String())

	str.Reset()
	revIter := tree.ReverseIterator([]byte{127, 206}, []byte{128,4})
	for revIter.Valid() {
		str.WriteString(fmt.Sprintf("%v %d\n", revIter.Key(), revIter.Value()))
		revIter.Next()
	}
correctContent = `[128 2] 2
[128 0] 0
[127 212] 212
[127 210] 210
[127 206] 106
`
	assert.Equal(t, correctContent, str.String())

	iter = tree.Iterator([]byte{128,4}, []byte{127, 206})
	assert.Equal(t, false, iter.Valid())
	revIter = tree.ReverseIterator([]byte{128,4}, []byte{127, 206})
	assert.Equal(t, false, revIter.Valid())
}

func TestLevelDBTree(t *testing.T) {
	dirname := "./tmp4testLVL"
	tree := &NVTreeLevelDB{}
	err := tree.Init(dirname, nil)
	if err != nil {
		panic(err)
	}

	tree.BeginWrite()
	tree.Set([]byte{128, 0}, 0)
	tree.Set([]byte{128, 2}, 2)
	tree.Set([]byte{128, 4}, 0)
	tree.Set([]byte{128, 6}, 6)
	tree.Delete([]byte{128, 4})
	tree.Set([]byte{128, 4}, 4)
	tree.Set([]byte{128, 8}, 7)
	tree.Set([]byte{128, 10}, 10)
	tree.EndWrite(0)
	tree.BeginWrite()
	tree.Set([]byte{128, 8}, 8)
	tree.Set([]byte{127, 200}, 200)
	tree.Set([]byte{127, 202}, 202)
	tree.Set([]byte{127, 204}, 200)
	tree.Set([]byte{127, 206}, 106)
	tree.Set([]byte{127, 208}, 208)
	tree.Set([]byte{127, 210}, 210)
	tree.Set([]byte{127, 212}, 212)
	tree.Delete([]byte{127, 208})
	tree.EndWrite(1)

	assert.Equal(t, uint64(0),  tree.Get([]byte{128, 0}))
	assert.Equal(t, uint64(2),  tree.Get([]byte{128, 2}))
	assert.Equal(t, uint64(4),  tree.Get([]byte{128, 4}))
	assert.Equal(t, uint64(6),  tree.Get([]byte{128, 6}))
	assert.Equal(t, uint64(8),  tree.Get([]byte{128, 8}))
	assert.Equal(t, uint64(10), tree.Get([]byte{128, 10}))

	assert.Equal(t, uint64(200),  tree.Get([]byte{127,  200}))
	assert.Equal(t, uint64(202),  tree.Get([]byte{127,  202}))
	assert.Equal(t, uint64(200),  tree.Get([]byte{127,  204}))
	assert.Equal(t, uint64(106),  tree.Get([]byte{127,  206}))
	assert.Equal(t, uint64(0),  tree.Get([]byte{127,  208}))

	testIter(t, tree)

	err = os.RemoveAll(dirname)
	if err != nil {
		panic(err)
	}
}

//func TestBtree(t *testing.T) {
//	btree := b.TreeNew(bytes.Compare)
//	x := []byte{127,0}
//	btree.Set(x, 8)
//	y := []byte{127,0}
//	btree.Set(y, 6)
//	y[1] = 1
//	v, ok := btree.Get(x) // can find
//	assert.Equal(t, uint64(6), v)
//	assert.Equal(t, true, ok)
//	_, ok = btree.Get(y) // can not find
//	assert.Equal(t, false, ok)
//	x[1] = 1
//	_, ok = btree.Get([]byte{127,0}) // can not find! since btree is internal key is changed
//	assert.Equal(t, false, ok)
//
//	btree = b.TreeNew(bytes.Compare)
//	x = []byte{127,0}
//	btree.SafeSet(x, 8)
//	y = []byte{127,0}
//	btree.SafeSet(y, 6)
//	y[1] = 1
//	v, ok = btree.Get(x) // can find
//	assert.Equal(t, uint64(6), v)
//	assert.Equal(t, true, ok)
//	_, ok = btree.Get(y) // can not find
//	assert.Equal(t, false, ok)
//	x[1] = 1
//	v, ok = btree.Get([]byte{127,0}) // can find! since btree is internal key is NOT changed
//	assert.Equal(t, uint64(6), v)
//	assert.Equal(t, true, ok)
//}


