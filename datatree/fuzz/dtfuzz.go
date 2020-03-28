package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/coinexchain/onvakv/datatree"
	"github.com/coinexchain/randsrc"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <rand-source-file> <round-count>\n", os.Args[0])
		return
	}
	randFilename := os.Args[1]
	roundCount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	rs := randsrc.NewRandSrcFromFile(randFilename)
	ctx := NewContext(DefaultConfig, rs)
	ctx.initialAppends()
	for i := 0; i< roundCount; i++ {
		ctx.step()
	}
}

type FuzzConfig struct {
	EndBlockStripe     uint32
	ReloadEveryNBlock  uint32
	RecoverEveryNBlock uint32
	PruneEveryNBlock   uint32
	KeyLenStripe       uint32
	ValueLenStripe     uint32
	DeactiveStripe     uint32
	MaxActiveCount     uint32
}

var DefaultConfig = FuzzConfig{
	EndBlockStripe:     1000,
	ReloadEveryNBlock:  30,
	RecoverEveryNBlock: 60,
	PruneEveryNBlock:   20,
	KeyLenStripe:       10,
	ValueLenStripe:     20,
	DeactiveStripe:     2,
	MaxActiveCount:     1024*1024,
}

type Context struct {
	tree      *datatree.Tree
	rs        randsrc.RandSrc
	cfg       FuzzConfig
	edgeNodes []*datatree.EdgeNode

	oldestTwigID     int64
	serialNum        int64
	lastPrunedTwigID int64
	activeCount      int64
	height           int64
}

const (
	defaultFileSize = 64*1024*1024
	dirName = "./dtfuzz"
)

func NewContext(cfg FuzzConfig, rs randsrc.RandSrc) *Context {
	os.RemoveAll(dirName)
	os.Mkdir(dirName, 0700)
	return &Context{
		tree: datatree.NewEmptyTree(defaultFileSize, dirName),
		rs:   rs,
		cfg:  cfg,
	}
}

func (ctx *Context) generateRandSN() int64 {
	oldestSN := ctx.oldestTwigID*datatree.LeafCountInTwig
	num := ctx.serialNum - oldestSN
	return oldestSN + int64(ctx.rs.GetUint64()%uint64(num))
}

func (ctx *Context) generateRandEntry() *datatree.Entry {
	e := &datatree.Entry{
		Key:        ctx.rs.GetBytes(int(ctx.rs.GetUint32()%ctx.cfg.KeyLenStripe)),
		Value:      ctx.rs.GetBytes(int(ctx.rs.GetUint32()%ctx.cfg.KeyLenStripe)),
		NextKey:    ctx.rs.GetBytes(int(ctx.rs.GetUint32()%ctx.cfg.KeyLenStripe)),
		Height:     ctx.height,
		LastHeight: 0,
		SerialNum:  ctx.serialNum,
	}
	ctx.serialNum += 1
	return e
}

func (ctx *Context) initialAppends() {
	ctx.activeCount = int64(ctx.cfg.MaxActiveCount/2)
	for i := int64(0); i < ctx.activeCount; i++ {
		entry := ctx.generateRandEntry()
		ctx.tree.AppendEntry(entry)
	}
}

func (ctx *Context) run() {
	ctx.initialAppends()
}

func (ctx *Context) step() {
	ctx.activeCount = int64(ctx.cfg.MaxActiveCount/2)
	entry := ctx.generateRandEntry()
	ctx.tree.AppendEntry(entry)
	if ctx.rs.GetUint32() % ctx.cfg.DeactiveStripe == 0 {
		sn := ctx.generateRandSN()
		if ctx.tree.GetActiveBit(sn) {
			ctx.tree.DeactiviateEntry(sn)
			ctx.activeCount--
		}
	}
	if ctx.rs.GetUint32() % ctx.cfg.EndBlockStripe == 0 {
		ctx.endBlock()
	}
}

func (ctx *Context) endBlock() {
	ctx.height++
	datatree.CheckHashConsistency(ctx.tree)
	if ctx.height % int64(ctx.cfg.ReloadEveryNBlock) == 0 {
		ctx.reloadTree()
	}
	if ctx.height % int64(ctx.cfg.RecoverEveryNBlock) == 0 {
		ctx.recoverTree()
	}
	if ctx.height % int64(ctx.cfg.PruneEveryNBlock) == 0 {
		ctx.pruneTree()
	}
}

func (ctx *Context) reloadTree() {
	ctx.tree.Sync()
	tree1 := datatree.LoadTree(defaultFileSize, dirName)

	datatree.CompareTreeNodes(ctx.tree, tree1)
	datatree.CheckHashConsistency(tree1)
	ctx.tree.Close()
	ctx.tree = tree1
}

func (ctx *Context) recoverTree() {
	ctx.tree.Sync()
	tree1 := datatree.RecoverTree(defaultFileSize, dirName,
		ctx.edgeNodes, ctx.oldestTwigID, ctx.serialNum >> datatree.TwigShift)

	datatree.CompareTreeNodes(ctx.tree, tree1)
	datatree.CheckHashConsistency(tree1)
	ctx.tree.Close()
	ctx.tree = tree1
}

func (ctx *Context) pruneTree() {
	for ctx.activeCount > int64(ctx.cfg.MaxActiveCount) || ctx.oldestTwigID%2 != 0 {
		entries := ctx.tree.GetActiveEntriesInTwig(ctx.oldestTwigID)
		ctx.oldestTwigID++
		ctx.activeCount -= int64(len(entries))
		for _, entry := range entries {
			ctx.tree.DeactiviateEntry(entry.SerialNum)
		}
	}
	bz := ctx.tree.PruneTwigs(ctx.lastPrunedTwigID, ctx.oldestTwigID)
	ctx.edgeNodes = datatree.BytesToEdgeNodes(bz)
	ctx.lastPrunedTwigID = ctx.oldestTwigID
}


