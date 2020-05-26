package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/coinexchain/onvakv/datatree"
	"github.com/coinexchain/randsrc"
)

const (
	PruneRatio = 0.5
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
	fmt.Printf("Initialized\n")
	for i := 0; i< roundCount; i++ {
		if i % 10000 == 0 {
			fmt.Printf("Now %d of %d\n", i, roundCount)
		}
		ctx.step()
	}
}

type FuzzConfig struct {
	EndBlockStripe     uint32 // run EndBlock every n steps
	ReloadEveryNBlock  uint32 // reload tree from disk every n blocks
	RecoverEveryNBlock uint32 // recover tree from disk every n blocks
	PruneEveryNBlock   uint32 // prune the tree every n blocks
	MaxKVLen           uint32 // max length of key and value
	DeactiveStripe     uint32 // deactive some entry every n steps
	DeactiveCount      uint32 // number of deactive try times
	MaxActiveCount     uint32 // the maximum count of active entries
}

var DefaultConfig = FuzzConfig{
	EndBlockStripe:     1000,
	ReloadEveryNBlock:  30,
	RecoverEveryNBlock: 60,
	PruneEveryNBlock:   20,
	MaxKVLen:           20,
	DeactiveStripe:     2,
	DeactiveCount:      8,
	MaxActiveCount:     1*1024*1024,
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
	defaultFileSize = 16*1024*1024
	dirName = "./datadir"
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

func (ctx *Context) oldestSN() int64 {
	return ctx.oldestTwigID * datatree.LeafCountInTwig
}

func (ctx *Context) generateRandSN() int64 {
	oldestSN := ctx.oldestSN()
	return oldestSN + int64(ctx.rs.GetUint64()%uint64(ctx.serialNum - oldestSN))
}

func (ctx *Context) generateRandEntry() *datatree.Entry {
	e := &datatree.Entry{
		Key:        ctx.rs.GetBytes(int(ctx.rs.GetUint32()%ctx.cfg.MaxKVLen)),
		Value:      ctx.rs.GetBytes(int(ctx.rs.GetUint32()%ctx.cfg.MaxKVLen)),
		NextKey:    ctx.rs.GetBytes(int(ctx.rs.GetUint32()%ctx.cfg.MaxKVLen)),
		Height:     ctx.height,
		LastHeight: 0,
		SerialNum:  ctx.serialNum,
	}
	ctx.serialNum++
	return e
}

func (ctx *Context) initialAppends() {
	ctx.activeCount = int64(ctx.cfg.MaxActiveCount/2)
	for i := int64(0); i < ctx.activeCount; i++ {
		entry := ctx.generateRandEntry()
		ctx.tree.AppendEntry(entry)
	}
}

func (ctx *Context) step() {
	entry := ctx.generateRandEntry()
	ctx.tree.AppendEntry(entry)
	ctx.activeCount++
	if ctx.rs.GetUint32() % ctx.cfg.DeactiveStripe == 0 {
		for i := 0; i < int(ctx.cfg.DeactiveCount); i++ {
			sn := ctx.generateRandSN()
			if ctx.tree.GetActiveBit(sn) {
				ctx.tree.DeactiviateEntry(sn)
				ctx.activeCount--
			}
		}
	}
	if ctx.rs.GetUint32() % ctx.cfg.EndBlockStripe == 0 {
		ctx.endBlock()
	}
}

func (ctx *Context) endBlock() {
	ctx.height++
	fmt.Printf("Now EndBlock\n")
	ctx.tree.EndBlock()
	datatree.CheckHashConsistency(ctx.tree)
	//if ctx.height % int64(ctx.cfg.ReloadEveryNBlock) == 0 {
	//	fmt.Printf("Now reloadTree\n")
	//	ctx.reloadTree()
	//}
	//if ctx.height % int64(ctx.cfg.RecoverEveryNBlock) == 0 {
	//	fmt.Printf("Now recoverTree\n")
	//	ctx.recoverTree()
	//}
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
	fmt.Printf("Try pruneTree %f %d %d\n", float64(ctx.activeCount) / float64(ctx.serialNum - ctx.oldestSN()), ctx.activeCount, ctx.serialNum - ctx.oldestSN())
	for float64(ctx.activeCount) / float64(ctx.serialNum - ctx.oldestSN()) < PruneRatio {
		entries := ctx.tree.GetActiveEntriesInTwig(ctx.oldestTwigID)
		for _, entry := range entries {
			sn := entry.SerialNum
			if ctx.tree.GetActiveBit(sn) {
				ctx.tree.DeactiviateEntry(sn)
				entry.SerialNum = ctx.serialNum
				ctx.serialNum++
				ctx.tree.AppendEntry(entry)
			}
		}
		ctx.tree.EvictTwig(ctx.oldestTwigID)
		ctx.oldestTwigID++
	}
	fmt.Printf("Now oldestTwigID %d serialNum %d\n", ctx.oldestTwigID, ctx.serialNum)
	ctx.tree.EndBlock()
	endID := ctx.oldestTwigID - 1
	ratio := float64(ctx.activeCount) / float64(ctx.serialNum - ctx.oldestSN())
	fmt.Printf("Now pruneTree(%f) %d %d\n", ratio, ctx.lastPrunedTwigID, endID)
	if endID - ctx.lastPrunedTwigID >= datatree.MinPruneCount {
		bz := ctx.tree.PruneTwigs(ctx.lastPrunedTwigID, endID)
		ctx.edgeNodes = datatree.BytesToEdgeNodes(bz)
		ctx.lastPrunedTwigID = endID
	}
}


