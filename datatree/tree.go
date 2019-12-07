package datatree

import (
	"encoding/binary"

	dbm "github.com/tendermint/tm-db"
)

var zerobuf [32]byte

const FirstLevelAboveTwig int = 13

/*
                 ____TwigRoot___                Level_12
                /               \
	       /                \
       leafMTRoot               activeBitsMTL3   Level_11
2	Level_10	2	activeBitsMTL2
4	Level_9		4	activeBitsMTL1
8	Level_8    8*32bytes	activeBitsMTL0
16	Level_7
32	Level_6
64	Level_5
128	Level_4
256	Level_3
512	Level_2
1024	Level_1
2048	Level_0
*/

const (
	TwigShift int64 = 11
	LeafCountInTwig int64 = 1 << TwigShift
	TwigMask  int64 = LeafCountInTwig - 1
)

type Twig struct {
	activeBits          [256]byte
	activeBitsMTL1      [4][32]byte
	activeBitsMTL2      [2][32]byte
	activeBitsMTL3      [32]byte
	leafMTRoot          [32]byte
	twigRoot            [32]byte
	FirstEntryPos       uint64
}

func (twig *Twig) syncL1(pos int, h *Hasher) {
	switch pos {
	case 0: h.Add(&(twig.activeBitsMTL1[0][:]), twig.activeBits[32*0:32*1], twig.activeBits[32*1:32*2])
	case 1: h.Add(&(twig.activeBitsMTL1[1][:]), twig.activeBits[32*2:32*3], twig.activeBits[32*3:32*4])
	case 2: h.Add(&(twig.activeBitsMTL1[2][:]), twig.activeBits[32*4:32*5], twig.activeBits[32*5:32*6])
	case 3: h.Add(&(twig.activeBitsMTL1[3][:]), twig.activeBits[32*6:32*7], twig.activeBits[32*7:32*8])
	default: panic("Can not reach here!")
	}
}

func (twig *Twig) syncL2(pos int, h *Hasher) {
	switch pos {
	case 0: h.Add(&(twig.activeBitsMTL2[0][:]), twig.activeBitsMTL1[0][:], twig.activeBitsMTL1[1][:])
	case 1: h.Add(&(twig.activeBitsMTL2[1][:]), twig.activeBitsMTL1[2][:], twig.activeBitsMTL1[3][:])
	default: panic("Can not reach here!")
	}
}

func (twig *Twig) syncL3(h *Hasher) {
	h.Add(&(twig.activeBitsMTL3[:]), twig.activeBitsMTL2[0][:], twig.activeBitsMTL2[1][:])
}

func (twig *Twig) syncTop(h *Hasher) {
	h.Add(&(twig.twigRoot[:]), twig.activeBitsMTL3[:], twig.leafMTRoot[:])
}

func (twig *Twig) clearBit(offset int64) {
	if offset < 0 || offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := byte(1) << (offset & 0x7)
	pos := int(offset >> 3)
	twig.activeBits[pos] &= ^mask
}
func (twig *Twig) getBit(offset int64) bool {
	if offset < 0 || offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := byte(1) << (offset & 0x7)
	pos := int(offset >> 3)
	return (twig.activeBits[pos] & mask) != 0
}

type NodePos int64

func Pos(level int64, n int64) NodePos {
	return (level<<56)|n
}

type EdgeNode struct {
	Pos   NodePos
	Value []byte
}

type Tree struct {
	entryFile *EntryFile
	twigMtFile  *TwigMtFile

	// the nodes in high level tree (higher than twigs)
	nodes map[int64][]byte

	activeTwigs map[NodePos]*Twig
	youngestTwigID int64

	mtree4YoungestTwig [4096][32]byte
	mtree4YTChangeStart int
	mtree4YTChangeEnd int

	twigsToBeDeleted []int64
	deactivations    map[int64]struct{}{}
}

func (tree *Tree) ReadEntry(pos uint64) *Entry {
	return tree.entryFile.ReadEntry(pos)
}

func (tree *Tree) GetActiveBit(sn int64) bool {
	twigID := sn >> TwigShift
	return tree.activeTwigs[twigID].getBit(sn & TwigMask)
}

func (tree *Tree) DeactiviateEntry(sn int64) {
	twigID := sn >> TwigShift
	tree.activeTwigs[twigID].clearBit(sn & TwigMask)
	tree.deactivations[sn>>9] = struct{}{}
}

func (tree *Tree) AppendEntry(entry *Entry) int64 {
	twigID := entry.SerialNum>>TwigShift
	if twigID != tree.youngestTwigID {
		panic("Not appending to the youngest twig")
	}
	twig := tree.activeTwigs[twigID]
	pos := entry.SerialNum & TwigMask
	twig.setBit(pos)
	if tree.mtree4YTChangeStart == -1 {
		tree.mtree4YTChangeStart = pos
	}
	tree.mtree4YTChangeEnd = pos

	bz := entry.ToBytes()
	pos := tree.entryFile.Append(bz)
	idx := entry.SerialNum & TwigMask
	tree.mtree4YTChangeEnd[idx] = hash(bz)

	if (entry.SerialNum & TwigMask) == TwigMask {
		tree.syncMT4YoungestTwig()
		tree.twigMtFile.AppendTwig(tree.mtree4YoungestTwig, twig.FirstEntryPos)
		for i:=0; i<len(tree.mtree4YoungestTwig); i++ {
			copy(tree.mtree4YoungestTwig[i], zerobuf)
		}
		tree.activeTwigs[twigID+1] = &Twig{FirstEntryPos: pos+len(bz)}
		tree.youngestTwigID++
	}
	return pos
}

func (tree *Tree) GetActiveEntriesInTwig(twigID int64) []*Entry {
	twig := tree.activeTwigs[twigID]
	return tree.entryFile.GetActiveEntriesInTwig(twig)
}

func (tree *Tree) PruneTwigsBefore(twigID int64) (newEdgeNodes []*EdgeNode) {
	twig := tree.activeTwigs[twigID]
	tree.entryFile.PruneHead(twig.FirstEntryPos)
	tree.twigMtFile.PruneHead(twigID*TwigSize)

	level := FirstLevelAboveTwig
	start := tree.lastPruneTwigID >> 1
	end := twigID >> 1
	for start != 0 && end != 0 {
		pos := Pos(level, end)
		edgeNode := &EdgeNode{Pos: pos, Value: tree.nodes[pos]}
		newEdgeNodes = append(newEdgeNodes, edgeNode)
		for i:=start; i<end; i++ {
			pos = Pos(level, i)
			delete(tree.nodes, pos)
		}
		start >>= 1
		end >>= 1
	}
	tree.lastPruneTwigID = twigID
	return
}

func (tree *Tree) EndBlock(maxLevel int) (rootHash []byte) {
	rootHash = tree.syncMT(maxLevel)
	for _, twigID := range tree.twigsToBeDeleted {
		delete(tree.activeTwigs, twigID)
	}
	tree.twigsToBeDeleted = tree.twigsToBeDeleted[:0]
	tree.entryFile.Sync()
	tree.twigMtFile.Sync()
	return
}

func (tree *Tree) DeleteActiveTwig(twigID int64) {
	tree.twigsToBeDeleted = append(tree.twigsToBeDeleted, twigID)
}

func (tree *Tree) syncNodesByLevel(level int8, nList map[int64]struct{}) map[int64]struct{} {
	if level < FirstLevelAboveTwig {
		panic("level is too small")
	}
	newList := make(map[int64]struct{})
	var h Hasher
	if level == FirstLevelAboveTwig {
		for i := range nList {
			nodePos = Pos(level, i)
			h.Add(&(tree.nodes[nodePos]), tree.activeTwigs[2*i].twigRoot, tree.activeTwigs[2*i+1].twigRoot)
			newList[i/2] = struct{}{}
		}
	} else {
		for i := range nList {
			nodePos = Pos(level, i)
			nodePosL = Pos(level, 2*i)
			nodePosR = Pos(level, 2*i+1)
			h.Add(&(tree.nodes[nodePos]), tree.nodes[nodePosL], tree.nodes[nodePosR])
			newList[i/2] = struct{}{}
		}
	}
	h.Run()
	return newList
}

func (tree *Tree) syncMT4ActiveBits(nList map[int64]struct{}) map[int64]struct{} {
	newList := make(map[int64]struct{})
	var h Hasher
	for i := range nList {
		twigID := i>>2
		tree.activeTwigs[twigID].syncL1(i&3, &h)
		newList[i/2] = struct{}{}
	}
	h.Run()
	nList = newList
	newList = make(map[int64]struct{})
	for i := range nList {
		twigID := i>>1
		tree.activeTwigs[twigID].syncL2(i&1, &h)
		newList[i/2] = struct{}{}
	}
	h.Run()
	nList = newList
	for i := range nList {
		tree.activeTwigs[i].syncL3(&h)
	}
	newList = make(map[int64]struct{})
	for i := range nList {
		tree.activeTwigs[i].syncTop(&h)
		newList[i/2] = struct{}{}
	}
	return newList
}

func (tree *Tree) syncMT4YoungestTwig() {
	var h Hasher
	for base:=2048; base >= 2; base>>=1 {
		for i:=tree.mtree4YTChangeStart&^1; i < tree.mtree4YTChangeEnd; i+=2 {
			h.Add(&(tree.mtree4YoungestTwig[i/2][:]), tree.mtree4YoungestTwig[i][:], tree.mtree4YoungestTwig[i+1][:])
		}
		h.Run()
		tree.mtree4YTChangeStart >>= 1
		tree.mtree4YTChangeEnd >>= 1
	}
	tree.mtree4YTChangeStart = -1
	tree.mtree4YTChangeEnd = 0
}

func (tree *Tree) syncMT(maxLevel int) []byte {
	tree.syncMT4YoungestTwig()
	nList = tree.syncMT4ActiveBits(tree.deactivations)
	for level:=FirstLevelAboveTwig; level < maxLevel; level++ {
		nList = tree.syncNodesByLevel(level, nList)
	}
	tree.deactivations = make(map[int64]struct{}{})
	return tree.nodes[Pos(maxLevel, 0)]
}

