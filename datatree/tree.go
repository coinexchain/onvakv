package datatree

import (
	"encoding/binary"
)

const FirstLevelAboveTwig int = 13

/*
                 ____TwigRoot___                Level_12
                /               \
	       /                \
1      leafMTRoot               activeBitsMTL3   Level_11
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
	TwigShift       = 11
	LeafCountInTwig = 1 << TwigShift // 2048
	TwigMask        = LeafCountInTwig - 1
)

var NullTwig Twig
var NullMT4Twig [4096][32]byte
var NullNodeInHigherTree [64][32]byte

type Twig struct {
	activeBits     [256]byte
	activeBitsMTL1 [4][32]byte
	activeBitsMTL2 [2][32]byte
	activeBitsMTL3 [32]byte
	leafMTRoot     [32]byte
	twigRoot       [32]byte
	FirstEntryPos  int64
}

func Init() {
	NullTwig.FirstEntryPos = -1
	for i := 0; i < 256; i++ {
		NullTwig.activeBits[i] = 0
	}
	var h Hasher
	NullTwig.syncL1(0, &h)
	NullTwig.syncL1(1, &h)
	NullTwig.syncL1(2, &h)
	NullTwig.syncL1(3, &h)
	h.Run()
	NullTwig.syncL2(0, &h)
	NullTwig.syncL2(1, &h)
	h.Run()
	NullTwig.syncL3(&h)
	h.Run()

	nullEntry := NullEntry()
	bz := EntryToBytes(nullEntry, nil)
	nullHash := hash(bz)
	level := byte(0)
	for stripe := 2048; stripe >= 1; stripe = stripe >> 1 {
		// use nullHash to fill one level of nodes
		for i := 0; i < stripe; i++ {
			copy(NullMT4Twig[stripe+i][:], nullHash[:])
		}
		nullHash = hash2(level, nullHash, nullHash)
		level++
	}
	copy(NullTwig.leafMTRoot[:], NullMT4Twig[1][:])

	NullTwig.syncTop(&h)
	h.Run()

	node := hash2(byte(FirstLevelAboveTwig), NullTwig.twigRoot[:], NullTwig.twigRoot[:])
	copy(NullNodeInHigherTree[FirstLevelAboveTwig][:], node)
	for i := FirstLevelAboveTwig + 1; i < len(NullNodeInHigherTree); i++ {
		node = hash2(byte(i), NullNodeInHigherTree[i-1][:], NullNodeInHigherTree[i-1][:])
		copy(NullNodeInHigherTree[i][:], node)
	}
}

func (twig *Twig) syncL1(pos int, h *Hasher) {
	switch pos {
	case 0:
		h.Add(8, twig.activeBitsMTL1[0][:], twig.activeBits[32*0:32*1], twig.activeBits[32*1:32*2])
	case 1:
		h.Add(8, twig.activeBitsMTL1[1][:], twig.activeBits[32*2:32*3], twig.activeBits[32*3:32*4])
	case 2:
		h.Add(8, twig.activeBitsMTL1[2][:], twig.activeBits[32*4:32*5], twig.activeBits[32*5:32*6])
	case 3:
		h.Add(8, twig.activeBitsMTL1[3][:], twig.activeBits[32*6:32*7], twig.activeBits[32*7:32*8])
	default:
		panic("Can not reach here!")
	}
}

func (twig *Twig) syncL2(pos int, h *Hasher) {
	switch pos {
	case 0:
		h.Add(9, twig.activeBitsMTL2[0][:], twig.activeBitsMTL1[0][:], twig.activeBitsMTL1[1][:])
	case 1:
		h.Add(9, twig.activeBitsMTL2[1][:], twig.activeBitsMTL1[2][:], twig.activeBitsMTL1[3][:])
	default:
		panic("Can not reach here!")
	}
}

func (twig *Twig) syncL3(h *Hasher) {
	h.Add(10, twig.activeBitsMTL3[:], twig.activeBitsMTL2[0][:], twig.activeBitsMTL2[1][:])
}

func (twig *Twig) syncTop(h *Hasher) {
	h.Add(11, twig.twigRoot[:], twig.activeBitsMTL3[:], twig.leafMTRoot[:])
}

func (twig *Twig) setBit(offset int) {
	if offset < 0 || offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := byte(1) << (offset & 0x7)
	pos := offset >> 3
	twig.activeBits[pos] |= mask
}
func (twig *Twig) clearBit(offset int) {
	if offset < 0 || offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := byte(1) << (offset & 0x7)
	pos := offset >> 3
	twig.activeBits[pos] &= ^mask
}
func (twig *Twig) getBit(offset int) bool {
	if offset < 0 || offset > LeafCountInTwig {
		panic("Invalid ID")
	}
	mask := byte(1) << (offset & 0x7)
	pos := offset >> 3
	return (twig.activeBits[pos] & mask) != 0
}

type NodePos int64

func Pos(level int, n int64) NodePos {
	return NodePos((int64(level) << 56) | n)
}

type EdgeNode struct {
	Pos   NodePos
	Value []byte
}

func EdgeNodesToBytes(edgeNodes []*EdgeNode) []byte {
	const stripe = 8 + 32
	res := make([]byte, len(edgeNodes)*stripe)
	for i, node := range edgeNodes {
		binary.LittleEndian.PutUint64(res[i*stripe:i*stripe+8], uint64(node.Pos))
		copy(res[i*stripe+8:(i+1)*stripe], node.Value)
	}
	return res
}

func BytesToEdgeNodes(bz []byte) []*EdgeNode {
	const stripe = 8 + 32
	if len(bz)%stripe != 0 {
		panic("Invalid byteslice length for EdgeNodes")
	}
	res := make([]*EdgeNode, 0, len(bz)/stripe)
	for i := 0; i < len(res); i++ {
		var value [32]byte
		pos := binary.LittleEndian.Uint64(bz[i*stripe : i*stripe+8])
		copy(value[:], bz[i*stripe+8 : (i+1)*stripe])
		res[i] = &EdgeNode{Pos: NodePos(pos), Value: value[:]}
	}
	return res
}

type Tree struct {
	entryFile  *EntryFile
	twigMtFile *TwigMtFile

	// the nodes in high level tree (higher than twigs)
	nodes map[NodePos][]byte

	activeTwigs        map[int64]*Twig
	mtree4YoungestTwig [4096][32]byte

	// The following variables are only used during the execution of one block
	youngestTwigID      int64
	mtree4YTChangeStart int
	mtree4YTChangeEnd   int
	twigsToBeDeleted    []int64
	deactivations       map[int64]struct{}
	deactivedSNList     []int64
}

func (tree *Tree) GetFileSizes() (int64, int64) {
	return tree.entryFile.Size(), tree.twigMtFile.Size()
}

func (tree *Tree) TruncateFiles(entryFileSize, twigMtFileSize int64) {
	tree.entryFile.Truncate(entryFileSize)
	tree.twigMtFile.Truncate(twigMtFileSize)
}

func (tree *Tree) ReadEntry(pos int64) (entry *Entry) {
	entry, _, _ = tree.entryFile.ReadEntry(pos)
	return
}

func (tree *Tree) GetActiveBit(sn int64) bool {
	twigID := sn >> TwigShift
	return tree.activeTwigs[twigID].getBit(int(sn & TwigMask))
}

func (tree *Tree) DeactiviateEntry(sn int64) {
	twigID := sn >> TwigShift
	tree.activeTwigs[twigID].clearBit(int(sn & TwigMask))
	tree.deactivations[sn>>9] = struct{}{} // 9??
	tree.deactivedSNList = append(tree.deactivedSNList, sn)
}

func (tree *Tree) AppendEntry(entry *Entry) int64 {
	twigID := entry.SerialNum >> TwigShift
	tree.youngestTwigID = twigID
	twig := tree.activeTwigs[twigID]
	position := int(entry.SerialNum & TwigMask)
	twig.setBit(position)
	if tree.mtree4YTChangeStart == -1 {
		tree.mtree4YTChangeStart = position
	}
	tree.mtree4YTChangeEnd = position

	bz := EntryToBytes(*entry, tree.deactivedSNList)
	tree.deactivedSNList = tree.deactivedSNList[:0] // clear its content
	pos := tree.entryFile.Append(bz)
	idx := entry.SerialNum & TwigMask
	copy(tree.mtree4YoungestTwig[idx][:], hash(bz))

	if (entry.SerialNum & TwigMask) == TwigMask {
		tree.syncMT4YoungestTwig()
		tree.twigMtFile.AppendTwig(tree.mtree4YoungestTwig[:], twig.FirstEntryPos)
		tree.youngestTwigID++
		tree.activeTwigs[tree.youngestTwigID] = &Twig{}
		*(tree.activeTwigs[tree.youngestTwigID]) = NullTwig
		tree.activeTwigs[tree.youngestTwigID].FirstEntryPos = pos + int64(len(bz))
		tree.mtree4YoungestTwig = NullMT4Twig
	}
	return pos
}

func (tree *Tree) GetActiveEntriesInTwig(twigID int64) []*Entry {
	twig := tree.activeTwigs[twigID]
	return tree.entryFile.GetActiveEntriesInTwig(twig)
}

func (tree *Tree) TwigCanBePruned(twigID int64) bool {
	// Can not prune an active twig
	_, ok := tree.activeTwigs[twigID]
	return !ok
}

func (tree *Tree) PruneTwigs(startID, endID int64) []byte {
	tree.entryFile.PruneHead(tree.twigMtFile.GetFirstEntryPos(endID))
	tree.twigMtFile.PruneHead(endID * TwigMtSize)

	level := FirstLevelAboveTwig
	start := startID >> 1
	end := endID >> 1
	var newEdgeNodes []*EdgeNode
	for start != 0 && end != 0 { //TODO
		pos := Pos(level, end)
		edgeNode := &EdgeNode{Pos: pos, Value: tree.nodes[pos]}
		newEdgeNodes = append(newEdgeNodes, edgeNode)
		for i := start; i < end; i++ {
			pos = Pos(level, i)
			delete(tree.nodes, pos)
		}
		start >>= 1
		end >>= 1
	}
	return EdgeNodesToBytes(newEdgeNodes)
}

func (tree *Tree) calcMaxLevel() int {
	maxLevel := FirstLevelAboveTwig
	for i := tree.youngestTwigID; i != 0; i = i >> 1 {
		maxLevel++
	}
	return maxLevel
}

func (tree *Tree) EndBlock() (rootHash []byte) {
	rootHash = tree.syncMT(tree.calcMaxLevel())
	for _, twigID := range tree.twigsToBeDeleted {
		delete(tree.activeTwigs, twigID)
	}
	tree.twigsToBeDeleted = tree.twigsToBeDeleted[:0] // clear its content
	tree.entryFile.Sync()
	tree.twigMtFile.Sync()
	return
}

func (tree *Tree) DeleteActiveTwig(twigID int64) {
	tree.twigsToBeDeleted = append(tree.twigsToBeDeleted, twigID)
}

func (tree *Tree) syncNodesByLevel(level int, nList map[int64]struct{}) map[int64]struct{} {
	if level < FirstLevelAboveTwig {
		panic("level is too small")
	}
	newList := make(map[int64]struct{})
	var h Hasher
	if level == FirstLevelAboveTwig {
		for i := range nList {
			nodePos := Pos(level, i)
			h.Add(byte(level), tree.nodes[nodePos], tree.activeTwigs[2*i].twigRoot[:], tree.activeTwigs[2*i+1].twigRoot[:])
			newList[i/2] = struct{}{}
		}
	} else {
		for i := range nList {
			nodePos := Pos(level, i)
			nodePosL := Pos(level, 2*i)
			nodePosR := Pos(level, 2*i+1)
			var nodeR []byte
			if 2*i+1 > ((tree.youngestTwigID + 2) >> level) {
				nodeR = append([]byte{}, NullNodeInHigherTree[level][:]...)
			} else {
				nodeR = tree.nodes[nodePosR]
			}
			h.Add(byte(level), tree.nodes[nodePos], tree.nodes[nodePosL], nodeR)
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
		twigID := i >> 2
		tree.activeTwigs[twigID].syncL1(int(i&3), &h)
		newList[i/2] = struct{}{}
	}
	h.Run()
	nList = newList
	newList = make(map[int64]struct{})
	for i := range nList {
		twigID := i >> 1
		tree.activeTwigs[twigID].syncL2(int(i&1), &h)
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
	level := byte(0)
	for base := 2048; base >= 2; base >>= 1 {
		for i := tree.mtree4YTChangeStart &^ 1; i < tree.mtree4YTChangeEnd; i += 2 {
			h.Add(level, tree.mtree4YoungestTwig[i/2][:], tree.mtree4YoungestTwig[i][:], tree.mtree4YoungestTwig[i+1][:])
		}
		h.Run()
		tree.mtree4YTChangeStart >>= 1
		tree.mtree4YTChangeEnd >>= 1
		level++
	}
	tree.mtree4YTChangeStart = -1 // reset its value
	tree.mtree4YTChangeEnd = 0
	copy(tree.activeTwigs[tree.youngestTwigID].leafMTRoot[:], tree.mtree4YoungestTwig[1][:])
}

func (tree *Tree) syncMT(maxLevel int) []byte {
	tree.syncMT4YoungestTwig()
	nList := tree.syncMT4ActiveBits(tree.deactivations)
	for level := FirstLevelAboveTwig; level < maxLevel; level++ {
		nList = tree.syncNodesByLevel(level, nList)
	}
	tree.deactivations = make(map[int64]struct{})
	return append([]byte{}, tree.nodes[Pos(maxLevel, 0)]...) // return the merkle root
}
