package datatree

import (
	"fmt"
	"encoding/binary"
	"math/bits"
	"sort"
)

const (
	FirstLevelAboveTwig int = 13
	MinPruneCount int64 = 4
)

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

func CopyNullTwig() *Twig {
	var twig Twig
	twig = NullTwig
	return &twig
}

type Twig struct {
	activeBits     [256]byte
	activeBitsMTL1 [4][32]byte
	activeBitsMTL2 [2][32]byte
	activeBitsMTL3 [32]byte
	leafMTRoot     [32]byte
	twigRoot       [32]byte
	FirstEntryPos  int64
}

func init() {
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

	node := hash2(byte(FirstLevelAboveTwig-1), NullTwig.twigRoot[:], NullTwig.twigRoot[:])
	copy(NullNodeInHigherTree[FirstLevelAboveTwig][:], node)
	for i := FirstLevelAboveTwig + 1; i < len(NullNodeInHigherTree); i++ {
		node = hash2(byte(i-1), NullNodeInHigherTree[i-1][:], NullNodeInHigherTree[i-1][:])
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
		if len(node.Value) != 32 {
			fmt.Printf("node.Value %#v\n", node.Value)
			panic("len(node.Value) != 32")
		}
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
	res := make([]*EdgeNode, len(bz)/stripe)
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
	// this variable can be recovered from saved edge nodes and activeTwigs
	nodes map[NodePos]*[32]byte

	// these two variables can be recovered from entry file
	activeTwigs        map[int64]*Twig
	mtree4YoungestTwig [4096][32]byte

	// The following variables are only used during the execution of one block
	youngestTwigID      int64
	mtree4YTChangeStart int
	mtree4YTChangeEnd   int
	twigsToBeDeleted    []int64
	touchedPosOf512b    map[int64]struct{}
	deactivedSNList     []int64
}

func calcMaxLevel(youngestTwigID int64) int {
	return FirstLevelAboveTwig + 63 - bits.LeadingZeros64(uint64(youngestTwigID))
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

func (tree *Tree) setEntryActiviation(sn int64, active bool) {
	twigID := sn >> TwigShift
	if active {
		tree.activeTwigs[twigID].setBit(int(sn & TwigMask))
	} else {
		tree.activeTwigs[twigID].clearBit(int(sn & TwigMask))
	}
	tree.touchedPosOf512b[sn/512] = struct{}{}
	tree.deactivedSNList = append(tree.deactivedSNList, sn)
}

func (tree *Tree) DeactiviateEntry(sn int64) {
	tree.setEntryActiviation(sn, false)
}

func (tree *Tree) AppendEntry(entry *Entry) int64 {
	//update youngestTwigID
	twigID := entry.SerialNum >> TwigShift
	tree.youngestTwigID = twigID
	// mark this entry as valid
	twig := tree.activeTwigs[twigID]
	position := int(entry.SerialNum & TwigMask)
	twig.setBit(position)
	// record ChangeStart/ChangeEnd for endblock sync
	if tree.mtree4YTChangeStart == -1 {
		tree.mtree4YTChangeStart = position
	}
	tree.mtree4YTChangeEnd = position

	// write the entry while flushing deactivedSNList
	bz := EntryToBytes(*entry, tree.deactivedSNList)
	tree.deactivedSNList = tree.deactivedSNList[:0] // clear its content
	pos := tree.entryFile.Append(bz)
	// update the corresponding leaf of merkle tree
	idx := entry.SerialNum & TwigMask
	copy(tree.mtree4YoungestTwig[idx][:], hash(bz))

	if (entry.SerialNum & TwigMask) == 0 { // when this is the first entry of current twig
		tree.activeTwigs[twigID].FirstEntryPos = pos
	} else if (entry.SerialNum & TwigMask) == TwigMask { // when this is the last entry of current twig
		// write the merkle tree of youngest twig to twigMtFile
		tree.syncMT4YoungestTwig()
		tree.twigMtFile.AppendTwig(tree.mtree4YoungestTwig[:], twig.FirstEntryPos)
		// allocate new twig as youngest twig
		tree.youngestTwigID++
		tree.activeTwigs[tree.youngestTwigID] = CopyNullTwig()
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

// Prune the twigs between startID and endID
func (tree *Tree) PruneTwigs(startID, endID int64) []byte {
	if endID - startID < MinPruneCount {
		panic(fmt.Sprintf("The count of pruned twigs is too small: %d", endID-startID))
	}
	tree.entryFile.PruneHead(tree.twigMtFile.GetFirstEntryPos(endID))
	tree.twigMtFile.PruneHead(endID * TwigMtSize)
	return tree.reapNodes(startID, endID)
}

// Remove useless nodes and reap the edge nodes
func (tree *Tree) reapNodes(startID, endID int64) []byte {
	if startID%2 != 0 || endID%2 != 0 {
		panic(fmt.Sprintf("Both startID and endID must be even. Now they are: %d %d", startID, endID))
	}
	start := startID >> 1
	end := endID >> 1
	var newEdgeNodes []*EdgeNode
	maxLevel := calcMaxLevel(tree.youngestTwigID)
	for level := FirstLevelAboveTwig; level <= maxLevel; level++ {
		endRound := end
		if end%2 != 0 {
			endRound--
		}
		pos := Pos(level, endRound)
		hash, ok := tree.nodes[pos]
		if !ok {
			fmt.Printf("What? can not find %d-%d\n", level, end)
		}
		edgeNode := &EdgeNode{Pos: pos, Value: (*hash)[:]}
		newEdgeNodes = append(newEdgeNodes, edgeNode)
		for i := start-1; i < endRound; i++ { // minus 1 from start to cover some margin nodes
			pos = Pos(level, i)
			delete(tree.nodes, pos)
		}
		start >>= 1
		end >>= 1
	}
	return EdgeNodesToBytes(newEdgeNodes)
}

func (tree *Tree) DeleteActiveTwig(twigID int64) {
	tree.twigsToBeDeleted = append(tree.twigsToBeDeleted, twigID)
}

func (tree *Tree) EndBlock() (rootHash []byte) {
	// sync up the merkle tree
	rootHash = tree.syncMT()
	// run the pending twig-deletion jobs
	// they were not deleted earlier becuase syncMT needs their content
	for _, twigID := range tree.twigsToBeDeleted {
		delete(tree.activeTwigs, twigID)
	}
	tree.twigsToBeDeleted = tree.twigsToBeDeleted[:0] // clear its content
	tree.entryFile.Sync()
	tree.twigMtFile.Sync()
	return
}

// following functions are used for syncing up merkle tree
func (tree *Tree) syncMT() []byte {
	maxLevel := calcMaxLevel(tree.youngestTwigID)
	tree.syncMT4YoungestTwig()
	nList := tree.syncMT4ActiveBits()
	tree.syncUpperNodes(nList)
	tree.touchedPosOf512b = make(map[int64]struct{}) // clear the list
	hash := tree.nodes[Pos(maxLevel, 0)]
	return append([]byte{}, (*hash)[:]...) // copy and return the merkle root
}

func (tree *Tree) syncUpperNodes(nList []int64) {
	maxLevel := calcMaxLevel(tree.youngestTwigID)
	for level := FirstLevelAboveTwig; level <= maxLevel; level++ {
		fmt.Printf("syncNodesByLevel: %d\n", level)
		nList = tree.syncNodesByLevel(level, nList)
	}
}

func maxNAtLevel(youngestTwigID int64, level int) int64 {
	if level < FirstLevelAboveTwig {
		panic("level is too small")
	}
	shift := level - FirstLevelAboveTwig
	maxN := youngestTwigID >> shift
	return maxN
}

func maxNPlus1AtLevel(youngestTwigID int64, level int) int64 {
	if level < FirstLevelAboveTwig {
		panic("level is too small")
	}
	shift := level - FirstLevelAboveTwig
	maxN := youngestTwigID >> shift
	mask := int64((1<<shift)-1)
	if (youngestTwigID & mask) != 0 {
		maxN += 1
	}
	return maxN
}

func (tree *Tree) syncNodesByLevel(level int, nList []int64) []int64 {
	maxN := maxNAtLevel(tree.youngestTwigID, level)
	newList := make([]int64, 0, len(nList))
	var h Hasher
	for _, i := range nList {
		nodePos := Pos(level, i)
		if _, ok := tree.nodes[nodePos]; !ok {
			//fmt.Printf("Now create parent node %d-%d\n", level, i)
			var zeroHash [32]byte
			tree.nodes[nodePos] = &zeroHash
		}
		if level == FirstLevelAboveTwig {
			left := tree.activeTwigs[2*i].twigRoot[:]
			right := NullTwig.twigRoot[:]
			if 2*i+1 <= tree.youngestTwigID {
				right = tree.activeTwigs[2*i+1].twigRoot[:]
			} else {
			//	fmt.Printf("Here we need a null right twig %d, youngestTwigID: %d\n", 2*i+1, tree.youngestTwigID)
			}
			parentNode := tree.nodes[nodePos]
			h.Add(byte(level-1), (*parentNode)[:], left, right)
			//fmt.Printf("left: %#v right: %#v\n", left, right)
			//fmt.Printf("New Job: %d-%d %d- %d %d\n", level, i, level-1, 2*i, 2*i+1)
		} else {
			nodePosL := Pos(level-1, 2*i)
			nodePosR := Pos(level-1, 2*i+1)
			if _, ok := tree.nodes[nodePosL]; !ok {
				panic(fmt.Sprintf("Failed to find the left child %d-%d %d- %d %d", level,i, level-1, 2*i, 2*i+1))
			}
			if _, ok := tree.nodes[nodePosR]; !ok {
				var h [32]byte
				copy(h[:], NullNodeInHigherTree[level][:])
				fmt.Printf("Here we create a node %d-%d\n", level-1, 2*i+1)
				tree.nodes[nodePosR] = &h
				if 2*i != maxN {
					panic("Not at the right edge, bug here")
				}
			}
			parentNode := tree.nodes[nodePos]
			nodeL := tree.nodes[nodePosL]
			nodeR := tree.nodes[nodePosR]
			h.Add(byte(level-1), (*parentNode)[:], (*nodeL)[:], (*nodeR)[:])
			//fmt.Printf("left: %#v right: %#v\n", (*nodeL)[:], (*nodeR)[:])
			//fmt.Printf("New Job: %d-%d %d- %d %d\n", level, i, level-1, 2*i, 2*i+1)
		}
		if len(newList) == 0 || newList[len(newList)-1] != i/2 {
			newList = append(newList, i/2)
		}
	}
	h.Run()
	return newList
}

func (tree *Tree) syncMT4ActiveBits() []int64 {
	nList := make([]int64, 0, len(tree.touchedPosOf512b))
	for i := range tree.touchedPosOf512b {
		nList = append(nList, i)
	}
	sort.Slice(nList, func(i, j int) bool {return nList[i] < nList[j]})

	newList := make([]int64, 0, len(nList))
	var h Hasher
	for _, i := range nList {
		twigID := int64(i >> 2)
		tree.activeTwigs[twigID].syncL1(int(i&3), &h)
		if len(newList) == 0 || newList[len(newList)-1] != i/2 {
			newList = append(newList, i/2)
		}
	}
	h.Run()
	nList = newList
	newList = make([]int64, 0, len(nList))
	for _, i := range nList {
		twigID := int64(i >> 1)
		tree.activeTwigs[twigID].syncL2(int(i&1), &h)
		if len(newList) == 0 || newList[len(newList)-1] != twigID {
			newList = append(newList, twigID)
		}
	}
	h.Run()
	nList = newList
	newList = make([]int64, 0, len(nList))
	for _, twigID := range nList {
		tree.activeTwigs[twigID].syncL3(&h)
	}
	h.Run()
	for _, twigID := range nList {
		tree.activeTwigs[twigID].syncTop(&h)
		if len(newList) == 0 || newList[len(newList)-1] != twigID/2 {
			newList = append(newList, twigID/2)
		}
	}
	h.Run()
	return newList
}

/*         1
     2            3
   4   5       6     7
 8  9 a b    c   d  e  f
*/
// Sync up the merkle tree, between ChangeStart and ChangeEnd
func (tree *Tree) syncMT4YoungestTwig() {
	if tree.mtree4YTChangeStart == -1 {// nothing changed
		return
	}
	var h Hasher
	level := byte(0)
	start, end := tree.mtree4YTChangeStart, tree.mtree4YTChangeEnd
	for base := LeafCountInTwig; base >= 2; base >>= 1 {
		//fmt.Printf("base: %d\n", base)
		endRound := end
		if end%2 == 1 {
			endRound++
		}
		for j := (start &^ 1); j <= endRound && j+1 < base; j += 2 {
			i := base + j
			h.Add(level, tree.mtree4YoungestTwig[i/2][:], tree.mtree4YoungestTwig[i][:], tree.mtree4YoungestTwig[i+1][:])
			//fmt.Printf("Now job: %d-%d(%d) %d(%d) %d(%d)\n", level, j/2, i/2, j, i, j+1, i+1)
		}
		h.Run()
		start >>= 1
		end >>= 1
		//if end%2 == 0 {
		//	end = end/2
		//} else {
		//	end = end/2 + 1
		//}
		level++
	}
	tree.mtree4YTChangeStart = -1 // reset its value
	tree.mtree4YTChangeEnd = 0
	copy(tree.activeTwigs[tree.youngestTwigID].leafMTRoot[:], tree.mtree4YoungestTwig[1][:])
}

