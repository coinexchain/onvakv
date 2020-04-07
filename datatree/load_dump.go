package datatree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/mmcloughlin/meow"

	"github.com/coinexchain/onvakv/types"
)

func LoadTwigFromFile(infile io.Reader) (twigID int64, twig Twig, err error) {
	var buf0,buf1 [8]byte
	var buf2 [4]byte
	var slices [13][]byte
	slices[0] = buf0[:]
	slices[1] = buf1[:]
	slices[2] = twig.activeBits[:]
	slices[3] = twig.activeBitsMTL1[0][:]
	slices[4] = twig.activeBitsMTL1[1][:]
	slices[5] = twig.activeBitsMTL1[2][:]
	slices[6] = twig.activeBitsMTL1[3][:]
	slices[7] = twig.activeBitsMTL2[0][:]
	slices[8] = twig.activeBitsMTL2[1][:]
	slices[9] = twig.activeBitsMTL3[:]
	slices[10] = twig.leafMTRoot[:]
	slices[11] = twig.twigRoot[:]
	slices[12] = buf2[:]
	h := meow.New32(0)
	for i, slice := range slices {
		_, err = infile.Read(slice)
		if err != nil {
			return
		}
		if i < 12 {
			h.Write(slice)
		}
	}
	if !bytes.Equal(buf2[:], h.Sum(nil)) {
		err = errors.New("Checksum mismatch")
	}

	twigID = int64(binary.LittleEndian.Uint64(buf0[:]))
	twig.FirstEntryPos = int64(binary.LittleEndian.Uint64(buf1[:]))
	return
}

func (twig *Twig) Dump(twigID int64, outfile io.Writer) error {
	var buf0,buf1 [8]byte
	binary.LittleEndian.PutUint64(buf0[:], uint64(twigID))
	binary.LittleEndian.PutUint64(buf1[:], uint64(twig.FirstEntryPos))
	var slices [13][]byte
	slices[0] = buf0[:]
	slices[1] = buf1[:]
	slices[2] = twig.activeBits[:]
	slices[3] = twig.activeBitsMTL1[0][:]
	slices[4] = twig.activeBitsMTL1[1][:]
	slices[5] = twig.activeBitsMTL1[2][:]
	slices[6] = twig.activeBitsMTL1[3][:]
	slices[7] = twig.activeBitsMTL2[0][:]
	slices[8] = twig.activeBitsMTL2[1][:]
	slices[9] = twig.activeBitsMTL3[:]
	slices[10] = twig.leafMTRoot[:]
	slices[11] = twig.twigRoot[:]
	h := meow.New32(0)
	for _, slice := range slices[:12] {
		h.Write(slice)
	}
	slices[12] = h.Sum(nil)
	for _, slice := range slices[:] {
		_, err := outfile.Write(slice)
		if err != nil {
			return err
		}
	}
	return nil
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


func (tree *Tree) DumpNodes(outfile io.Writer) error {
	for pos, hash := range tree.nodes {
		edgeNode := &EdgeNode{Pos: pos, Value: (*hash)[:]}
		h := meow.New32(0)
		var bzList [2][]byte
		bzList[0] = EdgeNodesToBytes([]*EdgeNode{edgeNode})
		h.Write(bzList[0])
		bzList[1] = h.Sum(nil)
		for _, bz := range bzList {
			_, err := outfile.Write(bz)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (tree *Tree) LoadNodes(infile io.Reader) error {
	var buf [8 + 32 + 4]byte
	for {
		_, err := infile.Read(buf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		h := meow.New32(0)
		h.Write(buf[:8+32])
		if !bytes.Equal(buf[8+32:], h.Sum(nil)) {
			return errors.New("Checksum mismatch")
		}
		var hash [32]byte
		edgeNodes := BytesToEdgeNodes(buf[:8+32])
		copy(hash[:], edgeNodes[0].Value)
		tree.nodes[edgeNodes[0].Pos] = &hash
	}
	return nil
}

func (tree *Tree) DumpMtree4YT(outfile io.Writer) error {
	h := meow.New32(0)
	for _, buf := range tree.mtree4YoungestTwig[:] {
		h.Write(buf[:])
		_, err := outfile.Write(buf[:])
		if err != nil {
			return err
		}
	}
	_, err := outfile.Write(h.Sum(nil))
	if err != nil {
		return err
	}
	return nil
}

func (tree *Tree) LoadMtree4YT(infile io.Reader) error {
	h := meow.New32(0)
	for i := range tree.mtree4YoungestTwig[:] {
		_, err := infile.Read(tree.mtree4YoungestTwig[i][:])
		if err != nil {
			return err
		}
		h.Write(tree.mtree4YoungestTwig[i][:])
	}
	var buf [4]byte
	_, err := infile.Read(buf[:])
	if err != nil {
		return err
	}
	if !bytes.Equal(buf[:], h.Sum(nil)) {
		return errors.New("Checksum mismatch")
	}
	return nil
}


func (tree *Tree) Sync() {
	tree.entryFile.Sync()
	tree.twigMtFile.Sync()

	twigList := make([]int64, 0, len(tree.activeTwigs))
	for twigID := range tree.activeTwigs {
		twigList = append(twigList, twigID)
	}
	sort.Slice(twigList, func(i, j int) bool {return twigList[i] < twigList[j]})
	twigFile, err := os.OpenFile(filepath.Join(tree.dirName, twigsPath), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0700)
	if err != nil {
		panic(err)
	}
	defer twigFile.Close()
	for _, twigID := range twigList {
		tree.activeTwigs[twigID].Dump(twigID, twigFile)
	}

	nodesFile, err := os.OpenFile(filepath.Join(tree.dirName, nodesPath), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0700)
	if err != nil {
		panic(err)
	}
	defer nodesFile.Close()
	err = tree.DumpNodes(nodesFile)
	if err != nil {
		panic(err)
	}

	mt4ytFile, err := os.OpenFile(filepath.Join(tree.dirName, mtree4YTPath), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0700)
	if err != nil {
		panic(err)
	}
	defer mt4ytFile.Close()
	err = tree.DumpMtree4YT(mt4ytFile)
	if err != nil {
		panic(err)
	}
}

func LoadTree(blockSize int, dirName string) *Tree {
	dirEntry := filepath.Join(dirName, entriesPath)
	entryFile, err := NewEntryFile(blockSize, dirEntry)
	if err != nil {
		panic(err)
	}
	dirTwigMt := filepath.Join(dirName, twigMtPath)
	twigMtFile, err := NewTwigMtFile(blockSize, dirTwigMt)
	if err != nil {
		panic(err)
	}
	tree := &Tree{
		entryFile:  &entryFile,
		twigMtFile: &twigMtFile,
		dirName:    dirName,

		nodes:          make(map[NodePos]*[32]byte),
		activeTwigs:    make(map[int64]*Twig),

		mtree4YTChangeStart: -1,
		mtree4YTChangeEnd:   -1,
		twigsToBeDeleted:    make([]int64, 0, 10),
		touchedPosOf512b:    make(map[int64]struct{}),
		deactivedSNList:     make([]int64, 0, 10),
	}

	twigFile, err := os.Open(filepath.Join(tree.dirName, twigsPath))
	if err != nil {
		panic(err)
	}
	defer twigFile.Close()
	for {
		twigID, twig, err := LoadTwigFromFile(twigFile)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Printf("Twig %d is loaded\n", twigID)
		tree.activeTwigs[twigID] = &twig
		if tree.youngestTwigID < twigID {
			tree.youngestTwigID = twigID
		}
	}

	nodesFile, err := os.Open(filepath.Join(tree.dirName, nodesPath))
	if err != nil {
		panic(err)
	}
	defer nodesFile.Close()
	err = tree.LoadNodes(nodesFile)
	if err != nil {
		panic(err)
	}

	mt4ytFile, err := os.Open(filepath.Join(tree.dirName, mtree4YTPath))
	if err != nil {
		panic(err)
	}
	defer mt4ytFile.Close()
	err = tree.LoadMtree4YT(mt4ytFile)
	if err != nil {
		panic(err)
	}
	return tree
}

func (tree *Tree) RecoverEntry(pos int64, entry *Entry, deactivedSNList []int64, oldestActiveTwigID int64) {
	// deactive some old entry
	for _, sn := range deactivedSNList {
		twigID := sn >> TwigShift
		if twigID >= oldestActiveTwigID {
			tree.DeactiviateEntry(sn)
		}
	}
	//update youngestTwigID
	twigID := entry.SerialNum >> TwigShift
	tree.youngestTwigID = twigID
	// mark this entry as valid
	tree.ActiviateEntry(entry.SerialNum)
	// record ChangeStart/ChangeEnd for endblock sync
	position := int(entry.SerialNum & TwigMask)
	if tree.mtree4YTChangeStart == -1 {
		tree.mtree4YTChangeStart = position
	}
	tree.mtree4YTChangeEnd = position

	// update the corresponding leaf of merkle tree
	bz := EntryToBytes(*entry, deactivedSNList)
	idx := entry.SerialNum & TwigMask
	copy(tree.mtree4YoungestTwig[LeafCountInTwig+idx][:], hash(bz))

	if idx == 0 { // when this is the first entry of current twig
		tree.activeTwigs[twigID].FirstEntryPos = pos
	} else if idx == TwigMask { // when this is the last entry of current twig
		// write the merkle tree of youngest twig to twigMtFile
		tree.syncMT4YoungestTwig()
		// allocate new twig as youngest twig
		tree.youngestTwigID++
		tree.activeTwigs[tree.youngestTwigID] = CopyNullTwig()
		tree.mtree4YoungestTwig = NullMT4Twig
	}
}

func (tree *Tree) ScanEntries(oldestActiveTwigID int64, handler types.EntryHandler) {
	pos := tree.twigMtFile.GetFirstEntryPos(oldestActiveTwigID)
	size := tree.entryFile.Size()
	for pos < size {
		entry, deactivedSNList, nextPos := tree.entryFile.ReadEntry(pos)
		handler(pos, entry, deactivedSNList)
		pos = nextPos
	}
}

func (tree *Tree) RecoverTwigs(oldestActiveTwigID int64) {
	tree.ScanEntries(oldestActiveTwigID, func(pos int64, entry *Entry, deactivedSNList []int64) {
		tree.RecoverEntry(pos, entry, deactivedSNList, oldestActiveTwigID)
	})
	tree.syncMT4YoungestTwig()
	tree.syncMT4ActiveBits()
	tree.touchedPosOf512b = make(map[int64]struct{}) // clear the list
}

func (tree *Tree) RecoverUpperNodes(edgeNodes []*EdgeNode, oldestActiveTwigID int64) {
	for _, edgeNode := range edgeNodes {
		var buf [32]byte
		copy(buf[:], edgeNode.Value)
		tree.nodes[edgeNode.Pos] = &buf
	}
	nList := make([]int64, 0, int(oldestActiveTwigID-tree.youngestTwigID+2)/2)
	for i:= oldestActiveTwigID; i <= tree.youngestTwigID; i++ {
		if len(nList) == 0 || nList[len(nList)-1] != i/2 {
			nList = append(nList, i/2)
		}
	}
	tree.syncUpperNodes(nList)
}

func RecoverTree(blockSize int, dirName string, edgeNodes []*EdgeNode, oldestActiveTwigID, youngestTwigID int64) *Tree {
	dirEntry := filepath.Join(dirName, entriesPath)
	entryFile, err := NewEntryFile(blockSize, dirEntry)
	if err != nil {
		panic(err)
	}
	dirTwigMt := filepath.Join(dirName, twigMtPath)
	twigMtFile, err := NewTwigMtFile(blockSize, dirTwigMt)
	if err != nil {
		panic(err)
	}
	tree := &Tree{
		entryFile:  &entryFile,
		twigMtFile: &twigMtFile,
		dirName:    dirName,

		nodes:          make(map[NodePos]*[32]byte),
		activeTwigs:    make(map[int64]*Twig),
		youngestTwigID: youngestTwigID,

		mtree4YTChangeStart: -1,
		mtree4YTChangeEnd:   -1,
		twigsToBeDeleted:    make([]int64, 0, 10),
		touchedPosOf512b:    make(map[int64]struct{}),
		deactivedSNList:     make([]int64, 0, 10),
	}
	tree.activeTwigs[oldestActiveTwigID] = CopyNullTwig()
	tree.mtree4YoungestTwig = NullMT4Twig
	tree.RecoverTwigs(oldestActiveTwigID)
	tree.RecoverUpperNodes(edgeNodes, oldestActiveTwigID)
	return tree
}

func CompareTreeNodes(treeA, treeB *Tree) {
	nodesA := treeA.nodes
	nodesB := treeB.nodes
	if len(nodesA) != len(nodesB) {
		panic("Different nodes count")
	}
	for pos := range nodesA {
		hashA := nodesA[pos]
		hashB := nodesB[pos]
		if !bytes.Equal(hashA[:], hashB[:]) {
			panic("Different Hash")
		}
	}
}

