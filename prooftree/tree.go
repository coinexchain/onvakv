package tree

const (
	LeafShift int64 = 11
	LeafCount int64 = 1<<LeafShift
	LeafMask int64 = LeafCount-1
)

var (
	middleShift int64 = 11
	middleMask int64 = (1<<middleShift)-1
	middleAndLeafShift int64 = middleShift + LeafShift
	middleAndLeafMask int64 = (1<<middleAndLeafShift)-1
)

type Entry struct {
	Namespace  []byte
	Key        []byte
	Value      []byte
	NextKey    []byte
	Height     int64
	LastHeight int64
	SerialNum  int64
}

//dump to file, re-cacalculate when dump file is crashed
type YoungestTwig struct {
	activeBits   [256]byte
	activeBitsMT [16][]byte
	leafMT      [4096][]byte
}

func (twig *YoungestTwig) syncLeafMT(lastMaxSerialNum int64) {
//TODO
}

func (twig *YoungestTwig) clear() {
//TODO
}
func (twig *YoungestTwig) setBit(offset int64) {
	if offset<0 || offset>LeafCount {
		panic("Invalid ID")
	}
	mask := byte(1)<<(offset&0x7)
	pos := int(offset>>3)
	twig.activeBits[pos] |= mask
}

type Twig struct {
	activeBits   [256]byte
	activeBitsMT [16][]byte
	leafMTRoot  []byte
}

func (twig *Twig) clearBit(offset int64) {
	if offset<0 || offset>LeafCount {
		panic("Invalid ID")
	}
	mask := byte(1)<<(offset&0x7)
	pos := int(offset>>3)
	twig.activeBits[pos] &= ^mask
}
func (twig *Twig) getBit(offset int64) bool {
	if offset<0 || offset>LeafCount {
		panic("Invalid ID")
	}
	mask := byte(1)<<(offset&0x7)
	pos := int(offset>>3)
	return (twig.activeBits[pos] & mask) != 0
}

type Limb struct {
	twigs   []Twig // should have 1<<middleShift entries
	middleMT  [][]byte // should have 2<<middleShift entries
}

// Limb archive file structure: 2048*4096 hashes for Twigs, 2048 hashes for Limb, Entries

func EmptyLimb() *Limb {
//TODO
	return nil
}

type NodePos struct {
	Level  int
	Nth    int
}

type Tree struct {
	db         DBStore
	fs         FileStore

	// the contents of nodes, activeLimbs and youngestTwig are saved to disk when exiting
	// and loaded from disk during re-starting.
	// if the program crashes, these contents can be re-computed from the information in db and fs
	// But the re-computing process is lengthy.

	// the nodes in high level tree (higher than limb)
	nodes             map[NodePos][]byte

	// contains all the limbs which have active twigs
	// remove old limb in syncMT, add new limb in allocateNewLimb
	activeLimbs       map[int64]*Limb

	// The Youngest Twig, AppendEntry add new entries to it
	// flushYoungestTwig flushes it to file and clears its content
	youngestTwig      YoungestTwig

	// -------------

	// the contents of deactivatedLimbs will be cleared at each commits
	// so we need not to save the contents to file when exiting

	// Record the limbs to be removed
	// ReapOldestActiveTwig fill this slice
	// Commit use this slice to update activeLimbs and run RemoveDeactivationRecordBeforeHeight
	deactivatedLimbs        []int64
}

type DBStore interface {
	Commit()
	// Current Height of blockchain, increased automatically in Commit
	GetCurrHeight() int64

	// contains the highest-reached heights of the limbs which are not pruned
	// remove old entries in PruneHeight, add&update in AppendEntry
	SetLimbHeight(limbID int64, height int64)
	GetLimbHeight(limbID int64) int64
	DeleteLimbHeight(limbID int64)

	// MaxSerialNum is the maximum serial num among all the entries
	SetMaxSerialNum(sn int64)
	GetMaxSerialNum() int64
	IncrMaxSerialNum()
	GetLastMaxSerialNum() int64

	// OldestLimb is the id of the oldest active limb
	SetOldestLimb(limbID int64)
	GetOldestLimb() int64
	IncrOldestLimb()

	// The lowest height which are not pruned, updated in PruneHeight
	SetLowestNonPrunedHeight(height int64)
	GetLowestNonPrunedHeight() int64

	// the count of all the active entries, increased in AppendEntry, decreased in DeactiviateEntry
	SetActiveEntryCount(count int64)
	GetActiveEntryCount() int64
	IncrActiveEntryCount()
	DecrActiveEntryCount()

	// the offset within a limb file, where a new entry can be appended
	// allocateNewLimb clears it to 0, AppendEntry updates it
	SetFileOffset4NextEntry(offset int64)
	GetFileOffset4NextEntry() int64

	// the ID of the oldest active twig, increased by ReapOldestActiveTwig
	SetOldestActiveTwigID(id int64)
	GetOldestActiveTwigID() int64
	IncrOldestActiveTwigID() int64

	// the file offset of the first entry inside a twig
	// flushYoungestTwig adds new entries and ReapOldestActiveTwig deletes old entries
	EnqueueStartPosOfTwig(twigID int64, filePos int64)
	DequeueStartPosOfTwig(twigID int64) int64

	// DeactiviateEntry use it to record deactivated entries at each height
	RecordDeactivationAtCurrHeight(sn int64)
	// syncMT use this function to get the IDs of the entries which are deactivated in current block
	GetDeactivationAtCurrHeight() []int64
	// Returns the deactivation records to re-build twigs' activeBits
	GetDeactivationRecordAfterHeight(height int64) []int64
	// syncMT use it to remove useless records, when removing useless limbs
	RemoveDeactivationRecordBeforeHeight(height int64)
}

type FileStore interface {
	// write the leaf-level merkle tree in a twig into the limb archive file
	WriteTwigMT(twigID int64, mt *[4096][]byte)
	// write the middle-level merkle tree in a limb into the limb archive file
	WriteLimbMT(limbID int64, mt *[][]byte)
	// delete a limb archive file
	DeleteLimbArchive(limbID int64, db DBStore)
	// write a new entry into the limb archive file at a given offset, returns the offset for the next entry
	WriteEntry(entry *Entry, offset int64) (nextOffset int64)
	// Given a limb archive file whose id is limbID, read at moust count entries out, from offset
	ReadEntriesInLimb(limbID int64, offset int64, count int64) ([]*Entry, error)
	// Given a limb archive file whose id is limbID, read one entry out at offset
	ReadEntryInLimb(limbID int64, offset int64) (*Entry, error)
}

func (tree *Tree) youngestActiveTwigID() int64 {
	return tree.db.GetMaxSerialNum()>>LeafShift
}

func (tree *Tree) allocateNewLimb() {
	currYoungestLimbID := tree.db.GetMaxSerialNum()>>middleAndLeafShift
	// Allocate a new empty limb
	tree.activeLimbs[currYoungestLimbID] = EmptyLimb()
	// Since we switched to a new limb archive file, the offset is reset to zero
	tree.db.SetFileOffset4NextEntry(0)
}

func (tree *Tree) flushYoungestTwig() {
	// record the start position for later use by ReapOldestActiveTwig
	currYoungestTwigID := tree.db.GetMaxSerialNum()>>LeafShift
	tree.db.EnqueueStartPosOfTwig(currYoungestTwigID, tree.db.GetFileOffset4NextEntry())

	// write the leafMT of the last youngest twig to limb archive file
	tree.youngestTwig.syncLeafMT(tree.db.GetLastMaxSerialNum())
	lastYoungestTwigID := currYoungestTwigID - 1
	tree.fs.WriteTwigMT(lastYoungestTwigID, &tree.youngestTwig.leafMT)

	// Copy the activeBits and MTRoot from youngestTwig to a normal twig
	twig := tree.getTwig(lastYoungestTwigID)
	copy(twig.activeBits[:], tree.youngestTwig.activeBits[:])
	copy(twig.activeBitsMT[:], tree.youngestTwig.activeBitsMT[:])
	twig.leafMTRoot = tree.youngestTwig.leafMT[1]

	// make the youngestTwig empty for later use
	tree.youngestTwig.clear()
}

func (tree *Tree) getTwig(twigID int64) *Twig {
	if twigID < tree.db.GetOldestActiveTwigID() || twigID > tree.youngestActiveTwigID() {
		panic("Invalid Twig Serial Number")
	}
	limb, ok := tree.activeLimbs[twigID >> middleShift]
	if !ok {
		panic("Invalid Limb ID")
	}
	return &(limb.twigs[twigID & middleMask])
}

func (tree *Tree) syncMT() {
}

// ==========================================================================================

func (tree *Tree) DeactiviateEntry(sn int64) {
	twigID := sn>>LeafShift
	tree.getTwig(twigID).clearBit(sn&LeafMask)
	tree.db.DecrActiveEntryCount()
	// record this entry in db
	tree.db.RecordDeactivationAtCurrHeight(sn)
}

func (tree *Tree) AppendEntry(entry *Entry) {
	// mark the entry as activated
	entry.SerialNum = tree.db.GetMaxSerialNum()
	tree.youngestTwig.setBit(entry.SerialNum & LeafMask)
	tree.db.IncrMaxSerialNum()
	tree.db.IncrActiveEntryCount()

	// update limb's height
	tree.db.SetLimbHeight(tree.db.GetMaxSerialNum()>>middleAndLeafShift, tree.db.GetCurrHeight())

	// write the entry to the limb archive file
	newOffset := tree.fs.WriteEntry(entry, tree.db.GetFileOffset4NextEntry())
	tree.db.SetFileOffset4NextEntry(newOffset)

	// when MaxSerialNum is large enough for a new twig or a new limb, allocate it!
	if (tree.db.GetMaxSerialNum() & middleAndLeafMask) == 0 {
		tree.flushYoungestTwig()
		tree.allocateNewLimb()
	} else if (tree.db.GetMaxSerialNum() & LeafMask) == 0 {
		tree.flushYoungestTwig()
	}
}

// Read all the entries out from the oldest active twig, return the active entries and then
// mark all the entries in this twig as deactivated
func (tree *Tree) ReapOldestActiveTwig() []*Entry {
	// Read all the entries out
	oldestActiveTwigID := tree.db.GetOldestActiveTwigID()
	tree.db.IncrOldestActiveTwigID()
	limbID := oldestActiveTwigID>>middleShift
	filePos := tree.db.DequeueStartPosOfTwig(oldestActiveTwigID)
	entries, err := tree.fs.ReadEntriesInLimb(limbID, filePos, LeafCount)
	if err!=nil {
		panic(err)
	}

	// return the active entries and then deactivate them
	entriesFiltered := make([]*Entry, 0, LeafCount)
	twig := tree.getTwig(tree.db.GetOldestActiveTwigID())
	for i, entry := range entries {
		if twig.getBit(int64(i)) {
			entriesFiltered = append(entriesFiltered, entry)
			tree.DeactiviateEntry(entry.SerialNum)
		}
	}

	// If all the twigs in a limb have been deactivated, record this limb
	if (oldestActiveTwigID&middleMask) == 0 {
		tree.deactivatedLimbs = append(tree.deactivatedLimbs, (oldestActiveTwigID>>middleShift)-1)
	}
	return entriesFiltered
}

func (tree *Tree) PruneHeight(height int64) bool {
	oldestActiveLimbID := tree.db.GetOldestActiveTwigID()>>middleShift
	if height >= tree.db.GetLimbHeight(oldestActiveLimbID) {
		return false // can not prune
	}
	if height < tree.db.GetLowestNonPrunedHeight() {
		return true // have already pruned
	}
	tree.db.SetLowestNonPrunedHeight(height + 1)

	// prune the limbs whose heights are small enough
	heightOfOldestLimb := tree.db.GetLimbHeight(tree.db.GetOldestLimb())
	for heightOfOldestLimb < height {
		tree.db.DeleteLimbHeight(tree.db.GetOldestLimb())
		tree.fs.DeleteLimbArchive(tree.db.GetOldestLimb(), tree.db)
		tree.db.IncrOldestLimb()
		heightOfOldestLimb = tree.db.GetLimbHeight(tree.db.GetOldestLimb())
	}
	tree.pruneNodesBeforeLimb(tree.db.GetOldestLimb())
	return true
}

// remove the useless nodes if the limbs whose IDs are less than limbID are pruned
func (tree *Tree) pruneNodesBeforeLimb(limbID int64) {
	//TODO
}

func (tree *Tree) ActiveTwigCount() int64 {
	return tree.youngestActiveTwigID() - tree.db.GetOldestActiveTwigID()+ 1
}

func (tree *Tree) GetMaxSerialNum() int64 {
	return tree.db.GetMaxSerialNum()
}

func (tree *Tree) ActiveEntryCount() int64 {
	return tree.db.GetActiveEntryCount()
}

func (tree *Tree) Commit(height int64) {
	tree.syncMT()
	for _, limbID := range tree.deactivatedLimbs {
		// write the inactiveLimb's middleMT to limb archive file
		tree.fs.WriteLimbMT(limbID, &(tree.activeLimbs[limbID].middleMT))
		// free the memory occupied by the inactive limb
		delete(tree.activeLimbs, limbID)
		// remove the deactivation records which are no longer useful in re-compute activeBits
		height := tree.db.GetLimbHeight(limbID)
		tree.db.RemoveDeactivationRecordBeforeHeight(height)
	}
	tree.deactivatedLimbs = tree.deactivatedLimbs[:0]
	tree.db.Commit()
}

// ======== @dbStore ========

const (
	ByteMeta                  = byte(0)
	ByteCurrHeight            = byte(0x10)
	ByteLimbHeight            = byte(0x11)
	ByteMaxSerialNum          = byte(0x12)
	ByteLastMaxSerialNum      = byte(0x13)
	ByteOldestLimb            = byte(0x14)
	ByteLowestNonPrunedHeight = byte(0x15)
	ByteActiveEntryCount      = byte(0x16)
	ByteFileOffset4NextEntry  = byte(0x17)
	ByteOldestActiveTwigID    = byte(0x18)
	ByteStartPosOfTwig        = byte(0x19)
	ByteDeactivation          = byte(0x1A)
)

type DBStoreWithTMDB interface {
	kvdb     db.DB
	batch    db.Batch

	currHeight int64
	maxSerialNum int64
	lastMaxSerialNum int64
	oldestLimb int64
	lowestNonPrunedHeight int64
	activeEntryCount int64
	fileOffset4NextEntry int64
	oldestActiveTwigID int64
	deactivations []int64
}

func encodeInt64Slice(slice []int64) []byte {
	res = make([]byte, 0, binary.MaxVarintLen64*(1+len(slice)))
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], len(slice))
	res = append(res, buf[:n]...)
	for _, value := range slice {
		n = binary.PutVarint(buf[:], value)
		res = append(res, buf[:n]...)
	}
	return res
}

func decodeInt64Slice(bz []byte, slicePtr *[]int64) {
	length, n := binary.Varint(bz)
	if n<=0 {
		panic("Fail in decoding")
	}
	bz = bz[n:]
	for i:=0; i<length; i++ {
		value, n := binary.Varint(bz)
		if n<=0 {
			panic("Fail in decoding")
		}
		*slicePtr = append(*slicePtr, value)
	}
}

func (db *DBStoreWithTMDB) ReloadFromKVDB() {
	bz := db.batch.Get([]byte{ByteMeta, ByteCurrHeight})
	db.currHeight = int64(binary.BigEndian.Uint64(bz))

	bz = db.batch.Get([]byte{ByteMeta, ByteMaxSerialNum})
	db.maxSerialNum = int64(binary.BigEndian.Uint64(bz))

	bz = db.batch.Get([]byte{ByteMeta, ByteLastMaxSerialNum})
	db.lastMaxSerialNum = int64(binary.BigEndian.Uint64(bz))

	bz = db.batch.Get([]byte{ByteMeta, ByteLowestNonPrunedHeight})
	db.lowestNonPrunedHeight = int64(binary.BigEndian.Uint64(bz))

	bz = db.batch.Get([]byte{ByteMeta, ByteActiveEntryCount})
	db.activeEntryCount = int64(binary.BigEndian.Uint64(bz))

	bz = db.batch.Get([]byte{ByteMeta, ByteFileOffset4NextEntry})
	db.fileOffset4NextEntry = int64(binary.BigEndian.Uint64(bz))

	bz = db.batch.Get([]byte{ByteMeta, ByteOldestActiveTwigID})
	db.oldestActiveTwigID = int64(binary.BigEndian.Uint64(bz))
}

func (db *DBStoreWithTMDB) Commit() {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(db.currHeight))
	db.batch.Set([]byte{ByteMeta, ByteCurrHeight}, buf[:])

	key := append([]byte{ByteMeta, ByteDeactivation}, buf[:]...)
	db.batch.Set(key, encodeInt64Slice(db.deactivations)
	db.deactivations = db.deactivations[:0]

	binary.BigEndian.PutUint64(buf[:], uint64(db.maxSerialNum))
	db.batch.Set([]byte{ByteMeta, ByteMaxSerialNum}, buf[:])

	binary.BigEndian.PutUint64(buf[:], uint64(db.lastMaxSerialNum))
	db.batch.Set([]byte{ByteMeta, ByteLastMaxSerialNum}, buf[:])
	db.lastMaxSerialNum = db.maxSerialNum

	binary.BigEndian.PutUint64(buf[:], uint64(db.lowestNonPrunedHeight))
	db.batch.Set([]byte{ByteMeta, ByteLowestNonPrunedHeight}, buf[:])

	binary.BigEndian.PutUint64(buf[:], uint64(db.activeEntryCount))
	db.batch.Set([]byte{ByteMeta, ByteActiveEntryCount}, buf[:])

	binary.BigEndian.PutUint64(buf[:], uint64(db.fileOffset4NextEntry))
	db.batch.Set([]byte{ByteMeta, ByteFileOffset4NextEntry}, buf[:])

	binary.BigEndian.PutUint64(buf[:], uint64(db.oldestActiveTwigID))
	db.batch.Set([]byte{ByteMeta, ByteOldestActiveTwigID}, buf[:])

	db.batch.WriteSync()
	db.batch = db.kvdb.NewBatch()
}

func (db *DBStoreWithTMDB) GetCurrHeight() int64 {
	return db.currHeight
}
func (db *DBStoreWithTMDB) SetLimbHeight(limbID int64, height int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(limbID))
	key := append([]byte{ByteMeta, ByteLimbHeight}, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(height))
	db.batch.Set(key, buf[:])
}
func (db *DBStoreWithTMDB) GetLimbHeight(limbID int64) int64 {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(limbID))
	key := append([]byte{ByteMeta, ByteLimbHeight}, buf[:]...)
	bz := db.batch.Get(key)
	if len(bz) == 0 {
		panic("Get failed")
	}
	return int64(binary.BigEndian.Uint64(bz))
}
func (db *DBStoreWithTMDB) DeleteLimbHeight(limbID int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(limbID))
	key := append([]byte{ByteMeta, ByteLimbHeight}, buf[:]...)
	db.batch.Delete(key)
}
func (db *DBStoreWithTMDB) SetMaxSerialNum(sn int64) {
	db.maxSerialNum = sn
}
func (db *DBStoreWithTMDB) GetMaxSerialNum() int64 {
	return db.maxSerialNum
}
func (db *DBStoreWithTMDB) IncrMaxSerialNum() {
	db.maxSerialNum++
}
func (db *DBStoreWithTMDB) GetLastMaxSerialNum() int64 {
	return db.lastMaxSerialNum
}
func (db *DBStoreWithTMDB) SetOldestLimb(limbID int64) {
	db.oldestLimb = limbID
}
func (db *DBStoreWithTMDB) GetOldestLimb() int64 {
	return db.oldestLimb
}
func (db *DBStoreWithTMDB) IncrOldestLimb() {
	db.oldestLimb++
}
func (db *DBStoreWithTMDB) SetLowestNonPrunedHeight(height int64) {
	db.lowestNonPrunedHeight = height
}
func (db *DBStoreWithTMDB) GetLowestNonPrunedHeight() int64 {
	return db.lowestNonPrunedHeight
}
func (db *DBStoreWithTMDB) SetActiveEntryCount(count int64) {
	db.activeEntryCount = count
}
func (db *DBStoreWithTMDB) GetActiveEntryCount() int64 {
	return db.activeEntryCount
}
func (db *DBStoreWithTMDB) IncrActiveEntryCount() {
	db.activeEntryCount++
}
func (db *DBStoreWithTMDB) DecrActiveEntryCount() {
	db.activeEntryCount--
}
func (db *DBStoreWithTMDB) SetFileOffset4NextEntry(offset int64) {
	db.fileOffset4NextEntry = offset
}
func (db *DBStoreWithTMDB) GetFileOffset4NextEntry() int64 {
	return db.fileOffset4NextEntry
}
func (db *DBStoreWithTMDB) SetOldestActiveTwigID(id int64) {
	db.oldestActiveTwigID = id
}
func (db *DBStoreWithTMDB) GetOldestActiveTwigID() int64 {
	return db.oldestActiveTwigID
}
func (db *DBStoreWithTMDB) IncrOldestActiveTwigID() int64 {
	db.oldestActiveTwigID++
}
func (db *DBStoreWithTMDB) EnqueueStartPosOfTwig(twigID int64, filePos int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(twigID))
	key := append([]byte{ByteMeta, ByteStartPosOfTwig}, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(filePos))
	db.batch.Set(key, buf[:])
}
func (db *DBStoreWithTMDB) DequeueStartPosOfTwig(twigID int64) int64 {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(twigID))
	key := append([]byte{ByteMeta, ByteStartPosOfTwig}, buf[:]...)
	bz := db.batch.Get(key)
	if len(bz) == 0 {
		panic("Get failed")
	}
	res := int64(binary.BigEndian.Uint64(bz))
	db.batch.Delete(key)
	return res
}
func (db *DBStoreWithTMDB) RecordDeactivationAtCurrHeight(sn int64) {
	db.deactivations = append(deactivations, sn)
}
func (db *DBStoreWithTMDB) GetDeactivationAtCurrHeight() []int64 {
	return db.deactivations
}
func (db *DBStoreWithTMDB) GetDeactivationRecordAfterHeight(height int64) []int64 {
	//TODO
}
func (db *DBStoreWithTMDB) RemoveDeactivationRecordBeforeHeight(height int64) {
	//TODO
}

// ======== @fileStore ========

// ======== @merkle ========

// ======== @init ========

// ======== @lookup ========

// ======== @top-level ========
