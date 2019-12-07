package metadb
//================================================================

type MetaDB interface {
	Commit()
	// Current Height of blockchain, increased automatically in Commit
	GetCurrHeight() int64

	SetTwigHeight(twigID int64, height int64)
	GetTwigHeight(twigID int64) int64
	DeleteTwigHeight(twigID int64)

	// MaxSerialNum is the maximum serial num among all the entries
	SetMaxSerialNum(sn int64)
	GetMaxSerialNum() int64
	IncrMaxSerialNum()
	GetLastMaxSerialNum() int64

	// The lowest height which are not pruned, updated in PruneHeight
	SetLowestNonPrunedHeight(height int64) //TODO
	GetLowestNonPrunedHeight() int64

	// the count of all the active entries, increased in AppendEntry, decreased in DeactiviateEntry
	SetActiveEntryCount(count int64)
	GetActiveEntryCount() int64
	IncrActiveEntryCount()
	DecrActiveEntryCount()

	// the ID of the oldest active twig, increased by ReapOldestActiveTwig
	SetOldestActiveTwigID(id int64)
	GetOldestActiveTwigID() int64
	IncrOldestActiveTwigID()
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

type MetaDBWithTMDB struct {
	kvdb  dbm.DB
	batch dbm.Batch

	currHeight            int64
	maxSerialNum          int64
	lastMaxSerialNum      int64
	oldestLimb            int64
	lowestNonPrunedHeight int64
	activeEntryCount      int64
	fileOffset4NextEntry  int64
	oldestActiveTwigID    int64
	deactivations         []int64
}

func encodeInt64Slice(slice []int64) []byte {
	res := make([]byte, 0, binary.MaxVarintLen64*(1+len(slice)))
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], int64(len(slice)))
	res = append(res, buf[:n]...)
	for _, value := range slice {
		n = binary.PutVarint(buf[:], value)
		res = append(res, buf[:n]...)
	}
	return res
}

func decodeInt64Slice(bz []byte, slicePtr *[]int64) {
	length, n := binary.Varint(bz)
	if n <= 0 {
		panic("Fail in decoding")
	}
	bz = bz[n:]
	for i := 0; i < int(length); i++ {
		value, n := binary.Varint(bz)
		if n <= 0 {
			panic("Fail in decoding")
		}
		*slicePtr = append(*slicePtr, value)
	}
}

func (db *MetaDBWithTMDB) ReloadFromKVDB() {
	bz := db.kvdb.Get([]byte{ByteMeta, ByteCurrHeight})
	db.currHeight = int64(binary.BigEndian.Uint64(bz))

	bz = db.kvdb.Get([]byte{ByteMeta, ByteMaxSerialNum})
	db.maxSerialNum = int64(binary.BigEndian.Uint64(bz))

	bz = db.kvdb.Get([]byte{ByteMeta, ByteLastMaxSerialNum})
	db.lastMaxSerialNum = int64(binary.BigEndian.Uint64(bz))

	bz = db.kvdb.Get([]byte{ByteMeta, ByteLowestNonPrunedHeight})
	db.lowestNonPrunedHeight = int64(binary.BigEndian.Uint64(bz))

	bz = db.kvdb.Get([]byte{ByteMeta, ByteActiveEntryCount})
	db.activeEntryCount = int64(binary.BigEndian.Uint64(bz))

	bz = db.kvdb.Get([]byte{ByteMeta, ByteFileOffset4NextEntry})
	db.fileOffset4NextEntry = int64(binary.BigEndian.Uint64(bz))

	bz = db.kvdb.Get([]byte{ByteMeta, ByteOldestActiveTwigID})
	db.oldestActiveTwigID = int64(binary.BigEndian.Uint64(bz))
}

func (db *MetaDBWithTMDB) Commit() {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(db.currHeight))
	db.batch.Set([]byte{ByteMeta, ByteCurrHeight}, buf[:])

	key := append([]byte{ByteMeta, ByteDeactivation}, buf[:]...)
	db.batch.Set(key, encodeInt64Slice(db.deactivations))
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

func (db *MetaDBWithTMDB) GetCurrHeight() int64 {
	return db.currHeight
}
func (db *MetaDBWithTMDB) SetLimbHeight(limbID int64, height int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(limbID))
	key := append([]byte{ByteMeta, ByteLimbHeight}, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(height))
	db.batch.Set(key, buf[:])
}
func (db *MetaDBWithTMDB) GetLimbHeight(limbID int64) int64 {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(limbID))
	key := append([]byte{ByteMeta, ByteLimbHeight}, buf[:]...)
	bz := db.kvdb.Get(key)
	if len(bz) == 0 {
		panic("Get failed")
	}
	return int64(binary.BigEndian.Uint64(bz))
}
func (db *MetaDBWithTMDB) DeleteLimbHeight(limbID int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(limbID))
	key := append([]byte{ByteMeta, ByteLimbHeight}, buf[:]...)
	db.batch.Delete(key)
}
func (db *MetaDBWithTMDB) SetMaxSerialNum(sn int64) {
	db.maxSerialNum = sn
}
func (db *MetaDBWithTMDB) GetMaxSerialNum() int64 {
	return db.maxSerialNum
}
func (db *MetaDBWithTMDB) IncrMaxSerialNum() {
	db.maxSerialNum++
}
func (db *MetaDBWithTMDB) GetLastMaxSerialNum() int64 {
	return db.lastMaxSerialNum
}
func (db *MetaDBWithTMDB) SetOldestLimb(limbID int64) {
	db.oldestLimb = limbID
}
func (db *MetaDBWithTMDB) GetOldestLimb() int64 {
	return db.oldestLimb
}
func (db *MetaDBWithTMDB) IncrOldestLimb() {
	db.oldestLimb++
}
func (db *MetaDBWithTMDB) SetLowestNonPrunedHeight(height int64) {
	db.lowestNonPrunedHeight = height
}
func (db *MetaDBWithTMDB) GetLowestNonPrunedHeight() int64 {
	return db.lowestNonPrunedHeight
}
func (db *MetaDBWithTMDB) SetActiveEntryCount(count int64) {
	db.activeEntryCount = count
}
func (db *MetaDBWithTMDB) GetActiveEntryCount() int64 {
	return db.activeEntryCount
}
func (db *MetaDBWithTMDB) IncrActiveEntryCount() {
	db.activeEntryCount++
}
func (db *MetaDBWithTMDB) DecrActiveEntryCount() {
	db.activeEntryCount--
}
func (db *MetaDBWithTMDB) SetFileOffset4NextEntry(offset int64) {
	db.fileOffset4NextEntry = offset
}
func (db *MetaDBWithTMDB) GetFileOffset4NextEntry() int64 {
	return db.fileOffset4NextEntry
}
func (db *MetaDBWithTMDB) SetOldestActiveTwigID(id int64) {
	db.oldestActiveTwigID = id
}
func (db *MetaDBWithTMDB) GetOldestActiveTwigID() int64 {
	return db.oldestActiveTwigID
}
func (db *MetaDBWithTMDB) IncrOldestActiveTwigID() {
	db.oldestActiveTwigID++
}
func (db *MetaDBWithTMDB) EnqueueStartPosOfTwig(twigID int64, filePos int64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(twigID))
	key := append([]byte{ByteMeta, ByteStartPosOfTwig}, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], uint64(filePos))
	db.batch.Set(key, buf[:])
}
func (db *MetaDBWithTMDB) DequeueStartPosOfTwig(twigID int64) int64 {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(twigID))
	key := append([]byte{ByteMeta, ByteStartPosOfTwig}, buf[:]...)
	bz := db.kvdb.Get(key)
	if len(bz) == 0 {
		panic("Get failed")
	}
	res := int64(binary.BigEndian.Uint64(bz))
	db.batch.Delete(key)
	return res
}
func (db *MetaDBWithTMDB) RecordDeactivationAtCurrHeight(sn int64) {
	db.deactivations = append(db.deactivations, sn)
}
func (db *MetaDBWithTMDB) GetDeactivationAtCurrHeight() []int64 {
	return db.deactivations
}
//func (db *MetaDBWithTMDB) GetDeactivationRecordAfterHeight(height int64) []int64 {
//	//TODO
//}
//func (db *MetaDBWithTMDB) RemoveDeactivationRecordBeforeHeight(height int64) {
//	//TODO
//}

// ======== @fileStore ========

// ======== @merkle ========

// ======== @init ========

// ======== @lookup ========

// ======== @top-level ========
