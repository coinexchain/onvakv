package metadb

import (
	"encoding/binary"
	"math"

	dbm "github.com/tendermint/tm-db"

	"github.com/coinexchain/onvakv/types"
	"github.com/coinexchain/onvakv/datatree"
)

const (
	ByteCurrHeight            = byte(0x10)
	ByteTwigMtFileSize        = byte(0x11)
	ByteEntryFileSize         = byte(0x12)
	ByteTwigHeight            = byte(0x13)
	ByteLastPrunedTwig        = byte(0x14)
	ByteEdgeNodes             = byte(0x15)
	ByteMaxSerialNum          = byte(0x16)
	ByteActiveEntryCount      = byte(0x17)
	ByteOldestActiveTwigID    = byte(0x18)
)

type MetaDBWithTMDB struct {
	kvdb  dbm.DB
	batch dbm.Batch

	currHeight         int64
	lastPrunedTwig     int64
	maxSerialNum       int64
	oldestActiveTwigID int64
	activeEntryCount   int64
}

var _ types.MetaDB = (*MetaDBWithTMDB)(nil)

func (db *MetaDBWithTMDB) ReloadFromKVDB() {
	bz := db.kvdb.Get([]byte{ByteCurrHeight})
	db.currHeight = int64(binary.LittleEndian.Uint64(bz))

	bz = db.kvdb.Get([]byte{ByteLastPrunedTwig})
	db.lastPrunedTwig = int64(binary.LittleEndian.Uint64(bz))

	bz = db.kvdb.Get([]byte{ByteMaxSerialNum})
	db.maxSerialNum = int64(binary.LittleEndian.Uint64(bz))

	bz = db.kvdb.Get([]byte{ByteOldestActiveTwigID})
	db.oldestActiveTwigID = int64(binary.LittleEndian.Uint64(bz))

	bz = db.kvdb.Get([]byte{ByteActiveEntryCount})
	db.activeEntryCount = int64(binary.LittleEndian.Uint64(bz))
}

func (db *MetaDBWithTMDB) Commit() {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(db.currHeight))
	db.batch.Set([]byte{ByteCurrHeight}, buf[:])

	binary.LittleEndian.PutUint64(buf[:], uint64(db.lastPrunedTwig))
	db.batch.Set([]byte{ByteLastPrunedTwig}, buf[:])

	binary.LittleEndian.PutUint64(buf[:], uint64(db.maxSerialNum))
	db.batch.Set([]byte{ByteMaxSerialNum}, buf[:])

	binary.LittleEndian.PutUint64(buf[:], uint64(db.oldestActiveTwigID))
	db.batch.Set([]byte{ByteOldestActiveTwigID}, buf[:])

	binary.LittleEndian.PutUint64(buf[:], uint64(db.activeEntryCount))
	db.batch.Set([]byte{ByteActiveEntryCount}, buf[:])

	db.batch.WriteSync()
	db.batch = db.kvdb.NewBatch()
}

func (db *MetaDBWithTMDB) SetCurrHeight(h int64) {
	db.currHeight = h
}

func (db *MetaDBWithTMDB) GetCurrHeight() int64 {
	return db.currHeight
}

func (db *MetaDBWithTMDB) SetTwigMtFileSize(size int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(size))
	db.batch.Set([]byte{ByteTwigMtFileSize}, buf[:])
}

func (db *MetaDBWithTMDB) GetTwigMtFileSize() int64 {
	bz := db.kvdb.Get([]byte{ByteTwigMtFileSize})
	return int64(binary.LittleEndian.Uint64(bz))
}

func (db *MetaDBWithTMDB) SetEntryFileSize(size int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(size))
	db.batch.Set([]byte{ByteEntryFileSize}, buf[:])
}

func (db *MetaDBWithTMDB) GetEntryFileSize() int64 {
	bz := db.kvdb.Get([]byte{ByteEntryFileSize})
	return int64(binary.LittleEndian.Uint64(bz))
}

func (db *MetaDBWithTMDB) setTwigHeight(twigID int64, height int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(twigID))
	key := append([]byte{ByteEntryFileSize}, buf[:]...)
	binary.LittleEndian.PutUint64(buf[:], uint64(height))
	db.batch.Set(key, buf[:])
}

func (db *MetaDBWithTMDB) GetTwigHeight(twigID int64) int64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(twigID))
	bz := db.kvdb.Get(append([]byte{ByteEntryFileSize}, buf[:]...))
	if len(bz) == 0 {
		return math.MinInt64
	}
	return int64(binary.LittleEndian.Uint64(bz))
}

func (db *MetaDBWithTMDB) DeleteTwigHeight(twigID int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(twigID))
	db.kvdb.Delete(append([]byte{ByteEntryFileSize}, buf[:]...))
}

func (db *MetaDBWithTMDB) SetLastPrunedTwig(twigID int64) {
	db.lastPrunedTwig = twigID
}

func (db *MetaDBWithTMDB) GetLastPrunedTwig() int64 {
	return db.lastPrunedTwig
}

func (db *MetaDBWithTMDB) GetEdgeNodes() []byte {
	return db.kvdb.Get([]byte{ByteEdgeNodes})
}

func (db *MetaDBWithTMDB) SetEdgeNodes(bz []byte) {
	db.kvdb.Set([]byte{ByteEdgeNodes}, bz)
}

func (db *MetaDBWithTMDB) GetMaxSerialNum() int64 {
	return db.maxSerialNum
}

func (db *MetaDBWithTMDB) IncrMaxSerialNum()  {
	db.maxSerialNum++
	if db.maxSerialNum%datatree.LeafCountInTwig == 0 {
		twigID := db.maxSerialNum/datatree.LeafCountInTwig
		db.setTwigHeight(twigID, db.currHeight)
	}
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

func (db *MetaDBWithTMDB) GetOldestActiveTwigID() int64 {
	return db.oldestActiveTwigID
}

func (db *MetaDBWithTMDB) IncrOldestActiveTwigID() {
	db.oldestActiveTwigID++
}


