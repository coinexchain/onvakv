package top

const StartReapThres int64 = 1000 * 1000

type TreeI interface {
	DeactiviateEntry(sn int64)
	AppendEntry(entry *Entry) uint64
	ReadEntry(pos uint64) *Entry
	GetActiveBit(sn int64) bool
	DeleteActiveTwig(twigID int64)
	GetActiveEntriesInTwig(twigID int64) []*Entry
	PruneTwigsBefore(twigID int64) error
	EndBlock()
}

type MetaDB interface {
	Commit()
	SetCurrHeight(h int64)
	GetCurrHeight() int64

	SetTwigFileSize(size int64)
	GetTwigFileSize() int64

	SetEntryFileSize(size int64)
	GetEntryFileSize() int64

	SetTwigOfHeight(twigID int64, height int64)
	GetTwigOfHeight(twigID int64) int64
	DeleteTwigOfHeight(twigID int64)

	// MaxSerialNum is the maximum serial num among all the entries
	SetMaxSerialNum(sn int64)
	GetMaxSerialNum() int64
	IncrMaxSerialNum()

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

type OnvaKV struct {
	db      MetaDB
	idxTree NVTree
	datTree TreeI
}

func appendHeight(k []byte, h int64) []byte {
	buf := make([]byte, len(k)+8)
	copy(buf, k)
	binary.LittleEndian.PutUint64(buf[len(k):], uint64(h))
	return buf
}

func (okv *OnvaKV) getPos(k []byte, h int64) (uint64, bool) {
	buf := appendHeight(k, h)
	iter := okv.idxTree.ReverseIterator([]byte{}, buf)
	ik := iter.Key()
	pos := iter.Value()
	if len(ik)-8 != len(k) || !bytes.Equal(ik[:len(k)], k) {
		return 0, false // not found
	}
	return pos, true
}

func (okv *OnvaKV) GetEntry(k []byte) *Entry {
	h := okv.db.GetCurrHeight()
	pos, ok := okv.getPos(k, h)
	if !ok {
		return nil
	}
	e := okv.datTree.ReadEntry(pos)
	if !okv.idxTree.GetActiveBit(e.SerialNum) {
		return nil // is not active
	}
	return e
}

//type Entry struct {
//	Key        []byte
//	Value      []byte
//	NextKey    []byte
//	Height     int64
//	LastHeight int64
//	SerialNum  int64
//}

func (okv *OnvaKV) Update(e *Entry, v []byte) {
	okv.DeactiviateEntry(e.SerialNum)
	e.LastHeight = e.Height
	e.Height = okv.db.GetCurrHeight()
	e.SerialNum = okv.db.GetMaxSerialNum()
	okv.db.IncrMaxSerialNum()
	pos := okv.datTree.AppendEntry(e)
	okv.idxTree.Set(appendHeight(e.Key, e.Height), pos)
}

func (okv *OnvaKV) Insert(prev *Entry, k []byte, v []byte) {
	curr := &Entry{
		Key:        k,
		Value:      v,
		NextKey:    prev.NextKey,
		Height:     okv.db.GetCurrHeight(),
		LastHeight: -1,
		SerialNum:  okv.db.GetMaxSerialNum(),
	}
	okv.db.IncrMaxSerialNum()
	pos := okv.datTree.AppendEntry(prev)
	okv.idxTree.Set(appendHeight(curr.Key, curr.Height), pos)

	okv.DeactiviateEntry(prev.SerialNum)
	prev.LastHeight = prev.Height
	prev.Height = okv.db.GetCurrHeight()
	prev.SerialNum = okv.db.GetMaxSerialNum()
	prev.NextKey = k
	okv.db.IncrMaxSerialNum()
	pos := okv.datTree.AppendEntry(prev)
	okv.idxTree.Set(appendHeight(prev.Key, prev.Height), pos)

	okv.db.IncrActiveEntryCount()
}

func (okv *OnvaKV) Delete(prev *Entry, curr *Entry) {
	okv.DeactiviateEntry(curr.SerialNum)
	okv.DeactiviateEntry(prev.SerialNum)
	prev.LastHeight = prev.Height
	prev.Height = okv.db.GetCurrHeight()
	prev.SerialNum = okv.db.GetMaxSerialNum()
	prev.NextKey = curr.NextKey
	okv.db.IncrMaxSerialNum()
	pos := okv.datTree.AppendEntry(prev)
	okv.idxTree.Set(appendHeight(prev.Key, prev.Height), pos)

	okv.db.DecrActiveEntryCount()
}

func (okv *OnvaKV) NumOfKeptEntries() {
	return okv.db.GetMaxSerialNum() - okv.db.GetOldestActiveTwigID()*LeafCountInTwig
}

func (okv *OnvaKV) EndWrite(height int64) {
	okv.db.SetCurrHeight(height)
	for okv.NumOfKeptEntries > okv.db.GetActiveEntryCount()*3 && okv.db.GetActiveEntryCount() > StartReapThres {
		twigID := okv.db.GetOldestActiveTwigID()
		entries := okv.datTree.GetActiveEntriesInTwig(twigID)
		for _, e := range entries {
			okv.DeactiviateEntry(e.SerialNum)
			e.SerialNum = okv.db.GetMaxSerialNum()
			okv.db.IncrMaxSerialNum()
			pos := okv.datTree.AppendEntry(prev)
			okv.idxTree.Set(appendHeight(e.Key, e.Height), pos)
		}
		okv.datTree.DeleteActiveTwig(twigID)
		okv.db.IncrOldestActiveTwigID()
	}
	okv.idxTree.EndWrite(height)
}

//type Iterator interface {
//	Domain() (start []byte, end []byte)
//	Valid() bool
//	Next()
//	Key() []byte
//	Value() uint64
//	Close()
//}
//// Non-volatile tree
//type NVTree interface {
//	Init(dirname string, repFn func(string)) error
//	BeginWrite()
//	EndWrite(height int64)
//	Iterator(start, end []byte) Iterator
//	ReverseIterator(start, end []byte) Iterator
//	Get(k []byte) uint64
//	Set(k []byte, v uint64)
//	Delete(k []byte)
//}
