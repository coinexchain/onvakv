package datatree

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

//func (ef *EntryFile) SkipEntry(off int64) int64 {
//func (ef *EntryFile) ReadEntry(off int64) (*Entry, []uint64, int64) {
//func NewEntryFile(blockSize int, dirName string) (res EntryFile, err error) {
//func (ef *EntryFile) GetActiveEntriesInTwig(twig *Twig) (res []*Entry) {


func makeEntries() []Entry {
	e0 := Entry{
		Key:        []byte("Key0Key0Key0Key0Key0Key0Key0Key0Key0"),
		Value:      []byte("Value0Value0Value0Value0Value0Value0"),
		NextKey:    []byte("NextKey0NextKey0NextKey0NextKey0NextKey0NextKey0"),
		Height:     0,
		LastHeight: 0,
		SerialNum:  0,
	}
	e1 := Entry{
		Key:        []byte("Key1Key ILOVEYOU 1Key1Key1"),
		Value:      []byte("Value1Value1"),
		NextKey:    []byte("NextKey1NextKey1NextKey1"),
		Height:     10,
		LastHeight: 3,
		SerialNum:  1,
	}
	e2 := Entry{
		Key:        []byte("Key2Key2Key2 ILOVEYOU Key2"),
		Value:      []byte("Value2 ILOVEYOU Value2"),
		NextKey:    []byte("NextKey2NextKey2 ILOVEYOU NextKey2"),
		Height:     20,
		LastHeight: 12,
		SerialNum:  2,
	}
	return []Entry{e0,e1,e2,NullEntry()}
}

func TestEntryFile(t *testing.T) {
	os.RemoveAll("./entryF")
	os.Mkdir("./entryF", 0700)

	entries := makeEntries()
	dSNL0 := []int64{1,2,3,4}
	dSNL1 := []int64{5}
	dSNL2 := []int64{}
	dSNL3 := []int64{10,1}

	ef, err := NewEntryFile(128*1024/*128KB*/, "./entryF")
	assert.Equal(t, nil, err)

	bz0 := EntryToBytes(entries[0], dSNL0)
	pos0 := ef.Append(bz0)
	bz1 := EntryToBytes(entries[1], dSNL1)
	pos1 := ef.Append(bz1)
	bz2 := EntryToBytes(entries[2], dSNL2)
	pos2 := ef.Append(bz2)
	bz3 := EntryToBytes(entries[3], dSNL3)
	pos3 := ef.Append(bz3)

	for i := 0; i < LeafCountInTwig; i+=4 {
		ef.Append(bz0)
		ef.Append(bz1)
		ef.Append(bz2)
		ef.Append(bz3)
	}

	ef.Sync()
	ef.Close()

	ef, err = NewEntryFile(128*1024/*128KB*/, "./entryF")
	assert.Equal(t, nil, err)

	e, l, next := ef.ReadEntry(pos0)
	assert.Equal(t, entries[0], *e)
	assert.Equal(t, dSNL0, l)
	assert.Equal(t, pos1, next)

	e, l, next = ef.ReadEntry(pos1)
	assert.Equal(t, entries[1], *e)
	assert.Equal(t, dSNL1, l)
	assert.Equal(t, pos2, next)

	e, l, next = ef.ReadEntry(pos2)
	assert.Equal(t, entries[2], *e)
	assert.Equal(t, 0, len(l))
	assert.Equal(t, pos3, next)

	e, l, _ = ef.ReadEntry(pos3)
	assert.Equal(t, entries[3], *e)
	assert.Equal(t, dSNL3, l)

	assert.Equal(t, pos2, ef.SkipEntry(pos1))
	assert.Equal(t, pos3, ef.SkipEntry(pos2))

	twig := &Twig{
		FirstEntryPos: pos3,
	}
	twig.activeBits[0] = 3 // 3 and 0
	twig.activeBits[255] = 128 // 2

	activeEntries := ef.GetActiveEntriesInTwig(twig)
	assert.Equal(t, 3, len(activeEntries))
	assert.Equal(t, entries[3], *activeEntries[0])
	assert.Equal(t, entries[0], *activeEntries[1])
	assert.Equal(t, entries[2], *activeEntries[2])

	ef.Close()
	os.RemoveAll("./entryF")
}
