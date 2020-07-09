package datatree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/coinexchain/onvakv/types"
)

type Entry = types.Entry

const MaxEntryBytes int = (1 << 24) - 1

var MagicBytes = [8]byte{byte('I'), byte('L'), byte('O'), byte('V'), byte('E'), byte('Y'), byte('O'), byte('U')}


func DummyEntry(sn int64) *Entry {
	return &Entry{
		Key:        []byte("dummy"),
		Value:      []byte("dummy"),
		NextKey:    []byte("dummy"),
		Height:     -2,
		LastHeight: -2,
		SerialNum:  sn,
	}
}

func NullEntry() Entry {
	return Entry{
		Key:        []byte{},
		Value:      []byte{},
		NextKey:    []byte{},
		Height:     -1,
		LastHeight: -1,
		SerialNum:  -1,
	}
}

// Entry serialization format:
// magicBytes 8-bytes
// 32b-totalLength (this length does not include padding, checksum and this field itself)
// magicBytesPos(list of 32b-int, -1 for ending), posistions are relative to the end of 32b-totalLength
// normalPayload
// DeactivedSerialNumList (list of 64b-int, -1 for ending)
// padding-zero-bytes

const MSB32 = uint32(1<<31)

func EntryToBytes(entry Entry, deactivedSerialNumList []int64) []byte {
	length := 4 + 4                                                        // 32b-totalLength and empty magicBytesPos
	length += 4*3 + len(entry.Key) + len(entry.Value) + len(entry.NextKey) // Three strings
	length += 8 * 3                                                        // Three int64
	length += (len(deactivedSerialNumList) + 1) * 8
	b := make([]byte, length)

	const start = 8
	writeEntryPayload(b[start:], entry, deactivedSerialNumList)

	magicBytesPosList := getAllPos(b[start:], MagicBytes[:])
	if len(magicBytesPosList) == 0 {
		binary.LittleEndian.PutUint32(b[:4], uint32(length-4))
		binary.LittleEndian.PutUint32(b[4:8], ^uint32(0))
		return b
	}

	// if magicBytesPosList is not empty:
	var zeroBuf [8]byte
	for _, pos := range magicBytesPosList {
		copy(b[start+pos:start+pos+8], zeroBuf[:]) // over-write the occurrence of magic bytes with zeros
	}
	length += 4 * len(magicBytesPosList)
	buf := make([]byte, length)
	// Re-write the new length. minus 4 because the first 4 bytes of length isn't included
	binary.LittleEndian.PutUint32(buf[:4], uint32(length-4))

	bytesAdded := 4 * len(magicBytesPosList)
	var i int
	for i = 0; i < len(magicBytesPosList); i++ {
		pos := magicBytesPosList[i] + bytesAdded /*32b-length*/
		binary.LittleEndian.PutUint32(buf[i*4+4:i*4+8], uint32(pos))
	}
	binary.LittleEndian.PutUint32(buf[i*4+4:i*4+8], ^uint32(0))
	copy(buf[i*4+8:], b[8:])
	return buf
}

func writeEntryPayload(b []byte, entry Entry, deactivedSerialNumList []int64) {
	i := 0
	binary.LittleEndian.PutUint32(b[i:i+4], uint32(len(entry.Key)))
	i += 4
	copy(b[i:], entry.Key)
	i += len(entry.Key)

	binary.LittleEndian.PutUint32(b[i:i+4], uint32(len(entry.Value)))
	i += 4
	copy(b[i:], entry.Value)
	i += len(entry.Value)

	binary.LittleEndian.PutUint32(b[i:i+4], uint32(len(entry.NextKey)))
	i += 4
	copy(b[i:], entry.NextKey)
	i += len(entry.NextKey)

	binary.LittleEndian.PutUint64(b[i:i+8], uint64(entry.Height))
	i += 8
	binary.LittleEndian.PutUint64(b[i:i+8], uint64(entry.LastHeight))
	i += 8
	binary.LittleEndian.PutUint64(b[i:i+8], uint64(entry.SerialNum))
	i += 8

	for _, sn := range deactivedSerialNumList {
		binary.LittleEndian.PutUint64(b[i:i+8], uint64(sn))
		i += 8
	}
	binary.LittleEndian.PutUint64(b[i:i+8], math.MaxUint64)
}

func getAllPos(s, sep []byte) (allpos []int) {
	for start, pos := 0, 0; start + len(sep) < len(s); start += pos + len(sep) {
		pos = bytes.Index(s[start:], sep)
		if pos == -1 {
			return
		}
		allpos = append(allpos, pos+start)
	}
	return
}

func EntryFromBytes(b []byte) (*Entry, []int64) {
	entry := &Entry{}
	i := 0

	length := int(binary.LittleEndian.Uint32(b[i : i+4]))
	i += 4
	entry.Key = append([]byte{}, b[i:i+length]...)
	i += length

	length = int(binary.LittleEndian.Uint32(b[i : i+4]))
	i += 4
	entry.Value = append([]byte{}, b[i:i+length]...)
	i += length

	length = int(binary.LittleEndian.Uint32(b[i : i+4]))
	i += 4
	entry.NextKey = append([]byte{}, b[i:i+length]...)
	i += length

	entry.Height = int64(binary.LittleEndian.Uint64(b[i : i+8]))
	i += 8
	entry.LastHeight = int64(binary.LittleEndian.Uint64(b[i : i+8]))
	i += 8
	entry.SerialNum = int64(binary.LittleEndian.Uint64(b[i : i+8]))
	i += 8

	var deactivedSerialNumList []int64
	sn := binary.LittleEndian.Uint64(b[i : i+8])
	for sn != math.MaxUint64 {
		deactivedSerialNumList = append(deactivedSerialNumList, int64(sn))
		i += 8
		sn = binary.LittleEndian.Uint64(b[i : i+8])
	}

	return entry, deactivedSerialNumList
}

type EntryFile struct {
	HPFile
}

func getPaddingSize(length int) int {
	rem := length % 8
	if rem == 0 {
		return 0
	} else {
		return 8 - rem
	}
}

func (ef *EntryFile) readMagicBytesAndLength(off int64) (lengthInt int64, lengthBytes []byte) {
	var buf [12]byte
	err := ef.HPFile.ReadAt(buf[:], off)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(buf[:8], MagicBytes[:]) {
		fmt.Printf("Now off %d %x\n", off, off)
		panic("Invalid MagicBytes")
	}
	length := binary.LittleEndian.Uint32(buf[8:12])
	if int(length) >= MaxEntryBytes {
		panic("Entry to long")
	}
	return int64(length), buf[8:12]
}

func getNextPos(off, length int64) int64 {
	length += 8 /*magicbytes*/ + 4 /*length*/
	paddingSize := getPaddingSize(int(length))
	paddedLen := length + int64(paddingSize)
	nextPos := off + paddedLen
	//fmt.Printf("off %d length %d paddingSize %d paddedLen %d nextPos %d\n", off, length, paddingSize, paddedLen, nextPos)
	return nextPos

}

func (ef *EntryFile) ReadEntry(off int64) (*Entry, []int64, int64) {
	length, _ := ef.readMagicBytesAndLength(off)
	nextPos := getNextPos(off, int64(length))
	b := make([]byte, 12+int(length)) // include 12 (magicbytes and length)
	err := ef.HPFile.ReadAt(b, off)
	b = b[12:] // ignore magicbytes and length
	if err != nil {
		panic(err)
	}
	var n int
	for n = 0; n < int(length); n += 4 { // recover magic bytes in payload
		pos := binary.LittleEndian.Uint32(b[n : n+4])
		if pos == ^(uint32(0)) {
			n += 4
			break
		}
		if int(pos) >= MaxEntryBytes {
			panic("Position to large")
		}
		copy(b[int(pos)+4:int(pos)+12], MagicBytes[:])
	}
	entry, deactivedSerialNumList := EntryFromBytes(b[n:length])
	return entry, deactivedSerialNumList, nextPos
}

func NewEntryFile(bufferSize, blockSize int, dirName string) (res EntryFile, err error) {
	res.HPFile, err = NewHPFile(bufferSize, blockSize, dirName)
	return
}

func (ef *EntryFile) Size() int64 {
	return ef.HPFile.Size()
}
func (ef *EntryFile) Truncate(size int64) {
	err := ef.HPFile.Truncate(size)
	if err != nil {
		panic(err)
	}
}
func (ef *EntryFile) Flush() {
	ef.HPFile.Flush()
}
func (ef *EntryFile) FlushAsync() {
	ef.HPFile.FlushAsync()
}
func (ef *EntryFile) Close() {
	err := ef.HPFile.Close()
	if err != nil {
		panic(err)
	}
}
func (ef *EntryFile) PruneHead(off int64) {
	err := ef.HPFile.PruneHead(off)
	if err != nil {
		panic(err)
	}
}

func (ef *EntryFile) Append(b []byte) (pos int64) {
	var bb [3][]byte
	bb[0] = MagicBytes[:]
	bb[1] = b
	paddingSize := getPaddingSize(len(bb[1]))
	bb[2] = make([]byte, paddingSize) // padding zero bytes
	pos, err := ef.HPFile.Append(bb[:])
	if pos%8 != 0 {
		panic("Entries are not aligned")
	}
	if err != nil {
		panic(err)
	}
	//fmt.Printf("Now Append At: %d len: %d\n", pos, len(b))
	return
}

func (ef *EntryFile) GetActiveEntriesInTwig(twig *Twig) (res []*Entry) {
	start := twig.FirstEntryPos
	for i := 0; i < LeafCountInTwig; i++ {
		if twig.getBit(i) {
			entry, _, next := ef.ReadEntry(start)
			start = next
			res = append(res, entry)
		} else { // skip an inactive entry
			var length int64
			length, _ = ef.readMagicBytesAndLength(start)
			start = getNextPos(start, length)
		}
	}
	return
}
