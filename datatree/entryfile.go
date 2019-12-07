package datatree

import (
	"bytes"

	"github.com/mmcloughlin/meow"
)

const MaxEntryBytes int = (1<<24)-1
const MagicBytes [8]byte = {byte('I'),byte('L'),byte('O'),byte('V'),byte('E'),byte('Y'),byte('O'),byte('U')}

type Entry struct {
	Key        string
	Value      string
	NextKey    string
	Height     int64
	LastHeight int64
	SerialNum  int64
}

// Entry serialization format:
// magicBytes 32b-totalLength-wo-padding magicBytesPos(list of 32b-int, -1 for ending) normalPayload padding-zero-bytes 32b-checksum

func (entry Entry) ToBytes() []byte {
	length := 4+4+4*3+8*3+len(entry.Key)+len(entry.Value)+len(entry.NextKey)
	b := make([]byte, length)

	i := 8
	binary.LittleEndian.PutUint32(b[i:i+4], len(entry.Key))
	i += 4
	copy(b[i:i+length], entry.Key)
	i += len(entry.Key)

	binary.LittleEndian.PutUint32(b[i:i+4], len(entry.Value))
	i += 4
	copy(b[i:i+length], entry.Value)
	i += len(entry.Value)

	binary.LittleEndian.PutUint32(b[i:i+4], len(entry.NextKey))
	i += 4
	copy(b[i:i+length], entry.NextKey)
	i += len(entry.NextKey)

	binary.LittleEndian.PutUint64(b[i:i+8], entry.Height)
	i += 8
	binary.LittleEndian.PutUint64(b[i:i+8], entry.LastHeight)
	i += 8
	binary.LittleEndian.PutUint64(b[i:i+8], entry.SerialNum)
	i += 8

	magicBytesPosList = getAllPos(b, MagicBytes, 8)
	if len(magicBytesPosList) ==0 {
		binary.LittleEndian.PutUint32(b[:4], length-4)
		binary.LittleEndian.PutUint32(b[4:8], ^uint32(0))
		return b
	}

	var zeroBuf [8]byte
	for _, pos := range magicBytesPosList {
		copy(b[pos+8:pos+8+8], zeroBuf[:])
	}
	buf = length + 4 * len(magicBytesPosList)
	binary.LittleEndian.PutUint32(buf[:4], length-4)
	var i int
	for i=0; i < len(magicBytesPosList); i++ {
		binary.LittleEndian.PutUint32(buf[i*4+4:i*4+8], magicBytesPosList[i])
	}
	binary.LittleEndian.PutUint32(buf[i*4+4:i*4+8], ^uint32(0))
	copy(buf[i*4+8:], b[8:])
	return buf
}

func getAllPos(s, sep []byte, start int) (allpos []int) {
	for start < len(s) {
		pos := bytes.Index(s[start:], sep)
		if pos == -1 {
			return
		}
		allpos = append(allpos, pos)
		start = pos+len(sep)
	}
	return
}

func EntryFromBytes(b []byte) *Entry {
	res := &Entry{}
	i := 0

	length := binary.LittleEndian.Uint32(b[i:i+4])
	i += 4
	res.Key = string(b[i:i+length])
	i += length

	length = binary.LittleEndian.Uint32(b[i:i+4])
	i += 4
	res.Value = string(b[i:i+length])
	i += length

	length = binary.LittleEndian.Uint32(b[i:i+4])
	i += 4
	res.NextKey = string(b[i:i+length])
	i += length

	res.Height = binary.LittleEndian.Uint64(b[i:i+8])
	i += 8
	res.LastHeight = binary.LittleEndian.Uint64(b[i:i+8])
	i += 8
	res.SerialNum = binary.LittleEndian.Uint64(b[i:i+8])

	return res
}

type EntryFile struct {
	HPFile
}

func getPaddingSize(length int) int {
	rem := length%8
	if 0 <= rem && rem<=4 {
		return 4 - rem
	} else { // 5 6 7
		return 12 - rem
	}
}

func (ef *EntryFile) SkipEntry(off int64) int64 {
	var buf [8]byte
	err := ef.HPFile.Read(buf[:], off)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(buf[:], MagicBytes[:]) {
		panic("Invalid MagicBytes")
	}
	err := ef.HPFile.Read(buf[:4], off+8)
	if err != nil {
		panic(err)
	}
	length := binary.LittleEndian.Uint32(buf[:4])
	if length >= MaxEntryBytes {
		panic("Entry to long")
	}
	paddingSize = getPaddingSize(length)
	return off+12+length+paddingSize+4
}

func (ef *EntryFile) ReadEntry(off int64) (*Entry, int64) {
	var buf [8]byte
	err := ef.HPFile.Read(buf[:], off)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(buf[:], MagicBytes[:]) {
		panic("Invalid MagicBytes")
	}
	h := meow.New32(0)
	err := ef.HPFile.Read(buf[:4], off+8)
	if err != nil {
		panic(err)
	}
	h.Write(buf[:4])
	length := binary.LittleEndian.Uint32(buf[:4])
	if length >= MaxEntryBytes {
		panic("Entry to long")
	}
	paddingSize = getPaddingSize(length)
	nextPos := off+12+length+paddingSize+4
	b := make([]byte, length+paddingSize+4)
	err := ef.HPFile.Read(b, off+12)
	if err != nil {
		panic(err)
	}
	h.Write(b[:length])
	if !bytes.Equal(b[length+paddingSize:], h.Sum(nil)) {
		panic("Checksum Error")
	}
	var n int
	for n=0; n<length; n+=4 {
		pos = binary.LittleEndian.Uint32(b[n:n+4])
		if pos >= MaxEntryBytes {
			panic("Position to large")
		}
		if pos == ^(uint32(0)) {
			break
		}
		copy(b[pos:pos+8], MagicBytes[:])
	}
	return EntryFromBytes(b[n:length]), nextPos
}

func NewEntryFile(blockSize int, dirName string) (res EntryFile, err error) {
	res, err = NewHPFile(blockSize, dirName)
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
func (ef *EntryFile) Sync() error {
	err := ef.HPFile.Sync()
	if err != nil {
		panic(err)
	}
}
func (ef *EntryFile) Close() error {
	err := ef.HPFile.Close()
	if err != nil {
		panic(err)
	}
}
func (ef *EntryFile) PruneHead(off int64) error {
	err := ef.HPFile.PruneHead(off)
	if err != nil {
		panic(err)
	}
}
func (ef *EntryFile) Append(b []byte) (pos uint64) {
	pos, err = ef.HPFile.Append(MagicBytes)
	if err != nil {
		panic(err)
	}
	_, err := ef.HPFile.Append(b)
	if err != nil {
		panic(err)
	}
	if pos%8 != nil {
		panic("Entries are not aligned")
	}
	padding := make([]byte, getPaddingSize(len(b)))
	_, err = ef.HPFile.Append(padding)
	if err != nil {
		panic(err)
	}
	h := meow.New32(0)
	h.Write(b)
	_, err = ef.HPFile.Append(h.Sum(nil))
	if err != nil {
		panic(err)
	}
	return pos >> 3
}

func (ef *EntryFile) GetActiveEntriesInTwig(twig *Twig) (res []*Entry) {
	start := twig.FirstEntryPos
	for i:=0; i < LeafCountInTwig; i++ {
		if twig.getBit(i) {
			entry, next := ef.ReadEntry(start)
			start = next
			res = append(res, entry)
		} else {
			start := ef.SkipEntry(start)
		}
	}
	return
}

