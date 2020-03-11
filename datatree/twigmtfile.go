package datatree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/mmcloughlin/meow"
)

type TwigMtFile struct {
	HPFile
}

func NewTwigMtFile(blockSize int, dirName string) (res TwigMtFile, err error) {
	res.HPFile, err = NewHPFile(blockSize, dirName)
	return
}

const TwigMtEntryCount = 4095
const TwigMtSize = 12 + TwigMtEntryCount*36

func (tf *TwigMtFile) AppendTwig(mtree [][32]byte, firstEntryPos int64) {
	if len(mtree) != TwigMtEntryCount {
		panic(fmt.Sprintf("len(mtree) != %d", TwigMtEntryCount))
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(firstEntryPos))
	h := meow.New32(0)
	h.Write(buf[:])
	_, err := tf.HPFile.Append([][]byte{buf[:], h.Sum(nil)}) // 8+4 bytes
	if err != nil {
		panic(err)
	}
	for i := 0; i < len(mtree); i++ { // 4095 iterations
		h = meow.New32(0)
		h.Write(mtree[i][:])
		_, err := tf.HPFile.Append([][]byte{mtree[i][:], h.Sum(nil)}) // 32+4 bytes
		if err != nil {
			panic(err)
		}
	}
}

func (tf *TwigMtFile) GetFirstEntryPos(twigID int64) int64 {
	var buf [12]byte
	err := tf.HPFile.ReadAt(buf[:], twigID*TwigMtSize)
	if err != nil {
		panic(err)
	}
	h := meow.New32(0)
	h.Write(buf[:8])
	if !bytes.Equal(buf[8:], h.Sum(nil)) {
		panic("Checksum Error!")
	}
	return int64(binary.LittleEndian.Uint64(buf[:8]))
}

func (tf *TwigMtFile) GetHashNode(twigID int64, hashID int) []byte {
	var buf [36]byte
	if hashID <= 0 || hashID >= 4096 {
		panic(fmt.Sprintf("Invalid hashID: %d", hashID))
	}
	offset := twigID*int64(TwigMtSize) + 12 + (int64(hashID)-1)*36
	err := tf.HPFile.ReadAt(buf[:], offset)
	if err != nil {
		panic(err)
	}
	h := meow.New32(0)
	h.Write(buf[:32])
	if !bytes.Equal(buf[32:], h.Sum(nil)) {
		panic("Checksum Error!")
	}
	return buf[:32]
}

func (tf *TwigMtFile) Size() int64 {
	return tf.HPFile.Size()
}
func (tf *TwigMtFile) Truncate(size int64) {
	err := tf.HPFile.Truncate(size)
	if err != nil {
		panic(err)
	}
	return
}
func (tf *TwigMtFile) Sync() {
	err := tf.HPFile.Sync()
	if err != nil {
		panic(err)
	}
}
func (tf *TwigMtFile) Close() {
	err := tf.HPFile.Close()
	if err != nil {
		panic(err)
	}
}
func (tf *TwigMtFile) PruneHead(off int64) {
	err := tf.HPFile.PruneHead(off)
	if err != nil {
		panic(err)
	}
}
