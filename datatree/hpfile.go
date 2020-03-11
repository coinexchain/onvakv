package datatree

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// Head prune-able file
type HPFile struct {
	fileMap   map[int]*os.File
	blockSize int
	dirName   string
	largestID int
}

func NewHPFile(blockSize int, dirName string) (HPFile, error) {
	res := HPFile{
		fileMap:   make(map[int]*os.File),
		blockSize: blockSize,
		dirName:   dirName,
	}
	fileInfoList, err := ioutil.ReadDir(dirName)
	if err != nil {
		return res, err
	}
	var idList []int
	for _, fileInfo := range fileInfoList {
		if fileInfo.IsDir() {
			continue
		}
		twoParts := strings.Split(fileInfo.Name(), "-")
		if len(twoParts) != 2 {
			return res, fmt.Errorf("%s does not match the pattern 'FileId-BlockSize'", fileInfo.Name())
		}
		id, err := strconv.ParseInt(twoParts[0], 10, 31)
		if err != nil {
			return res, err
		}
		if res.largestID < int(id) {
			res.largestID = int(id)
		}
		idList = append(idList, int(id))
		size, err := strconv.ParseInt(twoParts[1], 10, 31)
		if int64(blockSize) != size {
			return res, fmt.Errorf("Invalid Size! %d!=%d", size, blockSize)
		}
	}
	for _, id := range idList {
		fname := fmt.Sprintf("%s/%d-%d", dirName, id, blockSize)
		var err error
		if id == res.largestID { // will write to this latest file
			res.fileMap[id], err = os.OpenFile(fname, os.O_RDWR, 0700)
		} else {
			res.fileMap[id], err = os.Open(fname)
		}
		if err != nil {
			return res, err
		}
	}
	if len(idList) == 0 {
		fname := fmt.Sprintf("%s/%d-%d", dirName, 0, blockSize)
		var err error
		res.fileMap[0], err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0700)
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

func (hpf *HPFile) Size() int64 {
	//fmt.Printf("%#v %d\n", hpf.fileMap, hpf.largestID)
	f := hpf.fileMap[hpf.largestID]
	size, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		panic(err)
	}
	return int64(hpf.largestID)*int64(hpf.blockSize) + size
}

func (hpf *HPFile) Truncate(size int64) error {
	for size < int64(hpf.largestID)*int64(hpf.blockSize) {
		f := hpf.fileMap[hpf.largestID]
		err := f.Close()
		if err != nil {
			return err
		}
		fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, hpf.largestID, hpf.blockSize)
		err = os.Remove(fname)
		if err != nil {
			return err
		}
		delete(hpf.fileMap, hpf.largestID)
		hpf.largestID--
	}
	size -= int64(hpf.largestID)*int64(hpf.blockSize)
	err := hpf.fileMap[hpf.largestID].Close()
	if err != nil {
		return err
	}
	fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, hpf.largestID, hpf.blockSize)
	hpf.fileMap[hpf.largestID], err = os.OpenFile(fname, os.O_RDWR, 0700)
	if err != nil {
		return err
	}
	return hpf.fileMap[hpf.largestID].Truncate(size)
}

func (hpf *HPFile) Sync() error {
	return hpf.fileMap[hpf.largestID].Sync()
}

func (hpf *HPFile) Close() error {
	for _, file := range hpf.fileMap {
		err := file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (hpf *HPFile) ReadAt(buf []byte, off int64) error {
	fileID := off / int64(hpf.blockSize)
	pos := off % int64(hpf.blockSize)
	f, ok := hpf.fileMap[int(fileID)]
	if !ok {
		return fmt.Errorf("Can not find the file with id=%d (%d/%d)", fileID, off, hpf.blockSize)
	}
	_, err := f.ReadAt(buf, pos)
	return err
}

func (hpf *HPFile) Append(bufList [][]byte) (int64, error) {
	f := hpf.fileMap[hpf.largestID]
	size, err := f.Seek(0, os.SEEK_END)
	//fmt.Printf("size after Seek: %d\n", size)
	if err != nil {
		return 0, err
	}
	startPos := int64(hpf.largestID*hpf.blockSize) + size
	totalLen := 0
	for _, buf := range bufList {
		_, err = f.Write(buf)
		if err != nil {
			return 0, err
		}
		totalLen += len(buf)
	}
	overflowByteCount := size + int64(totalLen) - int64(hpf.blockSize)
	if overflowByteCount >= 0 {
		f.Sync()
		hpf.largestID++
		fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, hpf.largestID, hpf.blockSize)
		f, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0700)
		if err != nil {
			return 0, err
		}
		if overflowByteCount != 0 {
			zeroBytes := make([]byte, overflowByteCount)
			_, err = f.Write(zeroBytes)
			if err != nil {
				return 0, err
			}
		}
		hpf.fileMap[hpf.largestID] = f
	}
	return startPos, nil
}

func (hpf *HPFile) PruneHead(off int64) error {
	fileID := off / int64(hpf.blockSize)
	var idList []int
	for id, f := range hpf.fileMap {
		if id >= int(fileID) {
			continue
		}
		err := f.Close()
		if err != nil {
			return err
		}
		idList = append(idList, id)
	}
	for _, id := range idList {
		delete(hpf.fileMap, id)
		fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, id, hpf.blockSize)
		err := os.Remove(fname)
		if err != nil {
			return err
		}
	}
	return nil
}
