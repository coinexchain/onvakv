package datatree

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

const (
	BufferSize = 1024*1024 // 1MB
)

// Head prune-able file
type HPFile struct {
	fileMap        map[int]*os.File
	blockSize      int
	dirName        string
	largestID      int
	latestFileSize int64
	bufferSize     int
	buffer         []byte
}

func NewHPFile(bufferSize, blockSize int, dirName string) (HPFile, error) {
	res := HPFile{
		fileMap:    make(map[int]*os.File),
		blockSize:  blockSize,
		dirName:    dirName,
		bufferSize: bufferSize,
		buffer:     make([]byte, 0, bufferSize),
	}
	if blockSize % bufferSize != 0 {
		panic(fmt.Sprintf("Invalid blockSize 0x%x", blockSize))
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
		id, err := strconv.ParseInt(twoParts[0], 10, 63)
		if err != nil {
			return res, err
		}
		if res.largestID < int(id) {
			res.largestID = int(id)
		}
		idList = append(idList, int(id))
		size, err := strconv.ParseInt(twoParts[1], 10, 63)
		if int64(blockSize) != size {
			return res, fmt.Errorf("Invalid Size! %d!=%d", size, blockSize)
		}
	}
	for _, id := range idList {
		fname := fmt.Sprintf("%s/%d-%d", dirName, id, blockSize)
		var err error
		if id == res.largestID { // will write to this latest file
			res.fileMap[id], err = os.OpenFile(fname, os.O_RDWR, 0700)
			if err == nil {
				res.latestFileSize, err = res.fileMap[id].Seek(0, os.SEEK_END)
			}
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
	return int64(hpf.largestID)*int64(hpf.blockSize) + hpf.latestFileSize
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
	hpf.latestFileSize = size
	return hpf.fileMap[hpf.largestID].Truncate(size)
}

func (hpf *HPFile) Sync() error {
	if len(hpf.buffer) != 0 {
		_, err := hpf.fileMap[hpf.largestID].Write(hpf.buffer)
		if err != nil {
			return err
		}
		hpf.buffer = hpf.buffer[:0]
	}
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
	startPos := int64(hpf.largestID*hpf.blockSize) + hpf.latestFileSize
	for _, buf := range bufList {
		if len(buf) > hpf.bufferSize {
			panic("buf is too large")
		}
		hpf.latestFileSize += int64(len(buf))
		extraBytes := len(hpf.buffer) + len(buf) - hpf.bufferSize
		if extraBytes > 0 {
			hpf.buffer = append(hpf.buffer, buf[:len(buf)-extraBytes]...)
			buf = buf[len(buf)-extraBytes:]
			//pos, _ := f.Seek(0, os.SEEK_END)
			//fmt.Printf("Haha startPos %x: %x + %x > %x; real pos %x\n", startPos, len(hpf.buffer), len(buf), hpf.bufferSize, pos)
			_, err := f.Write(hpf.buffer)
			if err != nil {
				return 0, err
			}
			hpf.buffer = hpf.buffer[:0]
		}
		hpf.buffer = append(hpf.buffer, buf...)
	}
	overflowByteCount := hpf.latestFileSize - int64(hpf.blockSize)
	if overflowByteCount >= 0 {
		err := hpf.Sync()
		if err != nil {
			return 0, err
		}
		hpf.largestID++
		fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, hpf.largestID, hpf.blockSize)
		f, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0700)
		if err != nil {
			return 0, err
		}
		if overflowByteCount != 0 {
			hpf.buffer = hpf.buffer[:overflowByteCount]
			for i := 0; i < int(overflowByteCount); i++ {
				hpf.buffer[i] = 0
			}
		}
		hpf.fileMap[hpf.largestID] = f
		hpf.latestFileSize = overflowByteCount
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
