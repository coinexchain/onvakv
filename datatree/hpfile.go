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
	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		return nil, err
	}
	var idList []int
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		twoParts := strings.Split(file.Name(), "-")
		if len(twoParts) != 2 {
			return nil, fmt.Errorf("%s does not match the pattern 'FileId-BlockSize'", file.Name)
		}
		id, err := strconv.ParseInt(twoParts[0], 10, 31)
		if err != nil {
			return nil, err
		}
		if res.largestID < int(id) {
			res.largestID = int(id)
		}
		idList = append(idList, int(id))
		size, err := strconv.ParseInt(twoParts[1], 10, 31)
		if int64(blockSize) != size {
			return nil, fmt.Errorf("Invalid Size! %d!=%d", size, blockSize)
		}
	}
	for _, id := range idList {
		fname := fmt.Sprintf("%s/%d-%d", dirName, id, blockSize)
		var err error
		if id == res.largestID {
			res.fileMap[id], err = os.OpenFile(fname, os.O_RDWR, 0700)
		} else {
			res.fileMap[id], err = os.Open(fname)
		}
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (hpf *HPFile) Size() int64 {
	f := hpf.fileMap[hpf.largestID]
	size, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		panic(err)
	}
	return int64(hpf.largestID) * int64(hpf.blockSize) + size
}

func (hpf *HPFile) Truncate(size int64) error {
	for size > int64(hpf.largestID) * int64(hpf.blockSize) {
		f := hpf.fileMap[hpf.largestID]
		err := f.Close()
		if err != nil {
			return err
		}
		fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, hpf.largestID, hpf.blockSize)
		err := os.Remove(fname)
		if err != nil {
			return err
		}
		size -= int64(hpf.blockSize)
		hpf.largestID--
	}
	f := hpf.fileMap[hpf.largestID]
	return f.Truncate(size)
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

func (hpf *HPFile) ReadAt(b []byte, off int64) error {
	fileID := off / int64(hpf.blockSize)
	pos := off % int64(hpf.blockSize)
	f, ok := hpf.fileMap[int(fileID)]
	if !ok {
		return fmt.Errorf("Can not find the file with id=%d (%d/%d)", fileID, off, hpf.blockSize)
	}
	_, err := f.ReadAt(b, pos)
	return err
}

func (hpf *HPFile) Append(b []byte) (int64, error) {
	f := hpf.fileMap[hpf.largestID]
	size, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		return 0, err
	}
	_, err = f.Write(b)
	if err != nil {
		return 0, err
	}
	extra := size + int64(len(b)) - int64(hpf.blockSize)
	if extra >= 0 {
		f.Sync()
		hpf.largestID++
		fname := fmt.Sprintf("%s/%d-%d", hpf.dirName, hpf.largestID, hpf.blockSize)
		f, err = os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0700)
		if err != nil {
			return err
		}
		if extra != 0 {
			buf := make([]byte, extra)
			_, err = f.Write(buf)
			if err != nil {
				return err
			}
		}
		hpf.fileMap[hpf.largestID] = f
	}
	return hpf.largestID * int64(hpf.blockSize) + size, nil
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
