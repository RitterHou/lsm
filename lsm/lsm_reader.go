package lsm

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

// 用于只读数据
type Reader struct {
	path string
}

func (r *Reader) Get(key string) (string, bool) {
	ok := false
	valueTimestamp := uint64(0) // 当前值的时间
	value := ""                 // 最大时间对于的值

	// 根据得到的data来决定是否更新value的值
	var setValue = func(data Data) {
		if data.timestamp > valueTimestamp {
			value = data.value
			ok = true
			valueTimestamp = data.timestamp
		}
	}

	indexFilesPath := getIndexFilesPath(r.path)
	for _, indexFilePath := range indexFilesPath {
		segFilePath := strings.Replace(indexFilePath, indexFileSuffix, segmentFileSuffix, -1)
		if _, err := os.Stat(strings.Replace(indexFilePath, indexFileSuffix, unavailableFileSuffix, -1)); !os.IsNotExist(err) {
			// 如果当前段文件存在对应的ua文件，在不读取此文件
			continue
		}

		var err error
		indexData, err := ioutil.ReadFile(indexFilePath)
		if err != nil {
			log.Fatal(err)
		}

		indices := getIndexList(indexData)
		length := len(indices)
		low := uint32(0)
		high := uint32(0)
		if length > 1 { // 0或1个索引是没有意义的
			// 超过范围，无法被找到，跳过该索引文件
			if key < indices[0].key || key > indices[length-1].key {
				continue
			}
			for i := 0; i < length; i++ {
				if indices[i].key == key { // 直接命中索引
					low = indices[i].offset
					high = low
					break
				}
				if i == length-1 {
					// 最后一个元素都没有命中，不再需要范围查询了
					break
				}
				if indices[i].key < key && key < indices[i+1].key {
					low = indices[i].offset
					high = indices[i+1].offset
					break
				}
			}
		}

		segFile, err := os.Open(segFilePath)
		if err != nil {
			log.Fatal(err)
		}
		fileInfo, err := segFile.Stat()
		if err != nil {
			log.Fatal(err)
		}
		size := fileInfo.Size()

		if low == 0 && high == 0 { // 1. 索引失效
			//start := time.Now().UnixNano()
			for {
				thisKey, data := readKeyAndData(segFile)
				if thisKey == key {
					setValue(data)
					break
				}

				// 从当前偏移改变0，还是当前偏移
				pos, _ := segFile.Seek(0, io.SeekCurrent)
				if pos >= size {
					break
				}
			}
			// 索引失败的查询时间差不多是其它的1000倍
			//fmt.Printf("索引失败：%d\n", time.Now().UnixNano()-start)
		} else if low == high { // 2. 索引命中
			//start := time.Now().UnixNano()
			_, err = segFile.Seek(int64(low), 0)
			if err != nil {
				log.Fatal(err)
			}
			_, data := readKeyAndData(segFile)
			setValue(data)
			//fmt.Printf("索引命中：%d\n", time.Now().UnixNano()-start)
		} else if low < high { // 3. 索引范围命中
			//start := time.Now().UnixNano()
			_, err = segFile.Seek(int64(low), 0)
			if err != nil {
				log.Fatal(err)
			}

			for {
				thisKey, data := readKeyAndData(segFile)
				if thisKey == key {
					setValue(data)
					break
				}

				// 从当前偏移改变0，还是当前偏移
				pos, _ := segFile.Seek(0, io.SeekCurrent)
				if uint32(pos) >= high {
					break
				}
			}
			//fmt.Printf("范围索引命中：%d\n", time.Now().UnixNano()-start)
		}

		err = segFile.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	return value, ok
}

func NewLsmReader(director string) *Reader {
	if director == "" {
		dir, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		director = dir
	}
	reader := &Reader{path: director}
	return reader
}
