package lsm

import (
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

	// 根据得到的data来决定是否更新最终value的值
	var setValue = func(data Data) {
		if data.timestamp > valueTimestamp {
			value = data.value              // 更新值的内容
			ok = true                       // 已经找到了对应的值
			valueTimestamp = data.timestamp // 更新该值对应的时间戳
		}
	}

	indexFilesPath := getIndexFilesPath(r.path)
	// 根据所有的索引文件，去对应的段文件中检索数据
	for _, indexFilePath := range indexFilesPath {
		segFilePath := strings.Replace(indexFilePath, indexFileSuffix, segmentFileSuffix, -1)
		if _, err := os.Stat(strings.Replace(indexFilePath, indexFileSuffix, unavailableFileSuffix, -1)); !os.IsNotExist(err) {
			// 如果当前段文件存在对应的ua文件，则跳过此文件
			continue
		}

		var err error
		indexData, err := ioutil.ReadFile(indexFilePath)
		if err != nil {
			log.Fatal(err)
		}

		indices := getIndexList(indexData)
		length := len(indices)
		offsetLeft := uint32(0)  // 可检索范围内的最小索引下标
		offsetRight := uint32(0) // 可检索范围内的最大索引下标
		// 0或1个索引是没有意义的
		if length > 1 {
			// 超过范围，无法被找到，跳过该索引文件
			if key < indices[0].key || key > indices[length-1].key {
				continue
			}
			for i := 0; i < length; i++ {
				// 直接命中索引
				if indices[i].key == key {
					offsetLeft = indices[i].offset
					offsetRight = offsetLeft
					break
				}
				// 最后一个元素都没有命中，索引命中失败，需要全文检索
				if i == length-1 {
					break
				}
				// 索引范围命中，确定值应在指定的范围之中
				if indices[i].key < key && key < indices[i+1].key {
					offsetLeft = indices[i].offset
					offsetRight = indices[i+1].offset
					break
				}
			}
		}

		segFile, err := os.Open(segFilePath)
		if err != nil {
			log.Fatal(err)
		}
		size := getFileSize(segFile)

		if offsetLeft == 0 && offsetRight == 0 {
			// 1. 索引失效
			for {
				thisKey, data := readKeyAndData(segFile)
				if thisKey == key { // 取到对应的值
					setValue(data)
					break
				}

				position := getCurrentPosition(segFile)
				if position == size {
					// 最终也没能找到对应的值
					break
				}
			}
		} else if offsetLeft == offsetRight {
			// 2. 索引精确命中
			setCurrentPosition(segFile, offsetLeft)
			_, data := readKeyAndData(segFile)
			setValue(data)
		} else if offsetLeft < offsetRight {
			// 3. 索引范围命中
			setCurrentPosition(segFile, offsetLeft) // 从左开始查找

			for {
				thisKey, data := readKeyAndData(segFile)
				if thisKey == key {
					setValue(data)
					break
				}

				position := getCurrentPosition(segFile)
				if uint32(position) >= offsetRight {
					break
				}
			}
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
