package main

import (
	"encoding/binary"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	thresholdSize         = 1024 * 1024 * 3 // memTable转化为SSTable的大小阈值
	memTableCheckInterval = 1000 * 3        // 每隔指定的操作次数就检测一次内存表的大小
	indexOffset           = 1000            // 每隔offset个元素创建一个索引
	indexFileSuffix       = ".i"            // 索引文件的后缀名(index)
	segmentFileSuffix     = ".seg"          // 数据文件的后缀名(segment)
	unavailableFileSuffix = ".ua"           // 数据不可用标签文件的后缀名(unavailable)
	mergeCheckInterval    = 5               // 文件合并行为的检测时间间隔（秒）
	maxSegmentFileSize    = 5               // 当段文件数量超过这个限制的时候就会触发merge
	transLog              = "translog"      // transLog文件的名称，即事务日志(transaction log)
	transLogAsyncInterval = 1               // transLog异步的落盘时间间隔（秒）
	waitOldSegFileDelTime = 5               // 旧的段文件被打上废弃标签后等待一段时间再删除该文件（秒）
)

// 在指定目录中是否存在特定的后缀名文件
func isFileSuffixExist(director string, suffix string) bool {
	files, err := ioutil.ReadDir(director)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if file.Mode().IsRegular() && strings.HasSuffix(file.Name(), suffix) {
			return true
		}
	}
	return false
}

// 获取所有的索引文件的路径
func getIndexFilesPath(lsmPath string) []string {
	files, err := ioutil.ReadDir(lsmPath)
	if err != nil {
		log.Fatal(err)
	}

	paths := make([]string, 0)
	for _, file := range files {
		name := file.Name()
		if file.Mode().IsRegular() && strings.HasSuffix(name, indexFileSuffix) {
			paths = append(paths, path.Join(lsmPath, name))
		}
	}
	return paths
}

// 生成新的段文件名
func generateSegmentFileName(path string) string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	names := make(map[int]bool)
	for _, file := range files {
		name := file.Name()
		if file.Mode().IsRegular() && strings.HasSuffix(name, segmentFileSuffix) {
			name = strings.Replace(name, segmentFileSuffix, "", -1)
			nameInt, err := strconv.Atoi(name)
			if err != nil {
				log.Fatal(err)
			}
			names[nameInt] = true
		}
	}
	i := 0
	for {
		// 如果数字i在文件名中尚不存在，则可以使用
		if _, ok := names[i]; !ok {
			return strconv.Itoa(i) + segmentFileSuffix
		}
		i += 1
	}
}

// 从索引文件中获取索引列表
func getIndexList(data []byte) []Index {
	indices := make([]Index, 0)
	for len(data) > 0 {
		key, offset := parseBuf(data)
		data = data[offset:]
		indices = append(indices, Index{string(key), binary.LittleEndian.Uint32(data)})
		data = data[4:]
	}
	return indices
}

func uint32ToBytes(num uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, num)
	return buf
}

func uint64ToBytes(num uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, num)
	return buf
}

// 给一段字节数组加上头部信息
func addBufHead(buf []byte) []byte {
	length := len(buf)
	head := make([]byte, 0, 1)
	if length < 0xff {
		head = append(head, byte(length))
	} else {
		head = append(head, byte(0xff))
		head = append(head, uint32ToBytes(uint32(length))...)
	}
	body := append(head, buf...)
	return body
}

func encodeKeyAndData(key string, data Data) []byte {
	buf := addBufHead([]byte(key))
	buf = append(buf, addBufHead([]byte(data.value))...)
	buf = append(buf, uint64ToBytes(data.timestamp)...) // 时间戳
	return buf
}

func decodeKeyAndData(buf []byte) (string, Data, uint32) {
	keyBuf, keyOffset := parseBuf(buf)
	buf = buf[keyOffset:]
	valueBuf, valOffset := parseBuf(buf)
	buf = buf[valOffset:]

	timestampBuf := buf[:8]
	timestamp := binary.LittleEndian.Uint64(timestampBuf)
	return string(keyBuf), Data{value: string(valueBuf), timestamp: timestamp}, keyOffset + valOffset + 8
}

// 从一段字节数组中解析出body
func parseBuf(buf []byte) ([]byte, uint32) {
	offset := uint32(1)
	head := buf[0]
	if head < 0xff {
		offset += uint32(head)
		body := buf[1 : 1+head]
		return body, offset
	} else {
		length := binary.LittleEndian.Uint32(buf[1:])
		offset = offset + 4 + length
		body := buf[5 : 5+length]
		return body, offset
	}
}

func readBuf(file *os.File) []byte {
	headBuf := make([]byte, 1)
	_, err := file.Read(headBuf)
	if err != nil {
		log.Fatal(err)
	}
	head := headBuf[0]
	if head < 0xff {
		body := make([]byte, head)
		_, err := file.Read(body)
		if err != nil {
			log.Fatal(err)
		}
		return body
	} else {
		lengthBuf := make([]byte, 4)
		_, err := file.Read(lengthBuf)
		if err != nil {
			log.Fatal(err)
		}
		length := binary.LittleEndian.Uint32(lengthBuf)
		body := make([]byte, length)
		_, err = file.Read(body)
		if err != nil {
			log.Fatal(err)
		}
		return body
	}
}

func readKeyAndData(file *os.File) (string, Data) {
	key := readBuf(file)
	value := readBuf(file)

	timestampBuf := make([]byte, 8)
	_, err := file.Read(timestampBuf)
	if err != nil {
		log.Fatal(err)
	}
	timestamp := binary.LittleEndian.Uint64(timestampBuf)
	return string(key), Data{value: string(value), timestamp: timestamp}
}

// 获取指定文件的绝对路径
func GetFilePath(file *os.File) string {
	absPath, err := filepath.Abs(filepath.Dir(file.Name()))
	if err != nil {
		log.Fatal(err)
	}
	return path.Join(absPath, file.Name())
}
