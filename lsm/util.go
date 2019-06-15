package lsm

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
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
	writeLockFile         = "write.lock"    // 写LSM的文件锁
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
func getIndexFilesPath(director string) []string {
	files, err := ioutil.ReadDir(director)
	if err != nil {
		log.Fatal(err)
	}

	paths := make([]string, 0)
	for _, file := range files {
		name := file.Name()
		if file.Mode().IsRegular() && strings.HasSuffix(name, indexFileSuffix) {
			paths = append(paths, path.Join(director, name))
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

// 把key和data进行编码
func encodeKeyAndData(key string, data Data) []byte {
	buf := addBufHead([]byte(key))
	buf = append(buf, addBufHead([]byte(data.value))...)
	buf = append(buf, uint64ToBytes(data.timestamp)...) // 时间戳
	return buf
}

// 把字节数组解码为key的data
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

// 从文件中解码出一个字节数组
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

// 从文件中读取一组key和data
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

// 获取指定文件的大小
func getFileSize(file *os.File) int64 {
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	return fileInfo.Size()
}

// 获取文件的当前读写位置
func getCurrentPosition(file *os.File) int64 {
	// 从当前偏移改变0，还是当前偏移
	position, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Fatal(err)
	}
	return position
}

// 设置文件的当前读写位置
func setCurrentPosition(file *os.File, position uint32) {
	_, err := file.Seek(int64(position), 0)
	if err != nil {
		log.Fatal(err)
	}
}

// 找出当前目录最小的两个段文件
func getTwoSmallFiles(indexFilesPath []string) (string, string) {
	var low1, low2 int64 = 0, 0
	var file1, file2 string
	for i, indexFile := range indexFilesPath {
		segFilePath := strings.Replace(indexFile, indexFileSuffix, segmentFileSuffix, -1)
		segFile, err := os.Open(segFilePath)
		if err != nil {
			log.Fatal(err)
		}
		size := getFileSize(segFile)
		if i%2 == 0 {
			if low1 == 0 || size < low1 {
				low1 = size
				file1 = segFilePath
			}
		} else {
			if low2 == 0 || size < low2 {
				low2 = size
				file2 = segFilePath
			}
		}
		err = segFile.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
	return file1, file2
}

// 为归并操作创建新的目标文件
func createNewSegFile(director string) (*os.File, *os.File) {
	var segFile *os.File
	var indexFile *os.File
	for {
		segFilePath := path.Join(director, generateSegmentFileName(director))
		// 再次检测防止在此期间文件被创建
		if _, err := os.Stat(segFilePath); os.IsNotExist(err) {
			// 创建段文件
			segFile, err = os.Create(segFilePath)
			if err != nil {
				log.Fatal(err)
			}
			// 创建ua文件
			uaFilePath := strings.Replace(segFilePath, segmentFileSuffix, unavailableFileSuffix, -1)
			uaFile, err := os.Create(uaFilePath)
			if err != nil {
				log.Fatal(err)
			}
			err = uaFile.Close()
			if err != nil {
				log.Fatal(err)
			}
			// 创建索引文件
			indexFilePath := strings.Replace(segFilePath, segmentFileSuffix, indexFileSuffix, -1)
			indexFile, err = os.Create(indexFilePath)
			if err != nil {
				log.Fatal(err)
			}
			break
		}
	}
	return segFile, indexFile
}

// 进行归并操作
func merge(source1, source2, target, indexFile *os.File) {
	var err error
	segFile1Size := getFileSize(source1)
	segFile2Size := getFileSize(source2)

	i := uint64(0)
	var key1, key2 string
	var data1, data2 Data
	// 进行归并操作
	for {
		var key string // 段文件当前使用的key

		pos1, _ := source1.Seek(0, io.SeekCurrent)
		pos2, _ := source2.Seek(0, io.SeekCurrent)
		if pos1 == segFile1Size && pos2 == segFile2Size {
			break
		}
		if pos1 < segFile1Size && key1 == "" {
			key1, data1 = readKeyAndData(source1)
		}
		if pos2 < segFile2Size && key2 == "" {
			key2, data2 = readKeyAndData(source2)
		}

		if key1 == "" {
			_, err = target.Write(encodeKeyAndData(key2, data2))
			if err != nil {
				log.Fatal(err)
			}
			key = key2
			key2 = ""
		} else if key2 == "" {
			_, err = target.Write(encodeKeyAndData(key1, data1))
			if err != nil {
				log.Fatal(err)
			}
			key = key1
			key1 = "" // 置空表示该值已经被使用
		} else if key1 < key2 {
			_, err = target.Write(encodeKeyAndData(key1, data1))
			if err != nil {
				log.Fatal(err)
			}
			key = key1
			key1 = ""
		} else if key2 < key1 {
			_, err = target.Write(encodeKeyAndData(key2, data2))
			if err != nil {
				log.Fatal(err)
			}
			key = key2
			key2 = ""
		} else { // 相等则需要比较时间戳
			if data1.timestamp >= data2.timestamp {
				_, err = target.Write(encodeKeyAndData(key1, data1))
				if err != nil {
					log.Fatal(err)
				}
				key = key1
			} else {
				_, err = target.Write(encodeKeyAndData(key2, data2))
				if err != nil {
					log.Fatal(err)
				}
				key = key2
			}
			// 一个被正确的保存，另外一个被丢弃
			key1 = ""
			key2 = ""
		}

		pos1, _ = source1.Seek(0, io.SeekCurrent)
		pos2, _ = source2.Seek(0, io.SeekCurrent)
		// 写索引文件
		if i%indexOffset == 0 || (pos1 == segFile1Size && pos2 == segFile2Size) {
			_, err = indexFile.Write(addBufHead([]byte(key)))
			if err != nil {
				log.Fatal(err)
			}
			size, err := target.Seek(0, io.SeekCurrent)
			if err != nil {
				log.Fatal(err)
			}
			_, err = indexFile.Write(uint32ToBytes(uint32(size)))
			if err != nil {
				log.Fatal(err)
			}
		}
		i += 1
	}
}

// 关闭文件
func closeFile(file *os.File) {
	err := file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// 删除文件
func removeFile(file string) {
	err := os.Remove(file)
	if err != nil {
		log.Fatal(err)
	}
}
