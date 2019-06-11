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
	"time"
)

const (
	transLog          = "translog"  // transLog文件的名称
	thresholdSize     = 1024 * 1024 // memTable转化为SSTable的大小阈值
	indexOffset       = 1000        // 每隔offset创建一个索引
	indexFileSuffix   = ".i"        // 索引文件的后缀名
	segmentFileSuffix = ".seg"      // 数据文件的后缀名
)

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

func getNowBuf() []byte {
	now := time.Now().UnixNano()
	return uint64ToBytes(uint64(now))
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

func encodeKeyAndValue(key, value string) []byte {
	buf := addBufHead([]byte(key))
	buf = append(buf, addBufHead([]byte(value))...)
	return buf
}

func decodeKeyAndValue(buf []byte) (string, string, uint32) {
	keyBuf, keyOffset := parseBuf(buf)
	buf = buf[keyOffset:]
	valueBuf, valOffset := parseBuf(buf)
	return string(keyBuf), string(valueBuf), keyOffset + valOffset
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

// 获取指定文件的绝对路径
func GetFilePath(file *os.File) string {
	absPath, err := filepath.Abs(filepath.Dir(file.Name()))
	if err != nil {
		log.Fatal(err)
	}
	return path.Join(absPath, file.Name())
}
