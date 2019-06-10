package main

import "encoding/binary"

const transLog = "translog"

func Uint32ToBytes(num uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, num)
	return buf
}

// 给一段字节数组加上头部信息
func AddBufHead(buf []byte) []byte {
	length := len(buf)
	head := make([]byte, 0, 1)
	if length < 0xff {
		head = append(head, byte(length))
	} else {
		head = append(head, byte(0xff))
		head = append(head, Uint32ToBytes(uint32(length))...)
	}
	body := append(head, buf...)
	return body
}
