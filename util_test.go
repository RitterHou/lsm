package main

import (
	"fmt"
	"testing"
)

func TestParseBuf(t *testing.T) {
	s := [...]string{
		"1234你好世界你好世界你好世界你好世界你好世界你好世界📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚📚😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄",
		"2333333",
	}
	for _, s0 := range s {
		s1, n := parseBuf(addBufHead([]byte(s0)))
		if s0 != string(s1) {
			t.Fatal("解析错误")
		}
		t.Log(string(s1))
		t.Log(n)
	}

	buf := encodeKeyAndValue("name", s[0])
	buf = append(buf, encodeKeyAndValue("bobby", s[1])...)

	key, value, offset := decodeKeyAndValue(buf)
	fmt.Println(key, value)
	buf = buf[offset:]
	if value != s[0] {
		t.Fatal("解析错误")
	}

	key, value, offset = decodeKeyAndValue(buf)
	fmt.Println(key, value)
	if value != s[1] {
		t.Fatal("解析错误")
	}

	buf = buf[offset:]
	if len(buf) != 0 {
		t.Fatal("解析错误")
	}
}
