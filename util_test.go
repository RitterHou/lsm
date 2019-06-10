package main

import "testing"

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
}
