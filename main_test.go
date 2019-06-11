package main

import (
	"log"
	"math/rand"
	"testing"
	"time"
)

// 生成随机字符串
func randomString(n int) string {
	rand.Seed(time.Now().UnixNano())
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// 生成随机整数
func randomInt(from, to int) int {
	return rand.Intn(to-from) + from
}

func TestLsm(t *testing.T) {
	lsm, err := NewLsm("./")
	if err != nil {
		log.Fatal(err)
	}
	lsm.Set("name", "Mike")
	lsm.Set("age", "18")
	t.Log(lsm.Get("name"))
	t.Log(lsm.Get("hobby"))
	t.Log(lsm.Get("age"))
	lsm.Set("name", "Json")
	t.Log(lsm.Get("name"))

	for i := 0; i < 100000; i++ {
		key := randomString(randomInt(5, 15))
		value := randomString(randomInt(10, 50))
		lsm.Set(key, value)
	}
	lsm.Close()
}
