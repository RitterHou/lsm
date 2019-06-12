package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"
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
	start := time.Now().Unix()

	lsm, err := NewLsm("./", false)
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

	data, err := ioutil.ReadFile("./test.txt")
	if err != nil {
		log.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		tmp := strings.Split(line, ",")
		if len(tmp) > 1 {
			lsm.Set(tmp[0], tmp[1])
		}
	}
	lsm.Close()

	fmt.Printf("Cost %d seconds\n", time.Now().Unix()-start)
}
