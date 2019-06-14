package main

import (
	"fmt"
	"io/ioutil"
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

func TestEasySave(t *testing.T) {
	lsm, err := NewLsm("./", false)
	if err != nil {
		t.Fatal(err)
	}
	lsm.Set("name", "Mike")
	lsm.Set("age", "18")
	t.Log(lsm.Get("name"))
	t.Log(lsm.Get("hobby"))
	t.Log(lsm.Get("age"))
	lsm.Set("name", "Json")
	t.Log(lsm.Get("name"))

	lsm.Set("name", "约瑟马璐德威廉特纳")
	t.Log(lsm.Get("name"))

	lsm.Close()
}

func TestEasyQuery(t *testing.T) {
	lsm, err := NewLsm("./", false)
	if err != nil {
		t.Fatal(err)
	}
	name, ok := lsm.Get("name")
	if ok {
		t.Log(name)
	}
	hobby, ok := lsm.Get("hobby")
	if ok {
		t.Log(hobby)
	}
	age, ok := lsm.Get("age")
	if ok {
		t.Log(age)
	}

	lsm.Close()
}

func TestSave(t *testing.T) {
	start := time.Now().Unix()

	lsm, err := NewLsm("./", false)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
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

func TestQuery(t *testing.T) {
	lsm, err := NewLsm("./", false)
	if err != nil {
		t.Fatal(err)
	}
	name, ok := lsm.Get("name")
	if ok {
		t.Log(name)
	}
	hobby, ok := lsm.Get("hobby")
	if ok {
		t.Log(hobby)
	}
	age, ok := lsm.Get("age")
	if ok {
		t.Log(age)
	}
	t.Log(lsm.Get("g4829571_20212"))

	data, err := ioutil.ReadFile("./test.txt")
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	lines := strings.Split(string(data), "\n")
	start := time.Now().Unix()
	for _, line := range lines[:3000] {
		i += 1
		tmp := strings.Split(line, ",")
		if len(tmp) > 1 {
			key := tmp[0]
			value, ok := lsm.Get(key)
			if !ok {
				t.Fatal("Can't find key " + key)
			}
			if value != tmp[1] {
				t.Fatalf("key: %s, %s != %s", key, value, tmp[1])
			}
			// t.Logf("%s: %s\n", key, value)
			if i%1000 == 0 {
				// fmt.Printf("%6.2f%% %6ds %6d: %d %s: %s\n", float32(i)/float32(len(lines))*100, time.Now().Unix()-start, i, len(lines), key, value)
				fmt.Printf("%6.2f%% %3ds\n", float32(i)/float32(len(lines))*100, time.Now().Unix()-start)
				start = time.Now().Unix()
			}
		}
	}

	lsm.Close()
}
