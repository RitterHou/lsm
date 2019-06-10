package main

import (
	"fmt"
	"github.com/ryszard/goskiplist/skiplist"
	"log"
	"os"
	"path"
)

type Lsm struct {
	path     string
	memTable *skiplist.SkipList

	transLogFile *os.File
}

func (l *Lsm) Set(key string, value string) {
	l.appendTransLog(key, value) // 写transLog
	l.memTable.Set(key, value)
}

func (l *Lsm) Get(key string) (string, bool) {
	value, ok := l.memTable.Get(key)
	if ok {
		return value.(string), true
	}
	return "", false
}

// 每一条记录都需要写到transLog保证数据不会因为内存断电而丢失
func (l *Lsm) appendTransLog(key string, value string) {
	buf := AddBufHead([]byte(key))
	buf = append(buf, AddBufHead([]byte(value))...)

	var err error
	_, err = l.transLogFile.Write(buf)
	if err != nil {
		log.Fatal(err)
	}
	err = l.transLogFile.Sync()
	if err != nil {
		log.Fatal(err)
	}
}

func NewLsm(director string) *Lsm {
	if director == "" {
		dir, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		director = dir
	}

	transLogFile, err := os.Create(path.Join(director, transLog))
	if err != nil {
		log.Fatal(err)
	}
	return &Lsm{
		path:         director,
		memTable:     skiplist.NewStringMap(),
		transLogFile: transLogFile,
	}
}

func main() {
	lsm := NewLsm("./")
	lsm.Set("name", "Mike")
	lsm.Set("age", "18")
	fmt.Println(lsm.Get("name"))
	fmt.Println(lsm.Get("hobby"))
	fmt.Println(lsm.Get("age"))
	lsm.Set("name", "Json")
	fmt.Println(lsm.Get("name"))
}
