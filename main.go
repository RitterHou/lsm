package main

import (
	"fmt"
	"github.com/ryszard/goskiplist/skiplist"
	"io/ioutil"
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
	if l.memTable.Len()%100 == 0 {
		memTableSize := l.getMemTableSize()
		fmt.Println(memTableSize / 1024.0 / 1024.0)
		if memTableSize > thresholdSize {
			var err error
			err = l.createSortedStringTable()
			if err != nil {
				log.Fatal(err)
			}
			// 重置memTable
			l.memTable = skiplist.NewStringMap()
			err = l.resetTransLogFile()
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func (l *Lsm) getMemTableSize() uint64 {
	var memTableSize uint64 // 内存中占用的空间
	iterator := l.memTable.Iterator()
	for iterator.Next() {
		key := iterator.Key().(string)
		value := iterator.Value().(string)
		memTableSize = memTableSize + uint64(len(key)) + uint64(len(value))
	}
	return memTableSize
}

// 创建SSTable
func (l *Lsm) createSortedStringTable() error {
	buf := make([]byte, 0)
	iter := l.memTable.Iterator()
	for iter.Next() {
		buf = append(buf, addBufHead([]byte(iter.Key().(string)))...)
		buf = append(buf, addBufHead([]byte(iter.Value().(string)))...)
	}

	err := ioutil.WriteFile(path.Join(l.path, "1.seg"), buf, 0666)
	if err != nil {
		return err
	}
	return nil
}

// 重置日志文件
func (l *Lsm) resetTransLogFile() error {
	var err error
	err = l.transLogFile.Truncate(0)
	if err != nil {
		return err
	}
	_, err = l.transLogFile.Seek(0, 0)
	if err != nil {
		return err
	}
	err = l.transLogFile.Sync()
	if err != nil {
		return err
	}
	return nil
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
	buf := addBufHead([]byte(key))
	buf = append(buf, addBufHead([]byte(value))...)

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
