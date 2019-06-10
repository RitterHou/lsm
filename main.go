package main

import (
	"fmt"
	"github.com/ryszard/goskiplist/skiplist"
)

type Lsm struct {
	memTable *skiplist.SkipList
}

func (l *Lsm) Set(key string, value interface{}) {
	l.memTable.Set(key, value)
}

func (l *Lsm) Get(key string) interface{} {
	value, ok := l.memTable.Get(key)
	if ok {
		return value
	}
	return nil
}

func NewLsm() *Lsm {
	return &Lsm{memTable: skiplist.NewStringMap()}
}

func main() {
	lsm := NewLsm()
	lsm.Set("name", "Mike")
	lsm.Set("age", 18)
	fmt.Println(lsm.Get("name"))
	fmt.Println(lsm.Get("hobby"))
	fmt.Println(lsm.Get("age"))
	lsm.Set("name", "Json")
	fmt.Println(lsm.Get("name"))
}
