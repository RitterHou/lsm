package main

import (
	"testing"
)

func TestLsm(t *testing.T) {
	lsm := NewLsm("./")
	lsm.Set("name", "Mike")
	lsm.Set("age", "18")
	t.Log(lsm.Get("name"))
	t.Log(lsm.Get("hobby"))
	t.Log(lsm.Get("age"))
	lsm.Set("name", "Json")
	t.Log(lsm.Get("name"))
}
