package main

import (
	"bufio"
	"container/list"
	"strings"

	"github.com/viveksyngh/mapreduce/mapreduce"
)

func main() {
	mapreduce.Mapper("/tmp/input.txt", mapFunc, 3)
}

func mapFunc(key, value string) *list.List {

	scanner := bufio.NewScanner(strings.NewReader(value))
	scanner.Split(bufio.ScanWords)

	l := list.New()
	for scanner.Scan() {
		kv := mapreduce.KeyValue{Key: scanner.Text(), Value: "1"}
		l.PushBack(kv)
	}

	return l
}
