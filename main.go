package main

import (
	"bufio"
	"container/list"
	"strings"

	"strconv"

	"github.com/viveksyngh/mapreduce/mapreduce"
)

func main() {
	mapreduce.Mapper("word-count", 1, "/Users/viveks/Downloads/bible.txt", mapFunc, 1)
	mapreduce.Reducer("word-count", 1, "/Users/viveks/Downloads/bible.txt", reduceFunc, 1)
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

func reduceFunc(key string, values *list.List) mapreduce.KeyValue {
	var totalValue int
	for e := values.Front(); e != nil; e = e.Next() {
		value := e.Value.(string)
		intValue, err := strconv.Atoi(value)
		if err == nil {
			totalValue += intValue
		}
	}
	return mapreduce.KeyValue{Key: key, Value: strconv.Itoa(totalValue)}
}
