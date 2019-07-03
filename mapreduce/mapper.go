package mapreduce

import (
	"container/list"
	"fmt"
	"os"
	"path"
	"strconv"
)

//Map type of the map function
type Map func(key, value string) *list.List

func mapper(fileName string, mapFunc Map, reducerCount int) {

	_, bytes, err := readFile(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}

	result := mapFunc(fileName, string(bytes))

	for item := result.Front(); item != nil; item = item.Next() {
		kv := item.Value.(KeyValue)
		hash := getHash(kv.Key)
		reducefilename := getReduceFileName(fileName, int(hash))

		var reducefile *os.File
		if exists(reducefilename) {
			reducefile, err = os.OpenFile(reducefilename, os.O_APPEND|os.O_WRONLY, 0600)
		} else {
			reducefile, err = os.Create(reducefilename)
		}

		if err != nil {
			fmt.Println(err)
			return
		}
		defer reducefile.Close()

		_, err = reducefile.WriteString(fmt.Sprintf("%s\t%s", kv.Key, kv.Value))
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

//getReduceFileName get reducer file name
func getReduceFileName(fileName string, reducerNumber int) string {
	dir := path.Dir(fileName)
	file := path.Base(fileName)

	return path.Join(dir, file+"-"+strconv.Itoa(reducerNumber))
}
