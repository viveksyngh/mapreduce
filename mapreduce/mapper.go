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

//Mapper runs map function and produces intermediate output
func Mapper(fileName string, mapFunc Map, reducerCount int) {

	_, bytes, err := readFile(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}

	result := mapFunc(fileName, string(bytes))

	for item := result.Front(); item != nil; item = item.Next() {
		kv := item.Value.(KeyValue)
		hash := getHash(kv.Key)
		reducefilename := getReduceFilename(fileName, int(hash)%reducerCount)

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

		_, err = reducefile.WriteString(fmt.Sprintf("%s\t%s\n", kv.Key, kv.Value))
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

//getReduceFilename get reducer file name
func getReduceFilename(fileName string, reducerNumber int) string {
	dir := path.Dir(fileName)
	file := path.Base(fileName)

	return path.Join(dir, file+"-"+strconv.Itoa(reducerNumber))
}
