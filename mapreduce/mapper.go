package mapreduce

import (
	"container/list"
	"fmt"
)

//Map type of the map function
type Map func(key, value string) *list.List

func mapper(fileName string, mapFunc Map) {

	_, bytes, err := readFile(fileName)
	if err != nil {
		fmt.Println(err)
	}

	result := mapFunc(fileName, string(bytes))
	fmt.Println(result)
}
