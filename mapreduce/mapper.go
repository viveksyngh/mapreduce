package mapreduce

import "container/list"

//Map type of the map function
type Map func(key, value string) *list.List

func mapper(fileName string, mapFunc Map) {

}
