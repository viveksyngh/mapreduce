package mapreduce

import "container/list"

//Reduce reduce function type
type Reduce func(key string, values *list.List) KeyValue

func reducer(fileName string, reduceFunc Reduce) {

}
