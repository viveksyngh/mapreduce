package mapreduce

import (
	"bufio"
	"container/list"
	"fmt"
	"os"
	"sort"
	"strings"
)

//Reduce reduce function type
type Reduce func(key string, values *list.List) KeyValue

//Reducer reads intermediate data, runs reduce function and write output data
func Reducer(reducerID int, fileName string, reduceFunc Reduce, mapperCount int) {
	intermediateItems := make(map[string]*list.List)

	for i := 0; i < mapperCount; i++ {

		_, bytes, err := readFile(getReduceFilename(fileName, i, reducerID))
		if err != nil {
			fmt.Println(err)
			return
		}

		//Load and prepare data to be passed to reducer
		scanner := bufio.NewScanner(strings.NewReader(string(bytes)))
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			kv := strings.Split(scanner.Text(), "\t")
			key := kv[0]
			value := kv[1]
			_, ok := intermediateItems[key]
			if !ok {
				intermediateItems[key] = list.New()
			}
			intermediateItems[key].PushBack(value)
		}
	}

	//Combine and sort phase
	var keys []string
	for key := range intermediateItems {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	//Process data using user defined reduce function and write to output file
	outputfileName := fmt.Sprintf("%s-out", fileName)
	file, err := os.Create(outputfileName)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	for _, k := range keys {
		kv := reduceFunc(k, intermediateItems[k])
		file.WriteString(fmt.Sprintf("%s\t%s\n", kv.Key, kv.Value)) //TODO use different encoding method
	}
}
