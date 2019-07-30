package mapreduce

import (
	"hash/fnv"
	"os"
	"path"
	"strconv"
)

//KeyValue key value type pairs
type KeyValue struct {
	Key   string
	Value string
}

//readFile read file content
func readFile(fileName string) (int, []byte, error) {
	var buffer []byte
	var bytesRead int

	file, err := os.Open(fileName)
	if err != nil {
		return bytesRead, buffer, err
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		return bytesRead, buffer, err
	}

	filesize := fileinfo.Size()
	buffer = make([]byte, filesize)

	bytesRead, err = file.Read(buffer)
	if err != nil {
		return bytesRead, buffer, err
	}

	return bytesRead, buffer, err
}

//getHash return hash value of a string
func getHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

//exists checks if file exists or not
func exists(filename string) bool {
	if _, err := os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

//getReduceFilename get reducer file name
func getReduceFilename(fileName string, mapperID int, reducerNumber int) string {
	dir := path.Dir(fileName)
	file := path.Base(fileName)

	return path.Join(dir, file+"-"+strconv.Itoa(reducerNumber))
}
