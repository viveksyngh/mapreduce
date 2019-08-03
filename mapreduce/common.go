package mapreduce

import (
	"fmt"
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

//getIntermediateFileName get intermediate file path
func getIntermediateFileName(jobName string, mapperID int, reducerID int) string {
	baseDir := os.Getenv(DataDirEnvVar)
	if len(baseDir) == 0 {
		baseDir = DataDirectory
	}

	return path.Join(baseDir, jobName, getMapperName(mapperID), getReducerName(reducerID))
}

//getMapperName get job name from mapper ID
func getMapperName(mapperID int) string {
	mapperIDStr := strconv.Itoa(mapperID)
	for i := 0; i < 4-len(mapperIDStr); i++ {
		mapperIDStr = "0" + mapperIDStr
	}
	return fmt.Sprintf("m-%s", mapperIDStr)
}

//getReducerName get reducer job name from reducer ID
func getReducerName(reducerID int) string {
	reducerIDStr := strconv.Itoa(reducerID)
	for i := 0; i < 4-len(reducerIDStr); i++ {
		reducerIDStr = "0" + reducerIDStr
	}
	return fmt.Sprintf("r-%s", reducerIDStr)
}

func getOutputFileName(jobName string, reducerID int) string {
	reducerIDStr := strconv.Itoa(reducerID)
	for i := 0; i < 4-len(reducerIDStr); i++ {
		reducerIDStr = "0" + reducerIDStr
	}

	baseDir := os.Getenv(DataDirEnvVar)
	if len(baseDir) == 0 {
		baseDir = DataDirectory
	}

	return path.Join(baseDir, jobName, fmt.Sprintf("out-%s", reducerIDStr))
}

func createFileWithDir(fileName string) (*os.File, error) {
	dir := path.Dir(fileName)
	err := os.MkdirAll(dir, 0700)
	if err != nil {
		return nil, err
	}
	return os.Create(fileName)
}
