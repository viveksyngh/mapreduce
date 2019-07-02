package mapreduce

import "os"

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
