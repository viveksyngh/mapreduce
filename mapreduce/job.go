package mapreduce

//Job Map Reduce job definition
type Job struct {
	Name          string
	InputFilePath string
	MapperCount   int
	ReducerCount  int
}

//Start start a map reduce job
func (job *Job) Start() {

}
