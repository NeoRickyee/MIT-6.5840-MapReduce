package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	nReduce                            int
	Files                              []string
	PendingWorkerIndexToAllocate       MutexedInt
	PendingReadFilesIndex              MutexedInt
	AssignedFileIndexesFromWorkerIndex Mutexed2DString

	WorkerMapTaskCompletionStatus MutexedBoolSlice
}

type MutexedInt struct {
	Mu    sync.Mutex
	Index int
}

type Mutexed2DString struct {
	Mu  sync.Mutex
	Map [][]string
}

type MutexedBoolSlice struct {
	Mu        sync.Mutex
	BoolSlice []bool
	AllTrue   bool
}

func (m *Mutexed2DString) AddMapEntry(worker_index int, file_name string) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.Map[worker_index] = append(m.Map[worker_index], file_name)
}

func (i *MutexedInt) GetAndIncrementIndex() int {
	i.Mu.Lock()
	defer func() {
		i.Index++
		i.Mu.Unlock()
	}()
	return i.Index
}

func (bool_slice *MutexedBoolSlice) SetIndexTrue(index int) {
	// bool_slice.Mu.Lock()
	bool_slice.BoolSlice[index] = true
	// bool_slice.Mu.Unlock()
	for _, value := range bool_slice.BoolSlice {
		if !value {
			return
		}
	}
	bool_slice.AllTrue = true
}

// return File Name, Index, Error
func (c *Coordinator) GetNextFileName(worker_number int) (string, error) {
	var index int = c.PendingReadFilesIndex.GetAndIncrementIndex()
	if index >= len(c.Files) {
		return "", errors.New("No more file to process")
	}
	file_name := c.Files[index]
	c.AssignedFileIndexesFromWorkerIndex.AddMapEntry(worker_number, file_name)
	return file_name, nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RPC handler that initialize a worker
func (c *Coordinator) InitializeWorker(args *InitializeWorkerArgs, reply *InitializeWorkerReply) error {
	var worker_index int = c.PendingWorkerIndexToAllocate.GetAndIncrementIndex()
	reply.nReduce = c.nReduce
	reply.WorkerNumber = worker_index
	if worker_index >= c.nReduce {
		log.Fatalf("Allocated too much worker threads")
		return errors.New("Allocated too much worker threads")
	}
	return nil
}

// RPC handler that replies the name of a file that will be handled by
// the worker thread
func (c *Coordinator) NextFileNameToHandle(args *GetNextFileNameToHandleArgs, reply *GetNextFileNameToHandleReply) error {
	file_name, e := c.GetNextFileName(args.WorkerNumber)
	if e != nil {
		reply.FileName = ""
		reply.WaitForNextStage = true
	} else {
		reply.FileName = file_name
		reply.WaitForNextStage = false
	}
	return nil
}

// RPC handler that indicates that a worker has completed its Map task
func (c *Coordinator) WorkerMapTaskCompletion(args *WorkerMapTaskCompletionArgs, reply *WorkerMapTaskCompletionReply) error {
	c.WorkerMapTaskCompletionStatus.SetIndexTrue(args.WorkerNumber)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce, files, MutexedInt{Index: 0}, MutexedInt{Index: 0}, Mutexed2DString{Map: make([][]string, nReduce)}, MutexedBoolSlice{BoolSlice: make([]bool, nReduce), AllTrue: false}}

	for i := 0; i < nReduce; i++ {
		c.WorkerMapTaskCompletionStatus.BoolSlice[i] = false
	}

	c.server()
	return &c
}
