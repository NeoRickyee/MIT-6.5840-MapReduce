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
	nReduce               int
	Files                 []string
	PendingReadFilesIndex FileIndex
}

type FileIndex struct {
	Mu    sync.Mutex
	Index int
}

func (i *FileIndex) GetAndIncrementIndex() int {
	i.Mu.Lock()
	defer func() {
		i.Index++
		i.Mu.Unlock()
	}()
	return i.Index
}

// return File Name, Index, Error
func (c *Coordinator) GetNextFileName() (string, int, error) {
	var index int = c.PendingReadFilesIndex.GetAndIncrementIndex()
	if index >= len(c.Files) {
		return "", index, errors.New("No more file to process")
	}
	file_name := c.Files[index]
	return file_name, index, nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RPC handler that replies the name of a file that will be handled by
// the worker thread
func (c *Coordinator) NextFileNameToHandle(args *GetNextFileNameToHandleArgs, reply *GetNextFileNameToHandleReply) error {
	file_name, index, e := c.GetNextFileName()
	if e != nil {
		return e
	}
	reply.FileName = file_name
	reply.MapTaskNumber = index
	reply.nReduce = c.nReduce
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
	c := Coordinator{nReduce, files, FileIndex{Index: 0}}

	// Your code here.

	c.server()
	return &c
}
