package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	Files                 []string
	PendingReadFilesIndex int
}

func (c *Coordinator) GetNextFileName() (string, error) {
	if c.PendingReadFilesIndex >= len(c.Files) {
		return "", errors.New("No more file to process")
	}
	file_name := c.Files[c.PendingReadFilesIndex]
	c.PendingReadFilesIndex++
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

// RPC handler that replies the name of a file that will be handled by
// the worker thread
func (c *Coordinator) NextFileNameToHandle(args *GetNextFileNameToHandleArgs, reply *GetNextFileNameToHandleReply) error {
	file_name, e := c.GetNextFileName()
	if e != nil {
		return e
	}
	reply.FileName = file_name
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
	c := Coordinator{files, 0}

	// Your code here.

	c.server()
	return &c
}
