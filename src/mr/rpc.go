package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// RPC arguments to initialize a worker
type InitializeWorkerArgs struct{}
type InitializeWorkerReply struct {
	NReduce         int
	WorkerNumber    int
	TerminateWorker bool
}

// RPC arguments to get the next file name to handle
type GetNextFileNamesToHandleArgs struct {
	WorkerNumber int
}
type GetNextFileNamesToHandleReply struct {
	FileNames       []string
	MapTaskIndex    int
	WaitForTask     bool
	StartReduceTask bool
	TerminateWorker bool
}

type WorkerMapTaskCompletionArgs struct {
	WorkerNumber int
	MapTaskIndex int
}
type WorkerMapTaskCompletionReply struct {
	TerminateWorker bool
}

type GetNextReduceTaskArgs struct {
	WorkerNumber int
}
type GetNextReduceTaskReply struct {
	ReduceTaskIndex int
	WaitForTask     bool
	TerminateWorker bool
}

type ReduceCompletionArgs struct {
	WorkerNumber    int
	ReduceTaskIndex int
}
type ReduceCompletionReply struct {
	TerminateWorker bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
