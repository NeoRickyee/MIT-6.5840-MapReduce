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
	nReduce      int
	WorkerNumber int
}

// RPC arguments to get the next file name to handle
type GetNextFileNameToHandleArgs struct {
	WorkerNumber int
}
type GetNextFileNameToHandleReply struct {
	FileName         string
	WaitForNextStage bool
}

type WorkerMapTaskCompletionArgs struct {
	WorkerNumber int
}
type WorkerMapTaskCompletionReply struct{}

type WorkerWaitForReduceTaskArgs struct {
	WorkerNumber              int
	WaitingForFirstReduceTask bool
}
type WorkerWaitForReduceTaskReply struct {
	StartReduceTask     bool
	NewReduceTaskNumber int
	NoReduceTaskLeft    bool
}

type ReduceCompletionArgs struct {
	WorkerNumber int
}
type ReduceCompletionReply struct {
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
