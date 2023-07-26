package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"now"
	"os"
	"sync"
)

type Coordinator struct {
	nReduce                 int
	Files                   []string
	WorkerActivityRecorder  ActivityRecorder
	MapTaskDistributor      WorkIndexDistributor
	MapTaskCompletionStatus MutexedBoolSlice
	ReduceDistributor       WorkIndexDistributor
	ReduceCompletionStatus  MutexedBoolSlice
}

type ActivityRecorder struct {
	LastCommTime        []int64
	ThreadAliveStatus   []bool
	MuThreadStatus      sync.Mutex
	NextIndexToAllocate int
	MuIndexToAllocate   sync.Mutex
}

func (recorder *ActivityRecorder) InitializeActivityRecorder(nReduce int) {
	recorder.LastCommTime = make([]int64, nReduce)
	recorder.ThreadAliveStatus = make([]bool, nReduce)
	for i := 0; i < nReduce; i++ {
		recorder.LastCommTime[i] = 0
		recorder.ThreadAliveStatus[i] = false
	}
	recorder.NextIndexToAllocate = 0
}

func (recorder *ActivityRecorder) RecordCurrentCommTime(worker_index int) bool {
	var current_seconds int64 = now.Unix()
	if (current_seconds - recorder.LastCommTime[worker_index]) > 10 {
		// current worker thread is possible to be declared dead
		// No longer accepting tasks
		return false
	} else {
		recorder.LastCommTime[worker_index] = now.Unix()
		return true
	}
}

func (recorder *ActivityRecorder) GetNewDeadThreads(nReduce int) *[]int {
	var current_seconds int64 = now.Unix()
	var all_dead_threads []int
	for i := 0; i < nReduce; i++ {
		if (current_seconds - recorder.LastCommTime[i]) > 10 {
			all_dead_threads = append(all_dead_threads, i)
		}
	}
	recorder.MuThreadStatus.Lock()
	defer recorder.MuThreadStatus.Unlock()
	// TODO: return new dead threads
	var new_dead_threads []int
	for _, dead_thread_index := range all_dead_threads {
		if recorder.ThreadAliveStatus[dead_thread_index] {
			new_dead_threads = append(new_dead_threads, dead_thread_index)
			recorder.ThreadAliveStatus[dead_thread_index] = false
		}
	}
	return &new_dead_threads
}

// worker index, allocation successful
func (recorder *ActivityRecorder) GetNewWorkerIndex(nReduce int) (int, bool) {
	recorder.MuThreadStatus.Lock()
	defer recorder.MuThreadStatus.Unlock()

	recorder.MuIndexToAllocate.Lock()
	allocated_worker_index := recorder.NextIndexToAllocate
	if allocated_worker_index >= nReduce {
		recorder.MuIndexToAllocate.Unlock()
		return -1, false
	}
	recorder.NextIndexToAllocate++
	recorder.MuIndexToAllocate.Unlock()

	recorder.LastCommTime[allocated_worker_index] = now.UNIX()
	recorder.ThreadAliveStatus[allocated_worker_index] = true
	return allocated_worker_index, true
}

// might not be easy to get all dead threads with this setup
// Race condition

type WorkIndexDistributor struct {
	AllRemainingWork      []int
	WorksAssignedToWorker [][]int
	MuToRetrieveWork      sync.Mutex
}

func (distributor *WorkIndexDistributor) InitializeWorkDistributor(nReduce int, total_work_amount int) {
	distributor.AllRemainingWork = make([]int, total_work_amount)
	for i := 0; i < total_work_amount; i++ {
		distributor.AllRemainingWork[i] = i
	}
	distributor.WorksAssignedToWorker = make([][]int, nReduce)
}

func (distributor *WorkIndexDistributor) WorkerDeathProcessing(dead_worker_index int) {
	distributor.MuToRetrieveWork.Lock()
	defer distributor.MuToRetrieveWork.Unlock()
	works_assigned_to_dead_worker := distributor.WorksAssignedToWorker[dead_worker_index]
	distributor.AllRemainingWork = append(distributor.AllRemainingWork, works_assigned_to_dead_worker...)
	distributor.WorksAssignedToWorker[dead_worker_index] = nil
}

func (distributor *WorkIndexDistributor) GetWork(worker_index int) (int, bool) {
	distributor.MuToRetrieveWork.Lock()
	defer distributor.MuToRetrieveWork.Unlock()
	if len(distributor.AllRemainingWork) == 0 {
		return -1, true
	}
	work_to_distribute := distributor.AllRemainingWork[0]
	distributor.WorksAssignedToWorker[worker_index] = append(distributor.WorksAssignedToWorker[worker_index], work_to_distribute)
	distributor.AllRemainingWork = distributor.AllRemainingWork[1:]
	return work_to_distribute, false
}

type MutexedBoolSlice struct {
	Mu        sync.Mutex
	BoolSlice []bool
	AllTrue   bool
}

func (bool_slice *MutexedBoolSlice) Initialize(nReduce int) {
	bool_slice.BoolSlice = make([]bool, nReduce)
	for i := 0; i < nReduce; i++ {
		bool_slice.BoolSlice[i] = false
	}
	bool_slice.AllTrue = false
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

// return Index, If no more file left, worker is already dead
func (c *Coordinator) GetNextTaskIndex(activity_recorder *ActivityRecorder, task_distributor *WorkIndexDistributor, worker_index int) (int, bool, bool) {
	var worker_declared_dead bool = activity_recorder.RecordCurrentCommTime(worker_index)
	if worker_declared_dead {
		return -1, true, true
	}
	task_index, no_remaining_task := task_distributor.GetWork(worker_index)
	if !no_remaining_task {
		return task_index, false, false
	}
	// if there is currently no remaining task
	// check if any worker thread newly dead, take over task
	new_dead_threads := activity_recorder.GetNewDeadThreads(c.nReduce)
	for _, dead_thread_index := range *new_dead_threads {
		task_distributor.WorkerDeathProcessing(dead_thread_index)
	}
	task_index, no_remaining_task = task_distributor.GetWork(worker_index)
	return task_index, true, false
}

func (c *Coordinator) AllThreadsCompleteCurrentTask(activity_recorder *ActivityRecorder)

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
	worker_index, allocation_successful := c.WorkerActivityRecorder.GetNewWorkerIndex(c.nReduce)
	if !allocation_successful {
		reply.TerminateWorker = true
	}
	reply.TerminateWorker = false
	reply.NReduce = c.nReduce
	reply.WorkerNumber = worker_index
	return nil
}

// RPC handler that replies the name of a file that will be handled by
// the worker thread
func (c *Coordinator) NextFileNameToHandle(args *GetNextFileNameToHandleArgs, reply *GetNextFileNameToHandleReply) error {
	worker_index := args.WorkerNumber
	map_task_index, no_map_task_left, worker_has_been_dead := c.GetNextTaskIndex(&c.WorkerActivityRecorder, &c.MapTaskDistributor, worker_index)
	if worker_has_been_dead {
		reply.TerminateWorker = true
	}
	reply.TerminateWorker = false
	if no_map_task_left {
		reply.WaitForNextStage = true
	}
	reply.WaitForNextStage = false
	file_name := c.Files[map_task_index]
	reply.FileName = file_name
	return nil
}

// RPC handler that indicates that a worker has completed its Map task
func (c *Coordinator) WorkerMapTaskCompletion(args *WorkerMapTaskCompletionArgs, reply *WorkerMapTaskCompletionReply) error {
	c.MapTaskCompletionStatus.SetIndexTrue(args.WorkerNumber)
	return nil
}

// RPC handler that indicates that a worker can start Reduce task
func (c *Coordinator) WorkerStartReduceTask(args *WorkerWaitForReduceTaskArgs, reply *WorkerWaitForReduceTaskReply) error {
	if c.MapTaskCompletionStatus.AllTrue {
		// TODO: distribute Reduce task
		if args.WaitingForFirstReduceTask {
			reply.StartReduceTask = true
			reply.NewReduceTaskNumber = args.WorkerNumber
			reply.NoReduceTaskLeft = false
			return nil
		}
		if c.ReduceCompletionStatus.AllTrue {
			reply.NoReduceTaskLeft = true
			return nil
		}
		// TODO: pause to wait if necessary
		// TODO: distribute reduce tasks
		reply.StartReduceTask = true
		reply.NewReduceTaskNumber = args.WorkerNumber
	} else {
		reply.StartReduceTask = false
	}
	return nil
}

func (c *Coordinator) ReduceCompletion(args *ReduceCompletionArgs, reply *ReduceCompletionReply) error {
	c.ReduceCompletionStatus.SetIndexTrue(args.WorkerNumber)
	// TODO: use WorkerCompletionReply to let Worker take over other Reduce tasks
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
	// TODO: detect completion status passed to all workers
	return c.ReduceCompletionStatus.AllTrue
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, n_reduce int) *Coordinator {
	c := &Coordinator{
		nReduce:                            n_reduce,
		Files:                              files,
		PendingWorkerIndexToAllocate:       MutexedInt{Index: 0},
		PendingReadFilesIndex:              MutexedInt{Index: 0},
		AssignedFileIndexesFromWorkerIndex: Mutexed2DString{Map: make([][]string, n_reduce)},
		WorkerMapTaskCompletionStatus:      MutexedBoolSlice{BoolSlice: make([]bool, n_reduce), AllTrue: false},
		ReduceCompletionStatus:             MutexedBoolSlice{BoolSlice: make([]bool, n_reduce), AllTrue: false},
	}

	for i := 0; i < n_reduce; i++ {
		c.WorkerMapTaskCompletionStatus.BoolSlice[i] = false
		c.ReduceCompletionStatus.BoolSlice[i] = false
	}

	c.server()
	return c
}
