package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	nReduce                int
	Files                  []string
	WorkerActivityRecorder ActivityRecorder
	MapTaskDistributor     WorkIndexDistributor
	//MapTaskCompletionStatus MutexedBoolSlice
	ReduceDistributor WorkIndexDistributor
	//ReduceCompletionStatus  MutexedBoolSlice
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
	var current_seconds int64 = time.Now().Unix()
	if (current_seconds - recorder.LastCommTime[worker_index]) > 10 {
		// current worker thread is possible to be declared dead
		// No longer accepting tasks
		return false
	} else {
		recorder.LastCommTime[worker_index] = time.Now().Unix()
		return true
	}
}

func (recorder *ActivityRecorder) GetNewDeadThreads(nReduce int) *[]int {
	var current_seconds int64 = time.Now().Unix()
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

	recorder.LastCommTime[allocated_worker_index] = time.Now().Unix()
	recorder.ThreadAliveStatus[allocated_worker_index] = true
	return allocated_worker_index, true
}

// might not be easy to get all dead threads with this setup
// Race condition

type WorkIndexDistributor struct {
	AllRemainingWork      []int
	WorksAssignedToWorker [][]int
	MuToRetrieveWork      sync.Mutex
	WorkCompleted         []bool
	MuWorkCompleted       sync.Mutex
}

func (distributor *WorkIndexDistributor) InitializeWorkDistributor(nReduce int, total_work_amount int) {
	distributor.AllRemainingWork = make([]int, total_work_amount)
	distributor.WorkCompleted = make([]bool, total_work_amount)
	for i := 0; i < total_work_amount; i++ {
		distributor.AllRemainingWork[i] = i
		distributor.WorkCompleted[i] = false
	}
	distributor.WorksAssignedToWorker = make([][]int, nReduce)
}

func (distributor *WorkIndexDistributor) WorkerDeathProcessing(dead_worker_index int) {
	distributor.MuToRetrieveWork.Lock()
	defer distributor.MuToRetrieveWork.Unlock()
	var last_work_assigned_to_dead_worker int
	total_work_assigned_to_dead_worker := len(distributor.WorksAssignedToWorker[dead_worker_index])
	if total_work_assigned_to_dead_worker == 0 {
		return
	}
	last_work_assigned_to_dead_worker = distributor.WorksAssignedToWorker[dead_worker_index][total_work_assigned_to_dead_worker-1]
	distributor.MuWorkCompleted.Lock()
	if distributor.WorkCompleted[last_work_assigned_to_dead_worker] {
		return
	}
	distributor.MuWorkCompleted.Unlock()

	distributor.AllRemainingWork = append(distributor.AllRemainingWork, last_work_assigned_to_dead_worker)
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

func (distributor *WorkIndexDistributor) SetWorkComplete(work_index int) {
	distributor.MuWorkCompleted.Lock()
	defer distributor.MuWorkCompleted.Unlock()
	distributor.WorkCompleted[work_index] = true
}

func (distributor *WorkIndexDistributor) AllWorkComplete() bool {
	distributor.MuWorkCompleted.Lock()
	for _, val := range distributor.WorkCompleted {
		if !val {
			return false
		}
	}
	distributor.MuWorkCompleted.Unlock()
	return true
}

func (c *Coordinator) GetListOfFileNamesForEachWorkerIndex(worker_index int) []string {
	total_file_count := len(c.Files)
	var each_worker_file_count int = (total_file_count + c.nReduce - 1) / c.nReduce
	var starting_index int = worker_index * each_worker_file_count
	if starting_index >= total_file_count {
		log.Fatal("starting index larger than total length")
	}
	var ending_index int = starting_index + total_file_count
	if ending_index > total_file_count {
		ending_index = total_file_count
	}
	var distributed_file_names []string = c.Files[starting_index:ending_index]
	return distributed_file_names
}

// return: Index, If no more file left, worker is already dead
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
		return nil
	}
	reply.TerminateWorker = false
	reply.NReduce = c.nReduce
	reply.WorkerNumber = worker_index
	return nil
}

// RPC handler that replies the name of a file that will be handled by
// the worker thread
func (c *Coordinator) NextFileNamesToHandle(args *GetNextFileNamesToHandleArgs, reply *GetNextFileNamesToHandleReply) error {
	worker_index := args.WorkerNumber
	map_task_index, no_map_task_left, worker_has_been_dead := c.GetNextTaskIndex(&c.WorkerActivityRecorder, &c.MapTaskDistributor, worker_index)
	if worker_has_been_dead {
		reply.TerminateWorker = true
		return nil
	}
	reply.TerminateWorker = false
	if no_map_task_left {
		if c.MapTaskDistributor.AllWorkComplete() {
			reply.StartReduceTask = true
			reply.WaitForTask = false
		} else {
			reply.WaitForTask = true
			reply.StartReduceTask = false
		}
		return nil
	}
	reply.WaitForTask = false
	reply.StartReduceTask = false
	distributed_file_names := c.GetListOfFileNamesForEachWorkerIndex(map_task_index)
	reply.FileNames = distributed_file_names
	reply.MapTaskIndex = map_task_index
	return nil
}

// RPC handler that indicates that a worker has completed its Map task
func (c *Coordinator) WorkerMapTaskCompletion(args *WorkerMapTaskCompletionArgs, reply *WorkerMapTaskCompletionReply) error {
	var worker_declared_dead bool = c.WorkerActivityRecorder.RecordCurrentCommTime(args.WorkerNumber)
	if worker_declared_dead {
		reply.TerminateWorker = true
		return nil
	}
	reply.TerminateWorker = false
	c.MapTaskDistributor.SetWorkComplete(args.MapTaskIndex)
	return nil
}

func (c *Coordinator) NextReduceTaskToStart(arg *GetNextReduceTaskArgs, reply *GetNextReduceTaskReply) error {
	worker_index := arg.WorkerNumber
	reduce_task_index, no_reduce_task_left, worker_has_been_dead := c.GetNextTaskIndex(&c.WorkerActivityRecorder, &c.ReduceDistributor, worker_index)
	if worker_has_been_dead {
		reply.TerminateWorker = true
		return nil
	}
	reply.TerminateWorker = false
	if no_reduce_task_left {
		if c.ReduceDistributor.AllWorkComplete() {
			reply.TerminateWorker = true
			reply.WaitForTask = false
		} else {
			reply.WaitForTask = true
		}
		return nil
	}
	reply.WaitForTask = false
	reply.ReduceTaskIndex = reduce_task_index
	return nil
}

func (c *Coordinator) ReduceCompletion(args *ReduceCompletionArgs, reply *ReduceCompletionReply) error {
	var worker_declared_dead bool = c.WorkerActivityRecorder.RecordCurrentCommTime(args.WorkerNumber)
	if worker_declared_dead {
		reply.TerminateWorker = true
		return nil
	}
	reply.TerminateWorker = false
	c.ReduceDistributor.SetWorkComplete(args.ReduceTaskIndex)
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
	return c.ReduceDistributor.AllWorkComplete()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, n_reduce int) *Coordinator {
	c := &Coordinator{
		nReduce: n_reduce,
		Files:   files,
	}
	c.WorkerActivityRecorder.InitializeActivityRecorder(n_reduce)
	c.MapTaskDistributor.InitializeWorkDistributor(n_reduce, n_reduce)
	//c.MapTaskCompletionStatus.Initialize(n_reduce)
	c.ReduceDistributor.InitializeWorkDistributor(n_reduce, n_reduce)
	//c.ReduceCompletionStatus.Initialize(n_reduce)

	c.server()
	return c
}
