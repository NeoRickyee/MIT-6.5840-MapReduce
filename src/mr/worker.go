package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var nReduce int = 0
	var worker_index int
	var initialization_successful bool
	nReduce, worker_index, initialization_successful = Initialize()
	if !initialization_successful {
		return
	}
	if nReduce == 0 {
		log.Fatalf("nReduce value is 0")
		return
	}

	// create a loop starts here, to catch crashing worker

	for {
		intermediate := []KeyValue{}
		var file_names []string
		var map_task_index int
		var wait_for_task, go_to_reduce, terminate_worker bool
		for {
			file_names, map_task_index, wait_for_task, go_to_reduce, terminate_worker = FetchFileNamesToMap(worker_index)
			if terminate_worker {
				return
			}
			if !wait_for_task || go_to_reduce {
				break
			}
			time.Sleep(1 * time.Second) // Sleep for 1 second
		}
		if go_to_reduce {
			break
		}

		for _, file_name := range file_names {
			WorkerMapTask(mapf, file_name, &intermediate)
		}
		sort.Sort(ByKey(intermediate))
		StoreIntermediateToDisk(intermediate, nReduce, map_task_index)
		terminate_worker = IndicateMapTaskCompletion(worker_index, map_task_index)
		if terminate_worker {
			return
		}
	}

	// create a loop ends here, to catch crashing worker
	for {
		var reduce_task_index int
		var wait_for_task, terminate_worker bool
		for {
			reduce_task_index, wait_for_task, terminate_worker = FetchReduceTaskIndex(worker_index)
			if terminate_worker {
				return
			}
			if !wait_for_task {
				break
			}
			time.Sleep(1 * time.Second) // Sleep for 1 second
		}
		WorkerReduceTask(reducef, reduce_task_index, nReduce)
		terminate_worker = IndicateReduceTaskCompletion(worker_index, reduce_task_index)
		if terminate_worker {
			return
		}
	}
}

func WorkerMapTask(mapf func(string, string) []KeyValue, file_name string, intermediate *[]KeyValue) {
	// TODO: create failure condition for no Map task distributed
	if file_name == "" {
		log.Fatalf("Map task started but input file name is empty")
		return
	}

	// fmt.Printf("Fetched Map file name: %s, Total threads: %v\n", file_name, nReduce)

	file, err := os.Open(file_name)
	if err != nil {
		log.Fatalf("cannot open %v", file_name)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file_name)
		return
	}
	file.Close()
	kva := mapf(file_name, string(content))
	*intermediate = append(*intermediate, kva...)
}

func StoreIntermediateToDisk(intermediate []KeyValue, nReduce int, worker_index int) {
	// TODO: add error handling
	intermediate_for_each_worker := make([][]KeyValue, nReduce)
	for _, key_value := range intermediate {
		var map_task_number int = ihash(key_value.Key) % nReduce
		intermediate_for_each_worker[map_task_number] = append(intermediate_for_each_worker[map_task_number], key_value)
	}

	for i := 0; i < nReduce; i++ {
		var file_name string = "mr-"
		file_name = file_name + strconv.Itoa(worker_index) + "-" + strconv.Itoa(i)
		// "mr-X-Y"
		ofile, _ := os.Create(file_name)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate_for_each_worker[i] {
			enc.Encode(&kv)
		}
		ofile.Close()
	}
}

func WorkerReduceTask(reducef func(string, []string) string, worker_index int, nReduce int) {
	// read all files and combine inputs
	var kva []KeyValue
	for i := 0; i < nReduce; i++ {
		var file_name string = "mr-"
		file_name = file_name + strconv.Itoa(i) + "-" + strconv.Itoa(worker_index)
		// "mr-X-Y"
		file, err := os.Open(file_name)
		if err != nil {
			log.Fatalf("cannot open %v", file_name)
			// Could just be this Worker crashed before completing Map Task
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))

	// execute Reduce, and write result to file
	oname := "mr-out-" + strconv.Itoa(worker_index)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		//fmt.Printf("reply.Y %v\n", reply.Y)
		log.Fatalf("reply.Y %v\n", reply.Y)
	} else {
		log.Fatalf("call failed!\n")
	}
}

// Return nReduce, WorkerNumber, Initilization successful
func Initialize() (int, int, bool) {
	args := InitializeWorkerArgs{}
	reply := InitializeWorkerReply{}

	ok := call("Coordinator.InitializeWorker", &args, &reply)
	if ok {
		// fmt.Printf("reply.NReduce value is %v, reply.WorkerNumber is %v.\n", reply.NReduce, reply.WorkerNumber)
		return reply.NReduce, reply.WorkerNumber, !reply.TerminateWorker
	} else {
		fmt.Printf("Worker initialization failed!\n")
	}
	return 0, 1, false
}

// return: file_names, work_index, wait_for_task, go_to_reduce, terminate_worker
func FetchFileNamesToMap(worker_index int) ([]string, int, bool, bool, bool) {
	args := GetNextFileNamesToHandleArgs{worker_index}
	reply := GetNextFileNamesToHandleReply{}

	ok := call("Coordinator.NextFileNameToHandle", &args, &reply)
	if ok {
		return reply.FileNames, reply.MapTaskIndex, reply.WaitForTask, reply.StartReduceTask, reply.TerminateWorker
	} else {
		fmt.Printf("Worker fetch Map file name failed!\n")
	}
	return []string{}, 0, false, false, true
}

// return: terminate_worker
func IndicateMapTaskCompletion(worker_index int, map_task_index int) bool {
	args := WorkerMapTaskCompletionArgs{worker_index, map_task_index}
	reply := WorkerMapTaskCompletionReply{}
	// fmt.Println("Map Task Complete for worker number:", worker_index)
	ok := call("Coordinator.WorkerMapTaskCompletion", &args, &reply)
	if !ok {
		fmt.Printf("Worker indicates Map task completion failed!\n")
	}
	return reply.TerminateWorker
}

// return: reduce_task_index, wait_for_task, terminate_worker
func FetchReduceTaskIndex(worker_index int) (int, bool, bool) {
	args := GetNextReduceTaskArgs{worker_index}
	reply := GetNextFileNamesToHandleReply{}

	ok := call("Coordinator.NextReduceTaskToStart", &args, &reply)
	if ok {
		return reply.MapTaskIndex, reply.WaitForTask, reply.TerminateWorker
	} else {
		fmt.Printf("Worker Reduce task start failed!\n")
	}
	return 0, false, true
}

func IndicateReduceTaskCompletion(worker_index int, reduce_task_index int) bool {
	args := ReduceCompletionArgs{worker_index, reduce_task_index}
	reply := ReduceCompletionReply{}

	ok := call("Coordinator.ReduceCompletion", &args, &reply)
	if ok {
		return reply.TerminateWorker
	} else {
		fmt.Printf("Worker indicates Reduce completion failed!\n")
		return true
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
