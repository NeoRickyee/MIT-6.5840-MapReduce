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
	nReduce, worker_index = Initialize()
	if nReduce == 0 {
		// TODO: use the function that informs coordinator for termination
	}
	intermediate := []KeyValue{}

	for {
		file_name, wait_for_next_stage := FetchFileNameToMap(worker_index)
		if wait_for_next_stage {
			break
		}
		WorkerMapTask(mapf, file_name, &intermediate)
	}
	sort.Sort(ByKey(intermediate))
	StoreIntermediateToDisk(intermediate, nReduce, worker_index)
	IndicateMapTaskCompletion(worker_index)

	WaitForReduceTask(worker_index)
	// Permission to start Reduce task is received

	// TODO: start Reduce task
	WorkerReduceTask(reducef, worker_index, nReduce)
	// TODO: think about the case where this Worker needs to take over another Reduce task

	// TODO: indicate to Coordinator that this Worker is Done
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
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// Return nReduce, WorkerNumber
func Initialize() (int, int) {
	args := InitializeWorkerArgs{}
	reply := InitializeWorkerReply{}

	ok := call("Coordinator.InitializeWorker", &args, &reply)
	if ok {
		return reply.nReduce, reply.WorkerNumber
	} else {
		fmt.Printf("Worker initialization failed!\n")
	}
	return 0, 1
}

func FetchFileNameToMap(worker_index int) (string, bool) {
	args := GetNextFileNameToHandleArgs{worker_index}
	reply := GetNextFileNameToHandleReply{}

	ok := call("Coordinator.NextFileNameToHandle", &args, &reply)
	if ok {
		return reply.FileName, reply.WaitForNextStage
	} else {
		fmt.Printf("Worker fetch Map file name failed!\n")
	}
	return "", false
}

func IndicateMapTaskCompletion(worker_index int) {
	args := WorkerMapTaskCompletionArgs{worker_index}
	reply := WorkerMapTaskCompletionReply{}

	ok := call("Coordinator.WorkerMapTaskCompletion", &args, &reply)
	if !ok {
		fmt.Printf("Worker indicates Map task completion failed!\n")
	}
	return
}

func WaitForReduceTask(worker_index int) {
	args := WorkerWaitForReduceTaskArgs{worker_index}
	reply := WorkerWaitForReduceTaskReply{}

	for {
		time.Sleep(1 * time.Second) // Sleep for 1 second
		ok := call("Coordinator.WorkerStartReduceTask", &args, &reply)
		if !ok {
			fmt.Printf("Worker wait for reduce task failed!\n")
		}
		if reply.StartReduceTask {
			break
		}
	}
	return
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
