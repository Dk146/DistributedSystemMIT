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
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type MRWorker struct {
	JobType    string
	TaskNumber int
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		askJobReply := AskJob()

		if askJobReply.TaskType == MapTask {
			if askJobReply.FileName != "" {
				mapTask(askJobReply.FileName, mapf, askJobReply.TaskNumber, askJobReply.NReduce)
				AckJob(askJobReply.TaskNumber, MapTask)
			} else {
				time.Sleep(1 * time.Second)
				fmt.Println("Waiting")
			}
		} else {
			if askJobReply.FileName != "" && askJobReply.TaskNumber >= 0 {
				reduceTask(askJobReply.FileName, reducef, askJobReply.TaskNumber, askJobReply.NReduce)
				AckJob(askJobReply.TaskNumber, ReduceTask)
			} else if askJobReply.TaskNumber == -2 {
				fmt.Println("check")
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}
	}
	fmt.Println("Check")
}

func AskJob() *AskJobReply {
	args := AskJobArgs{}
	args.X = 2
	reply := AskJobReply{}
	ok := call("Coordinator.AskJob", &args, &reply)
	if !ok {
		return nil
	}
	return &reply
}

func AckJob(taskNumber int, taskType Task) bool {
	args := AckJobRequest{TaskNumber: taskNumber, TaskType: taskType}
	reply := AckJobResponse{}
	return call("Coordinator.AckJob", &args, &reply)
}

func mapTask(filename string, mapf func(string, string) []KeyValue, taskNumber, nReduce int) {
	content, err := readFile(filename)
	if err != nil {
		return
	}
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	writeFiles(kva, taskNumber, nReduce)
}

func writeFiles(intermediate []KeyValue, workerNumber, nReduce int) {
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []KeyValue{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k])
		}

		// If the file doesn't exist, create it, or append to the file
		filename := fmt.Sprintf("mr-%v-%v", workerNumber, ihash(intermediate[i].Key)%nReduce)
		f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range values {
			err := enc.Encode(&kv)
			if err != nil {
				return
			}
		}
		f.Close()

		i = j
	}
}

func reduceTask(reduceTask string, reducef func(string, []string) string, taskNumber, nReduce int) {
	kva := []KeyValue{}
	for i := 0; i < 8; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, reduceTask)
		// fmt.Println(reduceTask)
		fmt.Println(filename)
		file, err := os.OpenFile(filename, os.O_RDWR, 0644)
		if err != nil {
			// fmt.Println(filename)
			// fmt.Println("Err open file")
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
		// fmt.Println("len temp: ", len(kva))
	}

	sort.Sort(ByKey(kva))
	// fmt.Println("len: ", len(kva))

	oname := "mr-out-" + reduceTask
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-.
	//
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
	fmt.Println(oname)
}

func readFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return nil, err
	}
	file.Close()
	return content, nil
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
