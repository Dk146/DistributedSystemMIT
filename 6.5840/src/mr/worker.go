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
	CallExample()
	for {
		askJobReply := AskJob()
		if askJobReply.FileName == "" {
			break
		}
		mapTask(askJobReply.FileName, mapf, askJobReply.MapNumber, askJobReply.NReduce)
	}
}

func AskJob() *AskJobReply {
	args := AskJobArgs{}
	args.X = 2
	reply := AskJobReply{}
	ok := call("Coordinator.AskJob", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
	fmt.Println("CHEHEE", reply.NReduce)
	return &reply
}

func AckJob() {

}

func mapTask(filename string, mapf func(string, string) []KeyValue, taskNumber, nReduce int) {
	content, err := readFile(filename)
	if err != nil {
		return
	}
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	fmt.Println(kva)
	// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
	writeFiles(kva, taskNumber, nReduce)
}

func createFiles(mapNumber, reduceNumber int) []string {
	res := make([]string, mapNumber*reduceNumber)
	for i := 0; i < mapNumber; i++ {
		for j := 0; j < reduceNumber; j++ {
			filename := fmt.Sprintf("mr-%v-%v", i, j)
			res = append(res, filename)
		}
	}
	return res
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

// func openFiles(mapNumber, reduceNumber int) error {
// 	for i := 0; i < mapNumber; i++ {
// 		for j := 0; j < reduceNumber; j++ {
// 			filename := fmt.Sprintf("mr-%v-%v", i, j)
// 			file, err := os.Open(filename) // For read access.
// 			if err != nil {
// 				log.Fatal(err)
// 			}
// 		}
// 	}
// }

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
