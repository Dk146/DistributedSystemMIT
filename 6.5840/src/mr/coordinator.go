package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Task int64

const (
	MapTask Task = iota
	ReduceTask
)

type Status int64

const (
	Unstarted Status = iota
	Pending
	Done
)

type Coordinator struct {
	// Your definitions here.
	// Map task number and filename
	MapJobs    map[int]*TaskStatus
	turn       int
	nReduce    int
	currentJob Task
}

type TaskStatus struct {
	filename string
	status   Status
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskJob(args *AskJobArgs, reply *AskJobReply) error {
	reply.FileName, reply.MapNumber = c.GetUnstartedJob()
	reply.TaskType = MapTask
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) GetUnstartedJob() (string, int) {
	for k, v := range c.MapJobs {
		if v.status == Unstarted {
			fmt.Println("key", k, "   val", v.status)
			v.status = Pending
			return v.filename, k
		}
	}
	return "", -1
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
	c := Coordinator{}

	// Your code here.
	c.MapJobs = make(map[int]*TaskStatus)
	pos := 0
	for _, v := range files {
		c.MapJobs[pos] = &TaskStatus{filename: v, status: Unstarted}
		pos++
	}
	c.nReduce = nReduce
	c.currentJob = MapTask

	c.server()
	return &c
}
