package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	// Map task number and filename
	unstartedJob map[int]string
	turn         int
	nReduce      int
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
	reply.FileName, _ = c.GetUnstartedJob()
	reply.JobType = false
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) GetUnstartedJob() (string, int) {
	c.turn++
	pos := c.turn % c.nReduce
	return c.unstartedJob[pos], pos
}

func (c *Coordinator) SetUnstartedJob(files []string) {
	// c.unstartedJob = files
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
	c.unstartedJob = make(map[int]string)
	pos := 0
	for _, v := range files {
		c.unstartedJob[pos] = v
		pos++
	}
	c.nReduce = nReduce

	c.server()
	return &c
}
