package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Coordinator struct {
	// Your definitions here.
	Jobs          []string
	JobList       []Job
	ReduceKeyList []int
	NReduce       int
}

type Job struct {
	Id        int
	FileName  string
	IsMap     bool
	ReduceKey int
	Status    bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) DistributeJob(args *Request, reply *Reply) error {
	reduceKey := 0
	jobId := 0

	for i := range c.ReduceKeyList {
		if c.ReduceKeyList[i] == 0 {
			fmt.Println(i)
			reduceKey = i
			c.ReduceKeyList[i] = 1
			break
		}
	}

	for i := range c.JobList {
		if c.JobList[i].FileName == "" {
			jobId = i
			break
		}
	}

	job := Job{
		Id:        jobId,
		ReduceKey: reduceKey,
		FileName:  c.Jobs[reduceKey],
		IsMap:     true,
		Status:    false,
	}

	c.JobList[job.Id] = job

	reduceKey %= c.NReduce
	reply.Job = job
	reply.NReduce = c.NReduce

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	fmt.Print("注册rpc")
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	c.Jobs = files
	c.JobList = make([]Job, len(files))
	c.ReduceKeyList = make([]int, len(files))

	c.server()
	return &c
}
