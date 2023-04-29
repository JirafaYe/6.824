package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

var JobId int
var mutex sync.RWMutex

type Coordinator struct {
	// Your definitions here.
	MapChannel    chan *Job
	ReduceChannel chan *Job
	IsMapStatus   bool
	JobManager    map[int]*JobManager
	Jobs          []string
	NReduce       int
}

type JobManager struct {
	Job       *Job
	StartTime time.Time
}

type Job struct {
	Id       int
	FileName string
	IsMap    bool
	//Map任务为Nreduce，Reduce任务为Reduce任务号标识
	NReduce int
	Status  bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) DistributeJob(args *Request, reply *JobReply) error {
	mutex.Lock()
	fmt.Println("读锁DistributeJob")

	if c.IsMapStatus && len(c.MapChannel) != 0 {
		reply.Job = *<-c.MapChannel
		c.JobManager[reply.Job.Id].StartTime = time.Now()
	} else if !c.IsMapStatus && len(c.ReduceChannel) != 0 {
		reply.Job = *<-c.ReduceChannel
	}
	mutex.Unlock()
	fmt.Println("释放读锁DistributeJob")

	return nil
}

func (c *Coordinator) FinishJob(args *FinishRequest, reply *FinishResp) error {
	mutex.Lock()
	fmt.Println("锁finish")

	c.JobManager[args.JobId].Job.Status = true
	mutex.Unlock()
	fmt.Println("释放锁finish")
	reply.IsAccepted = true
	fmt.Println("co finish")
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
	//判断是否已执行完map任务
	finishMap := true
	fmt.Println(len(c.MapChannel))
	mutex.Lock()
	defer mutex.Unlock()
	fmt.Println("读锁Done")
	if c.IsMapStatus && len(c.MapChannel) == 0 {
		fmt.Println("releas读锁Done")
		for _, jobManager := range c.JobManager {
			if jobManager.Job.IsMap && !jobManager.Job.Status {
				finishMap = false
				break
			}
		}
		if finishMap {
			fmt.Println("map任务已完成")
			c.IsMapStatus = !finishMap
			fmt.Println("修改状态")
			// c.MakeReduceJob()
			fmt.Println("map任务已完成")
		}
	}
	// mutex.RUnlock()
	// fmt.Println("releas读锁Done")

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
	c.MapChannel = make(chan *Job, len(c.Jobs))
	c.ReduceChannel = make(chan *Job, nReduce)
	c.JobManager = make(map[int]*JobManager)
	c.IsMapStatus = true
	JobId = 0
	c.MakeMapJob()
	c.MakeReduceJob()

	c.server()
	return &c
}

func (c *Coordinator) MakeMapJob() {
	for _, v := range c.Jobs {
		job := &Job{
			Id:       JobId,
			FileName: v,
			IsMap:    true,
			Status:   false,
			NReduce:  c.NReduce,
		}
		c.MapChannel <- job

		c.JobManager[job.Id] = &JobManager{
			Job: job,
		}

		JobId += 1
	}
}

var ReduceKey int

func (c *Coordinator) MakeReduceJob() {
	ReduceKey = 0
	for ReduceKey < c.NReduce {
		fmt.Printf("%d\n", ReduceKey)
		fmt.Println(c.NReduce)
		job := &Job{
			Id:       JobId,
			FileName: "",
			IsMap:    false,
			Status:   false,
			NReduce:  ReduceKey,
		}
		c.ReduceChannel <- job

		c.JobManager[job.Id] = &JobManager{
			Job: job,
		}

		JobId += 1
		fmt.Printf("the %d loop", ReduceKey)
		ReduceKey += 1
	}
}
