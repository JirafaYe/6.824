package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := JobReply{}
	getJob(&reply)

	if reply.Job.IsMap && reply.Job.FileName != "" {
		fmt.Println("执行map")

		file, err := os.Open(reply.Job.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Job.FileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Job.FileName)
		}
		file.Close()

		v := []KeyValue{}

		kva := mapf(reply.Job.FileName, string(content))
		v = append(v, kva...)

		OutputTmpFile(v, &reply)

		finishReply := &FinishResp{}
		FinishJob(reply.Job.Id, finishReply)
	} else if !reply.Job.IsMap && reply.Job.FileName != "" {

	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func OutputTmpFile(v []KeyValue, reply *JobReply) {
	tmp := make(map[int][]KeyValue, reply.Job.NReduce)

	for _, v := range v {
		tmpKey := ihash(v.Key) % reply.Job.NReduce
		kv := tmp[tmpKey]
		tmp[tmpKey] = append(kv, v)
	}

	filename := "./mr-tmp/mr-tmp-" + strconv.Itoa(reply.Job.Id)

	for k, v := range tmp {
		tmpFile := filename + "-" + strconv.Itoa(k) + ".txt"
		ofile, _ := os.Create(tmpFile)

		//写出中间文件
		b, _ := json.Marshal(v)

		ofile.Write(b)
		ofile.Close()
	}
}
func FinishJob(jobId int, reply *FinishResp) {
	args := FinishRequest{
		JobId:  jobId,
		Status: true,
	}

	// fill in the argument(s).

	// declare a reply structure.

	// send the RPC request, wait for the reply.
	call("Coordinator.FinishJob", &args, &reply)

	// reply.Y should be 100.
	fmt.Println("finishJob")
}

func getJob(reply *JobReply) {
	args := Request{}

	// fill in the argument(s).

	// declare a reply structure.

	// send the RPC request, wait for the reply.
	call("Coordinator.DistributeJob", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("%v\n", reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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
