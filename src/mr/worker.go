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

	for {
		reply := JobReply{}
		getJob(&reply)
		// fmt.Println("rply", reply)
		if reply.IsDone {
			return
		}
		if reply.Job.IsMap && reply.Job.FileName != "" {
			MapJobWork(&reply, mapf)
			// break
		} else if !reply.Job.IsMap && reply.Job.Id != 0 {
			ReduceJobWork(&reply, reducef)
			// break
		} else {
			// fmt.Println("loop=====", i)
			fmt.Println(reply.Msg)
		}
		time.Sleep(1 * time.Second)
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func ReduceJobWork(reply *JobReply, reducef func(string, []string) string) {
	// fmt.Println("Reduce任务")
	kv := OpenIntermediaFiles(reply.Job.ReduceKey, reply.Job.FilesNum)
	less := func(i, j int) bool {
		return kv[i].Key > kv[j].Key
	}

	sort.Slice(kv, less)
	i := 0
	filename := "/root/workspace/6.824/src/main/mr-tmp/mr-out-" + strconv.Itoa(reply.Job.ReduceKey)
	ofile, _ := os.Create(filename)
	// fmt.Println(len(kv))
	for i < len(kv) {
		j := i + 1
		for j < len(kv) && kv[j].Key == kv[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kv[k].Value)
		}
		output := reducef(kv[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kv[i].Key, output)

		i = j
	}
	finishReply := &FinishResp{}
	FinishJob(reply.Job.Id, finishReply)
	// fmt.Println("完成reduce")
}

func MapJobWork(reply *JobReply, mapf func(string, string) []KeyValue) {
	// fmt.Println("执行map")

	file, err := os.Open(reply.Job.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", file.Name())
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Job.FileName)
	}
	file.Close()

	v := []KeyValue{}

	kva := mapf(reply.Job.FileName, string(content))
	v = append(v, kva...)

	OutputTmpFile(v, reply)

	finishReply := &FinishResp{}
	FinishJob(reply.Job.Id, finishReply)
}

func OpenIntermediaFiles(reduceKey int, nReduce int) []KeyValue {
	filename := "/root/workspace/6.824/src/main/mr-tmp/mr-tmp-"
	res := []KeyValue{}
	reduce := strconv.Itoa(reduceKey)
	for i := 0; i < nReduce; i++ {
		tmp := []KeyValue{}
		tmpName := filename + strconv.Itoa(i) + "-" + reduce + ".txt"
		// fmt.Println("name" + tmpName)
		file, err := os.Open(tmpName)
		if err != nil {
			log.Println("cannot open %v", tmpName)
			continue
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Println("cannot read %v", tmpName)
		}
		file.Close()
		json.Unmarshal(content, &tmp)
		// fmt.Println("byte[]:", +len(content))
		res = append(res, tmp...)
	}
	return res
}

func OutputTmpFile(kvs []KeyValue, reply *JobReply) {
	tmp := make(map[int][]KeyValue, reply.Job.NReduce)

	for _, v := range kvs {
		tmpKey := ihash(v.Key) % reply.Job.NReduce
		// kv := tmp[tmpKey]
		tmp[tmpKey] = append(tmp[tmpKey], v)
	}

	filename := "/root/workspace/6.824/src/main/mr-tmp/mr-tmp-" + strconv.Itoa(reply.Job.Id)

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
	// fmt.Println("finishJob")
}

func getJob(reply *JobReply) {
	args := Request{}

	// fill in the argument(s).

	// declare a reply structure.

	// send the RPC request, wait for the reply.
	call("Coordinator.DistributeJob", &args, &reply)

	// reply.Y should be 100.
	// fmt.Printf("%v\n", reply)
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
