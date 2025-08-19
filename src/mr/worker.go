package mr

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// KeyValue Iterator
type KeyValueIterator struct {
	currentKv KeyValue
	file      *os.File
	dec       *json.Decoder
}

// To acquire next KeyValue from decoder
func (iterator *KeyValueIterator) advance() bool {
	if err := iterator.dec.Decode(&iterator.currentKv); err != nil {
		return false
	}
	return true
}

func (iterator *KeyValueIterator) close() {
	iterator.file.Close()
}

// PriorityQueue Custom PriorityQueue implement
type PriorityQueue []*KeyValueIterator

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].currentKv.Key < pq[j].currentKv.Key
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*KeyValueIterator)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Enqueue(item *KeyValueIterator) {
	heap.Push(pq, item)
}

func (pq *PriorityQueue) Dequeue() *KeyValueIterator {
	item := heap.Pop(pq).(*KeyValueIterator)
	return item
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

	// Your worker implementation here.

	firstTime := true
	previousMapTask := true
	var mapIndex int
	var intermediateResult map[int]string
	var reduceIndex int
	var finalResult string

	// infinite loop to acquire task from cn
	for {
		// declare an argument structure.
		// argument here used to pass the previous task's result
		var args MRArgs
		if firstTime {
			args = MRArgs{FirstTime: true}
		} else {
			if previousMapTask {
				args = MRArgs{FirstTime: false, IsMapTask: true, MapIndex: mapIndex, IntermediateResult: intermediateResult}
			} else {
				args = MRArgs{FirstTime: false, IsMapTask: false, ReduceIndex: reduceIndex, FinalResult: finalResult}
			}
		}

		// declare a reply structure.
		reply := MRReply{}

		// send the RPC request, wait for the reply.
		// the "Coordinator.Example" tells the
		// receiving server that we'd like to call
		// the Example() method of struct Coordinator.
		log.Printf("Calling cn to acquire work, corresponding argument is %v\n", args)
		ok := call("Coordinator.RPCHandler", &args, &reply)
		log.Printf("Receive reply %v from cn\n", reply)
		if !ok {
			fmt.Printf("call failed!, exit current worker now\n")
			break
		}
		if reply.Done {
			fmt.Println("All tasks has finished, exit current worker now")
			break
		}
		// if currently all tasks are still running, just sleep
		if !reply.HasTask {
			time.Sleep(time.Second)
			continue
		}
		if reply.IsMapTask {
			intermediateResult = callMapFunc(mapf, reply.MapIndex, reply.Filename, reply.ReduceCount)
			mapIndex = reply.MapIndex
			previousMapTask = true
		} else {
			finalResult = callReduceFunc(reducef, reply.ReduceIndex, reply.ReduceMergeFiles)
			reduceIndex = reply.ReduceIndex
			previousMapTask = false
		}
		firstTime = false
	}
}

func callMapFunc(mapf func(string, string) []KeyValue, mapIndex int, filename string, reduceCount int) map[int]string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	// hash and write into n reduce files
	buckets := make(map[int][]KeyValue)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % reduceCount
		buckets[bucket] = append(buckets[bucket], kv)
	}
	result := make(map[int]string)
	for bucket, kva := range buckets {
		tempFile, err := os.CreateTemp(".", "intermediate-*")
		if err != nil {
			log.Fatalf("cannot create temp file in current working directory")
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("failed to encode %v to file", kv)
			}
		}
		tempFilename := tempFile.Name()
		tempFile.Close()
		filename := fmt.Sprintf("mr-%d-%d", mapIndex, bucket)
		os.Rename(tempFilename, filename)
		result[bucket] = filename
	}
	return result
}

func callReduceFunc(reducef func(string, []string) string, reduceIndex int, reduceMergeFiles []string) string {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	for _, filename := range reduceMergeFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		var currentKv KeyValue
		if err := dec.Decode(&currentKv); err == nil {
			pq.Enqueue(&KeyValueIterator{currentKv, file, dec})
		} else {
			file.Close()
		}
	}

	var currentKey string
	var values []string
	file, err := os.CreateTemp(".", "final-*")
	if err != nil {
		log.Fatalf("cannot create temp file in current working directory")
	}
	for {
		if pq.Len() == 0 {
			break
		}

		peek := pq.Dequeue()
		if len(values) == 0 {
			currentKey = peek.currentKv.Key
		} else if currentKey != peek.currentKv.Key {
			reduceValue := reducef(currentKey, values)
			fmt.Fprintf(file, "%v %v\n", currentKey, reduceValue)
			currentKey = peek.currentKv.Key
			values = values[:0]
		}
		values = append(values, peek.currentKv.Value)
		if peek.advance() {
			pq.Enqueue(peek)
		} else {
			peek.close()
		}
	}
	if len(values) != 0 {
		reduceValue := reducef(currentKey, values)
		fmt.Fprintf(file, "%v %v\n", currentKey, reduceValue)
	}
	tempFilename := file.Name()
	file.Close()
	filename := fmt.Sprintf("mr-out-%d", reduceIndex)
	os.Rename(tempFilename, filename)
	return filename
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
