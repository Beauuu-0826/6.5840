package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	// used to account the map tasks
	files              []string
	mapTasks           map[int]int
	intermediateResult map[int][]string
	mapMutex           sync.Mutex

	// used to account the reduce tasks
	reduceTasks             map[int]int
	finalResult             map[int]string
	mapTasksUnfinishedCount int
	reduceMutex             sync.Mutex
	reduceCond              *sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RPCHandler(args *MRArgs, reply *MRReply) error {
	c.processRPCInput(args)

	if done := c.Done(); done {
		reply.Done = true
		log.Printf("All tasks has finished, exit cn now\n")
		return nil
	}
	reply.Done = false
	c.mapMutex.Lock()
	for idx, status := range c.mapTasks {
		if status == 0 {
			reply.HasTask = true
			reply.IsMapTask = true
			reply.MapIndex = idx
			reply.Filename = c.files[idx]
			reply.ReduceCount = len(c.reduceTasks)
			c.mapTasks[idx] = 1
			time.AfterFunc(10*time.Second, func() {
				c.mapMutex.Lock()
				defer c.mapMutex.Unlock()
				if c.mapTasks[idx] == 1 {
					c.mapTasks[idx] = 0
					log.Printf("The %dth map task has crashed, reset its status.", idx)
				}
			})
			c.mapMutex.Unlock()
			return nil
		}
	}
	c.mapMutex.Unlock()

	c.reduceMutex.Lock()
	defer c.reduceMutex.Unlock()
	if c.mapTasksUnfinishedCount != 0 {
		// still got unfinished map tasks running, can't start reduce task now
		reply.HasTask = false
		return nil
	}
	for idx, status := range c.reduceTasks {
		if status == 0 {
			reply.HasTask = true
			reply.IsMapTask = false
			reply.ReduceIndex = idx
			reply.ReduceMergeFiles = c.intermediateResult[idx]
			c.reduceTasks[idx] = 1
			time.AfterFunc(10*time.Second, func() {
				c.reduceMutex.Lock()
				defer c.reduceMutex.Unlock()
				if c.reduceTasks[idx] == 1 {
					c.reduceTasks[idx] = 0
					log.Printf("The %dth reduce task has crashed, reset its status.", idx)
				}
			})
			return nil
		}
	}
	reply.HasTask = false
	return nil
}

func (c *Coordinator) processRPCInput(args *MRArgs) {
	if args.FirstTime {
		return
	}
	if args.IsMapTask {
		c.mapMutex.Lock()
		defer c.mapMutex.Unlock()
		if c.mapTasks[args.MapIndex] == 2 {
			return
		}
		c.mapTasks[args.MapIndex] = 2
		for bucket, filename := range args.IntermediateResult {
			c.intermediateResult[bucket] = append(c.intermediateResult[bucket], filename)
		}
		c.reduceMutex.Lock()
		defer c.reduceMutex.Unlock()
		c.mapTasksUnfinishedCount -= 1
		log.Printf("The %dth map task has finished\n", args.MapIndex)
	} else {
		c.reduceMutex.Lock()
		defer c.reduceMutex.Unlock()
		if c.reduceTasks[args.ReduceIndex] == 2 {
			return
		}
		c.reduceTasks[args.ReduceIndex] = 2
		c.finalResult[args.ReduceIndex] = args.FinalResult
		log.Printf("The %dth reduce task has finished\n", args.ReduceIndex)
	}
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
	// Your code here.
	c.reduceMutex.Lock()
	defer c.reduceMutex.Unlock()
	if c.mapTasksUnfinishedCount != 0 {
		return false
	}
	for _, status := range c.reduceTasks {
		if status != 2 {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.mapTasks = make(map[int]int)
	c.mapTasksUnfinishedCount = len(files)
	for idx := range files {
		c.mapTasks[idx] = 0
	}
	c.reduceTasks = make(map[int]int)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = 0
	}
	c.intermediateResult = make(map[int][]string)
	c.finalResult = make(map[int]string)
	c.reduceCond = sync.NewCond(&c.reduceMutex)

	c.server()
	return &c
}
