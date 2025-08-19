package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MRArgs struct {
	// true if it's first time call cn
	FirstTime bool

	// true if this is a map task's arguments for last call's reply
	IsMapTask bool

	// used for map task
	MapIndex           int
	IntermediateResult map[int]string

	// used for reduce task
	ReduceIndex int
	FinalResult string
}

type MRReply struct {
	// true if all tasks has finished, and the worker would exit
	Done bool

	HasTask bool

	// true if this is a map task's reply from cn
	IsMapTask bool

	// used for map task
	MapIndex    int
	Filename    string
	ReduceCount int

	// used for reduce task
	ReduceIndex      int
	ReduceMergeFiles []string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
