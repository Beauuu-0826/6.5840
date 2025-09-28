package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Req any
	Id  int64
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	opId            atomic.Int64
	pendingOps      map[int]Op
	pendingChannels map[int]chan any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:              me,
		maxraftstate:    maxraftstate,
		applyCh:         make(chan raftapi.ApplyMsg),
		sm:              sm,
		pendingOps:      make(map[int]Op, 0),
		pendingChannels: make(map[int]chan any, 0),
	}
	rsm.opId.Store(0)
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// reader func
func (rsm *RSM) reader() {
	for {
		select {
		case applyMsg, ok := <-rsm.applyCh:
			if !ok {
				for _, ch := range rsm.pendingChannels {
					close(ch)
				}
				return
			}
			if applyMsg.CommandValid {
				op, isOp := applyMsg.Command.(Op)
				returned := rsm.sm.DoOp(op.Req)
				rsm.mu.Lock()
				if pendingOp, exists := rsm.pendingOps[applyMsg.CommandIndex]; exists {
					// isOp = false means no-op
					if !isOp || pendingOp.Me != op.Me && pendingOp.Id != op.Id {
						for _, ch := range rsm.pendingChannels {
							close(ch)
						}
						rsm.pendingChannels = make(map[int]chan any)
						rsm.pendingOps = make(map[int]Op)
					} else {
						rsm.pendingChannels[applyMsg.CommandIndex] <- returned
						delete(rsm.pendingOps, applyMsg.CommandIndex)
						delete(rsm.pendingChannels, applyMsg.CommandIndex)
					}
				}
				rsm.mu.Unlock()
			}
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	opId := rsm.opId.Add(1)
	op := Op{
		Me:  rsm.me,
		Req: req,
		Id:  opId,
	}
	ch := make(chan any)
	rsm.mu.Lock()
	index, _, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.pendingOps[index] = op
	rsm.pendingChannels[index] = ch
	rsm.mu.Unlock()

	received, ok := <-ch
	// currently, only if shutdown and leadership change will close all those pending channels
	if !ok {
		return rpc.ErrWrongLeader, nil
	}
	return rpc.OK, received
}
