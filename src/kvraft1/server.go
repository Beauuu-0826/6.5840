package kvraft

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
	"bytes"
	"sync"
	"sync/atomic"
)

type Value struct {
	Val     string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu  sync.Mutex
	kvm map[string]Value
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		kv.mu.Lock()
		val, ok := kv.kvm[args.Key]
		kv.mu.Unlock()
		if !ok {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		reply.Err = rpc.OK
		reply.Value = val.Val
		reply.Version = val.Version
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}
		kv.mu.Lock()
		val, ok := kv.kvm[args.Key]
		if ok {
			if args.Version != val.Version {
				kv.mu.Unlock()
				reply.Err = rpc.ErrVersion
				return reply
			}
			kv.kvm[args.Key] = Value{args.Value, args.Version + 1}
			kv.mu.Unlock()
			reply.Err = rpc.OK
			return reply
		}
		// if key not exists and version != 0, then should return rpc.ErrNoKey
		if args.Version != 0 {
			kv.mu.Unlock()
			reply.Err = rpc.ErrNoKey
			return reply
		}
		// else insert the new key with version=1
		kv.kvm[args.Key] = Value{args.Value, 1}
		kv.mu.Unlock()
		reply.Err = rpc.OK
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvm)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvm map[string]Value
	d.Decode(&kvm)
	kv.kvm = kvm
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	castRep := rep.(rpc.GetReply)
	reply.Err = castRep.Err
	reply.Version = castRep.Version
	reply.Value = castRep.Value
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	castRep := rep.(rpc.PutReply)
	reply.Err = castRep.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, kvm: make(map[string]Value)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
