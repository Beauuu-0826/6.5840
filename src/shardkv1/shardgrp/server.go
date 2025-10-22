package shardgrp

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
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
	gid  tester.Tgid

	// Your code here
	mu sync.Mutex
	// shardId -> kv map
	kvm               map[shardcfg.Tshid]map[string]Value
	responsibleShards map[shardcfg.Tshid]bool
	maxNum            shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		shardId := shardcfg.Key2Shard(args.Key)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if _, responsible := kv.responsibleShards[shardId]; !responsible {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		shard, ok := kv.kvm[shardId]
		var val Value
		if ok {
			val, ok = shard[args.Key]
		}
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
		shardId := shardcfg.Key2Shard(args.Key)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if _, responsible := kv.responsibleShards[shardId]; !responsible {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		shard, existsShard := kv.kvm[shardId]
		var val Value
		var ok = false
		if existsShard {
			val, ok = shard[args.Key]
		}
		if ok {
			if args.Version != val.Version {
				//log.Printf("Server[gid=%v, me=%v] process put req for key %v return err version, current version is %v and args version is %v, the args value is %v and current value is %v", kv.gid, kv.me, args.Key, val.Version, args.Version, args.Value, val.Val)
				reply.Err = rpc.ErrVersion
				return reply
			}
			kv.kvm[shardId][args.Key] = Value{args.Value, args.Version + 1}
			//log.Printf("Server[gid=%v, me=%v] process put req for key %v ok, current version is %v and current value is %v", kv.gid, kv.me, args.Key, args.Version+1, args.Value)
			reply.Err = rpc.OK
			return reply
		}
		// if key not exists and version != 0, then should return rpc.ErrNoKey
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		// else insert the new key with version=1
		if !existsShard {
			kv.kvm[shardId] = make(map[string]Value)
		}
		kv.kvm[shardId][args.Key] = Value{args.Value, 1}
		reply.Err = rpc.OK
		return reply
	case shardrpc.FreezeShardArgs:
		reply := shardrpc.FreezeShardReply{}
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if args.Num < kv.maxNum {
			reply.Err = rpc.ErrVersion
			reply.Num = kv.maxNum
			return reply
		}
		reply.Err = rpc.OK
		kv.maxNum = args.Num
		delete(kv.responsibleShards, args.Shard)
		if shard, exists := kv.kvm[args.Shard]; exists {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(shard)
			reply.State = w.Bytes()
		}
		return reply
	case shardrpc.InstallShardArgs:
		reply := shardrpc.InstallShardReply{}
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if args.Num < kv.maxNum {
			reply.Err = rpc.ErrVersion
			return reply
		}
		reply.Err = rpc.OK
		kv.maxNum = args.Num
		kv.responsibleShards[args.Shard] = true
		if len(args.State) != 0 {
			r := bytes.NewBuffer(args.State)
			d := labgob.NewDecoder(r)
			var shard map[string]Value
			d.Decode(&shard)
			kv.kvm[args.Shard] = shard
		}
		return reply
	case shardrpc.DeleteShardArgs:
		reply := shardrpc.DeleteShardReply{}
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if args.Num < kv.maxNum {
			reply.Err = rpc.ErrVersion
			return reply
		}
		reply.Err = rpc.OK
		kv.maxNum = args.Num
		delete(kv.kvm, args.Shard)
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
	e.Encode(kv.responsibleShards)
	e.Encode(kv.maxNum)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvm map[shardcfg.Tshid]map[string]Value
	d.Decode(&kvm)
	kv.kvm = kvm
	var responsibleShards map[shardcfg.Tshid]bool
	d.Decode(&responsibleShards)
	kv.responsibleShards = responsibleShards
	d.Decode(&kv.maxNum)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
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
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	//log.Printf("The put args is %v, return err is %v, rep is %v", *args, err, rep)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	castRep := rep.(rpc.PutReply)
	reply.Err = castRep.Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	castRep := rep.(shardrpc.FreezeShardReply)
	reply.Err = castRep.Err
	reply.State = castRep.State
	reply.Num = castRep.Num
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	castRep := rep.(shardrpc.InstallShardReply)
	reply.Err = castRep.Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	castRep := rep.(shardrpc.DeleteShardReply)
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me, kvm: make(map[shardcfg.Tshid]map[string]Value), responsibleShards: make(map[shardcfg.Tshid]bool)}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	kv.maxNum = -1
	// when initialized, the first shard group is responsible for all shards
	if gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.responsibleShards[shardcfg.Tshid(i)] = true
		}
	}

	return []tester.IService{kv, kv.rsm.Raft()}
}
