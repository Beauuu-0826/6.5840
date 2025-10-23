package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
	"math/rand/v2"
	"sync/atomic"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader          atomic.Int32 // cached leader, may lose its leadership
	totalRetryLimit int
	peerRetryLimit  int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leader.Store(rand.Int32N(int32(len(servers))))
	ck.peerRetryLimit = 3
	ck.totalRetryLimit = len(ck.servers) * 3
	return ck
}

type GenericRPCReply struct {
	value   string
	version rpc.Tversion
	err     rpc.Err
}

// TODO refactor using aop or wrapper
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := &rpc.GetArgs{Key: key}
	peerRetryCount := 0
	for i := 0; i < ck.totalRetryLimit; i++ {
		reply := &rpc.GetReply{}
		curLeader := ck.leader.Load()
		ok := ck.clnt.Call(ck.servers[curLeader], "KVServer.Get", args, reply)
		if !ok {
			peerRetryCount += 1
			if peerRetryCount == ck.peerRetryLimit {
				ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
				peerRetryCount = 0
			}
			continue
		}
		peerRetryCount = 0
		if reply.Err == rpc.ErrWrongLeader {
			ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
			continue
		}
		if reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}
		return reply.Value, reply.Version, reply.Err
	}
	return "", 0, rpc.ErrWrongGroup
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := &rpc.PutArgs{Key: key, Value: value, Version: version}
	firstTime := true
	peerRetryCount := 0
	for i := 0; i < ck.totalRetryLimit; i++ {
		reply := &rpc.PutReply{}
		curLeader := ck.leader.Load()
		ok := ck.clnt.Call(ck.servers[curLeader], "KVServer.Put", args, reply)
		if !ok {
			// if rpc call failed to get reply, retry call and set firstTime = false
			firstTime = false
			peerRetryCount += 1
			if peerRetryCount == ck.peerRetryLimit {
				ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
				peerRetryCount = 0
			}
			continue
		}
		peerRetryCount = 0
		if reply.Err == rpc.ErrWrongLeader {
			//ck.leader.Store(rand.Int32N(int32(len(ck.servers))))
			ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
			continue
		}
		if reply.Err == rpc.ErrVersion {
			if firstTime {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		}
		return reply.Err
	}
	return rpc.ErrWrongGroup
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := &shardrpc.FreezeShardArgs{Shard: s, Num: num}
	firstTime := true
	peerRetryCount := 0
	for {
		reply := &shardrpc.FreezeShardReply{}
		curLeader := ck.leader.Load()
		ok := ck.clnt.Call(ck.servers[curLeader], "KVServer.FreezeShard", args, reply)
		if !ok {
			// if rpc call failed to get reply, retry call and set firstTime = false
			firstTime = false
			peerRetryCount += 1
			if peerRetryCount == ck.peerRetryLimit {
				ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
				peerRetryCount = 0
			}
			continue
		}
		peerRetryCount = 0
		if reply.Err == rpc.ErrWrongLeader {
			//ck.leader.Store(rand.Int32N(int32(len(ck.servers))))
			ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
			continue
		}
		if reply.Err == rpc.ErrVersion {
			if firstTime {
				return nil, rpc.ErrVersion
			}
			return nil, rpc.ErrMaybe
		}
		return reply.State, reply.Err
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := &shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	firstTime := true
	peerRetryCount := 0
	for {
		reply := &shardrpc.InstallShardReply{}
		curLeader := ck.leader.Load()
		ok := ck.clnt.Call(ck.servers[curLeader], "KVServer.InstallShard", args, reply)
		if !ok {
			// if rpc call failed to get reply, retry call and set firstTime = false
			firstTime = false
			peerRetryCount += 1
			if peerRetryCount == ck.peerRetryLimit {
				ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
				peerRetryCount = 0
			}
			continue
		}
		peerRetryCount = 0
		if reply.Err == rpc.ErrWrongLeader {
			//ck.leader.Store(rand.Int32N(int32(len(ck.servers))))
			ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
			continue
		}
		if reply.Err == rpc.ErrVersion {
			if firstTime {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		}
		return reply.Err
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := &shardrpc.DeleteShardArgs{Shard: s, Num: num}
	firstTime := true
	peerRetryCount := 0
	for {
		reply := &shardrpc.DeleteShardReply{}
		curLeader := ck.leader.Load()
		ok := ck.clnt.Call(ck.servers[curLeader], "KVServer.DeleteShard", args, reply)
		if !ok {
			// if rpc call failed to get reply, retry call and set firstTime = false
			firstTime = false
			peerRetryCount += 1
			if peerRetryCount == ck.peerRetryLimit {
				ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
				peerRetryCount = 0
			}
			continue
		}
		peerRetryCount = 0
		if reply.Err == rpc.ErrWrongLeader {
			//ck.leader.Store(rand.Int32N(int32(len(ck.servers))))
			ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
			continue
		}
		if reply.Err == rpc.ErrVersion {
			if firstTime {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		}
		return reply.Err
	}
}
