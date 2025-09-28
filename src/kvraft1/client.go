package kvraft

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"math/rand/v2"
	"sync/atomic"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leader atomic.Int32 // cached leader, may lose its leadership
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leader.Store(rand.Int32N(int32(len(servers))))
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	args := &rpc.GetArgs{Key: key}
	retryCount := 0
	for {
		reply := &rpc.GetReply{}
		curLeader := ck.leader.Load()
		ok := ck.clnt.Call(ck.servers[curLeader], "KVServer.Get", args, reply)
		if !ok {
			retryCount += 1
			if retryCount == 3 {
				ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
				retryCount = 0
			}
			continue
		}
		retryCount = 0
		if reply.Err == rpc.ErrWrongLeader {
			ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
			continue
		}
		if reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}
		return reply.Value, reply.Version, rpc.OK
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := &rpc.PutArgs{Key: key, Value: value, Version: version}
	firstTime := true
	retryCount := 0
	for {
		reply := &rpc.PutReply{}
		curLeader := ck.leader.Load()
		ok := ck.clnt.Call(ck.servers[curLeader], "KVServer.Put", args, reply)
		if !ok {
			// if rpc call failed to get reply, retry call and set firstTime = false
			firstTime = false
			retryCount += 1
			if retryCount == 3 {
				ck.leader.Store((curLeader + 1) % int32(len(ck.servers)))
				retryCount = 0
			}
			continue
		}
		retryCount = 0
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
