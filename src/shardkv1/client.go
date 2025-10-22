package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
	"sync"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
	mu        sync.Mutex
	gid2Clerk map[tester.Tgid]*shardgrp.Clerk // cache to avoid frequent wrong leader err
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:      clnt,
		sck:       sck,
		gid2Clerk: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) decideShardGroup(key string) *shardgrp.Clerk {
	shardConfig := ck.sck.Query()
	groupId, servers, _ := shardConfig.GidServers(shardcfg.Key2Shard(key))
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if _, exists := ck.gid2Clerk[groupId]; !exists {
		ck.gid2Clerk[groupId] = shardgrp.MakeClerk(ck.clnt, servers)
	}
	return ck.gid2Clerk[groupId]
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	for {
		clerk := ck.decideShardGroup(key)
		value, version, err := clerk.Get(key)
		if err == rpc.ErrWrongGroup {
			continue
		}
		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	firstTime := true
	for {
		clerk := ck.decideShardGroup(key)
		err := clerk.Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			firstTime = false
			continue
		}
		if err == rpc.ErrVersion {
			if firstTime {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		}
		return err
	}
}
