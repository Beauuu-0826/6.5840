package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

const ConfigKey = "CONFIG_KEY"
const NextConfigKey = "NEXT_CONFIG_KEY"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	nextCfgStr, _, err := sck.IKVClerk.Get(NextConfigKey)
	if err == rpc.ErrNoKey {
		return
	}
	next := shardcfg.FromString(nextCfgStr)
	current, currentV := sck.QueryWithVersion()
	if current.Num+1 != next.Num {
		return
	}
	// recovery
	gid2ChangeSet := make(map[tester.Tgid]*ChangeSet)
	for shardId, groupId := range current.Shards {
		if newGroupId := next.Shards[shardId]; newGroupId != groupId {
			if _, exists := gid2ChangeSet[groupId]; !exists {
				gid2ChangeSet[groupId] = &ChangeSet{}
			}
			if _, exists := gid2ChangeSet[newGroupId]; !exists {
				gid2ChangeSet[newGroupId] = &ChangeSet{}
			}
			gid2ChangeSet[groupId].removeShards = append(gid2ChangeSet[groupId].removeShards, shardcfg.Tshid(shardId))
			gid2ChangeSet[newGroupId].appendShards = append(gid2ChangeSet[newGroupId].appendShards, shardcfg.Tshid(shardId))
		}
	}
	shid2State := make(map[shardcfg.Tshid][]byte)
	gid2RemoveClerk := make(map[tester.Tgid]*shardgrp.Clerk)
	// Step1: Freeze shards
	for groupId, changeSet := range gid2ChangeSet {
		if len(changeSet.removeShards) != 0 {
			if _, exists := gid2RemoveClerk[groupId]; !exists {
				gid2RemoveClerk[groupId] = shardgrp.MakeClerk(sck.clnt, current.Groups[groupId])
			}
			clerk := gid2RemoveClerk[groupId]
			for _, removeShardId := range changeSet.removeShards {
				// assume now it will not fail
				state, _ := clerk.FreezeShard(removeShardId, next.Num)
				shid2State[removeShardId] = state
			}
		}
	}
	// Step2: Install shards
	gid2AppendClerk := make(map[tester.Tgid]*shardgrp.Clerk)
	for groupId, changeSet := range gid2ChangeSet {
		if len(changeSet.appendShards) != 0 {
			if _, exists := gid2AppendClerk[groupId]; !exists {
				gid2AppendClerk[groupId] = shardgrp.MakeClerk(sck.clnt, next.Groups[groupId])
			}
			clerk := gid2AppendClerk[groupId]
			for _, appendShardId := range changeSet.appendShards {
				clerk.InstallShard(appendShardId, shid2State[appendShardId], next.Num)
			}
		}
	}
	// Step3: Delete shards
	for groupId, changeSet := range gid2ChangeSet {
		if len(changeSet.removeShards) != 0 {
			clerk := gid2RemoveClerk[groupId]
			for _, removeShardId := range changeSet.removeShards {
				clerk.DeleteShard(removeShardId, next.Num)
			}
		}
	}
	// Update config
	sck.IKVClerk.Put(ConfigKey, next.String(), currentV)
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	sck.IKVClerk.Put(ConfigKey, cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	/**
	one we should notice is that once the ChangeConfigTo was called, it should not fail.
	And we should avoid the access for those keys that was moved would not direct to the old shard group
	to maintain linearization
	*/
	// Your code here.
	current, currentV := sck.QueryWithVersion()
	if !sck.StoreNextConfig(new, currentV) {
		// short circuit
		return
	}
	gid2ChangeSet := make(map[tester.Tgid]*ChangeSet)
	for shardId, groupId := range current.Shards {
		if newGroupId := new.Shards[shardId]; newGroupId != groupId {
			if _, exists := gid2ChangeSet[groupId]; !exists {
				gid2ChangeSet[groupId] = &ChangeSet{}
			}
			if _, exists := gid2ChangeSet[newGroupId]; !exists {
				gid2ChangeSet[newGroupId] = &ChangeSet{}
			}
			gid2ChangeSet[groupId].removeShards = append(gid2ChangeSet[groupId].removeShards, shardcfg.Tshid(shardId))
			gid2ChangeSet[newGroupId].appendShards = append(gid2ChangeSet[newGroupId].appendShards, shardcfg.Tshid(shardId))
		}
	}
	shid2State := make(map[shardcfg.Tshid][]byte)
	gid2RemoveClerk := make(map[tester.Tgid]*shardgrp.Clerk)
	// Step1: Freeze shards
	for groupId, changeSet := range gid2ChangeSet {
		if len(changeSet.removeShards) != 0 {
			if _, exists := gid2RemoveClerk[groupId]; !exists {
				gid2RemoveClerk[groupId] = shardgrp.MakeClerk(sck.clnt, current.Groups[groupId])
			}
			clerk := gid2RemoveClerk[groupId]
			for _, removeShardId := range changeSet.removeShards {
				// assume now it will not fail
				state, _ := clerk.FreezeShard(removeShardId, new.Num)
				shid2State[removeShardId] = state
			}
		}
	}
	// Step2: Install shards
	gid2AppendClerk := make(map[tester.Tgid]*shardgrp.Clerk)
	for groupId, changeSet := range gid2ChangeSet {
		if len(changeSet.appendShards) != 0 {
			if _, exists := gid2AppendClerk[groupId]; !exists {
				gid2AppendClerk[groupId] = shardgrp.MakeClerk(sck.clnt, new.Groups[groupId])
			}
			clerk := gid2AppendClerk[groupId]
			for _, appendShardId := range changeSet.appendShards {
				clerk.InstallShard(appendShardId, shid2State[appendShardId], new.Num)
			}
		}
	}
	// Step3: Delete shards
	for groupId, changeSet := range gid2ChangeSet {
		if len(changeSet.removeShards) != 0 {
			clerk := gid2RemoveClerk[groupId]
			for _, removeShardId := range changeSet.removeShards {
				clerk.DeleteShard(removeShardId, new.Num)
			}
		}
	}
	// Update config
	sck.IKVClerk.Put(ConfigKey, new.String(), currentV)
}

type ChangeSet struct {
	removeShards []shardcfg.Tshid
	appendShards []shardcfg.Tshid
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	cfgStr, _, err := sck.IKVClerk.Get(ConfigKey)
	if err == rpc.ErrNoKey {
		panic("No config available now")
	}
	return shardcfg.FromString(cfgStr)
}

func (sck *ShardCtrler) QueryWithVersion() (*shardcfg.ShardConfig, rpc.Tversion) {
	cfgStr, version, err := sck.IKVClerk.Get(ConfigKey)
	if err == rpc.ErrNoKey {
		panic("No config available now")
	}
	return shardcfg.FromString(cfgStr), version
}

func (sck *ShardCtrler) StoreNextConfig(nextConfig *shardcfg.ShardConfig, ver rpc.Tversion) bool {
	nextConfigStr := nextConfig.String()
	for {
		str, version, err := sck.IKVClerk.Get(NextConfigKey)
		if err == rpc.ErrNoKey {
			version = 0
		}
		// if last rpc returns ErrMaybe, we should check for the content and the version
		// if version and content equals what we expected, that means the last put request
		// executes successfully, so return true
		if version == ver && str == nextConfigStr {
			return true
		}
		if version+1 != ver {
			return false
		}
		putErr := sck.IKVClerk.Put(NextConfigKey, nextConfigStr, version)
		if putErr == rpc.OK {
			return true
		}
		if putErr == rpc.ErrVersion {
			return false
		}
		continue
	}
}
