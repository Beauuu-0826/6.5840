package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"log"
	"time"
)

const (
	Unlocked = "unlocked"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey    string       // lock key
	identifier string       // lock identifier
	ownLock    bool         // own lock
	version    rpc.Tversion // if ownLock=true, version records lock's version
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockKey = l
	lk.identifier = kvtest.RandValue(8)
	lk.ownLock = false
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	// block acquire, so there's an infinite loop
	for {
		val, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey || val == Unlocked {
			putErr := lk.ck.Put(lk.lockKey, lk.identifier, version)
			if putErr == rpc.OK {
				lk.ownLock = true
				lk.version = version + 1
				return
			}
			// in case the message dropped
			if putErr == rpc.ErrMaybe {
				tmpVal, tmpVersion, tmpErr := lk.ck.Get(lk.lockKey)
				if tmpErr == rpc.OK && tmpVal == lk.identifier {
					// this case the last rpc call's reply dropped, actually it acquire successfully
					lk.ownLock = true
					lk.version = tmpVersion
					return
				}
				// if tmpErr equals rpc.ErrNoKey then continue the for loop and retry to acquire the lock
			}
		}
		time.Sleep(time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	// short circuit when doesn't own lock
	if !lk.ownLock {
		return
	}
	err := lk.ck.Put(lk.lockKey, Unlocked, lk.version)
	if err == rpc.OK {
		lk.ownLock = false
		return
	}
	// in case the message dropped
	if err == rpc.ErrMaybe {
		// retry release if necessary
		val, version, getErr := lk.ck.Get(lk.lockKey)
		if getErr == rpc.ErrNoKey {
			log.Fatalf("Should never reach here, where lock key not exists")
		}
		if val == lk.identifier && version == lk.version {
			log.Printf("Last release request didn't send to server, retry it")
			lk.Release()
		} else {
			lk.ownLock = false
		}
		return
	}
	if err == rpc.ErrVersion || err == rpc.ErrNoKey {
		log.Fatalf("Should never reach here, where the version unmatch or lock key not exists!")
	}
	log.Fatalf("The err type is %v", err)
}
