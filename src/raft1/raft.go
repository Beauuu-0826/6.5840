package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type State string

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
)

// A Go object to store log entry information
type Log struct {
	Index   int
	Term    int64
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// apply channel related
	applyCh  chan raftapi.ApplyMsg
	chMu     sync.Mutex
	chClosed atomic.Bool

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm atomic.Int64 // using atomic.Int64 to avoid race detection fail, but actually no need
	votedFor    int          // -1 represents null
	log         []Log
	snapshot    []byte

	// volatile state
	state               atomic.Value // using atomic.Value to avoid race detection fail, but actually no need
	receiveHeartBeat    bool
	receiveCandidateReq bool
	commitIndex         atomic.Int64
	lastApplied         atomic.Int64
	leaderId            int

	// volatile leader state
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm.Load()), rf.state.Load() == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm.Load())
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int64
	d.Decode(&currentTerm)
	rf.currentTerm.Store(currentTerm)
	var votedFor int
	d.Decode(&votedFor)
	rf.votedFor = votedFor
	var log []Log
	d.Decode(&log)
	rf.log = log
	DPrintf("[Peer %v, Term %v] Recovered from crashed", rf.me, rf.currentTerm.Load())
}

func (rf *Raft) restoreSnapshot(snapshot []byte) {
	rf.snapshot = snapshot
	rf.commitIndex.Store(int64(rf.log[1].Index))
	rf.lastApplied.Store(int64(rf.log[1].Index))
	go rf.applyToStateMachine(raftapi.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotIndex: rf.log[1].Index,
		SnapshotTerm:  int(rf.log[1].Term),
	})
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshot = snapshot
	rf.log = append(rf.log[:1], rf.log[rf.relativeIndex(index):]...)
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64
	VoteGranted bool
	Voter       int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// if current voter has become a leader in current term, return false
	// if candidate's term is less than voter's current term, return false
	currentTerm := rf.currentTerm.Load()
	if args.Term == currentTerm && rf.state.Load() == LEADER || currentTerm > args.Term {
		reply.Term = currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > currentTerm {
		rf.discoverNewTerm(args.Term)
	}
	// if votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	logLength := len(rf.log)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(rf.log[logLength-1].Term < args.LastLogTerm ||
			(rf.log[logLength-1].Term == args.LastLogTerm && rf.log[logLength-1].Index <= args.LastLogIndex)) {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		rf.receiveCandidateReq = true
		reply.VoteGranted = true
		reply.Voter = rf.me
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm.Load()
	}
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []Log
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
	Peer    int
	// used for quick back up
	XTerm  int64
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm.Load()
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
		reply.Peer = rf.me
		return
	}
	// periodic heartbeats
	rf.receiveHeartBeat = true
	rf.leaderId = args.LeaderId
	if currentTerm < args.Term || rf.state.Load() == CANDIDATE {
		rf.discoverNewTerm(args.Term)
	}
	// check log matches or not
	lastLogIndex := rf.log[len(rf.log)-1].Index
	relativeIndex := rf.relativeIndex(args.PrevLogIndex)
	if lastLogIndex < args.PrevLogIndex || rf.log[relativeIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm.Load()
		reply.Peer = rf.me
		reply.XLen = lastLogIndex + 1
		if lastLogIndex >= args.PrevLogIndex {
			reply.XTerm = rf.log[relativeIndex].Term
			for i := relativeIndex; i >= 0; i-- {
				if rf.log[i].Term != reply.XTerm {
					reply.XIndex = rf.log[i+1].Index
					break
				}
			}
		} else {
			reply.XTerm = -1
			reply.XIndex = -1
		}
		return
	}
	reply.Success = true
	if len(args.Entries) != 0 && lastLogIndex < args.Entries[len(args.Entries)-1].Index {
		rf.log = append(rf.log[:relativeIndex+1], args.Entries...)
		rf.persist()
	} else {
		for i := 0; i < len(args.Entries); i++ {
			logIndex := relativeIndex + 1 + i
			if rf.log[logIndex].Term != args.Entries[i].Term || rf.log[logIndex].Index != args.Entries[i].Index {
				// conflict at logIndex
				rf.log = append(rf.log[:logIndex], args.Entries[i:]...)
				rf.persist()
				break
			}
		}
	}
	newCommitIndex := min(args.LeaderCommit, int64(args.PrevLogIndex+len(args.Entries)))
	if newCommitIndex > rf.commitIndex.Load() {
		rf.commitIndex.Store(newCommitIndex)
	}
}

type InstallSnapshotArgs struct {
	Term            int64
	LeaderId        int
	LastIncludedLog Log
	Snapshot        []byte
}

type InstallSnapshotReply struct {
	Term int64
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm.Load()
	if args.Term < currentTerm {
		reply.Term = currentTerm
		return
	}
	rf.receiveHeartBeat = true
	rf.leaderId = args.LeaderId
	if currentTerm < args.Term || rf.state.Load() == CANDIDATE {
		rf.discoverNewTerm(args.Term)
	}
	reply.Term = rf.currentTerm.Load()
	relativeIndex := rf.relativeIndex(args.LastIncludedLog.Index)
	// check if peer contains the last included log in snapshot
	if rf.existsLog(args.LastIncludedLog.Index) && rf.log[relativeIndex].Term == args.LastIncludedLog.Term {
		rf.log = append(rf.log[:1], rf.log[relativeIndex:]...)
		rf.snapshot = args.Snapshot
		rf.persist()
		return
	}
	rf.log = append(rf.log[:1], args.LastIncludedLog)
	rf.snapshot = args.Snapshot
	rf.persist()
	rf.lastApplied.Store(int64(args.LastIncludedLog.Index))
	rf.commitIndex.Store(int64(args.LastIncludedLog.Index))
	go rf.applyToStateMachine(raftapi.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotIndex: args.LastIncludedLog.Index,
		SnapshotTerm:  int(args.LastIncludedLog.Term),
	})
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in  rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() || rf.state.Load() != LEADER {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm.Load()
	logIndex := rf.log[len(rf.log)-1].Index + 1
	rf.log = append(rf.log, Log{logIndex, currentTerm, command})
	rf.persist()
	return logIndex, int(currentTerm), true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.chMu.Lock()
	defer rf.chMu.Unlock()
	rf.chClosed.Store(true)
	close(rf.applyCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// TODO leverage channel to close the go routine and restart when receive heartbeat or candidate req or candidate start election. And maybe when candidate learns that its log is not updated, it can prolong the election timeout
func (rf *Raft) ticker() {
	ms := 100 + (rand.Int63() % 150)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		stop := make(chan struct{}, 1)
		rf.mu.Lock()
		if rf.state.Load() == CANDIDATE || (rf.state.Load() == FOLLOWER && !rf.receiveCandidateReq && !rf.receiveHeartBeat) {
			DPrintf("[Peer %v, Term %v] Transform state into CANDIDATE, start election", rf.me, rf.currentTerm.Load())
			rf.state.Store(CANDIDATE)
			rf.currentTerm.Add(1)
			rf.votedFor = rf.me
			rf.persist()
			go rf.election(stop)
		} else {
			// reset heartbeat flag
			rf.receiveHeartBeat = false
			rf.receiveCandidateReq = false
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 200 and 400
		// milliseconds.
		ms := 200 + (rand.Int63() % 400)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		close(stop)
	}
}

func (rf *Raft) election(stop <-chan struct{}) {
	electionStop := make(chan struct{}, 1)
	replyChannel := make(chan RequestVoteReply, len(rf.peers))
	rf.mu.Lock()
	logLength := len(rf.log)
	args := &RequestVoteArgs{rf.currentTerm.Load(),
		rf.me, rf.log[logLength-1].Index, rf.log[logLength-1].Term}
	rf.mu.Unlock()
	// go send RequestVote RPC to all other servers
	for server := range rf.peers {
		if rf.me == server {
			continue
		}
		go func(replyChannel chan<- RequestVoteReply, stop <-chan struct{}, electionStop <-chan struct{}) {
			for {
				select {
				case <-stop:
					return
				case <-electionStop:
					return
				default:
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(server, args, reply)
					if ok {
						replyChannel <- *reply
						return
					}
				}
			}
		}(replyChannel, stop, electionStop)
	}
	receiveVotes := make([]int, 0)
	for rf.state.Load() == CANDIDATE && rf.killed() == false {
		select {
		case <-stop:
			DPrintf("[Peer %v, Term %v] Reach election timeout, stop current election go routine", rf.me, args.Term)
			close(electionStop)
			return
		case reply := <-replyChannel:
			if reply.VoteGranted == false && reply.Term > rf.currentTerm.Load() {
				rf.mu.Lock()
				// double check
				if reply.Term > rf.currentTerm.Load() {
					rf.discoverNewTerm(reply.Term)
				}
				rf.mu.Unlock()
				close(electionStop)
				return
			}
			if reply.VoteGranted {
				receiveVotes = append(receiveVotes, reply.Voter)
			}
			// receive majority vote, become leader in current term
			if len(receiveVotes)+1 > len(rf.peers)/2 {
				rf.mu.Lock()
				DPrintf("[Peer %v, Term %v] Receive marjority votes from %v, transform state into LEADER, start its term", rf.me, rf.currentTerm.Load(), receiveVotes)
				rf.state.Store(LEADER)
				rf.noop()
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
					rf.matchIndex[i] = 0
				}
				go rf.heartbeat()
				go rf.replicateLog()
				go rf.checkNewAgreement()
				rf.mu.Unlock()
				close(electionStop)
				return
			}
		default:
		}
	}
}

// no-op used to process the situation like when leader receive a command and then crashed or be partitioned,
// the new leader take office. And in the following time, no req arrived.
// When the old leader recovers, it will become leader in case that it owns the latest log,
// but due to figure 8's restriction, the system will not apply this last long entry because
// the entry's term is less than current term. Only if the system receives new req will the
// log entry be applied

// And no-op can handle this case when a leader take office, it will append a no-op log entry
// to force commitment of previous logs
func (rf *Raft) noop() {
	logIndex := rf.log[len(rf.log)-1].Index + 1
	rf.log = append(rf.log, Log{logIndex, rf.currentTerm.Load(), nil})
	rf.persist()
}

func (rf *Raft) checkNewAgreement() {
	for rf.killed() == false && rf.state.Load() == LEADER {
		for i := len(rf.log) - 1; rf.log[i].Index > int(rf.commitIndex.Load()) && rf.log[i].Term == rf.currentTerm.Load(); i-- {
			agreeCount := 0
			for _, matched := range rf.matchIndex {
				if matched >= rf.log[i].Index {
					agreeCount++
				}
			}
			if agreeCount+1 > len(rf.peers)/2 {
				rf.commitIndex.Store(int64(rf.log[i].Index))
				DPrintf("[Peer %v, Term %v] Agree at [%v, %v]", rf.me, rf.currentTerm.Load(), rf.log[i].Term, rf.log[i].Command)
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) replicateLog() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func() {
			for rf.killed() == false && rf.state.Load() == LEADER {
				rf.mu.Lock()
				lastLogIndex := rf.log[len(rf.log)-1].Index
				nextIndex := rf.nextIndex[server]
				if nextIndex <= lastLogIndex && (!rf.existsLog(nextIndex-1) || !rf.existsLog(nextIndex)) {
					args := &InstallSnapshotArgs{
						rf.currentTerm.Load(),
						rf.me,
						rf.log[1],
						rf.snapshot,
					}
					rf.mu.Unlock()
					reply := &InstallSnapshotReply{}
					if ok := rf.sendInstallSnapshot(server, args, reply); !ok {
						continue
					}
					if reply.Term > rf.currentTerm.Load() {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm.Load() {
							rf.discoverNewTerm(reply.Term)
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						rf.updateNextIndex(server, args.LastIncludedLog.Index+1)
						rf.updateMatchIndex(server, args.LastIncludedLog.Index)
						rf.mu.Unlock()
					}
				} else if nextIndex <= lastLogIndex {
					relativeIndex := rf.relativeIndex(nextIndex)
					// send append entries rpc
					args := &AppendEntriesArgs{
						rf.currentTerm.Load(),
						rf.me,
						rf.log[relativeIndex-1].Index,
						rf.log[relativeIndex-1].Term,
						rf.log[relativeIndex:],
						rf.commitIndex.Load()}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					if ok := rf.sendAppendEntries(server, args, reply); !ok {
						continue
					}
					if reply.Success {
						rf.mu.Lock()
						rf.updateNextIndex(server, lastLogIndex+1)
						rf.updateMatchIndex(server, lastLogIndex)
						rf.mu.Unlock()
					} else if reply.Term > rf.currentTerm.Load() {
						rf.mu.Lock()
						// double check
						if reply.Term > rf.currentTerm.Load() {
							rf.discoverNewTerm(reply.Term)
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						rf.backup(reply)
						rf.mu.Unlock()
					}
				} else {
					rf.mu.Unlock()
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
}

func (rf *Raft) heartbeat() {
	stopHeartbeat := int32(0)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func() {
			for rf.killed() == false && rf.state.Load() == LEADER && atomic.LoadInt32(&stopHeartbeat) == 0 {
				rf.mu.Lock()
				nextIndex := rf.nextIndex[server]
				if !rf.existsLog(nextIndex - 1) {
					args := &InstallSnapshotArgs{
						rf.currentTerm.Load(),
						rf.me,
						rf.log[1],
						rf.snapshot,
					}
					rf.mu.Unlock()
					reply := &InstallSnapshotReply{}
					if ok := rf.sendInstallSnapshot(server, args, reply); !ok {
						continue
					}
					if reply.Term > rf.currentTerm.Load() {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm.Load() {
							rf.discoverNewTerm(reply.Term)
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						rf.updateNextIndex(server, args.LastIncludedLog.Index+1)
						rf.updateMatchIndex(server, args.LastIncludedLog.Index)
						rf.mu.Unlock()
					}
				} else {
					relativeIndex := rf.relativeIndex(nextIndex)
					args := &AppendEntriesArgs{Term: rf.currentTerm.Load(),
						LeaderId:     rf.me,
						LeaderCommit: rf.commitIndex.Load(),
						PrevLogTerm:  rf.log[relativeIndex-1].Term,
						PrevLogIndex: nextIndex - 1}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, args, reply)
					if ok {
						if reply.Success {
						} else if reply.Term > rf.currentTerm.Load() {
							rf.mu.Lock()
							// double check
							if reply.Term > rf.currentTerm.Load() {
								rf.discoverNewTerm(reply.Term)
								atomic.StoreInt32(&stopHeartbeat, 1)
								rf.mu.Unlock()
								return
							}
							rf.mu.Unlock()
						} else {
							rf.mu.Lock()
							rf.backup(reply)
							rf.mu.Unlock()
						}
						time.Sleep(80 * time.Millisecond)
						continue
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
}

func (rf *Raft) applyLog() {
	for rf.killed() == false {
		if rf.commitIndex.Load() > rf.lastApplied.Load() {
			var applyMsgs []raftapi.ApplyMsg
			rf.mu.Lock()
			for rf.commitIndex.Load() > rf.lastApplied.Load() {
				appliedIndex := rf.lastApplied.Add(1)
				applyMsgs = append(applyMsgs, raftapi.ApplyMsg{CommandValid: true,
					Command:      rf.log[rf.relativeIndex(int(appliedIndex))].Command,
					CommandIndex: int(appliedIndex)})
			}
			rf.mu.Unlock()
			go rf.applyToStateMachine(applyMsgs...)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) discoverNewTerm(newTerm int64) {
	rf.state.Store(FOLLOWER)
	rf.currentTerm.Store(newTerm)
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) relativeIndex(logIndex int) int {
	logLength := len(rf.log)
	return logLength - 1 - (rf.log[logLength-1].Index - logIndex)
}

func (rf *Raft) backup(reply *AppendEntriesReply) {
	if reply.XTerm == -1 {
		rf.nextIndex[reply.Peer] = reply.XLen
		return
	}
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == reply.XTerm {
			rf.nextIndex[reply.Peer] = rf.log[i].Index + 1
			return
		}
	}
	rf.nextIndex[reply.Peer] = reply.XIndex
}

func (rf *Raft) applyToStateMachine(applyMsgs ...raftapi.ApplyMsg) {
	rf.chMu.Lock()
	defer rf.chMu.Unlock()
	if rf.chClosed.Load() {
		return
	}
	for _, msg := range applyMsgs {
		rf.applyCh <- msg
	}
}

func (rf *Raft) updateNextIndex(server int, nextIndex int) {
	if rf.nextIndex[server] < nextIndex {
		rf.nextIndex[server] = nextIndex
	}
}

func (rf *Raft) updateMatchIndex(server int, matchIndex int) {
	if rf.matchIndex[server] < matchIndex {
		rf.matchIndex[server] = matchIndex
	}
}

func (rf *Raft) existsLog(logIndex int) bool {
	if logIndex == 0 {
		return true
	}
	logLength := len(rf.log)
	return logIndex <= rf.log[logLength-1].Index && rf.relativeIndex(logIndex) >= 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.receiveHeartBeat = false
	rf.receiveCandidateReq = false
	rf.state.Store(FOLLOWER)
	rf.currentTerm.Store(0)
	rf.votedFor = -1
	rf.leaderId = -1
	rf.commitIndex.Store(0)
	rf.lastApplied.Store(0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{0, 0, 0})

	// initialize from state persisted before a crash
	if persister.RaftStateSize() != 0 {
		rf.readPersist(persister.ReadRaftState())
	}
	if persister.SnapshotSize() != 0 {
		rf.restoreSnapshot(persister.ReadSnapshot())
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	// start a go routine to apply log
	go rf.applyLog()

	return rf
}
