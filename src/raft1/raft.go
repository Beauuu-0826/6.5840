package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm atomic.Int64 // using atomic.Int64 to avoid race detection fail, but actually no need
	votedFor    int          // -1 represents null
	log         []Log

	// volatile state
	state               atomic.Value // using atomic.Value to avoid race detection fail, but actually no need
	receiveHeartBeat    bool
	receiveCandidateReq bool
	commitIndex         int
	lastApplied         int
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	// Your code here (3D).

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
	rf.receiveCandidateReq = true
	if args.Term > currentTerm {
		rf.discoverNewTerm(args.Term)
	}
	// if votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	logLength := len(rf.log)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.log[logLength-1].Term <= args.LastLogTerm && logLength-1 <= args.LastLogIndex {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Voter = rf.me
	} else {
		reply.VoteGranted = false
		reply.Term = currentTerm
	}
}

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int64
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm.Load()
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
		return
	}
	rf.leaderId = args.LeaderId
	if currentTerm < args.Term || rf.state.Load() == CANDIDATE {
		rf.discoverNewTerm(args.Term)
	}
	// periodic heartbeats
	rf.receiveHeartBeat = true
	if len(args.Entries) == 0 {
		reply.Success = true
		DPrintf("[Peer %v, Term %v] Receive heartbeat from leader[%v]", rf.me, rf.currentTerm.Load(), args.LeaderId)
		return
	}
	// TODO implement in lab3b log replication
	if len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = currentTerm
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
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
			go rf.election(stop)
		} else {
			// reset heartbeat flag
			rf.receiveHeartBeat = false
			rf.receiveCandidateReq = false
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 200 and 400
		// milliseconds.
		ms := 200 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		close(stop)
	}
}

func (rf *Raft) election(stop <-chan struct{}) {
	electionStop := make(chan struct{}, 1)
	replyChannel := make(chan RequestVoteReply, len(rf.peers))
	rf.mu.Lock()
	args := &RequestVoteArgs{rf.currentTerm.Load(),
		rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
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
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				go rf.heartbeat()
				rf.mu.Unlock()
				close(electionStop)
				return
			}
		default:
		}
	}
}

func (rf *Raft) heartbeat() {
	replyChannel := make(chan AppendEntriesReply, 2*len(rf.peers))
	stopHeartbeat := make(chan struct{}, 1)
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(replyChannel chan<- AppendEntriesReply, stopHeartbeat <-chan struct{}) {
			for {
				select {
				case <-stopHeartbeat:
					return
				default:
					args := &AppendEntriesArgs{Term: rf.currentTerm.Load(), LeaderId: rf.me}
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, args, reply)
					if ok {
						select {
						case replyChannel <- *reply:
						default:
						}
					}
					// sleep random time to deliver next heartbeat
					ms := 50 + (rand.Int63() % 100)
					time.Sleep(time.Duration(ms) * time.Millisecond)
				}
			}
		}(replyChannel, stopHeartbeat)
	}
	for rf.killed() == false && rf.state.Load() == LEADER {
		select {
		case reply := <-replyChannel:
			if !reply.Success && reply.Term > rf.currentTerm.Load() {
				rf.mu.Lock()
				// double check
				if reply.Term > rf.currentTerm.Load() {
					rf.discoverNewTerm(reply.Term)
					close(stopHeartbeat)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		default:
		}
	}
	close(stopHeartbeat)
}

func (rf *Raft) discoverNewTerm(newTerm int64) {
	rf.state.Store(FOLLOWER)
	rf.currentTerm.Store(newTerm)
	rf.votedFor = -1
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

	// Your initialization code here (3A, 3B, 3C).
	rf.receiveHeartBeat = false
	rf.receiveCandidateReq = false
	rf.state.Store(FOLLOWER)
	rf.currentTerm.Store(0)
	rf.votedFor = -1
	rf.leaderId = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{0, 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
