package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	// "github.com/meetalodariya/raft-consensus-mit/labgob"
	"github.com/meetalodariya/raft-consensus-mit/labrpc"
)

const (
	Follower  = "Follower"
	Leader    = "Leader"
	Candidate = "Candidate"

	// HeartbeatTimeout defines the timeout duration for heartbeats sent from leader to followers
	// in milliseconds.
	HeartbeatTimeout = 150

	// Election timeouts
	ElectionTimeoutMin = 250
	ElectionTimeoutMax = 450
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int32               // this peer's index into peers[]
	dead      int32               // set by Kill()
	tckr      *time.Ticker        // Ticker that ticks after election timeout

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentRole   string // Current role of the raft: Follower Leader Candidate
	currentTerm   int    // Current term in the cluster
	currentLeader int32  // index in peers[] of the current leader
	votedFor      int32  // index in peers[] of the peer who the current raft has voted for
	// commitIndex   int64
	// lastApplied   int64
	// log           []*ApplyMsg

	// for leader role
	nextIndex  map[int]int64
	matchIndex map[int]int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.currentRole == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).

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
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// field names must start with capital letters!
type RequestVoteArgs struct {
	CurrentTerm int
	CandidateID int32
}

// field names must start with capital letters!
type RequestVoteReply struct {
	CurrentTerm int
	PeerID      int32
	Granted     bool
}

type AppendEntriesArgs struct {
	CurrentTerm int
	LeaderID    int32
}

type AppendEntriesReply struct {
	CurrentTerm int
	PeerID      int32
	Success     bool
}

func (rf *Raft) ResetElectionTimer() {
	rf.tckr.Reset(getElectionTimeoutDuration())
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// There's a new term. step down to become a follower.
	if args.CurrentTerm > rf.currentTerm {
		rf.currentTerm = args.CurrentTerm
		rf.currentRole = Follower
		rf.votedFor = -1
	}

	// Check if the log is in order or not....

	// send the response back to caller
	if rf.currentTerm == args.CurrentTerm && rf.votedFor == -1 {
		rf.votedFor = args.CandidateID
		rf.ResetElectionTimer()

		reply.Granted = true
	} else {
		reply.Granted = false
	}

	fmt.Println(rf.me, ": voted ", reply.Granted, "to: ", args.CandidateID)

	reply.CurrentTerm = rf.currentTerm
	reply.PeerID = rf.me
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("%v: Heartbeat received from: %v at term: %v\n", rf.me, args.LeaderID, rf.currentTerm)

	if args.CurrentTerm > rf.currentTerm {
		rf.currentTerm = args.CurrentTerm
		rf.votedFor = -1
	}

	if rf.currentTerm == args.CurrentTerm {
		rf.currentRole = Follower
		rf.currentLeader = args.LeaderID
	}

	// TODO: check if the logs are in order or not

	if rf.currentTerm == args.CurrentTerm {
		// TODO: append the log....
		rf.ResetElectionTimer()

		reply.Success = true
	} else {
		reply.Success = false
	}

	reply.PeerID = rf.me
	reply.CurrentTerm = rf.currentTerm
}

func (rf *Raft) CallAppendEntries(server int, me int32, term int) (AppendEntriesReply, error) {
	args := AppendEntriesArgs{
		CurrentTerm: term,
		LeaderID:    me,
	}

	var rep AppendEntriesReply

	ok := rf.sendAppendEntries(server, &args, &rep)

	if ok {
		return rep, nil
	} else {
		return AppendEntriesReply{}, fmt.Errorf("%v: didn't get CallAppendEntries response from peer: %v", me, server)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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

func (rf *Raft) CallRequestVote(server int, me int32, term int) (RequestVoteReply, error) {
	args := RequestVoteArgs{
		CurrentTerm: term,
		CandidateID: me,
	}

	var rep RequestVoteReply

	ok := rf.sendRequestVote(server, &args, &rep)

	if ok {
		return rep, nil
	} else {
		return RequestVoteReply{}, fmt.Errorf("%v: didn't get CallRequestVote response from peer: %v", me, server)
	}
}

func (rf *Raft) AttemptElection() {
	rf.mu.Lock()
	rf.currentRole = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	me := rf.me
	term := rf.currentTerm
	rf.mu.Unlock()

	// counting the candidate itself
	votesReceived := 1
	responses := 1

	// check if there's a new term
	newTerm := int64(-1)

	cond := sync.NewCond(&rf.mu)

	fmt.Println(me, ": initiating the election at term: ", term)

	for index := range rf.peers {
		if index == int(me) {
			continue
		} else {
			go func(server int) {
				fmt.Printf("%v: Vote request sent to: %v at term: %v\n", me, server, term)

				// not need to call append entries as there is a higher term running in the cluster.
				if rf.GetAtomicInt64(&newTerm) > int64(-1) {
					return
				}

				reply, err := rf.CallRequestVote(server, me, term)
				if err != nil {
					fmt.Println(err.Error())
				}

				cond.L.Lock()
				defer cond.L.Unlock()

				if reply.Granted {
					votesReceived++
				} else if reply.CurrentTerm > term {
					rf.StoreAtomicInt64(&newTerm, int64(reply.CurrentTerm))
				}

				responses++

				cond.Broadcast()
			}(index)
		}
	}

	cond.L.Lock()

	quorum := math.Ceil((float64(len(rf.peers)+1) / 2.0))

	for float64(votesReceived) < quorum && responses != len(rf.peers) || rf.GetAtomicInt64(&newTerm) > int64(-1) {
		cond.Wait()
	}

	fmt.Println(me, ": votes received: ", votesReceived)
	fmt.Println(me, ": responses: ", responses)

	nt := rf.GetAtomicInt64(&newTerm)

	// Step down since there's a new term
	if nt > -1 {
		rf.currentTerm = int(nt)
		rf.currentRole = Follower
		rf.votedFor = -1
		rf.ResetElectionTimer()
		cond.L.Unlock()
	} else if float64(votesReceived) >= quorum && rf.currentRole == Candidate {
		fmt.Println(me, ": elected as leader")
		rf.currentRole = Leader
		rf.currentLeader = me
		rf.ResetElectionTimer()
		cond.L.Unlock()

		// Start sending the logs (or heartbeats) periodically to all the followers (blocking).
		rf.ReplicateLogs()
	} else {
		cond.L.Unlock()
	}
}

func (rf *Raft) ReplicateLogs() {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		me := rf.me
		term := rf.currentTerm

		if rf.currentRole != Leader {
			fmt.Println(me, ": stepping down")
			rf.mu.Unlock()

			return
		}
		rf.mu.Unlock()

		// check if there's a new term
		newTerm := int64(-1)

		cond := sync.NewCond(&rf.mu)

		for index := range rf.peers {
			if index == int(me) {
				continue
			} else {
				go func(server int) {
					fmt.Printf("%v: Heartbeat sent to: %v at term: %v\n", me, server, term)

					// not need to call append entries as there is a higher term running in the cluster.
					if rf.GetAtomicInt64(&newTerm) > int64(-1) {
						return
					}

					reply, err := rf.CallAppendEntries(server, me, term)
					if err != nil {
						fmt.Println(err.Error())
					}

					cond.L.Lock()
					defer cond.L.Unlock()

					if rf.currentTerm == reply.CurrentTerm && rf.currentRole == Leader {
						fmt.Printf("%v: Heartbeat acknowledged by: %v at term: %v\n", me, reply.PeerID, rf.currentTerm)
					} else if reply.CurrentTerm > rf.currentTerm {
						rf.StoreAtomicInt64(&newTerm, int64(reply.CurrentTerm))
					}

					cond.Broadcast()
				}(index)
			}
		}

		cond.L.Lock()

		if rf.GetAtomicInt64(&newTerm) == -1 {
			cond.Wait()
		}

		nt := rf.GetAtomicInt64(&newTerm)

		// Step down since there's a new term
		if nt > -1 {
			rf.currentRole = Follower
			rf.currentTerm = int(nt)
			rf.votedFor = -1
			cond.L.Unlock()
			rf.ResetElectionTimer()

			return
		}

		cond.L.Unlock()

		// heartbeats timeout
		time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
	}
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

	// Your code here (2B).

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
	rf.mu.RLock()
	fmt.Println(rf.me, ": terminating")
	rf.mu.RUnlock()

	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// func (rf *Raft) ticker() {
// 	for !rf.killed() {
// 		// Your code here (2A)
// 		// Check if a leader election should be started.
// 		if rf.currentRole != Leader || rf.currentLeader < 0 {
// 			rf.AttemptElection()
// 		} else {
// 			continue
// 		}

// 		// pause for a random amount of time between 50 and 350
// 		// milliseconds.
// 		ms := 50 + (rand.Int63() % 300)
// 		time.Sleep(time.Duration(ms) * time.Millisecond)
// 	}
// }

// Custom implementation of ticker using *time.Ticker.
func (rf *Raft) ticker() {
	for range rf.tckr.C {
		if rf.killed() {
			break
		}

		rf.AttemptElection()

		rf.ResetElectionTimer()
	}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)

	rf.currentRole = Follower
	rf.nextIndex = make(map[int]int64)
	rf.matchIndex = make(map[int]int64)
	rf.currentLeader = -1
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.tckr = time.NewTicker(getElectionTimeoutDuration())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
