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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

var wg sync.WaitGroup

type State int64

const (
	Follower State = iota
	Candidate
	Leader
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []EntryLog

	state         State
	lastHeartBeat time.Time

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	// Reinitialized after election
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = false
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		now := time.Now()
		diffTime := now.Sub(rf.lastHeartBeat).Milliseconds()
		if diffTime < 800 {
			isleader = true
		} else {
			rf.state = Follower
		}
	}
	rf.mu.Unlock()

	// if rf.state == Leader {
	// 	isleader = true
	// }
	return term, isleader
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidatedId int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	Debug(dVote, "ID: %d - request term: %d - ID: %d - current term : %d", args.CandidatedId, args.Term, rf.me, rf.currentTerm)
	if rf.currentTerm > args.Term || rf.commitIndex > args.LastLogIndex {
		// fmt.Println(rf.commitIndex, args.LastLogIndex)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidatedId) && rf.currentTerm <= args.Term {
		rf.votedFor = args.CandidatedId
		if rf.state == Leader {
			fmt.Println("Convert to follower>>>>>>>>>>>>>>>>1")
		}
		rf.state = Follower
		rf.lastHeartBeat = time.Now()
		reply.VoteGranted = true
	} else if rf.currentTerm < args.Term {
		rf.votedFor = args.CandidatedId
		if rf.state == Leader {
			fmt.Println("Convert to follower>>>>>>>>>>>>>>>>2")
		}
		rf.state = Follower
		rf.lastHeartBeat = time.Now()
		reply.VoteGranted = true
	}
	rf.currentTerm = args.Term
	rf.mu.Unlock()
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
	term, isLeader = rf.GetState()
	if !isLeader {
		return -1, -1, false
	}
	rf.mu.Lock()
	fmt.Println("Start", command)
	index = rf.nextIndex[rf.me]
	rf.log = append(rf.log, EntryLog{Command: command, Term: rf.currentTerm})
	rf.nextIndex[rf.me]++
	nPeers := len(rf.peers)
	me := rf.me
	rf.mu.Unlock()
	for i := 0; i < nPeers; i++ {
		if i != me {
			go rf.AppendLogToFollower(command, i, index)
		}
	}
	return index, term, isLeader
}

func (rf *Raft) AppendLogToFollower(command interface{}, i, index int) {
	prelogTerm := 1
	rf.mu.Lock()
	if len(rf.log) != 0 {
		prelogTerm = rf.log[len(rf.log)-1].Term
	}
	args := AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		PrevLogIndex:      rf.nextIndex[i],
		PrevLogTerm:       prelogTerm,
		Entries:           []EntryLog{{command, rf.currentTerm}},
		LeaderCommitIndex: rf.commitIndex,
	}
	reply := AppendEntriesReply{Success: false}
	rf.mu.Unlock()
	if rf.nextIndex[i] == index {
		rf.sendAppendEntries(i, &args, &reply)
	} else {
		return
	}
	if reply.Success {
		rf.mu.Lock()
		rf.nextIndex[i]++
		rf.matchIndex[i]++
		if rf.CheckMajorityOnIndex(args.PrevLogIndex) {
			commitIdx := int(math.Max(float64(rf.commitIndex), float64(rf.nextIndex[i])-1))
			if commitIdx > rf.commitIndex {
				rf.commitIndex = int(math.Max(float64(rf.commitIndex), float64(rf.nextIndex[i])-1))
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[index-1].Command, CommandIndex: index}
			}
		}
		rf.mu.Unlock()
	} else {
		fmt.Println("OnReplyAppendEntriesFail")
		rf.OnReplyAppendEntriesFail(i)
	}
}

func (rf *Raft) OnReplyAppendEntriesFail(i int) {
	prelogTerm := 1
	rf.mu.Lock()
	if len(rf.log) != 0 {
		prelogTerm = rf.log[len(rf.log)-1].Term
	}

	args := AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		PrevLogIndex:      rf.nextIndex[i],
		PrevLogTerm:       prelogTerm,
		Entries:           []EntryLog{rf.log[rf.nextIndex[i]-2]},
		LeaderCommitIndex: rf.commitIndex,
	}
	reply := AppendEntriesReply{Success: false}
	rf.mu.Unlock()
	for !reply.Success && rf.nextIndex[i] != rf.nextIndex[rf.me] {
		isSent := rf.sendAppendEntries(i, &args, &reply)
		rf.mu.Lock()
		if isSent {
			if reply.Success {
				rf.nextIndex[i]++
				if rf.nextIndex[i] >= rf.nextIndex[rf.me] {
					rf.mu.Unlock()
					fmt.Println("Break")
					break
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.mu.Unlock()
					fmt.Println("Break")
					break
				}
				rf.nextIndex[i]--
				// fmt.Println("NEXT INDEX ", rf.nextIndex[i])
				time.Sleep(100 * time.Millisecond)
			}
			args = AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderID:          rf.me,
				PrevLogIndex:      rf.nextIndex[i],
				PrevLogTerm:       prelogTerm,
				Entries:           []EntryLog{rf.log[rf.nextIndex[i]-1]},
				LeaderCommitIndex: rf.commitIndex,
			}
			reply = AppendEntriesReply{Success: false}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) CheckMajorityOnIndex(index int) bool {
	count := 0
	for i := 0; i < len(rf.peers); i++ {
		// fmt.Println(rf.nextIndex, index)
		if rf.nextIndex[i]-1 >= index {
			count++
		}
	}
	return count >= len(rf.peers)/2+1
}

func (rf *Raft) LeaderInit() {
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
	}
	fmt.Println(rf.nextIndex)
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
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		count := 1
		now := time.Now()
		rf.mu.Lock()
		diffTime := now.Sub(rf.lastHeartBeat).Milliseconds()
		if diffTime > ms+650 && rf.state != Leader {
			Debug(dElection, "ID: %d Start election", rf.me)
			rf.state = Candidate
			rf.currentTerm++
			fmt.Println("ID:", rf.me, " Start election", rf.currentTerm)
			rf.votedFor = rf.me
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					rf.mu.Lock()
					go func(i, me, currentTerm, peerLen int, state State) {
						lastLogIndex := len(rf.log)
						lastLogTerm := 0
						if len(rf.log) > 1 {
							lastLogTerm = rf.log[len(rf.log)-1].Term
						}
						args := RequestVoteArgs{
							Term:         currentTerm,
							CandidatedId: me,
							LastLogIndex: lastLogIndex,
							LastLogTerm:  lastLogTerm,
						}
						reply := RequestVoteReply{}
						if ok := rf.sendRequestVote(i, &args, &reply); !ok {
							return
						}
						if reply.Term > currentTerm {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.state = Follower
							rf.mu.Unlock()
						}
						if reply.VoteGranted && reply.Term <= currentTerm && state == Candidate {
							rf.mu.Lock()
							count++
							if count >= peerLen/2+1 {
								Debug(dElection, "ID: %d Become Leader in term %d", rf.me, rf.currentTerm)
								fmt.Println("ID: ", rf.me, " Become Leader in term ", rf.currentTerm)
								rf.lastHeartBeat = time.Now()
								rf.state = Leader
								rf.LeaderInit()
								rf.mu.Unlock()
								go rf.PeriodHeartBeat()
								return
							}
							rf.mu.Unlock()
						}
					}(i, rf.me, rf.currentTerm, len(rf.peers), rf.state)
					rf.mu.Unlock()
				}
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.lastHeartBeat = time.Now()
	reply.Term = rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	Debug(dInfo, "ID: %d term %d - receive heartbeat from - ID: %d term %d", rf.me, rf.currentTerm, args.LeaderID, args.Term)

	// 2
	if len(rf.log)+1 < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	// 3

	// 4
	if len(args.Entries) > 0 {
		// fmt.Println(rf.log, args.PrevLogIndex)
		if len(rf.log) < args.PrevLogIndex {
			rf.log = append(rf.log, args.Entries...)
			fmt.Println("ID ", rf.me, rf.log)
		}
	}

	// 5
	if args.LeaderCommitIndex > rf.commitIndex {
		resCommitIndex := int(math.Min(float64(args.LeaderCommitIndex), float64(len(rf.log))))
		for i := rf.commitIndex + 1; i <= resCommitIndex; i++ {
			// fmt.Println("COMMITCOMMAND", i, rf.me, "CHECK", i, rf.log[i-1].Command)
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i-1].Command, CommandIndex: i}
			// fmt.Println(rf.log[i-1].Command, i)
		}
		rf.commitIndex = resCommitIndex
		// fmt.Println("COMMIT INDEX", rf.commitIndex)
	}
	reply.Success = true
	rf.mu.Unlock()
}

func (rf *Raft) PeriodHeartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.lastHeartBeat = time.Now()
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(i int) {
						rf.mu.Lock()
						prevLogIndex := rf.nextIndex[rf.me] - 1
						prevLogTerm := -1
						if len(rf.log) > 0 {
							prevLogTerm = rf.log[len(rf.log)-1].Term
						}
						args := AppendEntriesArgs{
							Term:              rf.currentTerm,
							LeaderID:          rf.me,
							PrevLogIndex:      prevLogIndex,
							PrevLogTerm:       prevLogTerm,
							Entries:           []EntryLog{},
							LeaderCommitIndex: rf.commitIndex,
						}
						// fmt.Println(rf.commitIndex)
						reply := AppendEntriesReply{}
						rf.mu.Unlock()
						rf.sendAppendEntries(i, &args, &reply)
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							if rf.state == Leader {
								fmt.Println("Convert to follower>>>>>>>>>>>>>>>>5")
							}
							rf.state = Follower
						}
						rf.mu.Unlock()
					}(i)
				}
			}
		} else {
			rf.mu.Unlock()
			break
		}
		ms := 100
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.lastHeartBeat = time.Now()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.commitIndex = 0

	len := len(rf.peers)
	rf.nextIndex = make([]int, len)
	for i := 0; i < len; i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len)
	rf.log = make([]EntryLog, 0)

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

type EntryLog struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []EntryLog
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}
