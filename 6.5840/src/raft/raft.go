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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

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

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry
	applyChan   chan ApplyMsg

	state         State
	lastHeartBeat time.Time
	nextTimer     int

	// Volatile state on all servers
	commitIndex       int
	lastApplied       int
	LastIncludedIndex int
	LastIncludedTerm  int

	// Volatile state on leaders
	// Reinitialized after election
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

type AppendEntriesArgs struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// for optimization
	ConflictIndex int
	ConflicTerm   int
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
		// fmt.Println(now, rf.me, diffTime)
		if diffTime < 800 {
			isleader = true
		} else {
			// rf.state = Follower
			// rf.persist()
		}
	}
	rf.mu.Unlock()

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

	// // rf.mu.Lock()
	// // defer rf.mu.Unlock()
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.log)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	// // fmt.Println(raftstate)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.LastIncludedIndex) != nil ||
		e.Encode(rf.LastIncludedTerm) != nil {
	} else {
		data := w.Bytes()
		rf.persister.Save(data, nil)
	}
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

	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// d.Decode(&rf.currentTerm)
	// d.Decode(&rf.votedFor)
	// d.Decode(&rf.log)

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
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
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	Debug(dVote, "S%d rcv req vote from S%d", rf.me, args.CandidatedId)
	if rf.currentTerm > args.Term {
		return
	}

	if rf.currentTerm < args.Term {
		rf.stepDown(args.Term)
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidatedId {
		selfLogMoreUpdate := rf.isSelfMoreUpToDate(args.LastLogIndex, args.LastLogTerm)
		if !selfLogMoreUpdate {
			rf.votedFor = args.CandidatedId
			rf.state = Follower
			reply.VoteGranted = true
			rf.resetTimer()
			rf.persist()
			Debug(dInfo, "S%d grant vote %d %d", rf.me, rf.getLastLogIndex(), rf.getLastLogTerm())
		} else {
			Debug(dInfo, "S%d reject vote %d %d", rf.me, rf.getLastLogIndex(), rf.getLastLogTerm())
		}
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
	if isLeader {
		rf.mu.Lock()
		// index = len(rf.log)
		index = rf.nextIndex[rf.me]
		rf.log = append(rf.log, LogEntry{Term: term, Command: command, Index: index})
		rf.nextIndex[rf.me]++
		rf.persist()
		Debug(dInfo, "S%d append index %d", rf.me, index)
		rf.mu.Unlock()
	}
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
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		time.Sleep(100 * time.Millisecond)

		now := time.Now()
		rf.mu.Lock()
		diffTime := now.Sub(rf.lastHeartBeat).Milliseconds()
		if diffTime > int64(rf.nextTimer) && rf.state != Leader {
			rf.state = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			Debug(dELEC, "S%d Start election term %d - %d  %d", rf.me, rf.currentTerm, len(rf.log), rf.getLastLogTerm())
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.resetTimer()
	rf.mu.Unlock()
	count := 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		go func(i, me, currentTerm, peerLen int, state State) {
			rf.mu.Lock()
			lastLogIndex, lastTerm := rf.getLastLogIndexAndTerm()
			Debug(dELEC, "S%d send rq vote to S%d", rf.me, i)
			rf.mu.Unlock()
			args := RequestVoteArgs{Term: currentTerm, CandidatedId: me, LastLogIndex: lastLogIndex, LastLogTerm: lastTerm}
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &args, &reply); !ok {
				Debug(dELEC, "S%d fail to send rq vote to S%d", rf.me, i)
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.stepDown(args.Term)
			}
			if reply.VoteGranted && reply.Term <= rf.currentTerm && rf.state == Candidate {
				count++
				//win election
				Debug(dVote, "S%d rcv vote from S%d", me, i)
				if count >= peerLen/2+1 {
					if rf.state != Leader {
						Debug(dELEC, "S%d Become Leader in term %d", me, rf.currentTerm)
						rf.leaderInit()
						go rf.PeriodHeartBeat()
					}
					rf.mu.Unlock()
					return
				}
			} else {
				Debug(dVote, "S%d rcv reject from S%d", me, i)
			}
			rf.mu.Unlock()
		}(i, rf.me, rf.currentTerm, len(rf.peers), rf.state)
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = true

	persist := false

	switch {
	case rf.currentTerm < args.Term:
		rf.stepDown(args.Term)
		rf.resetTimer()
	case rf.currentTerm == args.Term:
		switch rf.state {
		case Follower:
			rf.resetTimer()
		case Candidate:
			rf.stepDown(args.Term)
			rf.resetTimer()
		case Leader:
			// fmt.Println("a leader receive a append entries request with same term, ignore")
		}
	case rf.currentTerm > args.Term:
		Debug(dInfo, "S%d rcv outdated from S%d", rf.me, args.LeaderID)
		reply.Success = false
		reply.ConflictIndex = -1
	}

	if args.PrevLogIndex > 0 && !rf.isLogAtIndexExist(args.PrevLogIndex) {
		reply.ConflicTerm = -1
		reply.ConflictIndex = len(rf.log) + 1
		reply.Success = false
	}

	if args.PrevLogIndex > 0 && rf.isLogAtIndexExist(args.PrevLogIndex) && rf.getLogEntryAtIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
		curTerm := rf.getLogEntryAtIndex(args.PrevLogIndex).Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.getLogEntryAtIndex(i).Term != curTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		reply.Success = false
	}

	if !reply.Success {
		Debug(dInfo, "S%d conflict S%d index %d", rf.me, args.LeaderID, reply.ConflictIndex)
		return
	}

	Debug(dInfo, "S%d receive heartbeat from S%d", rf.me, args.LeaderID)

	for i, entry := range args.Entries {
		persist = true
		if rf.isLogAtIndexExist(args.PrevLogIndex + 1 + i) {
			if rf.getLogEntryAtIndex(args.PrevLogIndex+1+i).Term != entry.Term {
				rf.log = rf.log[:args.PrevLogIndex+i]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}

	if persist {
		rf.persist()
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		if len(args.Entries) > 0 && args.LeaderCommitIndex > args.PrevLogIndex+len(args.Entries) {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		} else {
			rf.commitIndex = args.LeaderCommitIndex
		}
	}

	rf.applyEntries()
}

func (rf *Raft) PeriodHeartBeat() {
	rf.mu.Lock()
	npeers := len(rf.peers)
	rf.mu.Unlock()
	for i := 0; i < npeers; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				go func(i int) {
					rf.mu.Lock()
					preLogIndex := rf.nextIndex[i] - 1
					preLogTerm := 0
					var entries []LogEntry
					if rf.isLogAtIndexExist(preLogIndex) {
						preLogTerm = rf.getLogEntryAtIndex(preLogIndex).Term
						entries = rf.log[preLogIndex:]
					} else {
						entries = rf.log[:]
					}
					args := AppendEntriesArgs{
						Term:              rf.currentTerm,
						LeaderID:          rf.me,
						PrevLogIndex:      preLogIndex,
						PrevLogTerm:       preLogTerm,
						Entries:           entries,
						LeaderCommitIndex: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					Debug(dInfo, "S%d send append entries to S%d", rf.me, i)
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(i, &args, &reply)
					if ok {
						rf.mu.Lock()
						Debug(dInfo, "S%d success to send entries to S%d", rf.me, i)
						if rf.state != Leader {
							rf.mu.Unlock()
							return
						}
						if reply.Success {
							rf.resetTimer()
							if rf.nextIndex[i]-1 == args.PrevLogIndex {
								rf.nextIndex[i] = rf.nextIndex[i] + len(args.Entries)
								rf.matchIndex[i] = rf.nextIndex[i] - 1
							}
							rf.updateCommitedIndex()
						} else {
							if reply.ConflictIndex != -1 {
								rf.nextIndex[i] = reply.ConflictIndex
							}

						}
						rf.mu.Unlock()
					}
					if !ok {
						rf.mu.Lock()
						Debug(dInfo, "S%d fail to send entries to S%d", rf.me, i)
						rf.mu.Unlock()
					}

					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.stepDown(reply.Term)
					}
					rf.mu.Unlock()
				}(i)
				constraint := 10
				time.Sleep(time.Second / time.Duration(constraint))
			}
		}(i)
	}

}

func (rf *Raft) resetTimer() {
	rf.lastHeartBeat = time.Now()
	rf.nextTimer = 550 + (rand.Int() % 400)
	Debug(dTimer, "S%d reset timer", rf.me)
}

func (rf *Raft) leaderInit() {
	rf.state = Leader
	rf.persist()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) updateCommitedIndex() bool {
	majority := len(rf.peers) / 2
	for n := rf.commitIndex + 1; n <= rf.getLastLogIndex(); n++ {
		count := 1
		for peerIndex, mI := range rf.matchIndex {
			if peerIndex == rf.me {
				continue
			}

			if mI >= n {
				count++
			}
		}
		if rf.getLogEntryAtIndex(n).Term == rf.currentTerm && count > majority {
			rf.commitIndex = n
		}
	}
	return rf.applyEntries()
}

func (rf *Raft) applyEntries() bool {
	res := false
	if rf.lastApplied < rf.commitIndex {
		Debug(dCommit, "S%d -> chan %d to %d", rf.me, rf.lastApplied+1, rf.commitIndex)
	}
	for rf.lastApplied < rf.commitIndex {
		res = true
		rf.lastApplied += 1
		command := rf.getLogEntryAtIndex(rf.lastApplied).Command
		i := rf.lastApplied
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      command,
		}
	}
	return res
}

func (rf *Raft) isLogAtIndexExist(index int) bool {
	return len(rf.log) >= index && index > 0
}

func (rf *Raft) getLastLogIndex() int {
	res := len(rf.log)
	if res == 0 {
		return rf.LastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return rf.LastIncludedTerm
}

func (rf *Raft) getLogEntryAtIndex(index int) LogEntry {
	if index == 0 {
		return LogEntry{Term: 0}
	}
	return rf.log[index-rf.LastIncludedIndex-1]
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	return rf.getLastLogIndex(), rf.getLastLogTerm()
}

func (rf *Raft) isSelfMoreUpToDate(index, term int) bool {
	myLastLogIndex, myLastLogTerm := rf.getLastLogIndexAndTerm()
	if myLastLogTerm != term {
		return myLastLogTerm > term
	}
	return myLastLogIndex > index
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
	rf.resetTimer()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.applyChan = applyCh
	rf.commitIndex = 0
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
