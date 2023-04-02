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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
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
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type State string

type Log struct {
	Term  int
	Cmd   interface{}
	Index int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	electionTime  time.Time
	heartsbeats   time.Duration
	currentTerm   int
	votedFor      int
	commitedIndex int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	state         State
	logs          []Log
	applyCh       chan ApplyMsg
	applyCond     *sync.Cond

	// 2D
	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

const (
	Leader    State = "Leader"
	Follower        = "Follower"
	Candidate       = "Candidate"
)

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool

	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var logs []Log
	var votedFor int
	var lastIncludedTerm int
	var lastIncludedIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&logs) != nil || d.Decode(&votedFor) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.logs = logs
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		rf.lastApplied = lastIncludedIndex
		rf.commitedIndex = lastIncludedIndex
	}
	//rf.printState()
}

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// raft has process entries after lastIncludedIndex
	if rf.commitedIndex > lastIncludedIndex {
		return false
	}
	rf.lastIncludedTerm = lastIncludedTerm
	dummyLog := rf.logs[0:1]
	if rf.lastIndex() > lastIncludedIndex {
		dummyLog = append(dummyLog, rf.slice(lastIncludedIndex+1)...)
	}
	rf.lastIncludedIndex = lastIncludedIndex
	dummyLog[0].Term = rf.lastIncludedTerm
	dummyLog[0].Index = lastIncludedIndex
	rf.logs = dummyLog
	rf.snapshot = snapshot

	rf.lastApplied = lastIncludedIndex
	rf.commitedIndex = lastIncludedIndex

	rf.persist()
	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// case 1: follower receive leader's snapshot RPC that trim logs
	// case 2: raft state already snapshotted
	// case 3: can not going forward or backward
	if index > rf.lastIndex() || rf.lastIncludedIndex >= index || index != rf.lastApplied {
		return
	}
	rf.lastIncludedTerm = rf.logAt(index).Term
	dummyLog := rf.logs[0:1]
	dummyLog = append(dummyLog, rf.slice(index+1)...)
	rf.lastIncludedIndex = index
	dummyLog[0].Term = rf.lastIncludedTerm
	dummyLog[0].Index = index
	rf.logs = dummyLog
	rf.snapshot = snapshot
	rf.persist()
	//fmt.Printf("%d made snapshot at %d, Log: %v, lastIncludedIndex: %d, lastIncludedTerm: %d\n", rf.me, index, rf.logs, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	Conflit bool
	XLen    int
}

type InstallsnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) Installsnapshot(args *InstallsnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("[%d]: (term %d) follower 收到 [%v] Installsnapshot %v, lastIndex %v, lastTerm %v, logs %v\n", rf.me, rf.currentTerm, args.LeaderId, args, args.LastIncludedIndex, args.LastIncludedTerm, rf.logs)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	rf.resetTimer()
	if rf.state == Candidate {
		rf.state = Follower
	}
	// follower will compact itself
	if rf.commitedIndex >= args.LastIncludedIndex {
		return
	}

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	// do it asyncronously
	//fmt.Printf("[%d] send Install msg\n", rf.me)
	go func() {
		rf.applyCh <- applyMsg
	}()
}

func (rf *Raft) handleInstallSnapshot(server int, args *InstallsnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.sendSnapshot(server, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
	rf.nextIndex[server] = max(rf.nextIndex[server], rf.matchIndex[server]+1)
	//fmt.Printf("[%d] %d Install snapshot sucess next %d, match %d\n", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	// lock to prevent multiple appendEntries requests
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("[%d]: (term %d) follower 收到 [%v] AppendEntries %v, prevIndex %v, prevTerm %v, logs %v,leaderCommit %v\n", rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm, rf.logs, args.LeaderCommit)

	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	// don't handle old message
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetTimer()

	// receive old appendEntries after follower made snapshot
	if args.PrevLogIndex < rf.firstIndex() {
		reply.Success = true
		return
	}

	// log inconsistency
	if rf.lastIndex() < args.PrevLogIndex {
		reply.XLen = rf.lastIndex() + 1
		reply.Conflit = true
		reply.XTerm = -1
		reply.XIndex = -1
		//fmt.Printf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v\n", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	if rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm {
		// optimization that backs up nextIndex by more than one entry at a time
		reply.Conflit = true

		// need to set XIndex default 0
		reply.XIndex = rf.firstIndex()
		xTerm := rf.logAt(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex; xIndex > rf.firstIndex(); xIndex-- {
			if rf.logAt(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.lastIndex() + 1
		//fmt.Printf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v\n", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	// appendEntries rpc 3
	// for idx, entry := range args.Entries {
	// 	index := args.PrevLogIndex + idx + 1
	// 	if index <= rf.lastIndex() && rf.logs[index].Term != entry.Term {
	// 		rf.logs = rf.logs[0:index]
	// 		rf.persist()
	// 	}
	// 	if index > rf.lastIndex() {
	// 		rf.logs = append(rf.logs, args.Entries[idx:]...)
	// fmt.Printf("[%d]: follower append [%v]\n", rf.me, args.Entries[idx:])
	// 		rf.persist()
	// 		break
	// 	}
	// }
	var hasAllEntries = true
	if len(args.Entries) > 0 {
		for index, log := range args.Entries {
			if args.PrevLogIndex+index+1 > rf.lastIndex() || rf.logAt(args.PrevLogIndex+index+1).Term != log.Term {
				hasAllEntries = false
				break
			}
		}
	}
	if !hasAllEntries {
		rf.logs = rf.trim(args.PrevLogIndex + 1)
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}

	// update commitedIndex
	if args.LeaderCommit > rf.commitedIndex {
		rf.commitedIndex = min(rf.lastIndex(), args.LeaderCommit)
		rf.applyCond.Broadcast()
	}
	// fmt.Printf("%d append %d log %v Success\n", rf.me, args.LeaderId, *args)
	// fmt.Printf("%d term: %d, lastApplied: %d. commitedIndex: %d, log: %v\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitedIndex, rf.logs)

	reply.Success = true
}
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// lock to prevent multiple requestVote requests
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// old implementation: rf.votedFor = args.CandidateId
	// need to update, or raft will never vote for other
	// and result to infinite election
	// consider this case, a partitioned network with three followers,
	// one follower has up-to-date logs while another one has higher term,
	// the two followers timeout and voted for themselves, with old implementation
	// the two followers never update their votedFor, so they will never vote for each other.
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VotedGranted = false
		//fmt.Printf("[%d] refuse to voted for %d with higher term\n", rf.me, args.CandidateId)
		return
	}
	isUpToDate := args.LastLogTerm > rf.lastLog().Term || args.LastLogTerm == rf.lastLog().Term &&
		args.LastLogIndex >= rf.lastIndex()
	thisTermNotVoted := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	if thisTermNotVoted && isUpToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetTimer()
		reply.VotedGranted = true
		//fmt.Printf("[%d] voted for %d\n", rf.me, args.CandidateId)
	} else {
		//fmt.Printf("[%d] refuse to voted for %d\n", rf.me, args.CandidateId)
		reply.VotedGranted = false
	}
	reply.Term = rf.currentTerm
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
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendSnapshot(server int, args *InstallsnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.Installsnapshot", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// TODO: 实现PreVote机制
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	//fmt.Printf("[%d] start election at %d\n", rf.me, rf.currentTerm)
	rf.persist()
	rf.resetTimer()

	votes := 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastLog().Term,
	}
	for server := range rf.peers {
		if server != rf.me {
			go rf.handleRequestVote(server, &args, &votes)
		}
	}
}

func (rf *Raft) handleRequestVote(server int, args *RequestVoteArgs, votes *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// reset votedFor
		rf.becomeFollower(reply.Term)
		return
	}
	// don't handle old rpc
	if reply.Term < rf.currentTerm {
		return
	}
	if !reply.VotedGranted {
		return
	}
	if reply.VotedGranted {
		*votes++
	}

	if *votes > len(rf.peers)/2 && rf.state == Candidate && args.Term == rf.currentTerm {
		rf.state = Leader

		// initialize matchIndex and nextIndex
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i] = 0
			// initialize to leader's last log+1
			rf.nextIndex[i] = rf.lastIndex() + 1
		}
		//rf.printState()

		// sending heartsbeats
		rf.leaderAppendEntries(true)
	}
}

func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs) {
	// if it already step down, don't send appendEntries
	// in case that leader received a higher term while
	// hasn't sent all appendEntries out
	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// find a higher term, become follower
	if reply.Term > rf.currentTerm {
		// reset votedFor
		rf.becomeFollower(reply.Term)
		// if step down, return immediately
		return
	}
	if args.Term == rf.currentTerm {
		if reply.Success {
			// rf.matchIndex[server] = rf.lastIndex()
			// This is not safe, because rf.lastIndex could have been updated since when you sent the RPC
			// for example, after leader sending appendentries, it begins a Start() again.
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.matchIndex[server] = max(match, rf.matchIndex[server])
			rf.nextIndex[server] = max(next, rf.nextIndex[server])
			//fmt.Printf("[%v]: %v append success: next %v match %v\n", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
		} else if reply.Conflit {
			//fmt.Printf("[%v]: Conflict from %v %#v\n", rf.me, server, reply)
			// prevIndex > follower's lastIndex
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XLen
			} else {
				lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
				//fmt.Printf("[%v]: lastLogInXTerm %v\n", rf.me, lastLogInXTerm)
				if lastLogInXTerm > 0 {
					rf.nextIndex[server] = lastLogInXTerm
				} else {
					rf.nextIndex[server] = reply.XIndex
				}
			}
			//fmt.Printf("[%v]: leader nextIndex[%v] %v\n", rf.me, server, rf.nextIndex[server])
		} else if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}
		rf.leaderCommit()
	}
}

func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.lastIndex(); i >= rf.firstIndex(); i-- {
		term := rf.logAt(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

func (rf *Raft) printState() {
	fmt.Printf("--------------------------------------------------\n")
	fmt.Printf("id: %d state: %s,term: %d,committed: %d, lastApplied: %d, logs: %v\n", rf.me, rf.state, rf.currentTerm, rf.commitedIndex, rf.lastApplied, rf.logs)
	if rf.state == Leader {
		fmt.Printf("nextIndex: ")
		for server := range rf.peers {
			if rf.me != server {
				fmt.Printf("[%d]->%d ", server, rf.nextIndex[server])
			}
		}
		fmt.Printf("\n")
		fmt.Printf("matchIndex: ")
		for server := range rf.peers {
			if rf.me != server {
				fmt.Printf("[%d]->%d ", server, rf.matchIndex[server])
			}
		}
	}
	fmt.Printf("\n--------------------------------------------------\n")
}

// TODO: 实现Leader checkQuorum机制
func (rf *Raft) leaderAppendEntries(heartbeat bool) {
	rf.resetTimer()
	for i := range rf.peers {
		if rf.me != i {
			// rules for leader 3
			entries := []Log{}
			if rf.nextIndex[i] <= rf.lastIndex() || heartbeat {
				nextIndex := rf.nextIndex[i]
				if nextIndex > rf.lastIndex()+1 {
					nextIndex = rf.lastIndex()
				}
				if nextIndex <= 0 {
					nextIndex = 1
				}
				prevIndex := nextIndex - 1
				// previous log has been trimmed
				if prevIndex < rf.firstIndex() && prevIndex != 0 {
					args := InstallsnapshotArgs{
						Snapshot:          rf.snapshot,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
					}
					go rf.handleInstallSnapshot(i, &args)
				} else {
					entries = append(entries, rf.slice(nextIndex)...)
					prevTerm := rf.logAt(prevIndex).Term
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevIndex,
						PrevLogTerm:  prevTerm,
						Entries:      entries,
						LeaderCommit: rf.commitedIndex,
					}
					go rf.handleAppendEntries(i, &args)
				}
			}
		}
	}
}

func (rf *Raft) lastIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) lastLog() *Log {
	return rf.logAt(rf.lastIndex())
}

func (rf *Raft) firstIndex() int {
	return rf.lastIncludedIndex
}

func (rf *Raft) logAt(index int) *Log {
	return &rf.logs[index-rf.firstIndex()]
}

func (rf *Raft) trim(index int) []Log {
	return rf.logs[:index-rf.firstIndex()]
}

func (rf *Raft) slice(index int) []Log {
	return rf.logs[index-rf.firstIndex():]
}

// Start the service using Raft (e.g. a k/v server) wants to start
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if not leader return false
	if rf.state != Leader {
		return len(rf.logs), rf.currentTerm, false
	}

	term = rf.currentTerm
	index = rf.lastLog().Index + 1
	log := Log{Term: term, Cmd: command, Index: index}
	//fmt.Printf("[%v]: term %v Start %#v\n", rf.me, term, log)
	rf.logs = append(rf.logs, log)
	rf.persist()
	// index start from 1
	rf.leaderAppendEntries(false)
	return index, term, true
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// waiting for heartsbeats
		time.Sleep(rf.heartsbeats)

		rf.mu.Lock()
		if rf.state == Leader {
			// leader reset time prevent from election
			// send heartsbeats
			rf.leaderAppendEntries(true)
		}

		// if no heartsbeats has been received
		if time.Now().After(rf.electionTime) && rf.state != Leader {
			// start election(Follower) or restart election(candidate)
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetTimer() {
	timeouts := time.Duration(rand.Intn(150)+200) * time.Millisecond
	rf.electionTime = time.Now().Add(timeouts)
}

// Make the service or tester wants to create a Raft server. the ports
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
	rf.heartsbeats = 50 * time.Millisecond
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = 1
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logs = append(rf.logs, Log{})
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.snapshot = persister.ReadSnapshot()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) applier() {
	// use condition value to avoid applier thread keep holding
	// the lock. when commitedIndex<=lastApplied wait.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.killed() == false {
		// commit log
		if rf.commitedIndex > rf.lastApplied && rf.lastIndex() > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logAt(rf.lastApplied).Cmd,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.logAt(rf.lastApplied).Term,
			}
			//fmt.Printf("[%v]: COMMIT %d: %#v\n", rf.me, rf.lastApplied, rf.logAt(rf.lastApplied))
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) leaderCommit() {
	if rf.state != Leader {
		return
	}
	for i := rf.commitedIndex + 1; i <= rf.lastIndex(); i++ {
		// without locking,two different leader will modify count concurrently,
		// making count greater than majority when it doesn't

		// optimization: bypass logs that have different term with currentTerm
		if rf.logAt(i).Term != rf.currentTerm {
			continue
		}
		count := 1
		for peer := range rf.peers {
			if rf.me != peer && rf.matchIndex[peer] >= i {
				count++
			}
			if count > len(rf.peers)/2 {
				rf.commitedIndex = i
				// when ready to commit, wake up applier thread
				rf.applyCond.Broadcast()
				break
			}
		}
	}
}
