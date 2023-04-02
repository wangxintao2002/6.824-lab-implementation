# 6.824-lab-implementation
This is a implementation for 6.824 lab. Each part has passed 2000 tests.
## 前言

6.824的lab2是实现一个分布式共识算法raft，raft主要用于管理复制状态机之间日志的复制，保证日志的一致性，是一个有着强Leader限制的算法，日志只从Leader流向Follower，与同为共识算法的Paxos相比更易理解和实现。

## 学习背景

在做lab之前花点时间学习go语言基础语法，然后最好把 [Raft论文](http://nil.csail.mit.edu/6.824/2021/papers/raft-extended.pdf)熟读，理解Raft是如何工作的，再结合Raft官方做的[算法可视化](https://raft.github.io/)，基本上就能顺利的做出来。

## Raft实例结构

```go
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
```

根据论文的Figure2可以很轻松的写出raft的各个属性

- electionTime：每个raft实例的选举超时时间，follower如果在这时间之前未**收到来自leader的append entry**或**给一名candidate投票**，便成为candidate开始选举
- heartsbeats：心跳包发送周期

## Make( )函数——创建一个raft实例

```go
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
```

Raft以Follower的状态加入集群，当实例化raft时，需要设置一个**随机的**选举超时时间，以避免多个follower同时进行选举瓜分选票，还需要从Storage层读取volatile的状态（**currentTerm**、**votedfor**、**logs**、**lastIncludedIndex**、**lastIncludedTerm**）。并开启两个线程分别运行ticker和applier函数。

## ticker( )函数

```go
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
```

每隔一个心跳包周期，便进行一次判断：

- 对于Leader，发送append entries。
- 对于Follower或Candidate：如果**现在的时间超过了选举超时时间**，开始选举。

## Part 2A：Leader Election

实验第一部分是实现Leader的选举，当一个Follower计时器超时，就发起选举，给其他Raft发送RequestVote RPC。

### RequestVote RPC结构

```go
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
```

RPC中的**LastLogTerm**和**LastLogIndex**用于判断投票者和Candidate**谁的Log更up-to-date**。这两项在2A中用不上，因为在2A的测试用例里没有Log Replication的过程，所以投票者只需要比较自己的Term和RPC里的Term以及当前Term是否投过票来决定是否投票。

### RequestVote Reply

```go
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}
```

RequestVote的Reply比较简单，VoteGranted判断投票者是否投了票，Term则是接受者的Term。

### startElection( )

> To begin an election, a follower increments its current term and transitions to candidate state. It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster. 

```go
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	fmt.Printf("[%d] start election at %d\n", rf.me, rf.currentTerm)
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
```

这里用代码将论文里的描述复现，对每一个raft开启一个线程并行的发送RPC。这里需要给每一个Raft发送一个votes参数指针，用来记录Candidate收到的票数。

### handleRequestVote( )

```go
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
		rf.becomeLeader(reply.Term)
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
		rf.printState()

		// sending heartsbeats
		rf.leaderAppendEntries(true)
	}
}
```

Candidate在handleRequestVote函数处理每个Raft对其RequestVote的回复：

- 如果收到的Term比自己的Term大，根据论文里对所有server的规定，需要转变为Follower，并把当前Term设为更大的Term。
- 如果收到的Term比自己的Term小，说明Reply已经过期，该选票不是对当前Term的投票，直接返回不进行处理。
- 如果Candidate收到一个票数，votes加一，判断当前的票数是否大于一半的Raft数，还要判断自己当前是否还是Candidate（**有可能Candidate在处理其他选票时变成了Follower并返回**）以及RPC的Term是否和自己当前Term一致，不一致则说明这是旧的回复。
- 成功成为Leader后，初始化nextIndex和matchIndex，并给其他Raft发送AppendEntries。

### RequestVote( )

```go
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// lock to prevent multiple requestVote requests
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.becomeLeader(args.Term)
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VotedGranted = false
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
		fmt.Printf("[%d] voted for %d\n", rf.me, args.CandidateId)
	} else {
		fmt.Printf("[%d] refuse to voted for %d\n", rf.me, args.CandidateId)
		reply.VotedGranted = false
	}
	reply.Term = rf.currentTerm
}
```

Raft收到RPC后，先比较Term，若RPC中的Term更大，**无论自己当前是什么状态**都装变为Follower并将自己的votedFor置为-1；

若更小，则拒绝投票。

根据论文5.4.1的Election restriction，raft只会给大于等于自己当前Term且Candidate的Log至少和自己的一样up-to-date的Candidate投票，**成功投票后才会重置计时器**。

### 总结

PartA还算简单，照着论文的描述自己用代码复现一遍很快就能做出来，注意要完全按照论文里的描述来实现，如果按照自己的思路来的话可能会陷入一个及其痛苦的debug过程。这里的Leader选举还涉及有一个**PreVote的机制**，这个机制可以防止某个被partition的节点暴增currentTerm，也可以保证Leader的稳定性，日后有时间再进行实现

## Part2B：Log Replication

2B实现Raft的第二个部分，日志复制。server层接受客户端请求后通过Start函数向raft交付日志并开始复制。

### Start( )

```go
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

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
	fmt.Printf("[%v]: term %v Start %v\n", rf.me, term, log)
	rf.logs = append(rf.logs, log)
	rf.persist()
	// index start from 1
	rf.leaderAppendEntries(false)
	return index, term, isLeader
}
```

 Leader接收到请求后，将命令和Term做成日志还有该**日志项的index**（Lab2D中snapshot机制需要用到）追加到自己的日志中，并给Followers发送appendEntries

### LeaderAppendEntries( )

```go
func (rf *Raft) leaderAppendEntries(heartbeat bool) {
	rf.resetTimer()
	for i := range rf.peers {
		if rf.me != i {
			// rules for leader 3
			entries := []Log{}
			if rf.nextIndex[i] <= rf.lastIndex() || heartbeat {
				nextIndex := rf.nextIndex[i]
				if nextIndex <= 0 {
					nextIndex = 1
				}
				prevIndex := nextIndex - 1
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
```

该函数为Follower发送AppendEntries，如果不发送日志则为一个心跳包，需要注意的是，只有对于某个Follower,Leader的nextIndex小于Leader最后一项日志的索引，或是当前是心跳包时，才会发送。

如果nextIndex<=0(下标从1开始)，就设其为1，防止数组下标出界。为了提高效率可以将Leader从prevIndex之后的所有日志一并发出去。

### AppendEntries( )

先看看Follower如何处理AppendEntries RPC

```go
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	// lock to prevent multiple appendEntries requests
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.becomeLeader(args.Term)
	}
	// don't handle old message
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetTimer()

	// log inconsistency
	if rf.lastIndex() < args.PrevLogIndex {
		return
	}
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
	reply.Success = true
}
```

Follower收到Append Entries RPC后一样先判断Term大小，RPC的Term大，说明自己过时了，转为Follower；RPC的Term小，说明这是过时的RPC或者来自过时的Leader（比如说刚从网络分区恢复的Leader）。

- 判断自己的lastIndex，**若小于PrevLogIndex，说明Follower的log落后太多了**，直接放回false（后面会有优化方法加快Leader定位nextIndex的过程，在2B中还不需要实现这一方法）
- 注意，在追加Leader发来的日志时，**不能简单地将PrevLogIndex之后的日志全部换成Leader发来的日志**，这样会导致**旧RPC把Follower已经追加的新RPC的日志给删掉**，虽然不会导致安全性问题，但是这无疑增加了Leader需要发送RPC的数量，增加了网络负载。我们需要在追加日之前，检查是否已经包含RPC中的所有日志，如果包含，直接放回，不包含就把不包含的日志追加。
- 追加完成之后，记得**更新Follower的commit index**

### AppendEntries( )

接下来看看Leader如何处理Follower的回复

```go
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
   rf.becomeLeader(reply.Term)
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
   } else  {
      rf.nextIndex[server]--
   }
   rf.leaderCommit()
}
```

- 如果RPC Reply中Success为true，说明追加一致性检查通过，需要更新该Follower的nextIndex和matchIndex。注意：**不能直接的将nextIndex设为Leader当前的最后一条日志的Index**，因为可能Leader在发出AppendEntries后，日志的长度可能会发生改变，需将nextIndex设为PrevLogIndex+len(args.entries)。
- 若不成功，递减nextIndex

- 处理好后，Leader开始判断哪些日志可以提交，哪些不能

```go
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
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}
```

接下来看看applier，applier是每个Raft实例创建时开启的一个goroutine，用于将日志传输到applyChan，applier利用条件变量来控制同步。

- 若commit index > lastApplied，就要进行apply
- 反之，等待直到上面条件成立

```go
func (rf *Raft) leaderCommit() {
	if rf.state != Leader {
		return
	}
	for i := rf.commitedIndex + 1; i <= rf.lastIndex(); i++ {
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
```

Leader在处理完每个Follower的Reply后，就会判断是否有超过半数节点的matchIndex大于等于某一值，若等于，说明**该索引上日志已经可以被提交**，被则将commit index设为该值。之后唤醒applier线程进行apply

### 总结

......没什么好总结的，更新matchIndex和nextIndex时要注意取更新前和后的最大值以免日志回溯。

# Lab2C：persistence

