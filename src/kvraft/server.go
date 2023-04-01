package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	seqNumberSet Set // last seqNumber this server has handled
	data         map[string]string
	applyCond    *sync.Cond
	lastApplied  int

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	index, term, isLeader := kv.rf.Start(Op{
		Type: "Get",
		Key:  args.Key,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// TODO: wait until raft commit this command
	// Bug: no requests, no apply
	ok := kv.apply(index, term)
	if !ok {
		reply.Err = ErrOperation
		return
	}
	reply.Err = OK
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.seqNumberSet.Has(args.SeqNumber) {
		reply.Err = ErrDuplicatedRequest
		return
	}
	index, term, isLeader := kv.rf.Start(Op{Type: args.Op, Key: args.Key, Value: args.Value})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ok := kv.apply(index, term)
	if !ok {
		reply.Err = ErrOperation
		return
	}
	reply.Err = OK
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.seqNumberSet = make(Set)
	kv.data = make(map[string]string)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applyCond = sync.NewCond(&kv.mu)
	kv.lastApplied = 0

	// You may need initialization code here.

	return kv
}

func (kv *KVServer) apply(index int, term int) bool {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot)
			kv.lastApplied = m.SnapshotIndex
		} else if m.CommandValid && kv.lastApplied < m.CommandIndex { // after installing snapshot, lastApplied may increase
			command := m.Command.(Op)
			value, ok := kv.data[command.Key]
			if command.Type == "Put" || command.Type == "Append" && !ok {
				fmt.Printf("Append or Put key:%v value:%v ok(command index: %d)\n", command.Key, command.Value, m.CommandIndex)
				kv.data[command.Key] = command.Value
			} else if command.Type == "Append" {
				fmt.Printf("Append key:%v value:%v ok(command index: %d)\n", command.Key, command.Value, m.CommandIndex)
				kv.data[command.Key] += value
			} else if command.Type == "Get" {
				fmt.Printf("Get key:%v ok(command index: %d)\n", command.Key, m.CommandIndex)
			}
			kv.lastApplied = m.CommandIndex
			// Log Matching rule
			if index == m.CommandIndex {
				if term == m.CommandTerm {
					return true
				} else {
					return false
				}
			}
			// TODO: make snapshot
		}
	}
	return false
}
