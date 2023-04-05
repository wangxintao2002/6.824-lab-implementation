package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	Type      string
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type Command struct {
	Term  int
	Index string
}

type KVServer struct {
	mu                 sync.Mutex
	me                 int
	rf                 *raft.Raft
	applyCh            chan raft.ApplyMsg
	committedCommandCh map[int]chan Command // communicate between main and applier
	dead               int32                // set by Kill()

	maxraftstate int           // snapshot if log grows this big
	seqNumberSet map[int64]int // last seqNumber this server has handled
	data         map[string]string
	lastApplied  int

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	index, term, _ := kv.rf.Start(Op{
		Type:      "Get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClerkId,
		RequestId: args.SeqNumber,
	})
	ch := kv.getChan(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.committedCommandCh, index)
		kv.mu.Unlock()
	}()
	select {
	case command := <-ch:
		if command.Term == term {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.data[command.Index]
			kv.mu.Unlock()
			return
		} else {
			reply.Err = ErrOperation
			return
		}

	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		return
	}
}

func (kv *KVServer) getChan(index int) chan Command {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.committedCommandCh[index]
	if !ok {
		ch = make(chan Command, 1)
		kv.committedCommandCh[index] = ch
	}
	return ch
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	index, term, _ := kv.rf.Start(Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClerkId,
		RequestId: args.SeqNumber,
	})
	ch := kv.getChan(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.committedCommandCh, index)
		kv.mu.Unlock()
	}()
	select {
	case command := <-ch:
		if command.Term == term {
			reply.Err = OK
			return
		} else {
			reply.Err = ErrOperation
			return
		}

	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
		return
	}
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
	kv.seqNumberSet = make(map[int64]int)
	kv.data = make(map[string]string)
	kv.committedCommandCh = make(map[int]chan Command)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApplied = 0
	snapshot := kv.rf.ReadSnapshot()
	kv.DecodeSnapshot(snapshot)
	// fmt.Printf("%d read state: kv %#v set %#v\n",kv.me, kv.data,kv.seqNumberSet)
	go kv.apply()

	return kv
}

func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqNumberSet[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *KVServer) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	data := kv.data
	seqSet := kv.seqNumberSet
	//fmt.Printf("%d make state: kv %#v set %#v\n",kv.me, kv.data,kv.seqNumberSet)
	kv.mu.Unlock()
	_ = e.Encode(data)
	_ = e.Encode(seqSet)
	return w.Bytes()
}

func (kv *KVServer) DecodeSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var seqSet map[int64]int
	if d.Decode(&data) == nil && d.Decode(&seqSet) == nil {
		kv.data = data
		kv.seqNumberSet = seqSet
	} else {
	}
}

func (kv *KVServer) apply() {
	for !kv.killed() {
		m := <-kv.applyCh
		if m.SnapshotValid {
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {
				kv.mu.Lock()
				kv.DecodeSnapshot(m.Snapshot)
				//fmt.Printf("%d install snapshot: kv %#v set %#v\n", kv.me, kv.data, kv.seqNumberSet)
				kv.lastApplied = m.SnapshotIndex
				kv.mu.Unlock()
			}
		} else if m.CommandValid && kv.lastApplied < m.CommandIndex { // after installing snapshot, lastApplied may increase
			command := m.Command.(Op)
			if !kv.ifDuplicate(command.ClientId, command.RequestId) {
				kv.mu.Lock()
				switch command.Type {
				case "Put":
					//fmt.Printf("%d appending %v\n", kv.me, command.Value)
					kv.data[command.Key] = command.Value
					//fmt.Printf("%d Append or Put key:%v new value:%v ok(command index: %d)\n", kv.me, command.Key, kv.data[command.Key], m.CommandIndex)
				case "Append":
					kv.data[command.Key] = "" + kv.data[command.Key] + command.Value
					//fmt.Printf("%d Append key:%v value:%v ok(command index: %d)\n", kv.me, command.Key, kv.data[command.Key], m.CommandIndex)
				}
				kv.seqNumberSet[command.ClientId] = command.RequestId
				kv.mu.Unlock()
			}
			kv.lastApplied = m.CommandIndex
			kv.getChan(m.CommandIndex) <- Command{m.CommandTerm, command.Key}
			if kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
				snapshot := kv.MakeSnapshot()
				kv.rf.Snapshot(m.CommandIndex, snapshot)
			}
		}
	}
}
