package kvraft

import (
	"6.824/labrpc"
	"fmt"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId    int64
	seqNumber  int
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand()
	ck.seqNumber = 1
	ck.lastLeader = 0
	// You'll have to add code here.
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, ClerkId: ck.clerkId, SeqNumber: ck.seqNumber}

	// choose a random server to request
	server := ck.lastLeader
	fmt.Printf("%d Get key:%v\n", ck.clerkId, key)
	// keep trying forever
	for {
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if reply.Err == ErrWrongLeader || !ok {
			server = (server + 1) % len(ck.servers)
			continue
		} else if reply.Err == OK {
			ck.lastLeader = server
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		}
		server = (server + 1) % len(ck.servers)
	}
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.clerkId,
		SeqNumber: ck.seqNumber,
	}
	server := ck.lastLeader
	fmt.Printf("%d %s key:%v, value: %v\n", ck.clerkId, op, key, value)
	for {
		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == ErrWrongLeader || !ok {
			server = (server + 1) % len(ck.servers)
		} else if reply.Err == OK {
			ck.lastLeader = server
			ck.seqNumber++
			return
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
