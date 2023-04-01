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
	seqNumber  int64
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
	ck.seqNumber = nrand()
	ck.lastLeader = ck.randomServer()
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
	args := GetArgs{Key: key, ClerkId: ck.clerkId}
	reply := GetReply{}
	// choose a random server to request
	server := ck.lastLeader
	fmt.Printf("%d Get key:%v\n", ck.clerkId, key)
	// keep trying forever
	for true {
		for !ck.servers[server].Call("KVServer.Get", &args, &reply) {
		}
		if reply.Err == ErrWrongLeader {
			server = ck.randomServer()
		} else if reply.Err == OK {
			ck.lastLeader = server
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		}
	}
	return ""
}

func (ck *Clerk) randomServer() int {
	max := big.NewInt(int64(len(ck.servers) - 1))
	bigx, _ := rand.Int(rand.Reader, max)
	server := int(bigx.Int64())
	// fmt.Printf("%d choose %d\n", ck.clerkId, server)
	return server
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
	reply := PutAppendReply{}
	server := ck.lastLeader
	fmt.Printf("%d %s key:%v, value: %v\n", ck.clerkId, op, key, value)
	for true {
		for !ck.servers[server].Call("KVServer.PutAppend", &args, &reply) {
		}
		if reply.Err == ErrWrongLeader {
			server = ck.randomServer()
		} else if reply.Err == OK {
			ck.lastLeader = server
			ck.seqNumber = nrand()
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
