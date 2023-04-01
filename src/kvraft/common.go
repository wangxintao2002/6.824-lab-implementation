package kvraft

const (
	OK                   = "OK"
	ErrNoKey             = "ErrNoKey"
	ErrWrongLeader       = "ErrWrongLeader"
	ErrDuplicatedRequest = "ErrDuplicatedRequest"
	ErrOperation         = "ErrOperation"
)

type Set map[int64]struct{}

func (s Set) Has(key int64) bool {
	_, ok := s[key]
	return ok
}

func (s Set) Add(key int64) {
	s[key] = struct{}{}
}

func (s Set) Delete(key int64) {
	delete(s, key)
}

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	// for deduplicate
	SeqNumber int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	// for deduplicate
}

type GetReply struct {
	Err   Err
	Value string
}
