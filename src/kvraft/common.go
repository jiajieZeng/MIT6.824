package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut	   = "ErrTimeOut"
)

type Err string

const (
	PutOp    		= "PutOp"
	AppendOp      = "AppendOp"
	GetOp         = "GetOp"
)

type OpType string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId	int64
	SequenceNum	int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key 		string
	// You'll have to add definitions here.
	SequenceNum	int
	ClientId	int64
}

type GetReply struct {
	Err   Err
	Value string
}