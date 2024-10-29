package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut	   = "ErrTimeOut"
	ErrWrongShard  = "ErrWrongShard"
	ErrShardNotArrived = "ErrShardNotArrived"
	ErrConfigNotArrived = "ErrConfigNotArrived"
)

type Err string

const (
	PutOp    		= "PutOp"
	AppendOp      = "AppendOp"
	GetOp         = "GetOp"
	DeleteShardOp   = "DeleteShardOp"
	UpdateConfigOp  = "UpdateConfigOp"
	AddShardOp		= "AddShardOp"
)

type OpType string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SequenceNum int
	ClientId  int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	SequenceNum int
	ClientId  int64
	Op  string
	Shard int
}

type GetReply struct {
	Err   Err
	Value string
}

// Your server will need to periodically poll the shardctrler to learn about new configurations.
type SendShardArgs struct {
	Session		map[int64]OpResult
	ShardId		int
	Shard		Shard
	GroupId		int
	ConfigNum	int
}

type SendShardReply struct {
	Err		Err
}