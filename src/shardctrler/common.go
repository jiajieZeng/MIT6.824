package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid  // shard对应的是gid, 那么shards[i] = j表示i号分片在j号组下面
	Groups map[int][]string // gid -> servers[] 这个map仅仅是用来通过gid在这个配置中找到server的而已，是一个可查的表，跟负载均衡没关系
}

const (
	OK = "OK"
	ErrTimeOut = "ErrTimeOut"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	JoinOp = "JoinOp"
	LeaveOp = "LeaveOp"
	QueryOp = "QueryOp"
	MoveOp = "MoveOp"
)

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientId int64
	SequenceNum int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	ClientId int64
	SequenceNum int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ClientId int64
	SequenceNum int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	ClientId int64
	SequenceNum int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
