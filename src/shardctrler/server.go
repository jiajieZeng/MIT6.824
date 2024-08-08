package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead 			int
	session			map[int64] int
	opCh			map[int]chan Op
	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	ClientId	int64
	SequenceNum	int
	OpType		string
	// Join
	Servers 	map[int][]string
	// Leave
	GIDs		[]int
	// Move
	Shard		int
	GID			int
	// Query
	Num			int
	Cfg  		Config
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op {
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		Servers: 	args.Servers,
	}
	logIndex, _, isLeader := sc.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	opCh := sc.getOpCh(logIndex)
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- opCh:
		reply.WrongLeader = appliedOp.WrongLeader
		reply.Err = appliedOp.Err
	case <- timer.C:
		reply.Err = ErrTimeOut
		reply.WrongLeader = false
	}
	timer.Stop()
	go func() {
		sc.mu.Lock()
		if len(sc.opCh[logIndex]) == 0 {
			delete(sc.opCh, logIndex)
		}
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op {
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		GIDs:		args.GIDs,
	}
	logIndex, _, isLeader := sc.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	opCh := sc.getOpCh(logIndex)
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- opCh:
		if op.ClientId != appliedOp.ClientId || op.SequenceNum != appliedOp.SequenceNum {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
		}
	case <- timer.C:
		reply.Err = ErrTimeOut
		reply.WrongLeader = false
	}
	timer.Stop()
	go func() {
		sc.mu.Lock()
		if len(sc.opCh[logIndex]) == 0 {
			delete(sc.opCh, logIndex)
		}
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op {
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		Shard: 		args.Shard,
		GID:		args.GID,
	}
	logIndex, _, isLeader := sc.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	opCh := sc.getOpCh(logIndex)
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- opCh:
		if op.ClientId != appliedOp.ClientId || op.SequenceNum != appliedOp.SequenceNum {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
		}
	case <- timer.C:
		reply.Err = ErrTimeOut
		reply.WrongLeader = false
	}
	timer.Stop()
	go func() {
		sc.mu.Lock()
		if len(sc.opCh[logIndex]) == 0 {
			delete(sc.opCh, logIndex)
		}
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op {
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		Num:		args.Num,
	}
	logIndex, _, isLeader := sc.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	opCh := sc.getOpCh(logIndex)
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- opCh:
		if op.ClientId != appliedOp.ClientId || op.SequenceNum != appliedOp.SequenceNum {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.Err = OK
			reply.WrongLeader = false
		}
	case <- timer.C:
		reply.Err = ErrTimeOut
		reply.WrongLeader = false
	}
	timer.Stop()
	go func() {
		sc.mu.Lock()
		if len(sc.opCh[logIndex]) == 0 {
			delete(sc.opCh, logIndex)
		}
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) isDuplicate(clientId int64, sequenceNum int) -> bool {
	lastResponse, exist := kv.session[clientId]
	if exist && lastResponse.SequenceNum >= sequenceNum {
		return true
	}
	return false
}

func (sc *ShardCtrler) JoinHandler(Servers map[int][]string) {

}

func (sc *ShardCtrler) LeaveHandler(GIDs []int) {

}

func (sc *ShardCtrler) MoveHandler(GID []int) {

}

func (sc *ShardCtrler) QueryHandler(Num int) {

}

func (sc *ShardCtrler) applyMessage() {
	for {
		select {
		case msg := <- sc.applyCh:
			sc.mu.Lock()
			op := mgs.Command.(Op)
			if !isDuplicate(op.ClientId, op.SequenceNum) {
				switch op.OpType {
				case JoinOp:
					sc.JoinHandler(op.Servers)
				case LeaveOp:
					sc.LeaveHandler(op.GIDs)
				case MoveOp:
					sc.MoveHandler(op.GID)
				case QueryOp:
					sc.QueryHandler(op.Num)
				}
				sc.session[op.ClientId] = op.SequenceNum
			}
		}
	}
}

func (sc *ShardCtrler) getOpCh(index int) chan OpResult {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ret, exist := sc.opCh[index]
	if !exist {
		sc.opCh[index] = make(chan OpResult, 1)
		ret = sc.opCh[index]
	}
	return ret
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.session = make(map[int64]int)
	sc.opCh = make(map[int]chan Op)
	go sc.applyMessage()
	return sc
}
