package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "sort"
import "time"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead 			int
	session			map[int64] OpResult
	opCh			map[int]chan OpResult
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
}

type OpResult struct {
	SequenceNum	int
	Cfg  		Config
	Error       Err
	WrongLeader bool
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op {
		ClientId: args.ClientId,
		SequenceNum: args.SequenceNum,
		Servers: 	args.Servers,
		OpType: 	JoinOp,
	}
	DPrintf("[Join] Server [%v] get args [%v]", sc.rf.GetMe(), args)
	logIndex, _, isLeader := sc.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	opCh := sc.getOpCh(logIndex)
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- opCh:
		reply.Err = appliedOp.Error
		reply.WrongLeader = appliedOp.WrongLeader
	case <- timer.C:
		reply.Err = ErrTimeOut
		reply.WrongLeader = true
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
		OpType:		LeaveOp,
	}
	DPrintf("[Leave] Server [%v] get args [%v]", sc.rf.GetMe(), args)
	logIndex, _, isLeader := sc.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	opCh := sc.getOpCh(logIndex)
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- opCh:
		reply.Err = appliedOp.Error
		reply.WrongLeader = appliedOp.WrongLeader
	case <- timer.C:
		reply.Err = ErrTimeOut
		reply.WrongLeader = true
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
		OpType:		MoveOp,
	}
	DPrintf("[Move] Server [%v] get args [%v]", sc.rf.GetMe(), args)
	logIndex, _, isLeader := sc.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	opCh := sc.getOpCh(logIndex)
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- opCh:
		reply.Err = appliedOp.Error
		reply.WrongLeader = appliedOp.WrongLeader
	case <- timer.C:
		reply.Err = ErrTimeOut
		reply.WrongLeader = true
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
		OpType:		QueryOp,
	}
	DPrintf("[Query] Server [%v] get args [%v]", sc.rf.GetMe(), args)
	logIndex, _, isLeader := sc.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	opCh := sc.getOpCh(logIndex)
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- opCh:
		reply.Err = appliedOp.Error
		reply.WrongLeader = appliedOp.WrongLeader
		reply.Config = appliedOp.Cfg
	case <- timer.C:
		reply.Err = ErrTimeOut
		reply.WrongLeader = true
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

func (sc *ShardCtrler) isDuplicate(clientId int64, sequenceNum int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastResponse, exist := sc.session[clientId]
	if exist && lastResponse.SequenceNum >= sequenceNum {
		return true
	}
	return false
}

func (sc *ShardCtrler) getNewConfig() (Config, Config) {
	preCfg := sc.configs[len(sc.configs) - 1]
	cfg := Config {
		Num: preCfg.Num + 1,
		Groups: make(map[int][] string),
	}
	for i := range cfg.Shards {
		cfg.Shards[i] = preCfg.Shards[i]
	}
	return cfg, preCfg;
}

// 带锁进入的
func (sc *ShardCtrler) JoinHandler(Servers map[int][]string) {
	cfg, preCfg := sc.getNewConfig()
	DPrintf("[Before JoinHandler]  Server [%v] get latest cfg Num [%v] Shards [%v], Groups [%v]", sc.rf.GetMe(), preCfg.Num, preCfg.Shards, preCfg.Groups)
	// 首先先把旧配置的内容复制过来
	// map需要排序
	GIDs := make([]int, 0)
	for gid, server := range preCfg.Groups {
		newSrver := make([]string, len(server))
		copy(newSrver, server)
		cfg.Groups[gid] = newSrver
		GIDs = append(GIDs, gid)
	}
	for gid, server := range Servers {
		cfg.Groups[gid] = server
		GIDs = append(GIDs, gid)
	}
	// 当前的GID都进行了排序
	sort.Ints(GIDs)
	shardsSize := 10
	groupSize := len(cfg.Groups)
	shardsPerGroup := shardsSize / groupSize
	left := shardsSize % groupSize
	
	perGroupSize := make(map[int] int)	// 这个map用来映射一个group可以放多少个shard
	for _, gid := range GIDs {
		perGroupSize[gid] = shardsPerGroup
		if left > 0 {
			perGroupSize[gid]++
			left--
		}
	}
	for shard, gid := range cfg.Shards {
		if perGroupSize[gid] <= 0 {
			// 如果这个shard是超容量的，分配出去其他的组
			for _, GID := range GIDs {
				num := perGroupSize[GID]
				if num > 0 {
					cfg.Shards[shard] = GID
					perGroupSize[GID]--
					break
				}
			}
		} else {
			// 如果容量还有，那么就不需要去改变这个shard的分组了
			// 直接减少一个容量
			perGroupSize[gid]--
		}
	}
	sc.configs = append(sc.configs, cfg)
	last := sc.configs[len(sc.configs) - 1]
	DPrintf("[After JoinHandler] Server [%v] get latest cfg Num [%v] Shards [%v], Groups [%v]", sc.rf.GetMe(), last.Num, last.Shards, last.Groups)
}

// 带锁进入的
func (sc *ShardCtrler) LeaveHandler(GIDs []int) {
	cfg, preCfg := sc.getNewConfig()
	DPrintf("[Before LeaveHandler] Server [%v] get latest cfg Num [%v] Shards [%v], Groups [%v]", sc.rf.GetMe(), preCfg.Num, preCfg.Shards, preCfg.Groups)
	gidMap := make(map[int]bool)
	for _, gid := range GIDs {
		gidMap[gid] = true
	}
	// 保留未被删除的
	GIDs = make([]int, 0)
	for gid, server := range preCfg.Groups {
		if gidMap[gid] {
			continue
		}
		cfg.Groups[gid] = server
		GIDs = append(GIDs, gid)
	}
	sort.Ints(GIDs)
	if len(cfg.Groups) == 0 {
		for i := 0; i < 10; i++ {
			cfg.Shards[i] = 0
		}
		sc.configs = append(sc.configs, cfg)
		return
 	}
	shardsSize := 10
	groupSize := len(cfg.Groups)
	shardsPerGroup := shardsSize / groupSize
	left := shardsSize % groupSize
	perGroupSize := make(map[int] int)	// 这个map用来映射一个group可以放多少个shard
	for _, gid := range GIDs {
		perGroupSize[gid] = shardsPerGroup
		if left > 0 {
			perGroupSize[gid]++
			left--
		}
	}
	for shard, gid := range cfg.Shards {
		if gidMap[gid] || perGroupSize[gid] <= 0 {
			// 如果这个shard是超容量的, 或者被删除了的，分配出去其他的组
			for _, GID := range GIDs {
				num := perGroupSize[GID]
				if num > 0 {
					cfg.Shards[shard] = GID
					perGroupSize[GID]--
					break
				}
			}
		} else {
			// 如果容量还有，那么就不需要去改变这个shard的分组了
			// 直接减少一个容量
			perGroupSize[gid]--
		}
	}
	sc.configs = append(sc.configs, cfg)
	last := sc.configs[len(sc.configs) - 1]
	DPrintf("[After LeaveHandler] Server [%v] get latest cfg Num [%v] Shards [%v], Groups [%v]", sc.rf.GetMe(), last.Num, last.Shards, last.Groups)
}

// 带锁进入的
func (sc *ShardCtrler) MoveHandler(shard int, GID int) {
	// move之后不需要负载均衡
	cfg, preCfg := sc.getNewConfig()
	DPrintf("[Before MoveHandler] Server [%v] get latest cfg Num [%v] Shards [%v], Groups [%v]", sc.rf.GetMe(), preCfg.Num, preCfg.Shards, preCfg.Groups)
	for gid, server := range preCfg.Groups {
		newSrver := make([]string, len(server))
		copy(newSrver, server)
		cfg.Groups[gid] = newSrver
	}
	_, exist := cfg.Groups[GID]
	if exist {
		cfg.Shards[shard] = GID
	}
	sc.configs = append(sc.configs, cfg)
	last := sc.configs[len(sc.configs) - 1]
	DPrintf("[After MoveHandler] Server [%v] get latest cfg Num [%v] Shards [%v], Groups [%v]", sc.rf.GetMe(), last.Num, last.Shards, last.Groups)
}

// 带锁进入的
func (sc *ShardCtrler) QueryHandler(Num int) Config {
	preCfg := sc.configs[len(sc.configs) - 1]
	if Num == -1 || Num > preCfg.Num {
		DPrintf("[Afetr QueryHandler] Server [%v] get latest cfg Num [%v] Shards [%v], Groups [%v]", sc.rf.GetMe(), preCfg.Num, preCfg.Shards, preCfg.Groups)
		return preCfg
	}
	DPrintf("[Afetr QueryHandler] Server [%v] get latest cfg Num [%v] Shards [%v], Groups [%v]", sc.rf.GetMe(), sc.configs[Num].Num, sc.configs[Num].Shards, sc.configs[Num].Groups)
	return sc.configs[Num]
}

func (sc *ShardCtrler) applyMessage() {
	for {
		select {
		case msg := <- sc.applyCh:
			op := msg.Command.(Op)
			ret := OpResult {
				SequenceNum: op.SequenceNum,
				Error: ErrWrongLeader,
				WrongLeader: true,
			}
			if !sc.isDuplicate(op.ClientId, op.SequenceNum) {
				sc.mu.Lock()
				switch op.OpType {
				case JoinOp:
					sc.JoinHandler(op.Servers)
				case LeaveOp:
					sc.LeaveHandler(op.GIDs)
				case MoveOp:
					sc.MoveHandler(op.Shard, op.GID)
				case QueryOp:
					ret.Cfg = sc.QueryHandler(op.Num)
					DPrintf("[applyMesssage QueryOp] Server [%v] ret.Cfg [%v]", sc.rf.GetMe(), ret.Cfg)
				}
				ret.Error = OK
				ret.WrongLeader = false
				sc.session[op.ClientId] = ret
				sc.mu.Unlock()
			}
			term, isLeader := sc.rf.GetState()
			if isLeader && term == msg.CommandTerm {
				ch := sc.getOpCh(msg.CommandIndex)
				ch <- ret
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
	sc.session = make(map[int64]OpResult)
	sc.opCh = make(map[int]chan OpResult)
	go sc.applyMessage()
	return sc
}
