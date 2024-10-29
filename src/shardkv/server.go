package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"
import "time"
import "bytes"
import "sync/atomic"


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key 		string	
	Value 		string
	OpType		string
	SequenceNum	int
	ClientId	int64
	Config		shardctrler.Config
	ShardId     int
	Shard 		Shard
	Session		map[int64]OpResult
}

type OpResult struct {
	SequenceNum int
	Value 		string
	Err       Err
}

type Shard struct {
	keyValueService map[string]string
	Num		int  // config的Num
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead		 int32

	// Your definitions here.
	lastApplied int
	session     map[int64]OpResult
	opCh        map[int]chan OpResult
	config      shardctrler.Config
	preConfig   shardctrler.Config
	keyValueShard      []Shard  // 这里的状态机是分片状态机
	mck      *shardctrler.Clerk // shardctrl的client

}

// 带锁进入
func (kv *ShardKV) getOpCh(index int) chan OpResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ret, exist := kv.opCh[index]
	if !exist {
		kv.opCh[index] = make(chan OpResult, 1)
		ret = kv.opCh[index]
	}
	return ret
}

func (kv *ShardKV) Start(op Op) (Err, string) {
	kv.mu.Lock();
	logIndex, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		kv.mu.Unlock()
		return ErrWrongLeader, ""
	}
	var retErr Err
	var retValue string
	kv.mu.Unlock()
	ch := kv.getOpCh(logIndex)
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- ch:
		kv.mu.Lock()
		retErr = appliedOp.Err
		retValue = appliedOp.Value
	case <- timer.C:
		retErr = ErrTimeOut
		retValue = ""
	}
	timer.Stop()
	go func() {
		kv.mu.Lock()
		if (len(kv.opCh[logIndex]) == 0) {
			delete(kv.opCh, logIndex)
		}
		kv.mu.Unlock()
	}()
	return retErr, retValue
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {	// 不同组的
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if kv.keyValueShard[shard].keyValueService == nil {
		reply.Err = ErrShardNotArrived // 求错了分组
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op {
		Key: 	args.Key,
		OpType: args.Op,
		SequenceNum: args.SequenceNum,
		ClientId: args.ClientId,
	}
	reply.Err, reply.Value = kv.Start(op)
	// 还要再检查一遍吗
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shard] != kv.gid {	// 不同组的
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	} else if kv.keyValueShard[shard].keyValueService == nil {
		reply.Err = ErrShardNotArrived // 求错了分组
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op {
		Key: args.Key,
		Value: args.Value,
		OpType: args.Op,
		SequenceNum: args.SequenceNum,
		ClientId:	args.ClientId,
	}
	reply.Err, _ = kv.Start(op)
}

func (kv *ShardKV) isDuplicate(sequenceNum int, clientId int64) (bool, OpResult) {
	lastResponse, exist := kv.session[clientId]
	if exist && lastResponse.SequenceNum >= sequenceNum {
		return true, lastResponse
	}
	return false, lastResponse
}

func (kv *ShardKV) readPersistSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	kvErr := d.Decode(&kv.keyValueShard)
	sessionErr := d.Decode(&kv.session)
	cfgErr := d.Decode(&kv.config)
	preCfgErr := d.Decode(&kv.preConfig)
	if kvErr != nil || sessionErr != nil || cfgErr != nil || preCfgErr != nil {
		panic("applyMessage apply snapshot error");
	}
}

func (kv *ShardKV) applyMessage() {
	for !kv.killed() {
		select {
		case msg := <- kv.applyCh:
			if msg.SnapshotValid {
				kv.mu.Lock()
				term, index, snapshot := msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot
				if snapshot == nil || len(snapshot) == 0 || kv.lastApplied > index {
					kv.mu.Unlock()
					continue
				}
				if kv.rf.CondInstallSnapshot(term, index, snapshot) {
					r := bytes.NewBuffer(snapshot)
					d := labgob.NewDecoder(r)
					kv.lastApplied = index
					kvErr := d.Decode(&kv.keyValueShard)
					sessionErr := d.Decode(&kv.session)
					cfgErr := d.Decode(&kv.config)
					preCfgErr := d.Decode(&kv.preConfig)
					if kvErr != nil || sessionErr != nil || cfgErr != nil || preCfgErr != nil {
						panic("applyMessage apply snapshot error");
					}
				}
				kv.mu.Unlock()
			} else {
				kv.mu.Lock()
				logIndex := msg.CommandIndex
				if msg.Command == nil || logIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				op := msg.Command.(Op)
				ret := OpResult {
					SequenceNum: op.SequenceNum,
					Err:	OK,
				}
				if op.OpType == GetOp || op.OpType == PutOp || op.OpType == AppendOp {
					shard := key2shard(op.Key)
					if kv.config.Shards[shard] != kv.gid {
						ret.Err = ErrWrongGroup
					} else if kv.keyValueShard[shard].keyValueService == nil {
						ret.Err = ErrShardNotArrived
					} else {
						duplicate, retsult := kv.isDuplicate(op.SequenceNum, op.ClientId)
						if duplicate {
							ret = retsult
						} else {
							if op.OpType == GetOp {
								ret.Value = kv.keyValueShard[shard].keyValueService[op.Key]
							} else if op.OpType == PutOp {
								kv.keyValueShard[shard].keyValueService[op.Key] = op.Value
							} else {
								kv.keyValueShard[shard].keyValueService[op.Key] += op.Value
							}
						}
					}
				} else {
					if op.OpType == UpdateConfigOp {
						// 更新Config操作
						kv.UpdateConfig(op)
					} else if op.OpType == AddShardOp {
						// 新的配置还没到，不能把这个shard加进来
						if kv.config.Num < op.SequenceNum {
							ret.Err = ErrConfigNotArrived
						} else {
							kv.AddShard(op)
						}
					} else {
						if op.SequenceNum >= kv.config.Num {
							kv.keyValueShard[op.ShardId].keyValueService = nil
							kv.keyValueShard[op.ShardId].Num = op.SequenceNum
						}
					}
				}
				kv.session[op.ClientId] = ret
				if kv.requireSnapshot() {
					kv.Snapshot(msg.CommandIndex)
				}
				kv.mu.Unlock()
				ch := kv.getOpCh(logIndex)
				ch <- ret
			}
		}
	}
}

func (kv *ShardKV) Snapshot(index int) {
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.keyValueShard)
	e.Encode(kv.session)
	e.Encode(kv.config)
	e.Encode(kv.config)
	e.Encode(kv.preConfig)
	snapshot := w.Bytes()
	go kv.rf.Snapshot(index, snapshot)
}

// 带锁进入
func (kv *ShardKV) requireSnapshot() bool {
	if kv.maxraftstate == -1 {
		// if maxraftstate is -1, you do not have to snapshot. 
		return false
	}
	size := kv.rf.GetRaftStateSize()
	if size + 100 >= kv.maxraftstate {
		return true
	}
	return false
}

func (kv *ShardKV) AddShard(op Op) {
	if kv.keyValueShard[op.ShardId].keyValueService != nil || op.Shard.Num < kv.config.Num {
		// 这是一个过期的操作
		return
	}
	kv.keyValueShard[op.ShardId] = kv.migrateShard(op.Shard.Num, op.Shard.keyValueService)
	// 把session也复制过来
	for clientId, result := range op.Session {
		ret, exist := kv.session[clientId]
		if !exist || ret.SequenceNum < result.SequenceNum {
			kv.session[clientId] = result
		}
	}
}

func (kv *ShardKV) UpdateConfig(op Op) {
	cfg := kv.config
	newCfg := op.Config
	if cfg.Num < newCfg.Num {
		for shard, gid := range newCfg.Shards {
			// 建立这个分片的kv map
			if gid == kv.gid && cfg.Shards[shard] == 0 {
				kv.keyValueShard[shard].keyValueService = make(map[string] string)
				kv.keyValueShard[shard].Num = newCfg.Num
			}
		}
		kv.preConfig = cfg
		kv.config = newCfg
	}
}

// 要持续获得config
func (kv *ShardKV) configDetetion() {
	kv.mu.Lock()
	cfg := kv.config
	rf := kv.rf
	kv.mu.Unlock()
	for kv.killed() == false {
		_, isLeader := rf.GetState()
		if isLeader == false {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		if kv.allSend() == false {
			session := make(map[int64]OpResult)
			for key, value := range kv.session {
				session[key] = value
			}
			for shard, gid := range kv.preConfig.Shards {
				// 把新配置中不属于自己的shard发送出去
				if gid == kv.gid && kv.config.Shards[shard] != kv.gid && kv.keyValueShard[shard].Num < kv.config.Num {
					newShard := kv.migrateShard(kv.config.Num, kv.keyValueShard[shard].keyValueService)
					args := SendShardArgs {
						Session: session,
						ShardId: shard,
						Shard:   newShard,
						GroupId: gid,
						ConfigNum: kv.config.Num, 
					}
					serverList := kv.config.Groups[kv.config.Shards[shard]]
					servers := make([]*labrpc.ClientEnd, len(serverList))
					for i, name := range serverList {
						servers[i] = kv.make_end(name)
					}
					go func(servers []*labrpc.ClientEnd, args *SendShardArgs) {
						index := 0
						timeout := time.Now()
						for {
							var reply SendShardReply
							ok := servers[index].Call("ShardKV.SendShard", &args, &reply)
							if ok && reply.Err == OK || time.Now().Sub(timeout) >= time.Second {
								// 丢弃掉这个切片
								kv.mu.Lock()
								op := Op {
									OpType: DeleteShardOp,
									SequenceNum: kv.config.Num,
									ClientId:	int64(kv.gid),
									ShardId:	args.ShardId,
								}				
								kv.mu.Unlock()
								kv.Start(op)
								break			
							}
							index = (index + 1) % len(servers)
							if index == 0 {
								time.Sleep(100 * time.Millisecond)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if kv.allReceived() == false {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		cfg = kv.config
		client := kv.mck
		kv.mu.Unlock()
		newCfg := client.Query(cfg.Num + 1)
		DPrintf("[configDetection] Query new Config")
		if newCfg.Num != cfg.Num + 1 { 
			time.Sleep(100 * time.Millisecond)
			continue
		}
		op := Op {
			OpType: UpdateConfigOp,
			ClientId: int64(kv.gid),
			SequenceNum: newCfg.Num,
			Config:	newCfg,
		}
		kv.Start(op)
	}
}

func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply) {
	op := Op {
		OpType: AddShardOp,
		ClientId: int64(args.GroupId),
		SequenceNum: args.ConfigNum,
		ShardId:	args.ShardId,
		Shard:		args.Shard,
		Session:	args.Session,
	}
	reply.Err, _ = kv.Start(op)
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.preConfig.Shards {
		// 判断是否都接收到了分片
		if gid != kv.gid && kv.config.Shards[shard] == kv.gid && kv.keyValueShard[shard].Num < kv.config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) migrateShard(num int, keyValueService map[string]string) Shard {
	newShard := Shard {
		keyValueService: make(map[string]string),
		Num: num,
	}
	// copy
	for key, value := range keyValueService {
		newShard.keyValueService[key] = value
	}
	return newShard
}

func (kv *ShardKV) allSend() bool {
	for shard, gid := range kv.preConfig.Shards {
		// 当前kv所在的组分片不匹配，或者说，kv里面的配置版本比当前的配置版本小，还没有send
		if gid == kv.gid && kv.config.Shards[shard] != kv.gid && kv.keyValueShard[shard].Num < kv.config.Num {
			return false
		}
	}
	return true
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.dead = 0

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.readPersistSnapshot(persister.ReadSnapshot())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApplied = 0
	kv.session = make(map[int64]OpResult)
	kv.opCh = make(map[int]chan OpResult)
	kv.mu = sync.Mutex{}
	kv.keyValueShard = make([]Shard, shardctrler.NShards)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	go kv.applyMessage()
	go kv.configDetetion()
	return kv
}
