package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 这个lab居然还有performance的要求


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key 		string	
	Value 		string
	OpType		string
	SequenceNum	int
	ClientId	int64
}

type OpResult struct {
	SequenceNum		int 	
	Value 			string
	Err			Err
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied 	int
	keyvValueService	map[string]string	// replicated state machine
	/*
		Each server’s state machine maintains a session for each client.
		The session tracks the latest serial number processed for the client, along with the associated re
		sponse. If a server receives a command whose serial number has already been executed, it responds
		immediately without re-executing the request.
	*/
	session		map[int64]OpResult
	opCh		map[int]chan OpResult
}



// 带锁进入
func (kv *KVServer) requireSnapshot() bool {
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

// 带锁进入
func (kv *KVServer) snapshot(index int) {
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.keyvValueService)
	e.Encode(kv.session)
	snapshot := w.Bytes()
	kv.mu.Unlock()
	go kv.rf.Snapshot(index, snapshot)
}

// 带锁进入
func (kv *KVServer) getOpCh(index int) chan OpResult {
	ret, exist := kv.opCh[index]
	if !exist {
		kv.opCh[index] = make(chan OpResult, 1)
		ret = kv.opCh[index]
	}
	return ret
}

func (kv *KVServer) ServerGet(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 操作get
	if kv.killed() {
		// DPrintf("[Server Get] server killed args [%v]", args)
		reply.Err = ErrWrongLeader
		return
	}
	// 复制到log里面去
	op := Op {
		Key: args.Key,
		OpType: GetOp,
		SequenceNum: args.SequenceNum,
		ClientId:	args.ClientId,
	}
	logIndex, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		// DPrintf("[Server Get] server not leader [%v]", args)
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	opCh := kv.getOpCh(logIndex)
	kv.mu.Unlock()
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- opCh:
		reply.Value = appliedOp.Value
		reply.Err = appliedOp.Err
		// DPrintf("[Server Get] server get reply [%v]", reply)
	case <- timer.C:
		// DPrintf("[Server Get] server timeout")
		reply.Err = ErrTimeOut
	}
	timer.Stop()
	go func() {
		kv.mu.Lock()
		if len(kv.opCh[logIndex]) == 0 {
			delete(kv.opCh, logIndex)
		}
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDuplicate(sequenceNum int, clientId int64) (bool, OpResult) {
	lastResponse, exist := kv.session[clientId]
	if exist && lastResponse.SequenceNum >= sequenceNum {
		return true, lastResponse
	}
	return false, lastResponse
}

func (kv *KVServer) ServerPutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 操作put
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	ok, lastResponse := kv.isDuplicate(args.SequenceNum, args.ClientId)
	kv.mu.RUnlock()
	if ok {
		reply.Err = lastResponse.Err
		return
	}
	op := Op {
		Key: args.Key,
		Value: args.Value,
		OpType: args.Op,
		SequenceNum: args.SequenceNum,
		ClientId:	args.ClientId,
	}
	logIndex, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	opCh := kv.getOpCh(logIndex)
	kv.mu.Unlock()
	timer := time.NewTicker(100 * time.Millisecond)
	select {
	case appliedOp := <- opCh:
		reply.Err = appliedOp.Err
	case <- timer.C:
		reply.Err = ErrTimeOut
	}
	timer.Stop()
	go func() {
		kv.mu.Lock()
		if len(kv.opCh[logIndex]) == 0 {
			delete(kv.opCh, logIndex)
		}
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) applyMessage() {
	for !kv.killed() {
		select {
		case msg := <- kv.applyCh:
			if msg.SnapshotValid {
				// 这里直接抄lab2的test部分
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
					// DPrintf("[Server applyMessage] [%v]", kv.keyvValueService)
					if d.Decode(&kv.keyvValueService) != nil || d.Decode(&kv.session) != nil {
						panic("applyMessage apply snapshot error");
					}
					// DPrintf("[Server applyMessage] apply snapshot term [%v], index [%v]", term, index)
					// DPrintf("[Server applyMessage] [%v]", kv.keyvValueService)
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
				var ret OpResult
				if op.OpType == GetOp {
					value, exist := kv.keyvValueService[op.Key]
					if exist {
						ret = OpResult {
							SequenceNum: op.SequenceNum,
							Value:	value,
							Err:	OK,
						}
					} else {
						ret = OpResult {
							SequenceNum: op.SequenceNum,
							Value:	"",
							Err:	ErrNoKey,
						}
					}
					term, isLeader := kv.rf.GetState()
					if kv.requireSnapshot() {
						go kv.snapshot(msg.CommandIndex)
					}
					if isLeader && msg.CommandTerm == term {
						ch := kv.getOpCh(logIndex) 
						kv.mu.Unlock()
						ch <- ret	
					} else {
						kv.mu.Unlock()
					}
				} else {
					ok, _ := kv.isDuplicate(op.SequenceNum, op.ClientId)
					if !ok {
						if op.OpType == PutOp {
							kv.keyvValueService[op.Key] = op.Value
						} else if op.OpType == AppendOp {
							kv.keyvValueService[op.Key] += op.Value
						}
						// DPrintf("[Server apply message] apply op [%v] now kv [%v]",  op, kv.keyvValueService[op.Key])
						kv.session[op.ClientId] = OpResult {
							SequenceNum:	op.SequenceNum, 
							Value:			kv.keyvValueService[op.Key], 
							Err: 			OK,
						}
						term, isLeader := kv.rf.GetState()
						if kv.requireSnapshot() {
							go kv.snapshot(msg.CommandIndex)
						}
						if isLeader && msg.CommandTerm == term {
							ch := kv.getOpCh(logIndex) 
							kv.mu.Unlock()
							ch <- kv.session[op.ClientId]	
						} else {
							kv.mu.Unlock()
						}
					} else {
						kv.mu.Unlock()
					}
				}
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readPersistSnapshot() {
	data, index := kv.rf.GetPersistSnapshot()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	kv.lastApplied = index
	if d.Decode(&kv.keyvValueService) != nil || d.Decode(&kv.session) != nil {
		panic("Error: readPersistSnapshot")
	}
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.lastApplied = 0
	kv.keyvValueService = make(map[string]string)
	kv.session = make(map[int64]OpResult)
	kv.opCh = make(map[int]chan OpResult)
	kv.readPersistSnapshot()
	go kv.applyMessage()
	return kv
}
