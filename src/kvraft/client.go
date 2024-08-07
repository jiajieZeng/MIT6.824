package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId 	int64				// To implement this, each client is given a unique identifier
	sequenceNum 	int				// and clients assign unique serial numbers to every command. 
	leaderId	int				// leader in the cluster
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
	// You'll have to add code here.
	// 论文应该是发送一个registerRPC来初始化clientId
	ck.leaderId = 0	// 这个可以随机，那就初始化为0算了
	ck.clientId = nrand()
	ck.sequenceNum = 0		// 自增ID
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs {
		Key: 			key,
		SequenceNum: 	ck.sequenceNum,
		ClientId:		ck.clientId,
	}
	ck.sequenceNum++
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.ServerGet", &args, &reply)
		if ok {
			if reply.Err == OK {
				// DPrintf("[Client Get] Client [%v] send Get to leader [%v] OK value [%v]", ck.clientId, ck.leaderId, reply.Value)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				// DPrintf("[Client Get] Client [%v] send Get to leader [%v] ErrNoKey", ck.clientId, ck.leaderId)
				return ""
			} else {
				// DPrintf("[Client Get] Client [%v] send Get to leader [%v] Err [%v]", ck.clientId, ck.leaderId, reply.Err)
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			}
		} else {
			newLeaderId := (ck.leaderId + 1) % len(ck.servers)
			// DPrintf("[Client Get] Client [%v] failed to send Get to leader [%v] ok = false switch to new leader [%v]", ck.clientId, ck.leaderId, newLeaderId)
			ck.leaderId = newLeaderId
			time.Sleep(50 * time.Millisecond)
		}
	}
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// 感觉put和get的代码是一样的，不过为了打印一下日志，还是分开写
	// You will have to modify this function.
	args := PutAppendArgs {
		Key: 			key,
		Value: 			value,
		Op:				op,
		ClientId:		ck.clientId,
		SequenceNum:	ck.sequenceNum,
	}
	ck.sequenceNum++
	// DPrintf("[Client PutAppend] Client [%v] gen op id [%v] Key [%v] Value [%v] Op [%v]", 
			// ck.clientId, args.SequenceNum, args.Key, args.Value, args.Op)
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.ServerPutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				// DPrintf("[Client PutAppend] Client send PutAppend OK [%v]", args)
				return
			} else {
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			}
		} else {
			newLeaderId := (ck.leaderId + 1) % len(ck.servers)
			// DPrintf("[Client PutAppend] Client [%v] failed to send PutAppend to leader [%v] ok = false switch to new leader [%v]", ck.clientId, ck.leaderId, newLeaderId)
			ck.leaderId = newLeaderId
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PutOp")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "AppendOp")
}
