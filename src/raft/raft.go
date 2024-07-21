package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// 本实现的主旨是一把大锁保平安
// 讲什么high performance,fine-grained lock都是假的
// 直接coarse-grained lock，刚进入方法的时候，只要方法内部有操作共享资源，先锁起来再说
// 把整个raft跑一万遍没问题再改

import (
	//	"bytes"
	// "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	state        State
	heartBeat    time.Time
	electionTime time.Time
	// Persistente state on all servers
	currentTerm int // latest term server has seen(initialized to 0 on first boot, increases monotonically)
	voteFor     int // candidateId that received vote in current term (or null if none)
	// log entries; each entry contains command for state machine, and term when entry was
	// received by leader(first index is 1)
	log []LogEntry

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	// Reinitialized after election
	// for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server
	// (initialized to 0,in creases monotinically)
	matchIndex []int
	// 2B
	sendLogEntriesSignal []*sync.Cond // 条件变量，发送非空AppendEntrisRPC

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!AppendEntries
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int      // candidate's term
	CandidateId  int      // candidate requesting vote
	LastLogIndex int      // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entery
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // ture means candidate received vote
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clinets
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of prevLogIndex entry
	Entries      []LogEntry // log entries to stroe(empty ofr heartbeat;may send more than on for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm int // 冲突Term
	ConflictIndex int // 冲突index
}

// up-to-date返回1，否则 0
func (rf *Raft) upToDate(lastLogTerm int, lastLogIndex int) bool {
	if lastLogTerm > rf.getLastLogEntry().Term {
		return true
	} else if lastLogTerm == rf.getLastLogEntry().Term {
		// 2A没有推送日志，所以先直接返回真
		// 这里要比较currentTerm下谁的日志currentTerm长度更长
		return lastLogIndex >= rf.getLastLogEntry().Index
	} else {
		return false
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).C
	rf.mu.Lock()
<<<<<<< HEAD
	DPrintf("server [%v] currentTerm[%v] state[%v] voteFor[%v], get RequestVote from server[%v] args.Term[%v]", rf.me, rf.currentTerm, stateArray[rf.state], rf.voteFor, args.CandidateId, args.Term)
	// 不投票
	if rf.currentTerm > args.Term || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		DPrintf("server [%v] currentTerm[%v] state[%v] voteFor[%v] did not vote for server[%v]", rf.me, rf.currentTerm, stateArray[rf.state], rf.voteFor, args.CandidateId)
=======
	defer rf.mu.Unlock()
	DPrintf("[Before RequestVote] Server [%v] currentTerm[%v] state[%v] voteFor[%v], get RequestVote from server[%v] args.Term[%v]", rf.me, rf.currentTerm, stateArray[rf.state], rf.voteFor, args.CandidateId, args.Term)
	// 不投票
	if rf.currentTerm > args.Term || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		DPrintf("[After1 RequestVote] Server [%v] currentTerm[%v] state[%v] voteFor[%v] did not vote for server[%v]", rf.me, rf.currentTerm, stateArray[rf.state], rf.voteFor, args.CandidateId)
>>>>>>> lab2A
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	// 如果一个leader或者candidate发现了自己的任期的过时的，它会马上把状态转换为follower
	if rf.currentTerm < args.Term {
		rf.stateTrans(Follower)
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.electionTime = time.Now()
	}
	if !rf.upToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
<<<<<<< HEAD
		DPrintf("server [%v] currentTerm[%v] voteFor[%v] did not vote for server[%v]", rf.me, rf.currentTerm, rf.voteFor, args.CandidateId)
		rf.mu.Unlock()
=======
		DPrintf("[After2 RequestVote] Server [%v] currentTerm[%v] voteFor[%v] did not vote for server[%v]", rf.me, rf.currentTerm, rf.voteFor, args.CandidateId)
>>>>>>> lab2A
		return
	}
	rf.electionTime = time.Now()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.voteFor = args.CandidateId
	rf.stateTrans(Follower)
<<<<<<< HEAD
	DPrintf("server [%v] currentTerm[%v] state[%v], vote for server[%v]", rf.me, rf.currentTerm, stateArray[rf.state], args.CandidateId)
	rf.mu.Unlock()
=======
	DPrintf("[After3 RequestVote] Server [%v] currentTerm[%v] state[%v], vote for server[%v] reply[%v]", 
			rf.me, rf.currentTerm, stateArray[rf.state], args.CandidateId, reply)
>>>>>>> lab2A
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 发送AppendEntris RPC
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
<<<<<<< HEAD
	rf.electionTime = time.Now()
	DPrintf("[before] server [%v] in state [%v] currentTerm[%v] voteFor[%v] get AppendEntries from server [%v]\n", rf.me, stateArray[rf.state], rf.currentTerm, rf.voteFor, args.LeaderId)
=======
	defer rf.mu.Unlock()
	DPrintf("[before] Server [%v] in state [%v] currentTerm [%v] voteFor [%v] get AppendEntries from Server [%v]\n", 
			rf.me, stateArray[rf.state], rf.currentTerm, rf.voteFor, args.LeaderId)
	// DPrintf("[before] args.PrevLogIndex [%v] args.PrevLogTerm [%v]", args.PrevLogIndex, args.PrevLogTerm)
>>>>>>> lab2A
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
<<<<<<< HEAD
		rf.mu.Unlock()
=======
		DPrintf("[after] Server [%v] in state [%v] currentTerm [%v] > args.Term[%v]\n", 
					rf.me, stateArray[rf.state], rf.currentTerm, args.Term)
>>>>>>> lab2A
		return
	}
	// 等待投票时candidate可能来自收到新leader的信息
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.voteFor = -1
	}
<<<<<<< HEAD
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.stateTrans(Follower)
	// reply false if log doesn't contain an entry at preveLogIndex whose term matches prevLogTerm
	// if args.PrevLogIndex < rf.log[0].Index {
	// 	reply.Success = false
	// 	reply.Term = 0
	// 	return
	// }
	DPrintf("[after] server [%v] in state [%v] currentTerm[%v] voteFor[%v] get AppendEntries from server [%v]\n", rf.me, stateArray[rf.state], rf.currentTerm, rf.voteFor, args.LeaderId)
	rf.mu.Unlock()
=======
	rf.electionTime = time.Now()
	rf.stateTrans(Follower)
	if args.PrevLogIndex < rf.getFirstLogEntry().Index {
		reply.Success = false
		reply.Term = 0
		DPrintf("[after] Server [%v] in state [%v] args.PrevLogIndex [%v] < rf.getFirstLogEntry().Index [%v]\n", 
					rf.me, stateArray[rf.state], args.PrevLogIndex, rf.getFirstLogEntry().Index)
		return
	}
	// reply false if log doesn't contain an entry at preveLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex < rf.getFirstLogEntry().Index || args.PrevLogIndex > rf.getLastLogEntry().Index || rf.getLogEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		DPrintf("[after]Server [%v] currentTerm [%v] args.PrevLogIndex is [%v], out of index...", rf.me, rf.currentTerm, args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.getLastLogEntry().Index
		reply.ConflictIndex = rf.getLastLogEntry().Term
	} else if rf.getLogEntry(args.PrevLogIndex).Term == args.PrevLogTerm {	// 有这个PrevLog
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + 1 + i
			if index > rf.getLastLogEntry().Index {
				DPrintf("[inprogress] Server [%v] logs before append length [%v]\n", rf.me, len(rf.log))
				rf.log = append(rf.log, args.Entries[i:]...)
				DPrintf("[inprogress] Server [%v] logs after append length [%v]\n", rf.me, len(rf.log))
				break
			} else if rf.getLogEntry(index).Term != entry.Term {
				// 从这个位置开始覆盖
				idx := 0
				for rf.log[idx].Index != index {
					idx++
				}
				DPrintf("[inprogress] Server [%v] logs before append length [%v]\n", rf.me, len(rf.log))
				// DPrintf("[inprogress] Server [%v] entry.Index[%v] firstIndex [%v]", rf.me, entry.Index, firstIndex)
				log := append(rf.log[:idx], args.Entries[i:]...)
				// DPrintf("[inprogress] Server [%v] logs after append length [%v]\n", rf.me, len(log))
				newLog := make([]LogEntry, len(log))
				copy(newLog, log)
				rf.log = newLog
				DPrintf("[inprogress] Server [%v] logs after copy length [%v]\n", rf.me, len(rf.log))
				break
			}
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		reply.ConflictIndex = rf.getLastLogEntry().Index
		reply.ConflictTerm = rf.getLastLogEntry().Term
		rf.electionTime = time.Now()
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.getLastLogEntry().Index {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.getLastLogEntry().Index
			}
			rf.applyCond.Broadcast() // 唤醒aply的条件变量
		}
		DPrintf("[after] Server [%v] in state [%v] currentTerm [%v] voteFor [%v] get AppendEntries from Server [%v] success\n", rf.me, stateArray[rf.state], rf.currentTerm, rf.voteFor, args.LeaderId)
	} else {
		index := args.PrevLogIndex
		for index >= rf.getFirstLogEntry().Index && rf.getLogEntry(index).Term == rf.getLogEntry(args.PrevLogIndex).Term {
			index--
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		if index >= rf.getFirstLogEntry().Index {
			reply.ConflictIndex = index
			reply.ConflictTerm = rf.getLogEntry(index).Term
		}
	}
		// } else if !rf.matchPrevLogIndex(args.PrevLogIndex, args.PrevLogTerm) {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// 	lastIndex := rf.getLastLogEntry().Index
	// 	if lastIndex < args.PrevLogIndex {
	// 		// 最后一个Index都比前一个小，代表着这个follower缺了很多
	// 		reply.ConflictTerm = -1
	// 		reply.ConflictIndex = lastIndex + 1	// 回退到这个位置进行重传
	// 	} else {
	// 		// paper里面说这个机制似乎不是太重要，但是起码可以减少RPC的数量
	// 		// If desired, the protocol can be optimized to reduce the
	// 		// number of rejected AppendEntries RPCs. For example,
	// 		// when rejecting an AppendEntries request, the follower
	// 		// can include the term of the conflicting entry and the first
	// 		// index it stores for that term. 
	// 		firstIndex := rf.getFirstLogEntry().Index
	// 		reply.ConflictTerm = rf.log[args.PrevLogIndex - firstIndex].Term
	// 		index := args.PrevLogIndex - 1
	// 		for index >= firstIndex && rf.log[index - firstIndex].Term == reply.ConflictTerm {
	// 			index--
	// 		}
	// 		reply.ConflictIndex = index
	// 	}
	// }
	
	// if !rf.matchPrevLogIndex(args.PrevLogIndex, args.PrevLogTerm) {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// 	lastIndex := rf.getLastLogEntry().Index
	// 	if lastIndex < args.PrevLogIndex {
	// 		// 最后一个Index都比前一个小，代表着这个follower缺了很多
	// 		reply.ConflictTerm = -1
	// 		reply.ConflictIndex = lastIndex + 1	// 回退到这个位置进行重传
	// 	} else {
	// 		// paper里面说这个机制似乎不是太重要，但是起码可以减少RPC的数量
	// 		// If desired, the protocol can be optimized to reduce the
 	// 		// number of rejected AppendEntries RPCs. For example,
 	// 		// when rejecting an AppendEntries request, the follower
	// 		// can include the term of the conflicting entry and the first
 	// 		// index it stores for that term. 
	// 		firstIndex := rf.getFirstLogEntry().Index
	// 		reply.ConflictTerm = rf.log[args.PrevLogIndex - firstIndex].Term
	// 		index := args.PrevLogIndex - 1
	// 		for index >= firstIndex && rf.log[index - firstIndex].Term == reply.ConflictTerm {
	// 			index--
	// 		}
	// 		reply.ConflictIndex = index
			
	// 	}
	// 	DPrintf("[after] Server [%v] in state [%v] doesn't contain an entry at preveLogIndex [%v] whose term matches prevLogTerm [%v]\n", 
	// 				rf.me, stateArray[rf.state], args.PrevLogIndex, args.PrevLogTerm)
	// 	return
	// }
	// 覆盖形式的写appendEntries
	// firstIndex := rf.getFirstLogEntry().Index
	// for index, entry := range args.Entries {
	// 	if entry.Index - firstIndex >= len(rf.log) || rf.log[entry.Index - firstIndex].Term != entry.Term {
	// 		// DPrintf("[inprogress] Server [%v] logs before append length [%v]\n", rf.me, len(rf.log))
	// 		DPrintf("[inprogress] Server [%v] entry.Index[%v] firstIndex [%v]", rf.me, entry.Index, firstIndex)
	// 		log := append(rf.log[:entry.Index - firstIndex], args.Entries[index:]...)
	// 		// DPrintf("[inprogress] Server [%v] logs after append length [%v]\n", rf.me, len(log))
	// 		newLog := make([]LogEntry, len(log))
	// 		copy(newLog, log)
	// 		rf.log = newLog
	// 		// DPrintf("[inprogress] Server [%v] logs after copy length [%v]\n", rf.me, len(rf.log))
	// 		break
	// 	}
	// 	DPrintf("[inprogress] Server [%v] in state [%v] logs length [%v]\n", rf.me, stateArray[rf.state], len(rf.log))
	// }
	
}

func (rf *Raft) matchPrevLogIndex(prevLogIndex int, prevLogTerm int) bool {
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Term == prevLogTerm && rf.log[i].Index == prevLogIndex {
			return true
		}
	}
	return false
}

// 带锁进入
// the first index it stores for that term.
func (rf *Raft) getFirstLogEntry() LogEntry {
	return rf.log[0]
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Index == index {
			return rf.log[i]
		}
	}
	return LogEntry{}
}

func (rf *Raft) AppendLogEntries(command interface{}) LogEntry {
	index := rf.log[len(rf.log) - 1].Index + 1;
	newEntry := LogEntry {
		Index: index,
		Term: rf.currentTerm,
		Command: command,
	} 
	rf.log = append(rf.log, newEntry)
	return newEntry
>>>>>>> lab2A
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.state != Leader) {	// 不是leader直接返回
		return index, term, false
	}
	// 在这里接收到了一个command参数，command要通过AppendEntriesRPC进行发送
	// 把这个command放进log里面
	newEntry := rf.AppendLogEntries(command)
	DPrintf("Server %v receives a new command to replicate in term %v now log's length %v", rf.me, rf.currentTerm, len(rf.log))
	index = newEntry.Index
	term = rf.currentTerm
	go rf.broadcast(false)
	// 制作一个AppendEntrisRPC
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) stateTrans(newState State) {
	rf.state = newState
<<<<<<< HEAD
}

// 生成RequestVoteArgs，必须带着锁进入
func (rf *Raft) genRequestVoteArgs() RequestVoteArgs {
	logLen := len(rf.log)
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = logLen - 1
	args.LastLogTerm = rf.log[logLen-1]
	return args
}

=======
}

// 生成RequestVoteArgs，必须带着锁进入
func (rf *Raft) genRequestVoteArgs() RequestVoteArgs {
	logLen := len(rf.log)
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log[logLen-1].Index
	args.LastLogTerm = rf.log[logLen-1].Term
	return args
}

>>>>>>> lab2A
// 开始一次选举
// 根据经验，我们发现最简单的做法是首先记录回复中的任期（它可能比你当前的任期更高），
// 然后将当前任期与你在原始 RPC 中发送的任期进行比较。如果两者不同，则放弃回复并返回。
// 只有当两个术语相同时，才应继续处理回复。
func (rf *Raft) startElection() {
<<<<<<< HEAD
=======
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	}
	rf.electionTime = time.Now()
>>>>>>> lab2A
	rf.stateTrans(Candidate) // 状态转换
	rf.currentTerm += 1      // 提升任期
	rf.voteFor = rf.me       // 投票给自己
	grantedVoteNum := 1      // 投票计数
<<<<<<< HEAD
	DPrintf("server [%v] kick off election time out, now in state [%s], currentTerm [%v]\n", rf.me, stateArray[rf.state], rf.currentTerm)
=======
	DPrintf("Server [%v] kick off election time out, now in state [%s], currentTerm [%v]\n", rf.me, stateArray[rf.state], rf.currentTerm)
>>>>>>> lab2A
	args := rf.genRequestVoteArgs()
	for peer := range rf.peers { // 向所有的peer发送RequestVoteRPC
		if peer == rf.me {
			continue
		}
		// DPrintf("Server [%v] make goroutine for Server [%v]\n", rf.me, peer)
		go func(peer int) {
			reply := RequestVoteReply{}
<<<<<<< HEAD
			if !rf.sendRequestVote(peer, &args, &reply) {
				return
			}
			rf.mu.Lock()
			DPrintf("serve [%v] currentTerm[%v] state[%v] get reply [%v] from %v\n", rf.me, rf.currentTerm, stateArray[rf.state], reply, peer)
			if rf.currentTerm == args.Term && rf.state == Candidate {
				if reply.VoteGranted {
					grantedVoteNum++
					DPrintf("server [%v] currentTerm[%v] state[%v] get vote from server [%v] now votenum[%v]\n", rf.me, rf.currentTerm, stateArray[rf.state], peer, grantedVoteNum)
					// 当选
					if grantedVoteNum > len(rf.peers)/2 {
						rf.stateTrans(Leader)
						DPrintf("server [%v] came into leader, now in state [%v], currentTerm [%v]\n", rf.me, stateArray[rf.state], rf.currentTerm)
						// 7. leader会发送心跳给所有其他server来建立自己的权限，并防止再次选举。
						args := rf.genAppendEntreisArgs()
						rf.broadcast(&args)
=======
			// DPrintf("Server [%v] sendRequestVote to Server [%v]\n", rf.me, peer)
			ok := rf.sendRequestVote(peer, &args, &reply)
			if !ok || !reply.VoteGranted {
				// rf.mu.Lock()
				// defer rf.mu.Unlock()
				// if !ok {
					// DPrintf("Server [%v] currentTerm [%v] state[%v] failed to send RequestVote to Server [%v] ok[%v] vote [%v]", 
							// rf.me, rf.currentTerm, rf.state, peer, ok, reply.VoteGranted)
				// }
				return
			}
			rf.mu.Lock()
			// DPrintf("Server [%v] currentTerm[%v] state[%v] get reply [%v] from %v\n", rf.me, rf.currentTerm, stateArray[rf.state], reply, peer)
			defer rf.mu.Unlock()
			if rf.currentTerm == args.Term && rf.state == Candidate {
				if reply.VoteGranted {
					grantedVoteNum++
					// DPrintf("Server [%v] currentTerm[%v] state[%v] get vote from Server [%v] now votenum[%v]\n", rf.me, rf.currentTerm, stateArray[rf.state], peer, grantedVoteNum)
					// 当选
					if grantedVoteNum > len(rf.peers)/2 {
						rf.stateTrans(Leader)
						DPrintf("Server [%v] came into leader, now in state [%v], currentTerm [%v]\n", rf.me, stateArray[rf.state], rf.currentTerm)
						// 7. leader会发送心跳给所有其他server来建立自己的权限，并防止再次选举。
						// When a leader first comes to pwer, it initializes all nextIndex values to the index just after the last
						// one in its log
						for j := 0; j < len(rf.peers); j++ {
							if j == rf.me {
								continue
							}
							rf.nextIndex[j] = rf.getLastLogEntry().Index
						}
						rf.broadcast(true)
>>>>>>> lab2A
						rf.electionTime = time.Now()
					}
				} else if reply.Term > rf.currentTerm { // 发现比自己高的任期
					// 转换为Follower
					rf.stateTrans(Follower)
					rf.currentTerm = reply.Term
					rf.voteFor = -1
<<<<<<< HEAD
					DPrintf("server [%v] currentTerm[%v] found higher term[%v], transe to follower, now in state [%v]\n", rf.me, rf.currentTerm, reply.Term, stateArray[rf.state])
				}
				// else if reply.Term == rf.currentTerm {
				// 不知道怎么处理
				// }
=======
					DPrintf("Server [%v] currentTerm [%v] found higher term[%v], transe to follower, now in state [%v]\n", rf.me, rf.currentTerm, reply.Term, stateArray[rf.state])
				}
>>>>>>> lab2A
			}
			rf.mu.Unlock()
		}(peer)
	}
}

<<<<<<< HEAD
func (rf *Raft) replicate(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.sendAppendEntries(server, args, &reply)
}

// 开始发送心跳给follower
func (rf *Raft) broadcast(args *AppendEntriesArgs) {
=======
// 不带锁进入
func (rf *Raft) replicate(peer int, heartBeat bool) {
	rf.mu.Lock()
	if rf.state != Leader {	// 不是leader
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex > rf.getLastLogEntry().Index {
		prevLogIndex = rf.getLastLogEntry().Index
	}
	args := rf.genAppendEntreisArgs(prevLogIndex)
	reply := AppendEntriesReply{}
	DPrintf("Server [%v] send AppendEntrisRPC to Server [%v] with logs length [%v] begin {heartBeat[%v]}", rf.me, peer, len(args.Entries), heartBeat)
	// 发出去了，要处理一下reply
	rf.mu.Unlock()
	// 发送RPC的时候不可以带着锁
	if rf.sendAppendEntries(peer, &args, &reply) {
		rf.mu.Lock()
		DPrintf("Server [%v] currentTerm [%v] sending AppendEntrisRPC to Server [%v] success {heartBeat[%v]}", 
				rf.me, rf.currentTerm, peer, heartBeat)
		rf.mu.Unlock()
		rf.handleAppendEntrisReply(peer, args, reply)
	} else {
		rf.mu.Lock()
		DPrintf("Server [%v] currentTerm [%v] failed to send AppendEntrisRPC to Server [%v] {heartBeat[%v]}", 
				rf.me, rf.currentTerm, peer, heartBeat)
		rf.mu.Unlock()
	}
}

/*
 不带锁进入
*/
func (rf *Raft) handleAppendEntrisReply(peer int, args AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 发送成功了，需要改变一些性质
	if reply.Success == true {
		DPrintf("Server [%v] success in sending AppendEntriesRPC to Server [%v] with entries length [%v]\n", rf.me, peer, len(args.Entries))
		if len(args.Entries) == 0 {
			return
		}
		lastIndex := len(args.Entries) - 1
		rf.nextIndex[peer] = args.Entries[lastIndex].Index + 1
		rf.matchIndex[peer] = args.Entries[lastIndex].Index
		rf.commitLog(rf.matchIndex[peer])
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stateTrans(Follower)
		rf.voteFor = -1
		rf.currentTerm = reply.Term
		rf.electionTime = time.Now()
		return
	}
	// 没成功，要判断这是不是heartbeat
	if reply.ConflictIndex + 1 < rf.getFirstLogEntry().Index {
		return
	}
	if reply.ConflictIndex > rf.getLastLogEntry().Index {
		rf.nextIndex[peer] = rf.getLastLogEntry().Index + 1
	} else if rf.getLogEntry(reply.ConflictIndex).Term == reply.ConflictTerm {
		rf.nextIndex[peer] = reply.ConflictIndex + 1
	} else {
		// 回退一段log，减少RPC的发送数量
		index := reply.ConflictIndex
		for index >= rf.getFirstLogEntry().Index && rf.getLogEntry(index).Term == rf.getLogEntry(reply.ConflictIndex).Term {
			index--
		}
		rf.nextIndex[peer] = index + 1
	}
}

/*
 带锁进入
*/
func (rf *Raft) commitLog(index int) {
	if index <= rf.commitIndex {
		return
	}
	if index > rf.getLastLogEntry().Index {
		return
	}
	if index < rf.getFirstLogEntry().Index {
		return
	}
	if rf.getLogEntry(index).Term != rf.currentTerm {
		return
	}
	// 看看有多少follower复制了
	num := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if index <= rf.matchIndex[i] {
			num++
		}
	}
	if num > len(rf.peers) / 2 {	// majority
		rf.commitIndex = index
		if rf.commitIndex > rf.getLastLogEntry().Index {
			panic("handleReply: commit Log out of range")
		}
		// commit
		DPrintf("Server [%v] commit logIndex [%v]\n", rf.me, rf.commitIndex)
		rf.applyCond.Broadcast()
	} 
}

func (rf *Raft) applyMessage() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		i := rf.lastApplied + 1
		if i < rf.getFirstLogEntry().Index {
			DPrintf("Server [%v] apply try to index [%v] < rf.getFirstLogEntry().Index [%v]\n", rf.me, i, rf.getFirstLogEntry().Index)
			panic("index < firstIndex")
		}
		msg := ApplyMsg {
			CommandValid: true,
			Command: rf.getLogEntry(i).Command,
			CommandIndex: i,
		}
		DPrintf("Server [%v] apply index [%v] entry\n", rf.me, i)
		rf.applyCh <- msg
		rf.lastApplied = i
	}
}

func (rf *Raft) check(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果这台机器是server，并且peer的匹配ID小于当前logs的最后一个ID，也就是没有replicate完logs
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLogEntry().Index
}

func (rf *Raft) sendLogEntries(peer int) {
	rf.sendLogEntriesSignal[peer].L.Lock()
	defer rf.sendLogEntriesSignal[peer].L.Unlock()
	// DPrintf("Server [%v] currentTerm [%v] tempting to wake up replicate to peer [%v]", rf.me, rf.currentTerm, peer)
	for rf.killed() == false {	// 一直运行
		for !rf.check(peer) {
			rf.sendLogEntriesSignal[peer].Wait()	// 释放CPU资源，停止轮询
		}
		// DPrintf("Server [%v] currentTerm [%v] wake up replicate to peer [%v]", rf.me, rf.currentTerm, peer)
		// 这里开协程的话为什么会发那么多RPC？
		rf.replicate(peer, false)
	}
}

// 开始发送心跳给follower
func (rf *Raft) broadcast(heartBeatRPC bool) {
>>>>>>> lab2A
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
<<<<<<< HEAD
		// DPrintf("server [%v] currentTerm[%v], ready to send heratBeat to server [%v]\n", rf.me, rf.currentTerm, peer)
		go rf.replicate(peer, args)
=======
		if heartBeatRPC {	// 发送心跳
			// DPrintf("Server [%v] currentTerm[%v], ready to send heratBeat to Server [%v]\n", rf.me, rf.currentTerm, peer)
			go rf.replicate(peer, true)
		} else {
			rf.sendLogEntriesSignal[peer].Signal()
		}
>>>>>>> lab2A
	}
}

// 生成AppendEnrtisArgs, 必须带着锁进入
<<<<<<< HEAD
func (rf *Raft) genAppendEntreisArgs() AppendEntriesArgs {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm:  rf.log[len(rf.log)-1],
=======
// 注意，这个index是要发送的前一个，即prevLogEntry
func (rf *Raft) genAppendEntreisArgs(index int) AppendEntriesArgs {
	var prevLogEntry LogEntry
	var entries []LogEntry
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Index == index {
			prevLogEntry = rf.log[i]
		}
		// 应该是？Index之后的log都发过去
		if rf.log[i].Index > index /*&& rf.log[i].Term == rf.currentTerm */{
			entries = append(entries, rf.log[i])
		}
	}
	args := AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: index,
		PrevLogTerm: prevLogEntry.Term,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
>>>>>>> lab2A
	}
	return args
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
<<<<<<< HEAD
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		rf.mu.Lock()
		switch rf.state {
		case Leader:
			if rf.HeartBeatTimeOut() {
				args := rf.genAppendEntreisArgs()
				rf.broadcast(&args)
				rf.heartBeat = time.Now()
			}
=======
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
>>>>>>> lab2A
		case Follower:
			fallthrough
		case Candidate:
			if rf.ElectionTimeOut() {
<<<<<<< HEAD
				rf.electionTime = time.Now()
				rf.startElection()
			}
		}
		rf.mu.Unlock()
=======
				rf.startElection()
			}
		case Leader:
			if rf.HeartBeatTimeOut() {
				rf.mu.Lock()
				// DPrintf("Server [%v] in currentTerm [%v] reset heartbeat time", rf.me, rf.currentTerm)
				rf.heartBeat = time.Now()
				rf.mu.Unlock()
				rf.broadcast(true)
			}
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
>>>>>>> lab2A
		time.Sleep(50 * time.Millisecond)
	}
}

// ElectionTimeOut 随机选举超时
func (rf *Raft) ElectionTimeOut() bool {
<<<<<<< HEAD
	rand.Seed(time.Now().UnixNano())
	randomMilliseconds := 300 + rand.Intn(150) // 随机生成 300 到 450 之间的毫秒数
	randomDuration := time.Duration(randomMilliseconds) * time.Millisecond
	passedTime := time.Since(rf.electionTime)
	return passedTime > randomDuration
=======
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rand.Seed(time.Now().UnixNano())
	randomMilliseconds := 300 + rand.Intn(200) // 随机生成 300 到 500 之间的毫秒数
	randomDuration := time.Duration(randomMilliseconds) * time.Millisecond
	passedTime := time.Since(rf.electionTime)
	flag := passedTime > randomDuration 
	// if flag {
		// DPrintf("Server [%v] currentTerm [%v] counts election timeout [%v]", rf.me, rf.currentTerm, passedTime)
	// }
	return flag
>>>>>>> lab2A
}

// 心跳时间
func (rf *Raft) HeartBeatTimeOut() bool {
<<<<<<< HEAD
	duration := time.Duration(100) * time.Microsecond
	passedTime := time.Since(rf.heartBeat)
	return passedTime > duration
=======
	rf.mu.Lock()
	defer rf.mu.Unlock()
	duration := time.Duration(100) * time.Millisecond
	passedTime := time.Since(rf.heartBeat)
	flag := passedTime > duration
	// if flag {
		// DPrintf("Server [%v] currentTerm [%v] counts heartBeat timeout [%v] duration [%v]", rf.me, rf.currentTerm, passedTime, duration)
	// }
	return flag
>>>>>>> lab2A
}

// 获取最后一个日志
func (rf *Raft) getLastLogEntry() LogEntry {
	return rf.log[len(rf.log)-1]
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{}
	rf.state = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry {Index: 0, Term: 0}
	rf.applyCh = applyCh
	rf.electionTime = time.Now()
	rf.heartBeat = time.Now()
	rf.applyCond = sync.NewCond(&rf.mu)
	lastEntry := rf.getLastLogEntry()
	rf.sendLogEntriesSignal = make([]*sync.Cond, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastEntry.Index + 1
		if i != rf.me {
			rf.sendLogEntriesSignal[i] = sync.NewCond(&sync.Mutex{})
			go rf.sendLogEntries(i)
		}
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMessage()

	return rf
}
