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
	"bytes"
	// "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	// "fmt"
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
	CommandTerm	 int

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
	votedFor     int // candidateId that received vote in current term (or null if none)
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
	
	// 2D
	lastIncludedIndex 	int
	lastIncludedTerm 	int
	snapshot 			[]byte
}

func (rf *Raft) GetMe() int {
	return rf.me
}

func (rf *Raft) GetRaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetPersistSnapshot() ([]byte, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.snapshot, rf.lastIncludedIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = false
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// 带锁进入
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// currentTerm votedFor log[]
	// 持久化这几个状态，所以这几个状态发生改变的时候，就要进行持久化
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	// fmt.Printf("[persist] Server [%v] logs length [%v] state size [%v]\n", rf.me, len(rf.log), len(data))
	if rf.lastIncludedIndex > 0 {
		go rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	} else {
		go rf.persister.SaveRaftState(data)
	}
	// DPrintf("[Persist] Server [%v] persists currentTerm [%v], votedFor [%v], log's length [%v]", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []LogEntry
	termError := d.Decode(&currentTerm)
	voteError := d.Decode(&votedFor)
	logError := d.Decode(&log)
	lastIndexError := d.Decode(&lastIncludedIndex)
	lastTermError := d.Decode(&lastIncludedTerm)
	if  termError != nil || voteError != nil || logError != nil || lastIndexError != nil || lastTermError != nil {
		DPrintf("[readPersist] Server [%v] Error decoding, err1 [%v], err2 [%v], err3 [%v]", rf.me, termError, voteError, logError)
		panic("[readPersist] Error decoding")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.snapshot = rf.persister.ReadSnapshot()
		// DPrintf("rf.snapshot [%v]", rf.snapshot)
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
		DPrintf("[readPersist] Server [%v] Decode success currentTerm [%v], votedFor [%v], log's length [%v]",
				rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
		rf.mu.Unlock()
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// When a follower receives and handles an InstallSnapshot RPC, 
// it must hand the included snapshot to the service using Raft. 
// The InstallSnapshot handler can use the applyCh to send the snapshot to the service, 
// by putting the snapshot in ApplyMsg. The service reads from applyCh, 
// and invokes CondInstallSnapshot with the snapshot to tell Raft that the service is 
// switching to the passed-in snapshot state, 
// and that Raft should update its log at the same time. 
// (See applierSnap() in config.go to see how the tester service does this)
//
// CondInstallSnapshot should refuse to install a snapshot if it is an old snapshot 
// (i.e., if Raft has processed entries after the snapshot's lastIncludedTerm/lastIncludedIndex). 
// This is because Raft may handle other RPCs and send messages on the applyCh after 
// it handled the InstallSnapshot RPC, and before CondInstallSnapshot was invoked by the service. 
// It is not OK for Raft to go back to an older snapshot, 
// so older snapshots must be refused. When your implementation refuses the snapshot, 
// CondInstallSnapshot should just return false so that the service knows it shouldn't switch to the snapshot.
// 其实就是状态机在提交snapshot的时候，用这个函数来判断这个snapshot可不可以提交
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 过期的，不提交
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}
	DPrintf("[CondInstallSnapshot before] Server [%v] logs length [%v]\n", rf.me, len(rf.log))
	// 大于当前的日志长度
	if lastIncludedIndex > rf.getLastLogEntry().Index {
		rf.log = make([]LogEntry, 1)
	} else {
		// 截断日志
		idx := lastIncludedIndex - rf.getFirstLogEntry().Index
		// logLen := len(rf.log)
		// for idx < logLen && rf.log[idx].Index != lastIncludedIndex && rf.log[idx].Term != lastIncludedTerm {
		// 	idx++
		// }
		// 这里应该不用特判了，因为idx那个位置要留作dummy log
		retainedLog := rf.log[idx:]
		newLog := make([]LogEntry, len(retainedLog))
		copy(newLog, retainedLog)
		rf.log = newLog
	}
	// 提交这个snapshot到状态机
	// 需要改这些
	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Command = nil		
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	// 这个snapshot被提交到状态机了，所以要更新保存，并且持久化
	// 这里一定是follower进行提交的
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapshot = snapshot
	DPrintf("[CondInstallSnapshot after] Server [%v] logs length [%v]\n", rf.me, len(rf.log))
	rf.persist()
	DPrintf("[CondInstallSnapshot] Server [%v] currentTerm [%v] install snapshot lastIncludedTerm [%v] lastIncludedIndex [%v] firstLogIndex [%v] firstLogTerm [%v]", 
			rf.me, rf.currentTerm, lastIncludedTerm, lastIncludedIndex, rf.getFirstLogEntry().Index, rf.getFirstLogEntry().Term)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 创建快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	firstIndex := rf.getFirstLogEntry().Index
	if index <= firstIndex {
		// firstIndex是dummly log的index，所以等于也是不需要的
		DPrintf("[Snapshot] Server [%v] currentTerm [%v] firstLogIndex [%v] index [%v]", rf.me, rf.currentTerm, firstIndex, index)
		return
	}
	DPrintf("[Snapshot befroe] Server [%v] log size [%v]\n", rf.me, len(rf.log))
	idx := index - rf.getFirstLogEntry().Index
	// logLen := len(rf.log)
	// for idx < logLen && rf.log[idx].Index != index {
	// 	idx++
	// }
	// 当前肯定是leader才会进行snapshot，并且snapshot包括的都是提交了的log
	// Each server takes snapshots independently, covering just
	// the committed entries in its log.
	rf.lastIncludedTerm = rf.log[idx].Term
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot
	if rf.lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastIncludedIndex > rf.lastApplied {
		rf.lastApplied = rf.lastIncludedIndex
	}
	// 此时rf.log[idx].Index == index
	// 那么要把0-index的都抛弃掉
	// 要留dummy log
	retainedLog := rf.log[idx:]
	newLog := make([]LogEntry, len(retainedLog))
	copy(newLog, retainedLog)
	rf.log = newLog
	rf.log[0].Index = rf.lastIncludedIndex
	rf.log[0].Term = rf.lastIncludedTerm
	rf.log[0].Command = nil
	// 把当前创建的快照进行持久化
	rf.persist()
	rf.broadcast(true, true)
	DPrintf("[Snapshot after] Server [%v] log size [%v]\n", rf.me, len(rf.log))
	// DPrintf("[Snapshot] Server [%v] state [%v] currentTerm [%v] log's length [%v] firstLogIndex [%v] firstLogTerm [%v]", 
				// rf.me, stateArray[rf.state], rf.currentTerm, len(rf.log), rf.getFirstLogEntry().Index, rf.getFirstLogEntry().Term)
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
	Term    		int  // currentTerm for leader to update itself
	Success 		bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm 	int // 冲突Term
	ConflictIndex 	int // 冲突index
}

// InstallSnapshot RPC
type InstallSnapshotArgs struct {
	Term 				int	// leader's term
	LeaderId			int // so follwer can redirect clients
	LastIncludedIndex 	int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm	int // term of LastIncludedIndex
	// Offset	int	Hint里面说不要offset
	Data 				[]byte	// raw bytes of the snapshot chunk, starting at offset(那这里怎么搞)
	Done				bool	// true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term	int 	// currentTerm, for leader to update itself
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
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("[Before RequestVote] Server [%v] currentTerm[%v] state[%v] votedFor[%v], get RequestVote from server[%v] args.Term[%v]", rf.me, rf.currentTerm, stateArray[rf.state], rf.votedFor, args.CandidateId, args.Term)
	// 不投票
	if rf.currentTerm > args.Term || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		DPrintf("[After1 RequestVote] Server [%v] currentTerm[%v] state[%v] votedFor[%v] did not vote for server[%v]", rf.me, rf.currentTerm, stateArray[rf.state], rf.votedFor, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	// 如果一个leader或者candidate发现了自己的任期的过时的，它会马上把状态转换为follower
	if rf.currentTerm < args.Term {
		rf.stateTrans(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.electionTime = time.Now()
	}
	if !rf.upToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[After2 RequestVote] Server [%v] currentTerm[%v] votedFor[%v] did not vote for server[%v]", rf.me, rf.currentTerm, rf.votedFor, args.CandidateId)
		return
	}
	rf.electionTime = time.Now()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.stateTrans(Follower)
	DPrintf("[After3 RequestVote] Server [%v] currentTerm[%v] state[%v], vote for server[%v] reply[%v]", 
			rf.me, rf.currentTerm, stateArray[rf.state], args.CandidateId, reply)
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
func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	return ok
}

// 发送AppendEntris RPC
func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("[AppendEntries before] Server [%v] in state [%v] currentTerm [%v] votedFor [%v] get AppendEntries from Server [%v]\n", 
			rf.me, stateArray[rf.state], rf.currentTerm, rf.votedFor, args.LeaderId)
	DPrintf("[AppendEntries before] rf.getFirstLogEntry().Index [%v], rf.getLastLogEntry().Term [%v]", 
			rf.getFirstLogEntry().Index, rf.getLastLogEntry().Term)
	// DPrintf("[before] args.PrevLogIndex [%v] args.PrevLogTerm [%v]", args.PrevLogIndex, args.PrevLogTerm)
	// reply false if term < currentTerm
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf("[AppendEntries after1] Server [%v] in state [%v] currentTerm [%v] > args.Term[%v]\n", 
					rf.me, stateArray[rf.state], rf.currentTerm, args.Term)
		return
	}
	// 等待投票时candidate可能来自收到新leader的信息
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.votedFor = -1
	}
	rf.electionTime = time.Now()
	rf.stateTrans(Follower)
	if args.PrevLogIndex < rf.getFirstLogEntry().Index {
		reply.Success = false
		reply.Term = 0
		// reply.ConflictIndex = rf.getFirstLogEntry().Index
		// reply.ConflictTerm = rf.getFirstLogEntry().Term
		DPrintf("[AppendEntries after2] Server [%v] in state [%v] args.PrevLogIndex [%v] < rf.getFirstLogEntry().Index [%v]\n", 
					rf.me, stateArray[rf.state], args.PrevLogIndex, rf.getFirstLogEntry().Index)
		return
	}
	if /*args.PrevLogIndex + 1 < rf.getFirstLogEntry().Index || */args.PrevLogIndex > rf.getLastLogEntry().Index {
		DPrintf("[AppendEntries after3] Server [%v] currentTerm [%v] args.PrevLogIndex is [%v], rf.getFirstLogEntry().Index is [%v], \nrf.getLastLogEntry().Index() is [%v] out of index...", 
					rf.me, rf.currentTerm, args.PrevLogIndex, rf.getFirstLogEntry().Index, rf.getLastLogEntry().Index)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.getLastLogEntry().Index
		reply.ConflictTerm = rf.getLastLogEntry().Term
	} else if rf.getLogEntry(args.PrevLogIndex).Term == args.PrevLogTerm {	// 有这个PrevLog
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + 1 + i
			if index > rf.getLastLogEntry().Index {
				DPrintf("[AppendEntries inprogress] Server [%v] log before append length [%v]\n", rf.me, len(rf.log))
				rf.log = append(rf.log, args.Entries[i:]...)
				DPrintf("[AppendEntries inprogress] Server [%v] log after append length [%v]\n", rf.me, len(rf.log))
				break
			} else if rf.getLogEntry(index).Term != entry.Term {
				// 从这个位置开始覆盖
				// idx := 0
				// for rf.log[idx].Index != index {
				// 	idx++
				// }
				idx := index - rf.getFirstLogEntry().Index
				DPrintf("[AppendEntries inprogress] Server [%v] log before append length [%v]\n", rf.me, len(rf.log))
				// DPrintf("[inprogress] Server [%v] entry.Index[%v] firstIndex [%v]", rf.me, entry.Index, firstIndex)
				log := append(rf.log[:idx], args.Entries[i:]...)
				// DPrintf("[inprogress] Server [%v] log after append length [%v]\n", rf.me, len(log))
				newLog := make([]LogEntry, len(log))
				copy(newLog, log)
				rf.log = newLog
				DPrintf("[AppendEntries inprogress] Server [%v] log after copy length [%v]\n", rf.me, len(rf.log))
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
		// DPrintf("[AppendEntries after4] Server [%v] in state [%v] currentTerm [%v] votedFor [%v] get AppendEntries from Server [%v] success\n", rf.me, stateArray[rf.state], rf.currentTerm, rf.votedFor, args.LeaderId)
	} else {
		// DPrintf("[AppendEntries after5] rf.getLogEntry(args.PrevLogIndex).Term [%v], args.PrevLogTerm [%v]",
		// 		rf.getLogEntry(args.PrevLogIndex).Term, args.PrevLogTerm)
		// reply false if log doesn't contain an entry at preveLogIndex whose term matches prevLogTerm
		index := args.PrevLogIndex
		prevEntry := rf.getLogEntry(args.PrevLogIndex)
		for index >= rf.getFirstLogEntry().Index && rf.getLogEntry(index).Term == prevEntry.Term {
			index--
		}
		reply.Term = rf.currentTerm
		reply.Success = false
		if index >= rf.getFirstLogEntry().Index {
			reply.ConflictIndex = index
			reply.ConflictTerm = rf.getLogEntry(index).Term
			DPrintf("[AppendEntries after5] Delay log ConflictIndex [%v] ConflictTerm [%v]", reply.ConflictIndex, reply.ConflictTerm)
		} else {
			// 这个日志落后太多了，要返回使用snapshot进行更新
			reply.ConflictIndex = rf.lastIncludedIndex
			reply.ConflictTerm = rf.lastIncludedTerm
			DPrintf("[AppendEntries after5] Delay too many rf.lastIncludedIndex [%v] rf.lastIncludedTerm [%v]", rf.lastIncludedIndex, rf.lastIncludedTerm)
		}
		DPrintf("[AppendEntries after5] Server [%v] in state [%v] currentTerm [%v] votedFor [%v] get AppendEntries replyc.ConflictIndex [%v] reply.ConflictTerm [%v]\n", 
				rf.me, stateArray[rf.state], rf.currentTerm, rf.votedFor, reply.ConflictIndex, reply.ConflictTerm)
	}
}

// 带锁进入
// the first index it stores for that term.
func (rf *Raft) getFirstLogEntry() LogEntry {
	return rf.log[0]
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	idx := index - rf.getFirstLogEntry().Index
	if idx >= len(rf.log) || idx < 0 {
		return LogEntry{}
	}
	return rf.log[idx]
}

func (rf *Raft) AppendLogEntries(command interface{}) LogEntry {
	index := rf.log[len(rf.log) - 1].Index + 1;
	newEntry := LogEntry {
		Index: index,
		Term: rf.currentTerm,
		Command: command,
	} 
	rf.log = append(rf.log, newEntry)
	rf.persist()
	return newEntry
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
	// fmt.Printf("Server %v receives a new command to replicate in term %v now log's length %v\n", rf.me, rf.currentTerm, len(rf.log))
	index = newEntry.Index
	term = rf.currentTerm
	rf.broadcast(false, false)
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

// 开始一次选举
// 根据经验，我们发现最简单的做法是首先记录回复中的任期（它可能比你当前的任期更高），
// 然后将当前任期与你在原始 RPC 中发送的任期进行比较。如果两者不同，则放弃回复并返回。
// 只有当两个术语相同时，才应继续处理回复。
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	}
	rf.electionTime = time.Now()
	rf.stateTrans(Candidate) // 状态转换
	rf.currentTerm += 1      // 提升任期
	rf.votedFor = rf.me       // 投票给自己
	grantedVoteNum := 1      // 投票计数
	rf.persist()
	DPrintf("Server [%v] kick off election time out, now in state [%s], currentTerm [%v]\n", rf.me, stateArray[rf.state], rf.currentTerm)
	args := rf.genRequestVoteArgs()
	for peer := range rf.peers { // 向所有的peer发送RequestVoteRPC
		if peer == rf.me {
			continue
		}
		// DPrintf("Server [%v] make goroutine for Server [%v]\n", rf.me, peer)
		go func(peer int) {
			reply := RequestVoteReply{}
			DPrintf("Server [%v] sendRequestVote to Server [%v]\n", rf.me, peer)
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
						rf.broadcast(true, false)
						rf.electionTime = time.Now()
					}
				} else if reply.Term > rf.currentTerm { // 发现比自己高的任期
					// 转换为Follower
					rf.stateTrans(Follower)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					DPrintf("Server [%v] currentTerm [%v] found higher term[%v], transe to follower, now in state [%v]\n", rf.me, rf.currentTerm, reply.Term, stateArray[rf.state])
				}
			}
		}(peer)
	}
}

// 带锁进入
func (rf *Raft) genInstallSnapshotArgs() InstallSnapshotArgs {
	args := InstallSnapshotArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.lastIncludedTerm,
		Data: rf.snapshot,
		Done: true,
	}
	return args
}

// follower会从leader那里接收到snapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("[InstallSnapshot before] Server [%v] currentTerm [%v] get InstallSnapshotRPC", rf.me, rf.currentTerm)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[InstallSnapshot after1] Server [%v] args.Term [%v] < rf.currentTerm [%v]", rf.me, args.Term, rf.currentTerm)
		return
	}
	// 相当于接收到了AppendEntriesRPC
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.stateTrans(Follower)
	rf.electionTime = time.Now()
	
	// 接收到了leader的snapshot，要持久化一下
	if args.LastIncludedIndex > rf.lastIncludedIndex {
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.snapshot = args.Data
		// commit的不一定被applied了
		if args.LastIncludedIndex <= rf.lastApplied && args.LastIncludedIndex > rf.getFirstLogEntry().Index {
			// 这里也要去截断日志的
			// 这里的情况是，发送过来的snapshot包含的内容其实都已经被当前节点
			// 提交了，但还在log里面，没有压缩日志，所以在这里要进行日志压缩
			// 大于当前的日志长度
			if rf.lastIncludedIndex > rf.getLastLogEntry().Index {
				rf.log = make([]LogEntry, 1)
			} else {
				// 截断日志
				idx := rf.lastIncludedIndex - rf.getFirstLogEntry().Index
				retainedLog := rf.log[idx:]
				newLog := make([]LogEntry, len(retainedLog))
				copy(newLog, retainedLog)
				rf.log = newLog
			}
			// 提交这个snapshot到状态机
			// 需要改这些
			rf.log[0].Term = rf.lastIncludedTerm
			rf.log[0].Index = rf.lastIncludedIndex
			rf.log[0].Command = nil		
		} else if args.LastIncludedIndex > rf.commitIndex {
			msg := ApplyMsg {
				SnapshotValid: true,
				Snapshot: args.Data,
				SnapshotTerm: args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
			DPrintf("[InstallSnapshot after3] Server [%v] apply snapshot args.LastIncludedIndex [%v], args.LastIncludedTerm [%v]",
					rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
			go func() {
				rf.applyCh <- msg
			}()
		}
		// 剩下一种情况是lastIncludeIndex在lastApplied和commitIndex之间，这种直接抛弃掉
		// 等lastApplied追上来，会去到第一种情况
	}
}

// // follower会从leader那里接收到snapshot
// func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	DPrintf("[InstallSnapshot before] Server [%v] currentTerm [%v] get InstallSnapshotRPC", rf.me, rf.currentTerm)
// 	reply.Term = rf.currentTerm
// 	if args.Term < rf.currentTerm {
// 		DPrintf("[InstallSnapshot after1] Server [%v] args.Term [%v] < rf.currentTerm [%v]", rf.me, args.Term, rf.currentTerm)
// 		return
// 	}
// 	// 相当于接收到了AppendEntriesRPC
// 	if args.Term > rf.currentTerm {
// 		rf.currentTerm = args.Term
// 		rf.votedFor = -1
// 		rf.persist()
// 	}
// 	rf.stateTrans(Follower)
// 	rf.electionTime = time.Now()
// 	if args.LastIncludedIndex <= rf.commitIndex {
// 		// 过期的
// 		DPrintf("[InstallSnapshot after2] Server [%v] args.LastIncludedIndex [%v] < rf.commitIndex [%v]", 
// 					rf.me, args.LastIncludedIndex, rf.commitIndex)
// 		return
// 	}
// 	// 接收到了leader的snapshot，要持久化一下
// 	rf.lastIncludedIndex = args.LastIncludedIndex
// 	rf.lastIncludedTerm = args.LastIncludedTerm
// 	rf.snapshot = args.Data
// 	rf.persist()
// 	msg := ApplyMsg {
// 		SnapshotValid: true,
// 		Snapshot: args.Data,
// 		SnapshotTerm: args.LastIncludedTerm,
// 		SnapshotIndex: args.LastIncludedIndex,
// 	}
// 	DPrintf("[InstallSnapshot after3] Server [%v] apply snapshot args.LastIncludedIndex [%v], args.LastIncludedTerm [%v]",
// 			rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
// 	go func() {
// 		rf.applyCh <- msg
// 	}()
// }


// 不带锁进入
func (rf *Raft) replicate(peer int, heartBeat bool, isSnapshot bool) {
	rf.mu.Lock()
	if rf.state != Leader {	// 不是leader
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex > rf.getLastLogEntry().Index {
		prevLogIndex = rf.getLastLogEntry().Index
	}
	DPrintf("[replicate] Server [%v] prevLogIndex [%v], rf.getFirstLogEntry().Index [%v], rf.getLastLogEntry().Index [%v]", 
			rf.me, prevLogIndex, rf.getFirstLogEntry().Index, rf.getLastLogEntry().Index)
	if isSnapshot || prevLogIndex < rf.getFirstLogEntry().Index {
		// 发送snapshot
		args := rf.genInstallSnapshotArgs()
		reply := InstallSnapshotReply{}	
		DPrintf("Server [%v] send InstallSnapshotRPC to Server [%v] with snapshot begin {heartBeat[%v]}", rf.me, peer, heartBeat)
		rf.mu.Unlock()
		if rf.sendInstallSnapshot(peer, &args, &reply) {
			// rf.mu.Lock()
			// DPrintf("Server [%v] currentTerm [%v] sending InstallSnapshotRPC to Server [%v] success {heartBeat[%v]}", 
			// 		rf.me, rf.currentTerm, peer, heartBeat)
			// rf.mu.Unlock()
			rf.handleInstallSnapshotReply(peer, args, reply)
		}
	} else {
		// 直接发
		args := rf.genAppendEntriesArgs(prevLogIndex)
		reply := AppendEntriesReply{}
		DPrintf("Server [%v] send AppendEntrisRPC to Server [%v] with log length [%v] begin {heartBeat[%v]}", rf.me, peer, len(args.Entries), heartBeat)
		// 发出去了，要处理一下reply
		rf.mu.Unlock()
		// 发送RPC的时候不可以带着锁
		if rf.sendAppendEntries(peer, &args, &reply) {
			// rf.mu.Lock()
			// DPrintf("Server [%v] currentTerm [%v] sending AppendEntrisRPC to Server [%v] success {heartBeat[%v]}", 
			// 		rf.me, rf.currentTerm, peer, heartBeat)
			// rf.mu.Unlock()
			rf.handleAppendEntrisReply(peer, args, reply)
		} else {
			// rf.mu.Lock()
			// DPrintf("Server [%v] currentTerm [%v] failed to send AppendEntrisRPC to Server [%v] {heartBeat[%v]}", 
			// 		rf.me, rf.currentTerm, peer, heartBeat)
			// rf.mu.Unlock()
		}
	}
}

func (rf *Raft) handleInstallSnapshotReply(peer int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	if reply.Term < rf.currentTerm {
		DPrintf("[handleInstallSnapshotReply] args.Term [%v] < rf.currentTerm [%v]", args.Term, rf.currentTerm)
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stateTrans(Follower)
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.electionTime = time.Now()
		rf.persist()
		return
	}
	if rf.nextIndex[peer] <= args.LastIncludedIndex {
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
	if rf.matchIndex[peer] < args.LastIncludedIndex {
		rf.matchIndex[peer] = args.LastIncludedIndex
	}
	rf.commitLog(rf.matchIndex[peer])
}

/*
 不带锁进入
*/
func (rf *Raft) handleAppendEntrisReply(peer int, args AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term < rf.currentTerm {
		return
	}
	// 发送成功了，需要改变一些性质
	if reply.Success == true {
		DPrintf("[handleAppendEntrisReply1] Server [%v] success in sending AppendEntriesRPC to Server [%v] with entries length [%v]\n", rf.me, peer, len(args.Entries))
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
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.persist()
		DPrintf("[handleAppendEntrisReply2] Server [%v] failed to send AppendEntriesRPC to Server [%v] reply.Term [%v], rf.currentTerm [%v]",
				rf.me, peer, reply.Term, rf.currentTerm)
		return
	}
	// 落后太多，需要快照补救
	if reply.ConflictIndex <= rf.getFirstLogEntry().Index {
		rf.nextIndex[peer] = reply.ConflictIndex + 1
		return
	}
	if reply.ConflictIndex > rf.getLastLogEntry().Index {
		rf.nextIndex[peer] = rf.getLastLogEntry().Index + 1
		DPrintf("[handleAppendEntrisReply4] Server [%v] failed to send AppendEntriesRPC to Server [%v] reply.ConflictIndex [%v], rf.getLastLogEntry().Index [%v]",
				rf.me, peer, reply.ConflictIndex, rf.getLastLogEntry().Index)
	} else if rf.getLogEntry(reply.ConflictIndex).Term == reply.ConflictTerm {
		rf.nextIndex[peer] = reply.ConflictIndex + 1
		DPrintf("[handleAppendEntrisReply5]")
	} else {
		// 回退一段log，减少RPC的发送数量
		index := reply.ConflictIndex
		conflictEntry := rf.getLogEntry(reply.ConflictIndex)
		for index >= rf.getFirstLogEntry().Index && rf.getLogEntry(index).Term == conflictEntry.Term {
			index--
		}
		rf.nextIndex[peer] = index + 1
		DPrintf("[handleAppendEntrisReply5] Server [%v] failed to send AppendEntriesRPC to Server [%v] now nextIndex [%v]",
				rf.me, peer, rf.nextIndex[peer])
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
	// 只提交当前任期的日志
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

// func (rf *Raft) applyMessage() {
// 	for !rf.killed() {
// 		rf.mu.Lock()
// 		for rf.lastApplied >= rf.commitIndex {
// 			rf.applyCond.Wait()
// 		}
// 		firstIndex, commitIndex, lastApplied := rf.getFirstLogEntry().Index, rf.commitIndex, rf.lastApplied
// 		entries := make([]LogEntry, commitIndex - lastApplied)
// 		copy(entries, rf.log[lastApplied + 1 - firstIndex: commitIndex + 1 - firstIndex])
// 		rf.mu.Unlock()
// 		for _, entry := range entries {
// 			rf.applyCh <- ApplyMsg {
// 				CommandValid: true,
// 				Command: entry.Command,
// 				CommandIndex: entry.Index,
// 				CommandTerm: entry.Term,
// 			}
// 		}
// 		rf.mu.Lock()
// 		DPrintf("Server [%v] apply [%v, [%v]", rf.me, rf.lastApplied, commitIndex)
// 		if commitIndex > rf.lastApplied {
// 			rf.lastApplied = commitIndex
// 		}
// 		rf.mu.Unlock()
// 	}
// }

func (rf *Raft) applyMessage() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		i := rf.lastApplied + 1
		if i < rf.getFirstLogEntry().Index {
			DPrintf("Server [%v] apply try to index [%v] < rf.getFirstLogEntry().Index [%v]\n", rf.me, i, rf.getFirstLogEntry().Index)
			panic("index < firstIndex")
		}
		entry := rf.getLogEntry(i)
		msg := ApplyMsg {
			CommandValid: true,
			Command: entry.Command,
			CommandIndex: i,
			CommandTerm: entry.Term,
		}
		DPrintf("Server [%v] apply index [%v] entry\n", rf.me, i)
		rf.lastApplied = i
		rf.mu.Unlock()
		rf.applyCh <- msg
	}
}

func (rf *Raft) check(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果这台机器是server，并且peer的匹配ID小于当前log的最后一个ID，也就是没有replicate完log
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
		rf.replicate(peer, false, false)
	}
}

// 开始发送心跳给follower
func (rf *Raft) broadcast(heartBeatRPC bool, snapShotRPC bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if heartBeatRPC {	// 发送心跳
			// DPrintf("Server [%v] currentTerm[%v], ready to send heratBeat to Server [%v]\n", rf.me, rf.currentTerm, peer)
			go rf.replicate(peer, true, snapShotRPC)
		} else {
			rf.sendLogEntriesSignal[peer].Signal()
		}
	}
}

// 生成AppendEnrtisArgs, 必须带着锁进入
// 注意，这个index是要发送的前一个，即prevLogEntry
func (rf *Raft) genAppendEntriesArgs(index int) AppendEntriesArgs {
	prevLogEntry := rf.log[index - rf.getFirstLogEntry().Index]
	entries := rf.log[index - rf.getFirstLogEntry().Index + 1:]
	args := AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: index,
		PrevLogTerm: prevLogEntry.Term,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Follower:
			fallthrough
		case Candidate:
			if rf.ElectionTimeOut() {
				rf.startElection()
			}
		case Leader:
			if rf.HeartBeatTimeOut() {
				rf.mu.Lock()
				// DPrintf("Server [%v] in currentTerm [%v] reset heartbeat time", rf.me, rf.currentTerm)
				rf.heartBeat = time.Now()
				rf.mu.Unlock()
				rf.broadcast(true, false)
			}
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		time.Sleep(30 * time.Millisecond)
	}
}

// ElectionTimeOut 随机选举超时
func (rf *Raft) ElectionTimeOut() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rand.Seed(time.Now().UnixNano())
	randomMilliseconds := 200 + rand.Intn(200) // 随机生成 300 到 450 之间的毫秒数
	randomDuration := time.Duration(randomMilliseconds) * time.Millisecond
	passedTime := time.Since(rf.electionTime)
	flag := passedTime > randomDuration 
	// if flag {
		// DPrintf("Server [%v] currentTerm [%v] counts election timeout [%v]", rf.me, rf.currentTerm, passedTime)
	// }
	return flag
}

// 心跳时间
func (rf *Raft) HeartBeatTimeOut() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	duration := time.Duration(100) * time.Millisecond
	passedTime := time.Since(rf.heartBeat)
	flag := passedTime > duration
	// if flag {
		// DPrintf("Server [%v] currentTerm [%v] counts heartBeat timeout [%v] duration [%v]", rf.me, rf.currentTerm, passedTime, duration)
	// }
	return flag
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
	rf.votedFor = -1
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
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
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
	// rf.persist()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMessage()

	return rf
}
