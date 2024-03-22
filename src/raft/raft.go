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
	heartBeat    *time.Timer
	electionTime *time.Timer
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
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
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
	LastLogTerm  LogEntry // term of candidate's last log entery
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
	PrevLogTerm  LogEntry   // term of prevLogIndex entry
	Entries      []LogEntry // log entries to stroe(empty ofr heartbeat;may send more than on for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// up-to-date返回1，否则 0
func (rf *Raft) upToDate(lastLogTerm int, lastLogIndex int) bool {
	if lastLogTerm > rf.currentTerm {
		return true
	} else if lastLogTerm == rf.currentTerm {
		// 2A没有推送日志，所以先直接返回真
		// 这里要比较currentTerm下谁的日志currentTerm长度更长
		return true
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
	// 不投票
	if rf.currentTerm > args.Term || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 如果一个leader或者candidate发现了自己的任期的过时的，它会马上把状态转换为follower
	if rf.currentTerm < args.Term {
		rf.stateTrans(Follower)
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	if !rf.upToDate(args.Term, args.LastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.voteFor = args.CandidateId
	rf.stateTrans(Follower)
	rf.electionTime.Reset(getElectionTime())
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
	defer rf.mu.Unlock()
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 等待投票时candidate可能来自收到新leader的信息
	if args.Term > rf.currentTerm {
		rf.stateTrans(Follower)
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	rf.stateTrans(Follower)
	rf.electionTime.Reset(getElectionTime())
	// reply false if log doesn't contain an entry at preveLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex < rf.log[0].Index {
		reply.Success = false
		reply.Term = 0
		return
	}
	reply.Term = rf.currentTerm
	reply.Success = true
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
	// rf.mu.Lock()
	rf.state = newState
	// rf.mu.Unlock()
}

// 开始一次选举
// 根据经验，我们发现最简单的做法是首先记录回复中的术语（它可能比你当前的术语更高），
// 然后将当前任期与你在原始 RPC 中发送的任期进行比较。如果两者不同，则放弃回复并返回。
// 只有当两个术语相同时，才应继续处理回复。
func (rf *Raft) startElection() {
	rf.voteFor = rf.me  // 投票给自己
	grantedVoteNum := 1 // 投票计数
	logLen := len(rf.log)
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = logLen - 1
	args.LastLogTerm = rf.log[logLen-1]
	for peer := range rf.peers { // 向所有的peer发送RequestVoteRPC
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.state == Candidate {
					// 发现比自己高的任期
					if reply.Term > rf.currentTerm {
						// 转换为Follower
						rf.stateTrans(Follower)
						rf.currentTerm = reply.Term
						rf.voteFor = -1
					} else if reply.VoteGranted {
						grantedVoteNum++
						// 当选
						if grantedVoteNum > len(rf.peers)/2 {
							rf.stateTrans(Leader)
							// 7. leader会发送心跳给所有其他server来建立自己的权限，并防止再次选举。
							rf.broadcast()
							DPrintf("server [%v] came into leader, now in state [%v], currentTerm [%v]\n", rf.me, stateArray[rf.state], rf.currentTerm)
						}
					}
					// else if reply.Term == rf.currentTerm {
					// 不知道怎么处理
					// }
				}

			}
		}(peer)
	}
}

func (rf *Raft) replicate(server int) {
	rf.mu.Lock()
	args := new(AppendEntriesArgs)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = len(rf.log) - 1
	args.PrevLogTerm = rf.log[len(rf.log)-1]
	reply := new(AppendEntriesReply)
	rf.mu.Unlock()
	rf.sendAppendEntries(server, args, reply)
}

// 开始发送心跳给follower
func (rf *Raft) broadcast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicate(peer)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep(time.Second)
		select {
		case <-rf.electionTime.C:
			rf.mu.Lock()
			rf.stateTrans(Candidate) // 状态转换
			rf.currentTerm += 1      // 提升任期
			DPrintf("server [%v] kick off election time out, now int state [%s], currentTerm [%v]\n", rf.me, stateArray[rf.state], rf.currentTerm)
			rf.startElection()
			rf.electionTime.Reset(getElectionTime())
			rf.mu.Unlock()
		case <-rf.heartBeat.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.broadcast()
				rf.heartBeat.Reset(getHeartBeatTime())
			}
			rf.mu.Unlock()
		}
	}
}

// 随机的选举超时
func getElectionTime() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randomMilliseconds := 150 + rand.Intn(151)
	randomDuration := time.Duration(randomMilliseconds) * time.Millisecond
	return randomDuration
}

// 心跳时间
func getHeartBeatTime() time.Duration {
	return time.Duration(50) * time.Millisecond
	/*
		// 在另一个 goroutine 中处理计时器的事件
		go func() {
			for {
				<-timer.C
				fmt.Println("Timer tick")
			}
		}()
	*/
}

// 获取最后一个日志
func (rf *Raft) getLastLogEntry() LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	rf.applyCh = applyCh
	rf.electionTime = time.NewTimer(getElectionTime())
	rf.heartBeat = time.NewTimer(getHeartBeatTime())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastEntry := rf.getLastLogEntry()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastEntry.Index + 1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
