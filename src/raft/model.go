package raft

type State int

const (
	Candidate = iota
	Leader
	Follower
)

type LogEntry struct {
	Index        int // 索引
	Command      interface{}
	Term 		 int // 任期
}
