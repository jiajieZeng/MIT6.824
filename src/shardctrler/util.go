package shardctrler

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var stateArray = [3]string{"Candidate", "Leader", "Follower"}
