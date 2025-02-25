package raft

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"
)

type LogTopic string

var displayLogs atomic.Int32

const (
	dMake               LogTopic = "Make()"
	dStart              LogTopic = "Start()"
	dVote               LogTopic = "Vote"
	dRequestVoteHandler LogTopic = "RequestVoteHandler"
	dAppendEntries      LogTopic = "AppendEntriesRPC"
	dCommitIndex        LogTopic = "UpdateCommitIndex"
	dApplyLogEntries    LogTopic = "ApplyLogEntries"
	dReplicateLog       LogTopic = "ReplicateLog()"
	dPersist            LogTopic = "Persist"
	dRPC                LogTopic = "RPC"
)

func Debug(debugStartTime time.Time, logTopic LogTopic, nodeIndex int, nodeRole string, format string, args ...interface{}) {
	logs := displayLogs.Load()

	if logs == 0 {
		if os.Getenv("RAFT_DEBUG") == "1" {
			displayLogs.Store(1)
		} else {
			displayLogs.Store(2)
		}
	} else if logs != 1 {
		return
	}

	timeSince := time.Since(debugStartTime).Microseconds()

	prefix := fmt.Sprintf("%10s  time:%09d  LogTopic: %20v  NodeIndex: %02d - %10s  ", "Raft", timeSince, logTopic, nodeIndex, nodeRole)

	log.Printf(prefix+format+"\n", args...)
}
