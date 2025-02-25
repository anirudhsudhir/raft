package common

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type ClerkLogTopic string
type NodeLogTopic string

var displayLogs atomic.Int32

const (
	dGetKeyClerk        ClerkLogTopic = "GetKey"
	dPutKeyClerk        ClerkLogTopic = "PutKey"
	dAppendKeyClerk     ClerkLogTopic = "AppendKey"
	dInvalidPutAppendOp ClerkLogTopic = "Invalid Put/Append Op"
	dRPC                ClerkLogTopic = "RPC"
)

const (
	dInitNode                 NodeLogTopic = "InitNode"
	dGetKeyNode               NodeLogTopic = "GetKey"
	dPutKeyNode               NodeLogTopic = "PutKey"
	dAppendKeyNode            NodeLogTopic = "AppendKey"
	dApplyStateMachineUpdates NodeLogTopic = "Apply State Machine Updates"
)

func DebugNode(debugStartTime time.Time, logTopic NodeLogTopic, nodeIndex int, nodeRole string, format string, args ...interface{}) {
	logs := displayLogs.Load()

	if logs == 0 {
		if os.Getenv("KVSERVER_DEBUG") == "1" {
			displayLogs.Store(1)
		} else {
			displayLogs.Store(2)
		}
	} else if logs != 1 {
		return
	}

	timeSince := time.Since(debugStartTime).Microseconds()

	prefix := fmt.Sprintf("%-10s  time:%09d  LogTopic: %20v  NodeIndex: %02d - %10s  ", "DEBUGNODE", timeSince, logTopic, nodeIndex, nodeRole)

	log.Printf("\n\n"+strings.ToUpper(prefix)+format+"\n\n", args...)
}

func DebugClerk(logTopic ClerkLogTopic, format string, args ...interface{}) {
	prefix := fmt.Sprintf("%-10s  LogTopic: %20v  ", "DEBUGCLERK", logTopic)

	log.Printf("\n\n"+strings.ToUpper(prefix)+format+"\n\n", args...)
}
