package common

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"time"
)

type Clerk struct {
	servers []string

	// me:
	currentAssumedLeader int

	debugStartTime time.Time
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.currentAssumedLeader = -1
	ck.debugStartTime = time.Now()

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		key,
	}
	reply := GetReply{}

	if ck.currentAssumedLeader != -1 {
		DebugClerk(ck.debugStartTime, dGetKeyClerk, "Sending a Get RPC to Node = %d, args = %+v", ck.currentAssumedLeader, args)

		serverAddr := ck.servers[ck.currentAssumedLeader]
		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			DebugClerk(ck.debugStartTime, dRPC, "Error while dialing key-value cluster, server = %s -> %+v", serverAddr, err)
		} else {
			err := client.Call("KVServer.Get", args, &reply)
			if err != nil {
				DebugClerk(ck.debugStartTime, dRPC, "Get RPC to server %s failed -> %+v", serverAddr, err)
			} else {
				DebugClerk(ck.debugStartTime, dGetKeyClerk, "Get RPC successful, LeaderIndex = %d, args = %+v", ck.currentAssumedLeader, args)
				return reply.Value

			}
		}
	}

	// me: send RPCs to all nodes until successful if no current assumed leader or RPC fails
	for {
		for i, server := range ck.servers {
			DebugClerk(ck.debugStartTime, dGetKeyClerk, "Sending a Get RPC to Node = %d, args = %+v", i, args)

			client, err := rpc.Dial("tcp", server)
			if err != nil {
				DebugClerk(ck.debugStartTime, dRPC, "Error while dialing key-value cluster, server = %s -> %+v", server, err)
				continue
			} else {
				err := client.Call("KVServer.Get", args, &reply)
				if err != nil {
					DebugClerk(ck.debugStartTime, dRPC, "Get RPC to server %s failed -> %+v", server, err)
					continue
				} else {
					ck.currentAssumedLeader = i
					DebugClerk(ck.debugStartTime, dGetKeyClerk, "Get RPC successful, LeaderIndex = %d, args = %+v", ck.currentAssumedLeader, args)
					return reply.Value
				}
			}
		}
	}

	// DebugClerk(ck.debugStartTime, dGetKeyClerk, "Get RPC failed, Args = %+v, Reply = %+v", args, reply)
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		key,
		value,
	}

	reply := PutAppendReply{}

	switch op {
	case "Put":

		if ck.currentAssumedLeader != -1 {
			DebugClerk(ck.debugStartTime, dPutKeyClerk, "Sending a Put RPC to Node = %d, args = %+v", ck.currentAssumedLeader, args)

			serverAddr := ck.servers[ck.currentAssumedLeader]
			client, err := rpc.Dial("tcp", serverAddr)
			if err != nil {
				DebugClerk(ck.debugStartTime, dRPC, "Error while dialing key-value cluster, server = %s -> %+v", serverAddr, err)
			} else {
				err := client.Call("KVServer.Put", args, &reply)
				if err != nil {
					DebugClerk(ck.debugStartTime, dRPC, "Put RPC to server %s failed -> %+v", serverAddr, err)
				} else {
					DebugClerk(ck.debugStartTime, dPutKeyClerk, "Put RPC successful, LeaderIndex = %d, args = %+v", ck.currentAssumedLeader, args)
					return

				}
			}
		}

		// me: send RPCs to all nodes until successful if no current assumed leader or RPC fails
		for {
			for i, server := range ck.servers {
				DebugClerk(ck.debugStartTime, dPutKeyClerk, "Sending a Put RPC to Node = %d, args = %+v", i, args)

				client, err := rpc.Dial("tcp", server)
				if err != nil {
					DebugClerk(ck.debugStartTime, dRPC, "Error while dialing key-value cluster, server = %s -> %+v", server, err)
					continue
				} else {
					err := client.Call("KVServer.Put", args, &reply)
					if err != nil {
						DebugClerk(ck.debugStartTime, dRPC, "Put RPC to server %s failed -> %+v", server, err)
						continue
					} else {
						ck.currentAssumedLeader = i
						DebugClerk(ck.debugStartTime, dPutKeyClerk, "Put RPC successful, LeaderIndex = %d, args = %+v", ck.currentAssumedLeader, args)
						return
					}
				}
			}
		}

		// DebugClerk(ck.debugStartTime, dPutKeyClerk, "Put RPC failed, Args = %+v, Reply = %+v", args, reply)

	case "Append":

		if ck.currentAssumedLeader != -1 {
			DebugClerk(ck.debugStartTime, dAppendKeyClerk, "Sending a Append RPC to Node = %d, args = %+v", ck.currentAssumedLeader, args)

			serverAddr := ck.servers[ck.currentAssumedLeader]
			client, err := rpc.Dial("tcp", serverAddr)
			if err != nil {
				DebugClerk(ck.debugStartTime, dRPC, "Error while dialing key-value cluster, server = %s -> %+v", serverAddr, err)
			} else {
				err := client.Call("KVServer.Append", args, &reply)
				if err != nil {
					DebugClerk(ck.debugStartTime, dRPC, "Append RPC to server %s failed -> %+v", serverAddr, err)
				} else {
					DebugClerk(ck.debugStartTime, dAppendKeyClerk, "Append RPC successful, LeaderIndex = %d, args = %+v", ck.currentAssumedLeader, args)
					return

				}
			}
		}

		// me: send RPCs to all nodes until successful if no current assumed leader or RPC fails
		for {
			for i, server := range ck.servers {
				DebugClerk(ck.debugStartTime, dAppendKeyClerk, "Sending a Append RPC to Node = %d, args = %+v", i, args)

				client, err := rpc.Dial("tcp", server)
				if err != nil {
					DebugClerk(ck.debugStartTime, dRPC, "Error while dialing key-value cluster, server = %s -> %+v", server, err)
					continue
				} else {
					err := client.Call("KVServer.Append", args, &reply)
					if err != nil {
						DebugClerk(ck.debugStartTime, dRPC, "Append RPC to server %s failed -> %+v", server, err)
						continue
					} else {
						ck.currentAssumedLeader = i
						DebugClerk(ck.debugStartTime, dAppendKeyClerk, "Append RPC successful, LeaderIndex = %d, args = %+v", ck.currentAssumedLeader, args)
						return
					}
				}
			}
		}

		// DebugClerk(ck.debugStartTime, dAppendKeyClerk, "Append RPC failed, Args = %+v, Reply = %+v", args, reply)

	default:

		DebugClerk(ck.debugStartTime, dInvalidPutAppendOp, "Invalid Put/Append operation, args = %+v", args)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
