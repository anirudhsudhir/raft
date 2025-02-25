package common

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
)

type Clerk struct {
	servers              []string
	currentAssumedLeader int
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
		DebugClerk(dGetKeyClerk, "Sending a Get RPC to Node = %d, args = %+v", ck.currentAssumedLeader, args)

		serverAddr := ck.servers[ck.currentAssumedLeader]
		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			DebugClerk(dRPC, "Error while dialing key-value cluster, server = %s -> %+v", serverAddr, err)
		} else {
			err := client.Call("KVServer.Get", args, &reply)
			if err != nil {
				DebugClerk(dRPC, "Get RPC to server %s failed -> %+v", serverAddr, err)
			} else {
				if reply.Err == OK || reply.Err == ErrNoKey {
					DebugClerk(dGetKeyClerk, "Get RPC successful, KVNode-LeaderIndex = %d, args = %+v, reply = %+v", ck.currentAssumedLeader, args, reply)
					return reply.Value
				}
			}
		}
	}

	// me: send RPCs to all nodes until successful if no current assumed leader or RPC fails
	for {
		for i, server := range ck.servers {
			DebugClerk(dGetKeyClerk, "Sending a Get RPC to Node = %d, args = %+v", i, args)

			client, err := rpc.Dial("tcp", server)
			if err != nil {
				DebugClerk(dRPC, "Error while dialing key-value cluster, server = %s -> %+v", server, err)
				continue
			} else {
				err := client.Call("KVServer.Get", args, &reply)
				if err != nil {
					DebugClerk(dRPC, "Get RPC to server %s failed -> %+v", server, err)
					continue
				} else {
					if reply.Err == OK || reply.Err == ErrNoKey {
						ck.currentAssumedLeader = i
						DebugClerk(dGetKeyClerk, "Get RPC successful, KVNode-LeaderIndex = %d, args = %+v, reply = %+v", ck.currentAssumedLeader, args, reply)
						return reply.Value
					}
				}
			}
		}
	}

	// DebugClerk(dGetKeyClerk, "Get RPC failed, Args = %+v, Reply = %+v", args, reply)
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
			DebugClerk(dPutKeyClerk, "Sending a Put RPC to Node = %d, args = %+v", ck.currentAssumedLeader, args)

			serverAddr := ck.servers[ck.currentAssumedLeader]
			client, err := rpc.Dial("tcp", serverAddr)
			if err != nil {
				DebugClerk(dRPC, "Error while dialing key-value cluster, server = %s -> %+v", serverAddr, err)
			} else {
				err := client.Call("KVServer.Put", args, &reply)
				if err != nil {
					DebugClerk(dRPC, "Put RPC to server %s failed -> %+v", serverAddr, err)
				} else {
					if reply.Err == OK {
						DebugClerk(dPutKeyClerk, "Put RPC successful, KVNode-LeaderIndex = %d, args = %+v, reply = %+v", ck.currentAssumedLeader, args, reply)
						return
					}
				}
			}
		}

		// me: send RPCs to all nodes until successful if no current assumed leader or RPC fails
		for {
			for i, server := range ck.servers {
				DebugClerk(dPutKeyClerk, "Sending a Put RPC to Node = %d, args = %+v", i, args)

				client, err := rpc.Dial("tcp", server)
				if err != nil {
					DebugClerk(dRPC, "Error while dialing key-value cluster, server = %s -> %+v", server, err)
					continue
				} else {
					err := client.Call("KVServer.Put", args, &reply)
					if err != nil {
						DebugClerk(dRPC, "Put RPC to server %s failed -> %+v", server, err)
						continue
					} else {
						if reply.Err == OK {
							ck.currentAssumedLeader = i
							DebugClerk(dPutKeyClerk, "Put RPC successful, KVNode-LeaderIndex = %d, args = %+v, reply = %+v", ck.currentAssumedLeader, args, reply)
							return
						}
					}
				}
			}
		}

		// DebugClerk(dPutKeyClerk, "Put RPC failed, Args = %+v, Reply = %+v", args, reply)

	case "Append":

		if ck.currentAssumedLeader != -1 {
			DebugClerk(dAppendKeyClerk, "Sending a Append RPC to Node = %d, args = %+v", ck.currentAssumedLeader, args)

			serverAddr := ck.servers[ck.currentAssumedLeader]
			client, err := rpc.Dial("tcp", serverAddr)
			if err != nil {
				DebugClerk(dRPC, "Error while dialing key-value cluster, server = %s -> %+v", serverAddr, err)
			} else {
				err := client.Call("KVServer.Append", args, &reply)
				if err != nil {
					DebugClerk(dRPC, "Append RPC to server %s failed -> %+v", serverAddr, err)
				} else {
					if reply.Err == OK {
						DebugClerk(dAppendKeyClerk, "Append RPC successful, KVNode-LeaderIndex = %d, args = %+v, reply = %+v", ck.currentAssumedLeader, args, reply)
						return
					}
				}
			}
		}

		// me: send RPCs to all nodes until successful if no current assumed leader or RPC fails
		for {
			for i, server := range ck.servers {
				DebugClerk(dAppendKeyClerk, "Sending a Append RPC to Node = %d, args = %+v", i, args)

				client, err := rpc.Dial("tcp", server)
				if err != nil {
					DebugClerk(dRPC, "Error while dialing key-value cluster, server = %s -> %+v", server, err)
					continue
				} else {
					err := client.Call("KVServer.Append", args, &reply)
					if err != nil {
						DebugClerk(dRPC, "Append RPC to server %s failed -> %+v", server, err)
						continue
					} else {
						if reply.Err == OK {
							ck.currentAssumedLeader = i
							DebugClerk(dAppendKeyClerk, "Append RPC successful, KVNode-LeaderIndex = %d, args = %+v, reply = %+v", ck.currentAssumedLeader, args, reply)
							return
						}
					}
				}
			}
		}

		// DebugClerk(dAppendKeyClerk, "Append RPC failed, Args = %+v, Reply = %+v", args, reply)

	default:

		DebugClerk(dInvalidPutAppendOp, "Invalid Put/Append operation, args = %+v", args)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
