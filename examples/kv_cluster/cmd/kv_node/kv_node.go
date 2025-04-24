package main

import (
	"encoding/json"
	"log"
	"net"
	"net/rpc"
	"os"

	"github.com/anirudhsudhir/raft"
	"github.com/anirudhsudhir/raft/examples/kv_cluster/common"
)

type Config struct {
	RaftNodeAddr           []string `json:"raft_nodeAddrs"`
	RaftNodeIndex          int      `json:"raft_nodeIndex"`
	RaftCurrentNodePort    string   `json:"raft_currentNodePort"`
	RaftPersistedStatePath string   `json:"raft_persistedStatePath"`
	KVNodeAddr             []string `json:"kv_nodeAddrs"`
	KVNodeIndex            int      `json:"kv_nodeIndex"`
	KVCurrentNodePort      string   `json:"kv_currentNodePort"`
}

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		log.Println("Invalid usage!")
		log.Fatal("Usage: go run cmd/kv_node [path_to_config.json]\n")
	}

	config := parseConfig(os.Args[1])
	log.Printf("Parsed config -> %+v\n", config)

	raftPersister := raft.MakePersister()
	kvServer := common.StartKVServer(config.RaftNodeAddr, config.RaftNodeIndex, raftPersister, 0, config.RaftCurrentNodePort, config.RaftPersistedStatePath, config.KVNodeAddr, config.KVNodeIndex)
	config.initRPCHandlers(kvServer)
}

func parseConfig(configFile string) *Config {
	body, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to parse config %s -> %v\n", configFile, err)
	}

	config := &Config{}
	err = json.Unmarshal(body, config)
	if err != nil {
		log.Fatalf("Failed to unmarshal config %s to JSON -> %v\n", configFile, err)
	}

	return config
}

func (c *Config) initRPCHandlers(kvServer *common.KVServer) {
	log.Println("Starting to init RPC handlers")

	if err := rpc.Register(kvServer); err != nil {
		log.Fatalf("Failed to register RPC Service -> %v\n", err)
	}

	listener, err := net.Listen("tcp", ":"+c.KVCurrentNodePort)
	if err != nil {
		log.Fatalf("Failed to start RPC server listener -> %v\n", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Error while listening to RPC requests -> %v\n", err)
		}

		go rpc.ServeConn(conn)
	}
}
