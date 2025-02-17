package main

import (
	"encoding/json"
	"log"
	"net"
	"net/rpc"
	"os"

	"github.com/anirudhsudhir/raft"
)

type Config struct {
	NodeAddr []string `json:"nodeAddrs"`

	// Index of the port of the current node in NodeAddr
	NodeIndex int `json:"nodeIndex"`

	CurrentNodePort string `json:"currentNodePort"`
}

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		log.Println("Invalid usage!")
		log.Fatal("Usage: go run . [path_to_config.json]\n")
	}

	config := parseConfig(os.Args[1])
	log.Printf("Parsed config -> %+v\n", config)

	applyCh := make(chan raft.ApplyMsg)
	raftObj := raft.Make(config.NodeAddr, config.NodeIndex, raft.MakePersister(), applyCh)

	go func() {
		for range applyCh {
		}
	}()

	config.initRPCHandlers(raftObj)
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

func (c *Config) initRPCHandlers(raftObj *raft.Raft) {
	log.Println("Starting to init RPC handlers")

	if err := rpc.Register(raftObj); err != nil {
		log.Fatalf("Failed to register RPC Service -> %v\n", err)
	}

	listener, err := net.Listen("tcp", ":"+c.CurrentNodePort)
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
