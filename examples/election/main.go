package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/anirudhsudhir/raft"
)

type Config struct {
	NodeAddr []string `json:"nodeAddrs"`

	// Index of the port of the current node in NodeAddr
	NodeIndex int `json:"nodeIndex"`

	CurrentNodePort string `json:"currentNodePort"`

	PersistedStatePath string `json:"persistedStatePath"`
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
	_ = raft.Make(config.NodeAddr, config.NodeIndex, raft.MakePersister(), applyCh, config.CurrentNodePort, config.PersistedStatePath)

	func() {
		for range applyCh {
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
	// config.initRPCHandlers(raftObj)
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
