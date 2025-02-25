package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/anirudhsudhir/raft/examples/kv_cluster/common"
)

type ClerkConfig struct {
	Servers []string `json:"nodeAddrs"`
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: go run cmd/clerk [path_to_clerk_config] [operation(GET/PUT/APPEND)]")
	}

	cfg := parseConfig(os.Args[1])
	log.Printf("Parsed config is = %+v\n", cfg)

	clerk := common.MakeClerk(cfg.Servers)

	switch os.Args[2] {
	case "GET":
		if len(os.Args) < 4 {
			log.Fatalf("Need three arguments!")
		}
		val := clerk.Get(os.Args[3])
		if val != "" {
			log.Printf("Result of GET %s -> %s\n", os.Args[3], val)
		} else {
			log.Printf("Result of GET %s -> Key not present!\n", os.Args[3])
		}
	case "PUT":
		if len(os.Args) < 5 {
			log.Fatalf("Need four arguments!")
		}
		clerk.Put(os.Args[3], os.Args[4])
	case "APPEND":
		if len(os.Args) < 5 {
			log.Fatalf("Need four arguments!")
		}
		clerk.Append(os.Args[3], os.Args[4])
	default:
		log.Fatalf("Invalid operation!")
	}
}

func parseConfig(configPath string) ClerkConfig {
	body, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Failed to read config -> %v", err)
	}

	cfg := ClerkConfig{}

	err = json.Unmarshal(body, &cfg)
	if err != nil {
		log.Fatalf("Failed to unmarshal config -> %v", err)
	}

	return cfg
}
