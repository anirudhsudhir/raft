package raft

import (
	"io"
	"log"
	"os"
	"path"
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
}

func MakePersister() *Persister {
	ps := new(Persister)
	return ps
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) ReadRaftState(persistedStatePath string) []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	file, err := os.OpenFile(persistedStatePath, os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(path.Dir(persistedStatePath), 0750); err != nil {
				log.Fatalf("Failed to create directory to persist Raft state -> %+v", err)
			}
			if file, err = os.Create(persistedStatePath); err != nil {
				log.Fatalf("Failed to create file to persist Raft state -> %+v", err)
			}

		} else {
			log.Fatalf("FAILED to open file to read Raft state from disk, path = %q, err = %+v", persistedStatePath, err)
		}
	}

	ps.raftstate, err = io.ReadAll(file)
	if err != nil {
		log.Fatalf("FAILED to read Raft state from disk for persistence, path = %q, err = %+v", persistedStatePath, err)
	}
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *Persister) Save(raftstate []byte, persistedStatePath string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	file, err := os.OpenFile(persistedStatePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("FAILED to open file to persist Raft state to disk, path = %q, err = %+v", persistedStatePath, err)
	}

	_, err = file.Write(ps.raftstate)
	if err != nil {
		log.Fatalf("FAILED to write Raft state to disk for persistence, path = %q, err = %+v", persistedStatePath, err)
	}
}
