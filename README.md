# Raft

A raft library written in Go, adapted from my solutions to MIT's Distributed Systems Labs ([Link](https://github.com/anirudhsudhir/mit_dist_sys_labs)).

This library utilises the RPC package from Go's standard library ([net/rpc](https://pkg.go.dev/net/rpc)).

## Usage

To use the library, refer to the distrbuted key-value store implementation in the `examples` directory ([examples/kv_cluster/README.md](examples/kv_cluster/README.md)).

Note: This library can replicate logs of any type across a cluster. To use a custom type in the Raft log, define it in the `raft_cmd_types` package and include it in `raft_cmd_types/TypeRegistry`.

### Distributed key-value store implemented using the library

![Distributed key-value store](examples/kv_cluster/images/example_kv_cluster.png)

Note: Deserialising and committing the previously persisted Raft log entries to
the state machine upon node restart might take a few seconds. An immediate `GET`
request to the store might yield `KEY NOT FOUND` if a key is requested
immediately upon the cluster restart.
