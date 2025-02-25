# Examples: Distributed key-value store

The key-value store consists of two binaries, a `clerk` and a `node`.

### Node

Each node is a Raft instance which uses the Raft library to manage a distributed key-value store.
This example runs three key-value server nodes.

Each node requires a configuration.

### Clerk

The clerk is used to issue PUT, GET and APPEND commands to the key-value server.
Currently, the binary must be run repeatedly to issue new commands.

<!-- Until the leader among the cluster has been identified, the clerk issues commands sequentially to all nodes. -->
<!-- If the leader changes, the clerk retries RPCs sequentailly until the new leader has been found, and updates its internal state to send future RPCs to the leader directly. -->

The clerk requires a configuration.

## Notes

1. The example configurations for the clerk and the node are stored in the `configs/` directory.

2. Set the `RAFT_DEBUG=1` environment variable to view the Raft logs.
   Set the `KVSERVER_DEBUG=1` environment variable to view the key-value server state machine logs.
   (Setting the `KVSERVER_DEBUG` variable is not recommended as it clutters the console)

## Usage

1. Build the binaries

```sh
make
```

2. Run the k-v server nodes in different terminal sessions

```sh
# Node 1
RAFT_DEBUG=1 ./kv_node configs/nodeConfig_1.json

# Node 2
RAFT_DEBUG=1 ./kv_node configs/nodeConfig_2.json

# Node 3
RAFT_DEBUG=1 ./kv_node configs/nodeConfig_3.json
```

3. Issue commands to the cluster using the clerk in a new terminal session

```sh

# PUT command
./clerk configs/clerkConfig.json PUT foo bar

# GET command
./clerk configs/clerkConfig.json GET foo

# APPEND command
./clerk configs/clerkConfig.json APPEND foo foobar
```

![Image of example 1](images/example_kv_cluster.png)
