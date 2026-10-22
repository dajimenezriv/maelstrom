# Gossip Glomers in Go

My solutions to [Gossip Glomers](https://fly.io/dist-sys/), the distributed systems challenges by Fly.io and Kyle Kingsbury. Every challenge runs on [Maelstrom](https://github.com/jepsen-io/maelstrom), a Jepsen-based workbench that simulates a network, injects faults like partitions and latency, and checks the results for consistency.

A single binary handles all the workloads. Each challenge adds a message handler to the same `Node` in [main.go](main.go).

## Challenges

| #     | Challenge                | Workload                    |
| ----- | ------------------------ | --------------------------- |
| 1     | Echo                     | `echo`                      |
| 2     | Unique ID Generation     | `unique-ids`                |
| 3a    | Single-Node Broadcast    | `broadcast`                 |
| 3b    | Multi-Node Broadcast     | `broadcast`                 |
| 3c    | Fault-Tolerant Broadcast | `broadcast` + partitions    |
| 3d/3e | Efficient Broadcast      | `broadcast` + 100ms latency |

## Design

### Echo

Replies with the same `echo` field it received.

### Unique IDs

IDs are `<node-id>-<counter>`, for example `n2-41`. Node IDs are unique across the cluster, and each node increments its own `atomic.Int64`, so no two nodes can produce the same ID. Generating an ID needs no coordination between nodes, so it keeps working during a network partition (`--availability total`).

### Broadcast

- **Deduplication.** Each node keeps a set of the values it has seen. A `broadcast` with a value already in the set is acknowledged and then dropped. Without this, values would bounce around the topology forever.
- **Batched gossip.** New values are not forwarded inline. They go into a `pending` set, and a background goroutine flushes it to the node's neighbours once per second. This uses fewer messages per operation than forwarding every value as soon as it arrives.
- **Retry until acked.** A value stays in `pending` until a neighbour acknowledges it, so a value that was lost during a partition is sent again on the next tick after the partition heals.
- **No echo to the sender.** A value is never sent back to the node it came from.

## Running it

### Prerequisites

- Go (see [go.mod](go.mod) for the version)
- Maelstrom and its dependencies (a JDK, Graphviz and gnuplot). See the [Maelstrom installation guide](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md).

### Build

```bash
go install .
```

This puts the node binary at `~/go/bin/maelstrom`. That is the binary passed to `--bin` below. The `./maelstrom` in the commands is the Maelstrom test runner, run from the directory where you extracted it.

### Tests

```bash
# 1. Echo
./maelstrom test -w echo --bin ~/go/bin/maelstrom --node-count 1 --time-limit 10

# 2. Unique IDs
./maelstrom test -w unique-ids --bin ~/go/bin/maelstrom --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

# 3a. Single-node broadcast
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom --node-count 1 --time-limit 20 --rate 10

# 3b. Multi-node broadcast
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom --node-count 5 --time-limit 20 --rate 10

# 3c. Fault-tolerant broadcast
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom --node-count 5 --time-limit 20 --rate 10 --nemesis partition

# 3d/3e. Efficient broadcast
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom --node-count 25 --time-limit 20 --rate 100 --latency 100
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition
```

To browse the results of past runs (timelines, latency graphs, message counts), start Maelstrom's web UI at http://localhost:8080:

```bash
./maelstrom serve
```

### Unit tests

```bash
go test ./...
```
