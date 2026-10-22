package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type pendingMessage struct {
	from string
}

type Node struct {
	*maelstrom.Node

	genID atomic.Int64

	mu         sync.Mutex
	messages   map[int]struct{}
	pending    map[int]pendingMessage
	neighbours []string
}

func NewNode() *Node {
	return &Node{
		Node:     maelstrom.NewNode(),
		messages: make(map[int]struct{}),
		pending:  make(map[int]pendingMessage),
	}
}

func main() {
	if err := run(NewNode()); err != nil {
		log.Fatal(err)
	}
}

func run(n *Node) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go n.backgroundBroadcast(ctx)

	n.Handle("echo", n.handleEcho)
	n.Handle("generate", n.handleGenerate)
	n.Handle("topology", n.handleTopology)
	n.Handle("broadcast", n.handleBroadcast)
	n.Handle("read", n.handleRead)

	return n.Run()
}

func (n *Node) backgroundBroadcast(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.sendBatch()
		}
	}
}

func (n *Node) sendBatch() {
	n.mu.Lock()
	pending := n.pending
	neighbours := slices.Clone(n.neighbours)
	n.mu.Unlock()

	for value, p := range pending {
		for _, neighbour := range neighbours {
			if neighbour == p.from {
				continue
			}
			body := broadcastRequest{
				Type:    "broadcast",
				Message: value,
			}
			err := n.RPC(neighbour, body, func(msg maelstrom.Message) error {
				n.mu.Lock()
				delete(n.pending, value)
				n.mu.Unlock()
				return nil
			})
			if err != nil {
				log.Printf("gossip %d to %s: %v", value, neighbour, err)
			}
		}
	}
}

type echoRequest struct {
	Echo string `json:"echo"`
}

func (n *Node) handleEcho(msg maelstrom.Message) error {
	var body echoRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return n.Reply(msg, map[string]any{
		"type": "echo_ok",
		"echo": body.Echo,
	})
}

func (n *Node) handleGenerate(msg maelstrom.Message) error {
	return n.Reply(msg, map[string]any{
		"type": "generate_ok",
		"id":   fmt.Sprintf("%s-%d", n.ID(), n.genID.Add(1)),
	})
}

type topologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

func (n *Node) handleTopology(msg maelstrom.Message) error {
	var body topologyRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	n.mu.Lock()
	if neighbours, ok := body.Topology[n.ID()]; ok {
		n.neighbours = neighbours
	}
	n.mu.Unlock()

	return n.Reply(msg, map[string]any{"type": "topology_ok"})
}

type broadcastRequest struct {
	Type    string `json:"type"`
	MsgID   int64  `json:"msg_id"`
	Message int    `json:"message"`
}

func (n *Node) handleBroadcast(msg maelstrom.Message) error {
	var body broadcastRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	n.mu.Lock()
	if _, seen := n.messages[body.Message]; !seen {
		n.messages[body.Message] = struct{}{}
		n.pending[body.Message] = pendingMessage{from: msg.Src}
	}
	n.mu.Unlock()

	return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func (n *Node) handleRead(msg maelstrom.Message) error {
	n.mu.Lock()
	messages := slices.Sorted(maps.Keys(n.messages))
	n.mu.Unlock()

	return n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}
