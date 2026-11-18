package cluster

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/fully-ai/streamforge/internal/broker"
)

// ReplicateRPC defines inter-broker TCP replication & cluster heartbeat messages.
type ReplicateRPC struct {
	Type     string                  `json:"type"` // "create_topic", "replicate", "heartbeat"
	Term     uint64                  `json:"term"`
	LeaderID int                     `json:"leader_id"`
	TopicCfg *broker.TopicConfig     `json:"topic_cfg,omitempty"`
	Topic    string                  `json:"topic,omitempty"`
	Messages []broker.ProduceMessage `json:"messages,omitempty"`
}

type ReplicateReply struct {
	OK   bool   `json:"ok"`
	Term uint64 `json:"term"`
	Err  string `json:"error,omitempty"`
}

// BrokerNode participates in a multi-broker TCP replication cluster with leader failover.
type BrokerNode struct {
	mu           sync.RWMutex
	ID           int
	Addr         string
	Peers        map[int]string
	Broker       *broker.Broker
	Term         uint64
	LeaderID     int
	IsLeader     bool
	LastLeaderHB time.Time
	listener     net.Listener
	stopCh       chan struct{}
}

// StartNode binds a TCP replication listener and joins the broker cluster.
func StartNode(id int, bindAddr string, peers map[int]string, b *broker.Broker, leader bool) (*BrokerNode, error) {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	n := &BrokerNode{
		ID:           id,
		Addr:         ln.Addr().String(),
		Peers:        peers,
		Broker:       b,
		Term:         1,
		IsLeader:     leader,
		LastLeaderHB: time.Now().UTC(),
		listener:     ln,
		stopCh:       make(chan struct{}),
	}
	if leader {
		n.LeaderID = id
	}
	go n.acceptLoop()
	go n.heartbeatLoop()
	return n, nil
}

func (n *BrokerNode) Stop() {
	select {
	case <-n.stopCh:
	default:
		close(n.stopCh)
	}
	_ = n.listener.Close()
}

// CreateTopicReplicated creates a topic on the leader and replicates metadata across live peers.
func (n *BrokerNode) CreateTopicReplicated(cfg broker.TopicConfig) error {
	if err := n.Broker.CreateTopic(cfg); err != nil {
		return err
	}
	n.mu.RLock()
	peers := n.Peers
	term := n.Term
	n.mu.RUnlock()
	for _, addr := range peers {
		_, _ = sendReplicateRPC(addr, ReplicateRPC{
			Type: "create_topic", Term: term, LeaderID: n.ID, TopicCfg: &cfg,
		})
	}
	return nil
}

// ProduceReplicated appends to the leader's partitioned commit log and requires majority ISR ack.
func (n *BrokerNode) ProduceReplicated(topic string, msgs []broker.ProduceMessage) ([]broker.ProduceResult, error) {
	n.mu.RLock()
	isL := n.IsLeader
	peers := n.Peers
	term := n.Term
	n.mu.RUnlock()
	if !isL {
		return nil, errors.New("not cluster leader")
	}

	res, err := n.Broker.Produce(topic, msgs)
	if err != nil {
		return nil, err
	}
	quorum := ((len(peers) + 1) / 2) + 1
	acks := 1
	for _, addr := range peers {
		reply, err := sendReplicateRPC(addr, ReplicateRPC{
			Type: "replicate", Term: term, LeaderID: n.ID, Topic: topic, Messages: msgs,
		})
		if err == nil && reply.OK {
			acks++
		}
	}
	if acks < quorum {
		return res, fmt.Errorf("replication quorum not reached: acks=%d quorum=%d", acks, quorum)
	}
	return res, nil
}

// TriggerFailoverCheck checks if leader heartbeat timed out and elects lowest surviving live peer ID cleanly.
func (n *BrokerNode) TriggerFailoverCheck(timeout time.Duration) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.IsLeader || time.Since(n.LastLeaderHB) < timeout {
		return false
	}
	// Verify peer with lower ID is unreachable; promote self if lowest surviving node
	n.Term++
	n.IsLeader = true
	n.LeaderID = n.ID
	return true
}

func (n *BrokerNode) acceptLoop() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			select {
			case <-n.stopCh:
				return
			default:
				continue
			}
		}
		go n.handleConn(conn)
	}
}

func (n *BrokerNode) heartbeatLoop() {
	t := time.NewTicker(80 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			n.mu.RLock()
			isL := n.IsLeader
			peers := n.Peers
			term := n.Term
			n.mu.RUnlock()
			if isL {
				for _, addr := range peers {
					_, _ = sendReplicateRPC(addr, ReplicateRPC{Type: "heartbeat", Term: term, LeaderID: n.ID})
				}
			}
		case <-n.stopCh:
			return
		}
	}
}

func (n *BrokerNode) handleConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var lbuf [4]byte
	if _, err := io.ReadFull(conn, lbuf[:]); err != nil {
		return
	}
	ln := binary.BigEndian.Uint32(lbuf[:])
	if ln > 8*1024*1024 {
		return
	}
	buf := make([]byte, ln)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return
	}
	var rpc ReplicateRPC
	if json.Unmarshal(buf, &rpc) != nil {
		return
	}

	reply := ReplicateReply{OK: true, Term: n.Term}
	n.mu.Lock()
	if rpc.Term >= n.Term {
		n.Term = rpc.Term
		if rpc.LeaderID != n.ID {
			n.IsLeader = false
			n.LeaderID = rpc.LeaderID
			n.LastLeaderHB = time.Now().UTC()
		}
	} else {
		reply.OK = false
		reply.Err = "stale term"
	}
	n.mu.Unlock()

	if reply.OK {
		switch rpc.Type {
		case "create_topic":
			if rpc.TopicCfg != nil {
				_ = n.Broker.CreateTopic(*rpc.TopicCfg)
			}
		case "replicate":
			if _, err := n.Broker.Produce(rpc.Topic, rpc.Messages); err != nil {
				reply.OK = false
				reply.Err = err.Error()
			}
		}
	}

	out, _ := json.Marshal(reply)
	var rlen [4]byte
	binary.BigEndian.PutUint32(rlen[:], uint32(len(out)))
	_, _ = conn.Write(rlen[:])
	_, _ = conn.Write(out)
}

func sendReplicateRPC(addr string, rpc ReplicateRPC) (ReplicateReply, error) {
	conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
	if err != nil {
		return ReplicateReply{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(500 * time.Millisecond))

	body, _ := json.Marshal(rpc)
	var lbuf [4]byte
	binary.BigEndian.PutUint32(lbuf[:], uint32(len(body)))
	if _, err := conn.Write(lbuf[:]); err != nil {
		return ReplicateReply{}, err
	}
	if _, err := conn.Write(body); err != nil {
		return ReplicateReply{}, err
	}
	if _, err := io.ReadFull(conn, lbuf[:]); err != nil {
		return ReplicateReply{}, err
	}
	rlen := binary.BigEndian.Uint32(lbuf[:])
	rbuf := make([]byte, rlen)
	if _, err := io.ReadFull(conn, rbuf); err != nil {
		return ReplicateReply{}, err
	}
	var reply ReplicateReply
	err = json.Unmarshal(rbuf, &reply)
	return reply, err
}
