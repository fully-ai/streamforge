package cluster_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fully-ai/streamforge/internal/api"
	"github.com/fully-ai/streamforge/internal/broker"
	"github.com/fully-ai/streamforge/internal/cluster"
	"github.com/fully-ai/streamforge/internal/connect"
	"github.com/fully-ai/streamforge/internal/protocol"
	"github.com/fully-ai/streamforge/internal/stream"
	"github.com/fully-ai/streamforge/pkg/client"
)

func reservePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestMultiBrokerReplicationFailoverConnectorsAndWire(t *testing.T) {
	a1 := reservePort(t)
	a2 := reservePort(t)
	a3 := reservePort(t)

	b1, _ := broker.Open(t.TempDir())
	b2, _ := broker.Open(t.TempDir())
	b3, _ := broker.Open(t.TempDir())
	defer b1.Close()
	defer b2.Close()
	defer b3.Close()

	n1, err := cluster.StartNode(1, a1, map[int]string{2: a2, 3: a3}, b1, true)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := cluster.StartNode(2, a2, map[int]string{1: a1, 3: a3}, b2, false)
	if err != nil {
		t.Fatal(err)
	}
	n3, err := cluster.StartNode(3, a3, map[int]string{1: a1, 2: a2}, b3, false)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Stop()
	defer n3.Stop()

	if err := n1.CreateTopicReplicated(broker.TopicConfig{Name: "events", Partitions: 1, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := n1.ProduceReplicated("events", []broker.ProduceMessage{{Key: []byte("k"), Value: []byte(`{"ok":true}`)}}); err != nil {
		t.Fatal(err)
	}
	recs2, _ := b2.Fetch("events", 0, 0, 10)
	recs3, _ := b3.Fetch("events", 0, 0, 10)
	if len(recs2) != 1 || len(recs3) != 1 {
		t.Fatalf("expected follower replication count=1 on both followers, got b2=%d b3=%d", len(recs2), len(recs3))
	}

	// Simulate leader failure & follower failover promotion
	n1.Stop()
	time.Sleep(120 * time.Millisecond)
	if !n2.TriggerFailoverCheck(80 * time.Millisecond) {
		t.Fatal("expected n2 to assume leadership after heartbeat timeout")
	}
	if _, err := n2.ProduceReplicated("events", []broker.ProduceMessage{{Key: []byte("k2"), Value: []byte(`{"failover":true}`)}}); err != nil {
		t.Fatalf("post-failover ProduceReplicated: %v", err)
	}

	// Connectors E2E: FileTailSource -> Topic -> FileSink & Wire client + HTTP UI
	dir := t.TempDir()
	srcFile := filepath.Join(dir, "in.ndjson")
	dstFile := filepath.Join(dir, "out.ndjson")
	_ = os.WriteFile(srcFile, []byte("{\"msg\":\"hello\"}\n"), 0o640)

	fsrc := connect.NewFileTailSource("src-1", srcFile, "events", b2)
	n, err := fsrc.PollOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("FileTailSource: err=%v n=%d", err, n)
	}
	fsink := connect.NewFileSink("sink-1", "events", "cg-sink", dstFile, b2)
	if n, err := fsink.PollOnce(context.Background()); err != nil || n < 1 {
		t.Fatalf("FileSink: err=%v n=%d", err, n)
	}
	outBytes, _ := os.ReadFile(dstFile)
	if len(outBytes) == 0 {
		t.Fatal("expected output written by FileSink")
	}

	// Binary Wire Protocol Client SDK E2E
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	stopCh := make(chan struct{})
	defer close(stopCh)
	defer ln.Close()
	go protocol.ServeWire(ln, b2, "wire-tok", stopCh)

	cli, err := client.Dial(ln.Addr().String(), "wire-tok", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if err := cli.CreateTopic(broker.TopicConfig{Name: "wire_topic", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Produce("wire_topic", []byte("key1"), []byte("hello-wire")); err != nil {
		t.Fatal(err)
	}

	// Embedded Web Dashboard + HTTP Health check
	st := stream.NewEngine(b2, "")
	ts := httptest.NewServer(api.NewServer(b2, st, ""))
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("expected Web UI 200, err=%v resp=%v", err, resp)
	}
	resp.Body.Close()
}
