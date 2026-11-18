package broker

import (
	"errors"
	"fmt"
	"testing"

	"github.com/fully-ai/streamforge/internal/registry"
)

func TestBrokerOrderingRebalancePersistenceSchemasAndDLQ(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := b.CreateTopic(TopicConfig{Name: "orders", Partitions: 4}); err != nil {
		t.Fatal(err)
	}
	// Register v1 schema and verify incompatible evolution rejection
	_, err = b.Schemas().Register(registry.Schema{
		Subject:       "orders",
		Compatibility: registry.CompatBackward,
		Fields:        map[string]string{"id": "number", "region": "string"},
		Required:      []string{"id", "region"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Adding a NEW required field violates BACKWARD compatibility
	if _, err := b.Schemas().Register(registry.Schema{
		Subject:       "orders",
		Compatibility: registry.CompatBackward,
		Fields:        map[string]string{"id": "number", "region": "string", "currency": "string"},
		Required:      []string{"id", "region", "currency"},
	}); err == nil {
		t.Fatal("expected BACKWARD compatibility error")
	}

	// Produce invalid schema payload -> rejected
	if _, err := b.Produce("orders", []ProduceMessage{{Key: []byte("acct-1"), Value: []byte(`{"id":1}`)}}); err == nil {
		t.Fatal("expected schema validation failure for missing region")
	}

	// Key ordering: all messages with key "acct-1" land in the same partition in FIFO order
	var part0 int
	for i := 0; i < 5; i++ {
		res, err := b.Produce("orders", []ProduceMessage{{
			Key:   []byte("acct-1"),
			Value: []byte(fmt.Sprintf(`{"id":%d,"region":"eu"}`, i)),
		}})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			part0 = res[0].Partition
		} else if res[0].Partition != part0 {
			t.Fatalf("key partition mismatch: %d vs %d", res[0].Partition, part0)
		}
	}

	// Consumer group rebalancing
	g1, err := b.JoinConsumerGroup("cg-billing", "orders", "c1")
	if err != nil || len(g1.Assignments["c1"]) != 4 {
		t.Fatalf("expected c1 to own all 4 partitions, got %+v err=%v", g1, err)
	}
	g2, err := b.JoinConsumerGroup("cg-billing", "orders", "c2")
	if err != nil || len(g2.Assignments["c1"]) != 2 || len(g2.Assignments["c2"]) != 2 {
		t.Fatalf("expected 2/2 split after rebalance, got %+v err=%v", g2.Assignments, err)
	}

	if err := b.CommitOffset("cg-billing", "orders", part0, 3); err != nil {
		t.Fatal(err)
	}
	lag, err := b.GroupLag("cg-billing")
	if err != nil || lag[part0] != 2 {
		t.Fatalf("expected lag=2 on partition %d, got %+v err=%v", part0, lag, err)
	}

	// DLQ routing
	recs, _ := b.Fetch("orders", part0, 0, 1)
	if err := b.SendToDLQ("orders", recs[0], "bad downstream"); err != nil {
		t.Fatal(err)
	}
	dlqRecs, _ := b.Fetch("orders.dlq", 0, 0, 10)
	if len(dlqRecs) != 1 || dlqRecs[0].Headers["dlq_reason"] != "bad downstream" {
		t.Fatalf("unexpected dlq records: %+v", dlqRecs)
	}

	// Restart recovery of offsets and topics
	_ = b.Close()
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	lag2, _ := b2.GroupLag("cg-billing")
	if lag2[part0] != 2 {
		t.Fatalf("expected committed offsets to survive broker restart, got lag=%+v", lag2)
	}

	// Verify backpressure check
	_ = b2.CreateTopic(TopicConfig{Name: "tight", Partitions: 1, MaxInFlight: 1})
	_, err = b2.Produce("tight", []ProduceMessage{{Value: []byte("1")}, {Value: []byte("2")}})
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("expected ErrBackpressure when batch exceeds MaxInFlight=1, got %v", err)
	}
}
