package stream

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/fully-ai/streamforge/internal/broker"
)

func TestStreamDSLWindowAggregationJoinAndCheckpoints(t *testing.T) {
	dir := t.TempDir()
	b, err := broker.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	_ = b.CreateTopic(broker.TopicConfig{Name: "orders", Partitions: 1})
	_ = b.CreateTopic(broker.TopicConfig{Name: "high_orders", Partitions: 1})

	dsl := `FROM orders WHERE amount > 50 WINDOW TUMBLING 2 GROUP BY region AGGREGATE SUM(amount) EMIT TO high_orders`
	spec, err := ParseStreamDSL("high-value", dsl)
	if err != nil {
		t.Fatal(err)
	}

	ckpt := filepath.Join(dir, "stream_state.json")
	eng := NewEngine(b, ckpt)
	eng.RegisterJob(spec)
	eng.PutJoinLookup("ord-1", map[string]any{"tier": "vip"})

	_, _ = b.Produce("orders", []broker.ProduceMessage{
		{Key: []byte("ord-1"), Value: []byte(`{"amount":20,"region":"eu"}`)},  // filtered out
		{Key: []byte("ord-1"), Value: []byte(`{"amount":100,"region":"eu"}`)}, // kept (1/2)
	})
	emitted, err := eng.Step()
	if err != nil || emitted != 0 {
		t.Fatalf("expected 0 window emissions after 1 match, got %d err=%v", emitted, err)
	}

	// Simulate restart: reload Engine from disk checkpoint, then feed 2nd matching event!
	eng2 := NewEngine(b, ckpt)
	eng2.RegisterJob(spec)
	_, _ = b.Produce("orders", []broker.ProduceMessage{
		{Key: []byte("ord-2"), Value: []byte(`{"amount":150,"region":"eu"}`)}, // kept (2/2 -> flush!)
	})
	emitted2, err := eng2.Step()
	if err != nil || emitted2 != 1 {
		t.Fatalf("expected 1 window flush after restart, got %d err=%v", emitted2, err)
	}

	outRecs, _ := b.Fetch("high_orders", 0, 0, 10)
	if len(outRecs) != 1 {
		t.Fatalf("expected 1 emitted record in high_orders, got %d", len(outRecs))
	}
	var agg map[string]any
	_ = json.Unmarshal(outRecs[0].Value, &agg)
	if agg["sum"].(float64) != 250 || agg["count"].(float64) != 2 {
		t.Fatalf("unexpected window aggregate output: %+v", agg)
	}
}

func FuzzParseStreamDSL(f *testing.F) {
	f.Add("FROM a WHERE x > 10 WINDOW TUMBLING 5 GROUP BY k AGGREGATE SUM(x) EMIT TO b")
	f.Add("FROM")
	f.Add("")
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = ParseStreamDSL("j", s)
	})
}
