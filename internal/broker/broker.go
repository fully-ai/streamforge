package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fully-ai/streamforge/internal/logstore"
	"github.com/fully-ai/streamforge/internal/registry"
)

var (
	ErrTopicNotFound = errors.New("topic not found")
	ErrBackpressure  = errors.New("partition write queue saturated")
	validNameRe      = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

// TopicConfig defines partition count, retention, and segment sizing for a topic.
type TopicConfig struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	RetentionBytes    int64  `json:"retention_bytes"`
	MaxSegmentBytes   int64  `json:"max_segment_bytes"`
	Compress          bool   `json:"compress"`
	MaxInFlight       int    `json:"max_in_flight"`
}

// TopicInfo exposes live offsets across partitions for monitoring & CLI.
type TopicInfo struct {
	Config     TopicConfig    `json:"config"`
	Partitions map[int]uint64 `json:"partition_leo"`
	Segments   map[int]int    `json:"partition_segments"`
}

// GroupState tracks consumer group members, partition assignments, and durable offsets.
type GroupState struct {
	GroupID     string               `json:"group_id"`
	Topic       string               `json:"topic"`
	Offsets     map[int]uint64       `json:"offsets"`
	Members     map[string]time.Time `json:"members"`
	Assignments map[string][]int     `json:"assignments"`
}

// Broker coordinates topics, partitions, schema validation, consumer groups, and DLQs.
type Broker struct {
	mu           sync.RWMutex
	dir          string
	topics       map[string]*topicRuntime
	groups       map[string]*GroupState
	schemas      *registry.Registry
	dlqRetries   map[string]int
	rrCounter    atomic.Uint64
	producedTot  atomic.Uint64
	consumedTot  atomic.Uint64
	dlqRoutedTot atomic.Uint64
}

type topicRuntime struct {
	cfg        TopicConfig
	partitions []*logstore.PartitionLog
	inflight   atomic.Int64
}

// Open initializes or recovers a StreamForge Broker directory.
func Open(dir string) (*Broker, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	reg, err := registry.Open(filepath.Join(dir, "schemas.json"))
	if err != nil {
		return nil, err
	}
	b := &Broker{
		dir:        dir,
		topics:     make(map[string]*topicRuntime),
		groups:     make(map[string]*GroupState),
		schemas:    reg,
		dlqRetries: make(map[string]int),
	}
	if raw, err := os.ReadFile(filepath.Join(dir, "topics.json")); err == nil {
		var cfgs []TopicConfig
		if json.Unmarshal(raw, &cfgs) == nil {
			for _, c := range cfgs {
				_ = b.openTopicRuntime(c)
			}
		}
	}
	if raw, err := os.ReadFile(filepath.Join(dir, "offsets.json")); err == nil {
		_ = json.Unmarshal(raw, &b.groups)
	}
	return b, nil
}

func ValidateIdentifier(name string) error {
	if !validNameRe.MatchString(name) || name == "." || name == ".." {
		return fmt.Errorf("invalid identifier %q", name)
	}
	return nil
}

// CreateTopic provisions partition commit logs and an automatic `<topic>.dlq` topic when not already a DLQ.
func (b *Broker) CreateTopic(cfg TopicConfig) error {
	if err := ValidateIdentifier(cfg.Name); err != nil {
		return err
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 3
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 1
	}
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = 1024 * 1024
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 2048
	}

	b.mu.Lock()
	if _, exists := b.topics[cfg.Name]; exists {
		b.mu.Unlock()
		return fmt.Errorf("topic %q already exists", cfg.Name)
	}
	if err := b.openTopicRuntime(cfg); err != nil {
		b.mu.Unlock()
		return err
	}
	b.saveTopicsLocked()
	b.mu.Unlock()

	if !isDLQ(cfg.Name) {
		dlqCfg := cfg
		dlqCfg.Name = cfg.Name + ".dlq"
		dlqCfg.Partitions = 1
		_ = b.CreateTopic(dlqCfg)
	}
	return nil
}

func isDLQ(name string) bool {
	return len(name) > 4 && name[len(name)-4:] == ".dlq"
}

func (b *Broker) openTopicRuntime(cfg TopicConfig) error {
	parts := make([]*logstore.PartitionLog, 0, cfg.Partitions)
	for p := 0; p < cfg.Partitions; p++ {
		pdir := filepath.Join(b.dir, "topics", cfg.Name, fmt.Sprintf("part-%d", p))
		pl, err := logstore.Open(pdir, logstore.Config{
			MaxSegmentBytes: cfg.MaxSegmentBytes,
			SyncOnAppend:    false,
			CompressBatch:   cfg.Compress,
		})
		if err != nil {
			return err
		}
		parts = append(parts, pl)
	}
	b.topics[cfg.Name] = &topicRuntime{cfg: cfg, partitions: parts}
	return nil
}

// Schemas exposes the broker's integrated Schema Registry.
func (b *Broker) Schemas() *registry.Registry {
	return b.schemas
}

// ProduceMessage carries an input record for publishing.
type ProduceMessage struct {
	Partition *int              `json:"partition,omitempty"`
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// ProduceResult reports partition & assigned log offsets.
type ProduceResult struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	Offsets   []uint64 `json:"offsets"`
}

// Produce validates schemas, enforces bounded backpressure, chooses partition via FNV-1a key hash, and appends durably.
func (b *Broker) Produce(topic string, msgs []ProduceMessage) ([]ProduceResult, error) {
	b.mu.RLock()
	tr, ok := b.topics[topic]
	b.mu.RUnlock()
	if !ok {
		return nil, ErrTopicNotFound
	}
	cur := tr.inflight.Add(int64(len(msgs)))
	defer tr.inflight.Add(-int64(len(msgs)))
	if cur > int64(tr.cfg.MaxInFlight) {
		return nil, ErrBackpressure
	}

	byPart := make(map[int][]logstore.Record)
	for _, m := range msgs {
		if err := b.schemas.ValidatePayload(topic, m.Value); err != nil {
			return nil, err
		}
		pid := b.choosePartition(len(tr.partitions), m.Partition, m.Key)
		byPart[pid] = append(byPart[pid], logstore.Record{
			Key:     m.Key,
			Value:   m.Value,
			Headers: m.Headers,
		})
	}

	var results []ProduceResult
	for pid, batch := range byPart {
		offs, err := tr.partitions[pid].Append(batch)
		if err != nil {
			return results, err
		}
		b.producedTot.Add(uint64(len(offs)))
		if tr.cfg.RetentionBytes > 0 {
			_, _ = tr.partitions[pid].EnforceRetention(tr.cfg.RetentionBytes)
		}
		results = append(results, ProduceResult{
			Topic:     topic,
			Partition: pid,
			Offsets:   offs,
		})
	}
	return results, nil
}

func (b *Broker) choosePartition(numParts int, explicit *int, key []byte) int {
	if explicit != nil && *explicit >= 0 && *explicit < numParts {
		return *explicit
	}
	if len(key) > 0 {
		h := fnv.New32a()
		_, _ = h.Write(key)
		return int(h.Sum32()) % numParts
	}
	n := b.rrCounter.Add(1)
	return int(n % uint64(numParts))
}

// Fetch reads records from a specific topic partition starting at offset.
func (b *Broker) Fetch(topic string, partition int, offset uint64, max int) ([]logstore.Record, error) {
	b.mu.RLock()
	tr, ok := b.topics[topic]
	b.mu.RUnlock()
	if !ok {
		return nil, ErrTopicNotFound
	}
	if partition < 0 || partition >= len(tr.partitions) {
		return nil, fmt.Errorf("invalid partition %d", partition)
	}
	recs, err := tr.partitions[partition].Read(offset, max)
	if err == nil {
		b.consumedTot.Add(uint64(len(recs)))
	}
	return recs, err
}

// CompactTopic runs key compaction on closed segments across all partitions.
func (b *Broker) CompactTopic(topic string) (int, error) {
	b.mu.RLock()
	tr, ok := b.topics[topic]
	b.mu.RUnlock()
	if !ok {
		return 0, ErrTopicNotFound
	}
	total := 0
	for _, p := range tr.partitions {
		n, err := p.CompactKeys()
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// JoinConsumerGroup registers or refreshes a consumer member and rebalances partitions deterministically.
func (b *Broker) JoinConsumerGroup(groupID, topic, memberID string) (GroupState, error) {
	if err := ValidateIdentifier(groupID); err != nil {
		return GroupState{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	tr, ok := b.topics[topic]
	if !ok {
		return GroupState{}, ErrTopicNotFound
	}
	gs, exists := b.groups[groupID]
	if !exists {
		gs = &GroupState{
			GroupID:     groupID,
			Topic:       topic,
			Offsets:     make(map[int]uint64),
			Members:     make(map[string]time.Time),
			Assignments: make(map[string][]int),
		}
		b.groups[groupID] = gs
	}
	now := time.Now().UTC()
	gs.Members[memberID] = now
	// Evict stale members older than 30s
	for m, seen := range gs.Members {
		if now.Sub(seen) > 30*time.Second {
			delete(gs.Members, m)
		}
	}
	rebalanceGroup(gs, len(tr.partitions))
	b.saveOffsetsLocked()
	return cloneGroup(gs), nil
}

// LeaveConsumerGroup removes a member and rebalances remaining consumers cleanly.
func (b *Broker) LeaveConsumerGroup(groupID, memberID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	gs, ok := b.groups[groupID]
	if !ok {
		return nil
	}
	delete(gs.Members, memberID)
	if tr, ok := b.topics[gs.Topic]; ok {
		rebalanceGroup(gs, len(tr.partitions))
	}
	b.saveOffsetsLocked()
	return nil
}

func rebalanceGroup(gs *GroupState, numPartitions int) {
	gs.Assignments = make(map[string][]int)
	if len(gs.Members) == 0 {
		return
	}
	var members []string
	for m := range gs.Members {
		members = append(members, m)
	}
	sort.Strings(members)
	for p := 0; p < numPartitions; p++ {
		owner := members[p%len(members)]
		gs.Assignments[owner] = append(gs.Assignments[owner], p)
	}
}

// CommitOffset durably stores committed consumer group partition offsets.
func (b *Broker) CommitOffset(groupID, topic string, partition int, nextOffset uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	gs, exists := b.groups[groupID]
	if !exists {
		gs = &GroupState{
			GroupID:     groupID,
			Topic:       topic,
			Offsets:     make(map[int]uint64),
			Members:     make(map[string]time.Time),
			Assignments: make(map[string][]int),
		}
		b.groups[groupID] = gs
	}
	gs.Offsets[partition] = nextOffset
	b.saveOffsetsLocked()
	return nil
}

// SendToDLQ routes failed consumer messages to `<topic>.dlq` with failure metadata.
func (b *Broker) SendToDLQ(topic string, rec logstore.Record, reason string) error {
	hdrs := map[string]string{"dlq_source_topic": topic, "dlq_reason": reason}
	for k, v := range rec.Headers {
		hdrs[k] = v
	}
	_, err := b.Produce(topic+".dlq", []ProduceMessage{{
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: hdrs,
	}})
	if err == nil {
		b.dlqRoutedTot.Add(1)
	}
	return err
}

// GroupLag calculates total unconsumed records across all partitions for groupID.
func (b *Broker) GroupLag(groupID string) (map[int]uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	gs, ok := b.groups[groupID]
	if !ok {
		return nil, fmt.Errorf("group %q not found", groupID)
	}
	tr, ok := b.topics[gs.Topic]
	if !ok {
		return nil, ErrTopicNotFound
	}
	lag := make(map[int]uint64, len(tr.partitions))
	for pid, pl := range tr.partitions {
		leo := pl.LogEndOffset()
		committed := gs.Offsets[pid]
		if leo > committed {
			lag[pid] = leo - committed
		} else {
			lag[pid] = 0
		}
	}
	return lag, nil
}

// ListTopics returns live metadata and LEO per topic partition.
func (b *Broker) ListTopics() []TopicInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]TopicInfo, 0, len(b.topics))
	for _, tr := range b.topics {
		leos := make(map[int]uint64, len(tr.partitions))
		segs := make(map[int]int, len(tr.partitions))
		for pid, p := range tr.partitions {
			leos[pid] = p.LogEndOffset()
			segs[pid] = p.SegmentCount()
		}
		out = append(out, TopicInfo{Config: tr.cfg, Partitions: leos, Segments: segs})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Config.Name < out[j].Config.Name })
	return out
}

// ListGroups returns consumer groups and committed offsets.
func (b *Broker) ListGroups() []GroupState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]GroupState, 0, len(b.groups))
	for _, g := range b.groups {
		out = append(out, cloneGroup(g))
	}
	return out
}

// BrokerMetrics summarizes global produce/consume/DLQ counts.
type BrokerMetrics struct {
	Topics        int    `json:"topics"`
	Groups        int    `json:"groups"`
	ProducedTotal uint64 `json:"produced_total"`
	ConsumedTotal uint64 `json:"consumed_total"`
	DLQRouted     uint64 `json:"dlq_routed_total"`
}

func (b *Broker) Metrics() BrokerMetrics {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return BrokerMetrics{
		Topics:        len(b.topics),
		Groups:        len(b.groups),
		ProducedTotal: b.producedTot.Load(),
		ConsumedTotal: b.consumedTot.Load(),
		DLQRouted:     b.dlqRoutedTot.Load(),
	}
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, tr := range b.topics {
		for _, p := range tr.partitions {
			_ = p.Close()
		}
	}
	return nil
}

func (b *Broker) saveTopicsLocked() {
	var cfgs []TopicConfig
	for _, tr := range b.topics {
		cfgs = append(cfgs, tr.cfg)
	}
	raw, err := json.MarshalIndent(cfgs, "", "  ")
	if err == nil {
		tmp := filepath.Join(b.dir, "topics.json.tmp")
		if os.WriteFile(tmp, raw, 0o640) == nil {
			_ = os.Rename(tmp, filepath.Join(b.dir, "topics.json"))
		}
	}
}

func (b *Broker) saveOffsetsLocked() {
	raw, err := json.MarshalIndent(b.groups, "", "  ")
	if err == nil {
		tmp := filepath.Join(b.dir, "offsets.json.tmp")
		if os.WriteFile(tmp, raw, 0o640) == nil {
			_ = os.Rename(tmp, filepath.Join(b.dir, "offsets.json"))
		}
	}
}

func cloneGroup(g *GroupState) GroupState {
	cp := *g
	cp.Offsets = make(map[int]uint64, len(g.Offsets))
	for k, v := range g.Offsets {
		cp.Offsets[k] = v
	}
	cp.Members = make(map[string]time.Time, len(g.Members))
	for k, v := range g.Members {
		cp.Members[k] = v
	}
	cp.Assignments = make(map[string][]int, len(g.Assignments))
	for k, v := range g.Assignments {
		cp.Assignments[k] = append([]int(nil), v...)
	}
	return cp
}
