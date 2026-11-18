package stream

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fully-ai/streamforge/internal/broker"
	"github.com/fully-ai/streamforge/internal/logstore"
)

// QuerySpec defines a parsed StreamForge Stream DSL pipeline.
type QuerySpec struct {
	Name         string `json:"name"`
	SourceTopic  string `json:"source_topic"`
	EmitTopic    string `json:"emit_topic"`
	FilterField  string `json:"filter_field,omitempty"`
	FilterOp     string `json:"filter_op,omitempty"`
	FilterVal    string `json:"filter_val,omitempty"`
	WindowSize   int    `json:"window_size"` // number of matching events per tumbling batch (0 = pass-through)
	GroupByField string `json:"group_by_field,omitempty"`
	AggFunc      string `json:"agg_func,omitempty"` // COUNT or SUM
	AggField     string `json:"agg_field,omitempty"`
}

// ParseStreamDSL parses queries of the form:
// FROM orders WHERE amount > 100 WINDOW TUMBLING 5 GROUP BY region AGGREGATE SUM(amount) EMIT TO high_orders
func ParseStreamDSL(name, dsl string) (QuerySpec, error) {
	tokens := strings.Fields(strings.TrimSpace(dsl))
	if len(tokens) < 5 {
		return QuerySpec{}, fmt.Errorf("incomplete stream DSL: %q", dsl)
	}
	spec := QuerySpec{Name: name}
	i := 0
	for i < len(tokens) {
		up := strings.ToUpper(tokens[i])
		switch up {
		case "FROM":
			if i+1 >= len(tokens) {
				return spec, fmt.Errorf("expected source topic after FROM")
			}
			spec.SourceTopic = tokens[i+1]
			i += 2
		case "WHERE":
			if i+3 >= len(tokens) {
				return spec, fmt.Errorf("expected field op value after WHERE")
			}
			spec.FilterField = tokens[i+1]
			spec.FilterOp = tokens[i+2]
			spec.FilterVal = strings.Trim(tokens[i+3], `"'`)
			i += 4
		case "WINDOW":
			if i+2 >= len(tokens) || !strings.EqualFold(tokens[i+1], "TUMBLING") {
				return spec, fmt.Errorf("expected WINDOW TUMBLING <count>")
			}
			n, err := strconv.Atoi(strings.TrimSuffix(tokens[i+2], "s"))
			if err != nil || n <= 0 {
				return spec, fmt.Errorf("invalid window count %q", tokens[i+2])
			}
			spec.WindowSize = n
			i += 3
		case "GROUP":
			if i+2 >= len(tokens) || !strings.EqualFold(tokens[i+1], "BY") {
				return spec, fmt.Errorf("expected GROUP BY <field>")
			}
			spec.GroupByField = tokens[i+2]
			i += 3
		case "AGGREGATE":
			if i+1 >= len(tokens) {
				return spec, fmt.Errorf("expected aggregate function")
			}
			expr := tokens[i+1]
			if strings.HasPrefix(strings.ToUpper(expr), "COUNT(") {
				spec.AggFunc = "COUNT"
			} else if strings.HasPrefix(strings.ToUpper(expr), "SUM(") && strings.HasSuffix(expr, ")") {
				spec.AggFunc = "SUM"
				spec.AggField = expr[4 : len(expr)-1]
			} else {
				return spec, fmt.Errorf("unsupported aggregate %q", expr)
			}
			i += 2
		case "EMIT":
			if i+2 >= len(tokens) || !strings.EqualFold(tokens[i+1], "TO") {
				return spec, fmt.Errorf("expected EMIT TO <topic>")
			}
			spec.EmitTopic = tokens[i+2]
			i += 3
		default:
			return spec, fmt.Errorf("unexpected DSL token %q", tokens[i])
		}
	}
	if spec.SourceTopic == "" || spec.EmitTopic == "" {
		return spec, fmt.Errorf("stream DSL requires FROM <src> and EMIT TO <dst>")
	}
	return spec, nil
}

// CheckpointState stores durable partition offsets and active window accumulators.
type CheckpointState struct {
	Offsets      map[int]uint64     `json:"offsets"`
	WindowCounts map[string]int     `json:"window_counts"`
	WindowSums   map[string]float64 `json:"window_sums"`
	ProcessedInW int                `json:"processed_in_window"`
}

// Engine executes stateful Stream DSL pipelines with durable state checkpoints.
type Engine struct {
	mu             sync.Mutex
	broker         *broker.Broker
	checkpointPath string
	jobs           map[string]QuerySpec
	states         map[string]*CheckpointState
	lookupTable    map[string]map[string]any // optional stream-table enrichment join
}

// NewEngine creates a stateful stream processor backed by disk checkpoints.
func NewEngine(b *broker.Broker, checkpointPath string) *Engine {
	e := &Engine{
		broker:         b,
		checkpointPath: checkpointPath,
		jobs:           make(map[string]QuerySpec),
		states:         make(map[string]*CheckpointState),
		lookupTable:    make(map[string]map[string]any),
	}
	if checkpointPath != "" {
		if raw, err := os.ReadFile(checkpointPath); err == nil {
			_ = json.Unmarshal(raw, &e.states)
		}
	}
	return e
}

// RegisterJob registers a compiled Stream DSL pipeline.
func (e *Engine) RegisterJob(spec QuerySpec) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.jobs[spec.Name] = spec
	if _, ok := e.states[spec.Name]; !ok {
		e.states[spec.Name] = &CheckpointState{
			Offsets:      make(map[int]uint64),
			WindowCounts: make(map[string]int),
			WindowSums:   make(map[string]float64),
		}
	}
}

// PutJoinLookup populates a stateful key-value table used for stream-table joins during processing.
func (e *Engine) PutJoinLookup(key string, attrs map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lookupTable[key] = attrs
}

// Step processes new records across all registered stream jobs and persists window checkpoints.
func (e *Engine) Step() (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	emitted := 0

	topics := e.broker.ListTopics()
	partCounts := make(map[string]int)
	for _, t := range topics {
		partCounts[t.Config.Name] = len(t.Partitions)
	}

	for name, spec := range e.jobs {
		st := e.states[name]
		nParts := partCounts[spec.SourceTopic]
		for pid := 0; pid < nParts; pid++ {
			off := st.Offsets[pid]
			recs, err := e.broker.Fetch(spec.SourceTopic, pid, off, 200)
			if err != nil {
				return emitted, err
			}
			for _, rec := range recs {
				st.Offsets[pid] = rec.Offset + 1
				var obj map[string]any
				if json.Unmarshal(rec.Value, &obj) != nil {
					continue
				}
				if !matchesFilter(obj, spec.FilterField, spec.FilterOp, spec.FilterVal) {
					continue
				}
				// Optional stream-table join enrichment by record key
				if len(rec.Key) > 0 {
					if extra, ok := e.lookupTable[string(rec.Key)]; ok {
						for k, v := range extra {
							obj[k] = v
						}
					}
				}

				if spec.WindowSize <= 0 {
					outVal, _ := json.Marshal(obj)
					if _, err := e.broker.Produce(spec.EmitTopic, []broker.ProduceMessage{{Key: rec.Key, Value: outVal}}); err == nil {
						emitted++
					}
					continue
				}

				gk := "__all__"
				if spec.GroupByField != "" {
					if gv, ok := obj[spec.GroupByField]; ok {
						gk = fmt.Sprintf("%v", gv)
					}
				}
				st.WindowCounts[gk]++
				if spec.AggFunc == "SUM" && spec.AggField != "" {
					if fv, ok := toFloat(obj[spec.AggField]); ok {
						st.WindowSums[gk] += fv
					}
				}
				st.ProcessedInW++
				if st.ProcessedInW >= spec.WindowSize {
					emitted += e.flushWindowLocked(spec, st)
				}
			}
		}
	}
	e.saveLocked()
	return emitted, nil
}

func (e *Engine) flushWindowLocked(spec QuerySpec, st *CheckpointState) int {
	emitted := 0
	for gk, cnt := range st.WindowCounts {
		payload := map[string]any{
			"group":     gk,
			"count":     cnt,
			"sum":       st.WindowSums[gk],
			"window_at": time.Now().UTC().Format(time.RFC3339),
		}
		raw, _ := json.Marshal(payload)
		if _, err := e.broker.Produce(spec.EmitTopic, []broker.ProduceMessage{{Key: []byte(gk), Value: raw}}); err == nil {
			emitted++
		}
	}
	st.WindowCounts = make(map[string]int)
	st.WindowSums = make(map[string]float64)
	st.ProcessedInW = 0
	return emitted
}

func matchesFilter(obj map[string]any, field, op, target string) bool {
	if field == "" {
		return true
	}
	val, ok := obj[field]
	if !ok {
		return false
	}
	if op == "==" || op == "=" {
		return fmt.Sprintf("%v", val) == target
	}
	if op == "!=" {
		return fmt.Sprintf("%v", val) != target
	}
	lhs, okL := toFloat(val)
	rhs, errR := strconv.ParseFloat(target, 64)
	if !okL || errR != nil {
		return false
	}
	switch op {
	case ">":
		return lhs > rhs
	case ">=":
		return lhs >= rhs
	case "<":
		return lhs < rhs
	case "<=":
		return lhs <= rhs
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func (e *Engine) ListJobs() []QuerySpec {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]QuerySpec, 0, len(e.jobs))
	for _, j := range e.jobs {
		out = append(out, j)
	}
	return out
}

func (e *Engine) saveLocked() {
	if e.checkpointPath == "" {
		return
	}
	_ = os.MkdirAll(filepath.Dir(e.checkpointPath), 0o750)
	raw, err := json.MarshalIndent(e.states, "", "  ")
	if err == nil {
		tmp := e.checkpointPath + ".tmp"
		if os.WriteFile(tmp, raw, 0o640) == nil {
			_ = os.Rename(tmp, e.checkpointPath)
		}
	}
}

// Unused import safeguard if needed cleanly:
var _ = logstore.Record{}
