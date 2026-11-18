package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CompatibilityMode constrains how new schema versions may evolve.
type CompatibilityMode string

const (
	CompatNone     CompatibilityMode = "NONE"
	CompatBackward CompatibilityMode = "BACKWARD"
	CompatForward  CompatibilityMode = "FORWARD"
	CompatFull     CompatibilityMode = "FULL"
)

// Schema defines a versioned JSON payload field contract for a topic subject.
type Schema struct {
	Subject       string            `json:"subject"`
	Version       int               `json:"version"`
	Compatibility CompatibilityMode `json:"compatibility"`
	Fields        map[string]string `json:"fields"`   // field -> string | number | boolean | object
	Required      []string          `json:"required"` // non-optional keys
	CreatedAt     time.Time         `json:"created_at"`
}

// Registry stores persistent versioned schemas and enforces compatibility evolution.
type Registry struct {
	mu       sync.RWMutex
	path     string
	subjects map[string][]Schema
}

// Open loads or creates a Schema Registry persisted at path.
func Open(path string) (*Registry, error) {
	r := &Registry{
		path:     path,
		subjects: make(map[string][]Schema),
	}
	if path != "" {
		if raw, err := os.ReadFile(path); err == nil {
			_ = json.Unmarshal(raw, &r.subjects)
		}
	}
	return r, nil
}

// Register validates compatibility against prior version and appends the next version.
func (r *Registry) Register(s Schema) (Schema, error) {
	if s.Subject == "" || len(s.Fields) == 0 {
		return Schema{}, fmt.Errorf("schema requires subject and fields")
	}
	if s.Compatibility == "" {
		s.Compatibility = CompatBackward
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	versions := r.subjects[s.Subject]
	if len(versions) > 0 {
		prev := versions[len(versions)-1]
		if err := checkCompatibility(prev, s, s.Compatibility); err != nil {
			return Schema{}, err
		}
	}
	s.Version = len(versions) + 1
	s.CreatedAt = time.Now().UTC()
	r.subjects[s.Subject] = append(versions, s)
	r.saveLocked()
	return s, nil
}

func checkCompatibility(prev, next Schema, mode CompatibilityMode) error {
	if mode == CompatNone {
		return nil
	}
	// Check type changes on shared fields
	for k, prevType := range prev.Fields {
		if nextType, exists := next.Fields[k]; exists && nextType != prevType {
			return fmt.Errorf("incompatible type change on field %q (%s -> %s)", k, prevType, nextType)
		}
	}
	prevReq := toSet(prev.Required)
	nextReq := toSet(next.Required)

	// BACKWARD (new reader reads old data): new schema must NOT add brand-new required fields or delete previously required fields unexpectedly.
	if mode == CompatBackward || mode == CompatFull {
		for req := range nextReq {
			if _, existed := prev.Fields[req]; !existed {
				return fmt.Errorf("BACKWARD incompatible: added new required field %q", req)
			}
		}
		for oldField := range prev.Fields {
			if _, kept := next.Fields[oldField]; !kept {
				return fmt.Errorf("BACKWARD incompatible: removed field %q", oldField)
			}
		}
	}
	// FORWARD (old reader reads new data): new schema must NOT drop previously required fields or add new fields unknown to strict old required contract
	if mode == CompatForward || mode == CompatFull {
		for req := range prevReq {
			if _, kept := next.Fields[req]; !kept {
				return fmt.Errorf("FORWARD incompatible: removed required field %q", req)
			}
		}
		for k := range next.Fields {
			if _, existed := prev.Fields[k]; !existed {
				return fmt.Errorf("FORWARD incompatible: added unmapped field %q", k)
			}
		}
	}
	return nil
}

// ValidatePayload checks a JSON object payload against the latest schema registered for subject (if any).
func (r *Registry) ValidatePayload(subject string, payload []byte) error {
	r.mu.RLock()
	versions := r.subjects[subject]
	if len(versions) == 0 {
		r.mu.RUnlock()
		return nil
	}
	latest := versions[len(versions)-1]
	r.mu.RUnlock()

	var obj map[string]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		return fmt.Errorf("payload not valid JSON object for schema %s v%d: %w", subject, latest.Version, err)
	}
	for _, req := range latest.Required {
		if _, ok := obj[req]; !ok {
			return fmt.Errorf("missing required schema field %q (%s v%d)", req, subject, latest.Version)
		}
	}
	for k, expectedType := range latest.Fields {
		val, exists := obj[k]
		if !exists || val == nil {
			continue
		}
		switch expectedType {
		case "string":
			if _, ok := val.(string); !ok {
				return fmt.Errorf("field %q must be string", k)
			}
		case "number":
			if _, ok := val.(float64); !ok {
				return fmt.Errorf("field %q must be number", k)
			}
		case "boolean":
			if _, ok := val.(bool); !ok {
				return fmt.Errorf("field %q must be boolean", k)
			}
		}
	}
	return nil
}

// List returns all registered schemas by subject.
func (r *Registry) List() map[string][]Schema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string][]Schema, len(r.subjects))
	for k, v := range r.subjects {
		cp := make([]Schema, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

func (r *Registry) saveLocked() {
	if r.path == "" {
		return
	}
	_ = os.MkdirAll(filepath.Dir(r.path), 0o750)
	raw, err := json.MarshalIndent(r.subjects, "", "  ")
	if err != nil {
		return
	}
	tmp := r.path + ".tmp"
	if os.WriteFile(tmp, raw, 0o640) == nil {
		_ = os.Rename(tmp, r.path)
	}
}

func toSet(items []string) map[string]struct{} {
	s := make(map[string]struct{}, len(items))
	for _, it := range items {
		s[it] = struct{}{}
	}
	return s
}
