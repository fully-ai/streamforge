package connect

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/fully-ai/streamforge/internal/broker"
)

// Connector is implemented by built-in and custom sources/sinks.
type Connector interface {
	ID() string
	Kind() string
	PollOnce(ctx context.Context) (int, error)
}

// FileTailSource streams newline-delimited lines from a file into a StreamForge topic.
type FileTailSource struct {
	id     string
	path   string
	topic  string
	broker *broker.Broker
	offset int64
	mu     sync.Mutex
}

func NewFileTailSource(id, path, topic string, b *broker.Broker) *FileTailSource {
	return &FileTailSource{id: id, path: path, topic: topic, broker: b}
}

func (s *FileTailSource) ID() string   { return s.id }
func (s *FileTailSource) Kind() string { return "file_source" }

func (s *FileTailSource) PollOnce(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(s.path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if _, err := f.Seek(s.offset, io.SeekStart); err != nil {
		return 0, err
	}
	sc := bufio.NewScanner(f)
	var batch []broker.ProduceMessage
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) > 0 {
			cp := append([]byte(nil), line...)
			batch = append(batch, broker.ProduceMessage{Value: cp})
		}
	}
	if len(batch) == 0 {
		return 0, nil
	}
	if _, err := s.broker.Produce(s.topic, batch); err != nil {
		return 0, err
	}
	newOff, _ := f.Seek(0, io.SeekCurrent)
	s.offset = newOff
	return len(batch), nil
}

// FileSink consumes topic partition 0 records and writes NDJSON lines cleanly to disk.
type FileSink struct {
	id      string
	topic   string
	groupID string
	outPath string
	broker  *broker.Broker
	offset  uint64
	mu      sync.Mutex
}

func NewFileSink(id, topic, groupID, outPath string, b *broker.Broker) *FileSink {
	return &FileSink{id: id, topic: topic, groupID: groupID, outPath: outPath, broker: b}
}

func (s *FileSink) ID() string   { return s.id }
func (s *FileSink) Kind() string { return "file_sink" }

func (s *FileSink) PollOnce(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	recs, err := s.broker.Fetch(s.topic, 0, s.offset, 100)
	if err != nil || len(recs) == 0 {
		return 0, err
	}
	f, err := os.OpenFile(s.outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	for _, r := range recs {
		_, _ = f.Write(r.Value)
		_, _ = f.WriteString("\n")
		s.offset = r.Offset + 1
	}
	_ = s.broker.CommitOffset(s.groupID, s.topic, 0, s.offset)
	return len(recs), nil
}

// HTTPSink delivers topic records via HTTP POST and routes failed deliveries to `<topic>.dlq`.
type HTTPSink struct {
	id        string
	topic     string
	groupID   string
	targetURL string
	broker    *broker.Broker
	client    *http.Client
	offset    uint64
	mu        sync.Mutex
}

func NewHTTPSink(id, topic, groupID, targetURL string, b *broker.Broker) *HTTPSink {
	return &HTTPSink{
		id:        id,
		topic:     topic,
		groupID:   groupID,
		targetURL: targetURL,
		broker:    b,
		client:    &http.Client{Timeout: 3 * time.Second},
	}
}

func (s *HTTPSink) ID() string   { return s.id }
func (s *HTTPSink) Kind() string { return "http_sink" }

func (s *HTTPSink) PollOnce(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	recs, err := s.broker.Fetch(s.topic, 0, s.offset, 50)
	if err != nil || len(recs) == 0 {
		return 0, err
	}
	delivered := 0
	for _, r := range recs {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.targetURL, bytes.NewReader(r.Value))
		if err != nil {
			return delivered, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.client.Do(req)
		if err != nil || resp.StatusCode >= 400 {
			if resp != nil {
				_ = resp.Body.Close()
			}
			_ = s.broker.SendToDLQ(s.topic, r, fmt.Sprintf("http_sink failure: %v", err))
		} else {
			_ = resp.Body.Close()
			delivered++
		}
		s.offset = r.Offset + 1
		_ = s.broker.CommitOffset(s.groupID, s.topic, 0, s.offset)
	}
	return delivered, nil
}
