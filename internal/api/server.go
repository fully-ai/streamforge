package api

import (
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"

	"github.com/fully-ai/streamforge/internal/broker"
	"github.com/fully-ai/streamforge/internal/registry"
	"github.com/fully-ai/streamforge/internal/stream"
)

//go:embed web
var WebAssets embed.FS

// Server exposes StreamForge HTTP REST Admin/Produce/Consume API and embedded Web Dashboard.
type Server struct {
	broker    *broker.Broker
	streams   *stream.Engine
	authToken string
	mux       *http.ServeMux
	web       http.Handler
}

func NewServer(b *broker.Broker, st *stream.Engine, authToken string) *Server {
	s := &Server{
		broker:    b,
		streams:   st,
		authToken: authToken,
		mux:       http.NewServeMux(),
	}
	if sub, err := fs.Sub(WebAssets, "web"); err == nil {
		s.web = http.FileServer(http.FS(sub))
	}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	if !strings.HasPrefix(r.URL.Path, "/api/") && s.web != nil {
		s.web.ServeHTTP(w, r)
		return
	}
	if r.URL.Path != "/api/v1/health" && s.authToken != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.authToken)) != 1 {
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok", "metrics": s.broker.Metrics()})
	})
	s.mux.HandleFunc("GET /api/v1/topics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"topics": s.broker.ListTopics(), "metrics": s.broker.Metrics()})
	})
	s.mux.HandleFunc("POST /api/v1/topics", func(w http.ResponseWriter, r *http.Request) {
		var cfg broker.TopicConfig
		if err := json.NewDecoder(io.LimitReader(r.Body, 32*1024)).Decode(&cfg); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		if err := s.broker.CreateTopic(cfg); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		writeJSON(w, 201, cfg)
	})
	s.mux.HandleFunc("POST /api/v1/topics/{topic}/produce", func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		var body struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 2*1024*1024)).Decode(&body); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		res, err := s.broker.Produce(topic, []broker.ProduceMessage{{Key: []byte(body.Key), Value: []byte(body.Value)}})
		if err != nil {
			if errors.Is(err, broker.ErrBackpressure) {
				writeErr(w, 429, err.Error())
				return
			}
			writeErr(w, 400, err.Error())
			return
		}
		writeJSON(w, 200, map[string]any{"results": res})
	})
	s.mux.HandleFunc("GET /api/v1/topics/{topic}/consume", func(w http.ResponseWriter, r *http.Request) {
		topic := r.PathValue("topic")
		part, _ := strconv.Atoi(r.URL.Query().Get("partition"))
		off, _ := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 64)
		recs, err := s.broker.Fetch(topic, part, off, 50)
		if err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		writeJSON(w, 200, map[string]any{"records": recs})
	})
	s.mux.HandleFunc("GET /api/v1/groups", func(w http.ResponseWriter, r *http.Request) {
		groups := s.broker.ListGroups()
		lags := make(map[string]map[int]uint64)
		for _, g := range groups {
			if l, err := s.broker.GroupLag(g.GroupID); err == nil {
				lags[g.GroupID] = l
			}
		}
		writeJSON(w, 200, map[string]any{"groups": groups, "lags": lags})
	})
	s.mux.HandleFunc("GET /api/v1/schemas", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"schemas": s.broker.Schemas().List()})
	})
	s.mux.HandleFunc("POST /api/v1/schemas", func(w http.ResponseWriter, r *http.Request) {
		var sc registry.Schema
		if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&sc); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		saved, err := s.broker.Schemas().Register(sc)
		if err != nil {
			writeErr(w, 409, err.Error())
			return
		}
		writeJSON(w, 201, saved)
	})
	s.mux.HandleFunc("GET /api/v1/streams", func(w http.ResponseWriter, r *http.Request) {
		if s.streams == nil {
			writeJSON(w, 200, map[string]any{"jobs": []any{}})
			return
		}
		writeJSON(w, 200, map[string]any{"jobs": s.streams.ListJobs()})
	})
	s.mux.HandleFunc("POST /api/v1/streams", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Name string `json:"name"`
			DSL  string `json:"dsl"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 32*1024)).Decode(&body); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		spec, err := stream.ParseStreamDSL(body.Name, body.DSL)
		if err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		s.streams.RegisterJob(spec)
		emitted, _ := s.streams.Step()
		writeJSON(w, 201, map[string]any{"job": spec, "emitted_now": emitted})
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
