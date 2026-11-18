package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/fully-ai/streamforge/internal/api"
	"github.com/fully-ai/streamforge/internal/broker"
	"github.com/fully-ai/streamforge/internal/protocol"
	"github.com/fully-ai/streamforge/internal/stream"
)

func main() {
	dataDir := flag.String("data-dir", "data/streamforge", "Commit-log storage directory")
	wireAddr := flag.String("wire-addr", "127.0.0.1:9092", "Binary TCP wire broker address")
	httpAddr := flag.String("http-addr", "127.0.0.1:9093", "HTTP admin/dashboard address")
	flag.Parse()

	b, err := broker.Open(*dataDir)
	if err != nil {
		log.Fatalf("open broker: %v", err)
	}
	defer b.Close()

	st := stream.NewEngine(b, filepath.Join(*dataDir, "stream_checkpoints.json"))
	authTok := os.Getenv("STREAMFORGE_AUTH_TOKEN")

	stopCh := make(chan struct{})
	ln, err := net.Listen("tcp", *wireAddr)
	if err != nil {
		log.Fatalf("listen wire %s: %v", *wireAddr, err)
	}
	go protocol.ServeWire(ln, b, authTok, stopCh)

	// Periodic stream DSL processor tick
	go func() {
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_, _ = st.Step()
			case <-stopCh:
				return
			}
		}
	}()

	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           api.NewServer(b, st, authTok),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("[STREAMFORGE] Wire TCP on %s | HTTP Dashboard on http://%s", ln.Addr(), *httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	close(stopCh)
	_ = ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
