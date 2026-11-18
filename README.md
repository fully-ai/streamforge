# StreamForge

**StreamForge** is a distributed event streaming, commit-log broker, and stateful stream processing platform implemented in pure Go with zero external runtime dependencies.

> **Maturity:** Alpha (`v0.1.0-alpha.1`) — Segmented commit-log broker, crash-tail recovery, consumer group rebalancing, schema registry compatibility checks, stateful stream DSL processor, connectors, multi-broker TCP replication/failover, binary wire client, and embedded dashboard verified.

## Key Capabilities

- **Segmented Commit Logs (`internal/logstore`)**: Append-only `.log` + sparse `.index` segments, CRC32 record framing, optional Gzip compression, crash-tail recovery, size retention, and key-based compaction.
- **Broker & Consumer Groups (`internal/broker`)**: Multi-partition FNV-1a key-ordered topics, range rebalancing (`JoinConsumerGroup`/`LeaveConsumerGroup`), durable offset commits, lag calculation, `<topic>.dlq` dead-letter routing, and `ErrBackpressure` write throttling.
- **Schema Registry (`internal/registry`)**: Versioned field schemas with `BACKWARD`, `FORWARD`, `FULL`, and `NONE` compatibility guards and runtime payload validation.
- **Stateful Stream Engine (`internal/stream`)**: Stream DSL (`FROM ... WHERE ... WINDOW TUMBLING ... GROUP BY ... AGGREGATE ... EMIT TO ...`), stream-table enrichment joins, and durable window checkpoints.
- **Connectors & Cluster (`internal/connect`, `internal/cluster`)**: `FileTailSource`, `FileSink`, `HTTPSink`, and multi-broker TCP ISR replication with leader failover.
- **Binary TCP Wire Protocol, CLI & Web Console**: Length-prefixed binary TCP framing (`pkg/client`), `streamforge-cli`, HTTP REST API, and zero-CDN embedded Web UI.

## Quick Start

```bash
git clone https://github.com/fully-ai/streamforge
cd streamforge
./scripts/build.sh
./scripts/test.sh
./bin/streamforge -data-dir data/streamforge -wire-addr 127.0.0.1:9092 -http-addr 127.0.0.1:9093
```

Open `http://127.0.0.1:9093` or use `./bin/streamforge-cli create-topic orders 3`.

## Delivery Guarantees & Docs
- Guaranteed **at-least-once** delivery and strict per-partition FIFO ordering.
- [ARCHITECTURE.md](ARCHITECTURE.md), [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md), [SECURITY.md](SECURITY.md)
- Apache License 2.0 ([LICENSE](LICENSE))
