# StreamForge STRIDE Threat Model

| Asset | Threat | Mitigation |
| :--- | :--- | :--- |
| Append-only Log Segments | Disk bit-rot or partial write tampering | CRC32 IEEE checksum validated per record frame; truncated crash tails repaired cleanly at last valid boundary |
| TCP Binary & HTTP Admin API | Unauthorized produce/consume/admin | Constant-time `STREAMFORGE_AUTH_TOKEN` bearer & wire auth check, bounded 8 MiB frame size limit, per-IP rate limiting |
| Topic / Schema Identifiers | Path traversal writing segment files outside data dir | Strict `[A-Za-z0-9._-]+` sanitizer blocking `..`, `/`, `\` in topic/subject/group names |
| Broker Flooding | Unbounded memory queue exhaustion | Per-partition bounded pending/rate backpressure returning explicit `ErrBackpressure` / HTTP `429` |
| Stream DSL / Schema Parser | Malformed input DoS | Bounded expression length & strict field/type validation fuzz-tested via `go test -fuzz` |
