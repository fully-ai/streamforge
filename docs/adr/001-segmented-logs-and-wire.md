# ADR 001 — Append-Only Segmented Commit Logs & Pure-Go Wire Protocol

- **Status:** Accepted
- **Date:** 2026-11-18

## Decision
1. Implement binary append-only segmented `.log` files paired with fixed 16-byte sparse `.index` files (`[relative_offset:u32][file_pos:u64][timestamp_ms:u32]`), rolled cleanly at configurable byte thresholds.
2. Validate every record frame with CRC32 (`IEEE`), recover cleanly from partial tail writes after simulated broker crashes, and support both retention deletion and key-based compaction on closed segments.
3. Build pure Go TCP binary framing (`[len:u32][type:u8][payload]`) and HTTP REST + embedded Web UI with zero third-party Go dependencies (`go.mod` stdlib-only), eliminating third-party supply-chain risk.
