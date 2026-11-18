# StreamForge Architecture

StreamForge is a Kafka-inspired distributed event streaming and real-time stream processing platform written in pure Go (zero external runtime dependencies).

## Architecture Overview

```
Producers (TCP Wire / HTTP / Connectors)
   │  Produce (FNV-1a key partitioner -> FIFO ordering per partition)
   ▼
+-------------------------------------------------------------------+
| StreamForge Broker Cluster                                        |
|  - Leader Partition Append Log (Segment .log + Sparse .index)     |
|  - CRC32 Record Checksums + Optional Gzip Batch Compression       |
|  - ISR Follower TCP Replication & Heartbeat Failover Coordinator  |
|  - Bounded Backpressure + Size/Age Retention + Key Compaction     |
+--------+-----------------------------------+----------------------+
         │ Fetch / Consumer Group Rebalance  │ Schema Validation
         ▼                                   ▼
+-----------------------------+   +---------------------------------+
| Consumer Group Coordinator  |   | Schema Registry                 |
|  - Range Partition Rebalance|   |  - Versioned JSON Field Schemas |
|  - Durable Offset Commits   |   |  - BACKWARD/FORWARD/FULL Checks |
|  - Lag & Dead-Letter Queue  |   +---------------------------------+
+--------+--------------------+
         │ Consume / Transform / Window
         ▼
+-------------------------------------------------------------------+
| Stateful Stream Engine & Connectors                               |
|  - Filter / Map / Tumbling Window Aggregations + Disk Checkpoints |
|  - Stream DSL Runner + FileSource / FileSink / HTTPSink           |
+-------------------------------------------------------------------+
```

## Ordering & Delivery Guarantees

- **Per-Partition Strict Ordering**: Messages routed to the same partition (via deterministic FNV-1a key hashing or explicit partition ID) are appended sequentially with monotonically increasing `offset` (`0, 1, 2, ...`).
- **Delivery Semantics**: **At-least-once** delivery is explicitly guaranteed via durable disk WAL segments, ISR follower acknowledgment, consumer group offset commits (`offsets.json`), retry tracking, and Dead-Letter Queue routing. (Exactly-once idempotent transactions are listed on the v0.2 roadmap and are not claimed.)
