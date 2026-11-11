# Durable Buffer

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `processor:durable_buffer` (`urn:otel:processor:durable_buffer`)
- Feature gate: `durable-buffer`
- Stability: Experimental

## Overview

The Durable Buffer provides crash-resilient buffering using a
write-ahead log (WAL) and segment storage. Data is persisted before forwarding
downstream, enabling recovery after crashes or network outages.

It stores OTAP Arrow records, OTLP bytes (as-is in `pass_through` mode), and
pluggable Arrow record sets (see
[pluggable Arrow pdata](../../../../../docs/pluggable-arrow-pdata.md)) without
changing their representation. Other inputs are converted to OTAP Arrow records
before they are stored. It declares a pass-through wiring contract, so it can
sit between a pluggable Arrow receiver and a pluggable Arrow exporter. A
pluggable record set is replayed with its exact representation identity and
version, signal, root item count, table IDs, schema IDs, schema fingerprints,
and Arrow batches.

## Getting Started

Set a persistent storage path before placing the durable buffer ahead of an
exporter:

```yaml
type: processor:durable_buffer
config:
  path: /var/lib/otap/buffer
  retention_size_cap: 10 GiB
  size_cap_policy: backpressure
```

## Configuration

```yaml
type: processor:durable_buffer
config:
  # Directory for persistent storage (required)
  path: /var/lib/otap/buffer

  # Maximum disk space (default: 10 GiB)
  retention_size_cap: 10 GiB

  # Maximum age of data to retain (optional, no default)
  # When set, data older than this becomes eligible for removal.
  # Can be combined with retention_size_cap for dual-constraint retention.
  # max_age: 24h

  # Policy when size cap is reached (default: backpressure)
  # - backpressure: Block ingestion (no data loss)
  # - drop_oldest: Remove oldest segments (controlled data loss)
  size_cap_policy: backpressure

  # Interval for polling for available bundles (default: 100ms)
  poll_interval: 100ms

  # Maximum time a segment stays open before finalization (default: 1s)
  # Lower values reduce latency but increase I/O overhead.
  max_segment_open_duration: 1s

  # Maximum time between WAL fsyncs (default: 25ms)
  # With upstream_ack: after_wal_append, upstream is ACKed after the WAL
  # append. Set 0s to fsync before every ACK, so acknowledged data always
  # survives a crash (one fsync per bundle).
  wal_flush_interval: 25ms

  # When upstream is ACKed (default: after_wal_append)
  # - after_wal_append: ACK once the bundle is appended to the WAL. An OS
  #   crash or power loss can lose bundles appended since the last fsync.
  # - after_wal_sync: hold each ACK until a WAL fsync covering the bundle
  #   has completed. Held ACKs are released together by one fsync at most
  #   wal_flush_interval after the first one was held, so ACK latency rises
  #   by up to wal_flush_interval. Acknowledged data survives OS crash and
  #   power loss. With wal_flush_interval: 0s every append is already
  #   fsynced, so ACKs are sent immediately.
  upstream_ack: after_wal_append

  # Arrow IPC compression for WAL entries (default: {codec: none})
  # - {codec: none}: no compression
  # - {codec: lz4}: fast, moderate ratio
  # - {codec: zstd}: higher ratio, more CPU; optional level, e.g.
  #   {codec: zstd, level: 3}
  # Runs on every ingested bundle before the upstream ACK.
  wal_compression:
    codec: none

  # Arrow IPC compression for finalized segment files (default: {codec: none})
  # Same values as wal_compression. Compressed segments use less disk and are
  # decompressed each time they are read for delivery.
  segment_compression:
    codec: none

  # OTLP handling mode (default: pass_through)
  # - pass_through: Store OTLP as opaque binary, very CPU efficient
  # - convert_to_arrow: Convert to Arrow format, enables querying but higher CPU
  otlp_handling: pass_through

  # Initial retry delay after first NACK (default: 1s)
  initial_retry_interval: 1s

  # Maximum retry delay cap (default: 30s)
  max_retry_interval: 30s

  # Backoff multiplier (default: 2.0)
  retry_multiplier: 2.0

  # Maximum bundles in-flight to downstream (default: 1000)
  max_in_flight: 1000
```

## Architecture

Each processor instance (one per CPU core) has its own isolated storage engine:

```text
{path}/
+-- core_0/
|   +-- wal/
|   +-- segments/
+-- core_1/
|   +-- wal/
|   +-- segments/
+-- ...
```

## Message Flow

1. **Ingest**: Incoming data is written to the WAL, then ACK sent upstream
   (after an fsync when `wal_flush_interval` is `0s`). With
   `upstream_ack: after_wal_sync`, the ACK is held until the next WAL fsync
   instead; see [Acknowledging after WAL sync](#acknowledging-after-wal-sync).
2. **Segment Finalization**: When segment reaches size/time threshold, it's
   written to disk
3. **Forward**: Timer tick polls for finalized bundles, sends downstream
4. **ACK/NACK**: On ACK, bundle marked complete; on NACK, deferred for retry
5. **Cleanup**: Fully-consumed segments are deleted to reclaim disk space

### Acknowledging after WAL sync

By default the upstream ACK follows the WAL append, and the WAL is fsynced at
most `wal_flush_interval` later. An OS crash or power loss in that window can
lose bundles that were already acknowledged. Setting `wal_flush_interval: 0s`
closes the window at the cost of one fsync per bundle.

`upstream_ack: after_wal_sync` closes the window while still batching fsyncs.
After a bundle is appended, its ACK is held in memory. When the first ACK is
held, the node arms a deadline of `wal_flush_interval`. When the deadline
fires, the node runs one WAL fsync and then releases every held ACK. The
deadline does not depend on new data arriving, so an idle pipeline still
releases its ACKs on time. At most 1024 ACKs are held; when the list fills,
the node fsyncs immediately and releases all of them.

If the fsync fails, or any WAL write or fsync failed since the last release,
every held bundle is NACKed upstream as retryable. These
bundles were already appended to the WAL and may still be delivered from it,
so an upstream retry can produce duplicates. Delivery remains at-least-once.
On shutdown, the node runs a final fsync and releases (or NACKs) all held ACKs
before it flushes and drains storage.

> **Note:** The trade-off is latency. Upstream ACKs arrive up to
> `wal_flush_interval` later than with the default, in exchange for
> acknowledged data surviving OS crash and power loss. Callers that wait for
> each ACK before sending more data see their throughput limited by this
> latency.

## Telemetry

These tables list telemetry emitted directly by this node. Common engine
runtime metric sets may also be attached by the pipeline telemetry policy.

### Metric Sets

| Scope | Instrument(s) | Dimensions |
| --- | --- | --- |
| `processor.durable_buffer` | `read.errors`, `storage.bytes.used`, `storage.bytes.cap`, `retries.scheduled`, `in.flight`, `flush.failures`, `storage.utilization`, `wal.syncs`, `wal.sync.failures` | None |
| `processor.durable_buffer.bundles` | `resolved` | `outcome=acked\|deferred\|permanently_rejected` |
| `processor.durable_buffer.ingest` | `failures` | `failure=error\|backpressure` |
| `processor.durable_buffer.items` | `rejected`, `consumed`, `produced`, `requeued`, `queued` | `signal=traces\|metrics\|logs` |
| `processor.durable_buffer.reclaimed` | `segments`, `bytes` | `reason=drop_oldest\|expired` |
| `processor.durable_buffer.loss` | `bundles`, `bytes` | `reason=drop_oldest\|expired` |
| `processor.durable_buffer.loss` | `items` | `signal=traces\|metrics\|logs`, `reason=drop_oldest\|expired` |

Reclaimed `segments` and `bytes` describe physical segment files and persisted
storage removed by retention. Loss `bundles`, `bytes`, and `items` describe only
unresolved logical telemetry in those files. Logical bytes use the payload's
current representation: active Arrow ranges for OTAP and protobuf wire length
for OTLP pass-through. Expiry excludes bundles that every subscriber had already
resolved.

### Events

| Event | Severity | Description |
| --- | --- | --- |
| `durable_buffer.engine.*` | `info`, `error` | Engine initialization, readiness, and unavailable states. |
| `durable_buffer.ingest.*` | `warn`, `error` | Storage backpressure and ingest failures. |
| `durable_buffer.otlp.*` | `error` | OTLP adapter and conversion failures. |
| `durable_buffer.representation.unsupported` | `error` | A pluggable Arrow record set exceeds the Quiver persistence limits and is permanently NACKed. |
| `durable_buffer.flush.failed` | `warn` | Segment flush failure during timer processing. |
| `durable_buffer.maintenance.failed` | `warn` | Durable storage maintenance failure during timer processing. |
| `durable_buffer.drain.*` | `debug`, `error` | Drain loop budget, capacity, backpressure, and poll-failure states. |
| `durable_buffer.bundle.*` | `debug`, `warn`, `error` | Bundle forward, ACK/NACK, duplicate, conversion, and permanent-rejection states. |
| `durable_buffer.retry.*` | `debug`, `warn` | Retry scheduling, deferred retry, backpressure, claim, rearm, and reschedule states. |
| `durable_buffer.wal_sync.*` | `debug`, `warn`, `error` | WAL fsync deadline arming, stale deadline wakeups, and fsync failures that NACK held upstream ACKs. |
| `durable_buffer.shutdown.*` | `info`, `warn`, `error` | Shutdown WAL sync, flush, drain, deadline, and engine termination progress or failure. |
| `durable_buffer.timer.started` | `debug` | Periodic poll timer started on first processed message. |
| `durable_buffer.config.update` | `debug` | Runtime config update received. |

See [telemetry.md](telemetry.md) for maintenance notes and the expanded event inventory.

## Limits

- `path` is required and each core writes to an isolated subdirectory.
- Retention size is divided across assigned pipeline cores.
- `max_age` is based on segment finalization time, not telemetry timestamps.
- `convert_to_arrow` mode can increase CPU cost compared with `pass_through`.
- Compression settings can change between restarts; data already on disk is
  read with whatever codec it was written with. `retention_size_cap` and the
  WAL limits count compressed bytes on disk, while segment finalization is
  triggered by the uncompressed in-memory size. Compressed segments are
  decompressed on every read, which removes the zero-copy benefit of
  memory-mapped segment reads.
- An `lz4` setting is rejected with a `level`, and a `zstd` level outside the
  supported range fails configuration validation at startup.
- Releases without LZ4 support cannot read LZ4-compressed WAL files or
  segments. Before downgrading to one, switch away from `lz4` and drain the
  existing data. Zstandard data does not have this restriction.
- A pluggable Arrow record set can hold at most 12 tables, and its encoded
  representation manifest (identity plus table and schema IDs) at most 64 KiB.
  Larger record sets are permanently NACKed because they can never be
  persisted.
- The pluggable Arrow manifest is stored as Quiver bundle metadata. Releases
  without bundle metadata support stop WAL replay at the first bundle that
  carries metadata and drop the metadata from finalized segments, so they
  cannot replay pluggable record sets. Drain pluggable data before
  downgrading.

## Related Docs

- [Configuration model](../../../../../docs/configuration-model.md)
- [Processor taxonomy](../../../../../docs/processors.md)
- [Core node catalog](../../../README.md)
