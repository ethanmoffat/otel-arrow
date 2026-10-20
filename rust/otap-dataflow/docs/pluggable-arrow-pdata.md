# Pluggable Arrow pipeline data

This document describes the first production slice of pluggable Arrow pipeline
data tracked by
[open-telemetry/otel-arrow#3875](https://github.com/open-telemetry/otel-arrow/issues/3875).
It establishes the in-memory carrier, codec registration, and fail-closed
wiring behavior needed by a future Arrow Flight receiver, and durable
persistence through the durable-buffer processor. It does not implement the
Flight server, a Delta exporter, or mixed-signal envelopes.

## In-memory representation

`PdataArrowRecordSet` owns a named and versioned set of Arrow `RecordBatch`
values. Each table preserves:

- a format-local `u32` table ID;
- an adapter-defined schema ID;
- the exact 256-bit schema fingerprint supplied by the adapter; and
- the owned `RecordBatch`.

The carrier does not depend on OTAP protobuf payload enums or an
application-specific protocol crate. The representation adapter defines the
meaning of table IDs, schema IDs, root-item counts, and table relationships.

Cloning the carrier clones Arrow's reference-counted schema and array handles.
It does not copy array buffers. This makes the carrier suitable for ordinary
bounded DFE channels and fan-out while preserving the existing ownership model.

```rust,ignore
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::{
    PdataArrowRecordSet, PdataArrowTable, PdataRepresentation,
    PdataRepresentationId, PdataSchemaId, RepresentationVersion,
    SchemaFingerprint,
};

let representation = PdataRepresentation::new(
    PdataRepresentationId::new("example.logs"),
    RepresentationVersion::new(1, 0),
);
let table = PdataArrowTable::new(
    7,
    PdataSchemaId::new("example.logs.root"),
    SchemaFingerprint::new(adapter_fingerprint),
    record_batch,
)?;
let records = PdataArrowRecordSet::new(
    representation,
    SignalType::Logs,
    root_item_count,
    [table],
)?;
```

`PdataPayload` (exported from `otel-arrow-dfe-pdata-codec`, with `OtapPayload`
as its transitional alias) carries this value next to its encoded-bytes and
native OTAP storage. Construct it with `PdataPayload::from_pdata_arrow` or
`From<PdataArrowRecordSet>`, borrow it with `pdata_arrow_ref`, and recover it
with `into_pdata_arrow`. `PdataPayload::format()` reports
`PdataFormat::PluggableArrow` for these payloads. Signal type, representation
identity, item count, logical Arrow size, and retained memory remain available
without protobuf or Arrow IPC serialization. Encoded bytes and native
`OtapArrowRecords` remain the optimized storage forms.
`PdataPayload::representation()` exposes their compatibility identities through
the same identity model.

OTAP-only consumers use the existing `CodecService` paths (`view`,
`try_into_otap`, `materialize_otap`, and encoding for output). For a pluggable
Arrow payload these paths convert through the registered `PdataArrowDecoder`
via `CodecService::decode_arrow`. When no decoder is registered they return a
typed codec error that keeps the original payload recoverable, and the node
Nacks the message instead of panicking.

## Registration and conversion

`otel-arrow-dfe-pdata-codec` provides `register_pdata_arrow_codec!`.
Registration always enables identity discovery and pass-through. Conversion to
or from native OTAP is optional:

```rust,ignore
use otel_arrow_dfe_pdata_codec::{
    ArrowCodecMetadata, ArrowCodecRegistration, register_pdata_arrow_codec,
};

static EXAMPLE_METADATA: ArrowCodecMetadata = ArrowCodecMetadata::new(
    EXAMPLE_REPRESENTATION,
    &[SignalType::Logs],
);

register_pdata_arrow_codec!(
    EXAMPLE_CODEC,
    ArrowCodecRegistration::new(&EXAMPLE_METADATA)
        .with_decoder(create_decoder)
        .with_encoder(create_encoder),
);
```

A pass-through-only representation omits both factories. `CodecService`
creates registered mutable codec instances lazily per pipeline when a caller
uses `decode_arrow` or `encode_arrow`. Looking up an unregistered identity
returns `RegistryError::ArrowNotFound`; requesting a missing conversion
direction returns a typed unsupported-operation error.

The built-in OTLP byte codec now publishes structured version `1.0` and its
bidirectional capabilities through `CodecMetadata::representation()` and
`ResolvedCodec::capabilities()`. Native OTAP remains a direct compatibility
variant rather than being wrapped in a pluggable record set. Migrating native
OTAP table operations into the generalized registry is deliberately deferred.

## Wiring validation

Node factories declare one of these coarse startup contracts:

- `PdataWiringContract::STANDARD` for existing OTAP/OTLP-specific nodes;
- `PdataWiringContract::PASSTHROUGH` for nodes that preserve any
  representation;
- `PdataWiringContract::PLUGGABLE_ARROW_SOURCE` for a future Arrow receiver; or
- `PdataWiringContract::PLUGGABLE_ARROW_SINK` for an Arrow-only exporter; or
- `PdataWiringContract::ACCEPT_ALL` for a representation-agnostic sink.

The pipeline builder propagates possible representation families through the
graph. A pass-through processor preserves the incoming family. If pluggable
Arrow data can reach a standard-only processor or exporter, pipeline
construction fails with `Error::IncompatiblePdataWiring`.

The fan-out, retry, and signal-type router processors declare pass-through
behavior, and the noop exporter accepts all representations. Other existing
processors and exporters remain standard-only until they implement an explicit
Arrow capability. The batch processor has no batching plan for pluggable Arrow
record sets and permanently NACKs them if one reaches it.

The following is a topology example only. The component URNs do not exist yet:

```yaml
# Not runnable until the dependent Flight receiver and Delta exporter work lands.
nodes:
  flight:
    type: receiver:arrow_flight
  retry:
    type: processor:retry
  delta:
    type: exporter:delta
connections:
  - from: flight
    to: retry
  - from: retry
    to: delta
```

The future receiver factory must declare
`PLUGGABLE_ARROW_SOURCE`. The future exporter must declare
`PLUGGABLE_ARROW_SINK` before this topology can validate. The startup wiring
contract is intentionally coarse-grained in this slice; exact representation
identity and version support is resolved through the codec registry and
component configuration/runtime validation.

## Quiver persistence

The durable-buffer processor persists pluggable Arrow record sets through
Quiver and replays them unchanged:

- Each table is stored in its own Quiver slot with the adapter-supplied schema
  fingerprint. The first table uses a signal-specific slot (46 for logs, 47
  for metrics, 48 for traces) and the remaining tables use slots 49-59, all
  outside the OTAP and OTLP slot assignments. A record set may therefore hold
  at most 12 tables.
- A versioned manifest stored as Quiver bundle metadata records the
  representation identity and version, the signal, the root item count, and
  every table's ID, schema ID, and schema fingerprint. Quiver keeps the
  manifest in its write-ahead log and segment manifest, so it survives crash
  replay. The encoded manifest is bounded to 64 KiB.
- On replay the processor rebuilds an identical `PdataArrowRecordSet` and
  forwards it as a `PdataPayload` whose format is `PdataFormat::PluggableArrow`
  (built with `PdataPayload::from_pdata_arrow`; downstream nodes read it with
  `pdata_arrow_ref` or `into_pdata_arrow`). A corrupted or unknown manifest
  fails reconstruction with `BundleConversionError::InvalidPluggableManifest`
  rather than producing a record set with lost or misattributed identity; the
  processor counts a read error and rejects that bundle.
- Record sets that exceed the table-count or manifest-size limit are rejected
  with `BundleConversionError::PersistenceLimitExceeded` and permanently NACKed
  with the original payload.

The processor declares `PdataWiringContract::PASSTHROUGH`, so a topology such
as pluggable receiver -> `processor:durable_buffer` -> pluggable exporter
validates. Upstream is acknowledged after the write-ahead-log append. Set the
processor's `wal_flush_interval` to `0s` when an acknowledgement must imply
that the record set was fsynced.

Storing the manifest does not change the Quiver segment format version. Quiver
writes a separate WAL entry type only for bundles that carry metadata, and
finalized segments gain a nullable `bundle_metadata` manifest column only when
some bundle carries metadata. Releases that predate bundle metadata stop WAL
replay at the first such entry and ignore the column, so a downgrade drops the
manifest and those releases cannot replay pluggable record sets.

## Integration boundary for the Flight receiver

The next worker can:

1. decode or adapt an incoming Flight batch into `PdataArrowTable` values;
2. preserve the sender or adapter schema fingerprint exactly;
3. construct `PdataArrowRecordSet` with the registered identity and root count;
4. call `CodecService::admit_arrow` to fail closed if the identity/version or
   signal is not registered;
5. wrap it with `PdataPayload::from_pdata_arrow` (or `.into()`); and
6. send the resulting `OtapPdata` through the normal bounded effect-handler
   channel.

The receiver must not claim native OTAP conversion unless a registered
`PdataArrowDecoder` actually implements it.
