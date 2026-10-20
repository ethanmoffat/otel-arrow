// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Adapter implementing Quiver's `RecordBundle` trait for OTAP data types.
//!
//! This module bridges the OTAP data model (`OtapArrowRecords`) with Quiver's
//! storage layer by implementing the `RecordBundle` trait.
//!
//! # Slot ID Design
//!
//! Quiver's WAL uses a 64-bit bitmap for slot presence, so slot IDs must be < 64.
//! WAL slot IDs are an internal storage namespace. They are mapped explicitly
//! from `ArrowPayloadType` and are not protobuf wire numbers. Existing payloads
//! retain their historical slot assignments for persisted-segment compatibility.
//!
//! | Signal  | Payload Types (slot IDs)                                    | Table Count |
//! |---------|-------------------------------------------------------------|-------------|
//! | Shared  | RESOURCE_ATTRS (1), SCOPE_ATTRS (2)                         | 2           |
//! | Metrics | UNIVARIATE_METRICS (10) through METRIC_ATTRS (26)           | 17          |
//! | Logs    | LOGS (30), LOG_ATTRS (31)                                   | 2           |
//! | Traces  | SPANS (40) through SPAN_LINK_ATTRS (45)                     | 6           |
//!
//! Total tables per signal (including shared RESOURCE_ATTRS and SCOPE_ATTRS):
//! - Logs: 4 tables (LOGS, LOG_ATTRS, RESOURCE_ATTRS, SCOPE_ATTRS)
//! - Traces: 8 tables (SPANS, SPAN_ATTRS, SPAN_EVENTS, SPAN_LINKS,
//!   SPAN_EVENT_ATTRS, SPAN_LINK_ATTRS, RESOURCE_ATTRS, SCOPE_ATTRS)
//! - Metrics: 19 tables (UNIVARIATE_METRICS, MULTIVARIATE_METRICS, 4 data point
//!   types, 4 DP attrs, 3 exemplar types, 3 exemplar attrs, METRIC_ATTRS,
//!   RESOURCE_ATTRS, SCOPE_ATTRS)
//!
//! Reserved slots for OTLP pass-through (opaque binary storage):
//! - Slot 60: OTLP Logs
//! - Slot 61: OTLP Traces
//! - Slot 62: OTLP Metrics
//!
//! Reserved slots for pluggable Arrow record sets (`PdataArrowRecordSet`):
//! - Slot 46/47/48: first table of a Logs/Metrics/Traces record set, so the
//!   signal of a stored bundle stays derivable from its slot IDs
//! - Slots 49-59: remaining tables, in table-ID order (up to 12 tables total)
//!
//! A pluggable bundle also carries a versioned manifest in the Quiver bundle
//! metadata. It records the representation identity and version, the signal,
//! the root item count, and each table's ID, schema ID, and schema
//! fingerprint, so the original record set can be rebuilt exactly.
//!
//! Note: Slots identify payload *types*, not schemas. Within each slot, Quiver
//! supports many different schemas via `(slot_id, schema_fingerprint)` pairs
//! that create distinct "streams" (up to 100,000 per segment).

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use arrow::array::{BinaryArray, RecordBatch};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use otel_arrow_dfe_quiver::record_bundle::{
    BundleDescriptor, MAX_BUNDLE_METADATA_BYTES, PayloadRef, RecordBundle, SchemaFingerprint,
    SlotDescriptor, SlotId,
};
use otel_arrow_dfe_quiver::segment::ReconstructedBundle;

use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::otap::schema::SchemaIdBuilder;
use otel_arrow_dfe_pdata::otap::{Logs, Metrics, OtapArrowRecords, OtapBatchStore, Traces};
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::{
    OtapPayloadHelpers, OtlpProtoBytes, PdataArrowRecordSet, PdataArrowTable, PdataRepresentation,
    PdataRepresentationId, PdataSchemaId, RepresentationVersion,
};
use otel_arrow_dfe_pdata_codec::OtapPayload;

use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};

// -----------------------------------------------------------------------------
// Slot ID Constants
// -----------------------------------------------------------------------------

/// Number of positions available in Quiver's WAL slot bitmap.
const WAL_SLOT_COUNT: u16 = 64;

/// Construct a WAL slot while enforcing the bitmap-width invariant.
const fn wal_slot(raw: u16) -> SlotId {
    assert!(
        raw < WAL_SLOT_COUNT,
        "WAL slot must fit in the 64-slot bitmap"
    );
    SlotId::new(raw)
}

/// OTLP opaque binary storage slots (for pass-through mode)
/// These are in the 60-62 range to stay within the 64-slot WAL bitmap limit
mod otlp_slots {
    pub const OTLP_LOGS: u16 = 60;
    pub const OTLP_TRACES: u16 = 61;
    pub const OTLP_METRICS: u16 = 62;
}

/// Slots reserved for pluggable Arrow record sets.
///
/// They sit in the range left unassigned by the Arrow and OTLP mappings, so a
/// stored pluggable bundle can never be mistaken for an OTAP payload type.
mod pluggable_slots {
    pub const LOGS_ROOT: u16 = 46;
    pub const METRICS_ROOT: u16 = 47;
    pub const TRACES_ROOT: u16 = 48;
    pub const EXTRA_FIRST: u16 = 49;
    pub const EXTRA_LAST: u16 = 59;
}

/// Maximum number of tables in a pluggable Arrow record set Quiver can persist.
pub const MAX_PLUGGABLE_TABLES: usize =
    1 + (pluggable_slots::EXTRA_LAST - pluggable_slots::EXTRA_FIRST + 1) as usize;

/// Slot that stores the table at `index` (in table-ID order) of a pluggable
/// record set of the given signal. Returns `None` past [`MAX_PLUGGABLE_TABLES`].
fn pluggable_slot(signal_type: SignalType, index: usize) -> Option<SlotId> {
    if index == 0 {
        return Some(wal_slot(match signal_type {
            SignalType::Logs => pluggable_slots::LOGS_ROOT,
            SignalType::Metrics => pluggable_slots::METRICS_ROOT,
            SignalType::Traces => pluggable_slots::TRACES_ROOT,
        }));
    }
    let offset = u16::try_from(index - 1).ok()?;
    let raw = pluggable_slots::EXTRA_FIRST.checked_add(offset)?;
    (raw <= pluggable_slots::EXTRA_LAST).then(|| wal_slot(raw))
}

/// Signal of a pluggable record set whose first table is stored in `slot`.
const fn pluggable_root_signal(slot: SlotId) -> Option<SignalType> {
    match slot.raw() {
        pluggable_slots::LOGS_ROOT => Some(SignalType::Logs),
        pluggable_slots::METRICS_ROOT => Some(SignalType::Metrics),
        pluggable_slots::TRACES_ROOT => Some(SignalType::Traces),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum PayloadSignal {
    Shared,
    Signal(SignalType),
}

macro_rules! payload_signal {
    (Shared) => {
        PayloadSignal::Shared
    };
    ($signal:ident) => {
        PayloadSignal::Signal(SignalType::$signal)
    };
}

/// Define the stable Arrow payload mapping in both WAL directions.
///
/// The generated forward match is exhaustive, so adding a protocol payload
/// requires adding its slot and signal classification to this table.
macro_rules! define_arrow_wal_slots {
    ($( $payload:ident => ($slot:literal, $signal:ident) ),+ $(,)?) => {
        const fn to_slot_id(
            _signal_type: SignalType,
            payload_type: ArrowPayloadType,
        ) -> SlotId {
            let raw = match payload_type {
                // Bundle descriptors only iterate signal-specific allowed payload types,
                // which exclude Unknown. Reaching this arm is an internal invariant violation.
                ArrowPayloadType::Unknown => {
                    panic!("ArrowPayloadType::Unknown must not be persisted to WAL")
                }
                $(ArrowPayloadType::$payload => $slot,)+
            };
            wal_slot(raw)
        }

        const fn slot_to_payload_type(slot: SlotId) -> Option<ArrowPayloadType> {
            match slot.raw() {
                $($slot => Some(ArrowPayloadType::$payload),)+
                _ => None,
            }
        }

        const fn payload_signal_type(payload_type: ArrowPayloadType) -> Option<PayloadSignal> {
            match payload_type {
                ArrowPayloadType::Unknown => None,
                $(ArrowPayloadType::$payload => Some(payload_signal!($signal)),)+
            }
        }
    };
}

define_arrow_wal_slots! {
    ResourceAttrs => (1, Shared),
    ScopeAttrs => (2, Shared),
    UnivariateMetrics => (10, Metrics),
    NumberDataPoints => (11, Metrics),
    SummaryDataPoints => (12, Metrics),
    HistogramDataPoints => (13, Metrics),
    ExpHistogramDataPoints => (14, Metrics),
    NumberDpAttrs => (15, Metrics),
    SummaryDpAttrs => (16, Metrics),
    HistogramDpAttrs => (17, Metrics),
    ExpHistogramDpAttrs => (18, Metrics),
    NumberDpExemplars => (19, Metrics),
    HistogramDpExemplars => (20, Metrics),
    ExpHistogramDpExemplars => (21, Metrics),
    NumberDpExemplarAttrs => (22, Metrics),
    HistogramDpExemplarAttrs => (23, Metrics),
    ExpHistogramDpExemplarAttrs => (24, Metrics),
    MultivariateMetrics => (25, Metrics),
    MetricAttrs => (26, Metrics),
    Logs => (30, Logs),
    LogAttrs => (31, Logs),
    Spans => (40, Traces),
    SpanAttrs => (41, Traces),
    SpanEvents => (42, Traces),
    SpanLinks => (43, Traces),
    SpanEventAttrs => (44, Traces),
    SpanLinkAttrs => (45, Traces),
}

/// Convert signal type to OTLP slot ID (for opaque binary storage)
pub(crate) const fn to_otlp_slot_id(signal_type: SignalType) -> SlotId {
    wal_slot(match signal_type {
        SignalType::Logs => otlp_slots::OTLP_LOGS,
        SignalType::Traces => otlp_slots::OTLP_TRACES,
        SignalType::Metrics => otlp_slots::OTLP_METRICS,
    })
}

/// Check if a slot ID is an OTLP opaque binary slot
const fn is_otlp_slot(slot: SlotId) -> Option<SignalType> {
    match slot.raw() {
        otlp_slots::OTLP_LOGS => Some(SignalType::Logs),
        otlp_slots::OTLP_TRACES => Some(SignalType::Traces),
        otlp_slots::OTLP_METRICS => Some(SignalType::Metrics),
        _ => None,
    }
}

/// Determine signal type from a slot ID (works for both Arrow and OTLP slots).
///
/// Returns `None` for shared slots (RESOURCE_ATTRS, SCOPE_ATTRS) or unknown
/// slot IDs.
pub(crate) fn signal_type_from_slot_id(slot: SlotId) -> Option<SignalType> {
    // Check OTLP opaque slots first
    if let Some(st) = is_otlp_slot(slot) {
        return Some(st);
    }
    // Then the root slot of a pluggable Arrow record set
    if let Some(st) = pluggable_root_signal(slot) {
        return Some(st);
    }
    // Fall back to Arrow slot mapping
    from_slot_id(slot).map(|(st, _)| st)
}

/// Check if a slot ID represents a shared payload type (RESOURCE_ATTRS, SCOPE_ATTRS).
///
/// These slots are used by ALL signal types, so their presence alone cannot
/// determine the signal type of a bundle.
const fn is_shared_slot(slot: SlotId) -> bool {
    match slot_to_payload_type(slot) {
        Some(payload_type) => matches!(
            payload_signal_type(payload_type),
            Some(PayloadSignal::Shared)
        ),
        None => false,
    }
}

/// Convert a slot ID back to signal type and payload type (Arrow format only).
///
/// This reverse mapping is used during WAL recovery to reconstruct the signal
/// type without relying on protobuf number ranges.
///
/// # Returns
/// - `Some((signal_type, payload_type))` for signal-specific slots
/// - `None` for shared slots (RESOURCE_ATTRS, SCOPE_ATTRS) since they're used by all signals
/// - `None` for OTLP opaque slots (60-62) or invalid slot IDs
fn from_slot_id(slot: SlotId) -> Option<(SignalType, ArrowPayloadType)> {
    let payload_type = slot_to_payload_type(slot)?;
    let signal_type = match payload_signal_type(payload_type)? {
        PayloadSignal::Shared => return None,
        PayloadSignal::Signal(signal_type) => signal_type,
    };

    Some((signal_type, payload_type))
}

/// Get the slot label for a signal and payload type
fn slot_label(signal_type: SignalType, payload_type: ArrowPayloadType) -> Cow<'static, str> {
    let signal_prefix = match signal_type {
        SignalType::Logs => "Log",
        SignalType::Traces => "Trace",
        SignalType::Metrics => "Metric",
    };
    Cow::Owned(format!("{}:{}", signal_prefix, payload_type.as_str_name()))
}

/// Get the slot label for an OTLP opaque slot
const fn otlp_slot_label(signal_type: SignalType) -> Cow<'static, str> {
    Cow::Borrowed(match signal_type {
        SignalType::Logs => "OtlpLogs",
        SignalType::Traces => "OtlpTraces",
        SignalType::Metrics => "OtlpMetrics",
    })
}

/// Schema for OTLP opaque binary storage (single binary column)
fn otlp_binary_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "data",
        DataType::Binary,
        false,
    )]))
}

static OTLP_SCHEMA_FINGERPRINT: LazyLock<SchemaFingerprint> = LazyLock::new(|| {
    let hash = blake3::hash(b"otlp_binary_v1");
    *hash.as_bytes()
});

fn otlp_schema_fingerprint() -> SchemaFingerprint {
    *OTLP_SCHEMA_FINGERPRINT
}

/// Compute schema fingerprint from a RecordBatch using blake3.
fn compute_schema_fingerprint(batch: &RecordBatch) -> SchemaFingerprint {
    let schema = batch.schema();
    let mut builder = SchemaIdBuilder::new();
    let schema_id = builder.build_id(&schema);
    let hash = blake3::hash(schema_id.as_bytes());
    *hash.as_bytes()
}

// -----------------------------------------------------------------------------
// OtapRecordBundleAdapter
// -----------------------------------------------------------------------------

/// Adapter that wraps OtapArrowRecords and implements Quiver's RecordBundle trait.
///
/// The original records can be recovered via `into_inner()` for use in NACK scenarios.
pub struct OtapRecordBundleAdapter {
    /// The underlying OTAP arrow records
    records: OtapArrowRecords,
    /// The signal type (Logs, Traces, or Metrics)
    signal_type: SignalType,
    /// Cached bundle descriptor
    descriptor: BundleDescriptor,
    /// Ingestion timestamp
    ingestion_time: SystemTime,
}

impl OtapRecordBundleAdapter {
    /// Create a new adapter for the given OtapArrowRecords.
    #[must_use]
    pub fn new(records: OtapArrowRecords) -> Self {
        let signal_type = match &records {
            OtapArrowRecords::Logs(_) => SignalType::Logs,
            OtapArrowRecords::Traces(_) => SignalType::Traces,
            OtapArrowRecords::Metrics(_) => SignalType::Metrics,
        };
        let descriptor = Self::build_descriptor(&records, signal_type);
        Self {
            records,
            signal_type,
            descriptor,
            ingestion_time: SystemTime::now(),
        }
    }

    /// Consume the adapter and return the original OtapArrowRecords.
    ///
    /// This is useful for NACK scenarios where the original payload must be
    /// returned to the upstream after an ingest failure.
    #[must_use]
    pub fn into_inner(self) -> OtapArrowRecords {
        self.records
    }

    /// Build the bundle descriptor from the OTAP payload.
    fn build_descriptor(records: &OtapArrowRecords, signal_type: SignalType) -> BundleDescriptor {
        let mut slot_descriptors = Vec::new();

        for payload_type in records.allowed_payload_types() {
            if records.get(*payload_type).is_some() {
                let slot_id = to_slot_id(signal_type, *payload_type);
                slot_descriptors.push(SlotDescriptor::new(
                    slot_id,
                    slot_label(signal_type, *payload_type),
                ));
            }
        }

        BundleDescriptor::new(slot_descriptors)
    }
}

impl RecordBundle for OtapRecordBundleAdapter {
    fn descriptor(&self) -> &BundleDescriptor {
        &self.descriptor
    }

    fn ingestion_time(&self) -> SystemTime {
        self.ingestion_time
    }

    fn payload(&self, slot: SlotId) -> Option<PayloadRef<'_>> {
        // Get the payload type from the slot ID
        let payload_type = slot_to_payload_type(slot)?;

        // For signal-specific slots, verify the signal type matches
        if let Some((signal_type, _)) = from_slot_id(slot)
            && signal_type != self.signal_type
        {
            return None;
        }
        // Shared slots (RESOURCE_ATTRS, SCOPE_ATTRS) are allowed for any signal type

        let batch = self.records.get(payload_type)?;

        Some(PayloadRef {
            schema_fingerprint: compute_schema_fingerprint(batch),
            batch,
        })
    }

    fn item_count(&self) -> u64 {
        self.records.num_items() as u64
    }

    fn byte_count(&self) -> Option<u64> {
        self.records
            .num_bytes()
            .and_then(|bytes| u64::try_from(bytes).ok())
    }
}

// -----------------------------------------------------------------------------
// OtlpBytesAdapter (for pass-through mode)
// -----------------------------------------------------------------------------

/// Adapter that wraps OtlpProtoBytes and implements Quiver's RecordBundle trait.
///
/// This stores OTLP data as opaque binary for efficient pass-through pipelines.
/// The original bytes can be recovered via `into_inner()` for use in NACK scenarios.
pub struct OtlpBytesAdapter {
    /// The original OTLP bytes (preserved for recovery on NACK)
    bytes: OtlpProtoBytes,
    /// The signal type (Logs, Traces, or Metrics)
    signal_type: SignalType,
    /// The record batch containing the binary data
    batch: RecordBatch,
    /// Cached bundle descriptor
    descriptor: BundleDescriptor,
    /// Ingestion timestamp
    ingestion_time: SystemTime,
    /// Cached item count (computed once by scanning the protobuf wire
    /// format without full deserialization, to avoid repeated O(n) scans
    /// on the hot path).
    cached_item_count: u64,
}

impl OtlpBytesAdapter {
    /// Create a new adapter for the given OtlpProtoBytes.
    ///
    /// # Errors
    ///
    /// Returns the original bytes along with the error if the Arrow RecordBatch
    /// cannot be created, allowing the caller to NACK without cloning.
    pub fn new(bytes: OtlpProtoBytes) -> Result<Self, (BundleConversionError, OtlpProtoBytes)> {
        let signal_type = match &bytes {
            OtlpProtoBytes::ExportLogsRequest(_) => SignalType::Logs,
            OtlpProtoBytes::ExportMetricsRequest(_) => SignalType::Metrics,
            OtlpProtoBytes::ExportTracesRequest(_) => SignalType::Traces,
        };

        // Create a record batch with a single binary column containing the OTLP bytes.
        // Use zero-copy wrapping: clone_bytes() is just an Arc refcount bump,
        // and Buffer::from(Bytes) wraps without copying the payload data.
        let data_bytes = bytes.clone_bytes();
        let len = i32::try_from(data_bytes.len()).map_err(|_| {
            (
                BundleConversionError::RecordBatchCreationError(format!(
                    "OTLP payload too large for BinaryArray: {} bytes exceeds i32::MAX",
                    data_bytes.len()
                )),
                bytes.clone(),
            )
        })?;
        let data_buffer = Buffer::from(data_bytes);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, len]));
        let binary_array = BinaryArray::new(offsets, data_buffer, None);
        let batch = match RecordBatch::try_new(otlp_binary_schema(), vec![Arc::new(binary_array)]) {
            Ok(batch) => batch,
            Err(e) => {
                return Err((
                    BundleConversionError::RecordBatchCreationError(e.to_string()),
                    bytes,
                ));
            }
        };

        let slot_id = to_otlp_slot_id(signal_type);
        let descriptor = BundleDescriptor::new(vec![SlotDescriptor::new(
            slot_id,
            otlp_slot_label(signal_type),
        )]);

        // Compute item count once up front. The wire-format scan in
        // num_items() traverses the protobuf structure without full
        // deserialization and is O(message_size), so we cache it to
        // avoid a second scan when Quiver's open_segment calls item_count().
        let cached_item_count = bytes.num_items() as u64;

        Ok(Self {
            bytes,
            signal_type,
            batch,
            descriptor,
            ingestion_time: SystemTime::now(),
            cached_item_count,
        })
    }

    /// Consume the adapter and return the original OtlpProtoBytes.
    ///
    /// This is useful for NACK scenarios where the original payload must be
    /// returned to the upstream after an ingest failure.
    #[must_use]
    pub fn into_inner(self) -> OtlpProtoBytes {
        self.bytes
    }

    /// Returns the cached item count (computed once during construction).
    #[must_use]
    pub fn cached_item_count(&self) -> u64 {
        self.cached_item_count
    }
}

impl RecordBundle for OtlpBytesAdapter {
    fn descriptor(&self) -> &BundleDescriptor {
        &self.descriptor
    }

    fn ingestion_time(&self) -> SystemTime {
        self.ingestion_time
    }

    fn payload(&self, slot: SlotId) -> Option<PayloadRef<'_>> {
        // Check if this is our OTLP slot
        let slot_signal_type = is_otlp_slot(slot)?;
        if slot_signal_type != self.signal_type {
            return None;
        }

        Some(PayloadRef {
            schema_fingerprint: otlp_schema_fingerprint(),
            batch: &self.batch,
        })
    }

    fn item_count(&self) -> u64 {
        self.cached_item_count
    }

    fn byte_count(&self) -> Option<u64> {
        u64::try_from(self.bytes.num_bytes()).ok()
    }
}

// -----------------------------------------------------------------------------
// PdataArrowBundleAdapter (pluggable Arrow record sets)
// -----------------------------------------------------------------------------

/// Magic prefix and format version of the pluggable Arrow bundle manifest.
const PLUGGABLE_MANIFEST_MAGIC: [u8; 4] = *b"QPA\x01";

/// One table entry of a pluggable Arrow bundle manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PluggableTableEntry {
    slot: SlotId,
    table_id: u32,
    schema_fingerprint: [u8; 32],
    schema_id: String,
}

/// Versioned manifest persisted as Quiver bundle metadata for a pluggable
/// Arrow record set.
///
/// Layout (little-endian):
///
/// ```text
/// [u8;4] magic "QPA" + format version 1
/// u16 representation_id_len, [u8] representation_id (ASCII)
/// u16 version_major, u16 version_minor
/// u8  signal (0 = logs, 1 = metrics, 2 = traces)
/// u64 root_items
/// u8  table_count
/// table_count x { u16 slot, u32 table_id, [u8;32] schema_fingerprint,
///                 u32 schema_id_len, [u8] schema_id (UTF-8) }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
struct PluggableManifest {
    representation: PdataRepresentation,
    signal_type: SignalType,
    root_items: u64,
    tables: Vec<PluggableTableEntry>,
}

const fn signal_code(signal_type: SignalType) -> u8 {
    match signal_type {
        SignalType::Logs => 0,
        SignalType::Metrics => 1,
        SignalType::Traces => 2,
    }
}

const fn signal_from_code(code: u8) -> Option<SignalType> {
    match code {
        0 => Some(SignalType::Logs),
        1 => Some(SignalType::Metrics),
        2 => Some(SignalType::Traces),
        _ => None,
    }
}

impl PluggableManifest {
    fn encode(&self) -> Result<Vec<u8>, BundleConversionError> {
        let id = self.representation.id().as_str().as_bytes();
        let id_len = u16::try_from(id.len()).map_err(|_| {
            BundleConversionError::InvalidPluggableManifest(
                "representation ID is longer than 65535 bytes".to_owned(),
            )
        })?;
        let table_count = u8::try_from(self.tables.len()).map_err(|_| {
            BundleConversionError::InvalidPluggableManifest("too many tables".to_owned())
        })?;
        let version = self.representation.version();
        let mut out = Vec::with_capacity(32 + id.len() + self.tables.len() * 48);
        out.extend_from_slice(&PLUGGABLE_MANIFEST_MAGIC);
        out.extend_from_slice(&id_len.to_le_bytes());
        out.extend_from_slice(id);
        out.extend_from_slice(&version.major().to_le_bytes());
        out.extend_from_slice(&version.minor().to_le_bytes());
        out.push(signal_code(self.signal_type));
        out.extend_from_slice(&self.root_items.to_le_bytes());
        out.push(table_count);
        for table in &self.tables {
            let schema_id = table.schema_id.as_bytes();
            let schema_id_len = u32::try_from(schema_id.len()).map_err(|_| {
                BundleConversionError::InvalidPluggableManifest(
                    "schema ID is longer than u32::MAX bytes".to_owned(),
                )
            })?;
            out.extend_from_slice(&table.slot.raw().to_le_bytes());
            out.extend_from_slice(&table.table_id.to_le_bytes());
            out.extend_from_slice(&table.schema_fingerprint);
            out.extend_from_slice(&schema_id_len.to_le_bytes());
            out.extend_from_slice(schema_id);
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, BundleConversionError> {
        let mut reader = ManifestReader { bytes, position: 0 };
        if reader.take(PLUGGABLE_MANIFEST_MAGIC.len())? != PLUGGABLE_MANIFEST_MAGIC {
            return Err(BundleConversionError::InvalidPluggableManifest(
                "unknown bundle metadata format".to_owned(),
            ));
        }
        let id_len = usize::from(reader.u16()?);
        let id = std::str::from_utf8(reader.take(id_len)?)
            .map_err(|_| {
                BundleConversionError::InvalidPluggableManifest(
                    "representation ID is not UTF-8".to_owned(),
                )
            })?
            .to_owned();
        let major = reader.u16()?;
        let minor = reader.u16()?;
        let signal_type = signal_from_code(reader.u8()?).ok_or_else(|| {
            BundleConversionError::InvalidPluggableManifest("unknown signal code".to_owned())
        })?;
        let root_items = reader.u64()?;
        let table_count = usize::from(reader.u8()?);
        if table_count > MAX_PLUGGABLE_TABLES {
            return Err(BundleConversionError::InvalidPluggableManifest(format!(
                "manifest lists {table_count} tables, more than {MAX_PLUGGABLE_TABLES}"
            )));
        }
        let mut tables = Vec::with_capacity(table_count);
        for index in 0..table_count {
            let slot = SlotId::new(reader.u16()?);
            if pluggable_slot(signal_type, index) != Some(slot) {
                return Err(BundleConversionError::InvalidPluggableManifest(format!(
                    "table {index} uses unexpected slot {}",
                    slot.raw()
                )));
            }
            let table_id = reader.u32()?;
            let mut schema_fingerprint = [0u8; 32];
            schema_fingerprint.copy_from_slice(reader.take(32)?);
            let schema_id_len = usize::try_from(reader.u32()?).map_err(|_| {
                BundleConversionError::InvalidPluggableManifest(
                    "schema ID length overflow".to_owned(),
                )
            })?;
            let schema_id = std::str::from_utf8(reader.take(schema_id_len)?)
                .map_err(|_| {
                    BundleConversionError::InvalidPluggableManifest(
                        "schema ID is not UTF-8".to_owned(),
                    )
                })?
                .to_owned();
            tables.push(PluggableTableEntry {
                slot,
                table_id,
                schema_fingerprint,
                schema_id,
            });
        }
        if reader.position != bytes.len() {
            return Err(BundleConversionError::InvalidPluggableManifest(
                "unexpected trailing bytes".to_owned(),
            ));
        }
        Ok(Self {
            representation: PdataRepresentation::new(
                PdataRepresentationId::from(id),
                RepresentationVersion::new(major, minor),
            ),
            signal_type,
            root_items,
            tables,
        })
    }

    /// Rebuilds the original record set from the manifest and the slot payloads.
    fn into_record_set(
        self,
        payloads: &HashMap<SlotId, RecordBatch>,
    ) -> Result<PdataArrowRecordSet, BundleConversionError> {
        if payloads.len() != self.tables.len() {
            return Err(BundleConversionError::InvalidPluggableManifest(format!(
                "manifest lists {} tables but the bundle holds {} payloads",
                self.tables.len(),
                payloads.len()
            )));
        }
        let mut tables = Vec::with_capacity(self.tables.len());
        for entry in self.tables {
            let batch = payloads.get(&entry.slot).cloned().ok_or_else(|| {
                BundleConversionError::InvalidPluggableManifest(format!(
                    "missing payload for slot {}",
                    entry.slot.raw()
                ))
            })?;
            let table = PdataArrowTable::new(
                entry.table_id,
                PdataSchemaId::from(entry.schema_id),
                otel_arrow_dfe_pdata::SchemaFingerprint::new(entry.schema_fingerprint),
                batch,
            )
            .map_err(|error| BundleConversionError::InvalidPluggableManifest(error.to_string()))?;
            tables.push(table);
        }
        let root_items = usize::try_from(self.root_items).map_err(|_| {
            BundleConversionError::InvalidPluggableManifest("root item count overflow".to_owned())
        })?;
        PdataArrowRecordSet::new(self.representation, self.signal_type, root_items, tables)
            .map_err(|error| BundleConversionError::InvalidPluggableManifest(error.to_string()))
    }
}

/// Bounds-checked little-endian reader for manifest bytes.
struct ManifestReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> ManifestReader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], BundleConversionError> {
        let end = self
            .position
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| {
                BundleConversionError::InvalidPluggableManifest("manifest truncated".to_owned())
            })?;
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, BundleConversionError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, BundleConversionError> {
        let mut raw = [0u8; 2];
        raw.copy_from_slice(self.take(2)?);
        Ok(u16::from_le_bytes(raw))
    }

    fn u32(&mut self) -> Result<u32, BundleConversionError> {
        let mut raw = [0u8; 4];
        raw.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(raw))
    }

    fn u64(&mut self) -> Result<u64, BundleConversionError> {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(raw))
    }
}

/// Adapter that persists a pluggable Arrow record set (`PdataArrowRecordSet`)
/// through Quiver.
///
/// Each table is stored in its own slot with the adapter-supplied schema
/// fingerprint. The representation identity, signal, root item count, and
/// table/schema identities travel in a versioned manifest stored as Quiver
/// bundle metadata. The original record set can be recovered via
/// `into_inner()` for NACK scenarios.
pub struct PdataArrowBundleAdapter {
    /// The underlying pluggable record set.
    records: Box<PdataArrowRecordSet>,
    /// Table IDs in slot order.
    table_ids: Vec<u32>,
    /// Slots in table order.
    slots: Vec<SlotId>,
    /// Cached bundle descriptor.
    descriptor: BundleDescriptor,
    /// Encoded manifest persisted as bundle metadata.
    manifest: Vec<u8>,
    /// Ingestion timestamp.
    ingestion_time: SystemTime,
}

impl PdataArrowBundleAdapter {
    /// Creates an adapter for the given record set.
    ///
    /// # Errors
    ///
    /// Returns the original record set with the error when it exceeds the
    /// table count or manifest size Quiver can persist, so the caller can NACK
    /// without cloning.
    pub fn new(
        records: Box<PdataArrowRecordSet>,
    ) -> Result<Self, (BundleConversionError, Box<PdataArrowRecordSet>)> {
        let table_count = records.tables().len();
        if table_count > MAX_PLUGGABLE_TABLES {
            let error = BundleConversionError::PersistenceLimitExceeded {
                representation: records.representation().clone(),
                limit: QuiverPersistenceLimit::TableCount,
                actual: table_count,
                max: MAX_PLUGGABLE_TABLES,
            };
            return Err((error, records));
        }

        let signal_type = records.signal_type();
        // The table count was bounded above, so every index has a reserved slot.
        let slots = (0..table_count)
            .filter_map(|index| pluggable_slot(signal_type, index))
            .collect::<Vec<_>>();
        let mut table_ids = Vec::with_capacity(table_count);
        let mut entries = Vec::with_capacity(table_count);
        for (table, slot) in records.tables().zip(slots.iter().copied()) {
            table_ids.push(table.table_id());
            entries.push(PluggableTableEntry {
                slot,
                table_id: table.table_id(),
                schema_fingerprint: *table.schema_fingerprint().as_bytes(),
                schema_id: table.schema_id().as_str().to_owned(),
            });
        }

        let manifest = PluggableManifest {
            representation: records.representation().clone(),
            signal_type,
            root_items: records.num_items() as u64,
            tables: entries,
        };
        let manifest = match manifest.encode() {
            Ok(manifest) => manifest,
            Err(error) => return Err((error, records)),
        };
        if manifest.len() > MAX_BUNDLE_METADATA_BYTES {
            let error = BundleConversionError::PersistenceLimitExceeded {
                representation: records.representation().clone(),
                limit: QuiverPersistenceLimit::ManifestBytes,
                actual: manifest.len(),
                max: MAX_BUNDLE_METADATA_BYTES,
            };
            return Err((error, records));
        }

        let descriptor = BundleDescriptor::new(
            slots
                .iter()
                .map(|slot| SlotDescriptor::new(*slot, "PluggableArrow"))
                .collect(),
        );
        Ok(Self {
            records,
            table_ids,
            slots,
            descriptor,
            manifest,
            ingestion_time: SystemTime::now(),
        })
    }

    /// Consume the adapter and return the original record set.
    #[must_use]
    pub fn into_inner(self) -> Box<PdataArrowRecordSet> {
        self.records
    }
}

impl RecordBundle for PdataArrowBundleAdapter {
    fn descriptor(&self) -> &BundleDescriptor {
        &self.descriptor
    }

    fn ingestion_time(&self) -> SystemTime {
        self.ingestion_time
    }

    fn payload(&self, slot: SlotId) -> Option<PayloadRef<'_>> {
        let index = self.slots.iter().position(|candidate| *candidate == slot)?;
        let table = self.records.table(self.table_ids[index])?;
        Some(PayloadRef {
            schema_fingerprint: *table.schema_fingerprint().as_bytes(),
            batch: table.batch(),
        })
    }

    fn item_count(&self) -> u64 {
        self.records.num_items() as u64
    }

    fn byte_count(&self) -> Option<u64> {
        self.records
            .logical_arrow_bytes()
            .ok()
            .and_then(|bytes| u64::try_from(bytes).ok())
    }

    fn metadata(&self) -> Option<&[u8]> {
        Some(&self.manifest)
    }
}

/// Collects the populated slot payloads of a bundle.
fn bundle_payloads(bundle: &dyn RecordBundle) -> HashMap<SlotId, RecordBatch> {
    bundle
        .descriptor()
        .slots
        .iter()
        .filter_map(|slot| {
            bundle
                .payload(slot.id)
                .map(|payload| (slot.id, payload.batch.clone()))
        })
        .collect()
}

// -----------------------------------------------------------------------------
// ReconstructedBundle to OtapPdata conversion
// -----------------------------------------------------------------------------

/// Error returned when converting a bundle to OtapPdata fails.
#[derive(Debug, Clone, thiserror::Error)]
pub enum BundleConversionError {
    /// The bundle contains no payloads.
    #[error("empty bundle")]
    EmptyBundle,
    /// Could not determine the signal type from the slots present.
    #[error("could not determine signal type from slots")]
    UnknownSignalType,
    /// Failed to extract binary data from OTLP slot.
    #[error("failed to extract OTLP bytes: {0}")]
    OtlpExtractionError(String),
    /// Failed to create Arrow RecordBatch for OTLP storage.
    #[error("failed to create OTLP storage batch: {0}")]
    RecordBatchCreationError(String),
    /// A pluggable record set exceeds what Quiver can persist.
    #[error(
        "pdata representation `{representation}` exceeds the Quiver {limit} limit ({actual} > {max})"
    )]
    PersistenceLimitExceeded {
        /// Representation that cannot be persisted.
        representation: PdataRepresentation,
        /// Limit that was exceeded.
        limit: QuiverPersistenceLimit,
        /// Observed value.
        actual: usize,
        /// Supported maximum.
        max: usize,
    },
    /// Persisted pluggable bundle metadata could not be decoded or does not
    /// match the bundle payloads.
    #[error("invalid pluggable Arrow bundle manifest: {0}")]
    InvalidPluggableManifest(String),
}

/// Limits on pluggable Arrow record sets persisted through Quiver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuiverPersistenceLimit {
    /// Number of tables in one record set.
    TableCount,
    /// Encoded size of the representation manifest.
    ManifestBytes,
}

impl std::fmt::Display for QuiverPersistenceLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableCount => formatter.write_str("table count"),
            Self::ManifestBytes => formatter.write_str("manifest size"),
        }
    }
}

/// Check if the bundle contains an OTLP opaque slot.
fn find_otlp_slot(payloads: &HashMap<SlotId, RecordBatch>) -> Option<(SignalType, &RecordBatch)> {
    for (slot_id, batch) in payloads {
        if let Some(signal_type) = is_otlp_slot(*slot_id) {
            return Some((signal_type, batch));
        }
    }
    None
}

/// Determine signal type from the Arrow slots present in the bundle.
fn determine_signal_type(
    payloads: &HashMap<SlotId, RecordBatch>,
) -> Result<SignalType, BundleConversionError> {
    for slot_id in payloads.keys() {
        if let Some((signal_type, _)) = from_slot_id(*slot_id) {
            return Ok(signal_type);
        }
    }
    Err(BundleConversionError::UnknownSignalType)
}

/// Extract OTLP bytes from an OTLP bundle's record batch.
fn extract_otlp_bytes(
    signal_type: SignalType,
    batch: &RecordBatch,
) -> Result<OtlpProtoBytes, BundleConversionError> {
    // The batch should have a single binary column
    if batch.num_columns() != 1 {
        return Err(BundleConversionError::OtlpExtractionError(format!(
            "expected 1 column, got {}",
            batch.num_columns()
        )));
    }
    if batch.num_rows() != 1 {
        return Err(BundleConversionError::OtlpExtractionError(format!(
            "expected 1 row, got {}",
            batch.num_rows()
        )));
    }

    let column = batch.column(0);
    let binary_array = column
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            BundleConversionError::OtlpExtractionError("expected BinaryArray".to_string())
        })?;

    let bytes = binary_array.value(0).to_vec();
    Ok(OtlpProtoBytes::new_from_bytes(signal_type, bytes))
}

/// Recovers the logical item count from a decoded Quiver bundle.
///
/// This is used only for WAL entries that expire during startup, where the
/// existing WAL format does not persist `RecordBundle::item_count()`.
pub fn recover_item_count(bundle: &dyn RecordBundle) -> Option<u64> {
    if let Some(metadata) = bundle.metadata() {
        return PluggableManifest::decode(metadata)
            .ok()
            .map(|manifest| manifest.root_items);
    }
    let payloads = bundle_payloads(bundle);

    if let Some((signal_type, batch)) = find_otlp_slot(&payloads) {
        return extract_otlp_bytes(signal_type, batch)
            .ok()
            .map(|bytes| bytes.num_items() as u64);
    }

    let signal_type = determine_signal_type(&payloads).ok()?;
    let records = match signal_type {
        SignalType::Logs => create_records::<Logs>(&payloads).ok()?,
        SignalType::Traces => create_records::<Traces>(&payloads).ok()?,
        SignalType::Metrics => create_records::<Metrics>(&payloads).ok()?,
    };
    Some(records.num_items() as u64)
}

/// Recovers the logical byte count from a decoded Quiver bundle.
///
/// This is used for WAL entries because the WAL format does not persist
/// [`RecordBundle::byte_count`].
pub fn recover_byte_count(bundle: &dyn RecordBundle) -> Option<u64> {
    let payloads = bundle_payloads(bundle);
    if let Some(metadata) = bundle.metadata() {
        return PluggableManifest::decode(metadata)
            .and_then(|manifest| manifest.into_record_set(&payloads))
            .ok()
            .and_then(|records| records.logical_arrow_bytes().ok())
            .and_then(|bytes| u64::try_from(bytes).ok());
    }

    if let Some((signal_type, batch)) = find_otlp_slot(&payloads) {
        return extract_otlp_bytes(signal_type, batch)
            .ok()
            .and_then(|bytes| u64::try_from(bytes.num_bytes()).ok());
    }

    let signal_type = determine_signal_type(&payloads).ok()?;
    let records = match signal_type {
        SignalType::Logs => create_records::<Logs>(&payloads).ok()?,
        SignalType::Traces => create_records::<Traces>(&payloads).ok()?,
        SignalType::Metrics => create_records::<Metrics>(&payloads).ok()?,
    };
    records
        .num_bytes()
        .and_then(|bytes| u64::try_from(bytes).ok())
}

/// Convert a ReconstructedBundle back to OtapPdata.
///
/// This reconstructs the original telemetry data from Quiver's storage format.
/// It handles pluggable Arrow record sets (identified by their bundle
/// manifest), Arrow-format OTAP bundles, and OTLP opaque bundles.
pub fn convert_bundle_to_pdata(
    bundle: &ReconstructedBundle,
) -> Result<OtapPdata, BundleConversionError> {
    let payloads = bundle.payloads();

    if let Some(metadata) = bundle.metadata() {
        let records = PluggableManifest::decode(metadata)?.into_record_set(payloads)?;
        return Ok(OtapPdata::new(
            Context::default(),
            OtapPayload::from_pdata_arrow(records),
        ));
    }

    if payloads.is_empty() {
        return Err(BundleConversionError::EmptyBundle);
    }

    // First, check if this is an OTLP opaque bundle
    if let Some((signal_type, batch)) = find_otlp_slot(payloads) {
        let otlp_bytes = extract_otlp_bytes(signal_type, batch)?;
        let payload = OtapPayload::from(otlp_bytes);
        return Ok(OtapPdata::new(Context::default(), payload));
    }

    // Otherwise, it's an Arrow-format bundle
    let signal_type = determine_signal_type(payloads)?;

    // Create the appropriate OtapArrowRecords variant
    let records = match signal_type {
        SignalType::Logs => create_records::<Logs>(payloads)?,
        SignalType::Traces => create_records::<Traces>(payloads)?,
        SignalType::Metrics => create_records::<Metrics>(payloads)?,
    };

    // Wrap in OtapPayload and OtapPdata
    let payload = OtapPayload::from(records);
    Ok(OtapPdata::new(Context::default(), payload))
}

fn create_records<T>(
    payloads: &HashMap<SlotId, RecordBatch>,
) -> Result<OtapArrowRecords, BundleConversionError>
where
    T: OtapBatchStore + Into<OtapArrowRecords>,
{
    let mut store = T::default();

    for (slot_id, batch) in payloads {
        // Include signal-specific slots (any signal type)
        if let Some((signal_type, payload_type)) = from_slot_id(*slot_id) {
            debug_assert_eq!(signal_type, T::SIGNAL_TYPE, "Signal type should match slot");
            store
                .set(payload_type, batch.clone())
                .map_err(|e| BundleConversionError::RecordBatchCreationError(e.to_string()))?;
        }
        // Also include shared slots (RESOURCE_ATTRS, SCOPE_ATTRS)
        else if let Some(payload_type) = slot_to_payload_type(*slot_id)
            && is_shared_slot(*slot_id)
        {
            store
                .set(payload_type, batch.clone())
                .map_err(|e| BundleConversionError::RecordBatchCreationError(e.to_string()))?;
        }
    }

    Ok(store.into())
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;
    use otel_arrow_dfe_pdata::otap::OtapBatchStore;
    use otel_arrow_dfe_pdata::{SchemaFingerprint, logs, metrics, record_batch, traces};

    /// Helper to extract a single batch from an OtapArrowRecords.
    fn extract_batch(records: &OtapArrowRecords, payload_type: ArrowPayloadType) -> RecordBatch {
        records
            .get(payload_type)
            .unwrap_or_else(|| panic!("Expected batch for {payload_type:?}"))
            .clone()
    }

    fn pluggable_representation() -> PdataRepresentation {
        PdataRepresentation::new(
            PdataRepresentationId::new("example.logs"),
            RepresentationVersion::new(1, 2),
        )
    }

    fn pluggable_table(table_id: u32, schema_id: &'static str, values: &[u32]) -> PdataArrowTable {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::UInt32Array::from(values.to_vec()))],
        )
        .expect("valid batch");
        PdataArrowTable::new(
            table_id,
            PdataSchemaId::new(schema_id),
            SchemaFingerprint::new([table_id as u8; 32]),
            batch,
        )
        .expect("valid arbitrary Arrow table")
    }

    fn pluggable_records(signal_type: SignalType, table_count: u32) -> PdataArrowRecordSet {
        let tables = (0..table_count)
            .map(|index| pluggable_table(100 - index, "example.logs.table", &[index, index + 1]));
        PdataArrowRecordSet::new(pluggable_representation(), signal_type, 2, tables)
            .expect("valid arbitrary Arrow record set")
    }

    /// Simulates Quiver persistence: the payloads and metadata an engine stores
    /// for the adapter.
    fn persisted(adapter: &PdataArrowBundleAdapter) -> (HashMap<SlotId, RecordBatch>, Vec<u8>) {
        (
            bundle_payloads(adapter),
            adapter.metadata().expect("manifest").to_vec(),
        )
    }

    /// Scenario: A two-table pluggable Arrow record set is persisted through the adapter.
    /// Guarantees: The stored slots, fingerprints, and manifest rebuild a record set
    /// identical to the original, including representation identity and table/schema IDs.
    #[test]
    fn pluggable_record_set_round_trips_through_manifest() {
        let records = pluggable_records(SignalType::Logs, 2);
        let adapter = PdataArrowBundleAdapter::new(Box::new(records.clone())).expect("adapter");

        let slots = adapter
            .descriptor()
            .slots
            .iter()
            .map(|slot| slot.id.raw())
            .collect::<Vec<_>>();
        assert_eq!(slots, vec![46, 49]);
        assert_eq!(adapter.item_count(), 2);
        let root = adapter.payload(SlotId::new(46)).expect("root payload");
        assert_eq!(root.schema_fingerprint, [99; 32]);

        let (payloads, metadata) = persisted(&adapter);
        let rebuilt = PluggableManifest::decode(&metadata)
            .and_then(|manifest| manifest.into_record_set(&payloads))
            .expect("rebuild record set");
        assert_eq!(rebuilt, records);
        assert_eq!(rebuilt.representation().to_string(), "example.logs@1.2");
    }

    /// Scenario: Pluggable record sets of each signal are persisted.
    /// Guarantees: The root slot identifies the signal, so slot-based queued and
    /// loss metrics attribute pluggable items to the right signal.
    #[test]
    fn pluggable_root_slot_reports_record_set_signal() {
        for signal_type in [SignalType::Logs, SignalType::Metrics, SignalType::Traces] {
            let adapter = PdataArrowBundleAdapter::new(Box::new(pluggable_records(signal_type, 3)))
                .expect("adapter");
            let root = adapter.descriptor().slots[0].id;
            assert_eq!(signal_type_from_slot_id(root), Some(signal_type));
            for extra in &adapter.descriptor().slots[1..] {
                assert_eq!(signal_type_from_slot_id(extra.id), None);
            }
        }
    }

    /// Scenario: Every slot reserved for pluggable record sets is inspected.
    /// Guarantees: None of them collides with an OTAP Arrow payload slot or an OTLP slot.
    #[test]
    fn pluggable_slots_do_not_collide_with_arrow_or_otlp_slots() {
        let mut reserved = Vec::new();
        for signal_type in [SignalType::Logs, SignalType::Metrics, SignalType::Traces] {
            for index in 0..MAX_PLUGGABLE_TABLES {
                reserved.push(pluggable_slot(signal_type, index).expect("reserved slot"));
            }
            assert!(pluggable_slot(signal_type, MAX_PLUGGABLE_TABLES).is_none());
        }
        for slot in reserved {
            assert!(slot.raw() < WAL_SLOT_COUNT);
            assert!(slot_to_payload_type(slot).is_none(), "slot {}", slot.raw());
            assert!(is_otlp_slot(slot).is_none(), "slot {}", slot.raw());
        }
        assert_eq!(MAX_PLUGGABLE_TABLES, 12);
    }

    /// Scenario: A record set has more tables than the reserved slot range holds.
    /// Guarantees: The adapter returns a typed limit error and hands back the
    /// untouched record set so the processor can NACK without data loss.
    #[test]
    fn pluggable_record_set_with_too_many_tables_is_rejected() {
        let records = pluggable_records(SignalType::Logs, (MAX_PLUGGABLE_TABLES + 1) as u32);
        let Err((error, returned)) = PdataArrowBundleAdapter::new(Box::new(records.clone())) else {
            panic!("expected table-count rejection");
        };
        assert!(matches!(
            error,
            BundleConversionError::PersistenceLimitExceeded {
                limit: QuiverPersistenceLimit::TableCount,
                actual: 13,
                max: 12,
                ..
            }
        ));
        assert_eq!(*returned, records);
    }

    /// Scenario: A record set whose schema ID makes the manifest exceed Quiver's
    /// bundle metadata bound is persisted.
    /// Guarantees: The adapter rejects it with a typed manifest-size error.
    #[test]
    fn pluggable_record_set_with_oversized_manifest_is_rejected() {
        let schema_id: &'static str =
            Box::leak("s".repeat(MAX_BUNDLE_METADATA_BYTES).into_boxed_str());
        let records = PdataArrowRecordSet::new(
            pluggable_representation(),
            SignalType::Logs,
            1,
            [pluggable_table(1, schema_id, &[1])],
        )
        .expect("record set");
        assert!(matches!(
            PdataArrowBundleAdapter::new(Box::new(records)),
            Err((
                BundleConversionError::PersistenceLimitExceeded {
                    limit: QuiverPersistenceLimit::ManifestBytes,
                    ..
                },
                _
            ))
        ));
    }

    /// Scenario: Persisted manifests are corrupted in different ways.
    /// Guarantees: Decoding fails with a typed error instead of rebuilding a
    /// record set with lost or misattributed identity.
    #[test]
    fn pluggable_manifest_rejects_corruption() {
        let adapter =
            PdataArrowBundleAdapter::new(Box::new(pluggable_records(SignalType::Logs, 2)))
                .expect("adapter");
        let (payloads, metadata) = persisted(&adapter);

        let mut unknown_magic = metadata.clone();
        unknown_magic[0] = b'X';
        let truncated = metadata[..metadata.len() - 1].to_vec();
        let mut trailing = metadata.clone();
        trailing.push(0);
        for corrupted in [unknown_magic, truncated, trailing] {
            assert!(matches!(
                PluggableManifest::decode(&corrupted),
                Err(BundleConversionError::InvalidPluggableManifest(_))
            ));
        }

        let mut missing_payload = payloads.clone();
        let _ = missing_payload.remove(&SlotId::new(49));
        assert!(matches!(
            PluggableManifest::decode(&metadata)
                .and_then(|manifest| manifest.into_record_set(&missing_payload)),
            Err(BundleConversionError::InvalidPluggableManifest(_))
        ));
    }

    /// Scenario: WAL replay recovers item and byte counts for a pluggable bundle.
    /// Guarantees: The counts match the original record set's root items and
    /// logical Arrow bytes.
    #[test]
    fn recover_counts_from_pluggable_bundle() {
        let records = pluggable_records(SignalType::Traces, 2);
        let expected_bytes = records.logical_arrow_bytes().expect("bytes") as u64;
        let adapter = PdataArrowBundleAdapter::new(Box::new(records)).expect("adapter");

        assert_eq!(recover_item_count(&adapter), Some(2));
        assert_eq!(recover_byte_count(&adapter), Some(expected_bytes));
        assert_eq!(adapter.byte_count(), Some(expected_bytes));
    }

    /// Scenario: A pluggable record set is ingested into a real Quiver engine,
    /// finalized to a segment, and consumed.
    /// Guarantees: The consumed bundle converts back to the identical pluggable
    /// pdata payload, proving the end-to-end persistence contract.
    #[tokio::test]
    async fn pluggable_record_set_round_trips_through_quiver_engine() {
        use otel_arrow_dfe_quiver::subscriber::SubscriberId;
        use otel_arrow_dfe_quiver::{DiskBudget, QuiverConfig, QuiverEngine};

        let dir = tempfile::tempdir().expect("tempdir");
        let engine = QuiverEngine::open(
            QuiverConfig::default().with_data_dir(dir.path()),
            Arc::new(DiskBudget::unlimited()),
        )
        .await
        .expect("engine");
        let subscriber = SubscriberId::new("pluggable-test").expect("subscriber");
        engine
            .register_subscriber(subscriber.clone())
            .expect("register");
        engine.activate_subscriber(&subscriber).expect("activate");

        let records = pluggable_records(SignalType::Metrics, 3);
        let adapter = PdataArrowBundleAdapter::new(Box::new(records.clone())).expect("adapter");
        engine.ingest(&adapter).await.expect("ingest");
        engine.flush().await.expect("finalize");

        let handle = engine
            .poll_next_bundle(&subscriber)
            .expect("poll")
            .expect("bundle");
        let pdata = convert_bundle_to_pdata(handle.data()).expect("convert");
        handle.ack();
        let (_, payload) = pdata.into_parts();
        let rebuilt = payload
            .into_pdata_arrow()
            .expect("expected pluggable Arrow pdata");
        assert_eq!(*rebuilt, records);
    }

    /// Scenario: Every assigned Arrow WAL slot is decoded and encoded again.
    /// Guarantees: The explicit mapping is bijective for all 27 usable payload types.
    #[test]
    fn test_arrow_slot_mapping_is_bijective() {
        let mut mapped_count = 0;

        for raw in 0..WAL_SLOT_COUNT {
            let slot = SlotId::new(raw);
            if let Some(payload_type) = slot_to_payload_type(slot) {
                mapped_count += 1;
                assert_eq!(
                    to_slot_id(SignalType::Logs, payload_type),
                    slot,
                    "payload {payload_type:?} must map back to slot {raw}"
                );
            }
        }

        assert_eq!(mapped_count, 27);
    }

    /// Scenario: An unknown Arrow payload type is presented for WAL persistence.
    /// Guarantees: The payload is rejected before an unrecoverable slot can be written.
    #[test]
    #[should_panic(expected = "ArrowPayloadType::Unknown must not be persisted to WAL")]
    fn test_unknown_arrow_payload_is_rejected() {
        let _ = to_slot_id(SignalType::Logs, ArrowPayloadType::Unknown);
    }

    /// Scenario: Existing Arrow payloads are mapped after storage decoupling.
    /// Guarantees: Every persisted WAL assignment remains byte-for-byte compatible.
    #[test]
    fn test_existing_arrow_slot_assignments_are_stable() {
        let expected = [
            (ArrowPayloadType::ResourceAttrs, 1),
            (ArrowPayloadType::ScopeAttrs, 2),
            (ArrowPayloadType::UnivariateMetrics, 10),
            (ArrowPayloadType::NumberDataPoints, 11),
            (ArrowPayloadType::SummaryDataPoints, 12),
            (ArrowPayloadType::HistogramDataPoints, 13),
            (ArrowPayloadType::ExpHistogramDataPoints, 14),
            (ArrowPayloadType::NumberDpAttrs, 15),
            (ArrowPayloadType::SummaryDpAttrs, 16),
            (ArrowPayloadType::HistogramDpAttrs, 17),
            (ArrowPayloadType::ExpHistogramDpAttrs, 18),
            (ArrowPayloadType::NumberDpExemplars, 19),
            (ArrowPayloadType::HistogramDpExemplars, 20),
            (ArrowPayloadType::ExpHistogramDpExemplars, 21),
            (ArrowPayloadType::NumberDpExemplarAttrs, 22),
            (ArrowPayloadType::HistogramDpExemplarAttrs, 23),
            (ArrowPayloadType::ExpHistogramDpExemplarAttrs, 24),
            (ArrowPayloadType::MultivariateMetrics, 25),
            (ArrowPayloadType::MetricAttrs, 26),
            (ArrowPayloadType::Logs, 30),
            (ArrowPayloadType::LogAttrs, 31),
            (ArrowPayloadType::Spans, 40),
            (ArrowPayloadType::SpanAttrs, 41),
            (ArrowPayloadType::SpanEvents, 42),
            (ArrowPayloadType::SpanLinks, 43),
            (ArrowPayloadType::SpanEventAttrs, 44),
            (ArrowPayloadType::SpanLinkAttrs, 45),
        ];

        for (payload_type, raw) in expected {
            assert!(raw < WAL_SLOT_COUNT);
            assert_eq!(to_slot_id(SignalType::Logs, payload_type).raw(), raw);
            assert_eq!(slot_to_payload_type(SlotId::new(raw)), Some(payload_type));
        }
    }

    /// Scenario: Arrow and opaque OTLP payloads share the WAL slot bitmap.
    /// Guarantees: Current allocations are in range, unique, and never overlap.
    #[test]
    fn test_arrow_and_otlp_slot_allocations_do_not_collide() {
        let mut occupied = [false; WAL_SLOT_COUNT as usize];

        for raw in 0..WAL_SLOT_COUNT {
            if slot_to_payload_type(SlotId::new(raw)).is_some() {
                let index = raw as usize;
                assert!(!occupied[index], "duplicate Arrow WAL slot {raw}");
                occupied[index] = true;
            }
        }

        for raw in [
            otlp_slots::OTLP_LOGS,
            otlp_slots::OTLP_TRACES,
            otlp_slots::OTLP_METRICS,
        ] {
            assert!(raw < WAL_SLOT_COUNT);
            let index = raw as usize;
            assert!(
                !occupied[index],
                "opaque OTLP slot {raw} collides with Arrow"
            );
            occupied[index] = true;
        }
    }

    #[test]
    fn test_slot_id_roundtrip() {
        // Test that signal-specific payload types roundtrip correctly
        let test_cases = [
            (SignalType::Logs, ArrowPayloadType::Logs, SignalType::Logs),
            (
                SignalType::Logs,
                ArrowPayloadType::LogAttrs,
                SignalType::Logs,
            ),
            (
                SignalType::Traces,
                ArrowPayloadType::Spans,
                SignalType::Traces,
            ),
            (
                SignalType::Traces,
                ArrowPayloadType::SpanAttrs,
                SignalType::Traces,
            ),
            (
                SignalType::Metrics,
                ArrowPayloadType::UnivariateMetrics,
                SignalType::Metrics,
            ),
            (
                SignalType::Metrics,
                ArrowPayloadType::NumberDataPoints,
                SignalType::Metrics,
            ),
        ];

        for (signal_type, payload_type, expected_signal) in test_cases {
            let slot = to_slot_id(signal_type, payload_type);
            let (recovered_signal, recovered_payload) =
                from_slot_id(slot).expect("should reverse map");
            assert_eq!(
                expected_signal, recovered_signal,
                "signal type mismatch for {:?}",
                payload_type
            );
            assert_eq!(payload_type, recovered_payload, "payload type mismatch");
        }
    }

    #[test]
    fn test_shared_slots_return_none() {
        // Shared slots (RESOURCE_ATTRS, SCOPE_ATTRS) should return None from from_slot_id
        // because they're used by ALL signal types and we can't determine which one.
        let shared_slots = [
            ArrowPayloadType::ResourceAttrs,
            ArrowPayloadType::ScopeAttrs,
        ];

        for payload_type in shared_slots {
            let slot = to_slot_id(SignalType::Logs, payload_type);
            assert!(
                from_slot_id(slot).is_none(),
                "shared slot {:?} should return None",
                payload_type
            );
            assert!(
                is_shared_slot(slot),
                "slot {:?} should be identified as shared",
                payload_type
            );

            // But we can still get the payload type
            let recovered_payload = slot_to_payload_type(slot).expect("should get payload type");
            assert_eq!(payload_type, recovered_payload);
        }
    }

    #[test]
    fn test_signal_type_detection() {
        let records = logs!((Logs, ("id", UInt16, vec![1u16, 2, 3])));
        let mut payloads = HashMap::new();
        let _ = payloads.insert(
            to_slot_id(SignalType::Logs, ArrowPayloadType::Logs),
            extract_batch(&records.into(), ArrowPayloadType::Logs),
        );

        let signal = determine_signal_type(&payloads).unwrap();
        assert_eq!(signal, SignalType::Logs);
    }

    #[test]
    fn test_adapter_descriptor() {
        let records = OtapArrowRecords::Logs(logs!((Logs, ("id", UInt16, vec![1u16, 2, 3]))));
        let adapter = OtapRecordBundleAdapter::new(records);

        let descriptor = adapter.descriptor();
        assert_eq!(descriptor.slots.len(), 1);

        let expected_slot = to_slot_id(SignalType::Logs, ArrowPayloadType::Logs);
        assert!(descriptor.get(expected_slot).is_some());
    }

    #[test]
    fn test_adapter_payload_access() {
        let records = OtapArrowRecords::Logs(logs!((Logs, ("id", UInt16, vec![1u16, 2, 3]))));
        let adapter = OtapRecordBundleAdapter::new(records);

        let logs_slot = to_slot_id(SignalType::Logs, ArrowPayloadType::Logs);
        let payload = adapter.payload(logs_slot);
        assert!(payload.is_some());

        let spans_slot = to_slot_id(SignalType::Traces, ArrowPayloadType::Spans);
        let payload = adapter.payload(spans_slot);
        assert!(payload.is_none());
    }

    /// Scenario: A decoded Arrow logs bundle is inspected during WAL expiry.
    /// Guarantees: The counter reconstructs the original logical record count.
    #[test]
    fn recover_item_count_from_arrow_bundle() {
        let records = OtapArrowRecords::Logs(logs!((Logs, ("id", UInt16, vec![1u16, 2, 3]))));
        let adapter = OtapRecordBundleAdapter::new(records);

        assert_eq!(recover_item_count(&adapter), Some(3));
    }

    /// Scenario: An OTAP bundle is measured live and reconstructed from WAL slots.
    /// Guarantees: Both paths use the payload's canonical logical byte measurement.
    #[test]
    fn byte_count_from_arrow_bundle_uses_payload_num_bytes() {
        let records = OtapArrowRecords::Logs(logs!((Logs, ("id", UInt16, vec![1u16, 2, 3]))));
        let expected = records
            .num_bytes()
            .and_then(|bytes| u64::try_from(bytes).ok());
        let adapter = OtapRecordBundleAdapter::new(records);

        assert_eq!(adapter.byte_count(), expected);
        assert_eq!(recover_byte_count(&adapter), expected);
    }

    /// Scenario: A decoded OTLP pass-through bundle is inspected during WAL expiry.
    /// Guarantees: The counter uses the same protobuf item scan as live ingestion.
    #[test]
    fn recover_item_count_from_otlp_bundle() {
        let otlp =
            OtlpProtoBytes::new_from_bytes(SignalType::Logs, b"test OTLP protobuf data".to_vec());
        let adapter = OtlpBytesAdapter::new(otlp).map_err(|(e, _)| e).unwrap();

        assert_eq!(
            recover_item_count(&adapter),
            Some(adapter.cached_item_count())
        );
    }

    /// Scenario: An OTLP pass-through bundle is measured live and reconstructed from WAL slots.
    /// Guarantees: Both paths report the protobuf wire length.
    #[test]
    fn byte_count_from_otlp_bundle_uses_wire_length() {
        let payload = b"test OTLP protobuf data".to_vec();
        let expected = payload.len() as u64;
        let otlp = OtlpProtoBytes::new_from_bytes(SignalType::Logs, payload);
        let adapter = OtlpBytesAdapter::new(otlp).map_err(|(e, _)| e).unwrap();

        assert_eq!(adapter.byte_count(), Some(expected));
        assert_eq!(recover_byte_count(&adapter), Some(expected));
    }

    #[test]
    fn test_otlp_slot_id_roundtrip() {
        let test_cases = [SignalType::Logs, SignalType::Traces, SignalType::Metrics];

        for signal_type in test_cases {
            let slot = to_otlp_slot_id(signal_type);
            let recovered_signal = is_otlp_slot(slot).expect("should be OTLP slot");
            assert_eq!(signal_type, recovered_signal);

            // Verify it's not an Arrow slot
            assert!(from_slot_id(slot).is_none());
        }
    }

    #[test]
    fn test_otlp_bytes_adapter() {
        let test_bytes = b"test OTLP protobuf data".to_vec();
        let otlp = OtlpProtoBytes::new_from_bytes(SignalType::Logs, test_bytes.clone());

        let adapter = OtlpBytesAdapter::new(otlp).map_err(|(e, _)| e).unwrap();

        // Check descriptor
        let descriptor = adapter.descriptor();
        assert_eq!(descriptor.slots.len(), 1);

        let expected_slot = to_otlp_slot_id(SignalType::Logs);
        assert!(descriptor.get(expected_slot).is_some());

        // Check payload
        let payload = adapter.payload(expected_slot);
        assert!(payload.is_some());

        let payload_ref = payload.unwrap();
        assert_eq!(payload_ref.batch.num_rows(), 1);
        assert_eq!(payload_ref.batch.num_columns(), 1);

        // Verify the binary data is stored correctly
        let column = payload_ref.batch.column(0);
        let binary_array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary_array.value(0), test_bytes.as_slice());

        // Wrong signal type slot should return None
        let wrong_slot = to_otlp_slot_id(SignalType::Traces);
        assert!(adapter.payload(wrong_slot).is_none());
    }

    #[test]
    fn test_otlp_bytes_adapter_zero_copy() {
        let test_bytes = b"zero-copy verification payload".to_vec();
        let otlp = OtlpProtoBytes::new_from_bytes(SignalType::Logs, test_bytes);

        let adapter = OtlpBytesAdapter::new(otlp).map_err(|(e, _)| e).unwrap();

        let slot = to_otlp_slot_id(SignalType::Logs);
        let payload = adapter.payload(slot).unwrap();
        let column = payload.batch.column(0);
        let binary_array = column.as_any().downcast_ref::<BinaryArray>().unwrap();

        // The Arrow buffer should alias the original bytes (zero-copy).
        // Compare the pointer of the stored value with the original OtlpProtoBytes.
        let arrow_value_ptr = binary_array.value(0).as_ptr();
        let original_ptr = adapter.bytes.as_bytes().as_ptr();
        assert_eq!(
            arrow_value_ptr, original_ptr,
            "BinaryArray value should point to the same memory as OtlpProtoBytes (zero-copy)"
        );
    }

    #[test]
    fn test_otlp_bytes_adapter_empty_payload() {
        let otlp = OtlpProtoBytes::new_from_bytes(SignalType::Metrics, vec![]);

        let adapter = OtlpBytesAdapter::new(otlp).map_err(|(e, _)| e).unwrap();

        let slot = to_otlp_slot_id(SignalType::Metrics);
        let payload = adapter.payload(slot).unwrap();
        let column = payload.batch.column(0);
        let binary_array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary_array.value(0), b"");
    }

    #[test]
    fn test_extract_otlp_bytes() {
        let original_bytes = b"original OTLP data".to_vec();
        let otlp = OtlpProtoBytes::new_from_bytes(SignalType::Traces, original_bytes.clone());

        // Store in adapter
        let adapter = OtlpBytesAdapter::new(otlp).map_err(|(e, _)| e).unwrap();

        // Get the batch from the adapter
        let slot = to_otlp_slot_id(SignalType::Traces);
        let payload = adapter.payload(slot).unwrap();

        // Extract bytes back
        let extracted = extract_otlp_bytes(SignalType::Traces, payload.batch).unwrap();
        assert_eq!(extracted.as_bytes(), original_bytes.as_slice());
    }

    #[test]
    fn test_find_otlp_slot() {
        // Test with OTLP bundle
        let otlp = OtlpProtoBytes::new_from_bytes(SignalType::Metrics, b"data".to_vec());
        let adapter = OtlpBytesAdapter::new(otlp).map_err(|(e, _)| e).unwrap();
        let slot = to_otlp_slot_id(SignalType::Metrics);
        let payload = adapter.payload(slot).unwrap();

        let mut payloads = HashMap::new();
        let _ = payloads.insert(slot, payload.batch.clone());

        let result = find_otlp_slot(&payloads);
        assert!(result.is_some());
        let (signal_type, _) = result.unwrap();
        assert_eq!(signal_type, SignalType::Metrics);

        // Test with Arrow bundle (should return None)
        let records = OtapArrowRecords::Logs(logs!((Logs, ("id", UInt16, vec![1u16, 2, 3]))));
        let mut arrow_payloads = HashMap::new();
        let _ = arrow_payloads.insert(
            to_slot_id(SignalType::Logs, ArrowPayloadType::Logs),
            extract_batch(&records, ArrowPayloadType::Logs),
        );

        assert!(find_otlp_slot(&arrow_payloads).is_none());
    }

    // -------------------------------------------------------------------------
    // Roundtrip tests: Simulating WAL persist -> recover cycle
    // -------------------------------------------------------------------------

    #[test]
    fn test_traces_bundle_with_shared_slots_roundtrip() {
        // Simulate a Traces bundle that includes shared slots (RESOURCE_ATTRS, SCOPE_ATTRS)
        // This is the realistic case - bundles always have shared slots alongside signal-specific ones
        let records = OtapArrowRecords::Traces(traces!(
            (Spans, ("id", UInt16, vec![0u16, 1, 2])),
            (SpanAttrs, ("parent_id", UInt16, vec![0u16, 1, 2])),
            (ResourceAttrs, ("parent_id", UInt16, vec![0u16, 1, 2])),
            (ScopeAttrs, ("parent_id", UInt16, vec![0u16, 1, 2]))
        ));
        let adapter = OtapRecordBundleAdapter::new(records);

        // Simulate what Quiver does: extract slot->batch pairs
        let mut recovered_payloads: HashMap<SlotId, RecordBatch> = HashMap::new();
        for slot_desc in &adapter.descriptor().slots {
            if let Some(payload) = adapter.payload(slot_desc.id) {
                let _ = recovered_payloads.insert(slot_desc.id, payload.batch.clone());
            }
        }

        // Verify we have all 4 slots
        assert_eq!(recovered_payloads.len(), 4);

        // Now simulate recovery: determine signal type
        let signal_type = determine_signal_type(&recovered_payloads).unwrap();
        assert_eq!(signal_type, SignalType::Traces);

        // Reconstruct the records
        let reconstructed = create_records::<Traces>(&recovered_payloads).unwrap();

        // Verify all tables were recovered, including shared slots
        if let OtapArrowRecords::Traces(traces) = reconstructed {
            assert!(
                traces.get(ArrowPayloadType::Spans).is_some(),
                "SPANS should be recovered"
            );
            assert!(
                traces.get(ArrowPayloadType::SpanAttrs).is_some(),
                "SPAN_ATTRS should be recovered"
            );
            assert!(
                traces.get(ArrowPayloadType::ResourceAttrs).is_some(),
                "RESOURCE_ATTRS (shared) should be recovered"
            );
            assert!(
                traces.get(ArrowPayloadType::ScopeAttrs).is_some(),
                "SCOPE_ATTRS (shared) should be recovered"
            );
        } else {
            panic!("Expected Traces variant");
        }
    }

    #[test]
    fn test_metrics_bundle_with_shared_slots_roundtrip() {
        // Metrics bundle with shared slots
        let records = OtapArrowRecords::Metrics(metrics!(
            (UnivariateMetrics, ("id", UInt16, vec![0u16, 1, 2])),
            (
                NumberDataPoints,
                ("id", UInt32, vec![0u32, 1, 2]),
                ("parent_id", UInt16, vec![0u16, 1, 2])
            ),
            (ResourceAttrs, ("parent_id", UInt16, vec![0u16, 1, 2])),
            (ScopeAttrs, ("parent_id", UInt16, vec![0u16, 1, 2]))
        ));
        let adapter = OtapRecordBundleAdapter::new(records);

        // Extract slot->batch pairs (simulating WAL storage)
        let mut recovered_payloads: HashMap<SlotId, RecordBatch> = HashMap::new();
        for slot_desc in &adapter.descriptor().slots {
            if let Some(payload) = adapter.payload(slot_desc.id) {
                let _ = recovered_payloads.insert(slot_desc.id, payload.batch.clone());
            }
        }

        // Signal type should be detected from UNIVARIATE_METRICS or NUMBER_DATA_POINTS
        let signal_type = determine_signal_type(&recovered_payloads).unwrap();
        assert_eq!(signal_type, SignalType::Metrics);

        // Reconstruct and verify shared slots are included
        let reconstructed = create_records::<Metrics>(&recovered_payloads).unwrap();

        if let OtapArrowRecords::Metrics(metrics) = reconstructed {
            assert!(metrics.get(ArrowPayloadType::UnivariateMetrics).is_some());
            assert!(metrics.get(ArrowPayloadType::NumberDataPoints).is_some());
            assert!(
                metrics.get(ArrowPayloadType::ResourceAttrs).is_some(),
                "RESOURCE_ATTRS (shared) should be recovered for Metrics"
            );
            assert!(
                metrics.get(ArrowPayloadType::ScopeAttrs).is_some(),
                "SCOPE_ATTRS (shared) should be recovered for Metrics"
            );
        } else {
            panic!("Expected Metrics variant");
        }
    }

    #[test]
    fn test_logs_bundle_with_shared_slots_roundtrip() {
        // Logs bundle with shared slots
        let records = OtapArrowRecords::Logs(logs!(
            (Logs, ("id", UInt16, vec![0u16, 1, 2])),
            (LogAttrs, ("parent_id", UInt16, vec![0u16, 1, 2])),
            (ResourceAttrs, ("parent_id", UInt16, vec![0u16, 1, 2])),
            (ScopeAttrs, ("parent_id", UInt16, vec![0u16, 1, 2]))
        ));
        let adapter = OtapRecordBundleAdapter::new(records);

        // Extract slot->batch pairs
        let mut recovered_payloads: HashMap<SlotId, RecordBatch> = HashMap::new();
        for slot_desc in &adapter.descriptor().slots {
            if let Some(payload) = adapter.payload(slot_desc.id) {
                let _ = recovered_payloads.insert(slot_desc.id, payload.batch.clone());
            }
        }

        let signal_type = determine_signal_type(&recovered_payloads).unwrap();
        assert_eq!(signal_type, SignalType::Logs);

        let reconstructed = create_records::<Logs>(&recovered_payloads).unwrap();

        if let OtapArrowRecords::Logs(logs) = reconstructed {
            assert!(logs.get(ArrowPayloadType::Logs).is_some());
            assert!(logs.get(ArrowPayloadType::LogAttrs).is_some());
            assert!(
                logs.get(ArrowPayloadType::ResourceAttrs).is_some(),
                "RESOURCE_ATTRS (shared) should be recovered for Logs"
            );
            assert!(
                logs.get(ArrowPayloadType::ScopeAttrs).is_some(),
                "SCOPE_ATTRS (shared) should be recovered for Logs"
            );
        } else {
            panic!("Expected Logs variant");
        }
    }

    #[test]
    fn test_bundle_with_only_shared_slots_fails() {
        // Edge case: A bundle that somehow only has shared slots
        // (shouldn't happen in practice, but we should handle it gracefully)
        let records = OtapArrowRecords::Logs(logs!(
            (ResourceAttrs, ("parent_id", UInt16, vec![0u16, 1])),
            (ScopeAttrs, ("parent_id", UInt16, vec![0u16, 1]))
        ));
        let mut payloads = HashMap::new();
        let _ = payloads.insert(
            to_slot_id(SignalType::Logs, ArrowPayloadType::ResourceAttrs),
            extract_batch(&records, ArrowPayloadType::ResourceAttrs),
        );
        let _ = payloads.insert(
            to_slot_id(SignalType::Logs, ArrowPayloadType::ScopeAttrs),
            extract_batch(&records, ArrowPayloadType::ScopeAttrs),
        );

        // Should fail to determine signal type since shared slots return None
        let result = determine_signal_type(&payloads);
        assert!(
            result.is_err(),
            "Should fail when only shared slots are present"
        );
    }

    #[test]
    fn test_signal_type_detection_priority() {
        // When multiple signal-specific slots are present, the first one found determines the type
        // In practice, bundles only contain one signal type, but let's verify behavior

        // HashMap iteration order is not guaranteed, but all slots should be from same signal
        #[rustfmt::skip]
        let records = OtapArrowRecords::Traces(traces!(
            (Spans,
                ("id", UInt16, vec![0u16, 1])),
            (SpanEvents,
                ("id", UInt32, vec![0u32, 1]),
                ("parent_id", UInt16, vec![0u16, 1]))
        ));

        let mut payloads = HashMap::new();
        let _ = payloads.insert(
            to_slot_id(SignalType::Traces, ArrowPayloadType::Spans),
            extract_batch(&records, ArrowPayloadType::Spans),
        );
        let _ = payloads.insert(
            to_slot_id(SignalType::Traces, ArrowPayloadType::SpanEvents),
            extract_batch(&records, ArrowPayloadType::SpanEvents),
        );

        let signal = determine_signal_type(&payloads).unwrap();
        assert_eq!(signal, SignalType::Traces);
    }

    // -------------------------------------------------------------------------
    // Edge case and boundary tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_invalid_slot_ids_return_none() {
        // Test that gap/invalid slot IDs are handled correctly
        // These are slots that don't correspond to any ArrowPayloadType
        let invalid_slots = [
            0,  // UNKNOWN
            3,  // Gap between SCOPE_ATTRS (2) and UNIVARIATE_METRICS (10)
            9,  // Just before metrics range
            27, // Just after METRIC_ATTRS (26)
            29, // Just before LOGS (30)
            32, // Just after LOG_ATTRS (31)
            39, // Just before SPANS (40)
            46, // Just after SPAN_LINK_ATTRS (45)
            50, // Well outside any range
            59, // Just before OTLP range
        ];

        for raw in invalid_slots {
            let slot = SlotId::new(raw);

            // Should not be identified as shared
            if raw != 1 && raw != 2 {
                assert!(!is_shared_slot(slot), "slot {} should not be shared", raw);
            }

            // Should not map to a signal type (except 0 which is UNKNOWN)
            assert!(
                from_slot_id(slot).is_none(),
                "slot {} should return None from from_slot_id",
                raw
            );

            // slot_to_payload_type depends on whether prost accepts the value
            // For truly invalid values (gaps), it should return None
            if raw == 3
                || raw == 9
                || raw == 27
                || raw == 29
                || raw == 32
                || raw == 39
                || raw == 46
                || raw == 50
                || raw == 59
            {
                assert!(
                    slot_to_payload_type(slot).is_none(),
                    "slot {} should return None from slot_to_payload_type",
                    raw
                );
            }
        }
    }

    #[test]
    fn test_adapter_rejects_wrong_signal_slots() {
        // A Traces adapter should reject Logs and Metrics slots
        let records = OtapArrowRecords::Traces(traces!((Spans, ("id", UInt16, vec![0u16, 1, 2]))));
        let adapter = OtapRecordBundleAdapter::new(records);

        // Should reject Logs slot
        let logs_slot = to_slot_id(SignalType::Logs, ArrowPayloadType::Logs);
        assert!(
            adapter.payload(logs_slot).is_none(),
            "Traces adapter should reject Logs slot"
        );

        // Should reject Metrics slot
        let metrics_slot = to_slot_id(SignalType::Metrics, ArrowPayloadType::UnivariateMetrics);
        assert!(
            adapter.payload(metrics_slot).is_none(),
            "Traces adapter should reject Metrics slot"
        );

        // Should accept its own Traces slot
        let traces_slot = to_slot_id(SignalType::Traces, ArrowPayloadType::Spans);
        assert!(
            adapter.payload(traces_slot).is_some(),
            "Traces adapter should accept Traces slot"
        );
    }

    #[test]
    fn test_shared_slots_work_for_all_signal_adapters() {
        // RESOURCE_ATTRS and SCOPE_ATTRS should be accessible from any signal adapter
        // that has them set

        // Test with Logs
        let logs_adapter = OtapRecordBundleAdapter::new(OtapArrowRecords::Logs(logs!(
            (Logs, ("id", UInt16, vec![0u16, 1])),
            (ResourceAttrs, ("parent_id", UInt16, vec![0u16, 1]))
        )));

        let resource_slot = to_slot_id(SignalType::Logs, ArrowPayloadType::ResourceAttrs);
        assert!(
            logs_adapter.payload(resource_slot).is_some(),
            "Logs adapter should return RESOURCE_ATTRS"
        );

        // Test with Traces
        let traces_adapter = OtapRecordBundleAdapter::new(OtapArrowRecords::Traces(traces!(
            (Spans, ("id", UInt16, vec![0u16, 1])),
            (ResourceAttrs, ("parent_id", UInt16, vec![0u16, 1]))
        )));

        assert!(
            traces_adapter.payload(resource_slot).is_some(),
            "Traces adapter should return RESOURCE_ATTRS"
        );

        // Test with Metrics
        let metrics_adapter = OtapRecordBundleAdapter::new(OtapArrowRecords::Metrics(metrics!(
            (UnivariateMetrics, ("id", UInt16, vec![0u16, 1])),
            (ResourceAttrs, ("parent_id", UInt16, vec![0u16, 1]))
        )));

        assert!(
            metrics_adapter.payload(resource_slot).is_some(),
            "Metrics adapter should return RESOURCE_ATTRS"
        );
    }

    #[test]
    fn test_empty_bundle_conversion_fails() {
        // Empty payloads should fail conversion
        let payloads: HashMap<SlotId, RecordBatch> = HashMap::new();

        // Wrap in a mock ReconstructedBundle-like structure
        let result = determine_signal_type(&payloads);
        assert!(result.is_err());
    }

    #[test]
    fn test_adapter_with_no_tables() {
        // An adapter with no tables set should have empty descriptor
        let logs = Logs::default();
        let records = OtapArrowRecords::Logs(logs);
        let adapter = OtapRecordBundleAdapter::new(records);

        assert_eq!(adapter.descriptor().slots.len(), 0);
    }

    #[test]
    fn test_slot_id_boundary_values() {
        // Test the boundary values for each signal type range
        let boundary_cases = [
            // (slot_raw, expected_signal, expected_payload)
            (
                10,
                Some(SignalType::Metrics),
                ArrowPayloadType::UnivariateMetrics,
            ),
            (26, Some(SignalType::Metrics), ArrowPayloadType::MetricAttrs),
            (30, Some(SignalType::Logs), ArrowPayloadType::Logs),
            (31, Some(SignalType::Logs), ArrowPayloadType::LogAttrs),
            (40, Some(SignalType::Traces), ArrowPayloadType::Spans),
            (
                45,
                Some(SignalType::Traces),
                ArrowPayloadType::SpanLinkAttrs,
            ),
        ];

        for (raw, expected_signal, expected_payload) in boundary_cases {
            let slot = SlotId::new(raw);

            if let Some(expected) = expected_signal {
                let (signal, payload) = from_slot_id(slot)
                    .unwrap_or_else(|| panic!("slot {} should map to {:?}", raw, expected));
                assert_eq!(signal, expected, "signal mismatch for slot {}", raw);
                assert_eq!(
                    payload, expected_payload,
                    "payload mismatch for slot {}",
                    raw
                );
            }
        }
    }

    #[test]
    fn test_otlp_slots_are_not_arrow_slots() {
        // OTLP slots (60-62) should not be treated as Arrow payload slots
        for raw in [60, 61, 62] {
            let slot = SlotId::new(raw);

            // Should be identified as OTLP slot
            assert!(is_otlp_slot(slot).is_some(), "slot {} should be OTLP", raw);

            // Should NOT be an Arrow payload type
            assert!(
                slot_to_payload_type(slot).is_none(),
                "slot {} should not be Arrow payload",
                raw
            );

            // Should NOT return signal type from from_slot_id
            assert!(
                from_slot_id(slot).is_none(),
                "slot {} should not map via from_slot_id",
                raw
            );

            // Should NOT be a shared slot
            assert!(!is_shared_slot(slot), "slot {} should not be shared", raw);
        }
    }
}
