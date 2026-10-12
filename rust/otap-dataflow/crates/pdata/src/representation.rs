// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Stable identities and owned payloads for pluggable Arrow representations.

use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;
use std::fmt;

use arrow::array::RecordBatch;
use otel_arrow_dfe_config::SignalType;

use crate::otap::memory;

/// Stable name of a pdata representation contract.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PdataRepresentationId(Cow<'static, str>);

impl PdataRepresentationId {
    /// Built-in native OTAP Arrow representation.
    pub const OTAP_ARROW: Self = Self::new("otap-arrow");

    /// Built-in OTLP protobuf byte representation.
    pub const OTLP_BYTES: Self = Self::new("otlp-bytes");

    /// Declares a compile-time representation identifier.
    #[must_use]
    pub const fn new(id: &'static str) -> Self {
        Self(Cow::Borrowed(id))
    }

    /// Returns the stable configuration and diagnostic identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), RepresentationError> {
        let id = self.as_str();
        if id.is_empty() {
            return Err(RepresentationError::EmptyRepresentationId);
        }
        if !id.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'-' | b':')
        }) {
            return Err(RepresentationError::InvalidRepresentationId { id: self.clone() });
        }
        Ok(())
    }
}

impl Borrow<str> for PdataRepresentationId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl From<String> for PdataRepresentationId {
    fn from(id: String) -> Self {
        Self(Cow::Owned(id))
    }
}

impl fmt::Display for PdataRepresentationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Version of a pdata representation contract.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RepresentationVersion {
    major: u16,
    minor: u16,
}

impl RepresentationVersion {
    /// Creates a representation version.
    #[must_use]
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Incompatible contract version.
    #[must_use]
    pub const fn major(self) -> u16 {
        self.major
    }

    /// Backward-compatible contract version.
    #[must_use]
    pub const fn minor(self) -> u16 {
        self.minor
    }
}

impl fmt::Display for RepresentationVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.major, self.minor)
    }
}

/// Stable representation identifier and contract version.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PdataRepresentation {
    id: PdataRepresentationId,
    version: RepresentationVersion,
}

impl PdataRepresentation {
    /// Current compatibility identity for native OTAP Arrow records.
    pub const NATIVE_OTAP: Self = Self::new(
        PdataRepresentationId::OTAP_ARROW,
        RepresentationVersion::new(1, 0),
    );

    /// Current compatibility identity for OTLP protobuf bytes.
    pub const OTLP_BYTES: Self = Self::new(
        PdataRepresentationId::OTLP_BYTES,
        RepresentationVersion::new(1, 0),
    );

    /// Declares a representation identity.
    #[must_use]
    pub const fn new(id: PdataRepresentationId, version: RepresentationVersion) -> Self {
        Self { id, version }
    }

    /// Stable representation contract identifier.
    #[must_use]
    pub const fn id(&self) -> &PdataRepresentationId {
        &self.id
    }

    /// Representation contract version.
    #[must_use]
    pub const fn version(&self) -> RepresentationVersion {
        self.version
    }

    fn validate(&self) -> Result<(), RepresentationError> {
        self.id.validate()
    }
}

impl fmt::Display for PdataRepresentation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}@{}", self.id, self.version)
    }
}

/// Stable schema identity supplied by a representation adapter.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PdataSchemaId(Cow<'static, str>);

impl PdataSchemaId {
    /// Declares a compile-time schema identity.
    #[must_use]
    pub const fn new(id: &'static str) -> Self {
        Self(Cow::Borrowed(id))
    }

    /// Returns the adapter-defined schema identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), RepresentationError> {
        if self.as_str().is_empty() {
            Err(RepresentationError::EmptySchemaId)
        } else {
            Ok(())
        }
    }
}

impl Borrow<str> for PdataSchemaId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl From<String> for PdataSchemaId {
    fn from(id: String) -> Self {
        Self(Cow::Owned(id))
    }
}

impl fmt::Display for PdataSchemaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Adapter-supplied 256-bit schema fingerprint.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SchemaFingerprint([u8; 32]);

impl SchemaFingerprint {
    /// Creates a schema fingerprint without recomputing or interpreting it.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact adapter-supplied fingerprint bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for SchemaFingerprint {
    fn from(bytes: [u8; 32]) -> Self {
        Self::new(bytes)
    }
}

/// One format-local table in a pluggable Arrow representation.
#[derive(Clone, Debug, PartialEq)]
pub struct PdataArrowTable {
    table_id: u32,
    schema_id: PdataSchemaId,
    schema_fingerprint: SchemaFingerprint,
    batch: RecordBatch,
}

impl PdataArrowTable {
    /// Creates a format-local table while preserving adapter metadata verbatim.
    pub fn new(
        table_id: u32,
        schema_id: PdataSchemaId,
        schema_fingerprint: SchemaFingerprint,
        batch: RecordBatch,
    ) -> Result<Self, RepresentationError> {
        schema_id.validate()?;
        Ok(Self {
            table_id,
            schema_id,
            schema_fingerprint,
            batch,
        })
    }

    /// Identifier interpreted only by the named representation contract.
    #[must_use]
    pub const fn table_id(&self) -> u32 {
        self.table_id
    }

    /// Adapter-defined schema identity.
    #[must_use]
    pub const fn schema_id(&self) -> &PdataSchemaId {
        &self.schema_id
    }

    /// Adapter-supplied schema fingerprint.
    #[must_use]
    pub const fn schema_fingerprint(&self) -> SchemaFingerprint {
        self.schema_fingerprint
    }

    /// Owned Arrow batch.
    #[must_use]
    pub const fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Consumes the table and returns all adapter metadata with the owned batch.
    #[must_use]
    pub fn into_parts(self) -> (u32, PdataSchemaId, SchemaFingerprint, RecordBatch) {
        (
            self.table_id,
            self.schema_id,
            self.schema_fingerprint,
            self.batch,
        )
    }
}

/// A named, versioned set of format-local Arrow tables.
#[derive(Clone, Debug, PartialEq)]
pub struct PdataArrowRecordSet {
    representation: PdataRepresentation,
    signal: SignalType,
    root_items: usize,
    tables: BTreeMap<u32, PdataArrowTable>,
}

impl PdataArrowRecordSet {
    /// Creates a validated pluggable Arrow record set.
    pub fn new(
        representation: PdataRepresentation,
        signal: SignalType,
        root_items: usize,
        tables: impl IntoIterator<Item = PdataArrowTable>,
    ) -> Result<Self, RepresentationError> {
        representation.validate()?;

        let mut table_map = BTreeMap::new();
        for table in tables {
            let table_id = table.table_id();
            if table_map.insert(table_id, table).is_some() {
                return Err(RepresentationError::DuplicateTableId { table_id });
            }
        }

        Ok(Self {
            representation,
            signal,
            root_items,
            tables: table_map,
        })
    }

    /// Stable representation identity and version.
    #[must_use]
    pub const fn representation(&self) -> &PdataRepresentation {
        &self.representation
    }

    /// Semantic signal carried outside the representation-specific tables.
    #[must_use]
    pub const fn signal_type(&self) -> SignalType {
        self.signal
    }

    /// Format-defined number of root items.
    #[must_use]
    pub const fn num_items(&self) -> usize {
        self.root_items
    }

    /// Returns whether the representation has no root items.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.root_items == 0
    }

    /// Returns a format-local table.
    #[must_use]
    pub fn table(&self, table_id: u32) -> Option<&PdataArrowTable> {
        self.tables.get(&table_id)
    }

    /// Iterates over format-local tables in table-ID order.
    #[must_use]
    pub fn tables(&self) -> impl ExactSizeIterator<Item = &PdataArrowTable> {
        self.tables.values()
    }

    /// Consumes the record set and returns its identity, accounting, and tables.
    #[must_use]
    pub fn into_parts(self) -> (PdataRepresentation, SignalType, usize, Vec<PdataArrowTable>) {
        (
            self.representation,
            self.signal,
            self.root_items,
            self.tables.into_values().collect(),
        )
    }

    /// Logical Arrow bytes across all tables.
    pub fn logical_arrow_bytes(&self) -> crate::error::Result<usize> {
        self.tables.values().try_fold(0usize, |total, table| {
            let batch_bytes = memory::record_batch_logical_bytes(table.batch())
                .map_err(|source| crate::error::Error::LogicalArrowSize { source })?;
            total
                .checked_add(batch_bytes)
                .ok_or_else(|| crate::error::Error::LogicalArrowSize {
                    source: arrow::error::ArrowError::ComputeError(
                        "integer overflow computing pluggable Arrow byte size".to_owned(),
                    ),
                })
        })
    }

    /// Deduplicated retained Arrow buffer capacity across all tables.
    #[must_use]
    pub fn retained_memory_bytes(&self) -> usize {
        let mut seen = memory::CountedAllocations::default();
        self.tables
            .values()
            .map(|table| memory::record_batch_pinned_bytes(table.batch(), &mut seen))
            .sum()
    }

    pub(crate) fn take_payload(&mut self) -> Self {
        Self {
            representation: self.representation.clone(),
            signal: self.signal,
            root_items: std::mem::take(&mut self.root_items),
            tables: std::mem::take(&mut self.tables),
        }
    }
}

/// Error constructing or using a pluggable representation.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RepresentationError {
    /// The representation ID is empty.
    #[error("pdata representation ID must not be empty")]
    EmptyRepresentationId,
    /// The representation ID does not satisfy the stable identity syntax.
    #[error(
        "invalid pdata representation ID `{id}`; use lowercase ASCII letters, digits, '.', '_', '-', or ':'"
    )]
    InvalidRepresentationId {
        /// Invalid representation ID.
        id: PdataRepresentationId,
    },
    /// The adapter supplied an empty schema identity.
    #[error("pdata Arrow schema ID must not be empty")]
    EmptySchemaId,
    /// A format-local Arrow table ID occurs more than once.
    #[error("duplicate pdata Arrow table ID {table_id}")]
    DuplicateTableId {
        /// Duplicate format-local table ID.
        table_id: u32,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(values: Vec<u32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::UInt32,
                false,
            )])),
            vec![Arc::new(UInt32Array::from(values)) as ArrayRef],
        )
        .expect("valid test batch")
    }

    fn table(table_id: u32, schema_id: &str, fingerprint: u8) -> PdataArrowTable {
        PdataArrowTable::new(
            table_id,
            schema_id.to_owned().into(),
            SchemaFingerprint::new([fingerprint; 32]),
            batch(vec![1, 2, 3]),
        )
        .expect("valid test table")
    }

    /// Scenario: An adapter supplies table identity, schema identity, and a schema fingerprint.
    /// Guarantees: The record set preserves all adapter metadata without protocol-specific mapping.
    #[test]
    fn preserves_table_and_schema_identity() {
        let records = PdataArrowRecordSet::new(
            PdataRepresentation::new(
                PdataRepresentationId::new("example.logs"),
                RepresentationVersion::new(2, 3),
            ),
            SignalType::Logs,
            3,
            [table(7, "example.logs.root", 0xA5)],
        )
        .expect("valid representation");

        let stored = records.table(7).expect("table must be present");
        assert_eq!(records.representation().id().as_str(), "example.logs");
        assert_eq!(
            records.representation().version(),
            RepresentationVersion::new(2, 3)
        );
        assert_eq!(stored.schema_id().as_str(), "example.logs.root");
        assert_eq!(stored.schema_fingerprint().as_bytes(), &[0xA5; 32]);
    }

    /// Scenario: A pluggable Arrow record set is cloned for channel fan-out.
    /// Guarantees: Cloning shares Arrow arrays and their value buffer instead of deep-copying data.
    #[test]
    fn clone_shares_arrow_array_ownership() {
        let records = PdataArrowRecordSet::new(
            PdataRepresentation::new(
                PdataRepresentationId::new("example.logs"),
                RepresentationVersion::new(1, 0),
            ),
            SignalType::Logs,
            3,
            [table(7, "example.logs.root", 1)],
        )
        .expect("valid representation");
        let cloned = records.clone();

        let original_column = records.table(7).expect("table").batch().column(0);
        let cloned_column = cloned.table(7).expect("table").batch().column(0);
        assert!(Arc::ptr_eq(original_column, cloned_column));

        let original_values = original_column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("uint32 array")
            .values();
        let cloned_values = cloned_column
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("uint32 array")
            .values();
        assert_eq!(original_values.as_ptr(), cloned_values.as_ptr());
    }

    /// Scenario: Two Arrow tables use the same format-local table ID.
    /// Guarantees: Construction fails instead of replacing one table silently.
    #[test]
    fn rejects_duplicate_table_ids() {
        let result = PdataArrowRecordSet::new(
            PdataRepresentation::new(
                PdataRepresentationId::new("example.logs"),
                RepresentationVersion::new(1, 0),
            ),
            SignalType::Logs,
            1,
            [
                table(7, "example.logs.root", 1),
                table(7, "example.logs.other", 2),
            ],
        );

        assert_eq!(
            result.expect_err("duplicate table ID must fail"),
            RepresentationError::DuplicateTableId { table_id: 7 }
        );
    }
}
