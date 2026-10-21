// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Read-only inspection of a Quiver data directory.
//!
//! [`inspect_wal`] lists the write-ahead log (WAL) files, the persisted
//! consumer cursor, and every decodable WAL entry. An entry at or before the
//! cursor is already durable in a finalized segment. An entry after the cursor
//! exists only in the WAL; the next engine that opens the directory replays it
//! into a new segment. [`wal_entry_batches`] decodes the Arrow payloads of one
//! entry.
//!
//! Inspection never writes, truncates, or locks a file, so it is safe to run
//! against the data directory of a live engine. A concurrent writer may append
//! while the WAL is read: the result is a point-in-time view, and an entry that
//! is still being written is reported in [`WalInspection::stopped_early`].
//!
//! ```no_run
//! # fn main() -> otel_arrow_dfe_quiver::Result<()> {
//! let wal = otel_arrow_dfe_quiver::inspect::inspect_wal(std::path::Path::new("/var/lib/quiver"))?;
//! let replayable = wal.unconsumed().count();
//! println!("{} WAL entries, {replayable} not yet in a segment", wal.entries.len());
//! # Ok(())
//! # }
//! ```

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::RecordBatch;

use crate::error::Result;
use crate::record_bundle::{SchemaFingerprint, SlotId};
use crate::wal::{
    CursorSidecar, MultiFileWalReader, WalError, WalRecordBundle, decode_slot_payload,
};

/// Directory, relative to the data directory, that holds the WAL files.
const WAL_DIR: &str = "wal";
/// File name of the active WAL file; rotated files append `.N`.
const WAL_FILE: &str = "quiver.wal";

/// One WAL file on disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalFile {
    /// Path of the file.
    pub path: PathBuf,
    /// `None` for the active file, `Some(id)` for a rotated file.
    pub rotation_id: Option<u64>,
    /// File size in bytes, including the header.
    pub bytes: u64,
    /// WAL stream position of the first entry byte in this file.
    pub start_position: u64,
    /// WAL stream position just past the last complete entry in this file.
    pub end_position: u64,
}

/// One payload slot of a WAL entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalEntrySlot {
    /// Slot the payload belongs to.
    pub slot_id: SlotId,
    /// Fingerprint of the payload's Arrow schema.
    pub schema_fingerprint: SchemaFingerprint,
    /// Rows in the payload.
    pub rows: u32,
    /// Size of the serialized Arrow IPC payload.
    pub payload_bytes: u32,
}

/// One WAL entry, which is one persisted record bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalEntry {
    /// Monotonic entry sequence number.
    pub sequence: u64,
    /// WAL stream position where the entry starts.
    pub position: u64,
    /// WAL stream position just past the entry.
    pub next_position: u64,
    /// When the bundle was ingested.
    pub ingestion_time: SystemTime,
    /// Payload slots in slot order.
    pub slots: Vec<WalEntrySlot>,
    /// Opaque bundle metadata persisted with the entry, if any.
    pub metadata: Option<Vec<u8>>,
    /// The persisted cursor is at or past this entry: its bundle is already in
    /// a finalized segment and the entry is never replayed.
    pub consumed: bool,
}

/// Point-in-time view of a data directory's WAL.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WalInspection {
    /// WAL files, oldest first; empty when the directory has no WAL yet.
    pub files: Vec<WalFile>,
    /// Persisted consumer cursor (`wal/quiver.wal.cursor`), or `None` when no
    /// segment has been finalized yet.
    pub cursor_position: Option<u64>,
    /// WAL stream position where the next entry would be written.
    pub end_position: u64,
    /// Every decodable entry, oldest first.
    pub entries: Vec<WalEntry>,
    /// Why reading stopped before the end of the WAL, for example an entry
    /// left half-written by a crash or still being appended by a live writer.
    pub stopped_early: Option<String>,
}

impl WalInspection {
    /// Entries that exist only in the WAL and are replayed on the next open.
    pub fn unconsumed(&self) -> impl Iterator<Item = &WalEntry> {
        self.entries.iter().filter(|entry| !entry.consumed)
    }
}

/// Reads the WAL files, the persisted cursor, and every decodable entry of the
/// data directory without modifying anything.
///
/// A data directory without a WAL yields an empty inspection.
///
/// # Errors
///
/// Returns an error when a WAL file or the cursor sidecar exists but cannot be
/// read or has an invalid header.
pub fn inspect_wal(data_dir: &Path) -> Result<WalInspection> {
    let wal_path = data_dir.join(WAL_DIR).join(WAL_FILE);
    let cursor_position = read_cursor(&wal_path)?;
    let Some(reader) = open_reader(&wal_path)? else {
        return Ok(WalInspection {
            cursor_position,
            ..WalInspection::default()
        });
    };
    let files = reader
        .file_ranges()
        .map(|(path, rotation_id, start_position, end_position)| {
            Ok(WalFile {
                path: path.to_path_buf(),
                rotation_id,
                bytes: std::fs::metadata(path).map_err(WalError::Io)?.len(),
                start_position,
                end_position,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let end_position = reader.wal_end_position();
    let mut entries = Vec::new();
    let mut stopped_early = None;
    for entry in reader.iter_from(0)? {
        match entry {
            Ok(entry) => entries.push(summarize(&entry, cursor_position)),
            Err(error) => {
                stopped_early = Some(error.to_string());
                break;
            }
        }
    }
    Ok(WalInspection {
        files,
        cursor_position,
        end_position,
        entries,
        stopped_early,
    })
}

/// Decodes the Arrow payloads of the WAL entry with `sequence`.
///
/// Returns `None` when no complete entry has that sequence, for example because
/// its WAL file was already purged after the entry reached a segment.
///
/// # Errors
///
/// Returns an error when the WAL cannot be read before the entry is found, or
/// when the entry's payload is not a valid Arrow IPC stream.
pub fn wal_entry_batches(
    data_dir: &Path,
    sequence: u64,
) -> Result<Option<Vec<(SlotId, RecordBatch)>>> {
    let wal_path = data_dir.join(WAL_DIR).join(WAL_FILE);
    let Some(reader) = open_reader(&wal_path)? else {
        return Ok(None);
    };
    for entry in reader.iter_from(0)? {
        let entry = match entry {
            Ok(entry) => entry,
            // A half-written final entry ends the readable WAL.
            Err(WalError::UnexpectedEof(_)) => break,
            Err(error) => return Err(error.into()),
        };
        if entry.sequence != sequence {
            continue;
        }
        let batches = entry
            .slots
            .iter()
            .map(|slot| {
                decode_slot_payload(slot)
                    .map(|batch| (slot.slot_id, batch))
                    .ok_or(WalError::InvalidEntry(
                        "slot payload is not a valid Arrow IPC stream",
                    ))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        return Ok(Some(batches));
    }
    Ok(None)
}

fn open_reader(wal_path: &Path) -> Result<Option<MultiFileWalReader>> {
    match MultiFileWalReader::open(wal_path) {
        Ok(reader) => Ok(Some(reader)),
        Err(WalError::Io(error)) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_cursor(wal_path: &Path) -> Result<Option<u64>> {
    match std::fs::read(CursorSidecar::path_for(wal_path)) {
        Ok(bytes) => Ok(Some(CursorSidecar::decode(&bytes)?.wal_position)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(WalError::Io(error).into()),
    }
}

fn summarize(entry: &WalRecordBundle, cursor_position: Option<u64>) -> WalEntry {
    let ingestion_time = u64::try_from(entry.ingestion_ts_nanos)
        .map_or(UNIX_EPOCH, |nanos| UNIX_EPOCH + Duration::from_nanos(nanos));
    WalEntry {
        sequence: entry.sequence,
        position: entry.offset.position,
        next_position: entry.next_offset,
        ingestion_time,
        slots: entry
            .slots
            .iter()
            .map(|slot| WalEntrySlot {
                slot_id: slot.slot_id,
                schema_fingerprint: slot.schema_fingerprint,
                rows: slot.row_count,
                payload_bytes: slot.payload_len,
            })
            .collect(),
        metadata: entry.metadata.clone(),
        consumed: cursor_position.is_some_and(|cursor| entry.next_offset <= cursor),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::tempdir;

    use super::*;
    use crate::budget::DiskBudget;
    use crate::config::{QuiverConfig, WalConfig};
    use crate::engine::QuiverEngine;
    use crate::record_bundle::{BundleDescriptor, PayloadRef, RecordBundle, SlotDescriptor};

    const SLOT: SlotId = SlotId::new(0);
    const FINGERPRINT: SchemaFingerprint = [7; 32];

    struct TestBundle {
        descriptor: BundleDescriptor,
        batch: RecordBatch,
        metadata: Option<Vec<u8>>,
    }

    impl TestBundle {
        fn new(values: &[i64], metadata: Option<&[u8]>) -> Self {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
                    .expect("test batch");
            Self {
                descriptor: BundleDescriptor::new(vec![SlotDescriptor::new(SLOT, "Values")]),
                batch,
                metadata: metadata.map(<[u8]>::to_vec),
            }
        }
    }

    impl RecordBundle for TestBundle {
        fn descriptor(&self) -> &BundleDescriptor {
            &self.descriptor
        }

        fn ingestion_time(&self) -> SystemTime {
            SystemTime::now()
        }

        fn payload(&self, slot: SlotId) -> Option<PayloadRef<'_>> {
            (slot == SLOT).then_some(PayloadRef {
                schema_fingerprint: FINGERPRINT,
                batch: &self.batch,
            })
        }

        fn metadata(&self) -> Option<&[u8]> {
            self.metadata.as_deref()
        }
    }

    async fn open_engine(dir: &Path) -> Arc<QuiverEngine> {
        let config = QuiverConfig::builder()
            .data_dir(dir)
            .wal(WalConfig {
                flush_interval: Duration::ZERO,
                ..WalConfig::default()
            })
            .build()
            .expect("config");
        QuiverEngine::open(config, Arc::new(DiskBudget::unlimited()))
            .await
            .expect("engine")
    }

    /// Writes three bundles that reach a segment and two that stay WAL-only, as
    /// after a crash between segment finalizations.
    async fn crashed_directory(dir: &Path) {
        let engine = open_engine(dir).await;
        for value in 0..3 {
            engine
                .ingest(&TestBundle::new(&[value, value + 1], Some(b"finalized")))
                .await
                .expect("ingest");
        }
        engine.flush().await.expect("finalize segment");
        engine
            .ingest(&TestBundle::new(&[10, 11, 12], Some(b"pending")))
            .await
            .expect("ingest");
        engine
            .ingest(&TestBundle::new(&[20], None))
            .await
            .expect("ingest");
        // Dropping without shutdown leaves the open segment unfinalized.
    }

    /// Scenario: An engine finalizes a segment holding three bundles and then
    /// stops abruptly after persisting two more bundles to the WAL only.
    /// Guarantees: Inspection lists all five entries in order, marks the first
    /// three as consumed (behind the persisted cursor) and the last two as
    /// replayable, and reports slots, rows, metadata, and WAL positions.
    #[tokio::test]
    async fn inspection_separates_consumed_and_replayable_entries() {
        let dir = tempdir().expect("tempdir");
        crashed_directory(dir.path()).await;

        let wal = inspect_wal(dir.path()).expect("inspect");

        assert_eq!(wal.entries.len(), 5);
        assert_eq!(
            wal.entries
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(
            wal.entries
                .iter()
                .map(|entry| entry.consumed)
                .collect::<Vec<_>>(),
            [true, true, true, false, false]
        );
        assert_eq!(wal.unconsumed().count(), 2);
        assert_eq!(wal.cursor_position, Some(wal.entries[2].next_position));
        assert_eq!(wal.end_position, wal.entries[4].next_position);
        assert!(
            wal.entries
                .windows(2)
                .all(|pair| pair[0].next_position == pair[1].position)
        );
        assert_eq!(wal.entries[3].metadata.as_deref(), Some(&b"pending"[..]));
        assert_eq!(wal.entries[4].metadata, None);
        assert_eq!(wal.entries[3].slots.len(), 1);
        assert_eq!(wal.entries[3].slots[0].slot_id, SLOT);
        assert_eq!(wal.entries[3].slots[0].rows, 3);
        assert_eq!(wal.entries[3].slots[0].schema_fingerprint, FINGERPRINT);
        assert_eq!(wal.files.len(), 1);
        assert_eq!(wal.files[0].rotation_id, None);
        assert!(wal.files[0].bytes > wal.end_position);
        assert_eq!(wal.stopped_early, None);
    }

    /// Scenario: A caller asks for the Arrow payload of a replayable WAL entry
    /// and of a sequence that does not exist.
    /// Guarantees: The entry decodes to its original rows; an unknown sequence
    /// is `None` rather than an error.
    #[tokio::test]
    async fn entry_payloads_decode_to_their_original_rows() {
        let dir = tempdir().expect("tempdir");
        crashed_directory(dir.path()).await;

        let batches = wal_entry_batches(dir.path(), 3)
            .expect("decode")
            .expect("entry exists");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, SLOT);
        let values = batches[0]
            .1
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column")
            .values()
            .to_vec();
        assert_eq!(values, [10, 11, 12]);
        assert_eq!(wal_entry_batches(dir.path(), 99).expect("decode"), None);
    }

    /// Scenario: A crash leaves a half-written entry at the end of the WAL.
    /// Guarantees: Inspection still returns every complete entry, reports why
    /// it stopped, and leaves the WAL file byte-for-byte unchanged.
    #[tokio::test]
    async fn torn_tail_is_reported_without_modifying_the_wal() {
        let dir = tempdir().expect("tempdir");
        crashed_directory(dir.path()).await;
        let wal_file = dir.path().join(WAL_DIR).join(WAL_FILE);
        let _ = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_file)
            .expect("open WAL")
            .write(&[0xAB, 0xCD, 0xEF])
            .expect("append torn bytes");
        let before = std::fs::read(&wal_file).expect("read WAL");

        let wal = inspect_wal(dir.path()).expect("inspect");

        assert_eq!(wal.entries.len(), 5);
        assert!(wal.stopped_early.is_some());
        assert_eq!(std::fs::read(&wal_file).expect("read WAL"), before);
    }

    /// Scenario: A directory has never been opened by an engine.
    /// Guarantees: Inspection succeeds with no files, entries, or cursor.
    #[test]
    fn empty_directory_has_no_wal() {
        let dir = tempdir().expect("tempdir");

        let wal = inspect_wal(dir.path()).expect("inspect");

        assert_eq!(wal, WalInspection::default());
        assert_eq!(wal_entry_batches(dir.path(), 0).expect("decode"), None);
    }
}
