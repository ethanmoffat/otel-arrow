// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration structures for Quiver's persistence engine.

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::Duration;

use arrow_ipc::CompressionType;
use arrow_ipc::writer::IpcWriteOptions;

use crate::error::{QuiverError, Result};
use crate::segment_store::SegmentReadMode;

/// Controls the durability/throughput tradeoff for ingested data.
///
/// This determines whether the write-ahead log (WAL) is used to protect
/// data in the open segment before it is finalized to disk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DurabilityMode {
    /// Full WAL protection: each bundle is written to the WAL before acknowledgement.
    ///
    /// On crash, only bundles written since the last WAL fsync are lost
    /// (controlled by [`WalConfig::flush_interval`]). Call
    /// [`QuiverEngine::sync_wal`](crate::QuiverEngine::sync_wal) to make every
    /// earlier ingest durable before acknowledging it.
    ///
    /// This is the safest option but has lower throughput (~125 bundles/sec typical).
    #[default]
    Wal,

    /// Segment-only durability: WAL is disabled, data is only durable after
    /// segment finalization.
    ///
    /// On crash, the entire open segment is lost (potentially thousands of bundles).
    /// Provides ~3x higher throughput than WAL mode.
    ///
    /// Use this when:
    /// - Throughput is more important than durability
    /// - Data can be re-fetched from upstream on crash
    /// - You have other durability guarantees (e.g., upstream acknowledgement)
    SegmentOnly,
}

/// Arrow IPC buffer compression applied to persisted record batches.
///
/// Compression is applied per record batch by the Arrow IPC writer, and each
/// compressed batch records its codec in the IPC message. Readers detect the
/// codec from the data, so files written with any codec (including `None`)
/// remain readable after the setting changes.
///
/// With the `serde` feature, the value is an object tagged by `codec`, for
/// example `{"codec": "lz4"}` or `{"codec": "zstd", "level": 3}`. A `level`
/// is accepted only with `zstd`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(
        tag = "codec",
        rename_all = "snake_case",
        try_from = "IpcCompressionRepr"
    )
)]
pub enum IpcCompression {
    /// Write uncompressed Arrow IPC buffers.
    #[default]
    None,
    /// LZ4 frame compression. Fast, with a moderate compression ratio.
    Lz4,
    /// Zstandard compression. Higher compression ratio at a higher CPU cost.
    Zstd {
        /// Optional Zstandard compression level. When `None`, the Arrow IPC
        /// writer's default level is used.
        #[cfg_attr(feature = "serde", serde(default))]
        level: Option<i32>,
    },
}

impl IpcCompression {
    /// Builds the Arrow IPC write options for this compression setting.
    ///
    /// `IpcCompression::None` returns [`IpcWriteOptions::default()`], so the
    /// encoded bytes are identical to an uncompressed writer.
    ///
    /// # Errors
    ///
    /// Returns [`QuiverError::InvalidConfig`] if the Arrow IPC writer rejects
    /// the setting, for example a Zstandard level outside the supported range.
    pub fn ipc_write_options(&self) -> Result<IpcWriteOptions> {
        let (codec, level) = match *self {
            Self::None => return Ok(IpcWriteOptions::default()),
            Self::Lz4 => (CompressionType::LZ4_FRAME, None),
            Self::Zstd { level } => (CompressionType::ZSTD, level),
        };
        IpcWriteOptions::default()
            .try_with_compression(Some(codec))
            .and_then(|options| options.try_with_compression_level(level))
            .map_err(|e| {
                QuiverError::invalid_config(format!("unsupported compression {self:?}: {e}"))
            })
    }

    /// Checks that the Arrow IPC writer accepts this setting.
    ///
    /// # Errors
    ///
    /// Returns the same error as [`IpcCompression::ipc_write_options`].
    pub fn validate(&self) -> Result<()> {
        self.ipc_write_options().map(|_| ())
    }
}

/// Deserialization form of [`IpcCompression`]. Serde ignores extra fields on
/// unit variants of internally tagged enums, so parsing goes through this
/// struct to reject a `level` on codecs that do not take one.
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IpcCompressionRepr {
    codec: IpcCodecName,
    #[serde(default)]
    level: Option<i32>,
}

#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum IpcCodecName {
    None,
    Lz4,
    Zstd,
}

#[cfg(feature = "serde")]
impl TryFrom<IpcCompressionRepr> for IpcCompression {
    type Error = &'static str;

    fn try_from(repr: IpcCompressionRepr) -> std::result::Result<Self, Self::Error> {
        match (repr.codec, repr.level) {
            (IpcCodecName::None, None) => Ok(Self::None),
            (IpcCodecName::Lz4, None) => Ok(Self::Lz4),
            (IpcCodecName::Zstd, level) => Ok(Self::Zstd { level }),
            (IpcCodecName::None | IpcCodecName::Lz4, Some(_)) => {
                Err("`level` is only supported with codec `zstd`")
            }
        }
    }
}

/// Top-level configuration for the persistence engine.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QuiverConfig {
    /// Controls durability vs throughput tradeoff.
    pub durability: DurabilityMode,
    /// Write-ahead log tuning parameters (ignored when durability is `SegmentOnly`).
    pub wal: WalConfig,
    /// Segment creation tuning parameters.
    pub segment: SegmentConfig,
    /// Retention controls for finalized data.
    pub retention: RetentionConfig,
    /// Read mode for segment files (mmap vs standard I/O).
    pub read_mode: SegmentReadMode,
    /// Optional override for the base data directory.
    pub data_dir: PathBuf,
}

impl QuiverConfig {
    /// Returns a builder seeded with [`Default`] values.
    #[must_use]
    pub fn builder() -> QuiverConfigBuilder {
        QuiverConfigBuilder::default()
    }

    /// Ensures the configuration holds sane, non-zero values.
    pub fn validate(&self) -> Result<()> {
        // Only validate WAL config if WAL is enabled
        if self.durability == DurabilityMode::Wal {
            self.wal.validate()?;
        }
        self.segment.validate()?;
        self.retention.validate()?;

        if self.data_dir.as_os_str().is_empty() {
            return Err(QuiverError::invalid_config("data_dir must not be empty"));
        }

        Ok(())
    }

    /// Convenience helper that clones the configuration and replaces the
    /// `data_dir` while keeping other settings intact.
    pub fn with_data_dir<P: AsRef<Path>>(&self, data_dir: P) -> Self {
        let mut clone = self.clone();
        clone.data_dir = data_dir.as_ref().to_path_buf();
        clone
    }
}

impl Default for QuiverConfig {
    fn default() -> Self {
        Self {
            durability: DurabilityMode::default(),
            wal: WalConfig::default(),
            segment: SegmentConfig::default(),
            retention: RetentionConfig::default(),
            read_mode: SegmentReadMode::default(),
            data_dir: PathBuf::from("./quiver_data"),
        }
    }
}

/// Builder wrapper to allow incremental overrides while keeping future fields
/// backwards compatible.
#[derive(Debug, Default)]
pub struct QuiverConfigBuilder {
    durability: DurabilityMode,
    wal: WalConfig,
    segment: SegmentConfig,
    retention: RetentionConfig,
    read_mode: SegmentReadMode,
    data_dir: PathBuf,
}

impl QuiverConfigBuilder {
    /// Sets the durability mode.
    #[must_use]
    pub const fn durability(mut self, durability: DurabilityMode) -> Self {
        self.durability = durability;
        self
    }

    /// Applies a custom WAL configuration.
    #[must_use]
    pub const fn wal(mut self, wal: WalConfig) -> Self {
        self.wal = wal;
        self
    }

    /// Applies a custom segment configuration.
    #[must_use]
    pub const fn segment(mut self, segment: SegmentConfig) -> Self {
        self.segment = segment;
        self
    }

    /// Applies a custom retention configuration.
    #[must_use]
    pub const fn retention(mut self, retention: RetentionConfig) -> Self {
        self.retention = retention;
        self
    }

    /// Sets the segment read mode (mmap vs standard I/O).
    #[must_use]
    pub const fn read_mode(mut self, read_mode: SegmentReadMode) -> Self {
        self.read_mode = read_mode;
        self
    }

    /// Overrides the storage directory.
    #[must_use]
    pub fn data_dir<P: AsRef<Path>>(mut self, data_dir: P) -> Self {
        self.data_dir = data_dir.as_ref().to_path_buf();
        self
    }

    /// Consumes the builder and validates the resulting configuration.
    pub fn build(self) -> Result<QuiverConfig> {
        let cfg = QuiverConfig {
            durability: self.durability,
            wal: self.wal,
            segment: self.segment,
            retention: self.retention,
            read_mode: self.read_mode,
            data_dir: if self.data_dir.as_os_str().is_empty() {
                PathBuf::from("./quiver_data")
            } else {
                self.data_dir
            },
        };
        cfg.validate()?;
        Ok(cfg)
    }
}

/// Write-ahead-log related controls.
///
/// Note: WAL disk usage is tracked in the shared [`DiskBudget`](crate::DiskBudget)
/// alongside segments, so the configured disk budget cap applies to the
/// combined total of WAL files and segment files.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WalConfig {
    /// Maximum on-disk footprint (across active + rotated files).
    ///
    /// This is an internal WAL-specific cap that triggers backpressure when
    /// exceeded. The shared [`DiskBudget`](crate::DiskBudget) provides the
    /// overall disk limit that includes both WAL and segment storage.
    pub max_size_bytes: NonZeroU64,
    /// Maximum number of rotated WAL files retained during rotation.
    pub max_rotated_files: u16,
    /// Target data size to keep in the active WAL file before rotating.
    pub rotation_target_bytes: NonZeroU64,
    /// Preferred fsync cadence for durability vs. latency.
    ///
    /// The cadence is checked when a bundle is appended, so an idle WAL is not
    /// synced until the next append or an explicit
    /// [`QuiverEngine::sync_wal`](crate::QuiverEngine::sync_wal). Zero syncs
    /// every append.
    pub flush_interval: Duration,
    /// Arrow IPC compression for slot payloads written to the WAL.
    ///
    /// Compression runs on the ingest path before each append, so it adds CPU
    /// time to every ingested bundle. `max_size_bytes` and
    /// `rotation_target_bytes` measure the bytes written to WAL files, which
    /// are the compressed bytes when compression is enabled.
    ///
    /// Default: [`IpcCompression::None`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub compression: IpcCompression,
}

impl WalConfig {
    fn validate(&self) -> Result<()> {
        if self.max_rotated_files == 0 {
            return Err(QuiverError::invalid_config(
                "max_rotated_files must be at least 1",
            ));
        }
        if self.rotation_target_bytes > self.max_size_bytes {
            return Err(QuiverError::invalid_config(
                "rotation_target_bytes must not exceed max_size_bytes",
            ));
        }
        self.compression.validate()
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            // WAL max should accommodate (max_rotated_files + 1) * rotation_target_bytes
            // with some headroom. At 10 rotated files x 64 MB = 640 MB theoretical max,
            // 128 MB is conservative (allows ~2 rotations worth). The WAL naturally
            // stays smaller because rotation purges old files.
            max_size_bytes: NonZeroU64::new(128 * 1024 * 1024).expect("non-zero"),
            max_rotated_files: 10,
            rotation_target_bytes: NonZeroU64::new(64 * 1024 * 1024).expect("non-zero"),
            flush_interval: Duration::from_millis(25),
            compression: IpcCompression::None,
        }
    }
}

/// Segment tuning parameters.
///
/// Segments are finalized (sealed and written to disk) when *either* trigger fires:
///
/// 1. **Size trigger**: The open segment's estimated payload size reaches
///    [`target_size_bytes`](Self::target_size_bytes).
/// 2. **Time trigger**: The segment has been open longer than
///    [`max_open_duration`](Self::max_open_duration).
///
/// This dual-trigger approach balances efficiency and latency:
///
/// - **High-throughput workloads** hit the size trigger frequently, producing
///   consistently-sized segments with good compression and low metadata overhead.
/// - **Low-throughput or bursty workloads** hit the time trigger, ensuring data
///   becomes visible to downstream subscribers within a bounded latency even when
///   volume is insufficient to fill a segment.
///
/// # Tuning Guidance
///
/// | Scenario | Recommendation |
/// |----------|----------------|
/// | High throughput (>10 MB/s) | Increase `target_size_bytes` to 64-128 MB for better compression |
/// | Latency-sensitive | Decrease `max_open_duration` to 1-2 seconds |
/// | Memory-constrained | Decrease `target_size_bytes` (each core buffers up to this amount) |
/// | Low volume (<1 MB/s) | Default is fine; `max_open_duration` dominates |
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SegmentConfig {
    /// Target payload size (in bytes) for a finalized segment.
    ///
    /// When the open segment's estimated size reaches this threshold, finalization
    /// is triggered. Larger values improve compression and reduce per-segment
    /// overhead, but increase memory usage and latency to visibility.
    ///
    /// Default: 32 MB. Recommended range: 8-128 MB.
    pub target_size_bytes: NonZeroU64,

    /// Maximum time a segment may remain open before forced finalization.
    ///
    /// This ensures bounded latency for downstream visibility even when ingest
    /// volume is low. At high throughput, [`target_size_bytes`](Self::target_size_bytes)
    /// typically triggers first.
    ///
    /// Default: 5 seconds.
    pub max_open_duration: Duration,

    /// Soft cap on the number of streams active within a segment.
    ///
    /// Each unique `(slot, schema_fingerprint)` pair creates a new stream.
    /// Exceeding this limit may trigger early finalization to bound memory.
    pub max_stream_count: u32,

    /// Arrow IPC compression for record batches written to segment files.
    ///
    /// Compression runs when a segment is finalized. Compressed buffers are
    /// decompressed when a batch is read, so memory-mapped reads of compressed
    /// segments copy the decoded data instead of referencing the mapped file.
    ///
    /// [`target_size_bytes`](Self::target_size_bytes) is compared against the
    /// estimated in-memory (uncompressed) size of the open segment, so a
    /// compressed segment file is usually smaller than that target. The disk
    /// budget records the actual file size.
    ///
    /// Default: [`IpcCompression::None`].
    #[cfg_attr(feature = "serde", serde(default))]
    pub compression: IpcCompression,
}

impl SegmentConfig {
    fn validate(&self) -> Result<()> {
        if self.max_stream_count == 0 {
            return Err(QuiverError::invalid_config(
                "max_stream_count must be at least 1",
            ));
        }
        self.compression.validate()
    }
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            target_size_bytes: NonZeroU64::new(32 * 1024 * 1024).expect("non-zero"),
            max_open_duration: Duration::from_secs(5),
            max_stream_count: 512,
            compression: IpcCompression::None,
        }
    }
}

/// Policy that determines how storage pressure is relieved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RetentionPolicy {
    /// Strict durability: pause ingestion instead of deleting unprocessed data.
    #[default]
    Backpressure,
    /// Trade durability for throughput by deleting the oldest data first.
    DropOldest,
}

/// Retention-related configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RetentionConfig {
    /// Optional maximum wall-clock retention irrespective of size.
    ///
    /// When set, segments older than this duration are automatically deleted
    /// during maintenance, regardless of whether subscribers have consumed them.
    /// When `None` (the default), segments are retained indefinitely until
    /// consumed by all subscribers or evicted by size-based retention.
    pub max_age: Option<Duration>,
}

impl RetentionConfig {
    fn validate(&self) -> Result<()> {
        if self.max_age.is_some_and(|d| d.is_zero()) {
            return Err(QuiverError::invalid_config(
                "max_age must be greater than zero when set",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    #[test]
    fn builder_round_trip() {
        let cfg = QuiverConfig::builder()
            .data_dir("/tmp/quiver-test")
            .build()
            .expect("config builds");
        assert_eq!(cfg.data_dir, PathBuf::from("/tmp/quiver-test"));
    }

    #[test]
    fn invalid_policy_is_rejected() {
        let cfg = QuiverConfig {
            retention: RetentionConfig {
                max_age: Some(Duration::ZERO),
            },
            ..QuiverConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn empty_data_dir_is_rejected() {
        let cfg = QuiverConfig {
            data_dir: PathBuf::new(),
            ..QuiverConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn with_data_dir_overrides_path() {
        let cfg = QuiverConfig::default();
        let new_dir = cfg.with_data_dir("/tmp/with-data-dir");

        assert_eq!(new_dir.data_dir, PathBuf::from("/tmp/with-data-dir"));
        assert_eq!(cfg.wal, new_dir.wal);
        assert_eq!(cfg.segment, new_dir.segment);
        assert_eq!(cfg.retention, new_dir.retention);
    }

    #[test]
    fn builder_overrides_sub_configs() {
        let wal = WalConfig {
            max_size_bytes: NonZeroU64::new(1).unwrap(),
            max_rotated_files: 1,
            rotation_target_bytes: NonZeroU64::new(1).unwrap(),
            flush_interval: Duration::from_millis(1),
            compression: IpcCompression::None,
        };
        let segment = SegmentConfig {
            target_size_bytes: NonZeroU64::new(1024).unwrap(),
            max_open_duration: Duration::from_secs(1),
            max_stream_count: 1,
            compression: IpcCompression::None,
        };
        let retention = RetentionConfig {
            max_age: Some(Duration::from_secs(1)),
        };

        let cfg = QuiverConfig::builder()
            .wal(wal.clone())
            .segment(segment.clone())
            .retention(retention.clone())
            .build()
            .expect("builder should validate overrides");

        assert_eq!(cfg.wal, wal);
        assert_eq!(cfg.segment, segment);
        assert_eq!(cfg.retention, retention);
    }

    #[test]
    fn builder_falls_back_to_default_data_dir() {
        let cfg = QuiverConfig::builder()
            .data_dir("")
            .build()
            .expect("builder should fill default data dir");

        assert_eq!(cfg.data_dir, PathBuf::from("./quiver_data"));
    }

    #[test]
    fn wal_validate_rejects_zero_rotated_files() {
        let wal = WalConfig {
            max_size_bytes: NonZeroU64::new(1).unwrap(),
            max_rotated_files: 0,
            rotation_target_bytes: NonZeroU64::new(1).unwrap(),
            flush_interval: Duration::from_millis(1),
            compression: IpcCompression::None,
        };

        assert!(wal.validate().is_err());
    }

    #[test]
    fn wal_validate_rejects_rotation_target_exceeding_cap() {
        let wal = WalConfig {
            max_size_bytes: NonZeroU64::new(1024).unwrap(),
            max_rotated_files: 1,
            rotation_target_bytes: NonZeroU64::new(2048).unwrap(),
            flush_interval: Duration::from_millis(1),
            compression: IpcCompression::None,
        };

        assert!(wal.validate().is_err());
    }

    #[test]
    fn segment_validate_rejects_zero_streams() {
        let segment = SegmentConfig {
            target_size_bytes: NonZeroU64::new(1).unwrap(),
            max_open_duration: Duration::from_secs(1),
            max_stream_count: 0,
            compression: IpcCompression::None,
        };

        assert!(segment.validate().is_err());
    }

    /// Scenario: A WAL or segment config uses a Zstandard level outside the
    /// supported range, validated directly and through the config builder.
    /// Guarantees: Validation fails with an invalid-config error at
    /// configuration time, before any data is written.
    #[test]
    fn validate_rejects_out_of_range_zstd_level() {
        let bad = IpcCompression::Zstd { level: Some(1000) };

        let wal = WalConfig {
            compression: bad,
            ..WalConfig::default()
        };
        assert!(matches!(
            wal.validate(),
            Err(QuiverError::InvalidConfig { .. })
        ));

        let segment = SegmentConfig {
            compression: bad,
            ..SegmentConfig::default()
        };
        assert!(matches!(
            segment.validate(),
            Err(QuiverError::InvalidConfig { .. })
        ));

        let result = QuiverConfig::builder()
            .segment(SegmentConfig {
                compression: bad,
                ..SegmentConfig::default()
            })
            .build();
        assert!(matches!(result, Err(QuiverError::InvalidConfig { .. })));
    }
}
