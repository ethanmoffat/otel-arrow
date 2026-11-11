// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the durable buffer.
//!
//! # Dispatch Strategy
//!
//! The durable buffer should be connected with `RoundRobin` (or `Random`/`LeastLoaded`)
//! dispatch on its incoming edge. Using `Broadcast` will cause each message to be persisted
//! multiple times (once per core), leading to:
//! - Nx storage consumption
//! - Nx duplicate messages forwarded downstream
//!
//! See the [module documentation](super) for more details.

use std::path::PathBuf;
use std::time::Duration;

use byte_unit::{Byte, Unit};
use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_quiver::config::{IpcCompression, RetentionPolicy};
use serde::Deserialize;

/// Default retention size cap (10 GiB).
const fn default_retention_size_cap() -> Byte {
    Byte::from_u64_with_unit(10, Unit::GiB).expect("valid constant")
}

/// Default size cap policy.
const fn default_size_cap_policy() -> SizeCapPolicy {
    SizeCapPolicy::Backpressure
}

/// Default poll interval for checking for available bundles.
const fn default_poll_interval() -> Duration {
    Duration::from_millis(100)
}

/// Default OTLP handling mode.
const fn default_otlp_handling() -> OtlpHandling {
    OtlpHandling::PassThrough
}

/// Default maximum time a segment can stay open (1 seconds).
const fn default_max_segment_open_duration() -> Duration {
    Duration::from_secs(1)
}

/// Default WAL fsync interval (25 milliseconds), matching Quiver's default.
const fn default_wal_flush_interval() -> Duration {
    Duration::from_millis(25)
}

/// Default initial retry interval (1 second).
const fn default_initial_retry_interval() -> Duration {
    Duration::from_secs(1)
}

/// Default maximum retry interval (30 seconds).
const fn default_max_retry_interval() -> Duration {
    Duration::from_secs(30)
}

/// Default retry backoff multiplier.
const fn default_retry_multiplier() -> f64 {
    2.0
}

/// Default maximum bundles in-flight to downstream.
const fn default_max_in_flight() -> usize {
    1000
}

/// How to handle incoming OTLP data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtlpHandling {
    /// Store OTLP bytes as opaque binary (default).
    /// Most CPU-efficient for OTLP-to-OTLP pipelines.
    /// Data is not queryable but pass-through is very fast.
    #[default]
    PassThrough,
    /// Convert OTLP to Arrow format before storing.
    /// More CPU overhead but enables querying/filtering on stored data.
    ConvertToArrow,
}

/// Size cap policy when retention limit is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SizeCapPolicy {
    /// Apply backpressure when at capacity (default, no data loss).
    #[default]
    Backpressure,
    /// Drop oldest data when at capacity (allows controlled data loss).
    DropOldest,
}

/// When the durable buffer acknowledges a bundle to upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamAck {
    /// Acknowledge as soon as the bundle is appended to the write-ahead log
    /// (default). The append may still be in the operating system page cache,
    /// so an OS crash or power loss within `wal_flush_interval` can lose
    /// acknowledged data.
    #[default]
    AfterWalAppend,
    /// Hold the acknowledgement until a write-ahead log fsync that covers the
    /// bundle has completed. Fsyncs are batched at the `wal_flush_interval`
    /// cadence, so acknowledgement latency rises by up to that interval in
    /// exchange for acknowledged data surviving OS crash and power loss.
    AfterWalSync,
}

impl From<SizeCapPolicy> for RetentionPolicy {
    fn from(policy: SizeCapPolicy) -> Self {
        match policy {
            SizeCapPolicy::Backpressure => RetentionPolicy::Backpressure,
            SizeCapPolicy::DropOldest => RetentionPolicy::DropOldest,
        }
    }
}

/// Configuration for the durable buffer.
#[derive(Debug, Clone, Deserialize)]
pub struct DurableBufferConfig {
    /// Directory path for persistent storage.
    pub path: PathBuf,

    /// Maximum total disk space to use across all cores (e.g., "500 GiB", "100 MB").
    /// Supports both IEC (KiB, MiB, GiB) and SI (KB, MB, GB) units.
    ///
    /// This is the **total** disk budget for the pipeline. The processor automatically
    /// divides this across all cores (each core gets `retention_size_cap / num_cores`).
    /// For example, with a 10 GiB cap on a 4-core pipeline, each core gets ~2.5 GiB.
    #[serde(default = "default_retention_size_cap")]
    pub retention_size_cap: Byte,

    /// Maximum age of data to retain (e.g., "24h", "7d").
    ///
    /// Age is measured from when the segment was **finalized** (written to disk),
    /// not from the timestamps within the telemetry data itself.
    ///
    /// When set, segments older than this duration are automatically deleted
    /// during maintenance, regardless of whether they have been consumed.
    /// When `None` (the default), segments are retained indefinitely until
    /// consumed or evicted by size-based retention.
    ///
    /// To survive extended downstream outages without data loss,
    /// set `max_age` longer than the maximum outage duration you need to tolerate.
    #[serde(with = "humantime_serde", default)]
    pub max_age: Option<Duration>,

    /// Policy when size cap is reached.
    #[serde(default = "default_size_cap_policy")]
    pub size_cap_policy: SizeCapPolicy,

    /// Interval for polling for available bundles.
    #[serde(with = "humantime_serde", default = "default_poll_interval")]
    pub poll_interval: Duration,

    /// How to handle incoming OTLP data.
    /// Default: `pass_through` (store as opaque bytes for efficiency).
    #[serde(default = "default_otlp_handling")]
    pub otlp_handling: OtlpHandling,

    /// Maximum time a segment can stay open before automatic finalization.
    ///
    /// Data becomes visible to downstream only after segment finalization.
    /// Lower values reduce latency but increase I/O overhead.
    #[serde(
        with = "humantime_serde",
        default = "default_max_segment_open_duration"
    )]
    pub max_segment_open_duration: Duration,

    /// Maximum time between write-ahead log fsyncs (default: 25ms).
    ///
    /// With the default `upstream_ack: after_wal_append`, upstream is
    /// acknowledged once a bundle is appended to the WAL, so an OS crash or
    /// power loss can lose up to this much acknowledged data that the
    /// operating system had not yet written to disk. Set `0s` to fsync before
    /// every acknowledgement, at the cost of one fsync per ingested bundle, or
    /// set `upstream_ack: after_wal_sync` to hold acknowledgements until the
    /// next batched fsync.
    #[serde(with = "humantime_serde", default = "default_wal_flush_interval")]
    pub wal_flush_interval: Duration,

    /// When to acknowledge a bundle to upstream (default: `after_wal_append`).
    ///
    /// `after_wal_sync` holds each acknowledgement until a WAL fsync covering
    /// the bundle has completed. Fsyncs are batched: held acknowledgements are
    /// released together at most `wal_flush_interval` after the first one was
    /// held. With `wal_flush_interval: 0s` every append is already fsynced, so
    /// both values acknowledge immediately.
    #[serde(default)]
    pub upstream_ack: UpstreamAck,

    /// Arrow IPC compression for bundles written to the write-ahead log
    /// (default: `{codec: none}`).
    ///
    /// Accepts `{codec: none}`, `{codec: lz4}`, or `{codec: zstd}` with an
    /// optional `level`. Compression runs before each WAL append, so it adds
    /// CPU time to every ingested bundle in exchange for fewer bytes written.
    #[serde(default)]
    pub wal_compression: IpcCompression,

    /// Arrow IPC compression for finalized segment files (default:
    /// `{codec: none}`).
    ///
    /// Accepts the same values as `wal_compression`. Compressed segments use
    /// less disk space, and their record batches are decompressed each time
    /// they are read for delivery.
    #[serde(default)]
    pub segment_compression: IpcCompression,

    // --- Retry Configuration ------------------------------------------------
    /// Initial retry delay after first NACK (default: 1s).
    ///
    /// Used as the base for exponential backoff calculation.
    #[serde(with = "humantime_serde", default = "default_initial_retry_interval")]
    pub initial_retry_interval: Duration,

    /// Maximum retry delay (default: 30s).
    ///
    /// Caps the exponential backoff to prevent excessively long waits.
    #[serde(with = "humantime_serde", default = "default_max_retry_interval")]
    pub max_retry_interval: Duration,

    /// Multiplier for exponential backoff (default: 2.0).
    ///
    /// Each retry delay is: min(initial * multiplier^retry_count, max_interval).
    #[serde(default = "default_retry_multiplier")]
    pub retry_multiplier: f64,

    /// Maximum bundles in-flight to downstream (default: 1000).
    ///
    /// Limits concurrent downstream delivery attempts to prevent thundering
    /// herd problems after extended outages or restarts with large backlogs.
    /// When at limit, new sends are deferred until ACKs free up slots.
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: usize,
}

impl DurableBufferConfig {
    /// Get the retention size cap in bytes.
    #[must_use]
    pub const fn size_cap_bytes(&self) -> u64 {
        self.retention_size_cap.as_u64()
    }

    /// Get the retention policy for the storage engine.
    #[must_use]
    pub fn retention_policy(&self) -> RetentionPolicy {
        self.size_cap_policy.into()
    }

    /// Checks settings that parse successfully but can still be rejected,
    /// such as a Zstandard level outside the supported range.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (field, compression) in [
            ("wal_compression", &self.wal_compression),
            ("segment_compression", &self.segment_compression),
        ] {
            compression
                .validate()
                .map_err(|e| ConfigError::InvalidUserConfig {
                    error: format!("{field}: {e}"),
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_cap_serde() {
        // Test string with IEC units
        let json = r#"{"path": "/tmp/test", "retention_size_cap": "500 GiB"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.size_cap_bytes(), 500 * 1024 * 1024 * 1024);

        // Test string with SI units
        let json = r#"{"path": "/tmp/test", "retention_size_cap": "100 MB"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.size_cap_bytes(), 100 * 1000 * 1000);

        // Test raw bytes as string
        let json = r#"{"path": "/tmp/test", "retention_size_cap": "1073741824"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.size_cap_bytes(), 1024 * 1024 * 1024);

        // Test decimal values
        let json = r#"{"path": "/tmp/test", "retention_size_cap": "1.5 GiB"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.size_cap_bytes(),
            (1.5 * 1024.0 * 1024.0 * 1024.0) as u64
        );
    }

    #[test]
    fn test_size_cap_policy_serde() {
        let json = r#"{"path": "/tmp/test", "size_cap_policy": "drop_oldest"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.size_cap_policy, SizeCapPolicy::DropOldest);

        let json = r#"{"path": "/tmp/test", "size_cap_policy": "backpressure"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.size_cap_policy, SizeCapPolicy::Backpressure);
    }

    #[test]
    fn test_config_defaults() {
        let json = r#"{"path": "/tmp/test"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        // Default is 10 GiB
        assert_eq!(config.size_cap_bytes(), 10 * 1024 * 1024 * 1024);
        assert_eq!(config.size_cap_policy, SizeCapPolicy::Backpressure);
        assert_eq!(config.poll_interval, Duration::from_millis(100));
        assert!(config.max_age.is_none());
        // Default OTLP handling is pass_through
        assert_eq!(config.otlp_handling, OtlpHandling::PassThrough);
        // Default retry configuration
        assert_eq!(config.initial_retry_interval, Duration::from_secs(1));
        assert_eq!(config.max_retry_interval, Duration::from_secs(30));
        assert!((config.retry_multiplier - 2.0).abs() < f64::EPSILON);
        assert_eq!(config.max_in_flight, 1000);
        assert_eq!(config.wal_flush_interval, Duration::from_millis(25));
        assert_eq!(config.upstream_ack, UpstreamAck::AfterWalAppend);
    }

    /// Scenario: The upstream acknowledgement point is set to each supported
    /// value and to an unknown value.
    /// Guarantees: Both supported values parse into the matching variant, and
    /// an unknown value fails parsing instead of silently falling back to the
    /// default acknowledgement point.
    #[test]
    fn test_upstream_ack_serde() {
        let json = r#"{"path": "/tmp/test", "upstream_ack": "after_wal_append"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.upstream_ack, UpstreamAck::AfterWalAppend);

        let json = r#"{"path": "/tmp/test", "upstream_ack": "after_wal_sync"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.upstream_ack, UpstreamAck::AfterWalSync);

        let json = r#"{"path": "/tmp/test", "upstream_ack": "after_fsync"}"#;
        let err = serde_json::from_str::<DurableBufferConfig>(json)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown variant `after_fsync`"), "{err}");
    }

    /// Scenario: The WAL fsync cadence is configured to zero.
    /// Guarantees: The setting parses as an immediate fsync request rather than
    /// falling back to the default cadence.
    #[test]
    fn test_wal_flush_interval_serde() {
        let json = r#"{"path": "/tmp/test", "wal_flush_interval": "0s"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.wal_flush_interval, Duration::ZERO);

        let json = r#"{"path": "/tmp/test", "wal_flush_interval": "100ms"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.wal_flush_interval, Duration::from_millis(100));
    }

    #[test]
    fn test_retry_config_serde() {
        let json = r#"{
            "path": "/tmp/test",
            "initial_retry_interval": "500ms",
            "max_retry_interval": "1m",
            "retry_multiplier": 1.5,
            "max_in_flight": 500
        }"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.initial_retry_interval, Duration::from_millis(500));
        assert_eq!(config.max_retry_interval, Duration::from_secs(60));
        assert!((config.retry_multiplier - 1.5).abs() < f64::EPSILON);
        assert_eq!(config.max_in_flight, 500);
    }

    /// Scenario: The WAL and segment compression settings are omitted, or set
    /// to each supported codec, with and without a Zstandard level.
    /// Guarantees: Omitted settings default to no compression, and every
    /// supported codec parses into the matching Quiver setting and validates.
    #[test]
    fn test_compression_serde() {
        let config: DurableBufferConfig = serde_json::from_str(r#"{"path": "/tmp/test"}"#).unwrap();
        assert_eq!(config.wal_compression, IpcCompression::None);
        assert_eq!(config.segment_compression, IpcCompression::None);
        config.validate().expect("default config is valid");

        for (value, expected) in [
            (r#"{"codec": "none"}"#, IpcCompression::None),
            (r#"{"codec": "lz4"}"#, IpcCompression::Lz4),
            (r#"{"codec": "zstd"}"#, IpcCompression::Zstd { level: None }),
            (
                r#"{"codec": "zstd", "level": 7}"#,
                IpcCompression::Zstd { level: Some(7) },
            ),
        ] {
            let json = format!(
                r#"{{"path": "/tmp/test", "wal_compression": {value}, "segment_compression": {value}}}"#
            );
            let config: DurableBufferConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(config.wal_compression, expected, "{value}");
            assert_eq!(config.segment_compression, expected, "{value}");
            config.validate().expect("supported codec is valid");
        }
    }

    /// Scenario: A compression setting names an unknown codec, passes a level
    /// to lz4, or passes an out-of-range Zstandard level.
    /// Guarantees: Unknown codecs and a level on lz4 fail parsing with an
    /// error naming the problem, and an out-of-range level fails validation
    /// with an error naming the offending field.
    #[test]
    fn test_compression_rejects_invalid_settings() {
        let json = r#"{"path": "/tmp/test", "wal_compression": {"codec": "snappy"}}"#;
        let err = serde_json::from_str::<DurableBufferConfig>(json)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown variant `snappy`"), "{err}");

        let json = r#"{"path": "/tmp/test", "segment_compression": {"codec": "lz4", "level": 3}}"#;
        let err = serde_json::from_str::<DurableBufferConfig>(json)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`level` is only supported with codec `zstd`"),
            "{err}"
        );

        let json =
            r#"{"path": "/tmp/test", "segment_compression": {"codec": "zstd", "level": 1000}}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("segment_compression"), "{err}");
    }

    #[test]
    fn test_otlp_handling_serde() {
        let json = r#"{"path": "/tmp/test", "otlp_handling": "pass_through"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.otlp_handling, OtlpHandling::PassThrough);

        let json = r#"{"path": "/tmp/test", "otlp_handling": "convert_to_arrow"}"#;
        let config: DurableBufferConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.otlp_handling, OtlpHandling::ConvertToArrow);
    }
}
