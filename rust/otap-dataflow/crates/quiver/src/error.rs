// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Error types surfaced by the Quiver crate.

use std::borrow::Cow;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

/// Common result alias for Quiver operations.
pub type Result<T> = std::result::Result<T, QuiverError>;

/// Errors that can be produced by Quiver APIs.
#[derive(Debug, thiserror::Error)]
pub enum QuiverError {
    /// Raised when a caller provides an invalid configuration.
    #[error("invalid configuration: {message}")]
    InvalidConfig {
        /// Human-readable explanation of the validation failure.
        message: Cow<'static, str>,
    },
    /// Placeholder for functionality that has not yet been implemented.
    #[error("feature not implemented: {context}")]
    Unimplemented {
        /// Context string identifying the missing component.
        context: &'static str,
    },
    /// Raised when an operation is cancelled, typically due to shutdown.
    ///
    /// This is a graceful cancellation, not an error. Operations that receive
    /// this should clean up and return without considering it a failure.
    #[error("operation cancelled: {reason}")]
    Cancelled {
        /// Reason for the cancellation (e.g., "shutdown requested").
        reason: Cow<'static, str>,
    },
    /// Raised when storage capacity is exhausted (backpressure signal).
    ///
    /// Callers should slow down or pause ingestion until space is reclaimed.
    /// This is not a fatal error--retry after cleanup or subscriber catch-up.
    #[error("storage at capacity: {available} bytes available (soft cap: {soft_cap})")]
    StorageAtCapacity {
        /// Bytes currently available before hitting the soft cap.
        available: u64,
        /// The configured soft cap (ingest threshold).
        soft_cap: u64,
    },
    /// Wrapper for WAL-specific failures.
    #[error("wal error: {source}")]
    Wal {
        /// Underlying WAL error.
        #[from]
        source: crate::wal::WalError,
    },
    /// Wrapper for segment-specific failures.
    #[error("segment error: {source}")]
    Segment {
        /// Underlying segment error.
        #[from]
        source: crate::segment::SegmentError,
    },
    /// Raised by [`QuiverEngine::open`](crate::QuiverEngine::open) when
    /// another instance owns the data directory.
    ///
    /// The open is not retried. The caller decides whether to retry, wait, or
    /// use a different directory.
    #[error("data directory {} is owned by another instance: {}", data_dir.display(), DataDirOwnedHint(holder.as_deref()))]
    DataDirOwned {
        /// The data directory that could not be claimed.
        data_dir: PathBuf,
        /// The current holder, or `None` if another instance holds the lock
        /// but has not written (or has just removed) its ownership marker.
        holder: Option<Box<DataDirHolder>>,
    },
    /// Raised when the ownership marker in the data directory cannot be
    /// decoded. The marker is never overwritten in this case.
    #[error("invalid ownership marker {}: {message}", path.display())]
    InvalidOwnershipMarker {
        /// Path of the marker file.
        path: PathBuf,
        /// What is wrong and what the operator should check.
        message: String,
    },
    /// Raised when reading or writing the ownership marker or lock file
    /// fails while claiming the data directory.
    #[error("data directory ownership I/O error on {}: {source}", path.display())]
    OwnershipIo {
        /// Path of the file that failed.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
}

/// The current holder of a data directory, as recorded in its ownership
/// marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataDirHolder {
    /// Owner ID of the holder.
    pub owner_id: String,
    /// Host name of the holder.
    pub host: String,
    /// Process ID of the holder on its host.
    pub pid: u32,
    /// Time since the holder's last heartbeat, measured with this host's
    /// clock. Zero if the heartbeat is in the future.
    pub heartbeat_age: Duration,
    /// Whether the holder process was found running on this host.
    pub process_running: bool,
}

/// Formats the operator guidance for [`QuiverError::DataDirOwned`].
struct DataDirOwnedHint<'a>(Option<&'a DataDirHolder>);

impl fmt::Display for DataDirOwnedHint<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(holder) = self.0 else {
            return f.write_str(
                "an instance holds the lock file while starting or stopping; retry, or use a \
                 different data directory",
            );
        };
        write!(
            f,
            "owner `{}` on host `{}` (pid {}), last heartbeat {}s ago; ",
            holder.owner_id,
            holder.host,
            holder.pid,
            holder.heartbeat_age.as_secs()
        )?;
        if holder.process_running {
            f.write_str(
                "that process is running on this host, so stop it or use a different data \
                 directory",
            )
        } else {
            f.write_str(
                "stop that instance or use a different data directory. If it is no longer \
                 running, a new instance takes over once the heartbeat is older than the \
                 configured stale_after",
            )
        }
    }
}

impl QuiverError {
    /// Helper for creating [`QuiverError::InvalidConfig`] values.
    #[must_use]
    pub fn invalid_config(message: impl Into<Cow<'static, str>>) -> Self {
        Self::InvalidConfig {
            message: message.into(),
        }
    }

    /// Helper for creating [`QuiverError::Unimplemented`] values.
    #[must_use]
    pub const fn unimplemented(context: &'static str) -> Self {
        Self::Unimplemented { context }
    }

    /// Helper for creating [`QuiverError::Cancelled`] values.
    #[must_use]
    pub fn cancelled(reason: impl Into<Cow<'static, str>>) -> Self {
        Self::Cancelled {
            reason: reason.into(),
        }
    }

    /// Returns `true` if this error indicates a graceful cancellation.
    ///
    /// Cancelled operations should clean up and return without logging errors.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled { .. })
    }

    /// Returns `true` if this is a backpressure signal (storage at capacity).
    ///
    /// Callers can use this to distinguish recoverable capacity errors from
    /// fatal errors and implement appropriate backoff strategies.
    ///
    /// This returns `true` for both disk budget capacity (`StorageAtCapacity`)
    /// and WAL capacity (`WalAtCapacity`) errors.
    #[must_use]
    pub const fn is_at_capacity(&self) -> bool {
        match self {
            Self::StorageAtCapacity { .. } => true,
            Self::Wal { source } => source.is_at_capacity(),
            _ => false,
        }
    }

    /// Returns `true` if this error is recoverable (retry may succeed).
    ///
    /// Recoverable errors include:
    /// - Capacity errors (retry after space is freed)
    /// - Cancelled operations (not really an error)
    ///
    /// Non-recoverable errors include configuration errors, I/O failures,
    /// and data corruption.
    #[must_use]
    pub fn is_recoverable(&self) -> bool {
        self.is_at_capacity() || self.is_cancelled()
    }
}
