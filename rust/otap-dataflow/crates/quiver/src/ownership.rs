// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Ownership of a Quiver data directory.
//!
//! A data directory has at most one live owner. The owner is recorded in an
//! ownership marker file ([`MARKER_FILE_NAME`]) that the owner rewrites
//! periodically (the heartbeat). A separate lock file ([`LOCK_FILE_NAME`]) is
//! locked with an OS file lock for the lifetime of the owner. The lock is a
//! liveness hint: when it can be acquired and the marker says the previous
//! owner held it, the previous owner is gone and the directory can be claimed
//! immediately. When the lock is unavailable or unsupported, the heartbeat age
//! decides.
//!
//! See `ARCHITECTURE.md` ("Data Directory Ownership") for the claim rule and
//! its limitations.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use sysinfo::{Pid, ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System};

use crate::config::OwnershipConfig;
use crate::durable_fs;
use crate::error::{DataDirHolder, QuiverError, Result};
use crate::logging::{otel_error, otel_info, otel_warn};

/// File name of the ownership marker, directly under the data directory.
pub(crate) const MARKER_FILE_NAME: &str = "quiver.owner";
/// File name of the lock file, directly under the data directory.
///
/// The lock file is never replaced or deleted: an OS lock belongs to an open
/// file, and replacing the file would let a second opener lock a different
/// file with the same name.
pub(crate) const LOCK_FILE_NAME: &str = "quiver.lock";
/// Current marker format version.
const MARKER_VERSION: u32 = 1;
/// Allowed difference between a recorded and an observed process start time.
///
/// Some platforms derive the start time from the boot time, which can be
/// reported one second apart by different readers.
const START_TIME_TOLERANCE_SECS: u64 = 1;
/// Host name recorded when the platform does not report one.
const UNKNOWN_HOST: &str = "unknown";

/// Contents of the ownership marker file.
///
/// Encoded as `key=value` lines so an operator can read it with `cat`. The
/// first line is always `version=<n>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Marker {
    /// Owner ID of the holder (defaults to its host name).
    pub owner_id: String,
    /// Host name of the holder.
    pub host: String,
    /// Process ID of the holder.
    pub pid: u32,
    /// Start time of the holder process in seconds since the Unix epoch.
    pub pid_start_time: Option<u64>,
    /// PID namespace of the holder (Linux only).
    pub pid_namespace: Option<String>,
    /// Whether the holder holds the OS lock on the lock file.
    pub lock_held: bool,
    /// Time of the claim in milliseconds since the Unix epoch.
    pub claimed_unix_ms: u64,
    /// Time of the last heartbeat in milliseconds since the Unix epoch.
    pub heartbeat_unix_ms: u64,
}

/// Reason a marker file could not be decoded.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MarkerError {
    /// The marker declares a format version this build does not understand.
    UnsupportedVersion(String),
    /// The marker is not a valid version 1 marker.
    Malformed(String),
}

impl MarkerError {
    /// Message for the operator, including what to check.
    fn into_message(self) -> String {
        match self {
            Self::UnsupportedVersion(version) => format!(
                "unsupported marker format version {version}; a newer Quiver version may be \
                 using this directory, so check for one. If no instance uses the directory, \
                 delete the file"
            ),
            Self::Malformed(detail) => format!(
                "{detail}; check that no other program writes this file. If no Quiver \
                 instance uses the directory, delete the file"
            ),
        }
    }
}

impl Marker {
    /// Encodes the marker as `key=value` lines.
    pub(crate) fn encode(&self) -> String {
        let mut out = format!(
            "version={MARKER_VERSION}\nowner_id={}\nhost={}\npid={}\n",
            self.owner_id, self.host, self.pid
        );
        if let Some(start) = self.pid_start_time {
            out.push_str(&format!("pid_start_time={start}\n"));
        }
        if let Some(ns) = &self.pid_namespace {
            out.push_str(&format!("pid_namespace={ns}\n"));
        }
        out.push_str(&format!(
            "lock_held={}\nclaimed_unix_ms={}\nheartbeat_unix_ms={}\n",
            self.lock_held, self.claimed_unix_ms, self.heartbeat_unix_ms
        ));
        out
    }

    /// Decodes a marker. Unknown keys are ignored so a later version 1 writer
    /// can add optional fields.
    pub(crate) fn parse(bytes: &[u8]) -> std::result::Result<Self, MarkerError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| MarkerError::Malformed("marker is not valid UTF-8".to_owned()))?;
        let mut lines = text
            .lines()
            .map(|line| line.strip_suffix('\r').unwrap_or(line));
        let version = lines
            .next()
            .and_then(|line| line.strip_prefix("version="))
            .ok_or_else(|| {
                MarkerError::Malformed("marker does not start with a `version=` line".to_owned())
            })?;
        if version.parse::<u32>().ok() != Some(MARKER_VERSION) {
            return Err(MarkerError::UnsupportedVersion(version.to_owned()));
        }

        let mut fields: Vec<(&str, &str)> = Vec::new();
        for line in lines.filter(|line| !line.is_empty()) {
            let (key, value) = line.split_once('=').ok_or_else(|| {
                MarkerError::Malformed(format!("marker line `{line}` is not `key=value`"))
            })?;
            if fields.iter().any(|(existing, _)| *existing == key) {
                return Err(MarkerError::Malformed(format!(
                    "marker key `{key}` appears more than once"
                )));
            }
            fields.push((key, value));
        }
        let get = |key: &str| fields.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);
        let required = |key: &str| {
            get(key).ok_or_else(|| MarkerError::Malformed(format!("marker is missing `{key}`")))
        };
        fn number<T: std::str::FromStr>(
            key: &str,
            value: &str,
        ) -> std::result::Result<T, MarkerError> {
            value.parse().map_err(|_| {
                MarkerError::Malformed(format!("marker value `{key}={value}` is not a number"))
            })
        }

        let lock_held = match required("lock_held")? {
            "true" => true,
            "false" => false,
            other => {
                return Err(MarkerError::Malformed(format!(
                    "marker value `lock_held={other}` is not `true` or `false`"
                )));
            }
        };
        Ok(Self {
            owner_id: required("owner_id")?.to_owned(),
            host: required("host")?.to_owned(),
            pid: number("pid", required("pid")?)?,
            pid_start_time: get("pid_start_time")
                .map(|v| number("pid_start_time", v))
                .transpose()?,
            pid_namespace: get("pid_namespace").map(str::to_owned),
            lock_held,
            claimed_unix_ms: number("claimed_unix_ms", required("claimed_unix_ms")?)?,
            heartbeat_unix_ms: number("heartbeat_unix_ms", required("heartbeat_unix_ms")?)?,
        })
    }

    /// Age of the heartbeat at `now`. A heartbeat in the future has age zero.
    pub(crate) fn heartbeat_age(&self, now: SystemTime) -> Duration {
        Duration::from_millis(unix_ms(now).saturating_sub(self.heartbeat_unix_ms))
    }

    /// Returns `true` if both markers describe the same claim, ignoring the
    /// heartbeat time.
    fn same_claim(&self, other: &Self) -> bool {
        Self {
            heartbeat_unix_ms: other.heartbeat_unix_ms,
            ..self.clone()
        } == *other
    }

    fn holder(&self, now: SystemTime, process_running: bool) -> DataDirHolder {
        DataDirHolder {
            owner_id: self.owner_id.clone(),
            host: self.host.clone(),
            pid: self.pid,
            heartbeat_age: self.heartbeat_age(now),
            process_running,
        }
    }
}

/// Result of trying to take the OS lock on the lock file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LockOutcome {
    /// This opener now holds the lock.
    Acquired,
    /// Another open file holds the lock.
    HeldElsewhere,
    /// The file system reported an error other than "would block".
    Unsupported,
}

/// Why a claim is allowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClaimReason {
    /// There is no marker.
    Unowned,
    /// The lock was acquired and the previous holder had held it.
    LockReleased,
    /// The previous holder's heartbeat is older than `stale_after`.
    HeartbeatStale,
}

impl ClaimReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Unowned => "unowned",
            Self::LockReleased => "lock_released",
            Self::HeartbeatStale => "heartbeat_stale",
        }
    }
}

/// Why a claim is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefuseReason {
    /// Another open file holds the lock and the holder process is running on
    /// this host.
    HolderRunning,
    /// The holder's heartbeat is not older than `stale_after`.
    HeartbeatFresh,
    /// Another open file holds the lock but there is no marker: another
    /// instance is between taking the lock and writing the marker, or between
    /// removing the marker and releasing the lock.
    ClaimInProgress,
}

impl RefuseReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::HolderRunning => "holder_running",
            Self::HeartbeatFresh => "heartbeat_fresh",
            Self::ClaimInProgress => "claim_in_progress",
        }
    }
}

/// Outcome of the claim rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClaimDecision {
    /// Claim the directory.
    Claim(ClaimReason),
    /// Fail fast.
    Refuse(RefuseReason),
}

/// The claim rule.
///
/// 1. No marker, or the lock was acquired and the marker says its holder held
///    the lock: claim. A held lock with no marker refuses instead, because
///    another instance is part way through a claim or a release.
/// 2. The lock is held elsewhere and `holder_running` proves the holder is
///    running: refuse, even when its heartbeat is stale.
/// 3. Otherwise claim when the heartbeat is older than `stale_after`, else
///    refuse.
///
/// `holder_running` is only called when the lock is held elsewhere.
pub(crate) fn decide_claim(
    marker: Option<&Marker>,
    lock: LockOutcome,
    holder_running: impl FnOnce(&Marker) -> bool,
    now: SystemTime,
    stale_after: Duration,
) -> ClaimDecision {
    let Some(marker) = marker else {
        return if lock == LockOutcome::HeldElsewhere {
            ClaimDecision::Refuse(RefuseReason::ClaimInProgress)
        } else {
            ClaimDecision::Claim(ClaimReason::Unowned)
        };
    };
    if lock == LockOutcome::Acquired && marker.lock_held {
        return ClaimDecision::Claim(ClaimReason::LockReleased);
    }
    if lock == LockOutcome::HeldElsewhere && holder_running(marker) {
        return ClaimDecision::Refuse(RefuseReason::HolderRunning);
    }
    if marker.heartbeat_age(now) > stale_after {
        ClaimDecision::Claim(ClaimReason::HeartbeatStale)
    } else {
        ClaimDecision::Refuse(RefuseReason::HeartbeatFresh)
    }
}

/// Identity of the current process as recorded in a marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalIdentity {
    pub host: String,
    pub pid: u32,
    pub pid_start_time: Option<u64>,
    pub pid_namespace: Option<String>,
}

impl LocalIdentity {
    /// Reads the identity of the current process.
    pub(crate) fn current() -> Self {
        let pid = std::process::id();
        Self {
            host: sanitize(&System::host_name().unwrap_or_else(|_| UNKNOWN_HOST.to_owned())),
            pid,
            pid_start_time: process_start_time(pid),
            pid_namespace: current_pid_namespace(),
        }
    }

    /// Returns `true` only if the marker's holder is provably running: same
    /// host, same PID namespace, and a running process with that PID whose
    /// start time matches the marker.
    fn holder_running(&self, marker: &Marker) -> bool {
        if marker.host != self.host || marker.pid_namespace != self.pid_namespace {
            return false;
        }
        let Some(recorded) = marker.pid_start_time else {
            return false;
        };
        process_start_time(marker.pid)
            .is_some_and(|observed| observed.abs_diff(recorded) <= START_TIME_TOLERANCE_SECS)
    }
}

/// Start time of a running process in seconds since the Unix epoch, or `None`
/// if the process does not exist, has exited, or cannot be inspected.
fn process_start_time(pid: u32) -> Option<u64> {
    let mut system = System::new().ok()?;
    let pid = Pid::from_u32(pid);
    let _ = system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    let process = system.process(pid)?;
    if matches!(
        process.status(),
        ProcessStatus::Zombie | ProcessStatus::Dead
    ) {
        return None;
    }
    Some(process.start_time())
}

#[cfg(target_os = "linux")]
fn current_pid_namespace() -> Option<String> {
    fs::read_link("/proc/self/ns/pid")
        .ok()
        .map(|link| sanitize(&link.to_string_lossy()))
}

#[cfg(not(target_os = "linux"))]
const fn current_pid_namespace() -> Option<String> {
    None
}

/// Replaces characters that would break the line format.
fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

fn unix_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn ownership_io(path: &Path, source: io::Error) -> QuiverError {
    QuiverError::OwnershipIo {
        path: path.to_path_buf(),
        source,
    }
}

/// Reads and decodes the marker, returning `None` if it does not exist.
fn read_marker(path: &Path) -> Result<Option<Marker>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(ownership_io(path, e)),
    };
    Marker::parse(&bytes)
        .map(Some)
        .map_err(|e| QuiverError::InvalidOwnershipMarker {
            path: path.to_path_buf(),
            message: e.into_message(),
        })
}

/// Temporary file used to replace the marker.
///
/// The name is unique per claim so two writers never write the same temporary
/// file, which `durable_fs::replace_file` requires.
fn marker_temp_path(data_dir: &Path, marker: &Marker) -> PathBuf {
    data_dir.join(format!(
        "{MARKER_FILE_NAME}.{}.{}.tmp",
        marker.pid, marker.claimed_unix_ms
    ))
}

fn write_marker(data_dir: &Path, marker: &Marker) -> io::Result<()> {
    let temp = marker_temp_path(data_dir, marker);
    let result = durable_fs::replace_file(
        &temp,
        &data_dir.join(MARKER_FILE_NAME),
        marker.encode().as_bytes(),
        durable_fs::sync_parent_dir,
    );
    if result.is_err() {
        // Best effort: the temporary file only exists if the rename failed.
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Removes marker temporary files left by a writer that crashed before its
/// rename. Failures are ignored: a leftover file is harmless.
fn remove_leftover_marker_temps(data_dir: &Path) {
    let Ok(entries) = fs::read_dir(data_dir) else {
        return;
    };
    let prefix = format!("{MARKER_FILE_NAME}.");
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&prefix) && name.ends_with(".tmp") {
            let _ = fs::remove_file(entry.path());
        }
    }
}

/// Mutable state of a held claim.
struct ClaimState {
    /// The marker as last written by this holder.
    marker: Marker,
    /// Open lock file. Closing it releases the OS lock.
    lock_file: Option<File>,
    /// Set when the marker on disk no longer describes this claim.
    lost: bool,
}

/// Outcome of a heartbeat attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeartbeatOutcome {
    /// Less than `heartbeat_interval` has passed since the last heartbeat.
    NotDue,
    /// The marker was rewritten with a new heartbeat time.
    Written,
    /// The marker no longer describes this claim; heartbeats have stopped.
    Lost,
    /// Reading or writing the marker failed; the next heartbeat retries.
    Failed,
}

/// A claimed data directory. Dropping it releases the claim.
pub(crate) struct DataDirOwnership {
    data_dir: PathBuf,
    marker_path: PathBuf,
    heartbeat_interval_ms: u64,
    /// Heartbeat time of the last successful marker write, for a cheap due
    /// check without taking the state lock.
    last_heartbeat_ms: AtomicU64,
    state: Mutex<ClaimState>,
}

impl std::fmt::Debug for DataDirOwnership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataDirOwnership")
            .field("data_dir", &self.data_dir)
            .finish_non_exhaustive()
    }
}

impl DataDirOwnership {
    /// Applies the claim rule to `data_dir`, which must exist, and writes the
    /// marker if the claim is allowed.
    ///
    /// # Errors
    ///
    /// Returns [`QuiverError::DataDirOwned`] if another instance owns the
    /// directory, [`QuiverError::InvalidOwnershipMarker`] if the marker cannot
    /// be decoded, and [`QuiverError::OwnershipIo`] on I/O failures.
    pub(crate) fn claim(
        data_dir: &Path,
        config: &OwnershipConfig,
        now: SystemTime,
    ) -> Result<Self> {
        let local = LocalIdentity::current();
        Self::claim_as(data_dir, config, &local, now)
    }

    fn claim_as(
        data_dir: &Path,
        config: &OwnershipConfig,
        local: &LocalIdentity,
        now: SystemTime,
    ) -> Result<Self> {
        let lock_path = data_dir.join(LOCK_FILE_NAME);
        let marker_path = data_dir.join(MARKER_FILE_NAME);
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| ownership_io(&lock_path, e))?;
        let lock = match lock_file.try_lock() {
            Ok(()) => LockOutcome::Acquired,
            Err(TryLockError::WouldBlock) => LockOutcome::HeldElsewhere,
            Err(TryLockError::Error(e)) => {
                otel_warn!(
                    "quiver.ownership.claim",
                    path = %lock_path.display(),
                    error = %e,
                    error_type = "io",
                    reason = "lock_unsupported",
                    message = "file lock unavailable, ownership relies on the heartbeat",
                );
                LockOutcome::Unsupported
            }
        };

        let existing = read_marker(&marker_path)?;
        let mut holder_running = false;
        let decision = decide_claim(
            existing.as_ref(),
            lock,
            |marker| {
                holder_running = local.holder_running(marker);
                holder_running
            },
            now,
            config.stale_after,
        );

        let reason = match decision {
            ClaimDecision::Refuse(reason) => {
                let holder = existing.as_ref().map(|m| m.holder(now, holder_running));
                otel_error!(
                    "quiver.ownership.claim",
                    path = %data_dir.display(),
                    reason = reason.as_str(),
                    holder_owner_id = holder.as_ref().map(|h| h.owner_id.as_str()),
                    holder_host = holder.as_ref().map(|h| h.host.as_str()),
                    holder_pid = holder.as_ref().map(|h| h.pid),
                    holder_heartbeat_age_ms = holder.as_ref().map(|h| h.heartbeat_age.as_millis() as u64),
                );
                return Err(QuiverError::DataDirOwned {
                    data_dir: data_dir.to_path_buf(),
                    holder: holder.map(Box::new),
                });
            }
            ClaimDecision::Claim(reason) => reason,
        };

        let now_ms = unix_ms(now);
        let marker = Marker {
            owner_id: config
                .owner_id
                .clone()
                .unwrap_or_else(|| local.host.clone()),
            host: local.host.clone(),
            pid: local.pid,
            pid_start_time: local.pid_start_time,
            pid_namespace: local.pid_namespace.clone(),
            lock_held: lock == LockOutcome::Acquired,
            claimed_unix_ms: now_ms,
            heartbeat_unix_ms: now_ms,
        };
        remove_leftover_marker_temps(data_dir);
        write_marker(data_dir, &marker).map_err(|e| ownership_io(&marker_path, e))?;

        match &existing {
            None => otel_info!(
                "quiver.ownership.claim",
                path = %data_dir.display(),
                owner_id = %marker.owner_id,
                lock_held = marker.lock_held,
                reason = reason.as_str(),
            ),
            Some(previous) => otel_warn!(
                "quiver.ownership.claim",
                path = %data_dir.display(),
                owner_id = %marker.owner_id,
                lock_held = marker.lock_held,
                reason = reason.as_str(),
                previous_owner_id = %previous.owner_id,
                previous_host = %previous.host,
                previous_pid = previous.pid,
                previous_heartbeat_age_ms = previous.heartbeat_age(now).as_millis() as u64,
                message = "took over the data directory from a previous holder",
            ),
        }

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            marker_path,
            heartbeat_interval_ms: u64::try_from(config.heartbeat_interval.as_millis())
                .unwrap_or(u64::MAX),
            last_heartbeat_ms: AtomicU64::new(now_ms),
            state: Mutex::new(ClaimState {
                marker,
                lock_file: Some(lock_file),
                lost: false,
            }),
        })
    }

    /// Returns `true` if a heartbeat is due at `now`. Does no I/O.
    ///
    /// A clock that moved backwards also makes the heartbeat due, so the
    /// marker never keeps a heartbeat time ahead of the local clock.
    pub(crate) fn heartbeat_due(&self, now: SystemTime) -> bool {
        let now_ms = unix_ms(now);
        let last = self.last_heartbeat_ms.load(Ordering::Relaxed);
        now_ms < last || now_ms - last >= self.heartbeat_interval_ms
    }

    /// Rewrites the marker with heartbeat time `now` if a heartbeat is due.
    ///
    /// Before rewriting, the marker is read back. If it is missing, cannot be
    /// decoded, or no longer describes this claim, another instance has taken
    /// over: the loss is logged once and heartbeats stop. If the read fails
    /// with an I/O error, nothing is written and the next interval retries.
    /// Blocking: call from a blocking context.
    pub(crate) fn heartbeat(&self, now: SystemTime) -> HeartbeatOutcome {
        let mut state = self.state.lock();
        if state.lost {
            return HeartbeatOutcome::Lost;
        }
        if !self.heartbeat_due(now) {
            return HeartbeatOutcome::NotDue;
        }
        let now_ms = unix_ms(now);
        // Advance the due time even if the write fails, so a failing file
        // system is retried once per interval instead of on every call.
        self.last_heartbeat_ms.store(now_ms, Ordering::Relaxed);

        match read_marker(&self.marker_path) {
            Ok(Some(current)) if current.same_claim(&state.marker) => {}
            Ok(current) => {
                state.lost = true;
                otel_error!(
                    "quiver.ownership.heartbeat",
                    path = %self.data_dir.display(),
                    reason = "ownership_lost",
                    current_owner_id = current.as_ref().map(|m| m.owner_id.as_str()),
                    current_host = current.as_ref().map(|m| m.host.as_str()),
                    current_pid = current.as_ref().map(|m| m.pid),
                    message = "the ownership marker no longer names this instance; \
                               heartbeats stopped, stop this instance",
                );
                return HeartbeatOutcome::Lost;
            }
            Err(e @ QuiverError::OwnershipIo { .. }) => {
                // A read failure (for example a network file system timeout)
                // says nothing about who holds the directory. Skip the write so
                // another claim is never overwritten, and retry next interval.
                otel_warn!(
                    "quiver.ownership.heartbeat",
                    path = %self.marker_path.display(),
                    error = %e,
                    error_type = "io",
                    message = "marker read failed, retrying after the heartbeat interval",
                );
                return HeartbeatOutcome::Failed;
            }
            Err(e) => {
                state.lost = true;
                otel_error!(
                    "quiver.ownership.heartbeat",
                    path = %self.data_dir.display(),
                    error = %e,
                    reason = "ownership_lost",
                    message = "the ownership marker cannot be decoded; heartbeats stopped, \
                               stop this instance",
                );
                return HeartbeatOutcome::Lost;
            }
        }

        let mut next = state.marker.clone();
        next.heartbeat_unix_ms = now_ms;
        match write_marker(&self.data_dir, &next) {
            Ok(()) => {
                state.marker = next;
                HeartbeatOutcome::Written
            }
            Err(e) => {
                otel_warn!(
                    "quiver.ownership.heartbeat",
                    path = %self.marker_path.display(),
                    error = %e,
                    error_type = "io",
                    message = "heartbeat write failed, retrying after the heartbeat interval",
                );
                HeartbeatOutcome::Failed
            }
        }
    }

    /// Removes the marker if it still describes this claim, then releases the
    /// lock. Failures are logged; the next opener then follows the claim rule.
    fn release(&mut self) {
        let state = self.state.get_mut();
        if !state.lost {
            match read_marker(&self.marker_path) {
                Ok(Some(current)) if current.same_claim(&state.marker) => {
                    let removed = fs::remove_file(&self.marker_path)
                        .and_then(|()| durable_fs::sync_parent_dir(&self.marker_path));
                    match removed {
                        Ok(()) => otel_info!(
                            "quiver.ownership.release",
                            path = %self.data_dir.display(),
                        ),
                        Err(e) => otel_warn!(
                            "quiver.ownership.release",
                            path = %self.marker_path.display(),
                            error = %e,
                            error_type = "io",
                            message = "marker not removed, the next opener applies the claim rule",
                        ),
                    }
                }
                Ok(_) => otel_warn!(
                    "quiver.ownership.release",
                    path = %self.data_dir.display(),
                    reason = "ownership_lost",
                    message = "marker names another instance and was left in place",
                ),
                Err(e) => otel_warn!(
                    "quiver.ownership.release",
                    path = %self.marker_path.display(),
                    error = %e,
                    message = "marker unreadable and left in place",
                ),
            }
        }
        // Closing the file releases the OS lock. The lock file stays.
        drop(state.lock_file.take());
    }
}

impl Drop for DataDirOwnership {
    fn drop(&mut self) {
        self.release();
    }
}

/// Test helpers shared with engine tests.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// Reads the marker in `data_dir`.
    pub(crate) fn read(data_dir: &Path) -> Option<Marker> {
        read_marker(&data_dir.join(MARKER_FILE_NAME)).expect("marker readable")
    }

    /// Writes `marker` into `data_dir`.
    pub(crate) fn write(data_dir: &Path, marker: &Marker) {
        write_marker(data_dir, marker).expect("marker written");
    }

    /// A marker for a holder on another host.
    pub(crate) fn foreign_marker(heartbeat: SystemTime, lock_held: bool) -> Marker {
        Marker {
            owner_id: "other-owner".to_owned(),
            host: "other-host.invalid".to_owned(),
            pid: 4242,
            pid_start_time: Some(1_000),
            pid_namespace: None,
            lock_held,
            claimed_unix_ms: unix_ms(heartbeat),
            heartbeat_unix_ms: unix_ms(heartbeat),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::foreign_marker;
    use super::*;
    use tempfile::tempdir;

    const STALE_AFTER: Duration = Duration::from_secs(30);

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn marker_at(heartbeat_secs: u64, lock_held: bool) -> Marker {
        foreign_marker(at(heartbeat_secs), lock_held)
    }

    fn config() -> OwnershipConfig {
        OwnershipConfig {
            owner_id: Some("test-owner".to_owned()),
            heartbeat_interval: Duration::from_secs(5),
            stale_after: STALE_AFTER,
        }
    }

    fn decide(
        marker: Option<&Marker>,
        lock: LockOutcome,
        running: bool,
        now: u64,
    ) -> ClaimDecision {
        decide_claim(marker, lock, |_| running, at(now), STALE_AFTER)
    }

    /// Scenario: The claim rule runs with no marker under every lock outcome.
    /// Guarantees: A directory without a marker is claimed unless another open
    /// file holds the lock, which means a claim or release is in progress.
    #[test]
    fn decide_without_marker() {
        assert_eq!(
            decide(None, LockOutcome::Acquired, false, 100),
            ClaimDecision::Claim(ClaimReason::Unowned)
        );
        assert_eq!(
            decide(None, LockOutcome::Unsupported, false, 100),
            ClaimDecision::Claim(ClaimReason::Unowned)
        );
        assert_eq!(
            decide(None, LockOutcome::HeldElsewhere, false, 100),
            ClaimDecision::Refuse(RefuseReason::ClaimInProgress)
        );
    }

    /// Scenario: The lock is acquired and the marker says its holder held the
    /// lock, with a fresh heartbeat.
    /// Guarantees: The directory is claimed immediately because the previous
    /// holder released the lock by exiting, and liveness is not consulted.
    #[test]
    fn decide_lock_acquired_holder_held_lock() {
        let marker = marker_at(100, true);
        let decision = decide_claim(
            Some(&marker),
            LockOutcome::Acquired,
            |_| panic!("liveness must not be probed"),
            at(101),
            STALE_AFTER,
        );
        assert_eq!(decision, ClaimDecision::Claim(ClaimReason::LockReleased));
    }

    /// Scenario: The lock is acquired but the marker says its holder did not
    /// hold the lock.
    /// Guarantees: The heartbeat decides: fresh refuses, stale claims.
    #[test]
    fn decide_lock_acquired_holder_without_lock() {
        let marker = marker_at(100, false);
        assert_eq!(
            decide(Some(&marker), LockOutcome::Acquired, false, 130),
            ClaimDecision::Refuse(RefuseReason::HeartbeatFresh)
        );
        assert_eq!(
            decide(Some(&marker), LockOutcome::Acquired, false, 131),
            ClaimDecision::Claim(ClaimReason::HeartbeatStale)
        );
    }

    /// Scenario: The lock is held elsewhere and the holder is provably running,
    /// with both a fresh and a stale heartbeat.
    /// Guarantees: A running holder on this host is never taken over, even
    /// when it has stopped heartbeating.
    #[test]
    fn decide_lock_held_holder_running() {
        let marker = marker_at(100, true);
        assert_eq!(
            decide(Some(&marker), LockOutcome::HeldElsewhere, true, 101),
            ClaimDecision::Refuse(RefuseReason::HolderRunning)
        );
        assert_eq!(
            decide(Some(&marker), LockOutcome::HeldElsewhere, true, 10_000),
            ClaimDecision::Refuse(RefuseReason::HolderRunning)
        );
    }

    /// Scenario: The lock is held elsewhere and the holder cannot be proven
    /// running (for example it is on another host).
    /// Guarantees: The heartbeat decides: fresh refuses, stale claims.
    #[test]
    fn decide_lock_held_holder_not_provably_running() {
        let marker = marker_at(100, true);
        assert_eq!(
            decide(Some(&marker), LockOutcome::HeldElsewhere, false, 130),
            ClaimDecision::Refuse(RefuseReason::HeartbeatFresh)
        );
        assert_eq!(
            decide(Some(&marker), LockOutcome::HeldElsewhere, false, 131),
            ClaimDecision::Claim(ClaimReason::HeartbeatStale)
        );
    }

    /// Scenario: The lock is unsupported by the file system, whatever the
    /// marker says about the previous holder's lock.
    /// Guarantees: The heartbeat decides and liveness is not consulted.
    #[test]
    fn decide_lock_unsupported() {
        for lock_held in [true, false] {
            let marker = marker_at(100, lock_held);
            for (now, expected) in [
                (100, ClaimDecision::Refuse(RefuseReason::HeartbeatFresh)),
                (131, ClaimDecision::Claim(ClaimReason::HeartbeatStale)),
            ] {
                let decision = decide_claim(
                    Some(&marker),
                    LockOutcome::Unsupported,
                    |_| panic!("liveness must not be probed"),
                    at(now),
                    STALE_AFTER,
                );
                assert_eq!(decision, expected);
            }
        }
    }

    /// Scenario: The marker's heartbeat is ahead of the reader's clock.
    /// Guarantees: A future heartbeat counts as fresh, so clock skew toward the
    /// future never makes a holder stale.
    #[test]
    fn decide_future_heartbeat_is_fresh() {
        let marker = marker_at(1_000, false);
        assert_eq!(marker.heartbeat_age(at(10)), Duration::ZERO);
        assert_eq!(
            decide(Some(&marker), LockOutcome::Unsupported, false, 10),
            ClaimDecision::Refuse(RefuseReason::HeartbeatFresh)
        );
    }

    /// Scenario: A marker with and without optional fields is encoded and
    /// decoded, including Windows line endings and an unknown key.
    /// Guarantees: Encoding round-trips, `\r\n` and unknown keys are accepted.
    #[test]
    fn marker_round_trip() {
        let mut marker = marker_at(100, true);
        assert_eq!(
            Marker::parse(marker.encode().as_bytes()),
            Ok(marker.clone())
        );
        marker.pid_start_time = None;
        marker.pid_namespace = Some("pid:[4026531836]".to_owned());
        assert_eq!(
            Marker::parse(marker.encode().as_bytes()),
            Ok(marker.clone())
        );

        let crlf = format!("{}future_key=1\n", marker.encode()).replace('\n', "\r\n");
        assert_eq!(Marker::parse(crlf.as_bytes()), Ok(marker));
    }

    /// Scenario: Markers with an unknown version, a missing version line,
    /// missing or duplicate keys, bad values, or invalid UTF-8 are decoded.
    /// Guarantees: Each is rejected with the matching error, so a marker that
    /// this build does not understand is never silently overwritten.
    #[test]
    fn marker_rejects_unknown_or_corrupt() {
        let valid = marker_at(100, true).encode();
        assert_eq!(
            Marker::parse(valid.replace("version=1", "version=2").as_bytes()),
            Err(MarkerError::UnsupportedVersion("2".to_owned()))
        );
        let malformed = [
            valid.replace("version=1\n", ""),
            valid.replace("pid=4242\n", ""),
            format!("{valid}host=again\n"),
            valid.replace("pid=4242", "pid=abc"),
            valid.replace("lock_held=true", "lock_held=yes"),
            format!("{valid}no separator\n"),
            String::new(),
        ];
        for text in malformed {
            assert!(
                matches!(
                    Marker::parse(text.as_bytes()),
                    Err(MarkerError::Malformed(_))
                ),
                "accepted {text:?}"
            );
        }
        assert!(matches!(
            Marker::parse(&[0xff, 0xfe]),
            Err(MarkerError::Malformed(_))
        ));
    }

    /// Scenario: A data directory holds a corrupt marker or one with an unknown
    /// version when a claim is attempted.
    /// Guarantees: The claim fails with `InvalidOwnershipMarker` and the
    /// marker is left untouched.
    #[test]
    fn claim_rejects_invalid_marker() {
        let dir = tempdir().expect("tempdir");
        for contents in ["version=9\nowner_id=x\n", "garbage"] {
            let path = dir.path().join(MARKER_FILE_NAME);
            fs::write(&path, contents).expect("write marker");
            let err = DataDirOwnership::claim(dir.path(), &config(), SystemTime::now())
                .expect_err("claim must fail");
            assert!(
                matches!(err, QuiverError::InvalidOwnershipMarker { .. }),
                "unexpected error {err}"
            );
            assert_eq!(fs::read_to_string(&path).expect("read"), contents);
        }
    }

    /// Scenario: A fresh directory is claimed, then the claim is dropped.
    /// Guarantees: The claim writes a marker naming this process and holding
    /// the lock, creates the lock file, and dropping the claim removes the
    /// marker but keeps the lock file.
    #[test]
    fn claim_and_release_fresh_directory() {
        let dir = tempdir().expect("tempdir");
        let now = SystemTime::now();
        let ownership = DataDirOwnership::claim(dir.path(), &config(), now).expect("claim");
        let marker = test_support::read(dir.path()).expect("marker exists");
        let local = LocalIdentity::current();
        assert_eq!(marker.owner_id, "test-owner");
        assert_eq!(marker.host, local.host);
        assert_eq!(marker.pid, std::process::id());
        assert!(marker.lock_held, "lock should be supported on a local disk");
        assert_eq!(marker.heartbeat_unix_ms, unix_ms(now));
        assert!(dir.path().join(LOCK_FILE_NAME).exists());

        drop(ownership);
        assert!(test_support::read(dir.path()).is_none());
        assert!(dir.path().join(LOCK_FILE_NAME).exists());
    }

    /// Scenario: A directory holds a marker from another host with an old
    /// heartbeat, then one with a fresh heartbeat; the lock is free.
    /// Guarantees: The stale holder is taken over; the fresh one makes the
    /// claim fail with `DataDirOwned` naming that holder.
    #[test]
    fn claim_stale_and_fresh_foreign_holder() {
        let dir = tempdir().expect("tempdir");
        let now = at(1_000_000);
        test_support::write(dir.path(), &marker_at(1_000_000 - 31, false));
        let ownership = DataDirOwnership::claim(dir.path(), &config(), now).expect("takeover");
        assert_eq!(
            test_support::read(dir.path()).expect("marker").owner_id,
            "test-owner"
        );
        drop(ownership);

        test_support::write(dir.path(), &marker_at(1_000_000 - 30, false));
        let err = DataDirOwnership::claim(dir.path(), &config(), now).expect_err("fresh holder");
        match err {
            QuiverError::DataDirOwned {
                holder: Some(holder),
                ..
            } => {
                assert_eq!(holder.owner_id, "other-owner");
                assert_eq!(holder.host, "other-host.invalid");
                assert_eq!(holder.pid, 4242);
                assert_eq!(holder.heartbeat_age, Duration::from_secs(30));
                assert!(!holder.process_running);
            }
            other => panic!("unexpected error {other}"),
        }
    }

    /// Scenario: Heartbeats are attempted at injected times before, at, and
    /// after the heartbeat interval.
    /// Guarantees: The marker is rewritten only once the interval has passed
    /// since the last write, and a clock that moved backwards rewrites it.
    #[test]
    fn heartbeat_respects_interval() {
        let dir = tempdir().expect("tempdir");
        let t0 = at(1_000_000);
        let ownership = DataDirOwnership::claim(dir.path(), &config(), t0).expect("claim");
        let heartbeat = || {
            test_support::read(dir.path())
                .expect("marker")
                .heartbeat_unix_ms
        };

        let early = t0 + Duration::from_millis(4_999);
        assert!(!ownership.heartbeat_due(early));
        assert_eq!(ownership.heartbeat(early), HeartbeatOutcome::NotDue);
        assert_eq!(heartbeat(), unix_ms(t0));

        let t1 = t0 + Duration::from_secs(5);
        assert_eq!(ownership.heartbeat(t1), HeartbeatOutcome::Written);
        assert_eq!(heartbeat(), unix_ms(t1));
        assert_eq!(
            ownership.heartbeat(t1 + Duration::from_secs(4)),
            HeartbeatOutcome::NotDue
        );

        let backwards = t0 - Duration::from_secs(60);
        assert_eq!(ownership.heartbeat(backwards), HeartbeatOutcome::Written);
        assert_eq!(heartbeat(), unix_ms(backwards));
    }

    /// Scenario: Another instance replaces the marker while this claim is held.
    /// Guarantees: The next heartbeat reports the loss and stops writing, and
    /// release leaves the other instance's marker in place.
    #[test]
    fn heartbeat_detects_lost_ownership() {
        let dir = tempdir().expect("tempdir");
        let t0 = at(1_000_000);
        let ownership = DataDirOwnership::claim(dir.path(), &config(), t0).expect("claim");
        let other = marker_at(1_000_002, false);
        test_support::write(dir.path(), &other);

        let later = t0 + Duration::from_secs(10);
        assert_eq!(ownership.heartbeat(later), HeartbeatOutcome::Lost);
        assert_eq!(
            ownership.heartbeat(later + Duration::from_secs(10)),
            HeartbeatOutcome::Lost
        );
        drop(ownership);
        assert_eq!(test_support::read(dir.path()), Some(other));
    }

    /// Scenario: Reading the marker fails with an I/O error during a
    /// heartbeat (simulated by replacing the marker with a directory), then
    /// the marker becomes readable again.
    /// Guarantees: The failed heartbeat writes nothing and does not give up
    /// the claim; the heartbeat after the next interval succeeds.
    #[test]
    fn heartbeat_retries_after_marker_read_error() {
        let dir = tempdir().expect("tempdir");
        let t0 = at(1_000_000);
        let ownership = DataDirOwnership::claim(dir.path(), &config(), t0).expect("claim");
        let claimed = test_support::read(dir.path()).expect("marker");
        let marker_path = dir.path().join(MARKER_FILE_NAME);
        fs::remove_file(&marker_path).expect("remove marker");
        fs::create_dir(&marker_path).expect("marker path as directory");

        let t1 = t0 + Duration::from_secs(5);
        assert_eq!(ownership.heartbeat(t1), HeartbeatOutcome::Failed);
        assert!(marker_path.is_dir());

        fs::remove_dir(&marker_path).expect("remove directory");
        test_support::write(dir.path(), &claimed);
        let t2 = t1 + Duration::from_secs(5);
        assert_eq!(ownership.heartbeat(t2), HeartbeatOutcome::Written);
        assert_eq!(
            test_support::read(dir.path())
                .expect("marker")
                .heartbeat_unix_ms,
            unix_ms(t2)
        );
    }

    /// Scenario: The current process probes its own liveness from a marker it
    /// would write, and from markers with a different host, PID namespace,
    /// start time, or no start time.
    /// Guarantees: Only a marker matching this host, namespace, PID, and start
    /// time is provably running.
    #[test]
    fn holder_running_requires_matching_identity() {
        let local = LocalIdentity::current();
        let own = Marker {
            owner_id: "me".to_owned(),
            host: local.host.clone(),
            pid: local.pid,
            pid_start_time: local.pid_start_time,
            pid_namespace: local.pid_namespace.clone(),
            lock_held: true,
            claimed_unix_ms: 0,
            heartbeat_unix_ms: 0,
        };
        assert!(local.holder_running(&own));
        assert!(!local.holder_running(&Marker {
            host: "other-host.invalid".to_owned(),
            ..own.clone()
        }));
        assert!(!local.holder_running(&Marker {
            pid_namespace: Some("pid:[1]".to_owned()),
            ..own.clone()
        }));
        assert!(!local.holder_running(&Marker {
            pid_start_time: own.pid_start_time.map(|s| s + 3_600),
            ..own.clone()
        }));
        assert!(!local.holder_running(&Marker {
            pid_start_time: None,
            ..own
        }));
    }
}
