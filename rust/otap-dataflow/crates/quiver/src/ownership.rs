// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Ownership of a Quiver data directory.
//!
//! A data directory has at most one live owner. Every claim creates the next
//! generation file (`quiver.gen.<n>`) with an exclusive create, so of two
//! openers that decide to claim at the same moment only one wins. The owner
//! records its identity and generation in an ownership marker file
//! ([`MARKER_FILE_NAME`]) that it rewrites periodically (the heartbeat). A new
//! owner removes the generation files below its own, so a previous owner that
//! is still running finds its own generation file gone and stops writing (it
//! is fenced).
//!
//! A separate lock file ([`LOCK_FILE_NAME`]) is locked with an OS file lock for
//! the lifetime of the owner. The lock is a liveness hint: when it can be
//! acquired and the marker says the previous owner held it, the previous owner
//! is gone and the directory can be claimed immediately. When the lock is
//! unavailable or unsupported, the heartbeat age decides.
//!
//! See `ARCHITECTURE.md` ("Data Directory Ownership") for the claim rule and
//! its limitations.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use sysinfo::{Pid, ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System};

use crate::config::OwnershipConfig;
use crate::durable_fs;
use crate::error::{DataDirHolder, QuiverError, Result};
use crate::logging::{otel_debug, otel_error, otel_info, otel_warn};

/// File name of the ownership marker, directly under the data directory.
pub(crate) const MARKER_FILE_NAME: &str = "quiver.owner";
/// File name of the lock file, directly under the data directory.
///
/// The lock file is never replaced or deleted: an OS lock belongs to an open
/// file, and replacing the file would let a second opener lock a different
/// file with the same name.
pub(crate) const LOCK_FILE_NAME: &str = "quiver.lock";
/// Prefix of generation file names. The generation follows as a zero-padded
/// decimal number.
pub(crate) const GENERATION_FILE_PREFIX: &str = "quiver.gen.";
/// Number of low bits of a segment sequence number that count segments within
/// one generation. The generation occupies the bits above.
pub(crate) const GENERATION_SEQ_SHIFT: u32 = 40;
/// Largest generation a data directory can reach, so that
/// `generation << GENERATION_SEQ_SHIFT` fits in a segment sequence number.
pub(crate) const MAX_GENERATION: u64 = (1 << (64 - GENERATION_SEQ_SHIFT)) - 1;
/// Current marker format version.
const MARKER_VERSION: u32 = 2;
/// Oldest marker format version this build reads.
const MIN_MARKER_VERSION: u32 = 1;
/// Number of times a claim lists the generation files again when the newest
/// one disappears while it is being read.
const GENERATION_READ_ATTEMPTS: usize = 3;
/// Allowed difference between a recorded and an observed process start time.
///
/// Some platforms derive the start time from the boot time, which can be
/// reported one second apart by different readers.
const START_TIME_TOLERANCE_SECS: u64 = 1;
/// Host name recorded when the platform does not report one.
const UNKNOWN_HOST: &str = "unknown";

/// Contents of the ownership marker file and of generation files.
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
    /// Generation of the claim. A version 1 marker reads as generation 0.
    pub generation: u64,
    /// Time of the claim in milliseconds since the Unix epoch.
    pub claimed_unix_ms: u64,
    /// Time of the last heartbeat in milliseconds since the Unix epoch.
    pub heartbeat_unix_ms: u64,
    /// Whether the holder released the directory when it closed.
    pub released: bool,
}

/// Reason a marker file could not be decoded.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MarkerError {
    /// The marker declares a format version this build does not understand.
    UnsupportedVersion(String),
    /// The marker is not a valid marker of its declared version.
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
    /// Encodes the marker as `key=value` lines in the current format version.
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
            "lock_held={}\ngeneration={}\nclaimed_unix_ms={}\nheartbeat_unix_ms={}\nreleased={}\n",
            self.lock_held,
            self.generation,
            self.claimed_unix_ms,
            self.heartbeat_unix_ms,
            self.released
        ));
        out
    }

    /// Decodes a marker of version 1 or 2. Unknown keys are ignored so a later
    /// writer of the same version can add optional fields.
    pub(crate) fn parse(bytes: &[u8]) -> std::result::Result<Self, MarkerError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| MarkerError::Malformed("marker is not valid UTF-8".to_owned()))?;
        let mut lines = text
            .lines()
            .map(|line| line.strip_suffix('\r').unwrap_or(line));
        let version_text = lines
            .next()
            .and_then(|line| line.strip_prefix("version="))
            .ok_or_else(|| {
                MarkerError::Malformed("marker does not start with a `version=` line".to_owned())
            })?;
        let version = match version_text.parse::<u32>() {
            Ok(version) if (MIN_MARKER_VERSION..=MARKER_VERSION).contains(&version) => version,
            _ => return Err(MarkerError::UnsupportedVersion(version_text.to_owned())),
        };

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
        fn boolean(key: &str, value: &str) -> std::result::Result<bool, MarkerError> {
            match value {
                "true" => Ok(true),
                "false" => Ok(false),
                other => Err(MarkerError::Malformed(format!(
                    "marker value `{key}={other}` is not `true` or `false`"
                ))),
            }
        }

        // Version 1 markers predate generations and release markers.
        let (generation, released) = if version >= 2 {
            (
                number("generation", required("generation")?)?,
                get("released")
                    .map(|v| boolean("released", v))
                    .transpose()?
                    .unwrap_or(false),
            )
        } else {
            (0, false)
        };
        Ok(Self {
            owner_id: required("owner_id")?.to_owned(),
            host: required("host")?.to_owned(),
            pid: number("pid", required("pid")?)?,
            pid_start_time: get("pid_start_time")
                .map(|v| number("pid_start_time", v))
                .transpose()?,
            pid_namespace: get("pid_namespace").map(str::to_owned),
            lock_held: boolean("lock_held", required("lock_held")?)?,
            generation,
            claimed_unix_ms: number("claimed_unix_ms", required("claimed_unix_ms")?)?,
            heartbeat_unix_ms: number("heartbeat_unix_ms", required("heartbeat_unix_ms")?)?,
            released,
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

    fn holder(
        &self,
        now: SystemTime,
        process_running: bool,
        local: &LocalIdentity,
    ) -> DataDirHolder {
        DataDirHolder {
            owner_id: self.owner_id.clone(),
            host: self.host.clone(),
            pid: self.pid,
            heartbeat_age: self.heartbeat_age(now),
            process_running,
            same_process: local.is_current_process(self),
        }
    }
}

/// The newest generation file, when its generation is above the marker's.
///
/// Such a file belongs to a claim that has created its generation file but
/// has not written its marker yet, or to a claimer that stopped in between.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PendingClaim {
    /// The file names the claimer. Its heartbeat time is the claim time.
    Record(Marker),
    /// The file is empty or cannot be decoded, for example because the
    /// claimer stopped before writing it.
    Unreadable {
        /// Modification time of the file, used as the claim time.
        modified: SystemTime,
    },
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
    /// There is no marker and no generation file.
    Unowned,
    /// The previous holder released the directory when it closed.
    Released,
    /// The lock was acquired and the previous holder had held it.
    LockReleased,
    /// The previous holder's heartbeat is older than `stale_after`.
    HeartbeatStale,
    /// There is no marker and the only claim in progress was abandoned.
    ClaimAbandoned,
}

impl ClaimReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Unowned => "unowned",
            Self::Released => "released",
            Self::LockReleased => "lock_released",
            Self::HeartbeatStale => "heartbeat_stale",
            Self::ClaimAbandoned => "claim_abandoned",
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
    /// Another instance created a newer generation file and has not written
    /// its marker yet, and its claim is not older than `stale_after`.
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
/// A pending claim (a generation file newer than the marker) is checked
/// first:
///
/// 1. The lock was acquired and the claimer held the lock: the claimer is
///    gone, so its claim is abandoned.
/// 2. The lock is held elsewhere and `holder_running` proves the claimer is
///    running: refuse.
/// 3. Otherwise the claim is abandoned once it is older than `stale_after`,
///    and refused before that. An unreadable generation file is aged by its
///    modification time.
///
/// Then the marker decides:
///
/// 1. No marker: claim.
/// 2. The marker says its holder released the directory: claim.
/// 3. The lock was acquired and the marker says its holder held the lock:
///    claim.
/// 4. The lock is held elsewhere and `holder_running` proves the holder is
///    running: refuse, even when its heartbeat is stale.
/// 5. Otherwise claim when the heartbeat is older than `stale_after`, else
///    refuse.
///
/// `holder_running` is only called when the lock is held elsewhere.
pub(crate) fn decide_claim(
    marker: Option<&Marker>,
    pending: Option<&PendingClaim>,
    lock: LockOutcome,
    mut holder_running: impl FnMut(&Marker) -> bool,
    now: SystemTime,
    stale_after: Duration,
) -> ClaimDecision {
    if let Some(pending) = pending {
        let abandoned = match pending {
            PendingClaim::Record(record) => {
                if lock == LockOutcome::Acquired && record.lock_held {
                    true
                } else if lock == LockOutcome::HeldElsewhere && holder_running(record) {
                    return ClaimDecision::Refuse(RefuseReason::HolderRunning);
                } else {
                    record.heartbeat_age(now) > stale_after
                }
            }
            PendingClaim::Unreadable { modified } => {
                now.duration_since(*modified).unwrap_or_default() > stale_after
            }
        };
        if !abandoned {
            return ClaimDecision::Refuse(RefuseReason::ClaimInProgress);
        }
    }
    let Some(marker) = marker else {
        return ClaimDecision::Claim(if pending.is_some() {
            ClaimReason::ClaimAbandoned
        } else {
            ClaimReason::Unowned
        });
    };
    if marker.released {
        return ClaimDecision::Claim(ClaimReason::Released);
    }
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

    /// Returns `true` if the marker names this process: same host, PID
    /// namespace, and PID, with a recorded start time on both sides that
    /// matches. Unlike [`Self::holder_running`], this does not probe the
    /// process table.
    fn is_current_process(&self, marker: &Marker) -> bool {
        if marker.host != self.host
            || marker.pid_namespace != self.pid_namespace
            || marker.pid != self.pid
        {
            return false;
        }
        match (marker.pid_start_time, self.pid_start_time) {
            (Some(recorded), Some(local)) => recorded.abs_diff(local) <= START_TIME_TOLERANCE_SECS,
            _ => false,
        }
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
/// The name is unique per claim (generation and process) so two writers never
/// write the same temporary file, which `durable_fs::replace_file` requires.
fn marker_temp_path(data_dir: &Path, marker: &Marker) -> PathBuf {
    data_dir.join(format!(
        "{MARKER_FILE_NAME}.g{}.{}.tmp",
        marker.generation, marker.pid
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

/// Path of the generation file for `generation`.
pub(crate) fn generation_path(data_dir: &Path, generation: u64) -> PathBuf {
    data_dir.join(format!("{GENERATION_FILE_PREFIX}{generation:020}"))
}

/// Parses a generation file name. Any number of digits is accepted.
fn parse_generation_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix(GENERATION_FILE_PREFIX)?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Lists the generation files in `data_dir`, in ascending numeric order.
fn list_generations(data_dir: &Path) -> Result<Vec<u64>> {
    let entries = fs::read_dir(data_dir).map_err(|e| ownership_io(data_dir, e))?;
    let mut generations = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| ownership_io(data_dir, e))?;
        if let Some(generation) = entry.file_name().to_str().and_then(parse_generation_name) {
            generations.push(generation);
        }
    }
    generations.sort_unstable();
    Ok(generations)
}

/// Reads a generation file. Returns `None` if it no longer exists.
fn read_generation(data_dir: &Path, generation: u64) -> Result<Option<PendingClaim>> {
    let path = generation_path(data_dir, generation);
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(ownership_io(&path, e)),
    };
    let mut bytes = Vec::new();
    let _ = file
        .read_to_end(&mut bytes)
        .map_err(|e| ownership_io(&path, e))?;
    Ok(Some(match Marker::parse(&bytes) {
        Ok(record) => PendingClaim::Record(record),
        Err(_) => PendingClaim::Unreadable {
            modified: file
                .metadata()
                .and_then(|m| m.modified())
                .map_err(|e| ownership_io(&path, e))?,
        },
    }))
}

/// Creates the generation file for `record.generation` with an exclusive
/// create, writes `record` into it, and makes both durable. Returns `false`
/// if the file already exists.
fn create_generation_file(data_dir: &Path, record: &Marker) -> io::Result<bool> {
    let path = generation_path(data_dir, record.generation);
    let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::AlreadyExists => return Ok(false),
        Err(e) => return Err(e),
    };
    file.write_all(record.encode().as_bytes())?;
    file.sync_all()?;
    durable_fs::sync_parent_dir(&path)?;
    Ok(true)
}

/// The marker and pending claim read from a data directory.
struct Snapshot {
    marker: Option<Marker>,
    pending: Option<PendingClaim>,
    /// Highest generation seen in the marker or a generation file.
    newest_generation: u64,
}

/// Reads the marker and the newest generation file.
///
/// Returns `None` if the newest generation file kept disappearing while it
/// was being read, which means other instances are claiming concurrently.
fn read_snapshot(data_dir: &Path, marker_path: &Path) -> Result<Option<Snapshot>> {
    for _ in 0..GENERATION_READ_ATTEMPTS {
        let marker = read_marker(marker_path)?;
        let marker_generation = marker.as_ref().map_or(0, |m| m.generation);
        let newest_file = list_generations(data_dir)?.last().copied().unwrap_or(0);
        if newest_file <= marker_generation {
            return Ok(Some(Snapshot {
                marker,
                pending: None,
                newest_generation: marker_generation,
            }));
        }
        if let Some(pending) = read_generation(data_dir, newest_file)? {
            return Ok(Some(Snapshot {
                marker,
                pending: Some(pending),
                newest_generation: newest_file,
            }));
        }
    }
    Ok(None)
}

/// Allocation-free existence check of this holder's generation file.
struct GenerationProbe {
    path: PathBuf,
    #[cfg(unix)]
    c_path: std::ffi::CString,
    /// NUL-terminated UTF-16 path, or `None` if the path is too long for the
    /// plain Win32 call.
    #[cfg(windows)]
    wide_path: Option<Vec<u16>>,
}

impl GenerationProbe {
    fn new(path: PathBuf) -> io::Result<Self> {
        #[cfg(unix)]
        let c_path = {
            use std::os::unix::ffi::OsStrExt;
            std::ffi::CString::new(path.as_os_str().as_bytes())
                .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?
        };
        #[cfg(windows)]
        let wide_path = {
            use std::os::windows::ffi::OsStrExt;
            // MAX_PATH, including the terminating NUL.
            const MAX_PATH: usize = 260;
            let wide: Vec<u16> = path
                .as_os_str()
                .encode_wide()
                .chain(std::iter::once(0))
                .collect();
            (wide.len() <= MAX_PATH).then_some(wide)
        };
        Ok(Self {
            path,
            #[cfg(unix)]
            c_path,
            #[cfg(windows)]
            wide_path,
        })
    }

    /// Returns whether the file exists with one `stat` call (Unix) or one
    /// `GetFileAttributesW` call (Windows).
    #[cfg(unix)]
    fn exists(&self) -> io::Result<bool> {
        match nix::sys::stat::stat(self.c_path.as_c_str()) {
            Ok(_) => Ok(true),
            Err(errno) => {
                let error = io::Error::from(errno);
                if error.kind() == ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(error)
                }
            }
        }
    }

    #[cfg(windows)]
    #[allow(unsafe_code)]
    fn exists(&self) -> io::Result<bool> {
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileAttributesW, INVALID_FILE_ATTRIBUTES,
        };
        let Some(wide_path) = &self.wide_path else {
            return Self::exists_with_metadata(&self.path);
        };
        // SAFETY: `wide_path` is a NUL-terminated UTF-16 string that outlives
        // the call, and GetFileAttributesW only reads it.
        let attributes = unsafe { GetFileAttributesW(wide_path.as_ptr()) };
        if attributes != INVALID_FILE_ATTRIBUTES {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.kind() == ErrorKind::NotFound {
            Ok(false)
        } else {
            Err(error)
        }
    }

    #[cfg(not(any(unix, windows)))]
    fn exists(&self) -> io::Result<bool> {
        Self::exists_with_metadata(&self.path)
    }

    #[cfg(not(unix))]
    fn exists_with_metadata(path: &Path) -> io::Result<bool> {
        match fs::symlink_metadata(path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// Mutable state of a held claim.
struct ClaimState {
    /// The marker as last written by this holder.
    marker: Marker,
    /// Open lock file. Closing it releases the OS lock.
    lock_file: Option<File>,
}

/// Outcome of a heartbeat attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HeartbeatOutcome {
    /// Less than `heartbeat_interval` has passed since the last heartbeat.
    NotDue,
    /// The marker was rewritten with a new heartbeat time.
    Written,
    /// Another instance claimed the directory; this holder is fenced and
    /// heartbeats have stopped.
    Lost,
    /// Reading or writing the marker failed; the next heartbeat retries.
    Failed,
}

/// A claimed data directory. Dropping it releases the claim.
pub(crate) struct DataDirOwnership {
    data_dir: PathBuf,
    marker_path: PathBuf,
    generation: u64,
    generation_probe: GenerationProbe,
    heartbeat_interval_ms: u64,
    /// Heartbeat time of the last successful marker write, for a cheap due
    /// check without taking the state lock.
    last_heartbeat_ms: AtomicU64,
    /// Set once another instance is known to have claimed the directory.
    /// Never cleared.
    fenced: AtomicBool,
    /// The instance that claimed the directory, set once when fenced.
    new_owner: OnceLock<Option<String>>,
    state: Mutex<ClaimState>,
}

impl std::fmt::Debug for DataDirOwnership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataDirOwnership")
            .field("data_dir", &self.data_dir)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// A claim the rule allows, before its generation file exists.
struct Allowed {
    reason: ClaimReason,
    previous: Option<Marker>,
    generation: u64,
}

impl DataDirOwnership {
    /// Applies the claim rule to `data_dir`, which must exist, creates the
    /// next generation file, and writes the marker if the claim is allowed.
    ///
    /// # Errors
    ///
    /// Returns [`QuiverError::DataDirOwned`] if another instance owns the
    /// directory or is claiming it, [`QuiverError::InvalidOwnershipMarker`] if
    /// the marker cannot be decoded, [`QuiverError::GenerationsExhausted`] if
    /// no generation is left, and [`QuiverError::OwnershipIo`] on I/O
    /// failures.
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
        Self::claim_with_hook(data_dir, config, local, now, || {})
    }

    /// [`claim_as`](Self::claim_as) that calls `before_create` after the
    /// claim rule first allows the claim and before the generation file is
    /// created, so tests can let another claimer run in between.
    fn claim_with_hook(
        data_dir: &Path,
        config: &OwnershipConfig,
        local: &LocalIdentity,
        now: SystemTime,
        before_create: impl FnOnce(),
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

        let now_ms = unix_ms(now);
        let new_marker = |generation: u64| Marker {
            owner_id: config
                .owner_id
                .clone()
                .unwrap_or_else(|| local.host.clone()),
            host: local.host.clone(),
            pid: local.pid,
            pid_start_time: local.pid_start_time,
            pid_namespace: local.pid_namespace.clone(),
            lock_held: lock == LockOutcome::Acquired,
            generation,
            claimed_unix_ms: now_ms,
            heartbeat_unix_ms: now_ms,
            released: false,
        };

        // The exclusive create of the generation file decides between openers
        // that apply the rule at the same time. The loser applies the rule once
        // more against what the winner has written so far.
        let mut allowed = evaluate(data_dir, &marker_path, lock, local, now, config.stale_after)?;
        let mut marker = new_marker(allowed.generation);
        before_create();
        let generation_file = generation_path(data_dir, marker.generation);
        if !create_generation_file(data_dir, &marker)
            .map_err(|e| ownership_io(&generation_file, e))?
        {
            otel_warn!(
                "quiver.ownership.claim",
                path = %data_dir.display(),
                generation = marker.generation,
                reason = "generation_taken",
                message = "another instance created the same generation, applying the claim rule again",
            );
            allowed = evaluate(data_dir, &marker_path, lock, local, now, config.stale_after)?;
            marker = new_marker(allowed.generation);
            let generation_file = generation_path(data_dir, marker.generation);
            if !create_generation_file(data_dir, &marker)
                .map_err(|e| ownership_io(&generation_file, e))?
            {
                return Err(refuse(data_dir, RefuseReason::ClaimInProgress, None));
            }
        }
        let generation = marker.generation;
        let generation_file = generation_path(data_dir, generation);

        // Remove the generation files below this one. A previous holder that is
        // still running finds its own file gone at its next fence check.
        let generations = list_generations(data_dir)?;
        if generations
            .last()
            .is_some_and(|newest| *newest > generation)
        {
            // A later claim has already started; give way to it.
            let _ = fs::remove_file(&generation_file);
            return Err(refuse(data_dir, RefuseReason::ClaimInProgress, None));
        }
        for older in generations.into_iter().filter(|g| *g < generation) {
            let path = generation_path(data_dir, older);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => {}
                Err(e) => return Err(ownership_io(&path, e)),
            }
        }
        durable_fs::sync_parent_dir(&generation_file)
            .map_err(|e| ownership_io(&generation_file, e))?;

        remove_leftover_marker_temps(data_dir);
        write_marker(data_dir, &marker).map_err(|e| ownership_io(&marker_path, e))?;

        let reason = allowed.reason;
        match &allowed.previous {
            None => otel_info!(
                "quiver.ownership.claim",
                path = %data_dir.display(),
                owner_id = %marker.owner_id,
                generation = generation,
                lock_held = marker.lock_held,
                reason = reason.as_str(),
            ),
            Some(previous) if previous.released => otel_info!(
                "quiver.ownership.claim",
                path = %data_dir.display(),
                owner_id = %marker.owner_id,
                generation = generation,
                lock_held = marker.lock_held,
                reason = reason.as_str(),
                previous_owner_id = %previous.owner_id,
            ),
            Some(previous) => otel_warn!(
                "quiver.ownership.claim",
                path = %data_dir.display(),
                owner_id = %marker.owner_id,
                generation = generation,
                lock_held = marker.lock_held,
                reason = reason.as_str(),
                previous_owner_id = %previous.owner_id,
                previous_host = %previous.host,
                previous_pid = previous.pid,
                previous_generation = previous.generation,
                previous_heartbeat_age_ms = previous.heartbeat_age(now).as_millis() as u64,
                message = "took over the data directory from a previous holder",
            ),
        }

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            marker_path,
            generation,
            generation_probe: GenerationProbe::new(generation_file.clone())
                .map_err(|e| ownership_io(&generation_file, e))?,
            heartbeat_interval_ms: u64::try_from(config.heartbeat_interval.as_millis())
                .unwrap_or(u64::MAX),
            last_heartbeat_ms: AtomicU64::new(now_ms),
            fenced: AtomicBool::new(false),
            new_owner: OnceLock::new(),
            state: Mutex::new(ClaimState {
                marker,
                lock_file: Some(lock_file),
            }),
        })
    }

    /// Generation of this claim.
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns `true` once this holder is known to be fenced. Does no I/O.
    pub(crate) fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }

    /// The error returned by every write after this holder was fenced.
    pub(crate) fn fenced_error(&self) -> QuiverError {
        QuiverError::Fenced {
            data_dir: self.data_dir.clone(),
            generation: self.generation,
            new_owner: self.new_owner.get().cloned().flatten(),
        }
    }

    /// Checks that no other instance has claimed the directory since this
    /// claim: this holder's generation file must still exist.
    ///
    /// Costs one `stat` (Unix) or `GetFileAttributesW` (Windows) call and no
    /// allocation until the holder is fenced. Fencing is sticky.
    ///
    /// # Errors
    ///
    /// Returns [`QuiverError::Fenced`] if the holder is fenced, and
    /// [`QuiverError::OwnershipIo`] if the check itself fails (the holder is
    /// not fenced, and the next check tries again).
    pub(crate) fn check_fence(&self) -> Result<()> {
        if self.is_fenced() {
            return Err(self.fenced_error());
        }
        match self.generation_probe.exists() {
            Ok(true) => Ok(()),
            Ok(false) => {
                self.fence("generation_superseded");
                Err(self.fenced_error())
            }
            Err(e) => Err(ownership_io(&self.generation_probe.path, e)),
        }
    }

    /// Marks this holder fenced and logs it once, naming the new holder if
    /// the marker already records a later generation.
    fn fence(&self, reason: &'static str) {
        if self.fenced.swap(true, Ordering::AcqRel) {
            return;
        }
        // The claimer may not have written its marker yet; the marker then
        // still names this holder and says nothing about the new one.
        let current = read_marker(&self.marker_path)
            .ok()
            .flatten()
            .filter(|m| m.generation > self.generation);
        let _ = self.new_owner.set(current.as_ref().map(|m| {
            format!(
                "owner `{}` on host `{}` (pid {}, generation {})",
                m.owner_id, m.host, m.pid, m.generation
            )
        }));
        otel_error!(
            "quiver.ownership.fenced",
            path = %self.data_dir.display(),
            generation = self.generation,
            reason = reason,
            current_owner_id = current.as_ref().map(|m| m.owner_id.as_str()),
            current_host = current.as_ref().map(|m| m.host.as_str()),
            current_pid = current.as_ref().map(|m| m.pid),
            current_generation = current.as_ref().map(|m| m.generation),
            message = "another instance claimed the data directory; this instance no longer \
                       writes to it or acknowledges data, stop it",
        );
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
    /// Before rewriting, the fence is checked and the marker is read back. If
    /// this holder's generation file is gone, or the marker is missing, cannot
    /// be decoded, or names a claim of the same or a later generation, another
    /// instance has taken over: the holder becomes fenced and heartbeats stop.
    /// A marker of an earlier generation was rewritten by a fenced holder that
    /// had not noticed yet, and is replaced. If the check or the read fails
    /// with an I/O error, nothing is written and the next interval retries.
    /// Blocking: call from a blocking context.
    pub(crate) fn heartbeat(&self, now: SystemTime) -> HeartbeatOutcome {
        let mut state = self.state.lock();
        if self.is_fenced() {
            return HeartbeatOutcome::Lost;
        }
        if !self.heartbeat_due(now) {
            return HeartbeatOutcome::NotDue;
        }
        let now_ms = unix_ms(now);
        // Advance the due time even if the write fails, so a failing file
        // system is retried once per interval instead of on every call.
        self.last_heartbeat_ms.store(now_ms, Ordering::Relaxed);

        match self.check_fence() {
            Ok(()) => {}
            Err(QuiverError::Fenced { .. }) => return HeartbeatOutcome::Lost,
            Err(e) => {
                otel_warn!(
                    "quiver.ownership.heartbeat",
                    path = %self.data_dir.display(),
                    error = %e,
                    error_type = "io",
                    message = "fence check failed, retrying after the heartbeat interval",
                );
                return HeartbeatOutcome::Failed;
            }
        }

        match read_marker(&self.marker_path) {
            Ok(Some(current)) if current.same_claim(&state.marker) => {}
            Ok(Some(current)) if current.generation < self.generation => {
                otel_warn!(
                    "quiver.ownership.heartbeat",
                    path = %self.data_dir.display(),
                    generation = self.generation,
                    other_owner_id = %current.owner_id,
                    other_generation = current.generation,
                    reason = "older_generation_marker",
                    message = "a previous holder rewrote the marker before noticing it was \
                               fenced, restoring this claim",
                );
            }
            Ok(_) => {
                self.fence("marker_replaced");
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
            Err(_) => {
                self.fence("marker_invalid");
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

    /// Marks the marker released if this holder is not fenced and the marker
    /// still describes this claim, then releases the lock. The generation file
    /// stays so the next claim continues the generation sequence. Failures are
    /// logged; the next opener then follows the claim rule.
    fn release(&mut self) {
        let fenced = *self.fenced.get_mut();
        let state = self.state.get_mut();
        if fenced {
            otel_warn!(
                "quiver.ownership.release",
                path = %self.data_dir.display(),
                reason = "fenced",
                message = "another instance claimed the directory, marker left in place",
            );
        } else {
            match (
                self.generation_probe.exists(),
                read_marker(&self.marker_path),
            ) {
                (Ok(true), Ok(Some(current))) if current.same_claim(&state.marker) => {
                    let released = Marker {
                        released: true,
                        ..state.marker.clone()
                    };
                    match write_marker(&self.data_dir, &released) {
                        Ok(()) => otel_info!(
                            "quiver.ownership.release",
                            path = %self.data_dir.display(),
                            generation = self.generation,
                        ),
                        Err(e) => otel_warn!(
                            "quiver.ownership.release",
                            path = %self.marker_path.display(),
                            error = %e,
                            error_type = "io",
                            message = "marker not marked released, the next opener applies \
                                       the claim rule",
                        ),
                    }
                }
                (Ok(false), _) | (Ok(true), Ok(_)) => otel_warn!(
                    "quiver.ownership.release",
                    path = %self.data_dir.display(),
                    reason = "ownership_lost",
                    message = "another instance claimed the directory, marker left in place",
                ),
                (Err(e), _) | (_, Err(QuiverError::OwnershipIo { source: e, .. })) => {
                    otel_warn!(
                        "quiver.ownership.release",
                        path = %self.data_dir.display(),
                        error = %e,
                        error_type = "io",
                        message = "ownership files unreadable, marker left in place",
                    );
                }
                (_, Err(e)) => otel_warn!(
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

/// Applies the claim rule to the current directory contents.
///
/// Returns the generation to create if the claim is allowed, or the
/// `DataDirOwned` error to return.
fn evaluate(
    data_dir: &Path,
    marker_path: &Path,
    lock: LockOutcome,
    local: &LocalIdentity,
    now: SystemTime,
    stale_after: Duration,
) -> Result<Allowed> {
    let Some(snapshot) = read_snapshot(data_dir, marker_path)? else {
        return Err(refuse(data_dir, RefuseReason::ClaimInProgress, None));
    };
    let mut probed: Option<(Marker, bool)> = None;
    let decision = decide_claim(
        snapshot.marker.as_ref(),
        snapshot.pending.as_ref(),
        lock,
        |holder| {
            let running = local.holder_running(holder);
            probed = Some((holder.clone(), running));
            running
        },
        now,
        stale_after,
    );
    match decision {
        ClaimDecision::Claim(reason) => {
            let generation = snapshot.newest_generation.saturating_add(1);
            if generation > MAX_GENERATION {
                return Err(QuiverError::GenerationsExhausted {
                    data_dir: data_dir.to_path_buf(),
                    max: MAX_GENERATION,
                });
            }
            Ok(Allowed {
                reason,
                previous: snapshot.marker,
                generation,
            })
        }
        ClaimDecision::Refuse(reason) => {
            let holder = match reason {
                RefuseReason::HolderRunning => {
                    probed.map(|(holder, running)| holder.holder(now, running, local))
                }
                RefuseReason::ClaimInProgress => match snapshot.pending {
                    Some(PendingClaim::Record(record)) => Some(record.holder(now, false, local)),
                    _ => None,
                },
                RefuseReason::HeartbeatFresh => snapshot
                    .marker
                    .map(|marker| marker.holder(now, false, local)),
            };
            Err(refuse(data_dir, reason, holder))
        }
    }
}

/// Logs a refused claim and builds the error.
///
/// The refusal is logged at debug level because the caller decides how
/// serious it is: a caller may wait for a holder in the same process to
/// release the directory and retry, while another reports the returned error.
fn refuse(data_dir: &Path, reason: RefuseReason, holder: Option<DataDirHolder>) -> QuiverError {
    otel_debug!(
        "quiver.ownership.claim",
        path = %data_dir.display(),
        reason = reason.as_str(),
        holder_owner_id = holder.as_ref().map(|h| h.owner_id.as_str()),
        holder_host = holder.as_ref().map(|h| h.host.as_str()),
        holder_pid = holder.as_ref().map(|h| h.pid),
        holder_heartbeat_age_ms = holder.as_ref().map(|h| h.heartbeat_age.as_millis() as u64),
        holder_same_process = holder.as_ref().map(|h| h.same_process),
    );
    QuiverError::DataDirOwned {
        data_dir: data_dir.to_path_buf(),
        holder: holder.map(Box::new),
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

    /// Lists the generation files in `data_dir`.
    pub(crate) fn generations(data_dir: &Path) -> Vec<u64> {
        list_generations(data_dir).expect("generations listed")
    }

    /// A marker for a holder on another host, at generation 0.
    pub(crate) fn foreign_marker(heartbeat: SystemTime, lock_held: bool) -> Marker {
        Marker {
            owner_id: "other-owner".to_owned(),
            host: "other-host.invalid".to_owned(),
            pid: 4242,
            pid_start_time: Some(1_000),
            pid_namespace: None,
            lock_held,
            generation: 0,
            claimed_unix_ms: unix_ms(heartbeat),
            heartbeat_unix_ms: unix_ms(heartbeat),
            released: false,
        }
    }

    /// Creates the generation file of `marker` in `data_dir`.
    pub(crate) fn create_generation(data_dir: &Path, marker: &Marker) {
        assert!(create_generation_file(data_dir, marker).expect("generation created"));
    }

    /// Simulates a complete claim by an instance on another host at `now`:
    /// creates the next generation file, removes the older ones, and writes
    /// the marker. Returns the new marker.
    pub(crate) fn supersede(data_dir: &Path, now: SystemTime) -> Marker {
        let generations = list_generations(data_dir).expect("generations listed");
        let marker_generation = read(data_dir).map_or(0, |m| m.generation);
        let newest = generations
            .last()
            .copied()
            .unwrap_or(0)
            .max(marker_generation);
        let marker = Marker {
            generation: newest + 1,
            ..foreign_marker(now, false)
        };
        assert!(create_generation_file(data_dir, &marker).expect("generation created"));
        for older in generations {
            fs::remove_file(generation_path(data_dir, older)).expect("older generation removed");
        }
        write(data_dir, &marker);
        marker
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
        decide_claim(marker, None, lock, |_| running, at(now), STALE_AFTER)
    }

    /// Scenario: The claim rule runs with no marker and no pending claim under
    /// every lock outcome.
    /// Guarantees: A directory without a marker or generation file is claimed,
    /// even when another open file holds the lock, so a lock kept by a
    /// network file system server for a crashed client never blocks it.
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
            ClaimDecision::Claim(ClaimReason::Unowned)
        );
    }

    /// Scenario: The claim rule runs with a generation file newer than the
    /// marker (a claim in progress) whose record is fresh or stale, under
    /// each lock outcome, with and without a marker.
    /// Guarantees: A fresh claim in progress is refused; a stale one, or one
    /// whose claimer held the lock that is now free, is abandoned and the
    /// marker then decides; a provably running claimer is never taken over.
    #[test]
    fn decide_with_pending_claim() {
        let decide_pending = |marker: Option<&Marker>,
                              pending: &PendingClaim,
                              lock: LockOutcome,
                              running: bool,
                              now: u64| {
            decide_claim(
                marker,
                Some(pending),
                lock,
                |_| running,
                at(now),
                STALE_AFTER,
            )
        };
        let fresh_unlocked = PendingClaim::Record(Marker {
            generation: 1,
            ..marker_at(100, false)
        });
        for lock in [
            LockOutcome::Acquired,
            LockOutcome::HeldElsewhere,
            LockOutcome::Unsupported,
        ] {
            assert_eq!(
                decide_pending(None, &fresh_unlocked, lock, false, 130),
                ClaimDecision::Refuse(RefuseReason::ClaimInProgress),
                "{lock:?}"
            );
            assert_eq!(
                decide_pending(None, &fresh_unlocked, lock, false, 131),
                ClaimDecision::Claim(ClaimReason::ClaimAbandoned),
                "{lock:?}"
            );
        }

        let locked = PendingClaim::Record(Marker {
            generation: 1,
            ..marker_at(100, true)
        });
        assert_eq!(
            decide_pending(None, &locked, LockOutcome::Acquired, false, 101),
            ClaimDecision::Claim(ClaimReason::ClaimAbandoned)
        );
        assert_eq!(
            decide_pending(None, &locked, LockOutcome::HeldElsewhere, true, 10_000),
            ClaimDecision::Refuse(RefuseReason::HolderRunning)
        );

        // An abandoned claim leaves the decision to the marker.
        let fresh_marker = marker_at(120, false);
        assert_eq!(
            decide_pending(
                Some(&fresh_marker),
                &fresh_unlocked,
                LockOutcome::Unsupported,
                false,
                131
            ),
            ClaimDecision::Refuse(RefuseReason::HeartbeatFresh)
        );
        let released = Marker {
            released: true,
            ..fresh_marker
        };
        assert_eq!(
            decide_pending(
                Some(&released),
                &fresh_unlocked,
                LockOutcome::Unsupported,
                false,
                131
            ),
            ClaimDecision::Claim(ClaimReason::Released)
        );

        let unreadable = PendingClaim::Unreadable { modified: at(100) };
        assert_eq!(
            decide_pending(None, &unreadable, LockOutcome::Acquired, false, 130),
            ClaimDecision::Refuse(RefuseReason::ClaimInProgress)
        );
        assert_eq!(
            decide_pending(None, &unreadable, LockOutcome::Acquired, false, 131),
            ClaimDecision::Claim(ClaimReason::ClaimAbandoned)
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
            None,
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
                    None,
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
            Marker::parse(valid.replace("version=2", "version=3").as_bytes()),
            Err(MarkerError::UnsupportedVersion("3".to_owned()))
        );
        let malformed = [
            valid.replace("version=2\n", ""),
            valid.replace("generation=0\n", ""),
            valid.replace("released=false", "released=no"),
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

    /// Scenario: A version 1 marker, written before generations existed, is
    /// decoded, then a directory holding a stale version 1 marker is claimed.
    /// Guarantees: The version 1 marker reads as generation 0 and not
    /// released, and the claim replaces it with a version 2 marker of
    /// generation 1.
    #[test]
    fn version_1_marker_upgrades_to_version_2() {
        let v1 = "version=1\nowner_id=old\nhost=other-host.invalid\npid=7\n\
                  lock_held=false\nclaimed_unix_ms=1000\nheartbeat_unix_ms=2000\n";
        let parsed = Marker::parse(v1.as_bytes()).expect("version 1 decodes");
        assert_eq!(parsed.generation, 0);
        assert!(!parsed.released);
        assert_eq!(parsed.owner_id, "old");
        assert_eq!(parsed.heartbeat_unix_ms, 2_000);

        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join(MARKER_FILE_NAME), v1).expect("write v1 marker");
        let ownership =
            DataDirOwnership::claim(dir.path(), &config(), at(1_000_000)).expect("claim");
        assert_eq!(ownership.generation(), 1);
        let text = fs::read_to_string(dir.path().join(MARKER_FILE_NAME)).expect("read marker");
        assert!(text.starts_with("version=2\n"), "{text}");
        assert_eq!(
            test_support::read(dir.path()).expect("marker").generation,
            1
        );
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

    /// Scenario: A fresh directory is claimed, the claim is dropped, and the
    /// directory is claimed again.
    /// Guarantees: The claim writes a marker naming this process, holding the
    /// lock, and recording generation 1 with a matching generation file.
    /// Dropping the claim marks the marker released and keeps the generation
    /// and lock files; the next claim takes generation 2 at once and removes
    /// the generation 1 file.
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
        assert_eq!(marker.generation, 1);
        assert!(!marker.released);
        assert_eq!(ownership.generation(), 1);
        assert_eq!(test_support::generations(dir.path()), vec![1]);
        assert!(dir.path().join(LOCK_FILE_NAME).exists());

        drop(ownership);
        let released = test_support::read(dir.path()).expect("marker kept");
        assert!(released.released);
        assert_eq!(released.generation, 1);
        assert_eq!(test_support::generations(dir.path()), vec![1]);
        assert!(dir.path().join(LOCK_FILE_NAME).exists());

        let again = DataDirOwnership::claim(dir.path(), &config(), now).expect("claim again");
        assert_eq!(again.generation(), 2);
        assert_eq!(test_support::generations(dir.path()), vec![2]);
    }

    /// Scenario: Two openers both pass the claim rule; the other one creates
    /// the next generation file first (injected between this opener's rule
    /// and its exclusive create) and has not written its marker yet.
    /// Guarantees: Only one opener wins the generation; this opener applies
    /// the rule again, refuses with `DataDirOwned` naming the winner while the
    /// winner's claim is fresh, and leaves the winner's file in place.
    #[test]
    fn generation_race_loser_refuses_while_winner_fresh() {
        let dir = tempdir().expect("tempdir");
        let now = at(1_000_000);
        let winner = Marker {
            generation: 1,
            ..foreign_marker(now, false)
        };
        let err = DataDirOwnership::claim_with_hook(
            dir.path(),
            &config(),
            &LocalIdentity::current(),
            now,
            || assert!(create_generation_file(dir.path(), &winner).expect("winner creates")),
        )
        .expect_err("loser must refuse");
        match err {
            QuiverError::DataDirOwned {
                holder: Some(holder),
                ..
            } => assert_eq!(holder.owner_id, "other-owner"),
            other => panic!("unexpected error {other}"),
        }
        assert_eq!(test_support::generations(dir.path()), vec![1]);
        assert!(test_support::read(dir.path()).is_none());
    }

    /// Scenario: A claimer created generation file 1 and stopped before
    /// writing the marker, once with a decodable record and once with an
    /// empty file; another opener claims while that claim is fresh and after
    /// it is stale.
    /// Guarantees: The unfinished claim is refused while fresh and abandoned
    /// once older than `stale_after`, so a crashed claimer never blocks the
    /// directory for good; the new claim takes generation 2.
    #[test]
    fn crashed_claimer_is_claimable_once_stale() {
        let t0 = at(1_000_000);
        let fresh = t0 + STALE_AFTER;
        let stale = fresh + Duration::from_millis(1);
        for unreadable in [false, true] {
            let dir = tempdir().expect("tempdir");
            let record = Marker {
                generation: 1,
                ..foreign_marker(t0, false)
            };
            assert!(create_generation_file(dir.path(), &record).expect("create"));
            if unreadable {
                let file = OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(generation_path(dir.path(), 1))
                    .expect("open generation file");
                file.set_modified(t0).expect("set mtime");
            }

            let err = DataDirOwnership::claim(dir.path(), &config(), fresh)
                .expect_err("fresh claim in progress");
            assert!(matches!(err, QuiverError::DataDirOwned { .. }), "{err}");

            let ownership =
                DataDirOwnership::claim(dir.path(), &config(), stale).expect("abandoned claim");
            assert_eq!(ownership.generation(), 2);
            assert_eq!(test_support::generations(dir.path()), vec![2]);
        }
    }

    /// Scenario: A holder checks its fence, then another instance claims the
    /// directory and removes the holder's generation file; the file is then
    /// recreated.
    /// Guarantees: The check fails with `Fenced` naming the new owner from its
    /// marker once the file is gone, keeps
    /// failing after it reappears (fencing is sticky), and heartbeats stop.
    #[test]
    fn check_fence_detects_superseded_generation() {
        let dir = tempdir().expect("tempdir");
        let t0 = at(1_000_000);
        let ownership = DataDirOwnership::claim(dir.path(), &config(), t0).expect("claim");
        ownership.check_fence().expect("not fenced");
        assert!(!ownership.is_fenced());

        let other = test_support::supersede(dir.path(), t0 + Duration::from_secs(1));
        assert_eq!(other.generation, 2);
        let err = ownership.check_fence().expect_err("fenced");
        match &err {
            QuiverError::Fenced {
                generation: 1,
                new_owner: Some(new_owner),
                ..
            } => assert!(new_owner.contains("other-owner"), "{new_owner}"),
            other => panic!("unexpected error {other}"),
        }
        assert!(err.to_string().contains("other-owner"), "{err}");
        assert!(err.is_fenced());
        assert!(ownership.is_fenced());

        let ours = Marker {
            generation: 1,
            ..foreign_marker(t0, false)
        };
        assert!(create_generation_file(dir.path(), &ours).expect("recreate"));
        assert!(ownership.check_fence().is_err());
        assert_eq!(
            ownership.heartbeat(t0 + Duration::from_secs(10)),
            HeartbeatOutcome::Lost
        );
        assert_eq!(test_support::read(dir.path()), Some(other));
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
                assert!(!holder.same_process);
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

    /// Scenario: Another instance writes a marker of a later generation while
    /// this claim is held, without removing this holder's generation file.
    /// Guarantees: The next heartbeat reports the loss, fences the holder so
    /// every later fence check fails, and stops writing; release leaves the
    /// other instance's marker in place.
    #[test]
    fn heartbeat_detects_lost_ownership() {
        let dir = tempdir().expect("tempdir");
        let t0 = at(1_000_000);
        let ownership = DataDirOwnership::claim(dir.path(), &config(), t0).expect("claim");
        let other = Marker {
            generation: 5,
            ..marker_at(1_000_002, false)
        };
        test_support::write(dir.path(), &other);
        ownership
            .check_fence()
            .expect("generation file still present");

        let later = t0 + Duration::from_secs(10);
        assert_eq!(ownership.heartbeat(later), HeartbeatOutcome::Lost);
        assert!(ownership.is_fenced());
        assert!(matches!(
            ownership.check_fence(),
            Err(QuiverError::Fenced { .. })
        ));
        assert_eq!(
            ownership.heartbeat(later + Duration::from_secs(10)),
            HeartbeatOutcome::Lost
        );
        drop(ownership);
        assert_eq!(test_support::read(dir.path()), Some(other));
    }

    /// Scenario: A previous holder that had not noticed it was fenced rewrites
    /// the marker with its older generation while this claim is held.
    /// Guarantees: The next heartbeat keeps this claim and restores its own
    /// marker instead of fencing itself.
    #[test]
    fn heartbeat_restores_marker_of_older_generation() {
        let dir = tempdir().expect("tempdir");
        let t0 = at(1_000_000);
        let ownership = DataDirOwnership::claim(dir.path(), &config(), t0).expect("claim");
        test_support::write(dir.path(), &marker_at(1_000_002, false));

        let later = t0 + Duration::from_secs(10);
        assert_eq!(ownership.heartbeat(later), HeartbeatOutcome::Written);
        assert!(!ownership.is_fenced());
        let marker = test_support::read(dir.path()).expect("marker");
        assert_eq!(marker.owner_id, "test-owner");
        assert_eq!(marker.generation, 1);
        assert_eq!(marker.heartbeat_unix_ms, unix_ms(later));
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
            generation: 1,
            claimed_unix_ms: 0,
            heartbeat_unix_ms: 0,
            released: false,
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

    /// A fixed identity for this process, independent of the real one.
    fn this_process() -> LocalIdentity {
        LocalIdentity {
            host: "this-host.invalid".to_owned(),
            pid: 77,
            pid_start_time: Some(5_000),
            pid_namespace: Some("pid:[4026531836]".to_owned()),
        }
    }

    /// A marker written by `local` with its heartbeat at `heartbeat_secs`.
    fn marker_from(local: &LocalIdentity, heartbeat_secs: u64, generation: u64) -> Marker {
        Marker {
            owner_id: "sibling".to_owned(),
            host: local.host.clone(),
            pid: local.pid,
            pid_start_time: local.pid_start_time,
            pid_namespace: local.pid_namespace.clone(),
            lock_held: false,
            generation,
            claimed_unix_ms: unix_ms(at(heartbeat_secs)),
            heartbeat_unix_ms: unix_ms(at(heartbeat_secs)),
            released: false,
        }
    }

    /// Scenario: Markers are compared with a fixed identity of this process:
    /// an exact match, a start time within and beyond the tolerance, and a
    /// differing host, PID namespace, PID, or missing start time on either
    /// side.
    /// Guarantees: Only a marker with the same host, PID namespace, and PID,
    /// and a start time recorded on both sides within the tolerance, is
    /// reported as the current process.
    #[test]
    fn is_current_process_requires_matching_identity() {
        let local = this_process();
        let own = marker_from(&local, 0, 1);
        let cases = [
            ("exact match", own.clone(), true),
            (
                "start time within tolerance",
                Marker {
                    pid_start_time: Some(5_000 + START_TIME_TOLERANCE_SECS),
                    ..own.clone()
                },
                true,
            ),
            (
                "start time beyond tolerance",
                Marker {
                    pid_start_time: Some(5_000 + START_TIME_TOLERANCE_SECS + 1),
                    ..own.clone()
                },
                false,
            ),
            (
                "other host",
                Marker {
                    host: "other-host.invalid".to_owned(),
                    ..own.clone()
                },
                false,
            ),
            (
                "other PID namespace",
                Marker {
                    pid_namespace: Some("pid:[1]".to_owned()),
                    ..own.clone()
                },
                false,
            ),
            (
                "other PID",
                Marker {
                    pid: 78,
                    ..own.clone()
                },
                false,
            ),
            (
                "no recorded start time",
                Marker {
                    pid_start_time: None,
                    ..own.clone()
                },
                false,
            ),
        ];
        for (name, marker, expected) in cases {
            assert_eq!(local.is_current_process(&marker), expected, "{name}");
        }
        let unknown_start = LocalIdentity {
            pid_start_time: None,
            ..local
        };
        assert!(
            !unknown_start.is_current_process(&own),
            "no local start time"
        );
    }

    /// Scenario: The OS lock is unsupported and the directory holds a fresh
    /// marker written by this process, then instead a fresh pending claim
    /// record written by this process with no marker yet.
    /// Guarantees: Both refusals (`HeartbeatFresh` and `ClaimInProgress`)
    /// return `DataDirOwned` with a holder flagged as this same process and
    /// not as a probed running process.
    #[test]
    fn refusal_without_lock_flags_same_process_holder() {
        let local = this_process();
        let now = at(1_000_000);

        let dir = tempdir().expect("tempdir");
        test_support::write(dir.path(), &marker_from(&local, 1_000_000 - 5, 0));
        let marker_path = dir.path().join(MARKER_FILE_NAME);
        let Err(err) = evaluate(
            dir.path(),
            &marker_path,
            LockOutcome::Unsupported,
            &local,
            now,
            STALE_AFTER,
        ) else {
            panic!("fresh heartbeat refuses");
        };
        match err {
            QuiverError::DataDirOwned {
                holder: Some(holder),
                ..
            } => {
                assert_eq!(holder.owner_id, "sibling");
                assert!(holder.same_process);
                assert!(!holder.process_running);
            }
            other => panic!("unexpected error {other}"),
        }

        let dir = tempdir().expect("tempdir");
        test_support::create_generation(dir.path(), &marker_from(&local, 1_000_000 - 5, 1));
        let marker_path = dir.path().join(MARKER_FILE_NAME);
        let Err(err) = evaluate(
            dir.path(),
            &marker_path,
            LockOutcome::Unsupported,
            &local,
            now,
            STALE_AFTER,
        ) else {
            panic!("pending claim refuses");
        };
        match err {
            QuiverError::DataDirOwned {
                holder: Some(holder),
                ..
            } => {
                assert_eq!(holder.owner_id, "sibling");
                assert!(holder.same_process);
                assert!(!holder.process_running);
            }
            other => panic!("unexpected error {other}"),
        }
    }
}
