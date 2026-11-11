// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers that make file creation and replacement survive a machine
//! crash.
//!
//! Writing and syncing a file persists its contents, but the directory entry
//! that names the file is separate metadata. A create or rename is only
//! durable once the parent directory has been flushed as well:
//!
//! - On Unix the parent directory is opened read-only and `fsync`ed.
//! - On Windows the parent directory is opened with write access and
//!   `FILE_FLAG_BACKUP_SEMANTICS` (required to obtain a directory handle) and
//!   flushed with `FlushFileBuffers`, which asks the file system to write the
//!   directory's metadata and the storage device to flush its cache.
//!
//! See the "Crash Durability of File Updates" section of `ARCHITECTURE.md`
//! for the supported environments and remaining limitations.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Flag that lets `CreateFileW` return a handle to a directory.
///
/// See <https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew>.
#[cfg(windows)]
const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

/// Returns the directory that holds the entry for `path`.
fn parent_dir(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// Options for opening a directory handle that can be flushed.
fn directory_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FlushFileBuffers requires a handle with write access.
        let _ = options.write(true).custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    }
    #[cfg(not(windows))]
    let _ = options.read(true);
    options
}

/// Flushes the parent directory of `path` so that a preceding create or rename
/// of `path` survives a machine crash.
///
/// On platforms other than Unix and Windows this is a no-op.
pub(crate) fn sync_parent_dir(path: &Path) -> io::Result<()> {
    if cfg!(any(unix, windows)) {
        directory_open_options()
            .open(parent_dir(path))?
            .sync_all()?;
    }
    Ok(())
}

/// Async variant of [`sync_parent_dir`].
pub(crate) async fn sync_parent_dir_async(path: &Path) -> io::Result<()> {
    if cfg!(any(unix, windows)) {
        tokio::fs::OpenOptions::from(directory_open_options())
            .open(parent_dir(path))
            .await?
            .sync_all()
            .await?;
    }
    Ok(())
}

/// Atomically replaces `path` with `contents` and makes the replacement
/// durable.
///
/// The contents are written to `temp_path` and synced, the temporary file is
/// renamed over `path`, and `sync_parent` is called with `path` to persist the
/// rename. Production callers pass [`sync_parent_dir`]; tests substitute a
/// function that fails or pauses after the rename.
///
/// # Errors
///
/// Returns the first I/O error. An error from `sync_parent` means the
/// replacement is already visible but may not survive a machine crash, so a
/// reader can observe either the previous contents or the complete new
/// contents. `temp_path` must not be written concurrently by another caller.
pub(crate) fn replace_file(
    temp_path: &Path,
    path: &Path,
    contents: &[u8],
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(temp_path)?;
        file.write_all(contents)?;
        file.sync_all()?;
    }
    fs::rename(temp_path, path)?;
    sync_parent(path)
}

/// Runs [`replace_file`] on the blocking pool while holding `guard`.
///
/// `guard` is released only when the blocking work ends, even if the returned
/// future is dropped first. Passing an owned lock guard that every writer of
/// `temp_path` acquires therefore keeps a cancelled replacement from
/// overlapping the next one.
///
/// # Errors
///
/// Returns the error from [`replace_file`], or an error if the blocking task
/// panics or is cancelled by runtime shutdown.
pub(crate) async fn replace_file_blocking<G: Send + 'static>(
    temp_path: PathBuf,
    path: PathBuf,
    contents: Vec<u8>,
    guard: G,
    sync_parent: impl FnOnce(&Path) -> io::Result<()> + Send + 'static,
) -> io::Result<()> {
    // spawn_blocking: the write, fsync, rename, and directory flush are
    // blocking calls. Running them as one blocking task bounds each
    // replacement to a single pool thread and keeps `guard` alive until the
    // last step ends, which a sequence of tokio::fs calls cannot guarantee
    // after cancellation.
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        replace_file(&temp_path, &path, &contents, sync_parent)
    })
    .await
    .map_err(io::Error::other)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Scenario: A file is created by `replace_file` when no destination
    /// exists, then replaced by a second call with different contents.
    /// Guarantees: Both the initial creation and the replacement leave the
    /// destination holding exactly the latest contents and remove the
    /// temporary file.
    #[test]
    fn replace_file_creates_then_replaces() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint");
        let temp = dir.path().join("checkpoint.tmp");

        replace_file(&temp, &path, b"first", sync_parent_dir).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"first");
        assert!(!temp.exists());

        replace_file(&temp, &path, b"second, longer", sync_parent_dir).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"second, longer");
        assert!(!temp.exists());
    }

    /// Scenario: The directory flush that follows the rename fails after the
    /// replacement is already visible.
    /// Guarantees: `replace_file` returns the flush error, and the destination
    /// holds the complete new contents instead of a partial file.
    #[test]
    fn replace_file_reports_failure_after_rename() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint");
        let temp = dir.path().join("checkpoint.tmp");
        replace_file(&temp, &path, b"old", sync_parent_dir).unwrap();

        let err = replace_file(&temp, &path, b"new contents", |_| {
            Err(io::Error::other("injected flush failure"))
        })
        .unwrap_err();

        assert_eq!(err.to_string(), "injected flush failure");
        assert_eq!(fs::read(&path).unwrap(), b"new contents");
    }

    /// Scenario: A guarded replacement is cancelled by dropping its future
    /// while its blocking work is paused after the rename, and a second
    /// replacement of the same file is then started.
    /// Guarantees: The cancelled replacement keeps its lock guard until its
    /// blocking work ends, so the second replacement runs only afterwards and
    /// its contents are the ones that remain.
    #[tokio::test]
    async fn cancelled_replacement_holds_guard_until_blocking_work_ends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint");
        let temp = dir.path().join("checkpoint.tmp");
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let first = replace_file_blocking(
            temp.clone(),
            path.clone(),
            b"first".to_vec(),
            Arc::clone(&lock).lock_owned().await,
            move |path| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                sync_parent_dir(path)
            },
        );
        {
            tokio::pin!(first);
            tokio::select! {
                _ = &mut first => panic!("first replacement must pause in sync_parent"),
                entered = entered_rx => entered.unwrap(),
            }
        }
        assert!(
            lock.try_lock().is_err(),
            "the cancelled replacement must keep the guard while its blocking work runs"
        );

        let second = tokio::spawn({
            let lock = Arc::clone(&lock);
            let (temp, path) = (temp.clone(), path.clone());
            async move {
                let guard = lock.lock_owned().await;
                replace_file_blocking(temp, path, b"second".to_vec(), guard, sync_parent_dir).await
            }
        });
        release_tx.send(()).unwrap();
        second.await.unwrap().unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"second");
        assert!(!temp.exists());
    }

    /// Scenario: The parent directory of the target path does not exist when
    /// its directory entry is flushed, on every supported platform.
    /// Guarantees: The directory flush performs real I/O and reports the
    /// failure through both the sync and async helpers instead of silently
    /// succeeding.
    #[tokio::test]
    async fn sync_parent_dir_reports_missing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("file");

        let err = sync_parent_dir(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        let err = sync_parent_dir_async(&path).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);

        sync_parent_dir(&dir.path().join("file")).unwrap();
        sync_parent_dir_async(&dir.path().join("file"))
            .await
            .unwrap();
    }
}
