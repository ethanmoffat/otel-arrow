// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Upstream acknowledgements held until a WAL sync, for
//! `upstream_ack: after_wal_sync`.
//!
//! After a bundle is appended to the WAL, the processor holds its upstream ack
//! here instead of sending it. A single WAL sync (the barrier) then covers every
//! held ack at once, so fsyncs are batched (group commit). The processor runs
//! the barrier when the deadline wakeup fires, when the held list is full, and
//! before shutdown completes.
//!
//! Guarantees:
//!
//! - held acks are only handed back for release after the barrier returned
//!   `Ok`; because the processor handles messages one at a time, every ack held
//!   before the barrier started belongs to a bundle whose append completed
//!   before it, so the barrier covers all of them
//! - a failed barrier hands every held ack back for a NACK; nothing is dropped
//! - the held list never exceeds its capacity, and at most one deadline wakeup
//!   is tracked at a time
//!
//! This type owns no engine or effect handler state, so the hold, release and
//! failure paths can be tested with an injected barrier.

use otel_arrow_dfe_engine::control::{WakeupRevision, WakeupSlot};
use std::future::Future;

/// Wakeup slot for the WAL sync deadline. Slot 0 is the retry slot.
pub(super) const WAL_SYNC_WAKEUP_SLOT: WakeupSlot = WakeupSlot(1);

/// Maximum number of acks held before the processor runs the barrier
/// immediately.
///
/// Each held entry is only the request context with an empty payload, so the
/// memory cost is small. The bound also caps how many bundles one failed
/// barrier can NACK.
pub(super) const MAX_HELD_ACKS: usize = 1024;

/// Acks held until the next WAL sync, plus the deadline wakeup armed for them.
pub(super) struct WalSyncAcks<T> {
    held: Vec<T>,
    capacity: usize,
    armed_revision: Option<WakeupRevision>,
}

impl<T> WalSyncAcks<T> {
    /// Creates an empty list that holds at most `capacity` acks.
    pub(super) fn new(capacity: usize) -> Self {
        debug_assert!(capacity > 0, "held ack capacity must be nonzero");
        Self {
            held: Vec::new(),
            capacity: capacity.max(1),
            armed_revision: None,
        }
    }

    /// Returns the number of held acks.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.held.len()
    }

    /// Returns true when nothing is held.
    pub(super) fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    /// Returns true when the list is at capacity. The caller must run the
    /// barrier before holding another ack.
    pub(super) fn is_full(&self) -> bool {
        self.held.len() >= self.capacity
    }

    /// Holds one ack. Returns true when it is the first held ack, which means
    /// the caller must arm the deadline wakeup.
    pub(super) fn hold(&mut self, item: T) -> bool {
        debug_assert!(!self.is_full(), "run the barrier before holding more acks");
        let first = self.held.is_empty();
        self.held.push(item);
        first
    }

    /// Records the revision of the deadline wakeup armed for the held acks.
    pub(super) fn set_armed(&mut self, revision: WakeupRevision) {
        self.armed_revision = Some(revision);
    }

    /// Clears the armed deadline. Returns true when one was armed, so the
    /// caller can cancel the engine wakeup.
    pub(super) fn disarm(&mut self) -> bool {
        self.armed_revision.take().is_some()
    }

    /// Accepts a wakeup only when it matches the armed deadline slot and
    /// revision, and clears the armed deadline when it does. Wakeups for other
    /// slots and stale revisions are ignored.
    pub(super) fn accept_wakeup(&mut self, slot: WakeupSlot, revision: WakeupRevision) -> bool {
        if slot != WAL_SYNC_WAKEUP_SLOT || self.armed_revision != Some(revision) {
            return false;
        }
        self.armed_revision = None;
        true
    }

    /// Runs `barrier` and takes every held ack.
    ///
    /// Returns the acks to release on success, or the error together with the
    /// acks to NACK on failure. The barrier is not called when nothing is held.
    /// The armed deadline is left untouched; the caller disarms it first.
    pub(super) async fn sync_and_take<F, Fut, E>(
        &mut self,
        barrier: F,
    ) -> Result<Vec<T>, (E, Vec<T>)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        if self.held.is_empty() {
            return Ok(Vec::new());
        }
        let result = barrier().await;
        let taken = std::mem::take(&mut self.held);
        match result {
            Ok(()) => Ok(taken),
            Err(error) => Err((error, taken)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// Scenario: Three acks are held and the barrier succeeds.
    /// Guarantees: Only the first hold asks for the deadline to be armed, the
    /// barrier runs exactly once, and every held ack is returned for release
    /// in hold order, leaving the list empty.
    #[tokio::test]
    async fn successful_barrier_releases_all_held_in_order() {
        let mut acks = WalSyncAcks::new(8);
        assert!(acks.hold(1));
        assert!(!acks.hold(2));
        assert!(!acks.hold(3));
        assert_eq!(acks.len(), 3);

        let calls = Cell::new(0);
        let released = acks
            .sync_and_take(|| async {
                calls.set(calls.get() + 1);
                Ok::<(), &str>(())
            })
            .await
            .expect("barrier succeeds");

        assert_eq!(released, vec![1, 2, 3]);
        assert_eq!(calls.get(), 1);
        assert!(acks.is_empty());
        assert!(acks.hold(4), "next hold after release re-arms the deadline");
    }

    /// Scenario: Acks are held and the injected barrier fails.
    /// Guarantees: Every held ack is returned with the error for a NACK, none
    /// is returned for release, and the list is empty afterwards so no ack is
    /// left behind or reported twice.
    #[tokio::test]
    async fn failed_barrier_returns_all_held_for_nack() {
        let mut acks = WalSyncAcks::new(8);
        let _ = acks.hold("a");
        let _ = acks.hold("b");

        let (error, to_nack) = acks
            .sync_and_take(|| async { Err::<(), _>("fsync failed") })
            .await
            .expect_err("barrier fails");

        assert_eq!(error, "fsync failed");
        assert_eq!(to_nack, vec!["a", "b"]);
        assert!(acks.is_empty());
    }

    /// Scenario: The barrier is requested while nothing is held.
    /// Guarantees: The barrier is not called, so an idle node never performs a
    /// WAL sync on its own behalf.
    #[tokio::test]
    async fn empty_list_skips_barrier() {
        let mut acks = WalSyncAcks::<u32>::new(4);
        let calls = Cell::new(0);
        let released = acks
            .sync_and_take(|| async {
                calls.set(calls.get() + 1);
                Ok::<(), &str>(())
            })
            .await
            .expect("no barrier");
        assert!(released.is_empty());
        assert_eq!(calls.get(), 0);
    }

    /// Scenario: Acks are held until the list reaches its capacity.
    /// Guarantees: The list reports full exactly at the capacity, which is the
    /// signal the processor uses to run the barrier before holding more.
    #[test]
    fn reports_full_at_capacity() {
        let mut acks = WalSyncAcks::new(2);
        let _ = acks.hold(1);
        assert!(!acks.is_full());
        let _ = acks.hold(2);
        assert!(acks.is_full());
    }

    /// Scenario: A deadline is armed, then wakeups arrive for another slot,
    /// with a stale revision, and with the armed revision.
    /// Guarantees: Only the matching slot and revision are accepted, and the
    /// armed deadline is cleared exactly once.
    #[test]
    fn accept_wakeup_matches_slot_and_revision() {
        let mut acks = WalSyncAcks::<u32>::new(4);
        assert!(!acks.accept_wakeup(WAL_SYNC_WAKEUP_SLOT, 7));

        acks.set_armed(7);
        assert!(!acks.accept_wakeup(WakeupSlot(0), 7));
        assert!(!acks.accept_wakeup(WAL_SYNC_WAKEUP_SLOT, 6));
        assert!(acks.accept_wakeup(WAL_SYNC_WAKEUP_SLOT, 7));
        assert!(!acks.accept_wakeup(WAL_SYNC_WAKEUP_SLOT, 7));
    }

    /// Scenario: A deadline is armed and then disarmed before it fires.
    /// Guarantees: Disarm reports the armed deadline once so the caller cancels
    /// the engine wakeup, and the old revision is no longer accepted.
    #[test]
    fn disarm_clears_armed_deadline() {
        let mut acks = WalSyncAcks::<u32>::new(4);
        assert!(!acks.disarm());
        acks.set_armed(3);
        assert!(acks.disarm());
        assert!(!acks.disarm());
        assert!(!acks.accept_wakeup(WAL_SYNC_WAKEUP_SLOT, 3));
    }
}
