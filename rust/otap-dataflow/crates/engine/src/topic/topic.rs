// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Topic internals, nothing here is directly exposed to users.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::error::Error;
use crate::error::Error::{
    MessageNotTracked, SubscribeBalancedNotSupported, SubscribeBroadcastNotSupported,
    SubscribeSingleGroupViolation, SubscriptionClosed, TopicClosed,
};
use crate::topic::backend::{PublishFuture, PublishTrackedFuture, SubscriptionBackend, TopicState};
use crate::topic::subscription::{Delivery, RecvDelivery};
use crate::topic::types::{
    AckFromResult, BroadcastSubscriberId, Envelope, NackFromResult, PublishOutcome, RoutingKey,
    SubscriberOptions, TopicOptions, TrackedPublishOutcome, TrackedPublishPermit,
    TrackedPublishReceipt, TrackedPublishTracker, TrackedTryPublishOutcome,
};
use futures_core::Stream;
use otel_arrow_dfe_config::topic::{TopicBroadcastAckMode, TopicBroadcastOnLagPolicy};
use otel_arrow_dfe_config::{SubscriptionGroupName, TopicName};
use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet, hash_map};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// One queued balanced message. `L` carries extra state released when the
/// subscriber dequeues the message (keyed topics use it for the consumer load
/// of untracked messages).
struct QueuedEnvelope<T, L = ()> {
    envelope: Envelope<T>,
    _admission_permit: OwnedSemaphorePermit,
    _lease: L,
}

struct ConsumerGroup<T> {
    tx: async_channel::Sender<QueuedEnvelope<T>>,
    rx: async_channel::Receiver<QueuedEnvelope<T>>,
    admission: Arc<Semaphore>,
}

struct SingleGroup<T> {
    group_name: SubscriptionGroupName,
    tx: async_channel::Sender<QueuedEnvelope<T>>,
    rx: async_channel::Receiver<QueuedEnvelope<T>>,
    admission: Arc<Semaphore>,
}

pub(crate) enum BroadcastReadResult<T> {
    Ready(Envelope<T>),
    NotReady,
    Lagged { missed: u64, new_read_seq: u64 },
}

struct WakerSet {
    has_waiters: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
}

impl WakerSet {
    fn new() -> Self {
        Self {
            has_waiters: AtomicBool::new(false),
            wakers: Mutex::new(Vec::new()),
        }
    }

    fn register(&self, waker: &Waker) {
        let mut wakers = self.wakers.lock();
        for existing in wakers.iter_mut() {
            if existing.will_wake(waker) {
                existing.clone_from(waker);
                self.has_waiters.store(true, Ordering::Release);
                return;
            }
        }
        wakers.push(waker.clone());
        self.has_waiters.store(true, Ordering::Release);
    }

    fn wake_all(&self) {
        if !self.has_waiters.load(Ordering::Acquire) {
            return;
        }
        let wakers = {
            let mut guard = self.wakers.lock();
            self.has_waiters.store(false, Ordering::Release);
            std::mem::take(&mut *guard)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

fn send_queued_envelope<T, L>(
    sender: &async_channel::Sender<QueuedEnvelope<T, L>>,
    envelope: Envelope<T>,
    permit: OwnedSemaphorePermit,
    lease: L,
) -> Result<(), Error> {
    match sender.try_send(QueuedEnvelope {
        envelope,
        _admission_permit: permit,
        _lease: lease,
    }) {
        Ok(()) => Ok(()),
        Err(async_channel::TrySendError::Full(_)) => {
            panic!("reserved topic admission permit should guarantee queue capacity")
        }
        Err(async_channel::TrySendError::Closed(_)) => Err(TopicClosed),
    }
}

async fn acquire_balanced_permit(
    admission: &Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit, Error> {
    admission
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| TopicClosed)
}

fn try_acquire_balanced_permit(
    admission: &Arc<Semaphore>,
) -> Result<Option<OwnedSemaphorePermit>, Error> {
    match admission.clone().try_acquire_owned() {
        Ok(permit) => Ok(Some(permit)),
        Err(tokio::sync::TryAcquireError::NoPermits) => Ok(None),
        Err(tokio::sync::TryAcquireError::Closed) => Err(TopicClosed),
    }
}

type BalancedPermitVec = SmallVec<[OwnedSemaphorePermit; 4]>;

pub(crate) struct FastBroadcastRing<T: Send + Sync + 'static> {
    slots: Box<[Mutex<Option<(u64, Envelope<T>)>>]>,
    capacity: usize,
    mask: usize,
    write_seq: AtomicU64,
    waker_set: WakerSet,
    closed: AtomicBool,
}

impl<T: Send + Sync + 'static> FastBroadcastRing<T> {
    pub(super) fn new(capacity: usize) -> Self {
        let cap = capacity.max(2).next_power_of_two();
        let mask = cap - 1;
        debug_assert!(cap.is_power_of_two());
        debug_assert_eq!(mask, cap - 1);
        let mut slots = Vec::with_capacity(cap);
        for _ in 0..cap {
            slots.push(Mutex::new(None));
        }
        Self {
            slots: slots.into_boxed_slice(),
            capacity: cap,
            mask,
            write_seq: AtomicU64::new(0),
            waker_set: WakerSet::new(),
            closed: AtomicBool::new(false),
        }
    }

    /// Publish an envelope: reserve the next sequence, then write its slot.
    ///
    /// This is the default (`First`-mode) path and is equivalent to calling
    /// [`FastBroadcastRing::reserve_seq`] immediately followed by
    /// [`FastBroadcastRing::commit_slot`]. The two are split so `all`-mode
    /// broadcast publish can reserve the sequence under the subscriber-registry
    /// lock (to snapshot the exact consensus membership for that sequence) and then
    /// perform the heavier slot write plus waker fan-out after releasing the
    /// lock. See `reserve_seq`/`commit_slot` for the reserved-but-uncommitted
    /// window, which is read-safe (readers see `NotReady` and re-park).
    pub(crate) fn publish(&self, envelope: Envelope<T>) {
        let seq = self.reserve_seq();
        let _ = self.commit_slot(seq, envelope);
    }

    /// Reserve the next ring sequence without writing a slot.
    ///
    /// Paired with [`FastBroadcastRing::commit_slot`]. Splitting the reservation
    /// from the slot write lets `all`-mode broadcast publish reserve the
    /// sequence under the subscriber-registry lock and perform the (more
    /// expensive) slot write plus waker fan-out after releasing that lock.
    ///
    /// Between a reserve and its matching commit, `write_seq` is advanced but the
    /// slot is not yet written; [`FastBroadcastRing::try_read`] returns
    /// [`BroadcastReadResult::NotReady`] for that window and the reader re-parks
    /// on the waker, which fires from `commit_slot`.
    pub(crate) fn reserve_seq(&self) -> u64 {
        self.write_seq.fetch_add(1, Ordering::Release) + 1
    }

    /// Write the envelope into the slot reserved by [`FastBroadcastRing::reserve_seq`]
    /// and wake parked readers.
    ///
    /// A delayed publisher can reach this point after enough later reservations
    /// have wrapped onto the same slot. In that case, keep the newer sequence:
    /// the delayed message has already fallen outside the ring's retained range.
    #[must_use]
    pub(crate) fn commit_slot(&self, seq: u64, envelope: Envelope<T>) -> bool {
        let idx = ((seq - 1) as usize) & self.mask;
        let mut slot = self.slots[idx].lock();
        let can_commit = match &*slot {
            Some((slot_seq, _)) => *slot_seq < seq,
            None => true,
        };
        if can_commit {
            *slot = Some((seq, envelope));
        }
        drop(slot);
        self.waker_set.wake_all();
        can_commit
    }

    pub(crate) fn try_read(&self, read_seq: u64) -> BroadcastReadResult<T> {
        debug_assert!(read_seq > 0);
        let current_write = self.write_seq.load(Ordering::Acquire);
        if read_seq > current_write {
            return BroadcastReadResult::NotReady;
        }

        if current_write >= self.capacity as u64 && read_seq <= current_write - self.capacity as u64
        {
            let new_read_seq = current_write - self.capacity as u64 + 1;
            let missed = new_read_seq - read_seq;
            return BroadcastReadResult::Lagged {
                missed,
                new_read_seq,
            };
        }

        let idx = ((read_seq - 1) as usize) & self.mask;
        let slot = self.slots[idx].lock();
        match &*slot {
            Some((slot_seq, envelope)) if *slot_seq == read_seq => {
                BroadcastReadResult::Ready(envelope.clone())
            }
            Some((slot_seq, _)) if *slot_seq > read_seq => {
                let now = self.write_seq.load(Ordering::Acquire);
                if now >= self.capacity as u64 && read_seq <= now - self.capacity as u64 {
                    let new_read_seq = now - self.capacity as u64 + 1;
                    let missed = new_read_seq - read_seq;
                    BroadcastReadResult::Lagged {
                        missed,
                        new_read_seq,
                    }
                } else {
                    BroadcastReadResult::NotReady
                }
            }
            None => BroadcastReadResult::NotReady,
            Some((_slot_seq, _envelope)) => BroadcastReadResult::NotReady,
        }
    }

    pub(crate) fn current_seq(&self) -> u64 {
        self.write_seq.load(Ordering::Acquire)
    }

    pub(crate) fn register_waker(&self, waker: &Waker) {
        self.waker_set.register(waker);
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.waker_set.wake_all();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

pub(crate) enum TopicInner<T: Send + Sync + 'static> {
    BalancedOnly(BalancedOnlyTopic<T>),
    BroadcastOnly(BroadcastOnlyTopic<T>),
    KeyedBalanced(KeyedBalancedTopic<T>),
    Mixed(MixedTopic<T>),
}

impl<T: Send + Sync + 'static> TopicInner<T> {
    pub(crate) fn new(name: TopicName, opts: TopicOptions) -> Self {
        match opts {
            TopicOptions::BalancedOnly { capacity } => {
                TopicInner::BalancedOnly(BalancedOnlyTopic::new(name, capacity))
            }
            // `all` (consensus) resolution is honored only for broadcast-only
            // topics. `first` keeps the original lock-free behavior.
            TopicOptions::BroadcastOnly {
                capacity,
                on_lag,
                ack_mode,
            } => {
                TopicInner::BroadcastOnly(BroadcastOnlyTopic::new(name, capacity, on_lag, ack_mode))
            }
            TopicOptions::KeyedBalanced {
                capacity,
                load_factor_percent,
            } => TopicInner::KeyedBalanced(KeyedBalancedTopic::new(
                name,
                capacity,
                load_factor_percent,
            )),
            // `ack_mode` is intentionally ignored for Mixed: PR3 rejects `all` on
            // any non-broadcast-only topic, so Mixed is always `first`.
            TopicOptions::Mixed {
                balanced_capacity,
                broadcast_capacity,
                on_lag,
                ack_mode: _,
            } => TopicInner::Mixed(MixedTopic::new(
                name,
                balanced_capacity,
                broadcast_capacity,
                on_lag,
            )),
        }
    }
}

impl<T: Send + Sync + 'static> TopicState<T> for TopicInner<T> {
    fn name(&self) -> &TopicName {
        match self {
            TopicInner::BalancedOnly(topic) => &topic.name,
            TopicInner::BroadcastOnly(topic) => &topic.name,
            TopicInner::KeyedBalanced(topic) => &topic.name,
            TopicInner::Mixed(topic) => &topic.name,
        }
    }

    fn publish(&self, msg: Arc<T>) -> PublishFuture<'_> {
        Box::pin(async move {
            match self {
                TopicInner::BalancedOnly(topic) => {
                    let _id = topic.publish(msg).await?;
                    Ok(())
                }
                TopicInner::BroadcastOnly(topic) => {
                    let _id = topic.publish(msg)?;
                    Ok(())
                }
                TopicInner::KeyedBalanced(topic) => {
                    topic.publish(msg, (), None).await?;
                    Ok(())
                }
                TopicInner::Mixed(topic) => {
                    let _id = topic.publish(msg).await?;
                    Ok(())
                }
            }
        })
    }

    fn publish_tracked(
        &self,
        msg: Arc<T>,
        timeout: Duration,
        permit: TrackedPublishPermit,
    ) -> PublishTrackedFuture<'_> {
        self.publish_tracked_keyed(msg, timeout, permit, None)
    }

    fn publish_tracked_keyed(
        &self,
        msg: Arc<T>,
        timeout: Duration,
        permit: TrackedPublishPermit,
        key: Option<RoutingKey>,
    ) -> PublishTrackedFuture<'_> {
        Box::pin(async move {
            match self {
                TopicInner::BalancedOnly(topic) => {
                    topic.publish_tracked(msg, timeout, permit).await
                }
                TopicInner::BroadcastOnly(topic) => topic.publish_tracked(msg, timeout, permit),
                TopicInner::KeyedBalanced(topic) => {
                    topic.publish(msg, (timeout, permit), key).await
                }
                TopicInner::Mixed(topic) => topic.publish_tracked(msg, timeout, permit).await,
            }
        })
    }

    fn try_publish(&self, msg: Arc<T>) -> Result<PublishOutcome, Error> {
        match self {
            TopicInner::BalancedOnly(topic) => topic.try_publish(msg).map(|(outcome, _)| outcome),
            TopicInner::BroadcastOnly(topic) => topic.try_publish(msg).map(|(outcome, _)| outcome),
            TopicInner::KeyedBalanced(topic) => {
                topic
                    .try_publish(msg, (), None)
                    .map(|outcome| match outcome {
                        KeyedTryPublish::Published(_) => PublishOutcome::Published,
                        KeyedTryPublish::DroppedOnFull => PublishOutcome::DroppedOnFull,
                    })
            }
            TopicInner::Mixed(topic) => topic.try_publish(msg).map(|(outcome, _)| outcome),
        }
    }

    fn try_publish_tracked(
        &self,
        msg: Arc<T>,
        timeout: Duration,
        permit: TrackedPublishPermit,
    ) -> Result<TrackedTryPublishOutcome, Error> {
        self.try_publish_tracked_keyed(msg, timeout, permit, None)
    }

    fn try_publish_tracked_keyed(
        &self,
        msg: Arc<T>,
        timeout: Duration,
        permit: TrackedPublishPermit,
        key: Option<RoutingKey>,
    ) -> Result<TrackedTryPublishOutcome, Error> {
        match self {
            TopicInner::BalancedOnly(topic) => topic.try_publish_tracked(msg, timeout, permit),
            TopicInner::BroadcastOnly(topic) => topic.try_publish_tracked(msg, timeout, permit),
            TopicInner::KeyedBalanced(topic) => {
                topic
                    .try_publish(msg, (timeout, permit), key)
                    .map(|outcome| match outcome {
                        KeyedTryPublish::Published(receipt) => {
                            TrackedTryPublishOutcome::Published(receipt)
                        }
                        KeyedTryPublish::DroppedOnFull => TrackedTryPublishOutcome::DroppedOnFull,
                    })
            }
            TopicInner::Mixed(topic) => topic.try_publish_tracked(msg, timeout, permit),
        }
    }

    fn subscribe_balanced(
        &self,
        group: SubscriptionGroupName,
        opts: SubscriberOptions,
    ) -> Result<Box<dyn SubscriptionBackend<T>>, Error> {
        match self {
            TopicInner::BalancedOnly(topic) => topic
                .subscribe_balanced(group, opts)
                .map(|sub| Box::new(sub) as Box<dyn SubscriptionBackend<T>>),
            TopicInner::BroadcastOnly(_) => Err(SubscribeBalancedNotSupported),
            TopicInner::KeyedBalanced(topic) => topic
                .subscribe_balanced(group, opts)
                .map(|sub| Box::new(sub) as Box<dyn SubscriptionBackend<T>>),
            TopicInner::Mixed(topic) => topic
                .subscribe_balanced(group, opts)
                .map(|sub| Box::new(sub) as Box<dyn SubscriptionBackend<T>>),
        }
    }

    fn subscribe_broadcast(
        &self,
        opts: SubscriberOptions,
    ) -> Result<Box<dyn SubscriptionBackend<T>>, Error> {
        match self {
            TopicInner::BalancedOnly(_) | TopicInner::KeyedBalanced(_) => {
                Err(SubscribeBroadcastNotSupported)
            }
            TopicInner::BroadcastOnly(topic) => {
                Ok(Box::new(topic.subscribe_broadcast(opts)) as Box<dyn SubscriptionBackend<T>>)
            }
            TopicInner::Mixed(topic) => {
                Ok(Box::new(topic.subscribe_broadcast(opts)) as Box<dyn SubscriptionBackend<T>>)
            }
        }
    }

    fn broadcast_on_lag_policy(&self) -> TopicBroadcastOnLagPolicy {
        match self {
            TopicInner::BalancedOnly(_) | TopicInner::KeyedBalanced(_) => {
                TopicBroadcastOnLagPolicy::DropOldest
            }
            TopicInner::BroadcastOnly(topic) => topic.broadcast_on_lag,
            TopicInner::Mixed(topic) => topic.broadcast_on_lag,
        }
    }

    fn close(&self) {
        match self {
            TopicInner::BalancedOnly(topic) => topic.close(),
            TopicInner::BroadcastOnly(topic) => topic.close(),
            TopicInner::KeyedBalanced(topic) => topic.close(),
            TopicInner::Mixed(topic) => topic.close(),
        }
    }

    fn keyed_routing(&self) -> bool {
        matches!(self, TopicInner::KeyedBalanced(_))
    }

    #[cfg(test)]
    fn debug_balanced_available_permits(&self) -> Vec<(SubscriptionGroupName, usize)> {
        match self {
            TopicInner::BalancedOnly(topic) => topic
                .group
                .get()
                .map(|group| {
                    vec![(
                        group.group_name.clone(),
                        group.admission.available_permits(),
                    )]
                })
                .unwrap_or_default(),
            TopicInner::BroadcastOnly(_) | TopicInner::KeyedBalanced(_) => Vec::new(),
            TopicInner::Mixed(topic) => topic.balanced_available_permits(),
        }
    }

    #[cfg(test)]
    fn debug_keyed_state(&self) -> Option<(usize, Vec<usize>)> {
        match self {
            TopicInner::KeyedBalanced(topic) => Some(topic.debug_state()),
            _ => None,
        }
    }
}

pub(crate) struct BalancedOnlyTopic<T: Send + Sync + 'static> {
    name: TopicName,
    next_id: AtomicU64,
    group: OnceLock<SingleGroup<T>>,
    balanced_capacity: usize,
    outcomes: TrackedPublishTracker,
    closed: AtomicBool,
}

impl<T: Send + Sync + 'static> BalancedOnlyTopic<T> {
    fn new(name: TopicName, balanced_capacity: usize) -> Self {
        Self {
            name,
            next_id: AtomicU64::new(1),
            group: OnceLock::new(),
            balanced_capacity: balanced_capacity.max(1),
            outcomes: TrackedPublishTracker::new(),
            closed: AtomicBool::new(false),
        }
    }

    #[inline]
    fn next_message_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn publish(&self, msg: Arc<T>) -> Result<u64, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let id = self.next_message_id();
        if let Some(group) = self.group.get() {
            let permit = acquire_balanced_permit(&group.admission).await?;
            let envelope = Envelope {
                id,
                tracked: false,
                payload: msg,
            };
            send_queued_envelope(&group.tx, envelope, permit, ())?;
        }
        Ok(id)
    }

    async fn publish_tracked(
        &self,
        msg: Arc<T>,
        timeout: Duration,
        permit: TrackedPublishPermit,
    ) -> Result<TrackedPublishReceipt, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let id = self.next_message_id();
        if let Some(group) = self.group.get() {
            let admission_permit = acquire_balanced_permit(&group.admission).await?;
            let receipt = self.outcomes.register(id, timeout, permit);
            let envelope = Envelope {
                id,
                tracked: true,
                payload: msg,
            };
            send_queued_envelope(&group.tx, envelope, admission_permit, ())?;
            Ok(receipt)
        } else {
            Ok(self.outcomes.register(id, timeout, permit))
        }
    }

    fn try_publish(&self, msg: Arc<T>) -> Result<(PublishOutcome, u64), Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let id = self.next_message_id();
        if let Some(group) = self.group.get() {
            let Some(permit) = try_acquire_balanced_permit(&group.admission)? else {
                return Ok((PublishOutcome::DroppedOnFull, id));
            };
            let envelope = Envelope {
                id,
                tracked: false,
                payload: msg,
            };
            send_queued_envelope(&group.tx, envelope, permit, ())?;
        }
        Ok((PublishOutcome::Published, id))
    }

    fn try_publish_tracked(
        &self,
        msg: Arc<T>,
        timeout: Duration,
        permit: TrackedPublishPermit,
    ) -> Result<TrackedTryPublishOutcome, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let id = self.next_message_id();
        if let Some(group) = self.group.get() {
            let Some(admission_permit) = try_acquire_balanced_permit(&group.admission)? else {
                return Ok(TrackedTryPublishOutcome::DroppedOnFull);
            };
            let receipt = self.outcomes.register(id, timeout, permit);
            let envelope = Envelope {
                id,
                tracked: true,
                payload: msg,
            };
            send_queued_envelope(&group.tx, envelope, admission_permit, ())?;
            Ok(TrackedTryPublishOutcome::Published(receipt))
        } else {
            Ok(TrackedTryPublishOutcome::Published(
                self.outcomes.register(id, timeout, permit),
            ))
        }
    }

    fn subscribe_balanced(
        &self,
        group: SubscriptionGroupName,
        _opts: SubscriberOptions,
    ) -> Result<BalancedSub<T>, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let single_group = self.group.get_or_init(|| {
            let (tx, rx) = async_channel::bounded(self.balanced_capacity);
            SingleGroup {
                group_name: group.clone(),
                tx,
                rx,
                admission: Arc::new(Semaphore::new(self.balanced_capacity)),
            }
        });

        if single_group.group_name != group {
            return Err(SubscribeSingleGroupViolation);
        }

        Ok(BalancedSub {
            rx: Box::pin(single_group.rx.clone()),
            ack_state: AckState::new(self.outcomes.clone()),
        })
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.outcomes.close_all();
        if let Some(group) = self.group.get() {
            group.admission.close();
            let _ = group.tx.close();
        }
    }
}

const KEY_TABLE_SHARDS: usize = 16;
const CONSUMER_DEPARTED_REASON: &str = "balanced consumer departed";

/// Load counter for one consumer of a keyed balanced topic.
pub(crate) struct ConsumerLoad {
    /// Stable per-consumer seed for the rendezvous hash.
    id: u64,
    /// Messages routed to this consumer that have no terminal outcome yet:
    /// queued messages plus tracked messages that are not yet acked, nacked,
    /// timed out, or closed. Atomic because publishers read it while routing
    /// and leases release it from whichever task resolves the message. It is a
    /// plain counter that guards no other data, so relaxed ordering is enough.
    unresolved: AtomicUsize,
}

impl ConsumerLoad {
    fn load(&self) -> usize {
        self.unresolved.load(Ordering::Relaxed)
    }
}

struct KeyEntry {
    consumer: Arc<ConsumerLoad>,
    in_flight: usize,
}

/// Per-topic routing table: key hash to assigned consumer and the number of
/// the key's messages without a terminal outcome. Keys whose count reaches 0
/// leave the table, so its size is bounded by the number of unresolved keyed
/// messages.
pub(crate) struct KeyTable {
    /// Sharded by key hash so publishers routing different keys rarely
    /// contend. A shard lock is held only for one lookup and update and is
    /// never held while taking another lock.
    shards: [Mutex<HashMap<u64, KeyEntry>>; KEY_TABLE_SHARDS],
}

impl KeyTable {
    fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    fn shard(&self, hash: u64) -> &Mutex<HashMap<u64, KeyEntry>> {
        &self.shards[(hash as usize) % KEY_TABLE_SHARDS]
    }

    fn release(&self, hash: u64) {
        let mut shard = self.shard(hash).lock();
        if let hash_map::Entry::Occupied(mut slot) = shard.entry(hash) {
            let entry = slot.get_mut();
            debug_assert!(entry.in_flight > 0, "routing key in-flight count underflow");
            entry.in_flight = entry.in_flight.saturating_sub(1);
            if entry.in_flight == 0 {
                let _ = slot.remove();
            }
        } else {
            debug_assert!(false, "released a routing key missing from the key table");
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }
}

/// One message's share of a consumer load and, for keyed messages, of the
/// key's in-flight count. Dropping the lease releases both. Tracked messages
/// keep the lease in their [`TrackedPublishPermit`], which the tracker drops
/// exactly once on the terminal outcome. Untracked messages keep it in the
/// queued envelope, which is dropped when the subscriber dequeues it.
pub(crate) struct RoutingLease {
    consumer: Arc<ConsumerLoad>,
    key: Option<(Arc<KeyTable>, u64)>,
}

impl RoutingLease {
    fn new(consumer: Arc<ConsumerLoad>, key: Option<(Arc<KeyTable>, u64)>) -> Self {
        let _ = consumer.unresolved.fetch_add(1, Ordering::Relaxed);
        Self { consumer, key }
    }
}

impl Drop for RoutingLease {
    fn drop(&mut self) {
        if let Some((table, hash)) = self.key.take() {
            table.release(hash);
        }
        let _ = self.consumer.unresolved.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Rendezvous weight of a key for one consumer (splitmix64 finalizer).
fn rendezvous_weight(hash: u64, consumer_id: u64) -> u64 {
    let mut z = hash ^ consumer_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

type KeyedQueuedEnvelope<T> = QueuedEnvelope<T, Option<RoutingLease>>;

struct KeyedConsumer<T: Send + Sync + 'static> {
    load: Arc<ConsumerLoad>,
    tx: async_channel::Sender<KeyedQueuedEnvelope<T>>,
    admission: Arc<Semaphore>,
}

/// Outcome of a non-blocking keyed publish attempt.
enum KeyedTryPublish<R> {
    Published(R),
    DroppedOnFull,
}

/// Per-message tracking for keyed publish: `()` for untracked messages and
/// `(timeout, permit)` for tracked messages.
trait KeyedTracking: Send {
    type Receipt;

    const TRACKED: bool;

    /// Handle a publish when no consumer is subscribed.
    fn without_consumer(self, outcomes: &TrackedPublishTracker, id: u64) -> Self::Receipt;

    /// Enqueue one routed message and attach its lease where its terminal
    /// outcome releases it.
    fn enqueue<T: Send + Sync + 'static>(
        self,
        outcomes: &TrackedPublishTracker,
        consumer: &KeyedConsumer<T>,
        envelope: Envelope<T>,
        lease: RoutingLease,
        admission: OwnedSemaphorePermit,
    ) -> Result<Self::Receipt, Error>;
}

impl KeyedTracking for () {
    type Receipt = ();

    const TRACKED: bool = false;

    fn without_consumer(self, _outcomes: &TrackedPublishTracker, _id: u64) {}

    fn enqueue<T: Send + Sync + 'static>(
        self,
        _outcomes: &TrackedPublishTracker,
        consumer: &KeyedConsumer<T>,
        envelope: Envelope<T>,
        lease: RoutingLease,
        admission: OwnedSemaphorePermit,
    ) -> Result<(), Error> {
        // Untracked messages have no terminal outcome to observe, so the
        // lease is released when the subscriber dequeues the message.
        send_queued_envelope(&consumer.tx, envelope, admission, Some(lease))
    }
}

impl KeyedTracking for (Duration, TrackedPublishPermit) {
    type Receipt = TrackedPublishReceipt;

    const TRACKED: bool = true;

    fn without_consumer(self, outcomes: &TrackedPublishTracker, id: u64) -> TrackedPublishReceipt {
        let (timeout, permit) = self;
        outcomes.register(id, timeout, permit)
    }

    fn enqueue<T: Send + Sync + 'static>(
        self,
        outcomes: &TrackedPublishTracker,
        consumer: &KeyedConsumer<T>,
        envelope: Envelope<T>,
        lease: RoutingLease,
        admission: OwnedSemaphorePermit,
    ) -> Result<TrackedPublishReceipt, Error> {
        let (timeout, mut permit) = self;
        permit.attach_routing_lease(lease);
        let receipt = outcomes.register(envelope.id, timeout, permit);
        send_queued_envelope(&consumer.tx, envelope, admission, None)?;
        Ok(receipt)
    }
}

/// State shared by a keyed balanced topic and its subscriptions.
struct KeyedGroupState<T: Send + Sync + 'static> {
    group_name: OnceLock<SubscriptionGroupName>,
    /// Live consumers. Publishers route and enqueue under the read lock.
    /// Subscribe and departure take the write lock, so a consumer cannot close
    /// its queue while a publisher is enqueueing to it, and a consumer that is
    /// not in this list never receives new messages.
    consumers: RwLock<Vec<Arc<KeyedConsumer<T>>>>,
    next_consumer_id: AtomicU64,
    keys: Arc<KeyTable>,
    outcomes: TrackedPublishTracker,
    closed: AtomicBool,
    capacity: usize,
    load_factor_percent: u32,
}

impl<T: Send + Sync + 'static> KeyedGroupState<T> {
    /// Choose the consumer for one message and take its lease. The caller
    /// holds the consumer list read lock. Returns `None` when no consumer is
    /// subscribed.
    fn route(
        &self,
        consumers: &[Arc<KeyedConsumer<T>>],
        key: Option<RoutingKey>,
    ) -> Option<(Arc<KeyedConsumer<T>>, RoutingLease)> {
        if consumers.is_empty() {
            return None;
        }
        let Some(key) = key else {
            let index = consumers
                .iter()
                .enumerate()
                .min_by_key(|(_, consumer)| consumer.load.load())
                .map_or(0, |(index, _)| index);
            let consumer = Arc::clone(&consumers[index]);
            let lease = RoutingLease::new(Arc::clone(&consumer.load), None);
            return Some((consumer, lease));
        };

        let hash = key.hash_value();
        let index = {
            let mut shard = self.keys.shard(hash).lock();
            match shard.entry(hash) {
                hash_map::Entry::Occupied(mut slot) => {
                    let entry = slot.get_mut();
                    // A key whose consumer departed is re-placed. Its count
                    // carries over so it still reaches 0 exactly once.
                    let index = consumers
                        .iter()
                        .position(|consumer| Arc::ptr_eq(&consumer.load, &entry.consumer))
                        .unwrap_or_else(|| {
                            let index = self.place_new_key(consumers, hash);
                            entry.consumer = Arc::clone(&consumers[index].load);
                            index
                        });
                    entry.in_flight += 1;
                    index
                }
                hash_map::Entry::Vacant(slot) => {
                    let index = self.place_new_key(consumers, hash);
                    let _ = slot.insert(KeyEntry {
                        consumer: Arc::clone(&consumers[index].load),
                        in_flight: 1,
                    });
                    index
                }
            }
        };
        let consumer = Arc::clone(&consumers[index]);
        let lease = RoutingLease::new(
            Arc::clone(&consumer.load),
            Some((Arc::clone(&self.keys), hash)),
        );
        Some((consumer, lease))
    }

    /// Bounded-load rendezvous placement. Among consumers whose load is below
    /// `ceil(load_factor_percent / 100 * average load)` (counting the new
    /// message), pick the one with the highest rendezvous weight for the key.
    /// The least loaded consumer is always below the cap.
    fn place_new_key(&self, consumers: &[Arc<KeyedConsumer<T>>], hash: u64) -> usize {
        let loads: SmallVec<[usize; 16]> = consumers
            .iter()
            .map(|consumer| consumer.load.load())
            .collect();
        let total = loads.iter().map(|&load| load as u128).sum::<u128>() + 1;
        let percent = u128::from(self.load_factor_percent.max(100));
        let cap = (percent * total).div_ceil(100 * consumers.len() as u128);

        let mut least = 0;
        let mut best: Option<(usize, u64)> = None;
        for (index, consumer) in consumers.iter().enumerate() {
            if loads[index] < loads[least] {
                least = index;
            }
            if (loads[index] as u128) < cap {
                let weight = rendezvous_weight(hash, consumer.load.id);
                if best.is_none_or(|(_, best_weight)| weight > best_weight) {
                    best = Some((index, weight));
                }
            }
        }
        best.map_or(least, |(index, _)| index)
    }
}

pub(crate) struct KeyedBalancedTopic<T: Send + Sync + 'static> {
    name: TopicName,
    next_id: AtomicU64,
    state: Arc<KeyedGroupState<T>>,
}

impl<T: Send + Sync + 'static> KeyedBalancedTopic<T> {
    fn new(name: TopicName, capacity: usize, load_factor_percent: u32) -> Self {
        Self {
            name,
            next_id: AtomicU64::new(1),
            state: Arc::new(KeyedGroupState {
                group_name: OnceLock::new(),
                consumers: RwLock::new(Vec::new()),
                next_consumer_id: AtomicU64::new(1),
                keys: Arc::new(KeyTable::new()),
                outcomes: TrackedPublishTracker::new(),
                closed: AtomicBool::new(false),
                capacity: capacity.max(1),
                load_factor_percent,
            }),
        }
    }

    fn envelope(&self, msg: Arc<T>, tracked: bool) -> Envelope<T> {
        Envelope {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            tracked,
            payload: msg,
        }
    }

    /// Blocking publish shared by the untracked and tracked paths. Waits for
    /// space in the chosen consumer queue. If that consumer departs while the
    /// publisher waits, the message is routed again.
    async fn publish<K: KeyedTracking>(
        &self,
        msg: Arc<T>,
        tracking: K,
        key: Option<RoutingKey>,
    ) -> Result<K::Receipt, Error> {
        let state = &self.state;
        let envelope = self.envelope(msg, K::TRACKED);
        loop {
            if state.closed.load(Ordering::Acquire) {
                return Err(TopicClosed);
            }
            let routed = state.route(&state.consumers.read(), key);
            let Some((consumer, lease)) = routed else {
                return Ok(tracking.without_consumer(&state.outcomes, envelope.id));
            };
            // The lease is held while waiting, so the key stays on this
            // consumer and later messages for it queue behind this one.
            let Ok(admission) = consumer.admission.clone().acquire_owned().await else {
                continue;
            };
            let consumers = state.consumers.read();
            if consumer.admission.is_closed() {
                drop(consumers);
                continue;
            }
            return tracking.enqueue(&state.outcomes, &consumer, envelope, lease, admission);
        }
    }

    fn try_publish<K: KeyedTracking>(
        &self,
        msg: Arc<T>,
        tracking: K,
        key: Option<RoutingKey>,
    ) -> Result<KeyedTryPublish<K::Receipt>, Error> {
        let state = &self.state;
        if state.closed.load(Ordering::Acquire) {
            return Err(TopicClosed);
        }
        let envelope = self.envelope(msg, K::TRACKED);
        let consumers = state.consumers.read();
        let Some((consumer, lease)) = state.route(&consumers, key) else {
            return Ok(KeyedTryPublish::Published(
                tracking.without_consumer(&state.outcomes, envelope.id),
            ));
        };
        let Some(admission) = try_acquire_balanced_permit(&consumer.admission)? else {
            return Ok(KeyedTryPublish::DroppedOnFull);
        };
        tracking
            .enqueue(&state.outcomes, &consumer, envelope, lease, admission)
            .map(KeyedTryPublish::Published)
    }

    fn subscribe_balanced(
        &self,
        group: SubscriptionGroupName,
        _opts: SubscriberOptions,
    ) -> Result<KeyedBalancedSub<T>, Error> {
        let state = &self.state;
        if state.closed.load(Ordering::Acquire) {
            return Err(TopicClosed);
        }
        if *state.group_name.get_or_init(|| group.clone()) != group {
            return Err(SubscribeSingleGroupViolation);
        }

        let (tx, rx) = async_channel::bounded(state.capacity);
        let consumer = Arc::new(KeyedConsumer {
            load: Arc::new(ConsumerLoad {
                id: state.next_consumer_id.fetch_add(1, Ordering::Relaxed),
                unresolved: AtomicUsize::new(0),
            }),
            tx,
            admission: Arc::new(Semaphore::new(state.capacity)),
        });
        {
            let mut consumers = state.consumers.write();
            // Re-checked under the write lock: close sets the flag before it
            // walks the list, so either close sees this consumer or this
            // check sees the flag.
            if state.closed.load(Ordering::Acquire) {
                return Err(TopicClosed);
            }
            consumers.push(Arc::clone(&consumer));
        }
        Ok(KeyedBalancedSub {
            inner: BalancedSub {
                rx: Box::pin(rx),
                ack_state: AckState::new(state.outcomes.clone()),
            },
            state: Arc::clone(state),
            consumer,
        })
    }

    fn close(&self) {
        let state = &self.state;
        state.closed.store(true, Ordering::Release);
        state.outcomes.close_all();
        for consumer in state.consumers.read().iter() {
            consumer.admission.close();
            let _ = consumer.tx.close();
        }
    }

    #[cfg(test)]
    fn debug_state(&self) -> (usize, Vec<usize>) {
        let loads = self
            .state
            .consumers
            .read()
            .iter()
            .map(|consumer| consumer.load.load())
            .collect();
        (self.state.keys.len(), loads)
    }
}

pub(crate) struct KeyedBalancedSub<T: Send + Sync + 'static> {
    inner: BalancedSub<T, Option<RoutingLease>>,
    state: Arc<KeyedGroupState<T>>,
    consumer: Arc<KeyedConsumer<T>>,
}

impl<T: Send + Sync + 'static> SubscriptionBackend<T> for KeyedBalancedSub<T> {
    fn poll_recv_delivery(&mut self, cx: &mut Context<'_>) -> Poll<Result<RecvDelivery<T>, Error>> {
        self.inner.poll_recv_delivery(cx)
    }

    fn ack(&self, id: u64) -> Result<(), Error> {
        self.inner.ack(id)
    }

    fn nack(&self, id: u64, reason: Arc<str>) -> Result<(), Error> {
        self.inner.nack(id, reason)
    }
}

impl<T: Send + Sync + 'static> Drop for KeyedBalancedSub<T> {
    /// Consumer departure: stop routing to this consumer, then nack every
    /// tracked message still in its queue. Each nack drops the message lease,
    /// which releases its key. Keys whose messages this consumer already
    /// received stay counted until those messages reach a terminal outcome;
    /// the next message for such a key is placed on a live consumer.
    fn drop(&mut self) {
        {
            let mut consumers = self.state.consumers.write();
            consumers.retain(|consumer| !Arc::ptr_eq(consumer, &self.consumer));
            self.consumer.admission.close();
            let _ = self.consumer.tx.close();
        }
        let reason: Arc<str> = Arc::from(CONSUMER_DEPARTED_REASON);
        while let Ok(queued) = self.inner.rx.try_recv() {
            if queued.envelope.tracked {
                let _ = self.state.outcomes.resolve(
                    queued.envelope.id,
                    TrackedPublishOutcome::Nack {
                        reason: Arc::clone(&reason),
                    },
                );
            }
        }
    }
}

/// Registry of live broadcast subscribers for a single `all`-mode topic.
///
/// Constructed only when `ack_mode == All`. It is the single linearization
/// point for consensus membership: subscribe, tracked publish, and disconnect
/// all take its lock so a subscriber is either in a message's snapshot AND
/// started at or before that message's ring sequence, or excluded AND started
/// strictly after it. Lock order is always **registry -> tracker**; tracker code
/// must never call back into the registry.
struct BroadcastSubscriberRegistry {
    next_subscriber_id: AtomicU64,
    subscribers: Mutex<Arc<HashSet<BroadcastSubscriberId>>>,
}

impl BroadcastSubscriberRegistry {
    fn new() -> Self {
        Self {
            next_subscriber_id: AtomicU64::new(1),
            subscribers: Mutex::new(Arc::new(HashSet::new())),
        }
    }

    fn allocate_id(&self) -> BroadcastSubscriberId {
        BroadcastSubscriberId(self.next_subscriber_id.fetch_add(1, Ordering::Relaxed))
    }
}

pub(crate) struct BroadcastOnlyTopic<T: Send + Sync + 'static> {
    name: TopicName,
    next_id: AtomicU64,
    broadcast_ring: Arc<FastBroadcastRing<T>>,
    broadcast_on_lag: TopicBroadcastOnLagPolicy,
    outcomes: TrackedPublishTracker,
    closed: AtomicBool,
    ack_mode: TopicBroadcastAckMode,
    /// Subscriber registry, present only in `all` (consensus) mode. `first`
    /// mode never constructs or touches it, so it stays zero-overhead.
    registry: Option<Arc<BroadcastSubscriberRegistry>>,
}

impl<T: Send + Sync + 'static> BroadcastOnlyTopic<T> {
    fn new(
        name: TopicName,
        broadcast_capacity: usize,
        broadcast_on_lag: TopicBroadcastOnLagPolicy,
        ack_mode: TopicBroadcastAckMode,
    ) -> Self {
        let registry = match ack_mode {
            TopicBroadcastAckMode::All => Some(Arc::new(BroadcastSubscriberRegistry::new())),
            TopicBroadcastAckMode::First => None,
        };
        Self {
            name,
            next_id: AtomicU64::new(1),
            broadcast_ring: Arc::new(FastBroadcastRing::new(broadcast_capacity)),
            broadcast_on_lag,
            outcomes: TrackedPublishTracker::new(),
            closed: AtomicBool::new(false),
            ack_mode,
            registry,
        }
    }

    #[inline]
    fn next_message_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn publish(&self, msg: Arc<T>) -> Result<u64, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let id = self.next_message_id();
        self.broadcast_ring.publish(Envelope {
            id,
            tracked: false,
            payload: msg,
        });
        Ok(id)
    }

    fn publish_tracked(
        &self,
        msg: Arc<T>,
        timeout: Duration,
        permit: TrackedPublishPermit,
    ) -> Result<TrackedPublishReceipt, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let id = self.next_message_id();

        // `all` mode: snapshot the eligible subscriber set and reserve the ring
        // sequence atomically under the registry lock so the consensus
        // membership exactly matches who will receive this message. Membership
        // is stored copy-on-write behind an `Arc`, so the snapshot is an O(1)
        // refcount bump rather than a deep copy of the whole set. The heavier
        // slot write plus waker fan-out happen after releasing the lock.
        //
        // Lock order is registry -> tracker (`register_consensus` locks the
        // tracker); the tracker must never call back into the registry.
        if let Some(registry) = &self.registry {
            let subscribers = registry.subscribers.lock();
            let snapshot = Arc::clone(&subscribers);
            let seq = self.broadcast_ring.reserve_seq();
            let receipt = self
                .outcomes
                .register_consensus(id, timeout, permit, snapshot, seq);
            drop(subscribers);
            let committed = self.broadcast_ring.commit_slot(
                seq,
                Envelope {
                    id,
                    tracked: true,
                    payload: msg,
                },
            );
            if !committed {
                let _ = self.outcomes.resolve(
                    id,
                    TrackedPublishOutcome::Nack {
                        reason: Arc::from("broadcast message was overtaken before ring commit"),
                    },
                );
            }
            return Ok(receipt);
        }

        // `first` mode: unchanged single-call path, no registry involvement.
        let receipt = self.outcomes.register(id, timeout, permit);
        self.broadcast_ring.publish(Envelope {
            id,
            tracked: true,
            payload: msg,
        });
        Ok(receipt)
    }

    fn try_publish(&self, msg: Arc<T>) -> Result<(PublishOutcome, u64), Error> {
        let id = self.publish(msg)?;
        Ok((PublishOutcome::Published, id))
    }

    fn try_publish_tracked(
        &self,
        msg: Arc<T>,
        timeout: Duration,
        permit: TrackedPublishPermit,
    ) -> Result<TrackedTryPublishOutcome, Error> {
        let receipt = self.publish_tracked(msg, timeout, permit)?;
        Ok(TrackedTryPublishOutcome::Published(receipt))
    }

    fn subscribe_broadcast(&self, _opts: SubscriberOptions) -> BroadcastSub<T> {
        // `all` mode: allocate this subscriber's id and couple its `start_seq`
        // with insertion into the registry under one lock (the linearization
        // point). `first` mode stays lock-free.
        let (start_seq, subscriber_id) = match &self.registry {
            Some(registry) => {
                let mut subscribers = registry.subscribers.lock();
                let start_seq = self.broadcast_ring.current_seq() + 1;
                let subscriber_id = registry.allocate_id();
                let _ = Arc::make_mut(&mut subscribers).insert(subscriber_id);
                (start_seq, Some(subscriber_id))
            }
            None => (self.broadcast_ring.current_seq() + 1, None),
        };
        BroadcastSub {
            ring: Arc::clone(&self.broadcast_ring),
            cursor: Arc::new(Mutex::new(BroadcastCursor {
                read_seq: start_seq,
                permitted_seq: None,
                disconnected_on_lag: false,
                spun: false,
                permit_waiters: WakerSet::new(),
            })),
            on_lag: self.broadcast_on_lag,
            ack_state: AckState::broadcast(self.outcomes.clone(), self.ack_mode, subscriber_id),
            registry: self.registry.clone(),
            subscriber_id,
            disconnect_done: AtomicBool::new(false),
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.outcomes.close_all();
        self.broadcast_ring.close();
    }
}

pub(crate) struct MixedTopic<T: Send + Sync + 'static> {
    name: TopicName,
    next_id: AtomicU64,
    groups: RwLock<Vec<(SubscriptionGroupName, Arc<ConsumerGroup<T>>)>>,
    group_handles: RwLock<Arc<[Arc<ConsumerGroup<T>>]>>,
    has_balanced_groups: AtomicBool,
    balanced_capacity: usize,
    broadcast_ring: Arc<FastBroadcastRing<T>>,
    broadcast_on_lag: TopicBroadcastOnLagPolicy,
    outcomes: TrackedPublishTracker,
    closed: AtomicBool,
}

impl<T: Send + Sync + 'static> MixedTopic<T> {
    fn new(
        name: TopicName,
        balanced_capacity: usize,
        broadcast_capacity: usize,
        broadcast_on_lag: TopicBroadcastOnLagPolicy,
    ) -> Self {
        Self {
            name,
            next_id: AtomicU64::new(1),
            groups: RwLock::new(Vec::new()),
            group_handles: RwLock::new(Arc::from(Vec::new())),
            has_balanced_groups: AtomicBool::new(false),
            balanced_capacity: balanced_capacity.max(1),
            broadcast_ring: Arc::new(FastBroadcastRing::new(broadcast_capacity)),
            broadcast_on_lag,
            outcomes: TrackedPublishTracker::new(),
            closed: AtomicBool::new(false),
        }
    }

    #[inline]
    fn next_message_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    // Acquire balanced-group capacity atomically from the publisher's point of
    // view. Mixed topics are intentionally all-or-nothing across balanced and
    // broadcast delivery, so publishers must not keep permits reserved in
    // "fast" groups while they wait on a "slow" one. Drop any partial
    // acquisitions before awaiting capacity and retry from a fresh snapshot.
    async fn acquire_balanced_admission(
        &self,
    ) -> Result<(Arc<[Arc<ConsumerGroup<T>>]>, BalancedPermitVec), Error> {
        loop {
            let groups = self.group_handles.read().clone();
            let (permits, blocking_group) = Self::try_acquire_balanced_admission(&groups)?;
            if let Some(group) = blocking_group {
                drop(permits);
                let permit = acquire_balanced_permit(&group.admission).await?;
                drop(permit);
            } else {
                return Ok((groups, permits));
            }
        }
    }

    fn try_acquire_balanced_admission(
        groups: &Arc<[Arc<ConsumerGroup<T>>]>,
    ) -> Result<(BalancedPermitVec, Option<Arc<ConsumerGroup<T>>>), Error> {
        let mut permits = BalancedPermitVec::with_capacity(groups.len());
        for group in groups.as_ref() {
            match try_acquire_balanced_permit(&group.admission)? {
                Some(permit) => permits.push(permit),
                None => return Ok((permits, Some(Arc::clone(group)))),
            }
        }
        Ok((permits, None))
    }

    #[cfg(test)]
    pub(crate) fn balanced_available_permits(&self) -> Vec<(SubscriptionGroupName, usize)> {
        self.groups
            .read()
            .iter()
            .map(|(name, group)| (name.clone(), group.admission.available_permits()))
            .collect()
    }

    async fn publish(&self, msg: Arc<T>) -> Result<u64, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let id = self.next_message_id();
        if self.has_balanced_groups.load(Ordering::Acquire) {
            let (groups, permits) = self.acquire_balanced_admission().await?;
            let envelope = Envelope {
                id,
                tracked: false,
                payload: Arc::clone(&msg),
            };
            for (group, permit) in groups.as_ref().iter().zip(permits) {
                send_queued_envelope(&group.tx, envelope.clone(), permit, ())?;
            }
        }

        // Broadcast is intentionally last so mixed async publish has the same
        // visible contract as mixed try_publish: no broadcast subscriber can
        // observe a message before the balanced side has admitted it.
        self.broadcast_ring.publish(Envelope {
            id,
            tracked: false,
            payload: Arc::clone(&msg),
        });

        Ok(id)
    }

    async fn publish_tracked(
        &self,
        msg: Arc<T>,
        timeout: Duration,
        permit: TrackedPublishPermit,
    ) -> Result<TrackedPublishReceipt, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let id = self.next_message_id();
        if self.has_balanced_groups.load(Ordering::Acquire) {
            let (groups, permits) = self.acquire_balanced_admission().await?;
            // Tracked publish capacity is consumed only after admission
            // succeeds. Waiting on a full balanced group must not hand back a
            // receipt for a message that has not been accepted anywhere yet.
            let receipt = self.outcomes.register(id, timeout, permit);
            let envelope = Envelope {
                id,
                tracked: true,
                payload: Arc::clone(&msg),
            };
            for (group, admission_permit) in groups.as_ref().iter().zip(permits) {
                send_queued_envelope(&group.tx, envelope.clone(), admission_permit, ())?;
            }
            self.broadcast_ring.publish(Envelope {
                id,
                tracked: true,
                payload: Arc::clone(&msg),
            });
            Ok(receipt)
        } else {
            let receipt = self.outcomes.register(id, timeout, permit);
            self.broadcast_ring.publish(Envelope {
                id,
                tracked: true,
                payload: Arc::clone(&msg),
            });
            Ok(receipt)
        }
    }

    fn try_publish(&self, msg: Arc<T>) -> Result<(PublishOutcome, u64), Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let id = self.next_message_id();
        if !self.has_balanced_groups.load(Ordering::Acquire) {
            self.broadcast_ring.publish(Envelope {
                id,
                tracked: false,
                payload: Arc::clone(&msg),
            });
            return Ok((PublishOutcome::Published, id));
        }

        let groups = self.group_handles.read().clone();
        let envelope = Envelope {
            id,
            tracked: false,
            payload: Arc::clone(&msg),
        };
        let (permits, blocking_group) = Self::try_acquire_balanced_admission(&groups)?;
        if blocking_group.is_some() {
            // Keep try_publish all-or-nothing for mixed topics: if balanced
            // admission fails, nothing is published to broadcast either.
            Ok((PublishOutcome::DroppedOnFull, id))
        } else {
            for (group, permit) in groups.as_ref().iter().zip(permits) {
                send_queued_envelope(&group.tx, envelope.clone(), permit, ())?;
            }
            self.broadcast_ring.publish(Envelope {
                id,
                tracked: false,
                payload: Arc::clone(&msg),
            });
            Ok((PublishOutcome::Published, id))
        }
    }

    fn try_publish_tracked(
        &self,
        msg: Arc<T>,
        timeout: Duration,
        permit: TrackedPublishPermit,
    ) -> Result<TrackedTryPublishOutcome, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let id = self.next_message_id();
        if self.has_balanced_groups.load(Ordering::Acquire) {
            let groups = self.group_handles.read().clone();
            let (permits, blocking_group) = Self::try_acquire_balanced_admission(&groups)?;
            if blocking_group.is_some() {
                return Ok(TrackedTryPublishOutcome::DroppedOnFull);
            }
            let receipt = self.outcomes.register(id, timeout, permit);
            let envelope = Envelope {
                id,
                tracked: true,
                payload: Arc::clone(&msg),
            };
            for (group, admission_permit) in groups.as_ref().iter().zip(permits) {
                send_queued_envelope(&group.tx, envelope.clone(), admission_permit, ())?;
            }
            self.broadcast_ring.publish(Envelope {
                id,
                tracked: true,
                payload: msg,
            });
            Ok(TrackedTryPublishOutcome::Published(receipt))
        } else {
            let receipt = self.outcomes.register(id, timeout, permit);
            self.broadcast_ring.publish(Envelope {
                id,
                tracked: true,
                payload: msg,
            });
            Ok(TrackedTryPublishOutcome::Published(receipt))
        }
    }

    fn subscribe_balanced(
        &self,
        group: SubscriptionGroupName,
        _opts: SubscriberOptions,
    ) -> Result<BalancedSub<T>, Error> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TopicClosed);
        }

        let rx = {
            let mut groups = self.groups.write();
            if let Some((_name, group_entry)) = groups.iter().find(|(name, _)| *name == group) {
                group_entry.rx.clone()
            } else {
                let (tx, rx) = async_channel::bounded(self.balanced_capacity);
                let group_entry = Arc::new(ConsumerGroup {
                    tx,
                    rx: rx.clone(),
                    admission: Arc::new(Semaphore::new(self.balanced_capacity)),
                });
                groups.push((group.clone(), Arc::clone(&group_entry)));
                let snapshot: Arc<[Arc<ConsumerGroup<T>>]> = groups
                    .iter()
                    .map(|(_, group_entry)| Arc::clone(group_entry))
                    .collect::<Vec<_>>()
                    .into();
                *self.group_handles.write() = snapshot;
                self.has_balanced_groups.store(true, Ordering::Release);
                rx
            }
        };

        Ok(BalancedSub {
            rx: Box::pin(rx),
            ack_state: AckState::new(self.outcomes.clone()),
        })
    }

    fn subscribe_broadcast(&self, _opts: SubscriberOptions) -> BroadcastSub<T> {
        // Mixed topics never run `all` mode (PR3 rejects `all` on non
        // broadcast-only topics), so broadcast subs here are always `first`.
        let start_seq = self.broadcast_ring.current_seq() + 1;
        BroadcastSub {
            ring: Arc::clone(&self.broadcast_ring),
            cursor: Arc::new(Mutex::new(BroadcastCursor {
                read_seq: start_seq,
                permitted_seq: None,
                disconnected_on_lag: false,
                spun: false,
                permit_waiters: WakerSet::new(),
            })),
            on_lag: self.broadcast_on_lag,
            ack_state: AckState::new(self.outcomes.clone()),
            registry: None,
            subscriber_id: None,
            disconnect_done: AtomicBool::new(false),
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.outcomes.close_all();
        let groups = self.group_handles.read().clone();
        for group in groups.as_ref() {
            group.admission.close();
            let _ = group.tx.close();
        }
        self.has_balanced_groups.store(false, Ordering::Release);
        self.broadcast_ring.close();
    }
}

pub(crate) struct BalancedSub<T: Send + Sync + 'static, L: Send + 'static = ()> {
    rx: Pin<Box<async_channel::Receiver<QueuedEnvelope<T, L>>>>,
    ack_state: AckState,
}

impl<T: Send + Sync + 'static, L: Send + 'static> SubscriptionBackend<T> for BalancedSub<T, L> {
    fn poll_recv_delivery(&mut self, cx: &mut Context<'_>) -> Poll<Result<RecvDelivery<T>, Error>> {
        match self.rx.as_mut().poll_next(cx) {
            Poll::Ready(Some(queued)) => {
                Poll::Ready(Ok(RecvDelivery::Message(Delivery::new_in_memory(
                    queued.envelope,
                    InMemoryDeliveryFinalizer::balanced(self.ack_state.clone()),
                ))))
            }
            Poll::Ready(None) => Poll::Ready(Err(SubscriptionClosed)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn ack(&self, id: u64) -> Result<(), Error> {
        self.ack_state.send_ack(id)
    }

    fn nack(&self, id: u64, reason: Arc<str>) -> Result<(), Error> {
        self.ack_state.send_nack(id, reason)
    }
}

pub(crate) struct BroadcastSub<T: Send + Sync + 'static> {
    ring: Arc<FastBroadcastRing<T>>,
    cursor: Arc<Mutex<BroadcastCursor>>,
    on_lag: TopicBroadcastOnLagPolicy,
    ack_state: AckState,
    /// Subscriber registry, present only in `all` mode. Used to remove this
    /// subscriber and nack any messages it still owes on disconnect/drop.
    registry: Option<Arc<BroadcastSubscriberRegistry>>,
    /// This subscriber's consensus identity, present only in `all` mode.
    subscriber_id: Option<BroadcastSubscriberId>,
    /// Guards the disconnect path so it runs at most once (lag then drop, etc.).
    disconnect_done: AtomicBool,
}

impl<T: Send + Sync + 'static> SubscriptionBackend<T> for BroadcastSub<T> {
    fn poll_recv_delivery(&mut self, cx: &mut Context<'_>) -> Poll<Result<RecvDelivery<T>, Error>> {
        {
            let cursor = self.cursor.lock();
            if cursor.disconnected_on_lag {
                return Poll::Ready(Err(SubscriptionClosed));
            }
            if cursor.permitted_seq.is_some() {
                cursor.permit_waiters.register(cx.waker());
                return Poll::Pending;
            }
        }

        if let Some(result) = self.try_read_delivery() {
            return Poll::Ready(result);
        }

        let should_spin = {
            let mut cursor = self.cursor.lock();
            if cursor.spun {
                false
            } else {
                cursor.spun = true;
                true
            }
        };

        if should_spin {
            for _ in 0..32 {
                std::hint::spin_loop();
                if let Some(result) = self.try_read_delivery() {
                    return Poll::Ready(result);
                }
            }
        }

        self.ring.register_waker(cx.waker());
        {
            let cursor = self.cursor.lock();
            if cursor.permitted_seq.is_some() {
                cursor.permit_waiters.register(cx.waker());
                return Poll::Pending;
            }
        }

        match self.try_read_delivery() {
            Some(result) => Poll::Ready(result),
            None if self.ring.is_closed() => Poll::Ready(Err(SubscriptionClosed)),
            None => Poll::Pending,
        }
    }

    fn ack(&self, id: u64) -> Result<(), Error> {
        self.ack_state.send_ack(id)
    }

    fn nack(&self, id: u64, reason: Arc<str>) -> Result<(), Error> {
        self.ack_state.send_nack(id, reason)
    }
}

impl<T: Send + Sync + 'static> BroadcastSub<T> {
    fn try_read_delivery(&self) -> Option<Result<RecvDelivery<T>, Error>> {
        let read_seq = {
            let cursor = self.cursor.lock();
            if cursor.disconnected_on_lag || cursor.permitted_seq.is_some() {
                return None;
            }
            cursor.read_seq
        };

        match self.ring.try_read(read_seq) {
            BroadcastReadResult::Ready(envelope) => {
                let mut cursor = self.cursor.lock();
                if cursor.disconnected_on_lag || cursor.permitted_seq.is_some() {
                    return None;
                }
                cursor.permitted_seq = Some(read_seq);
                cursor.spun = false;
                Some(Ok(RecvDelivery::Message(Delivery::new_in_memory(
                    envelope,
                    InMemoryDeliveryFinalizer::broadcast(
                        read_seq,
                        Arc::clone(&self.cursor),
                        self.ack_state.clone(),
                    ),
                ))))
            }
            BroadcastReadResult::Lagged {
                missed,
                new_read_seq,
            } => {
                let disconnect = {
                    let mut cursor = self.cursor.lock();
                    cursor.read_seq = new_read_seq;
                    cursor.spun = false;
                    if self.on_lag == TopicBroadcastOnLagPolicy::Disconnect {
                        cursor.disconnected_on_lag = true;
                        true
                    } else {
                        false
                    }
                };
                if disconnect {
                    // In `all` mode this nacks any messages this subscriber
                    // still owes an ack for. `first` mode is a no-op.
                    self.disconnect(Arc::<str>::from(
                        "broadcast subscriber disconnected after lag",
                    ));
                } else {
                    // `DropOldest`: the subscriber stays connected but the
                    // skipped messages (ring sequence < new_read_seq) were
                    // overwritten before it read them, so in `all` mode it can
                    // never ack them. Nack exactly those owed entries; messages
                    // at or after new_read_seq remain readable. No-op in `first`
                    // mode.
                    self.nack_skipped_on_lag(
                        new_read_seq,
                        Arc::<str>::from("broadcast subscriber lagged past message"),
                    );
                }
                Some(Ok(RecvDelivery::Lagged { missed }))
            }
            BroadcastReadResult::NotReady => None,
        }
    }

    /// Remove this subscriber from the registry and nack every consensus
    /// (`all`-mode) message it still owes an ack for. No-op in `first` mode and
    /// idempotent across repeated calls (lag-disconnect followed by drop).
    ///
    /// Remove the subscriber under the registry lock, then release it before
    /// scanning tracked entries. Once removed, no later publish can include this
    /// subscriber in a new consensus snapshot.
    fn disconnect(&self, reason: Arc<str>) {
        let (Some(registry), Some(subscriber_id)) = (&self.registry, self.subscriber_id) else {
            return;
        };
        if self.disconnect_done.swap(true, Ordering::AcqRel) {
            return;
        }
        {
            let mut subscribers = registry.subscribers.lock();
            let _ = Arc::make_mut(&mut subscribers).remove(&subscriber_id);
        }
        self.ack_state
            .outcomes
            .nack_pending_for_subscriber(subscriber_id, reason);
    }

    /// Nack the `all`-mode consensus messages this subscriber skipped after a
    /// `DropOldest` lag (publish sequence `< new_read_seq`) without disconnecting
    /// it. No-op in `first` mode.
    ///
    /// Acquire and release the registry lock as a barrier before scanning. Any
    /// publish that reserved a sequence below `new_read_seq` must finish
    /// consensus registration before this lock can be acquired; a later publish
    /// receives a sequence at or above the threshold.
    fn nack_skipped_on_lag(&self, new_read_seq: u64, reason: Arc<str>) {
        let (Some(registry), Some(subscriber_id)) = (&self.registry, self.subscriber_id) else {
            return;
        };
        let subscribers = registry.subscribers.lock();
        drop(subscribers);
        self.ack_state
            .outcomes
            .nack_owed_before(subscriber_id, new_read_seq, reason);
    }
}

impl<T: Send + Sync + 'static> Drop for BroadcastSub<T> {
    fn drop(&mut self) {
        self.disconnect(Arc::<str>::from("broadcast subscriber dropped"));
    }
}

pub(crate) struct BroadcastCursor {
    read_seq: u64,
    permitted_seq: Option<u64>,
    disconnected_on_lag: bool,
    spun: bool,
    permit_waiters: WakerSet,
}

fn finish_broadcast_delivery(cursor: &Arc<Mutex<BroadcastCursor>>, seq: u64) {
    let mut cursor = cursor.lock();
    if cursor.permitted_seq == Some(seq) {
        cursor.read_seq = seq + 1;
        cursor.permitted_seq = None;
        cursor.spun = false;
        cursor.permit_waiters.wake_all();
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InMemoryDeliveryKind {
    Balanced,
    Broadcast,
}

pub(crate) enum InMemoryDeliveryFinalizer {
    Balanced {
        ack_state: AckState,
    },
    Broadcast {
        seq: u64,
        cursor: Arc<Mutex<BroadcastCursor>>,
        ack_state: AckState,
    },
}

impl InMemoryDeliveryFinalizer {
    pub(crate) fn balanced(ack_state: AckState) -> Self {
        Self::Balanced { ack_state }
    }

    pub(crate) fn broadcast(
        seq: u64,
        cursor: Arc<Mutex<BroadcastCursor>>,
        ack_state: AckState,
    ) -> Self {
        Self::Broadcast {
            seq,
            cursor,
            ack_state,
        }
    }

    pub(crate) fn commit(&mut self) {
        if let Self::Broadcast { seq, cursor, .. } = self {
            finish_broadcast_delivery(cursor, *seq);
        }
    }

    pub(crate) fn abort<T: Send + Sync + 'static>(
        &mut self,
        envelope: &Envelope<T>,
        reason: Arc<str>,
    ) -> Result<(), Error> {
        match self {
            Self::Balanced { ack_state } => {
                if envelope.tracked {
                    _ = ack_state.send_nack(envelope.id, reason);
                }
            }
            Self::Broadcast {
                seq,
                cursor,
                ack_state,
            } => {
                if envelope.tracked {
                    _ = ack_state.send_nack(envelope.id, reason);
                }
                finish_broadcast_delivery(cursor, *seq);
            }
        }
        Ok(())
    }

    pub(crate) fn abandon<T: Send + Sync + 'static>(&mut self, envelope: &Envelope<T>) {
        match self {
            Self::Balanced { ack_state } => {
                if envelope.tracked {
                    _ = ack_state.send_nack(
                        envelope.id,
                        Arc::<str>::from("topic delivery abandoned before forward"),
                    );
                }
            }
            Self::Broadcast {
                seq,
                cursor,
                ack_state,
            } => {
                if envelope.tracked {
                    _ = ack_state.send_nack(
                        envelope.id,
                        Arc::<str>::from("topic delivery abandoned before forward"),
                    );
                }
                finish_broadcast_delivery(cursor, *seq);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn kind(&self) -> InMemoryDeliveryKind {
        match self {
            Self::Balanced { .. } => InMemoryDeliveryKind::Balanced,
            Self::Broadcast { .. } => InMemoryDeliveryKind::Broadcast,
        }
    }
}

#[derive(Clone)]
pub(crate) struct AckState {
    pub(crate) outcomes: TrackedPublishTracker,
    /// Ack-resolution mode for this subscriber. `First` (the default for
    /// balanced and `first`-mode broadcast) resolves on the first Ack/Nack;
    /// `All` routes acks through per-subscriber consensus aggregation.
    ack_mode: TopicBroadcastAckMode,
    /// Consensus identity, present only for `all`-mode broadcast subscribers.
    subscriber_id: Option<BroadcastSubscriberId>,
}

impl AckState {
    /// `AckState` for balanced subscribers and `first`-mode broadcast: the
    /// first terminal Ack/Nack resolves the tracked publish.
    pub(crate) fn new(outcomes: TrackedPublishTracker) -> Self {
        Self {
            outcomes,
            ack_mode: TopicBroadcastAckMode::First,
            subscriber_id: None,
        }
    }

    /// `AckState` for a broadcast subscriber. In `all` mode (`subscriber_id` is
    /// `Some`) acks are aggregated per subscriber; in `first` mode it behaves
    /// like [`AckState::new`].
    pub(crate) fn broadcast(
        outcomes: TrackedPublishTracker,
        ack_mode: TopicBroadcastAckMode,
        subscriber_id: Option<BroadcastSubscriberId>,
    ) -> Self {
        Self {
            outcomes,
            ack_mode,
            subscriber_id,
        }
    }

    pub(crate) fn send_ack(&self, message_id: u64) -> Result<(), Error> {
        // `all`-mode broadcast: contribute this subscriber's Ack to the
        // consensus. The message stays tracked (Ok) until the set completes.
        if let (TopicBroadcastAckMode::All, Some(subscriber_id)) =
            (self.ack_mode, self.subscriber_id)
        {
            return match self.outcomes.resolve_ack_from(message_id, subscriber_id) {
                AckFromResult::Resolved | AckFromResult::StillPending => Ok(()),
                AckFromResult::NotTracked => Err(MessageNotTracked),
            };
        }

        if self
            .outcomes
            .resolve(message_id, TrackedPublishOutcome::Ack)
        {
            Ok(())
        } else {
            Err(MessageNotTracked)
        }
    }

    pub(crate) fn send_nack(
        &self,
        message_id: u64,
        reason: impl Into<Arc<str>>,
    ) -> Result<(), Error> {
        let reason = reason.into();

        // `all`-mode broadcast: only a subscriber that still requires the message
        // may fail the consensus. A Nack from a subscriber that has already acked
        // (or was never eligible) is a no-op, mirroring the disappearance handling
        // and keeping outcomes insensitive to accidental double-signalling.
        if let (TopicBroadcastAckMode::All, Some(subscriber_id)) =
            (self.ack_mode, self.subscriber_id)
        {
            return match self
                .outcomes
                .resolve_nack_from(message_id, subscriber_id, reason)
            {
                NackFromResult::Resolved | NackFromResult::NotRequired => Ok(()),
                NackFromResult::NotTracked => Err(MessageNotTracked),
            };
        }

        if self
            .outcomes
            .resolve(message_id, TrackedPublishOutcome::Nack { reason })
        {
            Ok(())
        } else {
            Err(MessageNotTracked)
        }
    }
}
