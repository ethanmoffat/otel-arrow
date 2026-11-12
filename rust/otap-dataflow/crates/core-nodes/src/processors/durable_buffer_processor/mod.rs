// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Durable buffer for crash-resilient buffering of pipeline data.
//!
//! This processor provides durable buffering by writing incoming
//! telemetry data to a write-ahead log and segment storage before forwarding
//! downstream. On acknowledgement from downstream, the data is marked as
//! consumed; on rejection, data can be replayed.
//!
//! OTAP Arrow records, OTLP bytes in `pass_through` mode, and pluggable Arrow
//! record sets are persisted and replayed without changing their
//! representation. Other inputs are converted to OTAP Arrow records before
//! they are stored.
//!
//! # Architecture
//!
//! ```text
//! Upstream -> DurableBuffer -> Downstream
//!                    v
//!              StorageEngine
//!                    v
//!            WAL + Segments
//! ```
//!
//! # Per-Core Isolation
//!
//! Each processor instance (one per CPU core) has its own isolated storage engine
//! with a separate WAL and segment store. Data is partitioned by core at runtime,
//! with each core's data stored in `{path}/core_{core_id}/`.
//!
//! # Dispatch Strategy Considerations
//!
//! **Important**: The dispatch strategy on the incoming edge affects behavior:
//!
//! | Strategy | Behavior | Recommendation |
//! |----------|----------|----------------|
//! | `RoundRobin` | Data distributed across cores, each persists its share | \[x\] **Recommended** |
//! | `Random` | Similar to round-robin | \[x\] OK |
//! | `LeastLoaded` | Similar to round-robin | \[x\] OK |
//! | `Broadcast` | Same data persisted N times (once per core) | (!) **Avoid** - causes Nx storage and duplicates |
//!
//! For the outgoing edge (to exporters), any dispatch strategy is valid.
//!
//! # Message Flow
//!
//! - `Message::Data`: Ingested to storage, ACK sent upstream after the WAL
//!   append, or held until a WAL sync covers the bundle when `upstream_ack` is
//!   `after_wal_sync`. With `forward_after_ingest`, the bundle is also sent
//!   downstream right after the WAL append when `max_in_flight` and the
//!   downstream channel allow it
//! - `TimerTick`: Poll storage for bundles, send downstream. A polled bundle
//!   that was already forwarded after ingest is not sent again: its handle is
//!   kept until downstream answers, or resolved with the answer that arrived
//!   before the segment was finalized
//! - `Ack`: Extract BundleRef from calldata, call handle.ack()
//! - `Nack (permanent)`: Call handle.reject() -- no retry
//! - `Nack (transient)`: Call handle.defer() and schedule retry via a wakeup
//! - `Wakeup`: Run a scheduled retry, or sync the WAL and release held ACKs
//! - `Shutdown`: Release held ACKs, then flush storage engine
//!
//! # Retry Behavior and Error Handling
//!
//! On NACK from downstream, bundles are handled based on the NACK's `permanent` status:
//!
//! - **Permanent NACKs** (e.g., malformed data, schema validation failures): The bundle
//!   is immediately rejected via `handle.reject()` and will not be retried. Monitor the
//!   `resolved{outcome="permanently_rejected"}` metric to detect data being dropped due to
//!   permanent failures.
//!
//! - **Transient NACKs** (e.g., network issues, temporary downstream unavailability): Bundles
//!   are retried with exponential backoff until either delivery succeeds or the data is evicted
//!   by the configured retention policy (`retention_size_cap` + `drop_oldest`).
//!
//! There is no `max_retries` limit for transient failures: a retry limit would cause
//! **data loss** during legitimate extended outages.
//!
//! **Operational guidance:**
//!
//! - Monitor `resolved{outcome="permanently_rejected"}` to detect permanent failures (data loss)
//! - Monitor `retries.scheduled` in the `processor.durable_buffer` metric scope to detect
//!   persistently failing data
//! - Use `retention_size_cap` to bound storage; `drop_oldest` policy evicts
//!   stuck data when space is needed for new data
//! - `max_in_flight` limit prevents thundering herd after recovery

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = DURABLE_BUFFER_URN,
    target = "otel.processor.durable_buffer",
);

mod bundle_adapter;
mod config;
mod deferred_retry_state;
mod metrics;
mod wal_sync_acks;

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_quiver::budget::DiskBudget;
use otel_arrow_dfe_quiver::segment::SegmentSeq;
use otel_arrow_dfe_quiver::segment_store::SegmentStore;
use otel_arrow_dfe_quiver::subscriber::{
    BundleHandle, BundleIndex, BundleRef, RegistryCallback, SegmentProvider, SubscriberId,
};
use otel_arrow_dfe_quiver::{
    DataDirHolder, QuiverConfig, QuiverEngine, QuiverError, RetentionLossCounts,
    RetentionLossSnapshot, SegmentVisibility,
};
use smallvec::smallvec;

use otel_arrow_dfe_otap::OTAP_PROCESSOR_FACTORIES;
use otel_arrow_dfe_otap::pdata::{Context, OtapPdata, PdataEffectHandlerExtension};

pub use bundle_adapter::{BundleConversionError, MAX_PLUGGABLE_TABLES, QuiverPersistenceLimit};
use bundle_adapter::{
    OtapRecordBundleAdapter, OtlpBytesAdapter, PdataArrowBundleAdapter, convert_bundle_to_pdata,
    recover_byte_count, recover_item_count, signal_type_from_slot_id,
};
pub use config::{
    DurableBufferConfig, OtlpHandling, OwnershipSettings, SizeCapPolicy, UpstreamAck,
};
use deferred_retry_state::DeferredRetryState;
#[cfg(test)]
use deferred_retry_state::RETRY_WAKEUP_SLOT;
use metrics::{BundleOutcome, DurableBufferMetrics, IngestFailure, LossReason};
#[cfg(test)]
use metrics::{LossAttributes, SignalLossAttributes};
use wal_sync_acks::{MAX_HELD_ACKS, WAL_SYNC_WAKEUP_SLOT, WalSyncAcks};

use otel_arrow_dfe_channel::error::SendError;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::Context8u8;
use otel_arrow_dfe_engine::control::{
    AckMsg, CallData, NackMsg, NodeControlMsg, WakeupRevision, WakeupSlot,
};
use otel_arrow_dfe_engine::error::{Error, TypedError};
use otel_arrow_dfe_engine::local::processor::EffectHandler;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_engine::{
    ConsumerEffectHandlerExtension, Interests, LocalWakeupRequirements, ProcessorFactory,
    ProcessorRuntimeRequirements, ProducerEffectHandlerExtension,
};
use otel_arrow_dfe_pdata_codec::OtapPayload;
#[cfg(test)]
use otel_arrow_dfe_telemetry::common_attributes::SignalAttributes;

/// URN for the durable buffer.
pub const DURABLE_BUFFER_URN: &str = "urn:otel:processor:durable_buffer";

/// Minimum interval between repeated warning logs for the same condition
/// (backpressure, flush failures). Prevents log flooding when the timer
/// tick fires every poll_interval (~100 ms).
const WARN_RATE_LIMIT: Duration = Duration::from_secs(10);

/// Subscriber ID used by this processor.
const SUBSCRIBER_ID: &str = "durable-buffer";

// -----------------------------------------------------------------------------
// BundleRef CallData Encoding
// -----------------------------------------------------------------------------

/// Encode a BundleRef into CallData for ACK/NACK tracking.
///
/// Layout: [segment_seq (u64), bundle_index (u32 packed into u64)]
fn encode_bundle_ref(bundle_ref: BundleRef) -> CallData {
    smallvec![
        Context8u8::from(bundle_ref.segment_seq.raw()),
        Context8u8::from(bundle_ref.bundle_index.raw() as u64),
    ]
}

/// Decode a BundleRef from CallData.
fn decode_bundle_ref(calldata: &CallData) -> Option<BundleRef> {
    if calldata.len() < 2 {
        return None;
    }
    let segment_seq = SegmentSeq::new(u64::from(calldata[0]));
    let bundle_index = BundleIndex::new(u64::from(calldata[1]) as u32);
    Some(BundleRef {
        segment_seq,
        bundle_index,
    })
}

// -----------------------------------------------------------------------------
// Pending Bundle Tracking
// -----------------------------------------------------------------------------

/// Resolve a claimed handle with a recorded early-forward outcome.
fn apply_early_outcome(handle: QuiverBundleHandle, outcome: EarlyOutcome) {
    match outcome {
        EarlyOutcome::Acked => handle.ack(),
        EarlyOutcome::Rejected => handle.reject(),
    }
}

/// State for tracking a pending downstream delivery.
///
/// Holds the Quiver bundle handle to keep the bundle claimed while in-flight.
/// When dropped without explicit ack(), the handle's Drop impl will call
/// on_deferred(), releasing the claim and making the bundle eligible for redelivery.
///
/// A bundle forwarded right after ingest (`forward_after_ingest`) has no
/// handle until its segment is finalized and the poll path returns it. The
/// poll path then stores the handle here instead of sending the bundle again.
struct PendingBundle {
    /// The Quiver bundle handle - keeps the bundle claimed while in-flight.
    /// `None` while an early-forwarded bundle's segment is not yet polled.
    handle: Option<QuiverBundleHandle>,
    /// Number of retries attempted.
    retry_count: u32,
    /// Number of items in this bundle (0 for legacy segments without manifest counts).
    item_count: u64,
    /// Signal type of this bundle.
    signal_type: SignalType,
}

/// Result of attempting to process a bundle with non-blocking send.
enum ProcessBundleResult {
    /// Bundle was successfully sent downstream.
    Sent,
    /// Bundle was skipped (already in-flight or scheduled for retry).
    Skipped,
    /// The downstream channel is full (backpressure).
    /// The bundle has been deferred and will be retried on the next tick.
    Backpressure,
    /// An unrecoverable error occurred.
    Error(Error),
    /// The bundle was forwarded right after ingest. Its handle is now tracked
    /// by the in-flight entry, or resolved with the downstream outcome that
    /// arrived before the segment was visible. Nothing was sent.
    Resolved,
}

/// Downstream outcome of an early-forwarded bundle that arrived before its
/// segment was visible to the subscriber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EarlyOutcome {
    /// Downstream acknowledged the bundle; ack the handle once claimed.
    Acked,
    /// Downstream permanently rejected the bundle; reject the handle once
    /// claimed.
    Rejected,
}

// -----------------------------------------------------------------------------
// DurableBuffer
// -----------------------------------------------------------------------------

/// Type alias for the bundle handle with our callback type.
type QuiverBundleHandle = BundleHandle<RegistryCallback<SegmentStore>>;

/// State of the Quiver engine (lazy initialization).
enum EngineState {
    /// Engine not yet initialized.
    Uninitialized,
    /// Another engine in this process holds the core's data directory, so
    /// the open is retried on each timer tick until that engine releases it
    /// or `ownership.handoff_timeout` passes. This happens while a live
    /// reconfiguration replaces the pipeline and the old instance is still
    /// running.
    AwaitingHandoff {
        /// When the first open was refused.
        since: Instant,
        /// The data message that arrived first, held until the engine opens.
        /// No more pdata is delivered while waiting (see `accept_pdata`).
        held: Option<OtapPdata>,
    },
    /// Engine initialized and ready.
    Ready {
        engine: Arc<QuiverEngine>,
        subscriber_id: SubscriberId,
    },
    /// Engine failed to initialize.
    Failed(String),
}

/// Error from [`DurableBuffer::init_engine`].
#[derive(Debug)]
enum InitError {
    /// The storage engine could not be opened, for example because another
    /// instance owns the data directory.
    Open(QuiverError),
    /// Any other setup step failed.
    Setup(Error),
}

impl std::fmt::Display for InitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(e) => write!(f, "failed to create Quiver engine: {e}"),
            Self::Setup(e) => write!(f, "{e}"),
        }
    }
}

/// What to do after opening the storage engine failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HandoffDecision {
    /// Another engine in this process holds the directory and the wait is
    /// still within `handoff_timeout`: retry later.
    Wait,
    /// Another engine in this process still holds the directory after
    /// `handoff_timeout`.
    TimedOut,
    /// Waiting cannot help: the holder is in another process or unknown, the
    /// error is not an ownership refusal, or waiting is disabled.
    Fail,
}

/// Decides whether a failed open should wait for the directory to be handed
/// over, given how long the node has already waited.
fn handoff_decision(
    error: &QuiverError,
    handoff_timeout: Duration,
    waited: Duration,
) -> HandoffDecision {
    let same_process = matches!(
        error,
        QuiverError::DataDirOwned { holder: Some(holder), .. } if holder.same_process
    );
    if !same_process || handoff_timeout.is_zero() {
        HandoffDecision::Fail
    } else if waited >= handoff_timeout {
        HandoffDecision::TimedOut
    } else {
        HandoffDecision::Wait
    }
}

/// The holder named by an ownership refusal, if any.
fn refusal_holder(error: &QuiverError) -> Option<&DataDirHolder> {
    match error {
        QuiverError::DataDirOwned {
            holder: Some(holder),
            ..
        } => Some(holder),
        _ => None,
    }
}

/// Outcome of trying to resume one deferred retry.
enum RetryResumeOutcome {
    /// The retry was re-claimed and sent downstream.
    Sent,
    /// The retry remains deferred because resend is blocked for now.
    Deferred,
    /// The retry no longer needs to be retried on this pass.
    Skipped,
    /// The bundle's segment is not finalized yet, so it cannot be claimed.
    /// The retry was rescheduled after `poll_interval`; other due retries
    /// can still run.
    AwaitingSegment,
}

/// Cached per-segment signal classification for queued-item gauge computation.
///
/// Populated once per segment (on first access after finalization) and never
/// invalidated -- segments are immutable after finalization.  Evicted when the
/// segment is no longer tracked by the subscriber.
struct SegmentMetricsSummary {
    /// Per-bundle `(item_count, signal_type)`, ordered by bundle index.
    bundles: Vec<(u64, Option<SignalType>)>,
    /// Pre-computed totals for the fully-pending case (no bundles resolved).
    total_logs: u64,
    total_metrics: u64,
    total_spans: u64,
}

/// Cached segment summary with recency tracking for bounded eviction.
struct CachedSegmentMetrics {
    summary: SegmentMetricsSummary,
    last_seen_generation: u64,
}

fn retention_loss_delta(
    current: RetentionLossSnapshot,
    previous: RetentionLossSnapshot,
) -> RetentionLossSnapshot {
    fn counts_delta(
        current: RetentionLossCounts,
        previous: RetentionLossCounts,
    ) -> RetentionLossCounts {
        RetentionLossCounts {
            segments: current.segments.saturating_sub(previous.segments),
            bundles: current.bundles.saturating_sub(previous.bundles),
            items: current.items.saturating_sub(previous.items),
            bytes: current.bytes.saturating_sub(previous.bytes),
            reclaimed_bytes: current
                .reclaimed_bytes
                .saturating_sub(previous.reclaimed_bytes),
        }
    }

    RetentionLossSnapshot {
        drop_oldest: counts_delta(current.drop_oldest, previous.drop_oldest),
        expired: counts_delta(current.expired, previous.expired),
    }
}

/// Durable buffer that provides crash-resilient buffering via Quiver.
///
/// # Segment Metrics Cache
///
/// To avoid per-tick allocations in `recompute_metrics`, this struct
/// maintains a `segment_cache` that maps finalized segment sequences to their
/// pre-computed per-bundle signal classification.  Because segments are
/// **immutable** after finalization, the cache entry for a given segment
/// never needs invalidation -- only eviction when the segment is cleaned up.
pub struct DurableBuffer {
    /// The Quiver engine state (lazy initialized on first message).
    engine_state: EngineState,

    /// Map of in-flight bundles awaiting downstream ACK/NACK.
    /// Key is the (segment_seq, bundle_index) pair encoded as a u128 for fast lookup.
    pending_bundles: HashMap<(u64, u32), PendingBundle>,

    /// Processor-local retry deferral state, driven by one wakeup slot plus a
    /// local ordered retry queue.
    deferred_retry_state: DeferredRetryState,

    /// Downstream outcomes of early-forwarded bundles whose segment was not
    /// visible yet when the outcome arrived. Applied on the first timer tick
    /// (or poll) after the segment is finalized, and dropped if finalization
    /// fails. Holds at most the bundles of the open segment plus those of
    /// segments finalized since the last timer tick, and is always empty when
    /// `forward_after_ingest` is off.
    early_outcomes: HashMap<(u64, u32), EarlyOutcome>,

    /// Upstream acks held until a WAL sync covers them, used when
    /// `upstream_ack` is `after_wal_sync`.
    wal_sync_acks: WalSyncAcks<OtapPdata>,

    /// Configuration.
    config: DurableBufferConfig,

    /// Core ID for per-core data directory.
    /// Per ARCHITECTURE.md, each core has its own Quiver instance.
    core_id: usize,

    /// Total number of cores running this pipeline.
    /// Used to divide the retention_size_cap across cores.
    num_cores: usize,

    /// Metrics.
    metrics: DurableBufferMetrics,

    /// Whether timer has been started.
    timer_started: bool,

    /// Last time we logged a flush warning (rate-limiting).
    last_flush_warn: Option<Instant>,

    /// Last time we logged a backpressure warning (rate-limiting).
    last_backpressure_warn: Option<Instant>,

    /// Set once the engine reports that another instance claimed this
    /// core's data directory. The engine refuses every write from then on,
    /// so the node NACKs incoming data and stops reading stored bundles.
    fenced: bool,

    /// Cached per-segment signal classification.
    ///
    /// Populated on first access per segment; evicted when the subscriber no
    /// longer references the segment (all bundles resolved and progress cleaned up)
    /// and also evicted by recency (LRU-style) when the bounded cache limit is exceeded.
    segment_cache: HashMap<u64, CachedSegmentMetrics>,

    /// Monotonic generation counter used to track segment cache recency.
    segment_cache_generation: u64,

    /// Segment IDs for which metadata load failure has already been warned.
    ///
    /// Prevents warning spam when `bundle_metadata` repeatedly fails for the
    /// same segment across telemetry ticks.
    metadata_load_warned_segments: HashSet<u64>,

    /// Last cumulative Quiver retention-loss totals used to compute segment/bundle deltas.
    last_loss_snapshot: RetentionLossSnapshot,
}

impl DurableBuffer {
    /// Creates a new durable buffer with the given configuration.
    ///
    /// Validates that the configured `retention_size_cap` is large enough for
    /// the Quiver engine to operate via [`DiskBudget::minimum_hard_cap()`].
    ///
    /// Note: The Quiver engine is lazily initialized on the first message
    /// to ensure we're running within an async context.
    pub fn new(
        config: DurableBufferConfig,
        pipeline_ctx: &PipelineContext,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        let metrics = DurableBufferMetrics::new(pipeline_ctx);
        let core_id = pipeline_ctx.core_id();
        let num_cores = pipeline_ctx.num_cores();

        // Validate that per-core budget is large enough for the engine.
        // QuiverEngine::open() enforces this lazily on first message. Catch it
        // here so the pipeline fails fast at construction time.
        let quiver_config = Self::build_quiver_config(&config);
        let total_size_cap = config.size_cap_bytes();
        let per_core_size_cap = total_size_cap / num_cores.max(1) as u64;
        let segment_size = quiver_config.segment.target_size_bytes.get();
        let wal_max = DiskBudget::effective_wal_size(&quiver_config);
        let min_per_core = DiskBudget::minimum_hard_cap(segment_size, wal_max);

        if per_core_size_cap < min_per_core {
            let min_total = min_per_core * num_cores as u64;
            let max_cores = total_size_cap / min_per_core;
            return Err(ConfigError::InvalidUserConfig {
                error: if max_cores == 0 {
                    format!(
                        "retention_size_cap ({total_size_cap} bytes) is too small: \
                         the engine requires at least {min_per_core} bytes per core \
                         (WAL max {wal_max} + 2 * segment size {segment_size}). \
                         Increase retention_size_cap to at least {min_total} bytes",
                    )
                } else {
                    format!(
                        "retention_size_cap ({total_size_cap} bytes) is too small for {num_cores} core(s): \
                         per-core capacity is {per_core_size_cap} bytes, but the engine requires at \
                         least {min_per_core} bytes per core (WAL max {wal_max} + 2 * segment size {segment_size}). \
                         Either increase retention_size_cap to at least {min_total} bytes, \
                         or reduce the core count to at most {max_cores}",
                    )
                },
            });
        }

        Ok(Self {
            engine_state: EngineState::Uninitialized,
            pending_bundles: HashMap::new(),
            deferred_retry_state: DeferredRetryState::new(),
            early_outcomes: HashMap::new(),
            wal_sync_acks: WalSyncAcks::new(MAX_HELD_ACKS),
            config,
            core_id,
            num_cores,
            metrics,
            timer_started: false,
            last_flush_warn: None,
            last_backpressure_warn: None,
            fenced: false,
            segment_cache: HashMap::new(),
            segment_cache_generation: 0,
            metadata_load_warned_segments: HashSet::new(),
            last_loss_snapshot: RetentionLossSnapshot::default(),
        })
    }

    /// Upper bound for cached finalized segment summaries.
    const MAX_SEGMENT_CACHE_ENTRIES: usize = 4096;

    /// Evict least-recently-used cached segment summaries when above the limit.
    fn enforce_segment_cache_bound(&mut self) {
        self.enforce_segment_cache_bound_with_limit(Self::MAX_SEGMENT_CACHE_ENTRIES);
    }

    fn enforce_segment_cache_bound_with_limit(&mut self, max_entries: usize) {
        let len = self.segment_cache.len();
        if len <= max_entries {
            return;
        }

        let evict_count = len - max_entries;
        let mut by_age: Vec<(u64, u64)> = self
            .segment_cache
            .iter()
            .map(|(seq, cached)| (*seq, cached.last_seen_generation))
            .collect();
        by_age.sort_by_key(|(_, generation)| *generation);

        for (seq, _) in by_age.into_iter().take(evict_count) {
            let _ = self.segment_cache.remove(&seq);
            let _ = self.metadata_load_warned_segments.remove(&seq);
        }
    }

    /// Build the [`QuiverConfig`] that will be used for the engine.
    ///
    /// This is the single source of truth for translating [`DurableBufferConfig`]
    /// fields into Quiver engine settings. Both [`new()`](Self::new) (for early
    /// validation) and [`init_engine()`](Self::init_engine) (for construction)
    /// call this so the two can never drift apart.
    ///
    /// The returned config does **not** include the per-core `data_dir`; that is
    /// added by `init_engine` since it depends on `core_id`.
    fn build_quiver_config(config: &DurableBufferConfig) -> QuiverConfig {
        // Exhaustive destructure: if a field is added to DurableBufferConfig,
        // the compiler will force you to handle it here (no `..`).
        let DurableBufferConfig {
            path: _,               // per-core subdir added by init_engine
            retention_size_cap: _, // drives the DiskBudget, not QuiverConfig
            max_age,
            size_cap_policy: _, // drives the DiskBudget policy
            poll_interval: _,   // DurableBuffer timer, not engine config
            otlp_handling: _,   // DurableBuffer adapter concern
            max_segment_open_duration,
            wal_flush_interval,
            upstream_ack: _, // DurableBuffer ack timing, uses QuiverEngine::sync_wal
            wal_compression,
            segment_compression,
            initial_retry_interval: _, // DurableBuffer retry logic
            max_retry_interval: _,     // DurableBuffer retry logic
            retry_multiplier: _,       // DurableBuffer retry logic
            max_in_flight: _,          // DurableBuffer flow control
            forward_after_ingest: _,   // DurableBuffer delivery timing
            ownership,
        } = config;

        let mut quiver_config = QuiverConfig::default();
        quiver_config.segment.max_open_duration = *max_segment_open_duration;
        quiver_config.retention.max_age = *max_age;
        quiver_config.wal.flush_interval = *wal_flush_interval;
        quiver_config.wal.compression = *wal_compression;
        quiver_config.segment.compression = *segment_compression;
        quiver_config.ownership = ownership.into();
        quiver_config
    }

    /// Calculate exponential backoff delay with jitter.
    ///
    /// Formula: min(initial * multiplier^retry_count, max_interval) * (0.5 + random(0.5))
    fn calculate_backoff(&self, retry_count: u32) -> Duration {
        let base = self.config.initial_retry_interval.as_secs_f64()
            * self.config.retry_multiplier.powi(retry_count as i32);
        let capped = base.min(self.config.max_retry_interval.as_secs_f64());

        // Add jitter: 50-100% of the capped value
        // Use a simple deterministic "jitter" based on retry_count to avoid rand dependency
        // This spreads retries but is deterministic for testing
        let jitter_factor = 0.5 + (((retry_count as f64 * 0.618033988749895) % 1.0) * 0.5);
        let with_jitter = capped * jitter_factor;

        Duration::from_secs_f64(with_jitter)
    }

    /// Check if we can send more bundles downstream (respects max_in_flight limit).
    fn can_send_more(&self) -> bool {
        self.pending_bundles.len() < self.config.max_in_flight
    }

    /// Resumes one deferred retry, either by sending it downstream again or by
    /// re-deferring it if downstream/backpressure constraints still apply.
    ///
    /// Guarantees:
    /// - respects `max_in_flight`
    /// - re-defers blocked retries with `poll_interval`
    /// - returns enough outcome information for the caller to decide whether
    ///   the current wakeup pass should keep resuming more due retries
    fn resume_retry(
        &mut self,
        bundle_ref: BundleRef,
        retry_count: u32,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<RetryResumeOutcome, Error> {
        if !self.can_send_more() {
            otel_debug!(
                "durable_buffer.retry.deferred",
                segment_seq = bundle_ref.segment_seq.raw(),
                bundle_index = bundle_ref.bundle_index.raw(),
                in_flight = self.pending_bundles.len(),
                max_in_flight = self.config.max_in_flight
            );

            if !self.deferred_retry_state.schedule_after(
                bundle_ref,
                retry_count,
                self.config.poll_interval,
                effect_handler,
            ) {
                otel_warn!("durable_buffer.retry.reschedule_failed");
            }
            return Ok(RetryResumeOutcome::Deferred);
        }

        let claim_result = {
            let (engine, subscriber_id) = self.engine()?;
            match engine.segment_visibility(bundle_ref.segment_seq) {
                SegmentVisibility::Visible => Some(engine.claim_bundle(subscriber_id, bundle_ref)),
                // An early-forwarded bundle can be NACKed before its segment
                // is finalized; it cannot be claimed yet.
                SegmentVisibility::Pending => None,
                // Finalization failed or the segment was already removed.
                SegmentVisibility::Unavailable => {
                    otel_debug!(
                        "durable_buffer.retry.segment_unavailable",
                        segment_seq = bundle_ref.segment_seq.raw(),
                        bundle_index = bundle_ref.bundle_index.raw()
                    );
                    return Ok(RetryResumeOutcome::Skipped);
                }
            }
        };
        let Some(claim_result) = claim_result else {
            otel_debug!(
                "durable_buffer.retry.awaiting_segment",
                segment_seq = bundle_ref.segment_seq.raw(),
                bundle_index = bundle_ref.bundle_index.raw()
            );
            if !self.deferred_retry_state.schedule_after(
                bundle_ref,
                retry_count,
                self.config.poll_interval,
                effect_handler,
            ) {
                otel_warn!("durable_buffer.retry.reschedule_failed");
            }
            return Ok(RetryResumeOutcome::AwaitingSegment);
        };

        match claim_result {
            Ok(handle) => match self.try_process_bundle_handle_with_retry_count(
                handle,
                retry_count,
                effect_handler,
            ) {
                ProcessBundleResult::Sent => {
                    otel_debug!(
                        "durable_buffer.retry.sent",
                        segment_seq = bundle_ref.segment_seq.raw(),
                        bundle_index = bundle_ref.bundle_index.raw(),
                        retry_count = retry_count
                    );
                    Ok(RetryResumeOutcome::Sent)
                }
                ProcessBundleResult::Skipped | ProcessBundleResult::Resolved => {
                    otel_warn!(
                        "durable_buffer.retry.skipped",
                        segment_seq = bundle_ref.segment_seq.raw(),
                        bundle_index = bundle_ref.bundle_index.raw()
                    );
                    Ok(RetryResumeOutcome::Skipped)
                }
                ProcessBundleResult::Backpressure => {
                    otel_debug!(
                        "durable_buffer.retry.backpressure",
                        segment_seq = bundle_ref.segment_seq.raw(),
                        bundle_index = bundle_ref.bundle_index.raw()
                    );

                    if !self.deferred_retry_state.schedule_after(
                        bundle_ref,
                        retry_count,
                        self.config.poll_interval,
                        effect_handler,
                    ) {
                        otel_warn!("durable_buffer.retry.reschedule_failed");
                    }
                    Ok(RetryResumeOutcome::Deferred)
                }
                ProcessBundleResult::Error(e) => Err(e),
            },
            Err(e) => {
                otel_debug!(
                    "durable_buffer.retry.claim_failed",
                    segment_seq = bundle_ref.segment_seq.raw(),
                    bundle_index = bundle_ref.bundle_index.raw(),
                    error = %e
                );
                Ok(RetryResumeOutcome::Skipped)
            }
        }
    }

    /// Lazily initialize the Quiver engine on first use.
    ///
    /// If another engine in this process holds the core's data directory and
    /// `ownership.handoff_timeout` is not zero, the node enters
    /// [`EngineState::AwaitingHandoff`] and returns `Ok`; the open is then
    /// retried on each timer tick by [`Self::retry_handoff`].
    async fn ensure_engine_initialized(&mut self, now: Instant) -> Result<(), Error> {
        match &self.engine_state {
            EngineState::Ready { .. } | EngineState::AwaitingHandoff { .. } => Ok(()),
            EngineState::Failed(msg) => Err(Error::InternalError {
                message: msg.clone(),
            }),
            EngineState::Uninitialized => match self.init_engine().await {
                Ok((engine, subscriber_id)) => {
                    self.finish_init(engine, subscriber_id);
                    Ok(())
                }
                Err(InitError::Open(e))
                    if handoff_decision(
                        &e,
                        self.config.ownership.handoff_timeout,
                        Duration::ZERO,
                    ) == HandoffDecision::Wait =>
                {
                    let holder = refusal_holder(&e);
                    let path = self.core_data_dir();
                    otel_warn!(
                        "durable_buffer.engine.handoff_wait",
                        path = %path.display(),
                        holder_owner_id = holder.map(|h| h.owner_id.as_str()),
                        holder_pid = holder.map(|h| h.pid),
                        handoff_timeout_ms = self.config.ownership.handoff_timeout.as_millis() as u64,
                        message = "another pipeline instance in this process holds the storage \
                                   directory; waiting for it to be released"
                    );
                    self.engine_state = EngineState::AwaitingHandoff {
                        since: now,
                        held: None,
                    };
                    Ok(())
                }
                Err(e) => {
                    if let InitError::Open(open_error) = &e {
                        self.log_ownership_refusal(open_error);
                    }
                    let msg = format!("failed to initialize Quiver engine: {}", e);
                    self.engine_state = EngineState::Failed(msg.clone());
                    Err(Error::InternalError { message: msg })
                }
            },
        }
    }

    /// Completes a successful open: seeds the queued gauges and marks the
    /// engine ready.
    fn finish_init(&mut self, engine: Arc<QuiverEngine>, subscriber_id: SubscriberId) {
        // Seed queued gauges immediately so they reflect persisted state
        // before the first telemetry tick.
        self.recompute_metrics(&engine, &subscriber_id);
        self.engine_state = EngineState::Ready {
            engine,
            subscriber_id,
        };
    }

    /// Logs an open that failed because the data directory is owned, naming
    /// the holder when known. Other open errors are reported by the caller.
    fn log_ownership_refusal(&self, error: &QuiverError) {
        if !matches!(error, QuiverError::DataDirOwned { .. }) {
            return;
        }
        let holder = refusal_holder(error);
        let path = self.core_data_dir();
        otel_error!(
            "durable_buffer.engine.ownership_refused",
            path = %path.display(),
            holder_owner_id = holder.map(|h| h.owner_id.as_str()),
            holder_host = holder.map(|h| h.host.as_str()),
            holder_pid = holder.map(|h| h.pid),
            holder_heartbeat_age_ms = holder.map(|h| h.heartbeat_age.as_millis() as u64),
            holder_same_process = holder.map(|h| h.same_process),
            error = %error
        );
    }

    /// Retries opening the engine while waiting for another engine in this
    /// process to release the data directory.
    ///
    /// On success the engine becomes ready and the held data message, if
    /// any, is ingested. While the directory is still held and `now` is
    /// within `ownership.handoff_timeout` of the first refusal, the node keeps
    /// waiting. Otherwise the held message is NACKed, the engine is marked
    /// failed, and an error is returned.
    async fn retry_handoff(
        &mut self,
        now: Instant,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let EngineState::AwaitingHandoff { since, .. } = self.engine_state else {
            return Ok(());
        };
        let waited = now.saturating_duration_since(since);
        let error = match self.open_engine().await {
            Ok((engine, subscriber_id)) => {
                let held = self.take_held();
                self.finish_init(engine, subscriber_id);
                let path = self.core_data_dir();
                otel_info!(
                    "durable_buffer.engine.handoff_complete",
                    path = %path.display(),
                    waited_ms = waited.as_millis() as u64
                );
                return match held {
                    Some(data) => self.handle_data(data, effect_handler).await,
                    None => Ok(()),
                };
            }
            Err(e) => e,
        };

        let decision = match &error {
            InitError::Open(e) => {
                handoff_decision(e, self.config.ownership.handoff_timeout, waited)
            }
            InitError::Setup(_) => HandoffDecision::Fail,
        };
        if decision == HandoffDecision::Wait {
            otel_debug!(
                "durable_buffer.engine.handoff_retry",
                waited_ms = waited.as_millis() as u64,
                error = %error
            );
            return Ok(());
        }

        let msg = if decision == HandoffDecision::TimedOut {
            format!(
                "failed to initialize Quiver engine: the storage directory was not handed over \
                 within ownership.handoff_timeout ({:?}): {error}",
                self.config.ownership.handoff_timeout
            )
        } else {
            format!("failed to initialize Quiver engine: {error}")
        };
        let holder = match &error {
            InitError::Open(e) => refusal_holder(e),
            InitError::Setup(_) => None,
        };
        let path = self.core_data_dir();
        otel_error!(
            "durable_buffer.engine.handoff_failed",
            path = %path.display(),
            timed_out = decision == HandoffDecision::TimedOut,
            waited_ms = waited.as_millis() as u64,
            holder_owner_id = holder.map(|h| h.owner_id.as_str()),
            holder_host = holder.map(|h| h.host.as_str()),
            holder_pid = holder.map(|h| h.pid),
            holder_same_process = holder.map(|h| h.same_process),
            error = %error
        );
        let held = self.take_held();
        self.engine_state = EngineState::Failed(msg.clone());
        self.nack_held(held, &msg, effect_handler).await?;
        Err(Error::InternalError { message: msg })
    }

    /// Holds a data message that arrived while waiting for the handoff.
    ///
    /// Only one message is held because pdata delivery pauses while waiting.
    /// A further message, delivered regardless, is NACKed so it is not lost.
    async fn hold_for_handoff(
        &mut self,
        data: OtapPdata,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let EngineState::AwaitingHandoff { held, .. } = &mut self.engine_state else {
            return self.handle_data(data, effect_handler).await;
        };
        if held.is_none() {
            *held = Some(data);
            return Ok(());
        }
        self.nack_held(
            Some(data),
            "durable buffer is waiting for its storage directory to be handed over",
            effect_handler,
        )
        .await
    }

    /// Stops waiting for the handoff because the node is shutting down, and
    /// NACKs the held data message, if any.
    async fn abandon_handoff(
        &mut self,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let EngineState::AwaitingHandoff { since, .. } = self.engine_state else {
            return Ok(());
        };
        let held = self.take_held();
        let path = self.core_data_dir();
        otel_warn!(
            "durable_buffer.engine.handoff_abandoned",
            path = %path.display(),
            waited_ms = Instant::now().saturating_duration_since(since).as_millis() as u64,
            nacked = held.is_some(),
            message = "shutting down before the storage directory was handed over"
        );
        let msg = "durable buffer shut down before its storage directory was handed over";
        self.engine_state = EngineState::Failed(msg.to_owned());
        self.nack_held(held, msg, effect_handler).await
    }

    /// Takes the data message held while waiting for the handoff.
    fn take_held(&mut self) -> Option<OtapPdata> {
        match &mut self.engine_state {
            EngineState::AwaitingHandoff { held, .. } => held.take(),
            _ => None,
        }
    }

    /// NACKs a data message that was never ingested, counting it as an
    /// ingest failure. The NACK is retryable so upstream can resend it.
    async fn nack_held(
        &mut self,
        held: Option<OtapPdata>,
        reason: &str,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let Some(data) = held else {
            return Ok(());
        };
        self.metrics
            .ingest_for(IngestFailure::Error)
            .failures
            .add(1);
        effect_handler.notify_nack(NackMsg::new(reason, data)).await
    }

    /// Recompute the queued item gauges from the subscriber registry.
    ///
    /// Accumulates item counts from two sources:
    /// 1. **Finalized segments**: Queried via subscriber progress and segment store.
    ///    Uses a per-segment cache to avoid re-computing signal type classification.
    /// 2. **Open (accumulating) segment**: Queried directly from the engine to include
    ///    in-flight items that have not yet been finalized.
    ///
    /// The gauges reflect all ingested but not-yet-ACKed items across both sources.
    ///
    /// *Snapshotting is intentionally non-atomic across these two sources.* During
    /// segment finalization there is a brief window where a just-finalized segment
    /// may appear in neither snapshot, causing a temporary under-count. Gauges
    /// will self-correct on the next telemetry tick.
    ///
    /// Uses a segment-level cache (`segment_cache`) for finalized segments to avoid
    /// per-tick allocations. Segments are immutable after finalization, so the
    /// per-bundle `(item_count, signal_type)` classification is computed once and
    /// reused on subsequent ticks.
    ///
    /// **Hot-path optimisation**: when no bundles in a segment have been resolved
    /// yet (the common case for recently-written segments), the pre-computed
    /// per-signal totals are used in O(1) rather than iterating the per-bundle vec.
    ///
    /// # Lock budget
    ///
    /// 1. `pending_segment_progress` -- subscriber read-lock (brief clone).
    /// 2. Per cache-miss: `bundle_metadata` -- segment-store read-lock.
    /// 3. `open_segment_metrics` -- engine open-segment lock (brief snapshot).
    ///
    /// After snapshots are taken, all iteration is lock-free.
    fn recompute_metrics(&mut self, engine: &QuiverEngine, subscriber_id: &SubscriberId) {
        self.segment_cache_generation = self.segment_cache_generation.wrapping_add(1);
        let current_generation = self.segment_cache_generation;

        // Step 1: snapshot the subscriber's segment progress (brief lock).
        let progress_snapshot = match engine.registry().pending_segment_progress(subscriber_id) {
            Ok(s) => s,
            Err(e) => {
                otel_warn!(
                    "durable_buffer.queued.recompute_error",
                    error = %e,
                    reason = "Queued item gauges may be stale"
                );
                return;
            }
        };

        let mut logs = 0u64;
        let mut metrics = 0u64;
        let mut spans = 0u64;

        // Step 2: iterate finalized segments, populating cache on miss.
        for (&seg_seq, progress) in &progress_snapshot {
            let seg_raw = seg_seq.raw();

            // Populate cache on first access for this segment.
            let summary = match self.segment_cache.entry(seg_raw) {
                Entry::Occupied(entry) => {
                    let cached = entry.into_mut();
                    cached.last_seen_generation = current_generation;
                    &mut cached.summary
                }
                Entry::Vacant(entry) => {
                    let summary = match engine.segment_store().bundle_metadata(seg_seq) {
                        Ok(metadata) => {
                            // Metadata load recovered (or succeeded first try), so
                            // clear any prior warning marker for this segment.
                            let _ = self.metadata_load_warned_segments.remove(&seg_raw);

                            let mut tl = 0u64;
                            let mut tm = 0u64;
                            let mut ts = 0u64;

                            let bundles: Vec<_> = metadata
                                .iter()
                                .map(|entry| {
                                    let sig = entry
                                        .slot_ids
                                        .iter()
                                        .copied()
                                        .find_map(signal_type_from_slot_id);
                                    match sig {
                                        Some(SignalType::Logs) => tl += entry.item_count,
                                        Some(SignalType::Metrics) => tm += entry.item_count,
                                        Some(SignalType::Traces) => ts += entry.item_count,
                                        None => {}
                                    }
                                    (entry.item_count, sig)
                                })
                                .collect();

                            SegmentMetricsSummary {
                                bundles,
                                total_logs: tl,
                                total_metrics: tm,
                                total_spans: ts,
                            }
                        }
                        Err(e) => {
                            // Warn once per segment to avoid per-tick log spam.
                            if self.metadata_load_warned_segments.insert(seg_raw) {
                                otel_warn!(
                                    "durable_buffer.queued.metadata_skip",
                                    segment = seg_raw,
                                    error = %e,
                                    reason = "Segment metadata unavailable; queued counts may under-report"
                                );
                            }
                            continue;
                        }
                    };
                    &mut entry
                        .insert(CachedSegmentMetrics {
                            summary,
                            last_seen_generation: current_generation,
                        })
                        .summary
                }
            };

            let mut seg_logs = 0;
            let mut seg_metrics = 0;
            let mut seg_spans = 0;

            // Fast path: no bundles resolved -> use precomputed totals.
            if progress.resolved_count() == 0 {
                seg_logs = summary.total_logs;
                seg_metrics = summary.total_metrics;
                seg_spans = summary.total_spans;
            } else {
                // Slow path: iterate per-bundle, skipping resolved.
                for (idx, &(item_count, signal)) in summary.bundles.iter().enumerate() {
                    let Ok(bundle_idx) = u32::try_from(idx) else {
                        otel_warn!(
                            "durable_buffer.queued.bundle_index_overflow",
                            segment = seg_raw,
                            bundle_index = idx,
                            reason = "Bundle index exceeds u32 range; skipping in queued gauge recompute"
                        );
                        continue;
                    };

                    if !progress.is_resolved(BundleIndex::new(bundle_idx)) {
                        match signal {
                            Some(SignalType::Logs) => seg_logs += item_count,
                            Some(SignalType::Metrics) => seg_metrics += item_count,
                            Some(SignalType::Traces) => seg_spans += item_count,
                            None => {}
                        }
                    }
                }
            }

            logs += seg_logs;
            metrics += seg_metrics;
            spans += seg_spans;
        }

        // Step 3: add items from the open (accumulating) segment.
        let open_bundles = engine.open_segment_bundle_summaries();
        for bundle in open_bundles {
            if bundle.item_count == 0 {
                continue;
            }

            let signal = bundle
                .slot_ids
                .iter()
                .copied()
                .find_map(signal_type_from_slot_id);

            match signal {
                Some(SignalType::Logs) => logs += bundle.item_count,
                Some(SignalType::Metrics) => metrics += bundle.item_count,
                Some(SignalType::Traces) => spans += bundle.item_count,
                None => {}
            }
        }

        // Step 4: evict cache entries for segments no longer tracked.
        self.segment_cache
            .retain(|seq, _| progress_snapshot.contains_key(&SegmentSeq::new(*seq)));

        let current_loss_snapshot = engine.retention_loss_snapshot();
        let loss_delta = retention_loss_delta(current_loss_snapshot, self.last_loss_snapshot);
        self.last_loss_snapshot = current_loss_snapshot;

        let dropped_reclaimed = self.metrics.reclaimed_for(LossReason::DropOldest);
        dropped_reclaimed
            .segments
            .add(loss_delta.drop_oldest.segments);
        dropped_reclaimed
            .bytes
            .add(loss_delta.drop_oldest.reclaimed_bytes);
        let dropped = self.metrics.loss_for(LossReason::DropOldest);
        dropped.bundles.add(loss_delta.drop_oldest.bundles);
        dropped.bytes.add(loss_delta.drop_oldest.bytes);

        let expired_reclaimed = self.metrics.reclaimed_for(LossReason::Expired);
        expired_reclaimed.segments.add(loss_delta.expired.segments);
        expired_reclaimed
            .bytes
            .add(loss_delta.expired.reclaimed_bytes);
        let expired = self.metrics.loss_for(LossReason::Expired);
        expired.bundles.add(loss_delta.expired.bundles);
        expired.bytes.add(loss_delta.expired.bytes);

        // Classify Quiver's opaque slot shapes in the OTAP layer, where signal
        // semantics are known.
        for (slot_ids, item_count) in engine.drain_dropped_items_by_shape() {
            if let Some(signal) = slot_ids.iter().copied().find_map(signal_type_from_slot_id) {
                self.metrics
                    .loss_items_for(signal, LossReason::DropOldest)
                    .items
                    .add(item_count);
            }
        }

        for (slot_ids, item_count) in engine.drain_expired_items_by_shape() {
            if let Some(signal) = slot_ids.iter().copied().find_map(signal_type_from_slot_id) {
                self.metrics
                    .loss_items_for(signal, LossReason::Expired)
                    .items
                    .add(item_count);
            }
        }

        self.metadata_load_warned_segments
            .retain(|seq| progress_snapshot.contains_key(&SegmentSeq::new(*seq)));
        self.enforce_segment_cache_bound();

        self.metrics
            .items_for_signal(SignalType::Logs)
            .queued
            .set(logs);
        self.metrics
            .items_for_signal(SignalType::Metrics)
            .queued
            .set(metrics);
        self.metrics
            .items_for_signal(SignalType::Traces)
            .queued
            .set(spans);
    }

    /// Initialize the Quiver engine and subscriber.
    ///
    /// Per ARCHITECTURE.md, each core has its own Quiver instance with a
    /// core-specific subdirectory to avoid cross-core locking.
    ///
    /// The configured `retention_size_cap` is divided by `num_cores` so that
    /// the total disk usage across all cores stays within the configured limit.
    async fn init_engine(&self) -> Result<(Arc<QuiverEngine>, SubscriberId), InitError> {
        let path = self.core_data_dir();
        otel_info!(
            "durable_buffer.engine.init",
            path = %path.display(),
            total_size_cap = self.config.size_cap_bytes(),
            per_core_size_cap = self.per_core_size_cap(),
            policy = ?self.config.retention_policy(),
            max_segment_open_duration = ?self.config.max_segment_open_duration,
            max_age = ?self.config.max_age
        );
        self.open_engine().await
    }

    /// This core's data directory under the configured path.
    fn core_data_dir(&self) -> std::path::PathBuf {
        self.config.path.join(format!("core_{}", self.core_id))
    }

    /// This core's equal share of the configured `retention_size_cap`.
    fn per_core_size_cap(&self) -> u64 {
        self.config.size_cap_bytes() / self.num_cores.max(1) as u64
    }

    /// Opens the Quiver engine and registers the subscriber. Called by
    /// [`Self::init_engine`] and again for each retry while waiting for a
    /// handoff.
    async fn open_engine(&self) -> Result<(Arc<QuiverEngine>, SubscriberId), InitError> {
        let policy = self.config.retention_policy();
        let per_core_size_cap = self.per_core_size_cap();

        // Build the QuiverConfig from our DurableBufferConfig (same helper used
        // in new() for early validation, ensuring the two stay in sync).
        let quiver_config =
            Self::build_quiver_config(&self.config).with_data_dir(self.core_data_dir());

        // Create disk budget with per-core share of the total cap.
        // DiskBudget::for_config() reads segment/WAL sizes directly from the
        // QuiverConfig, ensuring the budget matches the engine configuration.
        let budget = Arc::new(
            DiskBudget::for_config(per_core_size_cap, &quiver_config, policy).map_err(|e| {
                InitError::Setup(Error::InternalError {
                    message: format!("invalid budget configuration: {e}"),
                })
            })?,
        );

        // Build the Quiver engine
        let engine = QuiverEngine::builder(quiver_config)
            .with_budget(budget)
            .with_wal_item_counter(Arc::new(recover_item_count))
            .with_wal_byte_counter(Arc::new(recover_byte_count))
            .build()
            .await
            .map_err(InitError::Open)?;

        // Register subscriber
        let subscriber_id = SubscriberId::new(SUBSCRIBER_ID).map_err(|e| {
            InitError::Setup(Error::InternalError {
                message: format!("invalid subscriber ID: {}", e),
            })
        })?;

        engine
            .register_subscriber(subscriber_id.clone())
            .map_err(|e| {
                InitError::Setup(Error::InternalError {
                    message: format!("failed to register subscriber: {}", e),
                })
            })?;

        engine.activate_subscriber(&subscriber_id).map_err(|e| {
            InitError::Setup(Error::InternalError {
                message: format!("failed to activate subscriber: {}", e),
            })
        })?;

        otel_info!(
            "durable_buffer.engine.ready",
            path = %self.config.path.display(),
            subscriber_id = %subscriber_id.as_str()
        );
        Ok((engine, subscriber_id))
    }

    /// Get the engine and subscriber, assuming they're initialized.
    fn engine(&self) -> Result<(&Arc<QuiverEngine>, &SubscriberId), Error> {
        match &self.engine_state {
            EngineState::Ready {
                engine,
                subscriber_id,
            } => Ok((engine, subscriber_id)),
            EngineState::Failed(msg) => Err(Error::InternalError {
                message: msg.clone(),
            }),
            EngineState::Uninitialized => Err(Error::InternalError {
                message: "engine not initialized".to_string(),
            }),
            EngineState::AwaitingHandoff { .. } => Err(Error::InternalError {
                message: "waiting for the storage directory to be handed over".to_string(),
            }),
        }
    }

    /// Handle incoming data by ingesting to Quiver storage.
    ///
    /// # Data Flow
    ///
    /// 1. Data is written to Quiver's durable storage (WAL -> segment finalization)
    /// 2. With `forward_after_ingest`, the in-memory payload is sent downstream
    ///    right after the WAL append, if `max_in_flight` and the downstream
    ///    channel allow it
    /// 3. Upstream is ACK'd after successful durable write
    /// 4. Data becomes visible to subscribers after segment finalization
    /// 5. Timer tick polls for finalized bundles and forwards those not already
    ///    forwarded in step 2
    ///
    /// Segment finalization occurs when:
    /// - Segment reaches target size (default 32MB), or
    /// - Segment has been open for max_open_duration (default 5 seconds), or
    /// - A subsequent ingest triggers the time/size check
    ///
    /// This design ensures proper at-least-once delivery semantics with
    /// ACK tracking through the Quiver subscriber model.
    async fn handle_data(
        &mut self,
        data: OtapPdata,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let signal_type = data.signal_type();

        // Get the engine reference - NACK with original payload if unavailable
        let engine = match self.engine() {
            Ok((engine, _)) => Arc::clone(engine),
            Err(e) => {
                self.metrics
                    .ingest_for(IngestFailure::Error)
                    .failures
                    .add(1);
                otel_error!("durable_buffer.engine.unavailable", error = %e);
                effect_handler
                    .notify_nack(NackMsg::new(format!("engine not available: {}", e), data))
                    .await?;
                return Ok(());
            }
        };

        // Ingest based on payload type and configuration.
        // Adapters preserve the original payload via into_inner() for NACK on failure.
        // Pluggable Arrow record sets are stored unchanged with a representation
        // manifest. Preserve the existing durable OTLP format in pass-through mode.
        // Other inputs use recoverable native conversion before Arrow storage.
        // The adapter returns the payload on both success and failure, so it
        // can be forwarded early or returned to upstream without a copy.
        let (context, ingest_result, payload, item_count) =
            if data.payload_ref().format().is_pluggable_arrow() {
                let (context, payload) = data.into_parts();
                let records = payload
                    .into_pdata_arrow()
                    .expect("pluggable Arrow format must contain a record set");
                let item_count = records.num_items() as u64;
                match PdataArrowBundleAdapter::new(records) {
                    Ok(adapter) => {
                        let result = engine.ingest_with_ref(&adapter).await;
                        let payload = OtapPayload::from(adapter.into_inner());
                        (context, result, payload, item_count)
                    }
                    Err((error, records)) => {
                        // The record set exceeds what Quiver can persist; retrying
                        // the same payload can never succeed.
                        self.metrics
                            .ingest_for(IngestFailure::Error)
                            .failures
                            .add(1);
                        otel_error!("durable_buffer.representation.unsupported", error = %error);
                        effect_handler
                            .notify_nack(NackMsg::new_permanent(
                                error.to_string(),
                                OtapPdata::new(context, OtapPayload::from(records)),
                            ))
                            .await?;
                        return Ok(());
                    }
                }
            } else if matches!(self.config.otlp_handling, OtlpHandling::PassThrough)
                && data.payload_ref().encoding()
                    == Some(&otel_arrow_dfe_pdata_codec::PdataEncoding::OTLP)
            {
                let (context, payload) = data.into_parts();
                let otlp_bytes = otel_arrow_dfe_pdata::OtlpProtoBytes::new_from_bytes(
                    signal_type,
                    payload
                        .into_encoded_bytes()
                        .expect("OTLP format must contain encoded bytes"),
                );
                match OtlpBytesAdapter::new(otlp_bytes) {
                    Ok(adapter) => {
                        let num_items = adapter.cached_item_count();
                        let result = engine.ingest_with_ref(&adapter).await;
                        let payload = OtapPayload::from(adapter.into_inner());
                        (context, result, payload, num_items)
                    }
                    Err((error, original_bytes)) => {
                        self.metrics
                            .ingest_for(IngestFailure::Error)
                            .failures
                            .add(1);
                        otel_error!("durable_buffer.otlp.adapter_failed", error = %error);
                        effect_handler
                            .notify_nack(NackMsg::new(
                                format!("OTLP adapter creation failed: {error}"),
                                OtapPdata::new(context, OtapPayload::from(original_bytes)),
                            ))
                            .await?;
                        return Ok(());
                    }
                }
            } else {
                let arrow_pdata = match effect_handler.materialize_otap(data).await {
                    Ok(arrow_pdata) => arrow_pdata,
                    Err(error) => {
                        self.metrics
                            .ingest_for(IngestFailure::Error)
                            .failures
                            .add(1);
                        let (error, pdata) = error.into_parts();
                        effect_handler
                            .notify_nack(NackMsg::new_permanent(error.to_string(), pdata))
                            .await?;
                        return Ok(());
                    }
                };
                let (context, records) = arrow_pdata.into_parts();
                let item_count = records.num_items() as u64;
                let adapter = OtapRecordBundleAdapter::new(records);
                let result = engine.ingest_with_ref(&adapter).await;
                let payload = OtapPayload::from(adapter.into_inner());
                (context, result, payload, item_count)
            };

        // Handle ingest result
        match ingest_result {
            Ok(bundle_ref) => {
                self.metrics
                    .items_for_signal(signal_type)
                    .consumed
                    .add(item_count);

                if self.config.forward_after_ingest {
                    self.try_forward_after_ingest(
                        bundle_ref,
                        payload,
                        item_count,
                        signal_type,
                        effect_handler,
                    );
                }

                // ACK upstream after successful durable write, or hold the ACK
                // until a WAL sync covers it. Bundles not forwarded above are
                // forwarded downstream via timer tick after segment
                // finalization.
                let ack_pdata = OtapPdata::new(context, OtapPayload::empty(signal_type));
                self.ack_ingested(ack_pdata, effect_handler).await
            }
            Err(e) => {
                if e.is_fenced() {
                    self.metrics
                        .ingest_for(IngestFailure::Error)
                        .failures
                        .add(1);
                    self.note_fenced(&e);
                } else if e.is_at_capacity() {
                    // Normal backpressure: soft cap exceeded. Count separately
                    // and rate-limit logging to avoid flooding.
                    self.metrics
                        .ingest_for(IngestFailure::Backpressure)
                        .failures
                        .add(1);
                    let now = Instant::now();
                    let should_log = self
                        .last_backpressure_warn
                        .is_none_or(|last| now.duration_since(last) >= WARN_RATE_LIMIT);
                    if should_log {
                        self.last_backpressure_warn = Some(now);
                        otel_warn!("durable_buffer.ingest.backpressure", error = %e);
                    }
                } else {
                    self.metrics
                        .ingest_for(IngestFailure::Error)
                        .failures
                        .add(1);
                    otel_error!("durable_buffer.ingest.failed", error = %e);
                }

                // Preserve original payload so upstream can retry
                let nack_pdata = OtapPdata::new(context, payload);
                effect_handler
                    .notify_nack(NackMsg::new(
                        format!("durable buffer failed: {}", e),
                        nack_pdata,
                    ))
                    .await?;
                Ok(())
            }
        }
    }

    /// Records that the engine reported that another instance claimed this
    /// core's data directory, and logs it the first time.
    ///
    /// The error names the new owner when the engine read it from the
    /// ownership marker. Data whose ingest or WAL sync failed this way is
    /// NACKed (retryable) and never ACKed, so upstream keeps it.
    fn note_fenced(&mut self, error: &dyn std::fmt::Display) {
        if self.fenced {
            return;
        }
        self.fenced = true;
        otel_error!(
            "durable_buffer.data_dir.fenced",
            core_id = self.core_id as u64,
            error = %error,
            message = "another instance claimed this data directory; incoming data is NACKed \
                       and stored bundles are left to the new owner"
        );
    }

    /// Returns true when upstream acks are held until a WAL sync covers them.
    ///
    /// With `wal_flush_interval: 0s` the WAL syncs on every append, so the ack
    /// can be sent immediately in either mode.
    fn holds_acks_until_wal_sync(&self) -> bool {
        self.config.upstream_ack == UpstreamAck::AfterWalSync
            && !self.config.wal_flush_interval.is_zero()
    }

    /// Acknowledges an ingested bundle to upstream, or holds the ack until a
    /// WAL sync covers the bundle when `upstream_ack` is `after_wal_sync`.
    ///
    /// The first held ack arms a deadline wakeup at `wal_flush_interval`, so
    /// held acks are released even if no further data arrives. When the held
    /// list reaches its capacity, it is synced and released right away. The
    /// current ack is held first, so it shares the outcome of the sync that
    /// covers its bundle.
    async fn ack_ingested(
        &mut self,
        ack_pdata: OtapPdata,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        if !self.holds_acks_until_wal_sync() {
            return effect_handler.notify_ack(AckMsg::new(ack_pdata)).await;
        }

        let first_held = self.wal_sync_acks.hold(ack_pdata);
        if self.wal_sync_acks.is_full() {
            return self.sync_wal_and_release(effect_handler).await;
        }

        if first_held {
            let deadline = Instant::now() + self.config.wal_flush_interval;
            match effect_handler.set_wakeup(WAL_SYNC_WAKEUP_SLOT, deadline) {
                Ok(outcome) => self.wal_sync_acks.set_armed(outcome.revision()),
                Err(error) => {
                    // Without a deadline the held ack could wait indefinitely,
                    // so sync and release right away.
                    otel_warn!("durable_buffer.wal_sync.arm_failed", error = ?error);
                    self.sync_wal_and_release(effect_handler).await?;
                }
            }
        }
        Ok(())
    }

    /// Syncs the WAL and releases every held upstream ack.
    ///
    /// Every held ack belongs to a bundle whose WAL append completed before
    /// this call, so a successful sync covers all of them. On failure, every
    /// held bundle is NACKed (retryable) so upstream can resend it. The data
    /// may already be in the WAL, so a resend can duplicate it, which keeps
    /// at-least-once delivery.
    ///
    /// Every held ack is answered even if sending one of them fails; the first
    /// send error is returned afterwards.
    async fn sync_wal_and_release(
        &mut self,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        if self.wal_sync_acks.is_empty() {
            return Ok(());
        }
        if self.wal_sync_acks.disarm() {
            let _ = effect_handler.cancel_wakeup(WAL_SYNC_WAKEUP_SLOT);
        }

        // The barrier error carries whether the engine was fenced, so a
        // fenced sync is reported through `note_fenced`.
        let engine = self
            .engine()
            .map(|(engine, _)| Arc::clone(engine))
            .map_err(|e| (e.to_string(), false));
        let result = self
            .wal_sync_acks
            .sync_and_take(|| async move {
                engine?
                    .sync_wal()
                    .await
                    .map_err(|e| (e.to_string(), e.is_fenced()))
            })
            .await;

        let mut first_error = None;
        match result {
            Ok(released) => {
                self.metrics.operational_metrics.wal_syncs.add(1);
                for pdata in released {
                    if let Err(e) = effect_handler.notify_ack(AckMsg::new(pdata)).await {
                        let _ = first_error.get_or_insert(e);
                    }
                }
            }
            Err(((error, fenced), held)) => {
                self.metrics.operational_metrics.wal_sync_failures.add(1);
                if fenced {
                    self.note_fenced(&error);
                } else {
                    otel_error!(
                        "durable_buffer.wal_sync.failed",
                        error = %error,
                        nacked_bundles = held.len() as u64
                    );
                }
                for pdata in held {
                    let nack =
                        NackMsg::new(format!("durable buffer WAL sync failed: {error}"), pdata);
                    if let Err(e) = effect_handler.notify_nack(nack).await {
                        let _ = first_error.get_or_insert(e);
                    }
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Handles the WAL sync deadline wakeup by syncing and releasing held acks.
    async fn handle_wal_sync_wakeup(
        &mut self,
        slot: WakeupSlot,
        revision: WakeupRevision,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        if !self.wal_sync_acks.accept_wakeup(slot, revision) {
            otel_debug!(
                "durable_buffer.wal_sync.stale_wakeup",
                wakeup_revision = revision
            );
            return Ok(());
        }
        self.sync_wal_and_release(effect_handler).await
    }

    /// Handle timer tick by polling Quiver for available bundles.
    ///
    /// This is the primary path for forwarding data downstream. Bundles become
    /// visible here after segment finalization (triggered by size or time thresholds
    /// during ingest). Bundles already forwarded after ingest are not sent
    /// again; see `try_process_bundle_handle_with_retry_count`.
    ///
    /// To prevent blocking indefinitely when downstream applies backpressure,
    /// this method uses non-blocking sends. When the downstream channel is full,
    /// the bundle is deferred and processing stops, allowing the processor to
    /// receive new incoming data. Deferred bundles will be retried on the next tick.
    async fn handle_timer_tick(
        &mut self,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        // A fenced engine refuses every write, and the instance that claimed
        // the directory delivers the stored bundles.
        if self.fenced {
            return Ok(());
        }

        // Flush to finalize any segments that have exceeded their time threshold.
        // Segment finalization only happens during ingest() calls, so if there's
        // a gap between ingests, we need to explicitly flush to make bundles available.
        {
            let (engine, _) = self.engine()?;
            if let Err(e) = engine.flush().await {
                if e.is_fenced() {
                    self.note_fenced(&e);
                    return Ok(());
                }
                self.metrics.operational_metrics.flush_failures.add(1);
                // Rate-limit flush warnings since the timer tick fires every
                // poll_interval (~100ms).
                let now = Instant::now();
                let should_log = self
                    .last_flush_warn
                    .is_none_or(|last| now.duration_since(last) >= WARN_RATE_LIMIT);
                if should_log {
                    self.last_flush_warn = Some(now);
                    otel_warn!("durable_buffer.flush.failed", error = %e);
                }
            }
        }

        // Resolve early-forwarded bundles that downstream answered before
        // their segment was visible.
        self.sweep_early_outcomes()?;

        // Time-budgeted draining: spend at most 50% of poll_interval processing bundles.
        // This ensures we yield back promptly to handle incoming data, which is typically
        // higher priority than draining historical data (e.g., during recovery from a
        // network outage, new data reflects current system state).
        let drain_budget = self.config.poll_interval / 2;
        let deadline = Instant::now() + drain_budget;
        let mut bundles_processed = 0usize;

        // Track the first skipped bundle to detect when we've cycled through all
        // available bundles without making progress (all are in-flight or scheduled).
        let mut first_skipped: Option<(u64, u32)> = None;

        loop {
            // Check time budget before each bundle to ensure we yield back for new data
            if Instant::now() >= deadline {
                otel_debug!(
                    "durable_buffer.drain.budget_exhausted",
                    bundles_processed = bundles_processed,
                    budget_ms = drain_budget.as_millis()
                );
                break;
            }

            // Check max_in_flight limit to prevent thundering herd
            if !self.can_send_more() {
                otel_debug!(
                    "durable_buffer.drain.at_capacity",
                    bundles_processed = bundles_processed,
                    in_flight = self.pending_bundles.len(),
                    max_in_flight = self.config.max_in_flight
                );
                break;
            }

            // Get engine inside loop to avoid borrow conflict with self in process_bundle_handle
            let poll_result = {
                let (engine, subscriber_id) = self.engine()?;
                engine.poll_next_bundle(subscriber_id)
            };

            match poll_result {
                Ok(Some(handle)) => {
                    let bundle_key = (
                        handle.bundle_ref().segment_seq.raw(),
                        handle.bundle_ref().bundle_index.raw(),
                    );
                    match self.try_process_bundle_handle(handle, effect_handler) {
                        ProcessBundleResult::Sent => {
                            bundles_processed += 1;
                            first_skipped = None; // Reset on progress
                        }
                        ProcessBundleResult::Resolved => {
                            // Already forwarded right after ingest; nothing sent.
                            first_skipped = None;
                        }
                        ProcessBundleResult::Skipped => {
                            // Bundle was skipped (in-flight or scheduled for retry).
                            // Check if we've cycled back to the first skipped bundle.
                            if first_skipped == Some(bundle_key) {
                                // We've seen this bundle before - all available bundles
                                // are blocked. Break to avoid busy-looping.
                                otel_debug!(
                                    "durable_buffer.drain.all_blocked",
                                    bundles_processed = bundles_processed,
                                    in_flight = self.pending_bundles.len(),
                                    retry_scheduled = self.deferred_retry_state.scheduled_len()
                                );
                                break;
                            }
                            if first_skipped.is_none() {
                                first_skipped = Some(bundle_key);
                            }
                        }
                        ProcessBundleResult::Backpressure => {
                            // Downstream channel is full, stop processing and let the
                            // processor handle other messages (including incoming data)
                            otel_debug!(
                                "durable_buffer.drain.backpressure",
                                bundles_processed = bundles_processed
                            );
                            break;
                        }
                        ProcessBundleResult::Error(e) => {
                            return Err(e);
                        }
                    }
                }
                Ok(None) => {
                    // No more bundles available
                    break;
                }
                Err(e) => {
                    self.metrics.operational_metrics.read_errors.add(1);
                    otel_error!("durable_buffer.poll.failed", error = %e);
                    break;
                }
            }
        }

        // Always run maintenance at the end of each tick to:
        // 1. Refresh the data directory ownership heartbeat
        // 2. Flush dirty subscriber progress to disk (for crash recovery)
        // 3. Clean up fully-processed segments to reclaim disk space
        // This is essential for steady-state operation - without periodic maintenance,
        // segments accumulate and eventually fill the storage budget.
        {
            let (engine, _) = self.engine()?;
            if let Err(e) = engine.maintain().await {
                if e.is_fenced() {
                    self.note_fenced(&e);
                } else {
                    otel_warn!("durable_buffer.maintenance.failed", error = %e);
                }
            }
        }

        Ok(())
    }

    /// Process a bundle handle using non-blocking send.
    ///
    /// Returns `ProcessBundleResult::Sent` if the bundle was successfully sent downstream.
    /// Returns `ProcessBundleResult::Skipped` if the bundle was already in-flight.
    /// Returns `ProcessBundleResult::Backpressure` if the downstream channel is full.
    /// Returns `ProcessBundleResult::Error` for other errors.
    fn try_process_bundle_handle(
        &mut self,
        handle: QuiverBundleHandle,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> ProcessBundleResult {
        self.try_process_bundle_handle_with_retry_count(handle, 0, effect_handler)
    }

    /// Process a bundle handle with a specific retry count.
    ///
    /// This is the core send logic, used by both initial sends (retry_count=0)
    /// and retry attempts (retry_count > 0).
    fn try_process_bundle_handle_with_retry_count(
        &mut self,
        handle: QuiverBundleHandle,
        retry_count: u32,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> ProcessBundleResult {
        let bundle_ref = handle.bundle_ref();
        let key = (bundle_ref.segment_seq.raw(), bundle_ref.bundle_index.raw());

        // Skip if this bundle is already in-flight (waiting for ACK/NACK).
        // This should rarely happen since we keep the handle claimed, but can occur
        // if the bundle was previously sent and is still awaiting response.
        if let Some(pending) = self.pending_bundles.get_mut(&key) {
            if pending.handle.is_none() {
                // Forwarded right after ingest and still in flight. Keep the
                // claim until downstream answers instead of sending it again.
                pending.handle = Some(handle);
                return ProcessBundleResult::Resolved;
            }
            // Bundle is in-flight. Dropping the handle will trigger implicit defer,
            // but since we already hold the original handle, this one is a duplicate claim.
            // This shouldn't happen in normal operation since we keep bundles claimed.
            otel_warn!(
                "durable_buffer.bundle.duplicate",
                segment_seq = bundle_ref.segment_seq.raw(),
                bundle_index = bundle_ref.bundle_index.raw()
            );
            drop(handle); // Implicit defer
            return ProcessBundleResult::Skipped;
        }

        // Forwarded right after ingest and answered by downstream before the
        // segment was visible. Apply the recorded outcome.
        if !self.early_outcomes.is_empty()
            && let Some(outcome) = self.early_outcomes.remove(&key)
        {
            apply_early_outcome(handle, outcome);
            return ProcessBundleResult::Resolved;
        }

        // Skip if this bundle is scheduled for retry (waiting for backoff).
        // This enforces the exponential backoff - poll_next_bundle() returns
        // deferred bundles immediately, but we should wait for the retry delay.
        if self.deferred_retry_state.is_deferred_key(key) {
            // Bundle is waiting for backoff. Release the claim; it will be
            // re-claimed when the single durable-buffer retry wakeup resumes it.
            drop(handle); // Implicit defer
            return ProcessBundleResult::Skipped;
        }

        // Convert the reconstructed bundle to OtapPdata
        match convert_bundle_to_pdata(handle.data()) {
            Ok(pdata) => {
                // Use the manifest-derived item count carried by the handle.
                // This avoids re-scanning OTLP bytes on the drain path.
                let item_count = handle.item_count();
                let signal_type = pdata.signal_type();

                match self.send_bundle(pdata, bundle_ref, item_count, retry_count, effect_handler) {
                    ProcessBundleResult::Sent => {
                        // Store the handle to keep the bundle claimed until ACK/NACK.
                        // The bundle will not be redelivered while we hold the handle.
                        let _ = self.pending_bundles.insert(
                            key,
                            PendingBundle {
                                handle: Some(handle),
                                retry_count,
                                item_count,
                                signal_type,
                            },
                        );
                        self.metrics
                            .operational_metrics
                            .in_flight
                            .set(self.pending_bundles.len() as u64);
                        ProcessBundleResult::Sent
                    }
                    other => {
                        // Channel full (Backpressure) or a fatal send error.
                        // Dropping the handle triggers implicit defer: the
                        // bundle stays in Quiver and is eligible for
                        // redelivery on the next poll.
                        drop(handle);
                        other
                    }
                }
            }
            Err(e) => {
                self.metrics.operational_metrics.read_errors.add(1);
                otel_error!("durable_buffer.bundle.conversion_failed", error = %e);
                // Reject the bundle since we can't process it
                handle.reject();
                // Conversion error is not counted as "sent" but also shouldn't stop processing
                ProcessBundleResult::Skipped
            }
        }
    }

    /// Subscribe to the ACK/NACK of a bundle and send it downstream without
    /// blocking.
    ///
    /// Shared by bundles read back from a finalized segment and bundles
    /// forwarded right after ingest, so both carry the same calldata and are
    /// counted the same way. `pdata` must carry a fresh context: its ACK/NACK
    /// is routed back to this processor only.
    ///
    /// Returns `Sent`, `Backpressure` when the channel is full, or `Error`.
    fn send_bundle(
        &mut self,
        mut pdata: OtapPdata,
        bundle_ref: BundleRef,
        item_count: u64,
        retry_count: u32,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> ProcessBundleResult {
        let signal_type = pdata.signal_type();
        effect_handler.subscribe_to(
            Interests::ACKS | Interests::NACKS,
            encode_bundle_ref(bundle_ref),
            &mut pdata,
        );
        match effect_handler.try_send_message(pdata) {
            Ok(()) => {}
            Err(TypedError::ChannelSendError(SendError::Full(_pdata))) => {
                return ProcessBundleResult::Backpressure;
            }
            Err(TypedError::ChannelSendError(SendError::Closed(_pdata))) => {
                return ProcessBundleResult::Error(Error::ChannelSendError {
                    error: "downstream channel closed".to_string(),
                    closed: true,
                });
            }
            // Configuration error (no default port)
            Err(TypedError::Error(e)) => return ProcessBundleResult::Error(e),
            Err(e) => return ProcessBundleResult::Error(e.into()),
        }

        self.metrics
            .items_for_signal(signal_type)
            .produced
            .add(item_count);
        otel_debug!(
            "durable_buffer.bundle.forwarded",
            segment_seq = bundle_ref.segment_seq.raw(),
            bundle_index = bundle_ref.bundle_index.raw(),
            retry_count = retry_count
        );
        ProcessBundleResult::Sent
    }

    /// Forward a bundle downstream right after it was ingested, using the
    /// payload still in memory.
    ///
    /// Only forwards when `max_in_flight` allows it and the downstream channel
    /// accepts the bundle without waiting. Otherwise the payload is dropped
    /// and the bundle is delivered from its finalized segment as usual.
    fn try_forward_after_ingest(
        &mut self,
        bundle_ref: BundleRef,
        payload: OtapPayload,
        item_count: u64,
        signal_type: SignalType,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) {
        if !self.can_send_more() {
            return;
        }

        let pdata = OtapPdata::new(Context::default(), payload);
        match self.send_bundle(pdata, bundle_ref, item_count, 0, effect_handler) {
            ProcessBundleResult::Sent => {
                // No handle yet: the poll path attaches it once the segment is
                // finalized, instead of sending the bundle a second time.
                let _ = self.pending_bundles.insert(
                    (bundle_ref.segment_seq.raw(), bundle_ref.bundle_index.raw()),
                    PendingBundle {
                        handle: None,
                        retry_count: 0,
                        item_count,
                        signal_type,
                    },
                );
                self.metrics
                    .operational_metrics
                    .in_flight
                    .set(self.pending_bundles.len() as u64);
                self.metrics.operational_metrics.early_forwards.add(1);
            }
            ProcessBundleResult::Backpressure => {
                otel_debug!(
                    "durable_buffer.forward_after_ingest.skipped",
                    segment_seq = bundle_ref.segment_seq.raw(),
                    bundle_index = bundle_ref.bundle_index.raw(),
                    reason = "downstream channel full"
                );
            }
            ProcessBundleResult::Error(e) => {
                otel_debug!(
                    "durable_buffer.forward_after_ingest.skipped",
                    segment_seq = bundle_ref.segment_seq.raw(),
                    bundle_index = bundle_ref.bundle_index.raw(),
                    reason = %e
                );
            }
            ProcessBundleResult::Skipped | ProcessBundleResult::Resolved => {}
        }
    }

    /// Apply the downstream outcome of an early-forwarded bundle that has no
    /// handle yet.
    ///
    /// If the segment is visible the bundle is claimed and resolved now. If
    /// it is still being written, the outcome is kept in `early_outcomes` and
    /// applied once the segment is visible. If finalization failed, there is
    /// nothing to resolve: the data is only recoverable from the WAL on
    /// restart, the same as without early forwarding.
    fn resolve_early(&mut self, bundle_ref: BundleRef, outcome: EarlyOutcome) -> Result<(), Error> {
        let pending = {
            let (engine, subscriber_id) = self.engine()?;
            match engine.segment_visibility(bundle_ref.segment_seq) {
                SegmentVisibility::Visible => {
                    match engine.claim_bundle(subscriber_id, bundle_ref) {
                        Ok(handle) => apply_early_outcome(handle, outcome),
                        Err(e) => otel_debug!(
                            "durable_buffer.early_outcome.claim_failed",
                            segment_seq = bundle_ref.segment_seq.raw(),
                            bundle_index = bundle_ref.bundle_index.raw(),
                            error = %e
                        ),
                    }
                    false
                }
                SegmentVisibility::Pending => true,
                SegmentVisibility::Unavailable => {
                    otel_debug!(
                        "durable_buffer.early_outcome.segment_unavailable",
                        segment_seq = bundle_ref.segment_seq.raw(),
                        bundle_index = bundle_ref.bundle_index.raw()
                    );
                    false
                }
            }
        };
        if pending {
            let _ = self.early_outcomes.insert(
                (bundle_ref.segment_seq.raw(), bundle_ref.bundle_index.raw()),
                outcome,
            );
        }
        Ok(())
    }

    /// Apply recorded early outcomes whose segment has become visible, and
    /// drop those whose segment will never become visible.
    fn sweep_early_outcomes(&mut self) -> Result<(), Error> {
        if self.early_outcomes.is_empty() {
            return Ok(());
        }
        let (engine, subscriber_id) = {
            let (engine, subscriber_id) = self.engine()?;
            (Arc::clone(engine), subscriber_id.clone())
        };
        self.early_outcomes.retain(|&(seq, index), outcome| {
            let bundle_ref = BundleRef {
                segment_seq: SegmentSeq::new(seq),
                bundle_index: BundleIndex::new(index),
            };
            match engine.segment_visibility(bundle_ref.segment_seq) {
                SegmentVisibility::Pending => true,
                SegmentVisibility::Visible => {
                    match engine.claim_bundle(&subscriber_id, bundle_ref) {
                        Ok(handle) => apply_early_outcome(handle, *outcome),
                        Err(e) => otel_debug!(
                            "durable_buffer.early_outcome.claim_failed",
                            segment_seq = seq,
                            bundle_index = index,
                            error = %e
                        ),
                    }
                    false
                }
                SegmentVisibility::Unavailable => {
                    otel_debug!(
                        "durable_buffer.early_outcome.segment_unavailable",
                        segment_seq = seq,
                        bundle_index = index
                    );
                    false
                }
            }
        });
        Ok(())
    }

    /// Handle ACK from downstream.
    async fn handle_ack(
        &mut self,
        ack: AckMsg<OtapPdata>,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        // Extract BundleRef from calldata
        let Some(bundle_ref) = decode_bundle_ref(&ack.unwind.route.calldata) else {
            // Invalid calldata, just forward the ACK upstream
            return effect_handler.notify_ack(ack).await;
        };

        let key = (bundle_ref.segment_seq.raw(), bundle_ref.bundle_index.raw());

        // Remove from pending and acknowledge in Quiver using the stored handle
        if let Some(pending) = self.pending_bundles.remove(&key) {
            self.metrics
                .bundles_for(BundleOutcome::Acked)
                .resolved
                .add(1);
            self.metrics
                .operational_metrics
                .in_flight
                .set(self.pending_bundles.len() as u64);
            otel_debug!(
                "durable_buffer.bundle.acked",
                segment_seq = bundle_ref.segment_seq.raw(),
                bundle_index = bundle_ref.bundle_index.raw()
            );
            match pending.handle {
                Some(handle) => handle.ack(),
                None => self.resolve_early(bundle_ref, EarlyOutcome::Acked)?,
            }
        } else {
            otel_warn!(
                "durable_buffer.ack.unknown_bundle",
                segment_seq = bundle_ref.segment_seq.raw(),
                bundle_index = bundle_ref.bundle_index.raw()
            );
        }

        Ok(())
    }

    /// Handle NACK from downstream.
    ///
    /// For permanent NACKs (e.g., malformed data that will never succeed), the bundle
    /// is rejected immediately without retry.
    ///
    /// For transient NACKs, defers the bundle locally with exponential backoff
    /// and ensures the single durable-buffer wakeup tracks the earliest retry.
    async fn handle_nack(
        &mut self,
        nack: NackMsg<OtapPdata>,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        // Extract BundleRef from calldata
        let Some(bundle_ref) = decode_bundle_ref(&nack.unwind.route.calldata) else {
            // Invalid calldata, just forward the NACK upstream
            return effect_handler.notify_nack(nack).await;
        };

        let key = (bundle_ref.segment_seq.raw(), bundle_ref.bundle_index.raw());

        // Handle based on whether this is a permanent or transient failure
        if let Some(pending) = self.pending_bundles.remove(&key) {
            self.metrics
                .operational_metrics
                .in_flight
                .set(self.pending_bundles.len() as u64);

            // Permanent failures should not be retried - reject the bundle immediately
            if nack.permanent {
                self.metrics
                    .items_for_signal(pending.signal_type)
                    .rejected
                    .add(pending.item_count);
                self.metrics
                    .bundles_for(BundleOutcome::PermanentlyRejected)
                    .resolved
                    .add(1);

                otel_warn!(
                    "durable_buffer.bundle.rejected_permanent",
                    segment_seq = bundle_ref.segment_seq.raw(),
                    bundle_index = bundle_ref.bundle_index.raw(),
                    reason = %nack.reason
                );

                // Reject the bundle in Quiver (marks as permanently failed)
                return match pending.handle {
                    Some(handle) => {
                        handle.reject();
                        Ok(())
                    }
                    None => self.resolve_early(bundle_ref, EarlyOutcome::Rejected),
                };
            }

            // Transient failure - schedule retry with exponential backoff
            let retry_count = pending.retry_count + 1;

            self.metrics
                .items_for_signal(pending.signal_type)
                .requeued
                .add(pending.item_count);
            self.metrics
                .bundles_for(BundleOutcome::Deferred)
                .resolved
                .add(1);

            // Calculate backoff delay with jitter
            let backoff = self.calculate_backoff(retry_count);

            otel_debug!(
                "durable_buffer.bundle.nacked",
                segment_seq = bundle_ref.segment_seq.raw(),
                bundle_index = bundle_ref.bundle_index.raw(),
                retry_count = retry_count,
                backoff_ms = backoff.as_millis(),
                reason = %nack.reason
            );

            // Drop the handle to trigger implicit defer, releasing the claim.
            // The bundle stays in Quiver but is now available for re-claiming.
            // An early-forwarded bundle may have no handle yet; the retry
            // waits for its segment to be finalized (see `resume_retry`).
            // Note: No race with TimerTick here - df_engine processes messages
            // sequentially, so schedule_retry completes before any TimerTick runs.
            drop(pending.handle);

            // Schedule the retry
            if self.deferred_retry_state.schedule_after(
                bundle_ref,
                retry_count,
                backoff,
                effect_handler,
            ) {
                self.metrics.operational_metrics.retries_scheduled.add(1);
            } else {
                otel_warn!(
                    "durable_buffer.retry.schedule_failed",
                    segment_seq = bundle_ref.segment_seq.raw(),
                    bundle_index = bundle_ref.bundle_index.raw()
                );
            }
        } else {
            otel_warn!(
                "durable_buffer.nack.unknown_bundle",
                segment_seq = bundle_ref.segment_seq.raw(),
                bundle_index = bundle_ref.bundle_index.raw()
            );
        }

        Ok(())
    }

    /// Handle a retry wakeup.
    ///
    /// Re-claims the bundle from Quiver and attempts redelivery downstream.
    async fn handle_retry_wakeup(
        &mut self,
        slot: WakeupSlot,
        revision: WakeupRevision,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        if !self.deferred_retry_state.accept_wakeup(slot, revision) {
            otel_warn!(
                "durable_buffer.retry.unknown_wakeup",
                wakeup_slot = slot.0.to_string(),
                wakeup_revision = revision
            );
            return Ok(());
        }

        let mut rearm_no_earlier_than = None;
        loop {
            let now = Instant::now();
            let Some(retry) = self.deferred_retry_state.take_due_retry(now) else {
                break;
            };

            match self.resume_retry(retry.bundle_ref(), retry.retry_count(), effect_handler)? {
                RetryResumeOutcome::Sent
                | RetryResumeOutcome::Skipped
                | RetryResumeOutcome::AwaitingSegment => {}
                RetryResumeOutcome::Deferred => {
                    rearm_no_earlier_than = Some(now + self.config.poll_interval);
                    break;
                }
            }
        }

        if !self
            .deferred_retry_state
            .rearm_after_processing(effect_handler, rearm_no_earlier_than)
        {
            otel_warn!("durable_buffer.retry.rearm_failed");
        }

        Ok(())
    }

    /// Handle shutdown by flushing the Quiver engine and draining remaining bundles.
    ///
    /// The shutdown sequence is:
    /// 1. Sync the WAL and release (or NACK on failure) every upstream ACK held
    ///    for `upstream_ack: after_wal_sync`
    /// 2. Clear deferred-retry gating so parked retry bundles become drainable
    /// 3. Flush to finalize any open segment (makes data visible to subscribers)
    /// 4. Drain remaining bundles to downstream (best-effort, respects deadline)
    /// 5. Engine shutdown (always attempted - also finalizes open segment if flush was skipped)
    /// 6. With `forward_after_ingest`, apply the downstream outcomes recorded
    ///    for early-forwarded bundles and flush subscriber progress (always
    ///    attempted, regardless of the deadline)
    ///
    /// Note: Quiver's `shutdown()` internally calls `finalize_current_segment()`, so even
    /// if we skip the explicit flush due to deadline pressure, the engine shutdown will
    /// still persist any buffered data. The explicit flush + drain sequence is for
    /// orderly delivery to downstream, not for data durability.
    ///
    /// The deadline is enforced for the drain loop: if we run out of time, we skip
    /// remaining drain iterations and proceed to engine shutdown.
    async fn handle_shutdown(
        &mut self,
        deadline: Instant,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        otel_info!("durable_buffer.shutdown.start", deadline = ?deadline);

        // Only process if engine was initialized. Acks are only held after a
        // successful ingest, so none can be held without a ready engine.
        if !matches!(self.engine_state, EngineState::Ready { .. }) {
            debug_assert!(self.wal_sync_acks.is_empty());
            return Ok(());
        }

        // Answer every ack held for a WAL sync first, so upstream hears back
        // about each bundle it handed over even if the drain runs out of time.
        if let Err(e) = self.sync_wal_and_release(effect_handler).await {
            otel_warn!("durable_buffer.shutdown.wal_sync_release_failed", error = %e);
        }

        // Shutdown is terminal for this processor instance, so retry backoff no
        // longer matters. Clear local deferred-retry gating up front so bundles
        // that were parked behind backoff become drainable through the normal
        // Quiver poll loop below.
        self.deferred_retry_state.clear_for_shutdown();

        // Check deadline before flush/drain sequence
        if Instant::now() >= deadline {
            otel_warn!("durable_buffer.shutdown.deadline_exceeded");
        } else {
            // Flush to finalize any open segment - this makes buffered data visible
            // for the drain loop below. Even if this is skipped, engine.shutdown()
            // will finalize the segment.
            otel_info!("durable_buffer.shutdown.flushing");
            {
                let (engine, _) = self.engine()?;
                if let Err(e) = engine.flush().await {
                    if e.is_fenced() {
                        self.note_fenced(&e);
                    } else {
                        self.metrics.operational_metrics.flush_failures.add(1);
                        otel_error!("durable_buffer.shutdown.flush_failed", error = %e);
                    }
                }
            }
            self.sweep_early_outcomes()?;

            // Drain any remaining bundles that became available after flush.
            // A fenced node leaves them to the instance that claimed the
            // directory.
            let mut drained = 0u64;
            while !self.fenced {
                // Check deadline on each iteration
                if Instant::now() >= deadline {
                    otel_warn!(
                        "durable_buffer.shutdown.drain_deadline",
                        bundles_drained = drained
                    );
                    break;
                }

                let poll_result = {
                    let (engine, subscriber_id) = self.engine()?;
                    engine.poll_next_bundle(subscriber_id)
                };

                match poll_result {
                    Ok(Some(handle)) => {
                        match self.try_process_bundle_handle(handle, effect_handler) {
                            ProcessBundleResult::Sent => drained += 1,
                            ProcessBundleResult::Skipped | ProcessBundleResult::Resolved => {
                                // Continue draining
                            }
                            ProcessBundleResult::Backpressure => {
                                // During shutdown, if we hit backpressure just log and continue
                                // The bundle is deferred and will be picked up on next run
                                otel_warn!("durable_buffer.shutdown.backpressure");
                                break;
                            }
                            ProcessBundleResult::Error(e) => {
                                otel_warn!("durable_buffer.shutdown.bundle_error", error = %e);
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        otel_warn!("durable_buffer.shutdown.poll_error", error = %e);
                        break;
                    }
                }
            }
            if drained > 0 {
                otel_info!("durable_buffer.shutdown.drained", bundles_drained = drained);
            }
        }

        // Always attempt engine shutdown - this finalizes any open segment and
        // performs cleanup. Even if past deadline, this is fast (especially after
        // flush) and critical for data durability.
        {
            let (engine, _) = self.engine()?;
            if let Err(e) = engine.shutdown().await {
                if e.is_fenced() {
                    self.note_fenced(&e);
                } else {
                    otel_error!("durable_buffer.shutdown.engine_failed", error = %e);
                }
            } else {
                otel_info!("durable_buffer.shutdown.complete");
            }
        }

        // No timer tick runs after this, and `engine.shutdown()` may have just
        // made the open segment visible. Apply the downstream outcomes recorded
        // for early-forwarded bundles and persist the resulting subscriber
        // progress, so acknowledged bundles are not redelivered on restart.
        if self.config.forward_after_ingest {
            self.sweep_early_outcomes()?;
            let (engine, _) = self.engine()?;
            if let Err(e) = engine.flush_progress().await {
                if e.is_fenced() {
                    self.note_fenced(&e);
                } else {
                    otel_warn!("durable_buffer.shutdown.flush_progress_failed", error = %e);
                }
            }
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Processor Trait Implementation
// -----------------------------------------------------------------------------

#[async_trait(?Send)]
impl otel_arrow_dfe_engine::local::processor::Processor<OtapPdata> for DurableBuffer {
    fn runtime_requirements(&self) -> ProcessorRuntimeRequirements {
        ProcessorRuntimeRequirements {
            // One slot for deferred retries, one for the WAL sync deadline.
            local_wakeups: Some(LocalWakeupRequirements::new(2)),
            ..ProcessorRuntimeRequirements::none()
        }
    }

    /// Pauses pdata delivery while waiting for another pipeline instance in
    /// this process to hand over the data directory. The message that started
    /// the wait is held until the engine opens.
    ///
    /// Shutdown drains with `DrainPolicy::HonorAdmission`, so if pdata is
    /// buffered for a waiting node, the engine delivers `Shutdown` only at
    /// the drain deadline.
    fn accept_pdata(&self) -> bool {
        !matches!(self.engine_state, EngineState::AwaitingHandoff { .. })
    }

    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        // Ensure engine is initialized on first message (of any type)
        if matches!(self.engine_state, EngineState::Uninitialized) {
            self.ensure_engine_initialized(Instant::now()).await?;
        }

        // Start timer on first message (whether control or data) if not already started
        if !self.timer_started {
            let _ = effect_handler
                .start_periodic_timer(self.config.poll_interval)
                .await;
            self.timer_started = true;
            otel_debug!(
                "durable_buffer.timer.started",
                poll_interval = ?self.config.poll_interval
            );
        }

        // While waiting for the handoff there is no engine: data is held,
        // each timer tick retries the open, and shutdown NACKs the held data.
        // Other control messages need no engine: no bundle has been sent, so
        // no ack, nack, or wakeup refers to one, and telemetry skips the
        // storage gauges.
        let awaiting_handoff = matches!(self.engine_state, EngineState::AwaitingHandoff { .. });

        match msg {
            Message::PData(data) if awaiting_handoff => {
                self.hold_for_handoff(data, effect_handler).await
            }
            Message::PData(data) => self.handle_data(data, effect_handler).await,
            Message::Control(control_msg) => match control_msg {
                NodeControlMsg::TimerTick { .. } if awaiting_handoff => {
                    self.retry_handoff(Instant::now(), effect_handler).await
                }
                NodeControlMsg::TimerTick { .. } => self.handle_timer_tick(effect_handler).await,
                NodeControlMsg::Ack(ack) => self.handle_ack(ack, effect_handler).await,
                NodeControlMsg::Nack(nack) => self.handle_nack(nack, effect_handler).await,
                NodeControlMsg::Shutdown { .. } if awaiting_handoff => {
                    self.abandon_handoff(effect_handler).await
                }
                NodeControlMsg::Shutdown { deadline, .. } => {
                    self.handle_shutdown(deadline, effect_handler).await
                }
                NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                } => {
                    // Collect Quiver storage metrics
                    // (separate scope to avoid borrow conflicts with self)
                    let quiver_metrics = if let Ok((engine, subscriber_id)) = self.engine() {
                        let budget = engine.budget();
                        Some((
                            budget.used(),
                            budget.hard_cap(),
                            engine.clone(),
                            subscriber_id.clone(),
                        ))
                    } else {
                        None
                    };

                    if let Some((used, cap, engine, subscriber_id)) = quiver_metrics {
                        self.metrics
                            .operational_metrics
                            .storage_bytes_used
                            .set(used);
                        self.metrics.operational_metrics.storage_bytes_cap.set(cap);

                        let utilization = if cap > 0 {
                            (used.min(cap) as f64) / (cap as f64)
                        } else {
                            0.0
                        };
                        self.metrics
                            .operational_metrics
                            .storage_utilization
                            .set(utilization);

                        // Recompute queued item gauges from the subscriber
                        // registry. This is the single source of truth for
                        // these gauges, correctly accounting for ACKs,
                        // force-drops, and expiry.
                        self.recompute_metrics(&engine, &subscriber_id);
                    }

                    self.metrics
                        .report(&mut metrics_reporter)
                        .map_err(|e| Error::InternalError {
                            message: e.to_string(),
                        })
                }
                NodeControlMsg::Config { config } => {
                    otel_debug!("durable_buffer.config.update", config = ?config);
                    Ok(())
                }
                NodeControlMsg::MemoryPressureChanged { .. } => Ok(()),
                NodeControlMsg::DrainIngress { .. } => Ok(()),
                NodeControlMsg::Wakeup { slot, revision, .. } if slot == WAL_SYNC_WAKEUP_SLOT => {
                    self.handle_wal_sync_wakeup(slot, revision, effect_handler)
                        .await
                }
                NodeControlMsg::Wakeup { slot, revision, .. } => {
                    self.handle_retry_wakeup(slot, revision, effect_handler)
                        .await
                }
                NodeControlMsg::ResumeData { .. } => Ok(()),
            },
        }
    }
}

// -----------------------------------------------------------------------------
// Factory Registration
// -----------------------------------------------------------------------------

/// Factory function to create a DurableBuffer.
pub fn create_durable_buffer(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
    _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    let config: DurableBufferConfig =
        serde_json::from_value(node_config.config.clone()).map_err(|e| {
            ConfigError::InvalidUserConfig {
                error: format!("failed to parse durable buffer configuration: {}", e),
            }
        })?;

    // Create processor with lazy engine initialization
    // The Quiver engine will be initialized on the first message when we're
    // guaranteed to be in an async context
    let processor = DurableBuffer::new(config, &pipeline_ctx)?;

    Ok(ProcessorWrapper::local(
        processor,
        node,
        node_config,
        processor_config,
    ))
}

/// Register DurableBuffer as an OTAP processor factory.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Processor)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static DURABLE_BUFFER_FACTORY: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: DURABLE_BUFFER_URN,
    create: create_durable_buffer,
    context_declarations: None,
    // Standard OTLP/OTAP input stays standard, and pluggable Arrow record sets
    // are replayed unchanged.
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED
        .with_pdata(otel_arrow_dfe_engine::wiring_contract::PdataWiringContract::PASSTHROUGH),
    validate_config: validate_durable_buffer_config,
};

/// Static validation for the durable buffer configuration: parses the config
/// and checks values that deserialize but are still invalid.
fn validate_durable_buffer_config(config: &serde_json::Value) -> Result<(), ConfigError> {
    otel_arrow_dfe_config::validation::deserialize_typed_config::<DurableBufferConfig>(config)?
        .validate()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use otel_arrow_dfe_engine::context::ControllerContext;
    use otel_arrow_dfe_quiver::config::IpcCompression;
    use otel_arrow_dfe_quiver::record_bundle::{
        BundleDescriptor, PayloadRef, RecordBundle, SchemaFingerprint, SlotDescriptor, SlotId,
    };
    use otel_arrow_dfe_telemetry::attributes::AttributeEnum;
    use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
    use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    struct SimpleBundle {
        descriptor: BundleDescriptor,
        batch: RecordBatch,
        fingerprint: SchemaFingerprint,
        primary_slot: SlotId,
        item_count: u64,
        byte_count: u64,
    }

    impl RecordBundle for SimpleBundle {
        fn descriptor(&self) -> &BundleDescriptor {
            &self.descriptor
        }

        fn ingestion_time(&self) -> SystemTime {
            SystemTime::now()
        }

        fn payload(&self, slot: SlotId) -> Option<PayloadRef<'_>> {
            if slot == self.primary_slot {
                Some(PayloadRef {
                    schema_fingerprint: self.fingerprint,
                    batch: &self.batch,
                })
            } else {
                None
            }
        }

        fn item_count(&self) -> u64 {
            self.item_count
        }

        fn byte_count(&self) -> Option<u64> {
            Some(self.byte_count)
        }
    }

    async fn setup_test_processor(
        max_age: Option<Duration>,
    ) -> (
        DurableBuffer,
        Arc<QuiverEngine>,
        SubscriberId,
        tempfile::TempDir,
    ) {
        let registry = TelemetryRegistryHandle::default();
        let controller_ctx = ControllerContext::new(registry);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("test".into(), "test".into(), 0, 1, 0);

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let config = DurableBufferConfig {
            path: temp_dir.path().to_path_buf(),
            retention_size_cap: byte_unit::Byte::from_u64(256 * 1024 * 1024),
            max_age,
            size_cap_policy: SizeCapPolicy::Backpressure,
            poll_interval: Duration::from_millis(100),
            otlp_handling: OtlpHandling::PassThrough,
            max_segment_open_duration: Duration::from_secs(60),
            initial_retry_interval: Duration::from_secs(1),
            max_retry_interval: Duration::from_secs(30),
            retry_multiplier: 2.0,
            max_in_flight: 1000,
            wal_flush_interval: Duration::from_millis(25),
            upstream_ack: UpstreamAck::AfterWalAppend,
            wal_compression: IpcCompression::None,
            segment_compression: IpcCompression::None,
            forward_after_ingest: false,
            ownership: OwnershipSettings::default(),
        };

        let processor = DurableBuffer::new(config, &pipeline_ctx).unwrap();
        let (engine, subscriber_id) = processor.init_engine().await.unwrap();
        (processor, engine, subscriber_id, temp_dir)
    }

    fn make_simple_bundle(primary_slot: SlotId, item_count: u64) -> SimpleBundle {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .expect("valid batch");
        let byte_count = batch.get_array_memory_size() as u64;

        // Include shared slots (ResourceAttrs=1, ScopeAttrs=2) alongside the
        // primary signal slot, mirroring real OTAP bundles. This ensures
        // find_map(signal_type_from_slot_id) skips shared slots and correctly
        // classifies by the signal-specific slot.
        SimpleBundle {
            descriptor: BundleDescriptor::new(vec![
                SlotDescriptor::new(SlotId::new(1), "resource_attrs"),
                SlotDescriptor::new(SlotId::new(2), "scope_attrs"),
                SlotDescriptor::new(primary_slot, "test"),
            ]),
            batch,
            fingerprint: [0x11u8; 32],
            primary_slot,
            item_count,
            byte_count,
        }
    }

    /// Scenario: The engine validates a pipeline that routes pluggable Arrow pdata
    /// through the durable buffer.
    /// Guarantees: The factory accepts every representation family and preserves
    /// it, so pluggable receiver -> durable buffer -> pluggable exporter wiring
    /// validates instead of failing with an incompatible-wiring error.
    #[test]
    fn factory_declares_passthrough_wiring_contract() {
        use otel_arrow_dfe_engine::wiring_contract::PdataRepresentationSet;

        let contract = DURABLE_BUFFER_FACTORY.wiring_contract.pdata;
        assert_eq!(contract.accepted_inputs(), PdataRepresentationSet::ALL);
        assert_eq!(
            contract.outputs_for(PdataRepresentationSet::PLUGGABLE_ARROW),
            PdataRepresentationSet::PLUGGABLE_ARROW
        );
        assert_eq!(
            contract.outputs_for(PdataRepresentationSet::STANDARD),
            PdataRepresentationSet::STANDARD
        );
    }

    /// Builds a pluggable Arrow logs record set with `table_count` one-column tables.
    fn pluggable_test_records(table_count: u32) -> otel_arrow_dfe_pdata::PdataArrowRecordSet {
        use otel_arrow_dfe_pdata::{
            PdataArrowRecordSet, PdataArrowTable, PdataRepresentation, PdataRepresentationId,
            PdataSchemaId, RepresentationVersion,
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let tables = (0..table_count).map(|table_id| {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![table_id as i32, 7]))],
            )
            .expect("valid batch");
            PdataArrowTable::new(
                table_id,
                PdataSchemaId::new("example.logs.table"),
                otel_arrow_dfe_pdata::SchemaFingerprint::new([table_id as u8; 32]),
                batch,
            )
            .expect("valid table")
        });
        PdataArrowRecordSet::new(
            PdataRepresentation::new(
                PdataRepresentationId::new("example.logs"),
                RepresentationVersion::new(1, 0),
            ),
            SignalType::Logs,
            2,
            tables,
        )
        .expect("valid record set")
    }

    /// Creates a durable buffer processor that stores its data under `path`.
    fn create_test_durable_buffer(
        rt: &otel_arrow_dfe_engine::testing::processor::TestRuntime<OtapPdata>,
        name: &'static str,
        path: &std::path::Path,
    ) -> ProcessorWrapper<OtapPdata> {
        create_test_durable_buffer_with(rt, name, path, serde_json::json!({}))
    }

    /// Creates a durable buffer processor that stores its data under `path`,
    /// with each field of `overrides` added to or replacing the defaults used
    /// by [`create_test_durable_buffer`].
    fn create_test_durable_buffer_with(
        rt: &otel_arrow_dfe_engine::testing::processor::TestRuntime<OtapPdata>,
        name: &'static str,
        path: &std::path::Path,
        overrides: serde_json::Value,
    ) -> ProcessorWrapper<OtapPdata> {
        use otel_arrow_dfe_engine::testing::test_node;
        use serde_json::json;

        let controller = ControllerContext::new(rt.metrics_registry());
        let pipeline_ctx = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
        let mut node_config = NodeUserConfig::new_processor_config(DURABLE_BUFFER_URN);
        node_config.config = json!({
            "path": path,
            "retention_size_cap": "256 MiB",
            "poll_interval": "100ms",
            "max_segment_open_duration": "1s",
        });
        if let (Some(config), Some(overrides)) =
            (node_config.config.as_object_mut(), overrides.as_object())
        {
            for (key, value) in overrides {
                let _ = config.insert(key.clone(), value.clone());
            }
        }
        create_durable_buffer(
            pipeline_ctx,
            test_node(name),
            Arc::new(node_config),
            &ProcessorConfig::new(name),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("create durable buffer")
    }

    /// Scenario: A pluggable Arrow record set is sent through the durable buffer
    /// processor, acknowledged upstream, and forwarded on the next timer tick.
    /// Guarantees: The sender is ACKed once the record set is ingested, and the
    /// forwarded pdata carries a record set identical to the input, including
    /// its representation identity and tables.
    #[test]
    fn pluggable_arrow_payload_is_persisted_and_forwarded_unchanged() {
        use otel_arrow_dfe_engine::control::{
            PipelineCompletionMsg, pipeline_completion_msg_channel,
        };
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use otel_arrow_dfe_otap::testing::TestCallData;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor =
            create_test_durable_buffer(&rt, "durable-buffer-pluggable", temp_dir.path());
        let records = pluggable_test_records(3);

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (pipeline_completion_tx, mut pipeline_completion_rx) =
                    pipeline_completion_msg_channel(10);
                ctx.set_pipeline_completion_sender(pipeline_completion_tx);

                let input = OtapPdata::new_default(OtapPayload::from_pdata_arrow(records.clone()))
                    .test_subscribe_to(
                        Interests::ACKS | Interests::NACKS,
                        TestCallData::new_with(0, 0).into(),
                        1,
                    );
                ctx.process(Message::PData(input))
                    .await
                    .expect("process input");
                assert!(matches!(
                    pipeline_completion_rx.try_recv(),
                    Ok(PipelineCompletionMsg::DeliverAck { .. })
                ));

                ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                    .await
                    .expect("process timer tick");
                let outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1, "timer tick should emit one bundle");
                assert_eq!(outputs[0].payload_ref().pdata_arrow_ref(), Some(&records));
            })
            .validate(|_| async {});
    }

    /// Scenario: A pluggable Arrow record set with more tables than Quiver can
    /// persist is sent through the durable buffer processor.
    /// Guarantees: The sender gets a permanent NACK that names the exceeded limit
    /// and returns the original record set, and nothing is forwarded downstream.
    #[test]
    fn pluggable_arrow_payload_over_persistence_limit_is_permanently_nacked() {
        use otel_arrow_dfe_engine::control::{
            PipelineCompletionMsg, pipeline_completion_msg_channel,
        };
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use otel_arrow_dfe_otap::testing::TestCallData;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor =
            create_test_durable_buffer(&rt, "durable-buffer-pluggable-limit", temp_dir.path());
        let records = pluggable_test_records(MAX_PLUGGABLE_TABLES as u32 + 1);

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (pipeline_completion_tx, mut pipeline_completion_rx) =
                    pipeline_completion_msg_channel(10);
                ctx.set_pipeline_completion_sender(pipeline_completion_tx);

                let input = OtapPdata::new_default(OtapPayload::from_pdata_arrow(records.clone()))
                    .test_subscribe_to(
                        Interests::ACKS | Interests::NACKS,
                        TestCallData::new_with(0, 0).into(),
                        1,
                    );
                ctx.process(Message::PData(input))
                    .await
                    .expect("process input");

                let Ok(PipelineCompletionMsg::DeliverNack { nack }) =
                    pipeline_completion_rx.try_recv()
                else {
                    panic!("expected a NACK for the over-limit record set");
                };
                assert!(nack.permanent);
                assert!(nack.reason.contains("table count"), "{}", nack.reason);
                assert_eq!(nack.refused.payload_ref().pdata_arrow_ref(), Some(&records));

                ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                    .await
                    .expect("process timer tick");
                assert!(ctx.drain_pdata().await.is_empty());
            })
            .validate(|_| async {});
    }

    #[test]
    fn test_bundle_ref_encoding_roundtrip() {
        let bundle_ref = BundleRef {
            segment_seq: SegmentSeq::new(12345),
            bundle_index: BundleIndex::new(42),
        };

        let calldata = encode_bundle_ref(bundle_ref);
        let decoded = decode_bundle_ref(&calldata);

        assert!(decoded.is_some());
        let decoded = decoded.unwrap();
        assert_eq!(decoded.segment_seq.raw(), 12345);
        assert_eq!(decoded.bundle_index.raw(), 42);
    }

    #[test]
    fn test_decode_bundle_ref_empty_calldata() {
        let calldata: CallData = smallvec![];
        assert!(decode_bundle_ref(&calldata).is_none());
    }

    #[test]
    fn test_decode_bundle_ref_insufficient_calldata() {
        let calldata: CallData = smallvec![Context8u8::from(123u64)];
        assert!(decode_bundle_ref(&calldata).is_none());
    }

    /// Drains every pending upstream completion, returning the test calldata
    /// of each ACK and the reason of each NACK in arrival order.
    fn drain_upstream_completions(
        rx: &mut otel_arrow_dfe_engine::control::PipelineCompletionMsgReceiver<OtapPdata>,
    ) -> (Vec<otel_arrow_dfe_otap::testing::TestCallData>, Vec<String>) {
        use otel_arrow_dfe_engine::control::PipelineCompletionMsg;
        use otel_arrow_dfe_otap::testing::{TestCallData, next_ack};

        let mut acked = Vec::new();
        let mut nacked = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            match msg {
                PipelineCompletionMsg::DeliverAck { ack } => {
                    let (_, ack) = next_ack(ack).expect("ack subscriber");
                    acked.push(
                        TestCallData::try_from(ack.unwind.route.calldata).expect("test calldata"),
                    );
                }
                PipelineCompletionMsg::DeliverNack { nack } => nacked.push(nack.reason),
            }
        }
        (acked, nacked)
    }

    /// Returns the test calldata that [`make_log_input`] stamps for each id.
    fn expected_calldata(
        ids: std::ops::Range<usize>,
    ) -> Vec<otel_arrow_dfe_otap::testing::TestCallData> {
        ids.map(|id| otel_arrow_dfe_otap::testing::TestCallData::new_with(id as u64, 0))
            .collect()
    }

    /// Collects telemetry and returns this interval's `wal.syncs` and
    /// `wal.sync.failures` counter values.
    async fn wal_sync_counters(
        ctx: &mut otel_arrow_dfe_engine::testing::processor::TestContext<OtapPdata>,
    ) -> (u64, u64) {
        let (metrics_rx, reporter) = MetricsReporter::create_new_and_receiver(16);
        ctx.process(Message::Control(NodeControlMsg::CollectTelemetry {
            metrics_reporter: reporter,
        }))
        .await
        .expect("collect telemetry");

        let mut syncs = None;
        let mut failures = None;
        while let Ok(snapshot) = metrics_rx.try_recv() {
            let descriptor = snapshot.descriptor();
            if descriptor.name != "processor.durable_buffer" {
                continue;
            }
            for (field, value) in descriptor.metrics.iter().zip(snapshot.get_metrics()) {
                match field.name {
                    "wal.syncs" => syncs = Some(value.to_u64_lossy()),
                    "wal.sync.failures" => failures = Some(value.to_u64_lossy()),
                    _ => {}
                }
            }
        }
        (
            syncs.expect("wal.syncs reported"),
            failures.expect("wal.sync.failures reported"),
        )
    }

    /// Scenario: With `upstream_ack: after_wal_sync` and a WAL flush interval
    /// long enough that the WAL policy never syncs during the test, three
    /// bundles are ingested and the node then stays idle until its WAL sync
    /// deadline wakeup fires.
    /// Guarantees: No ACK reaches upstream before the node completes a WAL
    /// sync; the deadline is armed one flush interval after the first held
    /// ACK; the wakeup alone (no further ingest) runs exactly one sync and
    /// releases all three ACKs in ingest order, with no NACKs.
    #[test]
    fn after_wal_sync_holds_acks_until_deadline_wakeup_syncs_wal() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-wal-sync-deadline",
            temp_dir.path(),
            serde_json::json!({ "upstream_ack": "after_wal_sync", "wal_flush_interval": "1h" }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                let before_ingest = Instant::now();
                for id in 0..3 {
                    ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                        .await
                        .expect("ingest");
                }

                let (acked, nacked) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty(), "ACKs must wait for a WAL sync");
                assert!(nacked.is_empty());
                assert_eq!(wal_sync_counters(&mut ctx).await, (0, 0));
                let deadline = ctx
                    .next_local_control_deadline()
                    .expect("WAL sync deadline armed");
                assert!(deadline >= before_ingest + Duration::from_secs(3600));

                assert!(ctx.fire_wakeup().await.expect("WAL sync wakeup"));

                let (acked, nacked) = drain_upstream_completions(&mut rx);
                assert_eq!(acked, expected_calldata(0..3));
                assert!(nacked.is_empty());
                assert_eq!(wal_sync_counters(&mut ctx).await, (1, 0));
                assert!(
                    !ctx.fire_wakeup().await.expect("no wakeup"),
                    "no deadline stays armed once nothing is held"
                );
            })
            .validate(|_| async {});
    }

    /// Scenario: With `upstream_ack: after_wal_sync`, enough bundles are
    /// ingested to fill the held ACK list, then one more, without any wakeup
    /// firing.
    /// Guarantees: The ACK that fills the list is held with the others and
    /// one WAL sync releases all of them right away; the next bundle's ACK is
    /// held behind a fresh deadline and released when that wakeup fires.
    #[test]
    fn after_wal_sync_full_held_list_syncs_immediately() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-wal-sync-bound",
            temp_dir.path(),
            serde_json::json!({ "upstream_ack": "after_wal_sync", "wal_flush_interval": "1h" }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(MAX_HELD_ACKS + 16);
                ctx.set_pipeline_completion_sender(tx);

                let filling = MAX_HELD_ACKS - 1;
                for id in 0..filling {
                    ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                        .await
                        .expect("ingest");
                }
                let (acked, _) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty(), "a list below the bound is still held");
                assert_eq!(wal_sync_counters(&mut ctx).await, (0, 0));

                ctx.process(Message::PData(make_log_input("m-fill", filling)))
                    .await
                    .expect("ingest up to the bound");
                let (acked, nacked) = drain_upstream_completions(&mut rx);
                assert_eq!(acked, expected_calldata(0..MAX_HELD_ACKS));
                assert!(nacked.is_empty());
                assert_eq!(wal_sync_counters(&mut ctx).await, (1, 0));

                let last = MAX_HELD_ACKS;
                ctx.process(Message::PData(make_log_input("m-last", last)))
                    .await
                    .expect("ingest past the bound");
                let (acked, _) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty(), "the next ACK is held again");

                assert!(ctx.fire_wakeup().await.expect("WAL sync wakeup"));
                let (acked, _) = drain_upstream_completions(&mut rx);
                assert_eq!(acked, expected_calldata(last..last + 1));
            })
            .validate(|_| async {});
    }

    /// Ingests one bundle into a processor built with `overrides` and asserts
    /// that its ACK is sent during ingest with no WAL sync wakeup armed.
    fn assert_ack_sent_during_ingest(name: &'static str, overrides: serde_json::Value) {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(&rt, name, temp_dir.path(), overrides);

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("ingest");

                let (acked, nacked) = drain_upstream_completions(&mut rx);
                assert_eq!(acked, expected_calldata(0..1));
                assert!(nacked.is_empty());
                assert!(ctx.next_local_control_deadline().is_none());
                assert_eq!(wal_sync_counters(&mut ctx).await, (0, 0));
            })
            .validate(|_| async {});
    }

    /// Scenario: `upstream_ack` is absent from the config and the WAL flush
    /// interval is nonzero.
    /// Guarantees: The default keeps the existing behavior: the ACK is sent as
    /// soon as the WAL append returns, and no WAL sync wakeup is armed.
    #[test]
    fn default_upstream_ack_acks_after_wal_append() {
        assert_ack_sent_during_ingest(
            "durable-buffer-default-ack",
            serde_json::json!({ "wal_flush_interval": "1h" }),
        );
    }

    /// Scenario: `upstream_ack: after_wal_sync` is combined with
    /// `wal_flush_interval: 0s`, where the WAL syncs on every append.
    /// Guarantees: The config is accepted and the ACK is sent during ingest
    /// without holding it or arming a wakeup.
    #[test]
    fn after_wal_sync_with_zero_flush_interval_acks_immediately() {
        assert_ack_sent_during_ingest(
            "durable-buffer-wal-sync-zero-interval",
            serde_json::json!({ "upstream_ack": "after_wal_sync", "wal_flush_interval": "0s" }),
        );
    }

    /// Scenario: With `upstream_ack: after_wal_sync`, ACKs are held when a
    /// retry-slot wakeup and a WAL sync wakeup with a stale revision arrive.
    /// Guarantees: Neither wakeup releases held ACKs or runs a WAL sync; the
    /// armed WAL sync wakeup still releases them afterwards.
    #[test]
    fn after_wal_sync_ignores_unrelated_and_stale_wakeups() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-wal-sync-stale",
            temp_dir.path(),
            serde_json::json!({ "upstream_ack": "after_wal_sync", "wal_flush_interval": "1h" }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("ingest");
                for (slot, revision) in [(RETRY_WAKEUP_SLOT, 0), (WAL_SYNC_WAKEUP_SLOT, u64::MAX)] {
                    ctx.process(Message::Control(NodeControlMsg::Wakeup {
                        slot,
                        when: Instant::now(),
                        revision,
                    }))
                    .await
                    .expect("process wakeup");
                }
                let (acked, _) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty());
                assert_eq!(wal_sync_counters(&mut ctx).await, (0, 0));

                assert!(ctx.fire_wakeup().await.expect("WAL sync wakeup"));
                let (acked, _) = drain_upstream_completions(&mut rx);
                assert_eq!(acked, expected_calldata(0..1));
            })
            .validate(|_| async {});
    }

    /// Scenario: With `upstream_ack: after_wal_sync`, two ACKs are held when
    /// the node receives a shutdown request before the deadline wakeup fires.
    /// Guarantees: Shutdown syncs the WAL and releases both held ACKs before it
    /// completes, so upstream hears back about every bundle it handed over.
    #[test]
    fn after_wal_sync_shutdown_releases_held_acks() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-wal-sync-shutdown",
            temp_dir.path(),
            serde_json::json!({ "upstream_ack": "after_wal_sync", "wal_flush_interval": "1h" }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                for id in 0..2 {
                    ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                        .await
                        .expect("ingest");
                }
                let (acked, _) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty());

                ctx.process(Message::Control(NodeControlMsg::Shutdown {
                    deadline: Instant::now() + Duration::from_secs(5),
                    reason: "shutdown".to_owned(),
                }))
                .await
                .expect("shutdown");

                let (acked, nacked) = drain_upstream_completions(&mut rx);
                assert_eq!(acked, expected_calldata(0..2));
                assert!(nacked.is_empty());
            })
            .validate(|_| async {});
    }

    /// Environment variable that turns [`after_wal_sync_crash_child`] into the
    /// child process of [`after_wal_sync_acked_bundles_survive_process_kill`].
    /// Its value is the durable buffer storage path.
    const WAL_SYNC_CRASH_CHILD_DIR: &str = "OTAP_DURABLE_BUFFER_WAL_SYNC_CRASH_DIR";

    /// Line prefix the crash child prints for each bundle id ACKed upstream.
    const WAL_SYNC_CRASH_ACKED: &str = "wal-sync-crash-acked:";

    /// Line the crash child prints once it is ready to be killed.
    const WAL_SYNC_CRASH_READY: &str = "wal-sync-crash-ready";

    /// Scenario: Runs as the child process spawned by
    /// [`after_wal_sync_acked_bundles_survive_process_kill`]: it ingests
    /// bundles with `upstream_ack: after_wal_sync`, releases three batches of
    /// ACKs through the WAL sync deadline wakeup, leaves two more bundles held,
    /// and then waits to be killed. As an ordinary test it does nothing.
    /// Guarantees: Every bundle id is printed only after its upstream ACK was
    /// received, and the ready line is printed after all of them.
    #[test]
    fn after_wal_sync_crash_child() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use std::io::Write as _;

        let Some(dir) = std::env::var_os(WAL_SYNC_CRASH_CHILD_DIR) else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);

        let rt = TestRuntime::new();
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-wal-sync-crash-child",
            &dir,
            serde_json::json!({
                "upstream_ack": "after_wal_sync",
                "wal_flush_interval": "1h",
                "max_segment_open_duration": "1h",
            }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(64);
                ctx.set_pipeline_completion_sender(tx);
                let mut stdout = std::io::stdout();

                for batch in 0..3 {
                    for id in batch * 4..batch * 4 + 4 {
                        ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                            .await
                            .expect("ingest");
                    }
                    assert!(ctx.fire_wakeup().await.expect("WAL sync wakeup"));
                    let (acked, nacked) = drain_upstream_completions(&mut rx);
                    assert!(nacked.is_empty());
                    assert_eq!(acked, expected_calldata(batch * 4..batch * 4 + 4));
                    for id in batch * 4..batch * 4 + 4 {
                        writeln!(stdout, "{WAL_SYNC_CRASH_ACKED}m-{id}").expect("report ack");
                    }
                    stdout.flush().expect("flush acks");
                }

                for id in 12..14 {
                    ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                        .await
                        .expect("ingest held");
                }
                let (acked, _) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty(), "the last bundles stay held");

                writeln!(stdout, "{WAL_SYNC_CRASH_READY}").expect("report ready");
                stdout.flush().expect("flush ready");
                // Block until the parent kills this process.
                loop {
                    std::thread::park();
                }
            })
            .validate(|_| async {});
    }

    /// Scenario: A child process ingests bundles into a durable buffer with
    /// `upstream_ack: after_wal_sync`, reports each bundle id it received an
    /// upstream ACK for, and is killed while more ACKs are still held and the
    /// open segment was never finalized. The parent then reopens the same
    /// storage and drains it.
    /// Guarantees: Every bundle the child ACKed upstream is recovered from the
    /// WAL and delivered downstream after the process is killed. A process
    /// kill keeps the OS page cache, so this checks recovery of released ACKs
    /// across a crash; fsync coverage is checked by the quiver `sync_wal`
    /// tests.
    #[test]
    fn after_wal_sync_acked_bundles_survive_process_kill() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use std::collections::BTreeSet;
        use std::io::BufRead as _;
        use std::process::{Command, Stdio};

        /// Kills and reaps the child on every exit path, including failures.
        struct KillOnDrop(std::process::Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let child_test = concat!(module_path!(), "::after_wal_sync_crash_child")
            .split_once("::")
            .expect("crate-qualified module path")
            .1;
        let mut child = KillOnDrop(
            Command::new(std::env::current_exe().expect("test executable"))
                .args(["--exact", child_test, "--nocapture", "--test-threads=1"])
                .env(WAL_SYNC_CRASH_CHILD_DIR, temp_dir.path())
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .spawn()
                .expect("start crash child"),
        );

        // Read the child's report on a separate thread so a stuck child fails
        // the test after a bounded wait instead of hanging it.
        let stdout = child.0.stdout.take().expect("child stdout");
        let (report_tx, report_rx) = std::sync::mpsc::channel();
        let _reader = std::thread::spawn(move || {
            let mut acked = BTreeSet::new();
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                // libtest may print the test name on the same line as the
                // first report, so match the markers anywhere in the line.
                if let Some((_, id)) = line.split_once(WAL_SYNC_CRASH_ACKED) {
                    let _ = acked.insert(id.trim().to_owned());
                } else if line.contains(WAL_SYNC_CRASH_READY) {
                    let _ = report_tx.send(Some(acked));
                    return;
                }
            }
            let _ = report_tx.send(None);
        });
        let acked = report_rx
            .recv_timeout(Duration::from_secs(120))
            .expect("crash child reports within the timeout")
            .expect("crash child exited before it was ready");
        assert_eq!(acked.len(), 12, "the child should ACK three batches");

        child.0.kill().expect("kill crash child");
        let status = child.0.wait().expect("reap crash child");
        assert!(!status.success(), "the child must not exit cleanly");
        drop(child);

        let delivered = Arc::new(std::sync::Mutex::new(BTreeSet::<String>::new()));
        let rt = TestRuntime::new();
        let processor = create_test_durable_buffer(
            &rt,
            "durable-buffer-wal-sync-crash-recover",
            temp_dir.path(),
        );
        let delivered_in_test = Arc::clone(&delivered);
        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, _rx) = pipeline_completion_msg_channel(64);
                ctx.set_pipeline_completion_sender(tx);

                ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                    .await
                    .expect("timer tick");
                let mut outputs = ctx.drain_pdata().await;
                ctx.process(Message::Control(NodeControlMsg::Shutdown {
                    deadline: Instant::now() + Duration::from_secs(30),
                    reason: "shutdown".to_owned(),
                }))
                .await
                .expect("shutdown");
                outputs.extend(ctx.drain_pdata().await);

                let mut delivered = delivered_in_test.lock().expect("delivered ids");
                for out in &mut outputs {
                    for id in log_ids_in_output(out) {
                        let _ = delivered.insert(id);
                    }
                }
            })
            .validate(|_| async {});

        let delivered = delivered.lock().expect("delivered ids");
        let lost: Vec<_> = acked.difference(&delivered).collect();
        assert!(
            lost.is_empty(),
            "ACKed bundles lost after the crash: {lost:?}"
        );
    }

    type TestCtx = otel_arrow_dfe_engine::testing::processor::TestContext<OtapPdata>;

    /// Sends one timer tick to the processor.
    async fn timer_tick(ctx: &mut TestCtx) {
        ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
            .await
            .expect("timer tick");
    }

    /// Sends a shutdown with a generous deadline to the processor.
    async fn shutdown_now(ctx: &mut TestCtx) {
        ctx.process(Message::Control(NodeControlMsg::Shutdown {
            deadline: Instant::now() + Duration::from_secs(30),
            reason: "shutdown".to_owned(),
        }))
        .await
        .expect("shutdown");
    }

    /// Collects telemetry and returns every durable buffer metric value
    /// reported for this interval, keyed by `<metric set>/<field>` and summed
    /// across attribute sets (for example, the per-signal `items` sets).
    async fn durable_buffer_metrics(ctx: &mut TestCtx) -> HashMap<String, u64> {
        let (metrics_rx, reporter) = MetricsReporter::create_new_and_receiver(64);
        ctx.process(Message::Control(NodeControlMsg::CollectTelemetry {
            metrics_reporter: reporter,
        }))
        .await
        .expect("collect telemetry");

        let mut values = HashMap::new();
        while let Ok(snapshot) = metrics_rx.try_recv() {
            let descriptor = snapshot.descriptor();
            if !descriptor.name.starts_with("processor.durable_buffer") {
                continue;
            }
            for (field, value) in descriptor.metrics.iter().zip(snapshot.get_metrics()) {
                *values
                    .entry(format!("{}/{}", descriptor.name, field.name))
                    .or_insert(0) += value.to_u64_lossy();
            }
        }
        values
    }

    /// Returns one metric value from [`durable_buffer_metrics`] output, or 0
    /// when it was not reported.
    fn metric(values: &HashMap<String, u64>, key: &str) -> u64 {
        values.get(key).copied().unwrap_or(0)
    }

    /// Simulates the downstream ACK of `out`, returning the ACK routed to the
    /// durable buffer and the bundle reference decoded from its calldata.
    fn downstream_ack(out: OtapPdata) -> (AckMsg<OtapPdata>, BundleRef) {
        let (_, ack) =
            otel_arrow_dfe_otap::testing::next_ack(AckMsg::new(out)).expect("ack subscriber");
        let bundle_ref = decode_bundle_ref(&ack.unwind.route.calldata).expect("bundle calldata");
        (ack, bundle_ref)
    }

    /// Simulates a downstream NACK of `out`, transient or permanent.
    fn downstream_nack(out: OtapPdata, permanent: bool) -> NackMsg<OtapPdata> {
        let nack = if permanent {
            NackMsg::new_permanent("rejected", out)
        } else {
            NackMsg::new("retry", out)
        };
        let (_, nack) = otel_arrow_dfe_otap::testing::next_nack(nack).expect("nack subscriber");
        nack
    }

    /// Reopens the durable buffer stored under `path` with early forwarding
    /// off, collects everything it sends through one timer tick and a
    /// shutdown, and returns the delivered log ids in order (duplicates kept).
    fn redelivered_after_restart(path: &std::path::Path) -> Vec<String> {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let delivered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let delivered_in_test = Arc::clone(&delivered);
        let rt = TestRuntime::new();
        let processor = create_test_durable_buffer(&rt, "durable-buffer-restart", path);
        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, _rx) = pipeline_completion_msg_channel(64);
                ctx.set_pipeline_completion_sender(tx);

                timer_tick(&mut ctx).await;
                let mut outputs = ctx.drain_pdata().await;
                shutdown_now(&mut ctx).await;
                outputs.extend(ctx.drain_pdata().await);

                let mut delivered = delivered_in_test.lock().expect("delivered ids");
                for out in &mut outputs {
                    delivered.extend(log_ids_in_output(out));
                }
            })
            .validate(|_| async {});
        delivered.lock().expect("delivered ids").clone()
    }

    /// Scenario: Two bundles are ingested with `forward_after_ingest` left at
    /// its default, and a timer tick then finalizes their open segment.
    /// Guarantees: Nothing is sent downstream before the segment is
    /// finalized, the tick sends both bundles in ingest order, and no early
    /// forward is counted.
    #[test]
    fn forward_after_ingest_off_by_default_waits_for_segment() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-forward-default",
            temp_dir.path(),
            serde_json::json!({ "max_segment_open_duration": "1h" }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                for id in 0..2 {
                    ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                        .await
                        .expect("ingest");
                }
                assert_eq!(
                    drain_upstream_completions(&mut rx).0,
                    expected_calldata(0..2)
                );
                assert!(
                    ctx.drain_pdata().await.is_empty(),
                    "nothing sent before finalization"
                );

                timer_tick(&mut ctx).await;
                let mut outputs = ctx.drain_pdata().await;
                let ids: Vec<_> = outputs.iter_mut().flat_map(log_ids_in_output).collect();
                assert_eq!(ids, ["m-0", "m-1"]);

                let metrics = durable_buffer_metrics(&mut ctx).await;
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer/early.forwards"),
                    0
                );
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer.items/produced"),
                    2
                );
            })
            .validate(|_| async {});
    }

    /// Scenario: With `forward_after_ingest: true`, one bundle is ingested
    /// while its segment stays open, downstream never answers, and the
    /// processor is shut down and later reopened.
    /// Guarantees: The bundle is sent downstream during ingest, before any
    /// timer tick, and upstream still gets its ACK. The sent pdata carries
    /// only this processor's subscription (none of the upstream frames) with
    /// calldata naming bundle 0. The send is counted once in
    /// `early.forwards`, `produced` and `in.flight`. Shutdown finalizes the
    /// segment without sending the bundle again, and the unacknowledged
    /// bundle is delivered again after the restart.
    #[test]
    fn forward_after_ingest_sends_bundle_before_segment_finalization() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-forward-early",
            temp_dir.path(),
            serde_json::json!({ "forward_after_ingest": true, "max_segment_open_duration": "1h" }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("ingest");
                assert_eq!(
                    drain_upstream_completions(&mut rx).0,
                    expected_calldata(0..1)
                );

                let mut outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1, "sent during ingest");
                assert_eq!(log_ids_in_output(&mut outputs[0]), ["m-0"]);

                let (mut ack, bundle_ref) = downstream_ack(outputs[0].clone());
                assert_eq!(bundle_ref.bundle_index, BundleIndex::new(0));
                assert!(
                    ack.accepted
                        .context_mut()
                        .drain_to_next_subscriber(Interests::ACKS | Interests::NACKS)
                        .is_none(),
                    "the upstream subscription must not travel downstream"
                );

                let metrics = durable_buffer_metrics(&mut ctx).await;
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer/early.forwards"),
                    1
                );
                assert_eq!(metric(&metrics, "processor.durable_buffer/in.flight"), 1);
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer.items/produced"),
                    1
                );

                shutdown_now(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty(), "no second send");
            })
            .validate(|_| async {});

        assert_eq!(redelivered_after_restart(temp_dir.path()), ["m-0"]);
    }

    /// Scenario: With `forward_after_ingest: true` and `max_in_flight: 2`,
    /// bundles 0 and 1 are sent during ingest and bundle 2 is held back at
    /// capacity. Downstream ACKs bundle 1 before the segment is finalized, a
    /// timer tick then finalizes the segment while bundle 0 is still in
    /// flight, downstream ACKs bundles 0 and 2, and the processor restarts.
    /// Guarantees: The tick keeps the claim on in-flight bundle 0 without
    /// sending it again and moves on to deliver bundle 2 from the segment in
    /// the same tick, so a still-in-flight early bundle never stalls the
    /// segment path. The ACK of in-flight bundle 0 lands in Quiver, so a
    /// restart delivers nothing.
    #[test]
    fn forward_after_ingest_in_flight_bundle_is_not_resent() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-forward-in-flight",
            temp_dir.path(),
            serde_json::json!({
                "forward_after_ingest": true,
                "max_segment_open_duration": "1h",
                "max_in_flight": 2
            }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, _rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                for id in 0..3 {
                    ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                        .await
                        .expect("ingest");
                }
                let mut early = ctx.drain_pdata().await;
                let ids: Vec<_> = early.iter_mut().flat_map(log_ids_in_output).collect();
                assert_eq!(ids, ["m-0", "m-1"], "m-2 is held back at capacity");

                let (ack, _) = downstream_ack(early.remove(1));
                ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                    .await
                    .expect("ack m-1");

                timer_tick(&mut ctx).await;
                let mut from_segment = ctx.drain_pdata().await;
                let ids: Vec<_> = from_segment
                    .iter_mut()
                    .flat_map(log_ids_in_output)
                    .collect();
                assert_eq!(ids, ["m-2"], "m-0 is not sent again and does not stall m-2");

                for out in early.into_iter().chain(from_segment) {
                    let (ack, _) = downstream_ack(out);
                    ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                        .await
                        .expect("ack");
                }
                timer_tick(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty());

                let metrics = durable_buffer_metrics(&mut ctx).await;
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer/early.forwards"),
                    2
                );
                assert_eq!(metric(&metrics, "processor.durable_buffer/in.flight"), 0);
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer.items/produced"),
                    3
                );

                shutdown_now(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty());
            })
            .validate(|_| async {});

        assert!(redelivered_after_restart(temp_dir.path()).is_empty());
    }

    /// Scenario: With `forward_after_ingest: true`, downstream ACKs a bundle
    /// sent during ingest while its segment is still open, and the processor
    /// is shut down before any timer tick, once with a deadline that has
    /// already passed (flush and drain are skipped) and once with time to
    /// spare. Each run is then reopened on its storage.
    /// Guarantees: Shutdown applies the recorded ACK after the engine
    /// finalizes the segment and saves subscriber progress, whatever the
    /// deadline, so the acknowledged bundle is not delivered again after the
    /// restart.
    #[test]
    fn forward_after_ingest_shutdown_saves_early_acks() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        for deadline_passed in [true, false] {
            let rt = TestRuntime::new();
            let temp_dir = tempfile::tempdir().expect("tempdir");
            let processor = create_test_durable_buffer_with(
                &rt,
                "durable-buffer-forward-shutdown",
                temp_dir.path(),
                serde_json::json!({
                    "forward_after_ingest": true,
                    "max_segment_open_duration": "1h"
                }),
            );

            rt.set_processor(processor)
                .run_test(move |mut ctx| async move {
                    let (tx, _rx) = pipeline_completion_msg_channel(16);
                    ctx.set_pipeline_completion_sender(tx);

                    ctx.process(Message::PData(make_log_input("m-0", 0)))
                        .await
                        .expect("ingest");
                    let mut outputs = ctx.drain_pdata().await;
                    assert_eq!(outputs.len(), 1, "sent during ingest");
                    let (ack, _) = downstream_ack(outputs.remove(0));
                    ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                        .await
                        .expect("ack");

                    let deadline = if deadline_passed {
                        Instant::now()
                    } else {
                        Instant::now() + Duration::from_secs(30)
                    };
                    ctx.process(Message::Control(NodeControlMsg::Shutdown {
                        deadline,
                        reason: "shutdown".to_owned(),
                    }))
                    .await
                    .expect("shutdown");
                    assert!(ctx.drain_pdata().await.is_empty(), "no send at shutdown");
                })
                .validate(|_| async {});

            assert!(
                redelivered_after_restart(temp_dir.path()).is_empty(),
                "redelivered after restart (deadline_passed: {deadline_passed})"
            );
        }
    }

    /// Runs one bundle through early forwarding, has downstream answer it
    /// with `answer` while its segment is still open, then finalizes the
    /// segment with a timer tick and shuts down. Panics if the bundle is sent
    /// a second time.
    fn answer_early_bundle_before_segment_is_visible(
        name: &'static str,
        path: &std::path::Path,
        answer: fn(OtapPdata) -> NodeControlMsg<OtapPdata>,
    ) -> HashMap<String, u64> {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let metrics = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let metrics_in_test = Arc::clone(&metrics);
        let rt = TestRuntime::new();
        let processor = create_test_durable_buffer_with(
            &rt,
            name,
            path,
            serde_json::json!({ "forward_after_ingest": true, "max_segment_open_duration": "1h" }),
        );
        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, _rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("ingest");
                let mut outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1);
                ctx.process(Message::Control(answer(outputs.remove(0))))
                    .await
                    .expect("downstream answer");

                timer_tick(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty(), "no second send");
                let collected = durable_buffer_metrics(&mut ctx).await;
                shutdown_now(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty(), "no send at shutdown");
                *metrics_in_test.lock().expect("metrics") = collected;
            })
            .validate(|_| async {});
        metrics.lock().expect("metrics").clone()
    }

    /// Scenario: With `forward_after_ingest: true`, downstream ACKs a bundle
    /// sent during ingest before its segment is finalized; a timer tick then
    /// finalizes the segment, and the processor is restarted.
    /// Guarantees: The segment path does not send the bundle again, the
    /// recorded ACK is applied in Quiver once the segment is visible (so
    /// subscriber progress covers it and a restart delivers nothing), and
    /// `in.flight` drops to 0.
    #[test]
    fn forward_after_ingest_ack_before_segment_is_visible() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let metrics = answer_early_bundle_before_segment_is_visible(
            "durable-buffer-forward-ack-early",
            temp_dir.path(),
            |out| NodeControlMsg::Ack(downstream_ack(out).0),
        );
        assert_eq!(metric(&metrics, "processor.durable_buffer/in.flight"), 0);
        assert_eq!(
            metric(&metrics, "processor.durable_buffer.items/produced"),
            1
        );
        assert!(redelivered_after_restart(temp_dir.path()).is_empty());
    }

    /// Scenario: With `forward_after_ingest: true`, downstream permanently
    /// NACKs a bundle sent during ingest before its segment is finalized; a
    /// timer tick then finalizes the segment, and the processor is restarted.
    /// Guarantees: The segment path does not send the bundle again, the
    /// bundle is counted as rejected, and it is rejected in Quiver once the
    /// segment is visible, so a restart does not deliver it again.
    #[test]
    fn forward_after_ingest_permanent_nack_before_segment_is_visible() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let metrics = answer_early_bundle_before_segment_is_visible(
            "durable-buffer-forward-reject-early",
            temp_dir.path(),
            |out| NodeControlMsg::Nack(downstream_nack(out, true)),
        );
        assert_eq!(metric(&metrics, "processor.durable_buffer/in.flight"), 0);
        assert_eq!(
            metric(&metrics, "processor.durable_buffer.items/rejected"),
            1
        );
        assert!(redelivered_after_restart(temp_dir.path()).is_empty());
    }

    /// Scenario: With `forward_after_ingest: true` and zero retry backoff,
    /// downstream transiently NACKs a bundle sent during ingest. The retry
    /// wakeup fires while the segment is still open, a timer tick then
    /// finalizes the segment, and the rescheduled retry wakeup fires after
    /// its deadline.
    /// Guarantees: The retry is kept (rescheduled) while the segment is not
    /// visible, the timer tick does not bypass the retry backoff, the bundle
    /// is redelivered exactly once by the retry, and after its ACK nothing is
    /// delivered on restart.
    #[test]
    fn forward_after_ingest_transient_nack_before_segment_is_visible() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-forward-retry-early",
            temp_dir.path(),
            serde_json::json!({
                "forward_after_ingest": true,
                "max_segment_open_duration": "1h",
                "initial_retry_interval": "0s",
                "max_retry_interval": "0s",
            }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, _rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("ingest");
                let mut outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1);
                let nack = downstream_nack(outputs.remove(0), false);
                ctx.process(Message::Control(NodeControlMsg::Nack(nack)))
                    .await
                    .expect("nack");

                assert!(ctx.fire_wakeup().await.expect("retry wakeup"));
                assert!(
                    ctx.drain_pdata().await.is_empty(),
                    "segment is not visible yet"
                );
                let deadline = ctx
                    .next_local_control_deadline()
                    .expect("retry rescheduled while the segment is open");

                timer_tick(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty(), "retry backoff is kept");

                ctx.sleep(deadline.saturating_duration_since(Instant::now()))
                    .await;
                assert!(ctx.fire_wakeup().await.expect("retry wakeup"));
                let mut retried = ctx.drain_pdata().await;
                assert_eq!(retried.len(), 1, "redelivered once");
                assert_eq!(log_ids_in_output(&mut retried[0]), ["m-0"]);

                let (ack, _) = downstream_ack(retried.remove(0));
                ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                    .await
                    .expect("ack");
                timer_tick(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty());
                shutdown_now(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty());
            })
            .validate(|_| async {});

        assert!(redelivered_after_restart(temp_dir.path()).is_empty());
    }

    /// Scenario: With `forward_after_ingest: true` and `max_in_flight: 1`, a
    /// second bundle is ingested while the first, sent during ingest, is
    /// still in flight. Both are then ACKed in turn across timer ticks.
    /// Guarantees: The second bundle is not sent during ingest; it is sent by
    /// the segment path only after the first is ACKed. Each bundle is sent
    /// exactly once and nothing is delivered after a restart.
    #[test]
    fn forward_after_ingest_falls_back_at_max_in_flight() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-forward-max-in-flight",
            temp_dir.path(),
            serde_json::json!({
                "forward_after_ingest": true,
                "max_segment_open_duration": "1h",
                "max_in_flight": 1,
            }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, _rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                for id in 0..2 {
                    ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                        .await
                        .expect("ingest");
                }
                let mut first = ctx.drain_pdata().await;
                assert_eq!(first.len(), 1, "only the first bundle is sent early");
                assert_eq!(log_ids_in_output(&mut first[0]), ["m-0"]);

                timer_tick(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty(), "at max_in_flight");

                let (ack, _) = downstream_ack(first.remove(0));
                ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                    .await
                    .expect("ack");
                timer_tick(&mut ctx).await;
                let mut second = ctx.drain_pdata().await;
                assert_eq!(second.len(), 1);
                assert_eq!(log_ids_in_output(&mut second[0]), ["m-1"]);

                let (ack, _) = downstream_ack(second.remove(0));
                ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                    .await
                    .expect("ack");
                timer_tick(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty());

                let metrics = durable_buffer_metrics(&mut ctx).await;
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer/early.forwards"),
                    1
                );
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer.items/produced"),
                    2
                );
                shutdown_now(&mut ctx).await;
            })
            .validate(|_| async {});

        assert!(redelivered_after_restart(temp_dir.path()).is_empty());
    }

    /// Scenario: With `forward_after_ingest: true`, more bundles are ingested
    /// than the downstream channel can hold while nothing drains it; the
    /// channel is then drained and timer ticks finalize the segment.
    /// Guarantees: Bundles that did not fit are not sent during ingest and are
    /// delivered by the segment path instead; across both paths every bundle
    /// is sent exactly once, and `early.forwards` counts only the bundles
    /// that fit.
    #[test]
    fn forward_after_ingest_falls_back_when_channel_is_full() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use std::collections::BTreeSet;

        const BUNDLES: usize = 150;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-forward-channel-full",
            temp_dir.path(),
            serde_json::json!({ "forward_after_ingest": true, "max_segment_open_duration": "1h" }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, _rx) = pipeline_completion_msg_channel(BUNDLES * 2);
                ctx.set_pipeline_completion_sender(tx);

                for id in 0..BUNDLES {
                    ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                        .await
                        .expect("ingest");
                }
                let mut early = ctx.drain_pdata().await;
                assert!(!early.is_empty() && early.len() < BUNDLES);
                let metrics = durable_buffer_metrics(&mut ctx).await;
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer/early.forwards"),
                    early.len() as u64
                );

                let mut sent: Vec<String> = early.iter_mut().flat_map(log_ids_in_output).collect();
                // The drain loop of one tick is time budgeted, so tick until
                // nothing more is sent.
                loop {
                    timer_tick(&mut ctx).await;
                    let mut outputs = ctx.drain_pdata().await;
                    if outputs.is_empty() {
                        break;
                    }
                    sent.extend(outputs.iter_mut().flat_map(log_ids_in_output));
                }

                let unique: BTreeSet<_> = sent.iter().cloned().collect();
                assert_eq!(sent.len(), BUNDLES, "no bundle is sent twice");
                assert_eq!(unique.len(), BUNDLES, "every bundle is sent");
                let metrics = durable_buffer_metrics(&mut ctx).await;
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer.items/produced"),
                    (BUNDLES - early.len()) as u64
                );
            })
            .validate(|_| async {});
    }

    /// Ingests `input` twice into a durable buffer with early forwarding on
    /// and `max_in_flight: 1`, so the first copy is sent right after ingest
    /// and the second, in its own segment, is sent by the segment path.
    /// Returns both sent pdata, early first.
    fn early_and_segment_outputs(name: &'static str, input: OtapPdata) -> (OtapPdata, OtapPdata) {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let result = Arc::new(std::sync::Mutex::new(None));
        let result_in_test = Arc::clone(&result);
        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            name,
            temp_dir.path(),
            serde_json::json!({
                "forward_after_ingest": true,
                "max_segment_open_duration": "1h",
                "max_in_flight": 1,
            }),
        );
        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, _rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                ctx.process(Message::PData(input.clone()))
                    .await
                    .expect("ingest");
                let mut early = ctx.drain_pdata().await;
                assert_eq!(early.len(), 1, "sent during ingest");
                let early = early.remove(0);

                // Finalize the first segment so the second copy gets its own.
                timer_tick(&mut ctx).await;
                ctx.process(Message::PData(input)).await.expect("ingest");
                assert!(ctx.drain_pdata().await.is_empty(), "at max_in_flight");

                let (ack, _) = downstream_ack(early.clone());
                ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                    .await
                    .expect("ack");
                timer_tick(&mut ctx).await;
                let mut from_segment = ctx.drain_pdata().await;
                assert_eq!(from_segment.len(), 1, "sent from the segment");
                *result_in_test.lock().expect("outputs") = Some((early, from_segment.remove(0)));
            })
            .validate(|_| async {});
        let outputs = result.lock().expect("outputs").take();
        outputs.expect("both outputs captured")
    }

    /// Scenario: The same pluggable Arrow record set is sent once right after
    /// ingest and once from its finalized segment.
    /// Guarantees: Both sends carry the same signal type, payload format and
    /// record set (identity and tables), equal to the input.
    #[test]
    fn forward_after_ingest_matches_segment_path_for_pluggable_arrow() {
        let records = pluggable_test_records(3);
        let (early, from_segment) = early_and_segment_outputs(
            "durable-buffer-forward-eq-pluggable",
            OtapPdata::new_default(OtapPayload::from_pdata_arrow(records.clone())),
        );
        assert_eq!(early.signal_type(), from_segment.signal_type());
        assert_eq!(
            early.payload_ref().format(),
            from_segment.payload_ref().format()
        );
        assert_eq!(early.payload_ref().pdata_arrow_ref(), Some(&records));
        assert_eq!(from_segment.payload_ref().pdata_arrow_ref(), Some(&records));
    }

    /// Scenario: The same OTLP logs request, stored in pass-through mode, is
    /// sent once right after ingest and once from its finalized segment.
    /// Guarantees: Both sends carry the same signal type, payload format and
    /// encoded OTLP bytes.
    #[test]
    fn forward_after_ingest_matches_segment_path_for_otlp_pass_through() {
        let (early, from_segment) =
            early_and_segment_outputs("durable-buffer-forward-eq-otlp", make_log_input("m-0", 0));
        assert_eq!(early.signal_type(), from_segment.signal_type());
        assert_eq!(
            early.payload_ref().format(),
            from_segment.payload_ref().format()
        );
        let early_bytes = early.payload_ref().clone().into_encoded_bytes();
        assert!(early_bytes.is_some());
        assert_eq!(
            early_bytes,
            from_segment.payload_ref().clone().into_encoded_bytes()
        );
    }

    /// Scenario: The same OTAP Arrow logs batch is materialized, stored, and
    /// sent once right after ingest and once from its finalized segment.
    /// Guarantees: Both sends carry the same signal type, payload format and
    /// OTAP record batches, including schemas.
    #[test]
    fn forward_after_ingest_matches_segment_path_for_otap() {
        use otel_arrow_dfe_pdata::encode::encode_logs_otap_batch;
        use otel_arrow_dfe_pdata::testing::fixtures::DataGenerator;

        let logs = DataGenerator::new(1).generate_logs();
        let records = encode_logs_otap_batch(&logs).expect("encode logs");
        let (early, from_segment) = early_and_segment_outputs(
            "durable-buffer-forward-eq-otap",
            OtapPdata::new_default(records.into()),
        );
        assert_eq!(early.signal_type(), from_segment.signal_type());
        assert_eq!(
            early.payload_ref().format(),
            from_segment.payload_ref().format()
        );
        let early_records = early.payload_ref().otap_ref().expect("OTAP payload");
        assert_eq!(
            Some(early_records),
            from_segment.payload_ref().otap_ref(),
            "OTAP records differ between paths"
        );
    }

    /// Scenario: `forward_after_ingest: true` is combined with
    /// `upstream_ack: after_wal_sync` and a WAL flush interval long enough
    /// that only the deadline wakeup syncs the WAL. A bundle is ingested, the
    /// sync deadline wakeup fires, and downstream then ACKs the bundle.
    /// Guarantees: The bundle is sent downstream right after ingest while its
    /// upstream ACK is still held; the wakeup releases the upstream ACK
    /// without sending anything; and the segment path does not send the
    /// bundle again.
    #[test]
    fn forward_after_ingest_with_after_wal_sync() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-forward-wal-sync",
            temp_dir.path(),
            serde_json::json!({
                "forward_after_ingest": true,
                "upstream_ack": "after_wal_sync",
                "wal_flush_interval": "1h",
                "max_segment_open_duration": "1h",
            }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("ingest");
                let mut outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1, "sent during ingest");
                let (acked, nacked) = drain_upstream_completions(&mut rx);
                assert!(
                    acked.is_empty() && nacked.is_empty(),
                    "upstream ACK is held"
                );

                assert!(ctx.fire_wakeup().await.expect("WAL sync wakeup"));
                assert_eq!(
                    drain_upstream_completions(&mut rx).0,
                    expected_calldata(0..1)
                );
                assert!(ctx.drain_pdata().await.is_empty());
                assert_eq!(wal_sync_counters(&mut ctx).await, (1, 0));

                let (ack, _) = downstream_ack(outputs.remove(0));
                ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                    .await
                    .expect("ack");
                timer_tick(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty(), "no second send");
                shutdown_now(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty());
            })
            .validate(|_| async {});

        assert!(redelivered_after_restart(temp_dir.path()).is_empty());
    }

    /// Environment variable that turns [`forward_after_ingest_crash_child`]
    /// into the child process of
    /// [`forward_after_ingest_acked_bundles_survive_process_kill`]. Its value
    /// is the durable buffer storage path.
    const FORWARD_CRASH_CHILD_DIR: &str = "OTAP_DURABLE_BUFFER_FORWARD_CRASH_DIR";

    /// Line prefix the crash child prints for each bundle id ACKed upstream.
    const FORWARD_CRASH_ACKED: &str = "forward-crash-acked:";

    /// Line the crash child prints once it is ready to be killed.
    const FORWARD_CRASH_READY: &str = "forward-crash-ready";

    /// Scenario: Runs as the child process spawned by
    /// [`forward_after_ingest_acked_bundles_survive_process_kill`]: with
    /// `forward_after_ingest: true` it ingests three batches, finalizing the
    /// segments of the first two, while downstream never answers. It then
    /// waits to be killed. As an ordinary test it does nothing.
    /// Guarantees: Every bundle is sent downstream during ingest and its id is
    /// printed only after its upstream ACK was received; the ready line is
    /// printed after all of them.
    #[test]
    fn forward_after_ingest_crash_child() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use std::io::Write as _;

        let Some(dir) = std::env::var_os(FORWARD_CRASH_CHILD_DIR) else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);

        let rt = TestRuntime::new();
        // `wal_flush_interval: 0s` completes each WAL write before its ACK,
        // so the kill cannot race a WAL write still in progress. Survival of
        // an OS crash is covered by the `after_wal_sync` tests.
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-forward-crash-child",
            &dir,
            serde_json::json!({
                "forward_after_ingest": true,
                "wal_flush_interval": "0s",
                "max_segment_open_duration": "1h",
            }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(64);
                ctx.set_pipeline_completion_sender(tx);
                let mut stdout = std::io::stdout();

                for batch in 0..3 {
                    for id in batch * 4..batch * 4 + 4 {
                        ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                            .await
                            .expect("ingest");
                    }
                    assert_eq!(ctx.drain_pdata().await.len(), 4, "sent during ingest");
                    let (acked, nacked) = drain_upstream_completions(&mut rx);
                    assert!(nacked.is_empty());
                    assert_eq!(acked, expected_calldata(batch * 4..batch * 4 + 4));
                    for id in batch * 4..batch * 4 + 4 {
                        writeln!(stdout, "{FORWARD_CRASH_ACKED}m-{id}").expect("report ack");
                    }
                    stdout.flush().expect("flush acks");
                    if batch < 2 {
                        timer_tick(&mut ctx).await;
                        assert!(ctx.drain_pdata().await.is_empty(), "no second send");
                    }
                }

                writeln!(stdout, "{FORWARD_CRASH_READY}").expect("report ready");
                stdout.flush().expect("flush ready");
                // Block until the parent kills this process.
                loop {
                    std::thread::park();
                }
            })
            .validate(|_| async {});
    }

    /// Scenario: A child process runs a durable buffer with
    /// `forward_after_ingest: true` whose downstream never answers, reports
    /// each bundle id it received an upstream ACK for, and is killed while
    /// some bundles sit in finalized segments and others only in the open
    /// segment and WAL. The parent then reopens the same storage and drains
    /// it.
    /// Guarantees: Every bundle the child ACKed upstream is delivered after
    /// the process is killed, even though each was already sent downstream
    /// once before the kill.
    #[test]
    fn forward_after_ingest_acked_bundles_survive_process_kill() {
        use std::collections::BTreeSet;
        use std::io::BufRead as _;
        use std::process::{Command, Stdio};

        /// Kills and reaps the child on every exit path, including failures.
        struct KillOnDrop(std::process::Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let child_test = concat!(module_path!(), "::forward_after_ingest_crash_child")
            .split_once("::")
            .expect("crate-qualified module path")
            .1;
        let mut child = KillOnDrop(
            Command::new(std::env::current_exe().expect("test executable"))
                .args(["--exact", child_test, "--nocapture", "--test-threads=1"])
                .env(FORWARD_CRASH_CHILD_DIR, temp_dir.path())
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .spawn()
                .expect("start crash child"),
        );

        // Read the child's report on a separate thread so a stuck child fails
        // the test after a bounded wait instead of hanging it.
        let stdout = child.0.stdout.take().expect("child stdout");
        let (report_tx, report_rx) = std::sync::mpsc::channel();
        let _reader = std::thread::spawn(move || {
            let mut acked = BTreeSet::new();
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                // libtest may print the test name on the same line as the
                // first report, so match the markers anywhere in the line.
                if let Some((_, id)) = line.split_once(FORWARD_CRASH_ACKED) {
                    let _ = acked.insert(id.trim().to_owned());
                } else if line.contains(FORWARD_CRASH_READY) {
                    let _ = report_tx.send(Some(acked));
                    return;
                }
            }
            let _ = report_tx.send(None);
        });
        let acked = report_rx
            .recv_timeout(Duration::from_secs(120))
            .expect("crash child reports within the timeout")
            .expect("crash child exited before it was ready");
        assert_eq!(acked.len(), 12, "the child should ACK three batches");

        child.0.kill().expect("kill crash child");
        let status = child.0.wait().expect("reap crash child");
        assert!(!status.success(), "the child must not exit cleanly");
        drop(child);

        let delivered: BTreeSet<_> = redelivered_after_restart(temp_dir.path())
            .into_iter()
            .collect();
        let lost: Vec<_> = acked.difference(&delivered).collect();
        assert!(
            lost.is_empty(),
            "ACKed bundles lost after the crash: {lost:?}"
        );
    }

    /// Scenario: With `forward_after_ingest: true`, three bundles are sent
    /// during ingest and downstream ACKs the first while its segment is open.
    /// Finalizing the segment then fails because the segment directory was
    /// replaced by a regular file. Afterwards downstream ACKs the second
    /// bundle and transiently NACKs the third with zero backoff, the
    /// directory is restored, and the processor is shut down and reopened.
    /// Guarantees: No bookkeeping is left behind for the failed segment: the
    /// recorded ACK is dropped, the later ACK and NACK are absorbed, the
    /// retry is dropped instead of being rescheduled forever (no wakeup stays
    /// armed), `in.flight` returns to 0, and nothing is sent again. The data
    /// is recovered from the WAL after the restart.
    #[test]
    fn forward_after_ingest_cleans_up_after_failed_finalization() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let segments = temp_dir.path().join("core_0").join("segments");
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-forward-finalize-failure",
            temp_dir.path(),
            serde_json::json!({
                "forward_after_ingest": true,
                "max_segment_open_duration": "1h",
                "initial_retry_interval": "0s",
                "max_retry_interval": "0s",
            }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, _rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                for id in 0..3 {
                    ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                        .await
                        .expect("ingest");
                }
                let mut outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 3);
                let (ack, _) = downstream_ack(outputs.remove(0));
                ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                    .await
                    .expect("ack");

                std::fs::remove_dir_all(&segments).expect("remove segments dir");
                std::fs::write(&segments, b"not a directory").expect("block segments dir");
                timer_tick(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty());

                let (ack, _) = downstream_ack(outputs.remove(0));
                ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                    .await
                    .expect("ack");
                let nack = downstream_nack(outputs.remove(0), false);
                ctx.process(Message::Control(NodeControlMsg::Nack(nack)))
                    .await
                    .expect("nack");
                assert!(ctx.fire_wakeup().await.expect("retry wakeup"));
                assert!(ctx.drain_pdata().await.is_empty());
                assert!(
                    !ctx.fire_wakeup().await.expect("no wakeup"),
                    "the retry must not stay armed"
                );

                let metrics = durable_buffer_metrics(&mut ctx).await;
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer/flush.failures"),
                    1
                );
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer/early.forwards"),
                    3
                );
                assert_eq!(metric(&metrics, "processor.durable_buffer/in.flight"), 0);

                std::fs::remove_file(&segments).expect("unblock segments dir");
                std::fs::create_dir(&segments).expect("restore segments dir");
                timer_tick(&mut ctx).await;
                shutdown_now(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty());
            })
            .validate(|_| async {});

        assert_eq!(
            redelivered_after_restart(temp_dir.path()),
            ["m-0", "m-1", "m-2"]
        );
    }

    /// Scenario: Downstream ACKs a bundle forwarded right after ingest while
    /// its segment is still open, finalizing that segment then fails, and
    /// downstream permanently NACKs a second bundle of the same segment.
    /// Guarantees: The recorded ACK is kept while the segment is pending and
    /// removed by the next sweep once the segment can never become visible;
    /// the later NACK is not recorded. Early forward bookkeeping does not
    /// leak after a failed finalization.
    #[tokio::test]
    async fn early_outcomes_are_dropped_when_segment_finalization_fails() {
        let (mut processor, engine, subscriber_id, temp_dir) = setup_test_processor(None).await;
        processor.engine_state = EngineState::Ready {
            engine: Arc::clone(&engine),
            subscriber_id,
        };
        let first = engine
            .ingest_with_ref(&make_simple_bundle(SlotId::new(30), 1))
            .await
            .expect("ingest");
        let second = engine
            .ingest_with_ref(&make_simple_bundle(SlotId::new(30), 1))
            .await
            .expect("ingest");

        processor
            .resolve_early(first, EarlyOutcome::Acked)
            .expect("record ACK");
        processor.sweep_early_outcomes().expect("sweep");
        assert_eq!(processor.early_outcomes.len(), 1, "kept while pending");

        let segments = temp_dir.path().join("core_0").join("segments");
        std::fs::remove_dir_all(&segments).expect("remove segments dir");
        std::fs::write(&segments, b"not a directory").expect("block segments dir");
        assert!(engine.flush().await.is_err(), "finalization must fail");
        assert_eq!(
            engine.segment_visibility(first.segment_seq),
            SegmentVisibility::Unavailable
        );

        processor
            .resolve_early(second, EarlyOutcome::Rejected)
            .expect("drop NACK");
        processor.sweep_early_outcomes().expect("sweep");
        assert!(processor.early_outcomes.is_empty(), "no entry may leak");

        std::fs::remove_file(&segments).expect("unblock segments dir");
    }

    /// Scenario: one transient NACK arms a normal processor-local wakeup and the
    /// wakeup control message is later delivered through the processor inbox.
    /// Guarantees: the retry stays deferred until the wakeup arrives, and that
    /// wakeup resumes normal downstream delivery exactly once.
    #[test]
    fn test_retry_wakeup_resumes_retry_logic() {
        use otel_arrow_dfe_config::node::NodeUserConfig;
        use otel_arrow_dfe_engine::config::ProcessorConfig;
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::message::Message;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use otel_arrow_dfe_engine::testing::test_node;
        use otel_arrow_dfe_otap::testing::next_nack;
        use otel_arrow_dfe_pdata::encode::encode_logs_otap_batch;
        use otel_arrow_dfe_pdata::testing::fixtures::DataGenerator;
        use serde_json::json;

        let rt = TestRuntime::new();
        let controller = ControllerContext::new(rt.metrics_registry());
        let pipeline_ctx = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
        let temp_dir = tempfile::tempdir().expect("tempdir");

        let mut node_config = NodeUserConfig::new_processor_config(DURABLE_BUFFER_URN);
        node_config.config = json!({
            "path": temp_dir.path(),
            "retention_size_cap": "256 MiB",
            "poll_interval": "100ms",
            "max_segment_open_duration": "1s",
            "initial_retry_interval": "100ms",
            "max_retry_interval": "100ms",
            "retry_multiplier": 2.0,
            "max_in_flight": 1000
        });

        let processor = create_durable_buffer(
            pipeline_ctx,
            test_node("durable-buffer-retry-wakeup"),
            Arc::new(node_config),
            &ProcessorConfig::new("durable-buffer-retry-wakeup"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("create durable buffer");

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (pipeline_completion_tx, _pipeline_completion_rx) =
                    pipeline_completion_msg_channel(10);
                ctx.set_pipeline_completion_sender(pipeline_completion_tx);

                let mut datagen = DataGenerator::new(1);
                let input = datagen.generate_logs();
                let rec = encode_logs_otap_batch(&input).expect("encode logs");
                ctx.process(Message::PData(OtapPdata::new_default(rec.into())))
                    .await
                    .expect("process input");

                ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                    .await
                    .expect("process timer tick");
                let mut outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1, "timer tick should emit one bundle");

                let sent = outputs.pop().expect("sent bundle");
                let (_, nack) =
                    next_nack(NackMsg::new("retry", sent)).expect("expected nack subscriber");
                ctx.process(Message::Control(NodeControlMsg::Nack(nack)))
                    .await
                    .expect("process nack");
                assert!(
                    ctx.drain_pdata().await.is_empty(),
                    "nack should defer delivery until wakeup"
                );

                ctx.sleep(Duration::from_millis(200)).await;
                ctx.process(Message::Control(NodeControlMsg::Wakeup {
                    slot: RETRY_WAKEUP_SLOT,
                    when: Instant::now(),
                    revision: 0,
                }))
                .await
                .expect("process retry wakeup");

                let retried = ctx.drain_pdata().await;
                assert_eq!(retried.len(), 1, "wakeup should resume retry delivery");
                assert_eq!(retried[0].signal_type(), SignalType::Logs);
            })
            .validate(|_| async {});
    }

    /// Scenario: an unrelated wakeup arrives while durable-buffer has multiple
    /// deferred retries pending behind its single retry wakeup slot.
    /// Guarantees: the unrelated wakeup does not cause early redelivery or lose
    /// deferred retries; the matching wakeup later resumes all due retries.
    #[test]
    fn test_unknown_wakeup_does_not_lose_deferred_retries() {
        use otel_arrow_dfe_config::node::NodeUserConfig;
        use otel_arrow_dfe_engine::config::ProcessorConfig;
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::message::Message;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use otel_arrow_dfe_engine::testing::test_node;
        use otel_arrow_dfe_otap::testing::next_nack;
        use otel_arrow_dfe_pdata::encode::encode_logs_otap_batch;
        use otel_arrow_dfe_pdata::testing::fixtures::DataGenerator;
        use serde_json::json;

        let rt = TestRuntime::new();
        let controller = ControllerContext::new(rt.metrics_registry());
        let pipeline_ctx = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
        let temp_dir = tempfile::tempdir().expect("tempdir");

        let mut node_config = NodeUserConfig::new_processor_config(DURABLE_BUFFER_URN);
        node_config.config = json!({
            "path": temp_dir.path(),
            "retention_size_cap": "256 MiB",
            "poll_interval": "50ms",
            "max_segment_open_duration": "1s",
            "initial_retry_interval": "100ms",
            "max_retry_interval": "100ms",
            "retry_multiplier": 2.0,
            "max_in_flight": 1000
        });

        let processor = create_durable_buffer(
            pipeline_ctx,
            test_node("durable-buffer-unknown-wakeup"),
            Arc::new(node_config),
            &ProcessorConfig::with_channel_capacities("durable-buffer-unknown-wakeup", 1, 100),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("create durable buffer");

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (pipeline_completion_tx, _pipeline_completion_rx) =
                    pipeline_completion_msg_channel(10);
                ctx.set_pipeline_completion_sender(pipeline_completion_tx);

                let mut datagen = DataGenerator::new(2);
                for _ in 0..2 {
                    let input = datagen.generate_logs();
                    let rec = encode_logs_otap_batch(&input).expect("encode logs");
                    ctx.process(Message::PData(OtapPdata::new_default(rec.into())))
                        .await
                        .expect("process input");
                }

                ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                    .await
                    .expect("process timer tick");
                let mut outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 2, "timer tick should emit two bundles");
                for sent in outputs.drain(..) {
                    let (_, nack) =
                        next_nack(NackMsg::new("retry", sent)).expect("expected nack subscriber");
                    ctx.process(Message::Control(NodeControlMsg::Nack(nack)))
                        .await
                        .expect("process nack");
                }

                ctx.process(Message::Control(NodeControlMsg::Wakeup {
                    slot: WakeupSlot(999),
                    when: Instant::now(),
                    revision: 0,
                }))
                .await
                .expect("process unknown wakeup");
                assert!(
                    ctx.drain_pdata().await.is_empty(),
                    "unknown wakeup should not redeliver deferred retries"
                );

                ctx.sleep(Duration::from_millis(200)).await;
                ctx.process(Message::Control(NodeControlMsg::Wakeup {
                    slot: RETRY_WAKEUP_SLOT,
                    when: Instant::now(),
                    revision: 0,
                }))
                .await
                .expect("process shared retry wakeup");
                let retried = ctx.drain_pdata().await;
                assert_eq!(
                    retried.len(),
                    2,
                    "matching wakeup should resume all due deferred retries"
                );
            })
            .validate(|_| async {});
    }

    /// Scenario: two transient NACKs occur and both retries share the durable
    /// buffer's single wakeup slot.
    /// Guarantees: no retry is re-delivered before the shared wakeup fires, and
    /// one matching wakeup resumes all due retries.
    #[test]
    fn test_multiple_retries_share_single_wakeup() {
        use otel_arrow_dfe_config::node::NodeUserConfig;
        use otel_arrow_dfe_engine::config::ProcessorConfig;
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::message::Message;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use otel_arrow_dfe_engine::testing::test_node;
        use otel_arrow_dfe_otap::testing::next_nack;
        use otel_arrow_dfe_pdata::encode::encode_logs_otap_batch;
        use otel_arrow_dfe_pdata::testing::fixtures::DataGenerator;
        use serde_json::json;

        let rt = TestRuntime::new();
        let controller = ControllerContext::new(rt.metrics_registry());
        let pipeline_ctx = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
        let temp_dir = tempfile::tempdir().expect("tempdir");

        let mut node_config = NodeUserConfig::new_processor_config(DURABLE_BUFFER_URN);
        node_config.config = json!({
            "path": temp_dir.path(),
            "retention_size_cap": "256 MiB",
            "poll_interval": "50ms",
            "max_segment_open_duration": "1s",
            "initial_retry_interval": "100ms",
            "max_retry_interval": "100ms",
            "retry_multiplier": 2.0,
            "max_in_flight": 1000
        });

        let processor = create_durable_buffer(
            pipeline_ctx,
            test_node("durable-buffer-shared-retry-wakeup"),
            Arc::new(node_config),
            &ProcessorConfig::with_channel_capacities("durable-buffer-shared-retry-wakeup", 1, 100),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("create durable buffer");

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (pipeline_completion_tx, _pipeline_completion_rx) =
                    pipeline_completion_msg_channel(10);
                ctx.set_pipeline_completion_sender(pipeline_completion_tx);

                let mut datagen = DataGenerator::new(2);
                for _ in 0..2 {
                    let input = datagen.generate_logs();
                    let rec = encode_logs_otap_batch(&input).expect("encode logs");
                    ctx.process(Message::PData(OtapPdata::new_default(rec.into())))
                        .await
                        .expect("process input");
                }

                ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                    .await
                    .expect("process timer tick");
                let mut outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 2, "timer tick should emit two bundles");
                for sent in outputs.drain(..) {
                    let (_, nack) =
                        next_nack(NackMsg::new("retry", sent)).expect("expected nack subscriber");
                    ctx.process(Message::Control(NodeControlMsg::Nack(nack)))
                        .await
                        .expect("process nack");
                }

                ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                    .await
                    .expect("process immediate timer tick");
                assert!(
                    ctx.drain_pdata().await.is_empty(),
                    "shared wakeup retries should stay deferred until due"
                );

                ctx.sleep(Duration::from_millis(200)).await;
                ctx.process(Message::Control(NodeControlMsg::Wakeup {
                    slot: RETRY_WAKEUP_SLOT,
                    when: Instant::now(),
                    revision: 0,
                }))
                .await
                .expect("process shared retry wakeup");
                let wakeup_retry = ctx.drain_pdata().await;
                assert_eq!(
                    wakeup_retry.len(),
                    2,
                    "shared wakeup should resume all due retry deliveries"
                );
            })
            .validate(|_| async {});
    }

    /// Scenario: a bundle is transiently NACKed, becomes deferred behind the
    /// durable-buffer retry wakeup, and shutdown starts before that wakeup
    /// fires.
    /// Guarantees: shutdown clears deferred-retry gating so the existing drain
    /// loop can forward that parked bundle instead of leaving it restart-dependent.
    #[test]
    fn test_shutdown_drains_deferred_retry_bundle() {
        use otel_arrow_dfe_config::node::NodeUserConfig;
        use otel_arrow_dfe_engine::config::ProcessorConfig;
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::message::Message;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use otel_arrow_dfe_engine::testing::test_node;
        use otel_arrow_dfe_otap::testing::next_nack;
        use otel_arrow_dfe_pdata::encode::encode_logs_otap_batch;
        use otel_arrow_dfe_pdata::testing::fixtures::DataGenerator;
        use serde_json::json;

        let rt = TestRuntime::new();
        let controller = ControllerContext::new(rt.metrics_registry());
        let pipeline_ctx = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
        let temp_dir = tempfile::tempdir().expect("tempdir");

        let mut node_config = NodeUserConfig::new_processor_config(DURABLE_BUFFER_URN);
        node_config.config = json!({
            "path": temp_dir.path(),
            "retention_size_cap": "256 MiB",
            "poll_interval": "100ms",
            "max_segment_open_duration": "1s",
            "initial_retry_interval": "10s",
            "max_retry_interval": "10s",
            "retry_multiplier": 2.0,
            "max_in_flight": 1000
        });

        let processor = create_durable_buffer(
            pipeline_ctx,
            test_node("durable-buffer-shutdown-drain-deferred"),
            Arc::new(node_config),
            &ProcessorConfig::new("durable-buffer-shutdown-drain-deferred"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("create durable buffer");

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (pipeline_completion_tx, _pipeline_completion_rx) =
                    pipeline_completion_msg_channel(10);
                ctx.set_pipeline_completion_sender(pipeline_completion_tx);

                let mut datagen = DataGenerator::new(1);
                let input = datagen.generate_logs();
                let rec = encode_logs_otap_batch(&input).expect("encode logs");
                ctx.process(Message::PData(OtapPdata::new_default(rec.into())))
                    .await
                    .expect("process input");

                ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                    .await
                    .expect("process timer tick");
                let mut outputs = ctx.drain_pdata().await;
                assert_eq!(outputs.len(), 1, "timer tick should emit one bundle");

                let sent = outputs.pop().expect("sent bundle");
                let (_, nack) =
                    next_nack(NackMsg::new("retry", sent)).expect("expected nack subscriber");
                ctx.process(Message::Control(NodeControlMsg::Nack(nack)))
                    .await
                    .expect("process nack");
                assert!(
                    ctx.drain_pdata().await.is_empty(),
                    "nack should defer delivery until either wakeup or shutdown drain"
                );

                ctx.process(Message::Control(NodeControlMsg::Shutdown {
                    deadline: Instant::now() + Duration::from_secs(1),
                    reason: "shutdown".to_owned(),
                }))
                .await
                .expect("process shutdown");

                let drained = ctx.drain_pdata().await;
                assert_eq!(
                    drained.len(),
                    1,
                    "shutdown drain should forward the deferred retry bundle"
                );
                assert_eq!(drained[0].signal_type(), SignalType::Logs);
            })
            .validate(|_| async {});
    }

    /// Builds an OTLP pass-through logs input holding one log record whose
    /// `event_name` is `id`, subscribed for ACK/NACK with `subscriber_id`, so
    /// tests can track exactly which batches survive end to end.
    fn make_log_input(id: &str, subscriber_id: usize) -> OtapPdata {
        use otel_arrow_dfe_engine::Interests;
        use otel_arrow_dfe_otap::testing::TestCallData;
        use otel_arrow_dfe_pdata::OtlpProtoBytes;
        use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
            LogRecord, LogsData, ResourceLogs, ScopeLogs,
        };
        use prost::Message as _;

        let logs = LogsData {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        event_name: id.to_owned(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut bytes = Vec::new();
        logs.encode(&mut bytes).expect("encode logs");
        OtapPdata::new_default(OtlpProtoBytes::ExportLogsRequest(bytes.into()).into())
            .test_subscribe_to(
                Interests::ACKS | Interests::NACKS,
                TestCallData::new_with(subscriber_id as u64, 0).into(),
                999,
            )
    }

    /// Extracts the log record `event_name` ids from a pass-through OTLP
    /// output produced from [`make_log_input`] batches.
    fn log_ids_in_output(out: &mut OtapPdata) -> Vec<String> {
        use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::LogsData;
        use prost::Message as _;

        let bytes = match out.payload_ref().clone().into_encoded_bytes() {
            Some(b) => b,
            None => return Vec::new(),
        };
        let logs = LogsData::decode(bytes.as_ref()).expect("decode logs output");
        logs.resource_logs
            .iter()
            .flat_map(|rl| &rl.scope_logs)
            .flat_map(|sl| &sl.log_records)
            .map(|lr| lr.event_name.clone())
            .collect()
    }

    /// Scenario: a process is killed mid-WAL-rotation (leaving `quiver.wal.1`
    /// plus an empty active `quiver.wal`), the durable buffer restarts on the
    /// same directory, the first segment flush purges `quiver.wal.1`, and then
    /// more batches are ingested, acked upstream, and the processor is shut down.
    /// Guarantees: every batch the processor ACKed upstream is actually delivered
    /// downstream. Regression guard for the "restart at WAL position 0" bug,
    /// where a post-purge segment flush is rejected with "safe offset regressed",
    /// so the segment is never handed to the subscriber even though ingest ACKed
    /// the data -- silent data loss of acknowledged batches.
    #[test]
    fn crash_mid_wal_rotation_does_not_lose_acked_data_after_purge() {
        use otel_arrow_dfe_config::node::NodeUserConfig;
        use otel_arrow_dfe_engine::config::ProcessorConfig;
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_engine::control::{
            PipelineCompletionMsg, pipeline_completion_msg_channel,
        };
        use otel_arrow_dfe_engine::message::Message;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use otel_arrow_dfe_engine::testing::test_node;
        use otel_arrow_dfe_otap::testing::{TestCallData, next_ack};
        use serde_json::json;
        use std::collections::BTreeSet;
        use std::sync::{Arc, Mutex};

        fn node_config_json(path: &std::path::Path) -> NodeUserConfig {
            let mut node_config = NodeUserConfig::new_processor_config(DURABLE_BUFFER_URN);
            node_config.config = json!({
                "path": path,
                "retention_size_cap": "256 MiB",
                "poll_interval": "100ms",
                "max_segment_open_duration": "1s",
                "initial_retry_interval": "100ms",
                "max_retry_interval": "100ms",
                "retry_multiplier": 2.0,
                "max_in_flight": 1000
            });
            node_config
        }

        // One TempDir shared across all runs (the crash state lives on disk
        // between runs).
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let dir_path = temp_dir.path().to_path_buf();

        // Collected results, shared out of the run closures.
        let run1_delivered = Arc::new(Mutex::new(BTreeSet::<String>::new()));
        let run2_acked = Arc::new(Mutex::new(BTreeSet::<String>::new()));
        let run2_delivered = Arc::new(Mutex::new(BTreeSet::<String>::new()));
        let run2_wal1_purged = Arc::new(Mutex::new(false));
        let run3_delivered = Arc::new(Mutex::new(Vec::<String>::new()));

        // ---------------------------------------------------------------
        // Run 1: build up a long WAL history, deliver + ack it, shut down.
        // ---------------------------------------------------------------
        {
            let rt = TestRuntime::new();
            let controller = ControllerContext::new(rt.metrics_registry());
            let pipeline_ctx =
                controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
            let processor = create_durable_buffer(
                pipeline_ctx,
                test_node("durable-buffer-crash-run1"),
                Arc::new(node_config_json(&dir_path)),
                &ProcessorConfig::new("durable-buffer-crash-run1"),
                &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
            )
            .expect("create durable buffer run1");

            let delivered = Arc::clone(&run1_delivered);
            rt.set_processor(processor)
                .run_test(move |mut ctx| async move {
                    let (tx, _rx) = pipeline_completion_msg_channel(256);
                    ctx.set_pipeline_completion_sender(tx);

                    for i in 0..20 {
                        let input = make_log_input(&format!("hist-{i:02}"), i);
                        ctx.process(Message::PData(input))
                            .await
                            .expect("ingest history");
                    }

                    // Finalize + deliver downstream.
                    ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                        .await
                        .expect("tick 1");

                    let mut outputs = ctx.drain_pdata().await;
                    for out in &mut outputs {
                        for id in log_ids_in_output(out) {
                            let _ = delivered.lock().unwrap().insert(id);
                        }
                    }
                    // Ack everything downstream so subscriber progress advances.
                    for out in outputs {
                        if let Some((_, ack)) = next_ack(AckMsg::new(out)) {
                            ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                                .await
                                .expect("downstream ack run1");
                        }
                    }
                    // Second tick persists subscriber progress via maintain().
                    ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                        .await
                        .expect("tick 1b");

                    ctx.process(Message::Control(NodeControlMsg::Shutdown {
                        deadline: Instant::now() + Duration::from_secs(1),
                        reason: "shutdown".to_owned(),
                    }))
                    .await
                    .expect("shutdown run1");
                })
                .validate(|_| async {});
        }

        // ---------------------------------------------------------------
        // Between runs: recreate the exact on-disk state left by a crash in
        // the middle of a WAL rotation -- the active file renamed to wal.1,
        // and a fresh empty active file.
        // ---------------------------------------------------------------
        let wal_dir = dir_path.join("core_0").join("wal");
        let active_wal = wal_dir.join("quiver.wal");
        let rotated_wal = wal_dir.join("quiver.wal.1");
        assert!(
            active_wal.exists(),
            "run1 should have produced an active WAL at {}",
            active_wal.display()
        );
        assert!(
            !rotated_wal.exists(),
            "run1 should not have rotated the WAL on its own"
        );
        std::fs::rename(&active_wal, &rotated_wal).expect("rename wal -> wal.1");
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&active_wal)
            .expect("create empty active wal");

        // ---------------------------------------------------------------
        // Run 2: restart on the same dir. One flush purges wal.1, then later
        // flushes are rejected ("safe offset regressed"), silently dropping
        // acked data.
        // ---------------------------------------------------------------
        {
            let rt = TestRuntime::new();
            let controller = ControllerContext::new(rt.metrics_registry());
            let pipeline_ctx =
                controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
            let processor = create_durable_buffer(
                pipeline_ctx,
                test_node("durable-buffer-crash-run2"),
                Arc::new(node_config_json(&dir_path)),
                &ProcessorConfig::new("durable-buffer-crash-run2"),
                &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
            )
            .expect("create durable buffer run2");

            let acked = Arc::clone(&run2_acked);
            let delivered = Arc::clone(&run2_delivered);
            let purged = Arc::clone(&run2_wal1_purged);
            let rotated_wal_check = rotated_wal.clone();
            rt.set_processor(processor)
                .run_test(move |mut ctx| async move {
                    let (tx, mut rx) = pipeline_completion_msg_channel(256);
                    ctx.set_pipeline_completion_sender(tx);

                    // Helper closures can't borrow ctx mutably across awaits, so
                    // inline the per-message steps.
                    // m-0 drives the first flush, which purges wal.1.
                    for i in 0..5 {
                        let id = format!("m-{i}");
                        ctx.process(Message::PData(make_log_input(&id, 1000 + i)))
                            .await
                            .expect("ingest run2");

                        // Observe the upstream ACK the processor emits after the
                        // durable write. The processor preserves the input
                        // context, so the subscriber frame (carrying our
                        // TestCallData) comes back on the ack. Match it back to
                        // the message id via the subscriber id we stamped.
                        while let Ok(msg) = rx.try_recv() {
                            if let PipelineCompletionMsg::DeliverAck { ack } = msg
                                && let Some((_, ack)) = next_ack(ack)
                                && let Ok(got) = TestCallData::try_from(ack.unwind.route.calldata)
                            {
                                for j in 0..5u64 {
                                    if got == TestCallData::new_with(1000 + j, 0) {
                                        let _ = acked.lock().unwrap().insert(format!("m-{j}"));
                                    }
                                }
                            }
                        }

                        // Flush + deliver downstream.
                        ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                            .await
                            .expect("tick run2");
                        let mut outputs = ctx.drain_pdata().await;
                        for out in &mut outputs {
                            for lr_id in log_ids_in_output(out) {
                                let _ = delivered.lock().unwrap().insert(lr_id);
                            }
                        }
                        for out in outputs {
                            if let Some((_, ack)) = next_ack(AckMsg::new(out)) {
                                ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                                    .await
                                    .expect("downstream ack run2");
                            }
                        }

                        // After the first message's flush, wal.1 must be purged:
                        // this is the precondition that exposes the bug.
                        if i == 0 {
                            *purged.lock().unwrap() = !rotated_wal_check.exists();
                        }
                    }

                    // Shutdown flushes + drains any remaining bundles downstream.
                    ctx.process(Message::Control(NodeControlMsg::Shutdown {
                        deadline: Instant::now() + Duration::from_secs(1),
                        reason: "shutdown".to_owned(),
                    }))
                    .await
                    .expect("shutdown run2");
                    let mut outputs = ctx.drain_pdata().await;
                    for out in &mut outputs {
                        for lr_id in log_ids_in_output(out) {
                            let _ = delivered.lock().unwrap().insert(lr_id);
                        }
                    }
                })
                .validate(|_| async {});
        }

        // ---------------------------------------------------------------
        // Run 3: restart once more to see whether the acked-but-undelivered
        // data is recovered (delivered late), duplicated, or lost for good.
        // ---------------------------------------------------------------
        {
            let rt = TestRuntime::new();
            let controller = ControllerContext::new(rt.metrics_registry());
            let pipeline_ctx =
                controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
            let processor = create_durable_buffer(
                pipeline_ctx,
                test_node("durable-buffer-crash-run3"),
                Arc::new(node_config_json(&dir_path)),
                &ProcessorConfig::new("durable-buffer-crash-run3"),
                &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
            )
            .expect("create durable buffer run3");

            let delivered = Arc::clone(&run3_delivered);
            rt.set_processor(processor)
                .run_test(move |mut ctx| async move {
                    let (tx, _rx) = pipeline_completion_msg_channel(256);
                    ctx.set_pipeline_completion_sender(tx);

                    // A tick starts the engine (replay) and flushes/delivers.
                    for _ in 0..3 {
                        ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                            .await
                            .expect("tick run3");
                        let mut outputs = ctx.drain_pdata().await;
                        for out in &mut outputs {
                            for lr_id in log_ids_in_output(out) {
                                delivered.lock().unwrap().push(lr_id);
                            }
                        }
                        for out in outputs {
                            if let Some((_, ack)) = next_ack(AckMsg::new(out)) {
                                ctx.process(Message::Control(NodeControlMsg::Ack(ack)))
                                    .await
                                    .expect("downstream ack run3");
                            }
                        }
                    }

                    ctx.process(Message::Control(NodeControlMsg::Shutdown {
                        deadline: Instant::now() + Duration::from_secs(1),
                        reason: "shutdown".to_owned(),
                    }))
                    .await
                    .expect("shutdown run3");
                    let mut outputs = ctx.drain_pdata().await;
                    for out in &mut outputs {
                        for lr_id in log_ids_in_output(out) {
                            delivered.lock().unwrap().push(lr_id);
                        }
                    }
                })
                .validate(|_| async {});
        }

        // ---------------------------------------------------------------
        // Assertions.
        // ---------------------------------------------------------------
        let run1_delivered = run1_delivered.lock().unwrap().clone();
        let run2_acked = run2_acked.lock().unwrap().clone();
        let run2_delivered = run2_delivered.lock().unwrap().clone();
        let wal1_purged = *run2_wal1_purged.lock().unwrap();
        let run3_delivered = run3_delivered.lock().unwrap().clone();

        assert_eq!(run1_delivered.len(), 20, "run1 should deliver all history");
        assert!(
            wal1_purged,
            "run2: wal.1 must be purged by the first flush to exercise the bug"
        );
        assert!(
            !run2_acked.is_empty(),
            "run2 should ACK ingested batches upstream"
        );

        // The core invariant: anything ACKed upstream must be delivered
        // downstream within the same process lifetime.
        let acked_but_lost: BTreeSet<String> =
            run2_acked.difference(&run2_delivered).cloned().collect();
        let recovered_in_run3: BTreeSet<String> = acked_but_lost
            .iter()
            .filter(|id| run3_delivered.contains(id))
            .cloned()
            .collect();
        let never_delivered: BTreeSet<String> = acked_but_lost
            .iter()
            .filter(|id| !run3_delivered.contains(id))
            .cloned()
            .collect();

        assert!(
            acked_but_lost.is_empty(),
            "data ACKed upstream but never delivered downstream in run2: {:?}\n\
             acked={:?} delivered={:?}\n\
             recovered after another restart (run3): {:?}\n\
             never delivered even after run3: {:?}\n\
             run3 delivery order (shows duplicates): {:?}",
            acked_but_lost,
            run2_acked,
            run2_delivered,
            recovered_in_run3,
            never_delivered,
            run3_delivered,
        );
    }

    /// Scenario: A durable buffer configured with zstd WAL compression and lz4
    /// segment compression ingests batches; some are finalized into a segment
    /// and delivered but never acked downstream, the rest stay WAL-only. The
    /// process stops without a shutdown, then a new processor on the same
    /// directory starts and delivers.
    /// Guarantees: Batches stored in compressed segments and compressed WAL
    /// entries are all recovered and delivered downstream after the restart.
    #[test]
    fn compressed_wal_and_segments_are_delivered_after_restart() {
        use otel_arrow_dfe_config::node::NodeUserConfig;
        use otel_arrow_dfe_engine::config::ProcessorConfig;
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::message::Message;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use otel_arrow_dfe_engine::testing::test_node;
        use serde_json::json;
        use std::collections::BTreeSet;
        use std::sync::Mutex;

        fn create(
            path: &std::path::Path,
            name: &'static str,
            rt: &TestRuntime<OtapPdata>,
        ) -> ProcessorWrapper<OtapPdata> {
            let mut node_config = NodeUserConfig::new_processor_config(DURABLE_BUFFER_URN);
            node_config.config = json!({
                "path": path,
                "retention_size_cap": "256 MiB",
                "poll_interval": "100ms",
                "max_segment_open_duration": "1h",
                "wal_flush_interval": "0s",
                "wal_compression": {"codec": "zstd", "level": 3},
                "segment_compression": {"codec": "lz4"}
            });
            let controller = ControllerContext::new(rt.metrics_registry());
            let pipeline_ctx =
                controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
            create_durable_buffer(
                pipeline_ctx,
                test_node(name),
                Arc::new(node_config),
                &ProcessorConfig::new(name),
                &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
            )
            .expect("create durable buffer")
        }

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let dir_path = temp_dir.path().to_path_buf();
        let run1_delivered = Arc::new(Mutex::new(BTreeSet::<String>::new()));
        let run2_delivered = Arc::new(Mutex::new(BTreeSet::<String>::new()));

        // Run 1: finalize seg-* into a segment and deliver it without acking,
        // then append wal-* to the WAL only and stop without a shutdown.
        {
            let rt = TestRuntime::new();
            let processor = create(&dir_path, "durable-buffer-codec-run1", &rt);
            let delivered = Arc::clone(&run1_delivered);
            rt.set_processor(processor)
                .run_test(move |mut ctx| async move {
                    let (tx, _rx) = pipeline_completion_msg_channel(256);
                    ctx.set_pipeline_completion_sender(tx);

                    for i in 0..5 {
                        ctx.process(Message::PData(make_log_input(&format!("seg-{i}"), i)))
                            .await
                            .expect("ingest segment batch");
                    }
                    ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                        .await
                        .expect("tick run1");
                    for mut out in ctx.drain_pdata().await {
                        delivered
                            .lock()
                            .unwrap()
                            .extend(log_ids_in_output(&mut out));
                    }

                    for i in 0..5 {
                        ctx.process(Message::PData(make_log_input(&format!("wal-{i}"), 100 + i)))
                            .await
                            .expect("ingest WAL batch");
                    }
                })
                .validate(|_| async {});
        }

        // Run 2: restart with the same settings and deliver everything.
        {
            let rt = TestRuntime::new();
            let processor = create(&dir_path, "durable-buffer-codec-run2", &rt);
            let delivered = Arc::clone(&run2_delivered);
            rt.set_processor(processor)
                .run_test(move |mut ctx| async move {
                    let (tx, _rx) = pipeline_completion_msg_channel(256);
                    ctx.set_pipeline_completion_sender(tx);

                    ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                        .await
                        .expect("tick run2");
                    let mut outputs = ctx.drain_pdata().await;
                    ctx.process(Message::Control(NodeControlMsg::Shutdown {
                        deadline: Instant::now() + Duration::from_secs(1),
                        reason: "shutdown".to_owned(),
                    }))
                    .await
                    .expect("shutdown run2");
                    outputs.extend(ctx.drain_pdata().await);

                    for mut out in outputs {
                        delivered
                            .lock()
                            .unwrap()
                            .extend(log_ids_in_output(&mut out));
                    }
                })
                .validate(|_| async {});
        }

        let seg_ids: BTreeSet<String> = (0..5).map(|i| format!("seg-{i}")).collect();
        let all_ids: BTreeSet<String> = (0..5)
            .map(|i| format!("seg-{i}"))
            .chain((0..5).map(|i| format!("wal-{i}")))
            .collect();
        assert_eq!(*run1_delivered.lock().unwrap(), seg_ids);
        assert_eq!(*run2_delivered.lock().unwrap(), all_ids);
    }

    #[test]
    fn test_backoff_calculation() {
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;

        let registry = TelemetryRegistryHandle::default();
        let controller_ctx = ControllerContext::new(registry);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("test".into(), "test".into(), 0, 1, 0);

        let config = DurableBufferConfig {
            path: std::path::PathBuf::from("/tmp/test"),
            retention_size_cap: byte_unit::Byte::from_u64(256 * 1024 * 1024), // 256 MiB (above min for default WAL/segment sizes)
            max_age: None,
            size_cap_policy: SizeCapPolicy::Backpressure,
            poll_interval: Duration::from_millis(100),
            otlp_handling: OtlpHandling::PassThrough,
            max_segment_open_duration: Duration::from_secs(1),
            initial_retry_interval: Duration::from_secs(1),
            max_retry_interval: Duration::from_secs(30),
            retry_multiplier: 2.0,
            max_in_flight: 1000,
            wal_flush_interval: Duration::from_millis(25),
            upstream_ack: UpstreamAck::AfterWalAppend,
            wal_compression: IpcCompression::None,
            segment_compression: IpcCompression::None,
            forward_after_ingest: false,
            ownership: OwnershipSettings::default(),
        };

        let processor = DurableBuffer::new(config, &pipeline_ctx).unwrap();

        // retry 0: 1s * 2^0 = 1s (with jitter 0.5-1.0x)
        let backoff0 = processor.calculate_backoff(0);
        assert!(backoff0 >= Duration::from_millis(500));
        assert!(backoff0 <= Duration::from_millis(1000));

        // retry 1: 1s * 2^1 = 2s (with jitter 0.5-1.0x)
        let backoff1 = processor.calculate_backoff(1);
        assert!(backoff1 >= Duration::from_millis(1000));
        assert!(backoff1 <= Duration::from_millis(2000));

        // retry 5: 1s * 2^5 = 32s, capped to 30s (with jitter 0.5-1.0x)
        let backoff5 = processor.calculate_backoff(5);
        assert!(backoff5 >= Duration::from_millis(15000)); // 30s * 0.5
        assert!(backoff5 <= Duration::from_millis(30000)); // 30s * 1.0

        // Large retry count: should stay capped at max_retry_interval
        let backoff100 = processor.calculate_backoff(100);
        assert!(backoff100 >= Duration::from_millis(15000));
        assert!(backoff100 <= Duration::from_millis(30000));
    }

    /// Bundle resolution outcomes are exported as separate items.
    #[test]
    fn test_nack_metrics_snapshot_field_positions() {
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
        use otel_arrow_dfe_telemetry::reporter::MetricsReporter;

        let registry = TelemetryRegistryHandle::default();
        let controller_ctx = ControllerContext::new(registry);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("test".into(), "test".into(), 0, 1, 0);

        let config = DurableBufferConfig {
            path: std::path::PathBuf::from("/tmp/test-metrics"),
            retention_size_cap: byte_unit::Byte::from_u64(256 * 1024 * 1024), // 256 MiB
            max_age: None,
            size_cap_policy: SizeCapPolicy::Backpressure,
            poll_interval: Duration::from_millis(100),
            otlp_handling: OtlpHandling::PassThrough,
            max_segment_open_duration: Duration::from_secs(1),
            initial_retry_interval: Duration::from_secs(1),
            max_retry_interval: Duration::from_secs(30),
            retry_multiplier: 2.0,
            max_in_flight: 1000,
            wal_flush_interval: Duration::from_millis(25),
            upstream_ack: UpstreamAck::AfterWalAppend,
            wal_compression: IpcCompression::None,
            segment_compression: IpcCompression::None,
            forward_after_ingest: false,
            ownership: OwnershipSettings::default(),
        };

        let mut processor = DurableBuffer::new(config, &pipeline_ctx).unwrap();

        processor
            .metrics
            .bundles_for(BundleOutcome::PermanentlyRejected)
            .resolved
            .add(3);
        processor
            .metrics
            .bundles_for(BundleOutcome::Deferred)
            .resolved
            .add(5);
        processor
            .metrics
            .bundles_for(BundleOutcome::Acked)
            .resolved
            .add(10);

        let (metrics_rx, mut reporter) = MetricsReporter::create_new_and_receiver(3);
        reporter
            .report_measurement(&mut processor.metrics.bundle_metrics)
            .unwrap();

        let mut outcomes = [0u64; BundleOutcome::CARDINALITY];
        while let Ok(snapshot) = metrics_rx.try_recv() {
            outcomes[snapshot.bucket()] = snapshot.get_metrics()[0].to_u64_lossy();
        }
        assert_eq!(outcomes, [10, 5, 3]);
    }

    /// Test that permanent NACKs decrement the `queued_*` gauges.
    ///
    /// The `queued_log_records` (and siblings) gauge tracks items ingested but
    /// not yet resolved (ACKed or rejected). When a bundle is permanently
    /// NACKed, it must be decremented just like an ACK -- otherwise the gauge
    /// drifts upward, giving operators a false picture of backlog.
    #[test]
    fn test_permanent_nack_decrements_queued_gauge() {
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;

        let registry = TelemetryRegistryHandle::default();
        let controller_ctx = ControllerContext::new(registry);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("test".into(), "test".into(), 0, 1, 0);

        let config = DurableBufferConfig {
            path: std::path::PathBuf::from("/tmp/test-queued-gauge"),
            retention_size_cap: byte_unit::Byte::from_u64(256 * 1024 * 1024),
            max_age: None,
            size_cap_policy: SizeCapPolicy::Backpressure,
            poll_interval: Duration::from_millis(100),
            otlp_handling: OtlpHandling::PassThrough,
            max_segment_open_duration: Duration::from_secs(1),
            initial_retry_interval: Duration::from_secs(1),
            max_retry_interval: Duration::from_secs(30),
            retry_multiplier: 2.0,
            max_in_flight: 1000,
            wal_flush_interval: Duration::from_millis(25),
            upstream_ack: UpstreamAck::AfterWalAppend,
            wal_compression: IpcCompression::None,
            segment_compression: IpcCompression::None,
            forward_after_ingest: false,
            ownership: OwnershipSettings::default(),
        };

        let mut processor = DurableBuffer::new(config, &pipeline_ctx).unwrap();
        let logs = SignalAttributes {
            signal: SignalType::Logs,
        };
        let metrics = SignalAttributes {
            signal: SignalType::Metrics,
        };
        let traces = SignalAttributes {
            signal: SignalType::Traces,
        };

        // Simulate: 100 log records queued (as if 100 items were ingested).
        let mut queued_log_records = 100u64;
        processor
            .metrics
            .item_metrics
            .with(logs)
            .queued
            .set(queued_log_records);

        // Simulate: permanent NACK path decrements by 30 items
        // (mirrors the code in handle_nack when nack.permanent is true)
        queued_log_records = queued_log_records.saturating_sub(30);
        processor
            .metrics
            .item_metrics
            .with(logs)
            .queued
            .set(queued_log_records);

        assert_eq!(
            processor.metrics.item_metrics.get(logs).queued.get(),
            70,
            "queued_log_records should be 70 after decrementing 30 from 100"
        );

        // Simulate: ACK the remaining 70
        queued_log_records = queued_log_records.saturating_sub(70);
        processor
            .metrics
            .item_metrics
            .with(logs)
            .queued
            .set(queued_log_records);

        assert_eq!(
            processor.metrics.item_metrics.get(logs).queued.get(),
            0,
            "queued_log_records should be 0 after all items resolved"
        );

        // Verify the same for metrics and spans signals
        let mut queued_metric_points = 50u64;
        processor
            .metrics
            .item_metrics
            .with(metrics)
            .queued
            .set(queued_metric_points);
        queued_metric_points = queued_metric_points.saturating_sub(50);
        processor
            .metrics
            .item_metrics
            .with(metrics)
            .queued
            .set(queued_metric_points);

        let mut queued_spans = 25u64;
        processor
            .metrics
            .item_metrics
            .with(traces)
            .queued
            .set(queued_spans);
        queued_spans = queued_spans.saturating_sub(25);
        processor
            .metrics
            .item_metrics
            .with(traces)
            .queued
            .set(queued_spans);

        assert_eq!(
            processor.metrics.item_metrics.get(metrics).queued.get(),
            0,
            "queued_metric_points should be 0"
        );
        assert_eq!(
            processor.metrics.item_metrics.get(traces).queued.get(),
            0,
            "queued_spans should be 0"
        );
    }

    #[tokio::test]
    async fn test_open_segment_included_in_queued_gauge() {
        let (mut processor, engine, subscriber_id, _temp_dir) = setup_test_processor(None).await;

        let slot_id = SlotId::new(30);
        let bundle = make_simple_bundle(slot_id, 5);

        engine.ingest(&bundle).await.unwrap();

        // Verify the bundle is actually in the open segment before recomputing gauges
        let open_bundles = engine.open_segment_bundle_summaries();
        assert_eq!(
            open_bundles.len(),
            1,
            "bundle should be in open segment and accessible"
        );
        assert_eq!(
            open_bundles[0].item_count, 5,
            "open segment bundle should have correct item count"
        );
        assert!(
            open_bundles[0].slot_ids.contains(&slot_id),
            "open segment bundle should have the logs slot"
        );

        processor.recompute_metrics(&engine, &subscriber_id);

        assert_eq!(
            processor
                .metrics
                .item_metrics
                .get(SignalAttributes {
                    signal: SignalType::Logs,
                })
                .queued
                .get(),
            5,
            "queued_log_records gauge should include open-segment items"
        );
    }

    #[test]
    fn test_segment_cache_bound_evicts_oldest_and_warn_marker() {
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;

        let registry = TelemetryRegistryHandle::default();
        let controller_ctx = ControllerContext::new(registry);
        let pipeline_ctx =
            controller_ctx.pipeline_context_with("test".into(), "test".into(), 0, 1, 0);

        let config = DurableBufferConfig {
            path: std::path::PathBuf::from("/tmp/test-cache-bound"),
            retention_size_cap: byte_unit::Byte::from_u64(256 * 1024 * 1024),
            max_age: None,
            size_cap_policy: SizeCapPolicy::Backpressure,
            poll_interval: Duration::from_millis(100),
            otlp_handling: OtlpHandling::PassThrough,
            max_segment_open_duration: Duration::from_secs(1),
            initial_retry_interval: Duration::from_secs(1),
            max_retry_interval: Duration::from_secs(30),
            retry_multiplier: 2.0,
            max_in_flight: 1000,
            wal_flush_interval: Duration::from_millis(25),
            upstream_ack: UpstreamAck::AfterWalAppend,
            wal_compression: IpcCompression::None,
            segment_compression: IpcCompression::None,
            forward_after_ingest: false,
            ownership: OwnershipSettings::default(),
        };

        let mut processor = DurableBuffer::new(config, &pipeline_ctx).unwrap();
        let _ = processor.segment_cache.insert(
            10,
            CachedSegmentMetrics {
                summary: SegmentMetricsSummary {
                    bundles: vec![],
                    total_logs: 0,
                    total_metrics: 0,
                    total_spans: 0,
                },
                last_seen_generation: 1,
            },
        );
        let _ = processor.segment_cache.insert(
            20,
            CachedSegmentMetrics {
                summary: SegmentMetricsSummary {
                    bundles: vec![],
                    total_logs: 0,
                    total_metrics: 0,
                    total_spans: 0,
                },
                last_seen_generation: 2,
            },
        );
        let _ = processor.segment_cache.insert(
            30,
            CachedSegmentMetrics {
                summary: SegmentMetricsSummary {
                    bundles: vec![],
                    total_logs: 0,
                    total_metrics: 0,
                    total_spans: 0,
                },
                last_seen_generation: 3,
            },
        );
        let _ = processor.metadata_load_warned_segments.insert(10);

        processor.enforce_segment_cache_bound_with_limit(2);

        assert_eq!(processor.segment_cache.len(), 2);
        assert!(
            !processor.segment_cache.contains_key(&10),
            "oldest segment should be evicted"
        );
        assert!(
            !processor.metadata_load_warned_segments.contains(&10),
            "evicted segment warning marker should be removed"
        );
        assert!(processor.segment_cache.contains_key(&20));
        assert!(processor.segment_cache.contains_key(&30));
    }

    #[test]
    fn test_storage_utilization_reporting() {
        use otel_arrow_dfe_config::node::NodeUserConfig;
        use otel_arrow_dfe_engine::config::ProcessorConfig;
        use otel_arrow_dfe_engine::context::ControllerContext;
        use otel_arrow_dfe_engine::message::Message;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;
        use otel_arrow_dfe_engine::testing::test_node;
        use otel_arrow_dfe_pdata::encode::encode_logs_otap_batch;
        use otel_arrow_dfe_pdata::testing::fixtures::DataGenerator;
        use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
        use serde_json::json;

        let rt = TestRuntime::new();
        let controller = ControllerContext::new(rt.metrics_registry());
        let pipeline_ctx = controller.pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0);
        let temp_dir = tempfile::tempdir().expect("tempdir");

        let mut node_config = NodeUserConfig::new_processor_config(DURABLE_BUFFER_URN);
        node_config.config = json!({
            "path": temp_dir.path(),
            "retention_size_cap": "256 MiB",
            "poll_interval": "100ms",
            "max_segment_open_duration": "1s",
            "initial_retry_interval": "100ms",
            "max_retry_interval": "100ms",
            "retry_multiplier": 2.0,
            "max_in_flight": 1000
        });

        let processor = create_durable_buffer(
            pipeline_ctx,
            test_node("durable-buffer-utilization-test"),
            Arc::new(node_config),
            &ProcessorConfig::new("durable-buffer-utilization-test"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("create durable buffer");

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                // Ingest some logs to write data to disk (so used bytes > 0)
                let mut datagen = DataGenerator::new(1);
                let input = datagen.generate_logs();
                let rec = encode_logs_otap_batch(&input).expect("encode logs");
                ctx.process(Message::PData(OtapPdata::new_default(rec.into())))
                    .await
                    .expect("process input");

                // Trigger flush to finalize the open segment and write it to disk
                ctx.process(Message::Control(NodeControlMsg::TimerTick {}))
                    .await
                    .expect("process timer tick");

                // Trigger metrics collection
                let (metrics_rx, reporter) = MetricsReporter::create_new_and_receiver(1);
                ctx.process(Message::Control(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter,
                }))
                .await
                .expect("collect telemetry");

                let snap = metrics_rx.try_recv().unwrap();
                let metrics = snap.get_metrics();

                const IDX_STORAGE_UTILIZATION: usize = 6;
                const IDX_STORAGE_BYTES_USED: usize = 1;
                const IDX_STORAGE_BYTES_CAP: usize = 2;

                let used = metrics[IDX_STORAGE_BYTES_USED].to_u64_lossy();
                let cap = metrics[IDX_STORAGE_BYTES_CAP].to_u64_lossy();
                let reported_utilization = metrics[IDX_STORAGE_UTILIZATION].to_f64();

                assert!(used > 0, "used bytes should be greater than 0");
                assert_eq!(cap, 256 * 1024 * 1024, "cap bytes should be 256 MiB");
                assert_eq!(
                    reported_utilization,
                    used as f64 / cap as f64,
                    "reported utilization should match used / cap"
                );
            })
            .validate(|_| async {});
    }

    /// Scenario: DropOldest and max_age retention each remove log items.
    /// Guarantees: Signal-partitioned item loss stays interval-scoped.
    #[tokio::test]
    async fn test_per_signal_dropped_expired_metrics() {
        let (mut processor, engine, subscriber_id, _temp_dir) =
            setup_test_processor(Some(Duration::from_millis(5))).await;

        let slot_id = SlotId::new(30);
        let (metrics_rx, mut reporter) = MetricsReporter::create_new_and_receiver(6);

        // Report a fresh interval; returns (dropped_log_records, expired_log_records).
        let mut sample = |p: &mut DurableBuffer| {
            p.recompute_metrics(&engine, &subscriber_id);
            let sample = (
                p.metrics
                    .loss_item_metrics
                    .get(SignalLossAttributes {
                        signal: SignalType::Logs,
                        reason: LossReason::DropOldest,
                    })
                    .items
                    .get(),
                p.metrics
                    .loss_item_metrics
                    .get(SignalLossAttributes {
                        signal: SignalType::Logs,
                        reason: LossReason::Expired,
                    })
                    .items
                    .get(),
            );
            reporter
                .report_measurement(&mut p.metrics.loss_item_metrics)
                .unwrap();
            while metrics_rx.try_recv().is_ok() {}
            sample
        };

        // Interval 1: drop a 5-record segment -> dropped reports 5.
        engine
            .ingest(&make_simple_bundle(slot_id, 5))
            .await
            .unwrap();
        engine.flush().await.unwrap();
        assert_eq!(engine.force_drop_oldest_pending_segments(), 1);
        assert_eq!(sample(&mut processor), (5, 0));

        // Interval 2: expire a 3-record segment; no new drops. The dropped counter
        // was reset by the previous flush, so it reads 0 -- proving these are
        // per-interval (delta) values, not running totals.
        engine
            .ingest(&make_simple_bundle(slot_id, 3))
            .await
            .unwrap();
        engine.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(engine.cleanup_expired_segments().unwrap(), 1);
        assert_eq!(sample(&mut processor), (0, 3));
        assert_eq!(
            processor
                .metrics
                .reclaimed_metrics
                .get(LossAttributes {
                    reason: LossReason::Expired,
                })
                .segments
                .get(),
            1
        );

        // Interval 3: drop a 4-record segment. Dropped reports only this interval's
        // 4 (not the lifetime 9); expired was reset and reads 0.
        engine
            .ingest(&make_simple_bundle(slot_id, 4))
            .await
            .unwrap();
        engine.flush().await.unwrap();
        assert_eq!(engine.force_drop_oldest_pending_segments(), 1);
        assert_eq!(sample(&mut processor), (4, 0));
    }

    /// Scenario: Physical reclamation and logical loss occur in separate reporting intervals.
    /// Guarantees: Each reason-only counter reports its delta and resets after export.
    #[tokio::test]
    async fn test_aggregate_loss_metrics_are_delta_counters() {
        let (mut processor, engine, subscriber_id, _temp_dir) =
            setup_test_processor(Some(Duration::from_millis(5))).await;
        let (metrics_rx, mut reporter) = MetricsReporter::create_new_and_receiver(4);

        let mut sample = |processor: &mut DurableBuffer| {
            processor.recompute_metrics(&engine, &subscriber_id);
            let dropped = processor.metrics.loss_metrics.get(LossAttributes {
                reason: LossReason::DropOldest,
            });
            let dropped_reclaimed = processor.metrics.reclaimed_metrics.get(LossAttributes {
                reason: LossReason::DropOldest,
            });
            let dropped_values = (
                dropped_reclaimed.segments.get(),
                dropped_reclaimed.bytes.get(),
                dropped.bundles.get(),
                dropped.bytes.get(),
            );
            let expired = processor.metrics.loss_metrics.get(LossAttributes {
                reason: LossReason::Expired,
            });
            let expired_reclaimed = processor.metrics.reclaimed_metrics.get(LossAttributes {
                reason: LossReason::Expired,
            });
            let expired_values = (
                expired_reclaimed.segments.get(),
                expired_reclaimed.bytes.get(),
                expired.bundles.get(),
                expired.bytes.get(),
            );

            reporter
                .report_measurement(&mut processor.metrics.loss_metrics)
                .expect("report loss metrics");
            reporter
                .report_measurement(&mut processor.metrics.reclaimed_metrics)
                .expect("report reclaimed metrics");
            while metrics_rx.try_recv().is_ok() {}

            (dropped_values, expired_values)
        };

        let dropped_bundle = make_simple_bundle(SlotId::new(30), 5);
        let dropped_logical_bytes = dropped_bundle.byte_count().expect("logical byte count");
        engine
            .ingest(&dropped_bundle)
            .await
            .expect("ingest dropped bundle");
        engine.flush().await.expect("flush dropped bundle");
        let dropped_bytes = engine
            .segment_store()
            .segment_sequences()
            .into_iter()
            .min()
            .map(|seq| {
                engine
                    .segment_store()
                    .segment_file_size(seq)
                    .expect("segment file size")
            })
            .expect("oldest segment");
        assert_eq!(engine.force_drop_oldest_pending_segments(), 1);
        assert_eq!(
            sample(&mut processor),
            ((1, dropped_bytes, 1, dropped_logical_bytes), (0, 0, 0, 0))
        );

        let expired_bundle = make_simple_bundle(SlotId::new(30), 3);
        let expired_logical_bytes = expired_bundle.byte_count().expect("logical byte count");
        engine
            .ingest(&expired_bundle)
            .await
            .expect("ingest expired bundle");
        engine.flush().await.expect("flush expired bundle");
        let expired_bytes = engine
            .segment_store()
            .segment_sequences()
            .into_iter()
            .map(|seq| {
                engine
                    .segment_store()
                    .segment_file_size(seq)
                    .expect("segment file size")
            })
            .sum::<u64>();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            engine.cleanup_expired_segments().expect("expire segment"),
            1
        );
        assert_eq!(
            sample(&mut processor),
            ((0, 0, 0, 0), (1, expired_bytes, 1, expired_logical_bytes))
        );

        assert_eq!(sample(&mut processor), ((0, 0, 0, 0), (0, 0, 0, 0)));
    }

    /// Scenario: DropOldest removes a metrics bundle containing 42 items.
    /// Guarantees: Item loss is attributed to the metrics signal.
    #[tokio::test]
    async fn test_dropped_metrics_item_count() {
        let (mut processor, engine, subscriber_id, _temp_dir) = setup_test_processor(None).await;

        // Metrics slot (11: Arrow metric data point slot)
        let slot_id = SlotId::new(11);
        let bundle = make_simple_bundle(slot_id, 42);

        engine.ingest(&bundle).await.unwrap();
        engine.flush().await.unwrap();

        let dropped = engine.force_drop_oldest_pending_segments();
        assert_eq!(dropped, 1);

        processor.recompute_metrics(&engine, &subscriber_id);

        assert_eq!(
            processor
                .metrics
                .loss_item_metrics
                .get(SignalLossAttributes {
                    signal: SignalType::Metrics,
                    reason: LossReason::DropOldest,
                })
                .items
                .get(),
            42
        );
    }

    /// Scenario: DropOldest removes a trace bundle containing 55 spans.
    /// Guarantees: Item loss is attributed to the traces signal.
    #[tokio::test]
    async fn test_dropped_spans_item_count() {
        let (mut processor, engine, subscriber_id, _temp_dir) = setup_test_processor(None).await;

        // Arrow spans slot (40)
        let slot_id = SlotId::new(40);
        let bundle = make_simple_bundle(slot_id, 55);

        engine.ingest(&bundle).await.unwrap();
        engine.flush().await.unwrap();

        let dropped = engine.force_drop_oldest_pending_segments();
        assert_eq!(dropped, 1);

        processor.recompute_metrics(&engine, &subscriber_id);

        assert_eq!(
            processor
                .metrics
                .loss_item_metrics
                .get(SignalLossAttributes {
                    signal: SignalType::Traces,
                    reason: LossReason::DropOldest,
                })
                .items
                .get(),
            55
        );
    }

    /// Scenario: DropOldest removes an OTLP pass-through log bundle.
    /// Guarantees: Logical log loss uses the adapter item count.
    #[tokio::test]
    async fn test_dropped_otlp_passthrough_item_count() {
        let (mut processor, engine, subscriber_id, _temp_dir) = setup_test_processor(None).await;

        // OTLP Logs slot (60)
        let slot_id = SlotId::new(60);
        let bundle = make_simple_bundle(slot_id, 99); // represents 99 logical records in the OTLP payload

        engine.ingest(&bundle).await.unwrap();
        engine.flush().await.unwrap();

        let dropped = engine.force_drop_oldest_pending_segments();
        assert_eq!(dropped, 1);

        processor.recompute_metrics(&engine, &subscriber_id);

        // Verify we tracked the logical item count (99) rather than the batch row count (1)
        assert_eq!(
            processor
                .metrics
                .loss_item_metrics
                .get(SignalLossAttributes {
                    signal: SignalType::Logs,
                    reason: LossReason::DropOldest,
                })
                .items
                .get(),
            99
        );
    }

    /// Scenario: DropOldest and expiry remove items for all three signals.
    /// Guarantees: Item totals reconcile across signal and reason dimensions.
    #[tokio::test]
    async fn test_reconciliation() {
        let (mut processor, engine, subscriber_id, _temp_dir) =
            setup_test_processor(Some(Duration::from_millis(5))).await;

        // 1. Ingest logs, traces, metrics bundles to be dropped
        let b_logs = make_simple_bundle(SlotId::new(30), 10);
        let b_traces = make_simple_bundle(SlotId::new(40), 20);
        let b_metrics = make_simple_bundle(SlotId::new(11), 30);

        engine.ingest(&b_logs).await.unwrap();
        engine.flush().await.unwrap();
        engine.ingest(&b_traces).await.unwrap();
        engine.flush().await.unwrap();
        engine.ingest(&b_metrics).await.unwrap();
        engine.flush().await.unwrap();

        // Drop all 3 segments
        let mut dropped = 0;
        loop {
            let n = engine.force_drop_oldest_pending_segments();
            if n == 0 {
                break;
            }
            dropped += n;
        }
        assert_eq!(dropped, 3);

        // 2. Ingest logs, traces, metrics bundles to be expired
        let b_logs_exp = make_simple_bundle(SlotId::new(30), 5);
        let b_traces_exp = make_simple_bundle(SlotId::new(40), 15);
        let b_metrics_exp = make_simple_bundle(SlotId::new(11), 25);

        engine.ingest(&b_logs_exp).await.unwrap();
        engine.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        engine.ingest(&b_traces_exp).await.unwrap();
        engine.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        engine.ingest(&b_metrics_exp).await.unwrap();
        engine.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Wait for segments to expire
        tokio::time::sleep(Duration::from_millis(20)).await;
        let expired = engine.cleanup_expired_segments().unwrap();
        assert_eq!(expired, 3);

        processor.recompute_metrics(&engine, &subscriber_id);

        let item_loss = |signal, reason| {
            processor
                .metrics
                .loss_item_metrics
                .get(SignalLossAttributes { signal, reason })
                .items
                .get()
        };
        let dropped_logs = item_loss(SignalType::Logs, LossReason::DropOldest);
        let dropped_spans = item_loss(SignalType::Traces, LossReason::DropOldest);
        let dropped_metrics = item_loss(SignalType::Metrics, LossReason::DropOldest);
        let expired_logs = item_loss(SignalType::Logs, LossReason::Expired);
        let expired_spans = item_loss(SignalType::Traces, LossReason::Expired);
        let expired_metrics = item_loss(SignalType::Metrics, LossReason::Expired);
        // Assert dropped signal breakdown.
        assert_eq!(dropped_logs, 10);
        assert_eq!(dropped_spans, 20);
        assert_eq!(dropped_metrics, 30);
        assert_eq!(dropped_logs + dropped_spans + dropped_metrics, 60);

        // Assert expired signal breakdown.
        assert_eq!(expired_logs, 5);
        assert_eq!(expired_spans, 15);
        assert_eq!(expired_metrics, 25);
        assert_eq!(expired_logs + expired_spans + expired_metrics, 45);
    }

    /// Opens a Quiver engine on the durable buffer's core 0 directory under
    /// `path`, standing in for another pipeline instance in this process that
    /// holds the storage directory.
    async fn hold_storage(path: &std::path::Path) -> Arc<QuiverEngine> {
        QuiverEngine::open(
            QuiverConfig::default().with_data_dir(path.join("core_0")),
            Arc::new(DiskBudget::unlimited()),
        )
        .await
        .expect("holder opens")
    }

    /// Scenario: Another Quiver engine in this process already owns the
    /// core's storage directory when a durable buffer configured with the
    /// same `path` and `ownership.handoff_timeout: 0s` receives its first
    /// message.
    /// Guarantees: With waiting disabled, the durable buffer fails to
    /// initialize its engine right away with an error reporting the directory
    /// as held by another engine in this process, and the existing owner
    /// keeps its claim.
    #[test]
    fn engine_init_fails_when_storage_is_owned() {
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().to_path_buf();
        let rt = TestRuntime::new();
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-owned-storage",
            temp_dir.path(),
            serde_json::json!({"ownership": {"handoff_timeout": "0s"}}),
        );
        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let holder = hold_storage(&path).await;

                let err = ctx
                    .process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect_err("engine initialization must fail")
                    .to_string();
                assert!(err.contains("failed to initialize Quiver engine"), "{err}");
                assert!(err.contains("is owned by another instance"), "{err}");
                assert!(err.contains("another engine in this process"), "{err}");

                let marker = std::fs::read_to_string(path.join("core_0").join("quiver.owner"))
                    .expect("holder marker");
                assert!(
                    marker.contains(&format!("pid={}\n", std::process::id())),
                    "{marker}"
                );
                drop(holder);
            })
            .validate(|_| async {});
    }

    /// Scenario: Another Quiver engine in this process holds the core's
    /// storage directory when the durable buffer receives its first message,
    /// and releases it after one timer tick.
    /// Guarantees: The durable buffer holds the message without answering it
    /// and pauses pdata delivery while the directory is held, then opens its
    /// engine on the first timer tick after the release, ingests and ACKs the
    /// held message, and accepts pdata again.
    #[test]
    fn engine_init_waits_for_same_process_handoff() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().to_path_buf();
        let rt = TestRuntime::new();
        let processor = create_test_durable_buffer(&rt, "durable-buffer-handoff", temp_dir.path());
        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (completion_tx, mut completion_rx) = pipeline_completion_msg_channel(10);
                ctx.set_pipeline_completion_sender(completion_tx);
                let holder = hold_storage(&path).await;

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("message held while waiting");
                assert!(!ctx.accept_pdata(), "pdata delivery pauses while waiting");
                timer_tick(&mut ctx).await;
                assert!(!ctx.accept_pdata(), "still waiting while the holder runs");
                let (acked, nacked) = drain_upstream_completions(&mut completion_rx);
                assert!(acked.is_empty() && nacked.is_empty(), "held, not answered");

                holder.shutdown().await.expect("holder shuts down");
                drop(holder);
                timer_tick(&mut ctx).await;

                assert!(
                    ctx.accept_pdata(),
                    "pdata delivery resumes after the handoff"
                );
                let (acked, nacked) = drain_upstream_completions(&mut completion_rx);
                assert_eq!(acked, expected_calldata(0..1));
                assert!(nacked.is_empty(), "{nacked:?}");
                let metrics = durable_buffer_metrics(&mut ctx).await;
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer.ingest/failures"),
                    0
                );
            })
            .validate(|_| async {});
    }

    /// Scenario: Another Quiver engine in this process holds the core's
    /// storage directory for longer than `ownership.handoff_timeout` after
    /// the durable buffer received a message.
    /// Guarantees: The first timer tick after the timeout fails the engine
    /// with an error naming the timeout, NACKs the held message so upstream
    /// can resend it, counts it as an ingest failure, and stops pausing pdata
    /// delivery.
    #[test]
    fn engine_init_fails_when_handoff_times_out() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().to_path_buf();
        let rt = TestRuntime::new();
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-handoff-timeout",
            temp_dir.path(),
            serde_json::json!({"ownership": {"handoff_timeout": "10ms"}}),
        );
        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (completion_tx, mut completion_rx) = pipeline_completion_msg_channel(10);
                ctx.set_pipeline_completion_sender(completion_tx);
                let holder = hold_storage(&path).await;

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("message held while waiting");
                assert!(!ctx.accept_pdata());
                ctx.sleep(Duration::from_millis(20)).await;

                let err = ctx
                    .process(Message::Control(NodeControlMsg::TimerTick {}))
                    .await
                    .expect_err("handoff must time out")
                    .to_string();
                assert!(err.contains("within ownership.handoff_timeout"), "{err}");
                assert!(err.contains("is owned by another instance"), "{err}");

                assert!(ctx.accept_pdata(), "a failed engine no longer pauses pdata");
                let (acked, nacked) = drain_upstream_completions(&mut completion_rx);
                assert!(acked.is_empty(), "{acked:?}");
                assert_eq!(nacked.len(), 1, "{nacked:?}");
                assert!(
                    nacked[0].contains("storage directory was not handed over"),
                    "{nacked:?}"
                );
                let metrics = durable_buffer_metrics(&mut ctx).await;
                assert_eq!(
                    metric(&metrics, "processor.durable_buffer.ingest/failures"),
                    1
                );
                drop(holder);
            })
            .validate(|_| async {});
    }

    /// Scenario: The durable buffer is shut down while it waits for another
    /// Quiver engine in this process to release the storage directory, with
    /// one message held.
    /// Guarantees: Shutdown succeeds without waiting for the directory, NACKs
    /// the held message so upstream can resend it, and leaves the other
    /// engine's claim in place.
    #[test]
    fn shutdown_while_awaiting_handoff_nacks_held_message() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().to_path_buf();
        let rt = TestRuntime::new();
        let processor =
            create_test_durable_buffer(&rt, "durable-buffer-handoff-shutdown", temp_dir.path());
        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (completion_tx, mut completion_rx) = pipeline_completion_msg_channel(10);
                ctx.set_pipeline_completion_sender(completion_tx);
                let holder = hold_storage(&path).await;

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("message held while waiting");
                shutdown_now(&mut ctx).await;

                let (acked, nacked) = drain_upstream_completions(&mut completion_rx);
                assert!(acked.is_empty(), "{acked:?}");
                assert_eq!(nacked.len(), 1, "{nacked:?}");
                assert!(nacked[0].contains("shut down"), "{nacked:?}");
                let marker = std::fs::read_to_string(path.join("core_0").join("quiver.owner"))
                    .expect("holder marker");
                assert!(marker.contains("released=false\n"), "{marker}");
                drop(holder);
            })
            .validate(|_| async {});
    }

    /// Scenario: The first message the durable buffer receives is a
    /// telemetry collection, while another Quiver engine in this process
    /// holds the storage directory; the holder then releases it.
    /// Guarantees: Telemetry is still reported while waiting, the wait starts
    /// with nothing held, and the next timer tick after the release opens the
    /// engine so later data is ingested and ACKed.
    #[test]
    fn telemetry_starts_handoff_wait_without_held_data() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().to_path_buf();
        let rt = TestRuntime::new();
        let processor =
            create_test_durable_buffer(&rt, "durable-buffer-handoff-telemetry", temp_dir.path());
        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (completion_tx, mut completion_rx) = pipeline_completion_msg_channel(10);
                ctx.set_pipeline_completion_sender(completion_tx);
                let holder = hold_storage(&path).await;

                let metrics = durable_buffer_metrics(&mut ctx).await;
                assert!(!metrics.is_empty(), "telemetry is reported while waiting");
                assert!(!ctx.accept_pdata(), "waiting after the first message");

                drop(holder);
                timer_tick(&mut ctx).await;
                assert!(ctx.accept_pdata());
                let (acked, nacked) = drain_upstream_completions(&mut completion_rx);
                assert!(acked.is_empty() && nacked.is_empty(), "nothing was held");

                ctx.process(Message::PData(make_log_input("m-1", 1)))
                    .await
                    .expect("ingest after the handoff");
                let (acked, nacked) = drain_upstream_completions(&mut completion_rx);
                assert_eq!(acked, expected_calldata(1..2));
                assert!(nacked.is_empty(), "{nacked:?}");
            })
            .validate(|_| async {});
    }

    /// Scenario: `handoff_decision` is evaluated for ownership refusals by a
    /// holder in this process, by a holder in another process, and with no
    /// recorded holder, and for an unrelated open error, with waiting enabled
    /// and disabled.
    /// Guarantees: Only a refusal by a holder in this process waits, and only
    /// while the timeout is non-zero and not yet reached; every other case
    /// fails right away.
    #[test]
    fn handoff_decision_waits_only_for_same_process_holder() {
        let owned = |same_process: Option<bool>| QuiverError::DataDirOwned {
            data_dir: "core_0".into(),
            holder: same_process.map(|same_process| {
                Box::new(DataDirHolder {
                    owner_id: "owner".to_owned(),
                    host: "host".to_owned(),
                    pid: 1,
                    heartbeat_age: Duration::ZERO,
                    process_running: true,
                    same_process,
                })
            }),
        };
        let other = QuiverError::GenerationsExhausted {
            data_dir: "core_0".into(),
            max: 1,
        };
        let timeout = Duration::from_secs(90);
        let cases = [
            (
                owned(Some(true)),
                timeout,
                Duration::ZERO,
                HandoffDecision::Wait,
            ),
            (
                owned(Some(true)),
                timeout,
                Duration::from_secs(89),
                HandoffDecision::Wait,
            ),
            (
                owned(Some(true)),
                timeout,
                timeout,
                HandoffDecision::TimedOut,
            ),
            (
                owned(Some(true)),
                Duration::ZERO,
                Duration::ZERO,
                HandoffDecision::Fail,
            ),
            (
                owned(Some(false)),
                timeout,
                Duration::ZERO,
                HandoffDecision::Fail,
            ),
            (owned(None), timeout, Duration::ZERO, HandoffDecision::Fail),
            (other, timeout, Duration::ZERO, HandoffDecision::Fail),
        ];
        for (error, handoff_timeout, waited, expected) in cases {
            assert_eq!(
                handoff_decision(&error, handoff_timeout, waited),
                expected,
                "{error} timeout={handoff_timeout:?} waited={waited:?}"
            );
        }
    }

    /// Simulates another instance claiming the core's storage directory after
    /// the durable buffer claimed generation 1: a claim creates the next
    /// generation file and removes the older ones, which fences the durable
    /// buffer's engine.
    fn claim_storage_elsewhere(path: &std::path::Path) {
        let core_dir = path.join("core_0");
        std::fs::write(core_dir.join("quiver.gen.00000000000000000002"), b"")
            .expect("write generation file");
        std::fs::remove_file(core_dir.join("quiver.gen.00000000000000000001"))
            .expect("the durable buffer holds generation 1");
    }

    /// Scenario: With `forward_after_ingest: true` and the default upstream
    /// ACK, one bundle is ingested and ACKed, then another instance claims
    /// the storage directory and two more bundles arrive, followed by a timer
    /// tick and shutdown.
    /// Guarantees: Both bundles that arrive after the claim are NACKed with
    /// an error naming the claim and are never ACKed or sent downstream, and
    /// the node stops reading stored bundles without failing.
    #[test]
    fn fenced_storage_nacks_ingest_with_default_ack() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().to_path_buf();
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-fenced-default-ack",
            temp_dir.path(),
            serde_json::json!({
                "forward_after_ingest": true,
                "wal_flush_interval": "1h",
                "max_segment_open_duration": "1h",
            }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("ingest");
                assert_eq!(
                    drain_upstream_completions(&mut rx).0,
                    expected_calldata(0..1)
                );
                assert_eq!(ctx.drain_pdata().await.len(), 1, "sent during ingest");

                claim_storage_elsewhere(&path);
                for id in 1..3 {
                    ctx.process(Message::PData(make_log_input(&format!("m-{id}"), id)))
                        .await
                        .expect("a fenced ingest is answered with a NACK");
                }
                let (acked, nacked) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty(), "a fenced node must not ACK");
                assert_eq!(nacked.len(), 2);
                assert!(
                    nacked
                        .iter()
                        .all(|reason| reason.contains("was claimed by")),
                    "{nacked:?}"
                );
                assert!(ctx.drain_pdata().await.is_empty(), "nothing is forwarded");

                timer_tick(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty());
                shutdown_now(&mut ctx).await;
                assert!(ctx.drain_pdata().await.is_empty());
                let (acked, _) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty());
            })
            .validate(|_| async {});
    }

    /// Scenario: With `forward_after_ingest: true` and `upstream_ack:
    /// after_wal_sync`, one bundle is ingested and its ACK is held, then
    /// another instance claims the storage directory, one more bundle
    /// arrives, and the WAL sync deadline wakeup fires.
    /// Guarantees: The new bundle is NACKed during ingest, the held bundle is
    /// NACKed when its WAL sync is refused, and no ACK reaches upstream for
    /// either of them.
    #[test]
    fn fenced_storage_nacks_held_acks_with_after_wal_sync() {
        use otel_arrow_dfe_engine::control::pipeline_completion_msg_channel;
        use otel_arrow_dfe_engine::testing::processor::TestRuntime;

        let rt = TestRuntime::new();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().to_path_buf();
        let processor = create_test_durable_buffer_with(
            &rt,
            "durable-buffer-fenced-wal-sync",
            temp_dir.path(),
            serde_json::json!({
                "forward_after_ingest": true,
                "upstream_ack": "after_wal_sync",
                "wal_flush_interval": "1h",
                "max_segment_open_duration": "1h",
            }),
        );

        rt.set_processor(processor)
            .run_test(move |mut ctx| async move {
                let (tx, mut rx) = pipeline_completion_msg_channel(16);
                ctx.set_pipeline_completion_sender(tx);

                ctx.process(Message::PData(make_log_input("m-0", 0)))
                    .await
                    .expect("ingest");
                assert_eq!(ctx.drain_pdata().await.len(), 1, "sent during ingest");
                let (acked, nacked) = drain_upstream_completions(&mut rx);
                assert!(
                    acked.is_empty() && nacked.is_empty(),
                    "upstream ACK is held"
                );

                claim_storage_elsewhere(&path);
                ctx.process(Message::PData(make_log_input("m-1", 1)))
                    .await
                    .expect("a fenced ingest is answered with a NACK");
                let (acked, nacked) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty());
                assert_eq!(nacked.len(), 1);
                assert!(nacked[0].contains("was claimed by"), "{nacked:?}");
                assert!(ctx.drain_pdata().await.is_empty(), "nothing is forwarded");

                assert!(ctx.fire_wakeup().await.expect("WAL sync wakeup"));
                let (acked, nacked) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty(), "the held ACK must not be released");
                assert_eq!(nacked.len(), 1);
                assert!(nacked[0].contains("was claimed by"), "{nacked:?}");
                assert_eq!(wal_sync_counters(&mut ctx).await, (0, 1));

                shutdown_now(&mut ctx).await;
                let (acked, _) = drain_upstream_completions(&mut rx);
                assert!(acked.is_empty());
            })
            .validate(|_| async {});
    }
}
