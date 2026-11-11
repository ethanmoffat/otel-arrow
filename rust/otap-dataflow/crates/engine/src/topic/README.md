# Topic Module

This module provides the engine-facing topic abstraction used to decouple
pipeline nodes through named topics.

For a higher-level design view with architecture diagrams, see
[`docs/topic-architecture.md`](../../../../docs/topic-architecture.md).

## Goals

- Backend-agnostic API for topic publish/subscribe.
- Decouple publisher and subscriber nodes through named topics.
- Support both balanced and broadcast subscriptions behind one contract.
- Keep engine-facing code stable while allowing multiple backends
  (in-memory now, persistent/distributed later).

## Architecture

The topic module is organized in three layers:

1. Public API layer

- `TopicBroker<T>`: create, lookup, list, remove, and close topics.
- `TopicHandle<T>`: pure publish and subscribe runtime entry point.
- `PipelineTopicBinding<T>`: pipeline-scoped wrapper with resolved
  `queue_on_full` and `ack_propagation` defaults.
- `Subscription<T>`: receive + ack/nack interface.
- `TopicSet<T>`: per-pipeline resolved map of local topic alias ->
  `PipelineTopicBinding<T>`.

1. Backend abstraction layer

- `TopicBackend<T>`: factory creating per-topic state.
- `TopicState<T>`: shared per-topic operations (`publish`,
  `try_publish`, subscribe, close).
- `SubscriptionBackend<T>`: per-subscription receive + ack/nack backend.

1. Backend implementation layer (current)

- `InMemoryBackend` creates `TopicInner<T>`.
- `TopicInner<T>` selects one variant from `TopicOptions`:
  - `BalancedOnly`
  - `KeyedBalanced`
  - `BroadcastOnly`
  - `Mixed`

High-level flow:

```text
Publisher/Subscriber Node
    -> TopicSet (local alias resolution)
    -> PipelineTopicBinding
    -> TopicHandle
    -> dyn TopicState (backend-agnostic contract)
    -> backend implementation (in-memory today)
```

## YAML Mapping

Runtime YAML declares topics under `topics.<name>` or
`groups.<group>.topics.<name>`:

```yaml
topics:
  ingress:
    backend: in_memory
    impl_selection: auto
    policies:
      balanced:
        queue_capacity: 1024
        on_full: block
      broadcast:
        queue_capacity: 4096
        on_lag: drop_oldest
        ack_mode: first
      ack_propagation:
        mode: disabled
        max_in_flight: 1024
        timeout: 30s
```

Mapping from YAML to runtime behavior:

- `receiver:topic` with `subscription.mode: balanced` uses the
  balanced delivery path.
- `receiver:topic` with `subscription.mode: broadcast` uses the
  broadcast delivery path.
- The controller infers `TopicOptions::BalancedOnly`,
  `BroadcastOnly`, or `Mixed` from actual topic usage.
- `impl_selection: force_mixed` disables that optimization and always
  selects `TopicOptions::Mixed`.
- `balanced.queue_capacity` and `balanced.on_full` govern balanced
  consumer-group queues.
- `broadcast.queue_capacity`, `broadcast.on_lag`, and `broadcast.ack_mode`
  govern the broadcast ring, slow-subscriber behavior, and tracked Ack/Nack
  aggregation.
- `balanced.keyed_routing` enables keyed balanced delivery and selects
  `TopicOptions::KeyedBalanced`. See
  [Keyed balanced delivery](#keyed-balanced-delivery).
- `ack_propagation.mode` controls whether topic hops can bridge Ack/Nack
  across pipelines.
- `ack_propagation.max_in_flight` and `ack_propagation.timeout` govern
  tracked publish outcomes per publisher handle when propagation is enabled.
- `exporter:topic.config.queue_on_full` is a per-publisher override
  for balanced full-queue behavior; it does not override broadcast lag
  policy.
- The controller rejects cycles in the global topic wiring graph at
  startup, including same-pipeline feedback through a topic and
  multi-pipeline topic loops.

`broadcast.ack_mode` defaults to `first`, preserving first-subscriber-wins
behavior. Set it to `all` on a broadcast-only topic to Ack upstream only after
every subscriber eligible at publish time Acks. Any required Nack, or a
required subscriber disappearing before Acking, resolves upstream as Nack.
An empty eligible-subscriber snapshot resolves immediately as Nack, so a
producer cannot report successful delivery before any topic receiver subscribes
during startup or live reconfiguration. Recovery still requires the upstream
source to retry or durably buffer Nacked messages.

`all` requires `ack_propagation.mode: auto` and
`broadcast.on_lag: disconnect`; startup rejects disabled Ack propagation,
mixed topics, and balanced-only topics.

Choose the component after each topic receiver based on the required Ack
boundary:

```text
export completion:  receiver -> retry          -> exporter
durable acceptance: receiver -> durable buffer -> exporter
```

Retry branches keep the topic outcome pending until exporters reach terminal
outcomes. Durable-buffer branches Ack after persistence and retry exporters
independently. The topic timeout must exceed the corresponding retry budget or
durable write latency. `all` is not a distributed transaction and does not
roll back destinations that already exported successfully.

## Keyed balanced delivery

By default a balanced group shares one queue, so consecutive messages
that belong together (for example, all messages for one tenant) can be
processed by different consumers in parallel and finish out of order.
Keyed balanced delivery routes each message by a **routing key** so that
messages with the same key keep their order end to end while different
keys still spread across the consumers of the group.

Enable it on the topic and name the key source on the topic exporter:

```yaml
topics:
  ingress:
    policies:
      balanced:
        queue_capacity: 1024
        on_full: block
        keyed_routing:
          load_factor_percent: 125
      ack_propagation:
        mode: auto

# exporter:topic node config
config:
  topic: ingress
  routing_key_from_transport_header: x-tenant
```

The exporter reads the first value of the named transport header
(case-insensitive) from the message context and hashes it into a
`RoutingKey`. The header must be captured into the context by the
[transport header policy](../../../../docs/transport-headers.md).
Messages without the header are unkeyed.

Routing rules:

- Each consumer in the group gets its own bounded queue of
  `balanced.queue_capacity` messages.
- The topic keeps a per-key count of messages that have no terminal
  outcome yet. A keyed message is in flight from publish until it is
  Acked, Nacked, times out, or is dropped. Delivery to the consumer
  does not end it.
- A key with in-flight messages always goes to the consumer that holds
  it, so the key never runs on two consumers at once. A key can move
  only when its count is 0.
- A new or idle key is placed by a rendezvous hash among consumers
  whose load is below `ceil(load_factor_percent / 100 * average load)`,
  where the average counts the message being placed. The least loaded
  consumer is always below the cap, so one busy range of keys cannot pin
  a consumer while others idle.
- The load of a consumer is the number of messages routed to it that
  have no terminal outcome yet: queued messages plus delivered tracked
  messages that are not resolved.
- Unkeyed messages go to the consumer with the lowest load.
- When the chosen queue is full, `balanced.on_full` applies (`block`
  waits for that queue, `drop_newest` drops the message). A dropped
  message releases its key right away.
- The key table is sharded and only holds keys with in-flight messages,
  so its size is bounded by the number of unresolved messages.
- When a consumer departs, it stops receiving new messages and its
  queued tracked messages are Nacked, which releases their keys. The
  next message for a key it held goes to a live consumer.

Requirements and validation:

- Keyed routing requires `ack_propagation.mode: auto`. The topic counts
  a key down only on a terminal outcome, so the exporter tracks keyed
  messages even when the upstream message has no Ack/Nack interest.
- `load_factor_percent` defaults to 125 and must be greater than 100.
- Keyed routing supports one balanced group and no broadcast
  subscribers on the topic. The controller rejects keyed topics that
  would select `Mixed` or `BroadcastOnly`, including topics with
  `impl_selection: force_mixed` or without a topic receiver, and the
  topic rejects a second balanced group.
- `routing_key_from_transport_header` on an exporter requires the
  target topic to enable `keyed_routing`.

> Note that a single hot key cannot spread across consumers. Stickiness
> wins over balance for one key, so one key that carries most of the
> traffic is processed by one consumer at a time.
>
> Note that `durable_buffer` does not persist the message context today
> (see #4058). A routing key set before a `durable_buffer` is gone after
> it, so set or restore the key after any `durable_buffer` in the
> pipeline.

## Observability Notes

Current topic exporter and receiver metrics cover publish behavior, lag, and
Ack/Nack bridging. A future observability improvement is to expose topic-owned
gauges for balanced group count and broadcast subscriber count once per topic,
rather than duplicating those values in node-local metrics.

## Backend Capability Contract

Current minimal behavior:

- The controller validates topic declarations before broker creation.
- Unsupported backend, mode, or policy combinations fail fast at
  startup with explicit errors.
- Capability checks are startup-only; publish/recv hot paths are unchanged.
- The controller capability table records keyed balanced routing
  support per backend. The in-memory backend supports it.

Recommended full engine-level contract for a future second backend:

1. Each backend should declare a `TopicBackendCapabilities` contract in
   the engine topic layer.
1. Capabilities should cover backend availability plus support for the
   selected runtime mode (`BalancedOnly`, `BroadcastOnly`, `Mixed`) and
   policy families such as `broadcast.on_lag`,
   `balanced.keyed_routing`, and `ack_propagation.mode`.
1. Topic creation should validate the selected backend, mode, and
   policies against that contract before instantiating backend state.
1. Failures should use explicit errors such as
   `UnsupportedTopicBackend`, `UnsupportedTopicMode`, and
   `UnsupportedTopicPolicy` rather than generic internal errors.
1. Capability validation should remain on the topic creation path only;
   it should not add work to publish or receive hot paths.

That fuller contract is not implemented in the engine layer yet. Today
the controller owns the startup validation, while the broker/backend
API stays unchanged and infallible for the built-in in-memory backend.

## Example Use Cases

### 1. Work distribution (balanced)

```rust
use otel_arrow_dfe_engine::topic::{
    SubscriberOptions, SubscriptionMode, TopicBroker, TopicOptions,
};
use otel_arrow_dfe_config::SubscriptionGroupName;

let broker = TopicBroker::<u64>::new();
let topic = broker.create_in_memory_topic(
    "ingress",
    TopicOptions::BalancedOnly { capacity: 1024 },
)?;

let mut worker_a = topic.subscribe(
    SubscriptionMode::Balanced {
        group: SubscriptionGroupName::from("workers"),
    },
    SubscriberOptions::default(),
)?;
let mut worker_b = topic.subscribe(
    SubscriptionMode::Balanced {
        group: SubscriptionGroupName::from("workers"),
    },
    SubscriberOptions::default(),
)?;

// Each published message is delivered to exactly one subscriber in
// "workers".
```

### 2. Fan-out analytics (broadcast)

```rust
use otel_arrow_dfe_engine::topic::{
    SubscriberOptions, SubscriptionMode, TopicBroadcastOnLagPolicy,
    TopicBroker, TopicOptions,
};

let broker = TopicBroker::<u64>::new();
let topic = broker.create_in_memory_topic(
    "audit",
    TopicOptions::BroadcastOnly {
        capacity: 4096,
        on_lag: TopicBroadcastOnLagPolicy::DropOldest,
    },
)?;

let mut sink_a = topic.subscribe(
    SubscriptionMode::Broadcast,
    SubscriberOptions::default(),
)?;
let mut sink_b = topic.subscribe(
    SubscriptionMode::Broadcast,
    SubscriberOptions::default(),
)?;

// Both subscribers receive each message from their subscribe point onward.
```

### 3. Mixed criticality (balanced + broadcast on one topic)

```rust
use otel_arrow_dfe_engine::topic::{
    SubscriberOptions, SubscriptionMode, TopicBroadcastOnLagPolicy,
    TopicBroker, TopicOptions,
};
use otel_arrow_dfe_config::SubscriptionGroupName;

let broker = TopicBroker::<u64>::new();
let topic = broker.create_in_memory_topic(
    "events",
    TopicOptions::Mixed {
        balanced_capacity: 1024,
        broadcast_capacity: 4096,
        on_lag: TopicBroadcastOnLagPolicy::DropOldest,
    },
)?;

let mut primary = topic.subscribe(
    SubscriptionMode::Balanced {
        group: SubscriptionGroupName::from("primary"),
    },
    SubscriberOptions::default(),
)?;
let mut security = topic.subscribe(
    SubscriptionMode::Broadcast,
    SubscriberOptions::default(),
)?;
let mut anomaly = topic.subscribe(
    SubscriptionMode::Broadcast,
    SubscriberOptions::default(),
)?;
```

### 4. Backend selection per topic

```rust
use otel_arrow_dfe_engine::topic::{
    InMemoryBackend, TopicBroker, TopicOptions,
};

let broker = TopicBroker::<u64>::new();
let _topic = broker.create_topic(
    "t1",
    TopicOptions::default(),
    InMemoryBackend,
)?;

// Another backend can be plugged in later via the same TopicBackend trait.
```

## Public Contracts

### Runtime lifecycle

1. Topics are created with `TopicBroker::create_topic` /
   `create_topics` (or in-memory convenience methods).
2. Nodes obtain a `TopicHandle` via broker lookup, or a
   `PipelineTopicBinding` from a `TopicSet`.
3. Publishers use `publish`/`try_publish`; subscribers use `subscribe`
   -> `Subscription::recv`.
4. `TopicHandle::close` or `TopicBroker::remove_topic` closes the topic.
5. After close:

- publish operations return `Error::TopicClosed`.
- receive side eventually observes `Error::SubscriptionClosed`.
- balanced subscribe attempts are rejected with `Error::TopicClosed`.

### Publish contract

- `publish(msg).await`
  - Accepts a message or returns an error.
  - May await under balanced backpressure.
- `try_publish(msg)`
  - Never awaits.
  - Returns `PublishOutcome::Published` or `PublishOutcome::DroppedOnFull`.
- `TrackedTopicPublisher::publish_keyed` and `try_publish_keyed` take an
  optional `RoutingKey` and follow the same contract as `publish` and
  `try_publish`. The key is used only on `KeyedBalanced` topics.
- In `Mixed` mode, `try_publish` is all-or-nothing across balanced and
  broadcast delivery. If any balanced queue is full, it returns
  `DroppedOnFull` and does not publish to broadcast.
- Messages are not retained for future subscribers: subscribing later
  does not replay pre-subscribe history.

### Delivery + ack/nack contract

- Balanced mode (`SubscriptionMode::Balanced { group }`)
  - One consumer group receives one logical stream.
  - Within a group, each delivered message goes to exactly one subscriber.
  - Different groups receive independently.
  - With keyed routing, messages with the same key go to one subscriber
    while the key has in-flight messages, in publish order.
- Broadcast mode (`SubscriptionMode::Broadcast`)
  - Each subscriber receives each message from its subscribe point.
  - With `TopicBroadcastOnLagPolicy::DropOldest`, slow subscribers may
    receive `RecvItem::Lagged { missed }` and continue from the oldest
    retained message.
  - With `TopicBroadcastOnLagPolicy::Disconnect`, slow subscribers
    receive a final `RecvItem::Lagged { missed }` and the next
    `recv()` returns `Error::SubscriptionClosed`.
- Ack/Nack
  - Subscribers ack/nack by message ID (`Subscription::ack` /
    `Subscription::nack`).
  - Publishers that need terminal Ack/Nack use
    `TopicHandle::tracked_publisher()`.
  - Tracked publishes resolve through `TrackedPublishReceipt`.
  - Ack/nack on an untracked message returns `Error::MessageNotTracked`.

## Guarantees

- One stable engine-facing contract for topic operations, independent
  of backend implementation.
- Strongly-typed topic and balanced group identifiers (`TopicName`,
  `SubscriptionGroupName`) at API boundaries.
- Explicit mode compatibility errors:
  - `Error::SubscribeBalancedNotSupported`
  - `Error::SubscribeBroadcastNotSupported`
  - `Error::SubscribeSingleGroupViolation` (for balanced-only topics
    with a different group)
- Broker duplicate checks are atomic in `create_topics`: if any
  duplicate exists, no topic from that batch is inserted.
- `TopicSet::remove` only detaches local alias mapping; it does not
  close the underlying topic.
- With keyed routing, each key reaches at most one consumer at a time
  and its messages are queued in publish order. A key moves only when
  its in-flight count is 0, or when its consumer departs.
- Each terminal outcome (Ack, Nack, timeout, drop on full, departure,
  topic close) releases a keyed message exactly once.

## Current Limits

- Current integrated backend is in-memory; no built-in persistent or
  distributed backend is wired yet.
- Delivery is bounded by configured capacities:
  - balanced channels are bounded and can backpressure (`publish`) or
    report drop-on-full (`try_publish`)
  - broadcast uses a bounded ring buffer and slow subscribers either
    continue after `RecvItem::Lagged` or disconnect, depending on
    `TopicBroadcastOnLagPolicy`
- No replay/history for late subscribers: delivery starts from subscribe time.
- `SubscriberOptions` is currently empty (no per-subscriber tuning
  knobs exposed yet).
- Keyed routing supports a single balanced group with no broadcast
  subscribers, and a single hot key cannot spread across consumers.
- When a consumer departs with keyed messages it already received but
  has not resolved, the next message for those keys goes to another
  consumer before those earlier messages resolve, so per-key order is
  not guaranteed across a departure.
- The keyed load cap is checked without a global lock, so concurrent
  publishers can briefly place keys slightly above it.

## Non-Goals

- No guarantee that all backends provide identical internal behavior
  for every policy.
- No persistence/distributed semantics in this abstraction layer itself
  (in the current impl).
- No exactly-once semantics promised by this module.
