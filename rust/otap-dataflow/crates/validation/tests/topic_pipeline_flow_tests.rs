// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end topic flow tests for OTAP topic nodes.

use bytes::Bytes;
use otel_arrow_dfe_config::TopicName;
use otel_arrow_dfe_config::context::ContextEntryName;
use otel_arrow_dfe_config::context::transport_headers::{
    TransportHeader, TransportHeaders, ValueKind,
};
use otel_arrow_dfe_config::node::{NodeKind, NodeUserConfig};
use otel_arrow_dfe_config::pipeline::{PipelineConfigBuilder, PipelineType};
use otel_arrow_dfe_core_nodes::exporters::topic_exporter::{TOPIC_EXPORTER, TOPIC_EXPORTER_URN};
use otel_arrow_dfe_core_nodes::receivers::topic_receiver::{TOPIC_RECEIVER, TOPIC_RECEIVER_URN};
use otel_arrow_dfe_engine::Interests;
use otel_arrow_dfe_engine::config::{ExporterConfig, ReceiverConfig};
use otel_arrow_dfe_engine::control::{
    Controllable, NodeControlMsg, pipeline_completion_msg_channel, runtime_ctrl_msg_channel,
};
use otel_arrow_dfe_engine::effect_handler::SourceTagging;
use otel_arrow_dfe_engine::local::message::{LocalReceiver, LocalSender};
use otel_arrow_dfe_engine::message::{Receiver as PDataReceiver, Sender as PDataSender};
use otel_arrow_dfe_engine::node::{NodeWithPDataReceiver, NodeWithPDataSender};
use otel_arrow_dfe_engine::testing::exporter::create_test_pipeline_context;
use otel_arrow_dfe_engine::testing::install_test_context_bindings;
use otel_arrow_dfe_engine::testing::{
    create_not_send_channel, setup_test_runtime, test_node, test_pipeline_runtime_services,
};
use otel_arrow_dfe_engine::topic::{
    PipelineTopicBinding, TopicAckPropagationMode, TopicBroadcastAckMode,
    TopicBroadcastOnLagPolicy, TopicBroker, TopicOptions, TopicSet,
};
use otel_arrow_dfe_otap::OTAP_PIPELINE_FACTORY;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::OtlpProtoBytes;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::InstrumentationScope;
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use prost::Message as _;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn make_test_pdata() -> OtapPdata {
    let logs = otel_arrow_dfe_pdata::testing::fixtures::log_with_no_scope();
    let mut bytes = Vec::new();
    logs.encode(&mut bytes).expect("log payload should encode");
    OtapPdata::new_todo_context(OtlpProtoBytes::ExportLogsRequest(Bytes::from(bytes)).into())
}

/// Scenario: an OTAP topic exporter sends pdata to a receiver on the same broker topic.
/// Guarantees: the receiver obtains the original signal payload.
#[test]
fn topic_exporter_to_topic_receiver_transfers_pdata() {
    let (rt, local_tasks) = setup_test_runtime();
    rt.block_on(local_tasks.run_until(async move {
        let broker = TopicBroker::<OtapPdata>::new();
        let topic_name = TopicName::parse("ingress").expect("topic name should parse");
        let handle = broker
            .create_in_memory_topic(
                topic_name.clone(),
                TopicOptions::Mixed {
                    balanced_capacity: 16,
                    broadcast_capacity: 16,
                    on_lag: TopicBroadcastOnLagPolicy::DropOldest,
                    ack_mode: TopicBroadcastAckMode::First,
                },
            )
            .expect("topic should be created");

        let exporter_set = TopicSet::new("exporter-set");
        _ = exporter_set.insert(topic_name.clone(), handle.clone());
        let receiver_set = TopicSet::new("receiver-set");
        _ = receiver_set.insert(topic_name.clone(), handle);

        let mut exporter_ctx = create_test_pipeline_context();
        exporter_ctx.set_topic_set(exporter_set);
        let mut receiver_ctx = create_test_pipeline_context();
        receiver_ctx.set_topic_set(receiver_set);

        let exporter_node = test_node("topic_exporter");
        let receiver_node = test_node("topic_receiver");

        let mut exporter_user_cfg = NodeUserConfig::new_exporter_config(TOPIC_EXPORTER_URN);
        exporter_user_cfg.config = json!({
            "topic": "ingress",
            "queue_on_full": "block"
        });
        let mut receiver_user_cfg = NodeUserConfig::new_receiver_config(TOPIC_RECEIVER_URN);
        receiver_user_cfg.config = json!({
            "topic": "ingress",
            "subscription": {
                "mode": "balanced",
                "group": "sut-workers"
            }
        });

        let mut exporter = (TOPIC_EXPORTER.create)(
            exporter_ctx,
            exporter_node.clone(),
            Arc::new(exporter_user_cfg),
            &ExporterConfig::new("topic_exporter"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("topic exporter should be created");

        let mut receiver = (TOPIC_RECEIVER.create)(
            receiver_ctx,
            receiver_node.clone(),
            Arc::new(receiver_user_cfg),
            &ReceiverConfig::new("topic_receiver"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("topic receiver should be created");

        let (exporter_input_tx, exporter_input_rx) = create_not_send_channel::<OtapPdata>(8);
        exporter
            .set_pdata_receiver(
                exporter_node.clone(),
                PDataReceiver::Local(LocalReceiver::mpsc(exporter_input_rx)),
            )
            .expect("exporter input channel should be wired");

        let (receiver_output_tx, receiver_output_rx) = create_not_send_channel::<OtapPdata>(8);
        receiver
            .set_pdata_sender(
                receiver_node.clone(),
                "".into(),
                PDataSender::Local(LocalSender::mpsc(receiver_output_tx)),
            )
            .expect("receiver output channel should be wired");

        let exporter_ctrl = exporter.control_sender();
        let receiver_ctrl = receiver.control_sender();
        let (runtime_ctrl_tx, _runtime_ctrl_rx) = runtime_ctrl_msg_channel::<OtapPdata>(32);
        let (pipeline_completion_tx, _pipeline_completion_rx) =
            pipeline_completion_msg_channel::<OtapPdata>(32);
        let (_metrics_rx, metrics_reporter) = MetricsReporter::create_new_and_receiver(64);
        let exporter_metrics = metrics_reporter.clone();
        let receiver_metrics = metrics_reporter.clone();
        let exporter_ctrl_tx = runtime_ctrl_tx.clone();
        let receiver_ctrl_tx = runtime_ctrl_tx.clone();
        let exporter_completion_tx = pipeline_completion_tx.clone();
        let receiver_completion_tx = pipeline_completion_tx.clone();
        let runtime_services = test_pipeline_runtime_services();
        let exporter_runtime_services = runtime_services.clone();

        let exporter_task = tokio::task::spawn_local(async move {
            exporter
                .start(
                    exporter_ctrl_tx,
                    exporter_completion_tx,
                    exporter_metrics,
                    Interests::empty(),
                    exporter_runtime_services,
                )
                .await
        });
        let receiver_task = tokio::task::spawn_local(async move {
            receiver
                .start(
                    receiver_ctrl_tx,
                    receiver_completion_tx,
                    receiver_metrics,
                    Interests::empty(),
                    runtime_services,
                )
                .await
        });

        let mut expected = make_test_pdata();
        exporter_input_tx
            .send(expected.clone())
            .expect("message should be sent to topic exporter");

        let mut delivered = tokio::time::timeout(Duration::from_secs(2), receiver_output_rx.recv())
            .await
            .expect("timed out waiting for topic receiver output")
            .expect("receiver output channel should stay open");

        assert_eq!(
            delivered.num_items(),
            expected.num_items(),
            "topic receiver should forward one pdata message"
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        exporter_ctrl
            .send(NodeControlMsg::Shutdown {
                deadline,
                reason: "test shutdown".to_owned(),
            })
            .await
            .expect("exporter shutdown should be sent");
        receiver_ctrl
            .send(NodeControlMsg::Shutdown {
                deadline,
                reason: "test shutdown".to_owned(),
            })
            .await
            .expect("receiver shutdown should be sent");

        let exporter_result = exporter_task.await.expect("exporter task should join");
        let receiver_result = receiver_task.await.expect("receiver task should join");
        assert!(exporter_result.is_ok(), "exporter should stop cleanly");
        assert!(receiver_result.is_ok(), "receiver should stop cleanly");
    }));
}

/// Scenario: a topic receiver is configured to tag incoming pdata with its source.
/// Guarantees: received pdata contains the configured source tag.
#[test]
fn topic_receiver_applies_source_tag_when_enabled() {
    let (rt, local_tasks) = setup_test_runtime();
    rt.block_on(local_tasks.run_until(async move {
        let broker = TopicBroker::<OtapPdata>::new();
        let topic_name = TopicName::parse("ingress").expect("topic name should parse");
        let handle = broker
            .create_in_memory_topic(
                topic_name.clone(),
                TopicOptions::Mixed {
                    balanced_capacity: 16,
                    broadcast_capacity: 16,
                    on_lag: TopicBroadcastOnLagPolicy::DropOldest,
                    ack_mode: TopicBroadcastAckMode::First,
                },
            )
            .expect("topic should be created");

        let exporter_set = TopicSet::new("exporter-set");
        _ = exporter_set.insert(topic_name.clone(), handle.clone());
        let receiver_set = TopicSet::new("receiver-set");
        _ = receiver_set.insert(topic_name.clone(), handle);

        let mut exporter_ctx = create_test_pipeline_context();
        exporter_ctx.set_topic_set(exporter_set);
        let mut receiver_ctx = create_test_pipeline_context();
        receiver_ctx.set_topic_set(receiver_set);

        let exporter_node = test_node("topic_exporter");
        let receiver_node = test_node("topic_receiver");

        let mut exporter_user_cfg = NodeUserConfig::new_exporter_config(TOPIC_EXPORTER_URN);
        exporter_user_cfg.config = json!({"topic": "ingress"});
        let mut receiver_user_cfg = NodeUserConfig::new_receiver_config(TOPIC_RECEIVER_URN);
        receiver_user_cfg.config = json!({
            "topic": "ingress",
            "subscription": {
                "mode": "balanced",
                "group": "sut-workers"
            }
        });

        let mut exporter = (TOPIC_EXPORTER.create)(
            exporter_ctx,
            exporter_node.clone(),
            Arc::new(exporter_user_cfg),
            &ExporterConfig::new("topic_exporter"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("topic exporter should be created");

        let mut receiver = (TOPIC_RECEIVER.create)(
            receiver_ctx,
            receiver_node.clone(),
            Arc::new(receiver_user_cfg),
            &ReceiverConfig::new("topic_receiver"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("topic receiver should be created");

        // Match runtime fan-in behavior where source tagging is enabled.
        receiver.set_source_tagging(SourceTagging::Enabled);

        let (exporter_input_tx, exporter_input_rx) = create_not_send_channel::<OtapPdata>(8);
        exporter
            .set_pdata_receiver(
                exporter_node.clone(),
                PDataReceiver::Local(LocalReceiver::mpsc(exporter_input_rx)),
            )
            .expect("exporter input channel should be wired");

        let (receiver_output_tx, receiver_output_rx) = create_not_send_channel::<OtapPdata>(8);
        receiver
            .set_pdata_sender(
                receiver_node.clone(),
                "".into(),
                PDataSender::Local(LocalSender::mpsc(receiver_output_tx)),
            )
            .expect("receiver output channel should be wired");

        let exporter_ctrl = exporter.control_sender();
        let receiver_ctrl = receiver.control_sender();
        let (runtime_ctrl_tx, _runtime_ctrl_rx) = runtime_ctrl_msg_channel::<OtapPdata>(32);
        let (pipeline_completion_tx, _pipeline_completion_rx) =
            pipeline_completion_msg_channel::<OtapPdata>(32);
        let (_metrics_rx, metrics_reporter) = MetricsReporter::create_new_and_receiver(64);
        let exporter_metrics = metrics_reporter.clone();
        let receiver_metrics = metrics_reporter.clone();
        let exporter_ctrl_tx = runtime_ctrl_tx.clone();
        let receiver_ctrl_tx = runtime_ctrl_tx.clone();
        let exporter_completion_tx = pipeline_completion_tx.clone();
        let receiver_completion_tx = pipeline_completion_tx.clone();
        let runtime_services = test_pipeline_runtime_services();
        let exporter_runtime_services = runtime_services.clone();

        let exporter_task = tokio::task::spawn_local(async move {
            exporter
                .start(
                    exporter_ctrl_tx,
                    exporter_completion_tx,
                    exporter_metrics,
                    Interests::empty(),
                    exporter_runtime_services,
                )
                .await
        });
        let receiver_task = tokio::task::spawn_local(async move {
            receiver
                .start(
                    receiver_ctrl_tx,
                    receiver_completion_tx,
                    receiver_metrics,
                    Interests::empty(),
                    runtime_services,
                )
                .await
        });

        exporter_input_tx
            .send(make_test_pdata())
            .expect("message should be sent to topic exporter");

        let delivered = tokio::time::timeout(Duration::from_secs(2), receiver_output_rx.recv())
            .await
            .expect("timed out waiting for topic receiver output")
            .expect("receiver output channel should stay open");

        assert_eq!(
            delivered.get_source_node(),
            Some(receiver_node.index),
            "receiver should tag outgoing pdata with source node when enabled"
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        exporter_ctrl
            .send(NodeControlMsg::Shutdown {
                deadline,
                reason: "test shutdown".to_owned(),
            })
            .await
            .expect("exporter shutdown should be sent");
        receiver_ctrl
            .send(NodeControlMsg::Shutdown {
                deadline,
                reason: "test shutdown".to_owned(),
            })
            .await
            .expect("receiver shutdown should be sent");

        let exporter_result = exporter_task.await.expect("exporter task should join");
        let receiver_result = receiver_task.await.expect("receiver task should join");
        assert!(exporter_result.is_ok(), "exporter should stop cleanly");
        assert!(receiver_result.is_ok(), "receiver should stop cleanly");
    }));
}

/// Builds a logs payload whose item count identifies the message in tests.
fn make_logs_pdata_with_items(items: usize) -> OtapPdata {
    let records: Vec<LogRecord> = (0..items)
        .map(|i| LogRecord::build().time_unix_nano(i as u64 + 1).finish())
        .collect();
    let logs = LogsData::new(vec![ResourceLogs::new(
        Resource::default(),
        vec![ScopeLogs::new(InstrumentationScope::default(), records)],
    )]);
    let mut bytes = Vec::new();
    logs.encode(&mut bytes).expect("log payload should encode");
    OtapPdata::new_todo_context(OtlpProtoBytes::ExportLogsRequest(Bytes::from(bytes)).into())
}

/// Scenario: a topic exporter reads a routing key from a transport header and
/// publishes to a keyed balanced topic consumed by two topic receivers in one
/// group. Four messages for key `a` are published before four for key `b`,
/// and no message is acknowledged while they are delivered.
/// Guarantees: every message of a key reaches the same receiver in publish
/// order, and the second key is placed on the other receiver because the
/// first receiver is at the load cap.
#[test]
fn topic_exporter_routes_by_transport_header_key_to_sticky_receivers() {
    const PER_KEY: usize = 4;
    let (rt, local_tasks) = setup_test_runtime();
    rt.block_on(local_tasks.run_until(async move {
        let broker = TopicBroker::<OtapPdata>::new();
        let topic_name = TopicName::parse("ingress").expect("topic name should parse");
        let handle = broker
            .create_in_memory_topic(
                topic_name.clone(),
                TopicOptions::KeyedBalanced {
                    capacity: 16,
                    load_factor_percent: 125,
                },
            )
            .expect("topic should be created");
        let binding = PipelineTopicBinding::new(handle)
            .with_default_ack_propagation_mode(TopicAckPropagationMode::Auto);
        let topic_set = TopicSet::new("topic-set");
        _ = topic_set.insert(topic_name.clone(), binding);

        let header = ContextEntryName::try_from("x-tenant").expect("valid context entry name");
        let exporter_user_config = json!({
            "topic": "ingress",
            "routing_key_from_transport_header": header.as_ref(),
        });
        let pipeline_config = PipelineConfigBuilder::new()
            .add_receiver(
                "upstream",
                TOPIC_RECEIVER_URN,
                Some(json!({"topic": "ingress"})),
            )
            .add_exporter(
                "topic_exporter",
                TOPIC_EXPORTER_URN,
                Some(exporter_user_config.clone()),
            )
            .one_of("upstream", ["topic_exporter"])
            .build(PipelineType::Otap, "test_grp", "test_pipeline")
            .expect("pipeline config should build");
        let mut exporter_ctx = create_test_pipeline_context().with_node_context(
            "topic_exporter".into(),
            TOPIC_EXPORTER_URN.into(),
            NodeKind::Exporter,
            HashMap::new(),
        );
        install_test_context_bindings(&mut exporter_ctx, &OTAP_PIPELINE_FACTORY, pipeline_config)
            .expect("context bindings should compile");
        exporter_ctx.set_topic_set(topic_set.clone());

        let exporter_node = test_node("topic_exporter");
        let mut exporter_user_cfg = NodeUserConfig::new_exporter_config(TOPIC_EXPORTER_URN);
        exporter_user_cfg.config = exporter_user_config;
        let mut exporter = (TOPIC_EXPORTER.create)(
            exporter_ctx,
            exporter_node.clone(),
            Arc::new(exporter_user_cfg),
            &ExporterConfig::new("topic_exporter"),
            &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
        )
        .expect("topic exporter should be created");
        let (exporter_input_tx, exporter_input_rx) = create_not_send_channel::<OtapPdata>(16);
        exporter
            .set_pdata_receiver(
                exporter_node.clone(),
                PDataReceiver::Local(LocalReceiver::mpsc(exporter_input_rx)),
            )
            .expect("exporter input channel should be wired");

        let (runtime_ctrl_tx, _runtime_ctrl_rx) = runtime_ctrl_msg_channel::<OtapPdata>(32);
        let (pipeline_completion_tx, _pipeline_completion_rx) =
            pipeline_completion_msg_channel::<OtapPdata>(64);
        let (_metrics_rx, metrics_reporter) = MetricsReporter::create_new_and_receiver(64);
        let runtime_services = test_pipeline_runtime_services();

        let mut receiver_ctrls = Vec::new();
        let mut receiver_outputs = Vec::new();
        let mut receiver_tasks = Vec::new();
        for name in ["receiver_0", "receiver_1"] {
            let mut receiver_ctx = create_test_pipeline_context();
            receiver_ctx.set_topic_set(topic_set.clone());
            let receiver_node = test_node(name);
            let mut receiver_user_cfg = NodeUserConfig::new_receiver_config(TOPIC_RECEIVER_URN);
            receiver_user_cfg.config = json!({
                "topic": "ingress",
                "subscription": {"mode": "balanced", "group": "workers"}
            });
            let mut receiver = (TOPIC_RECEIVER.create)(
                receiver_ctx,
                receiver_node.clone(),
                Arc::new(receiver_user_cfg),
                &ReceiverConfig::new(name),
                &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
            )
            .expect("topic receiver should be created");
            let (output_tx, output_rx) = create_not_send_channel::<OtapPdata>(16);
            receiver
                .set_pdata_sender(
                    receiver_node,
                    "".into(),
                    PDataSender::Local(LocalSender::mpsc(output_tx)),
                )
                .expect("receiver output channel should be wired");
            receiver_ctrls.push(receiver.control_sender());
            receiver_outputs.push(output_rx);
            let ctrl_tx = runtime_ctrl_tx.clone();
            let completion_tx = pipeline_completion_tx.clone();
            let metrics = metrics_reporter.clone();
            let services = runtime_services.clone();
            receiver_tasks.push(tokio::task::spawn_local(async move {
                receiver
                    .start(
                        ctrl_tx,
                        completion_tx,
                        metrics,
                        Interests::empty(),
                        services,
                    )
                    .await
            }));
        }
        // Both receivers subscribed in their factories, so placement sees two
        // consumers from the first publish.

        let exporter_ctrl = exporter.control_sender();
        let exporter_task = tokio::task::spawn_local(async move {
            exporter
                .start(
                    runtime_ctrl_tx,
                    pipeline_completion_tx,
                    metrics_reporter,
                    Interests::empty(),
                    runtime_services,
                )
                .await
        });

        // Item counts encode key and sequence: key `a` uses odd counts and key
        // `b` uses even counts, both increasing with the sequence number.
        for (key, parity) in [("a", 1), ("b", 2)] {
            for seq in 0..PER_KEY {
                let mut headers = TransportHeaders::new();
                headers.push(TransportHeader::new(
                    header.clone(),
                    ValueKind::Text,
                    key.as_bytes(),
                ));
                let pdata =
                    make_logs_pdata_with_items(2 * seq + parity).with_transport_headers(headers);
                exporter_input_tx
                    .send(pdata)
                    .expect("message should be sent to topic exporter");
            }
        }

        let mut received: [Vec<usize>; 2] = [Vec::new(), Vec::new()];
        let [output_0, output_1] = &mut receiver_outputs[..] else {
            unreachable!("two receivers were created");
        };
        while received[0].len() + received[1].len() < 2 * PER_KEY {
            let (index, delivered) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    item = output_0.recv() => (0, item),
                    item = output_1.recv() => (1, item),
                }
            })
            .await
            .expect("timed out waiting for keyed delivery");
            let mut delivered = delivered.expect("receiver output channel should stay open");
            received[index].push(delivered.num_items());
        }

        let odd: Vec<usize> = (0..PER_KEY).map(|seq| 2 * seq + 1).collect();
        let even: Vec<usize> = (0..PER_KEY).map(|seq| 2 * seq + 2).collect();
        let split = (received[0] == odd && received[1] == even)
            || (received[0] == even && received[1] == odd);
        assert!(
            split,
            "each key must stay on one receiver in publish order and the keys \
             must be spread: {received:?}"
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        for ctrl in std::iter::once(exporter_ctrl).chain(receiver_ctrls) {
            ctrl.send(NodeControlMsg::Shutdown {
                deadline,
                reason: "test shutdown".to_owned(),
            })
            .await
            .expect("shutdown should be sent");
        }
        assert!(
            exporter_task
                .await
                .expect("exporter task should join")
                .is_ok(),
            "exporter should stop cleanly"
        );
        for task in receiver_tasks {
            assert!(
                task.await.expect("receiver task should join").is_ok(),
                "receiver should stop cleanly"
            );
        }
    }));
}
