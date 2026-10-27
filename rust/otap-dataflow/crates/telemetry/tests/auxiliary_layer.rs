// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Embedding instrumentation installed through the process-global auxiliary layer factory.
//!
//! The factory and early global subscriber are process-wide, so this test lives in its
//! own integration test binary to keep them away from other telemetry tests.

use otel_arrow_dfe_config::settings::telemetry::logs::LogLevel;
use otel_arrow_dfe_telemetry::self_tracing::LogContext;
use otel_arrow_dfe_telemetry::tracing_init::{
    ProviderSetup, TracingSetup, init_global_auxiliary_subscriber, install_auxiliary_layer_factory,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

const TARGET: &str = "auxiliary_layer_test";

static EVENTS: AtomicUsize = AtomicUsize::new(0);

struct Auxiliary;

impl<S: Subscriber> Layer<S> for Auxiliary {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() == TARGET {
            _ = EVENTS.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn off() -> LogLevel {
    LogLevel::try_from("off".to_owned()).expect("valid filter")
}

/// Scenario: Embedded instrumentation starts before DFE logging, then worker dispatches disable logs.
/// Guarantees: Configuring global logs and Noop/off worker providers never suppresses instrumentation.
#[test]
fn auxiliary_instrumentation_survives_noop_and_filtered_thread_dispatches() {
    assert!(install_auxiliary_layer_factory(|| Box::new(Auxiliary)).is_ok());
    init_global_auxiliary_subscriber().expect("early global instrumentation");
    TracingSetup::new(ProviderSetup::ConsoleDirect, off(), LogContext::new)
        .try_init_global()
        .expect("configure existing global logging");
    tracing::info!(target: TARGET, "early global");
    for provider in [ProviderSetup::Noop, ProviderSetup::ConsoleDirect] {
        let setup = TracingSetup::new(provider, off(), LogContext::new);
        setup.with_subscriber(|| {
            tracing::info!(target: TARGET, "local");
        });
        std::thread::spawn(move || {
            setup.with_subscriber(|| {
                tracing::info!(target: TARGET, "worker");
            });
        })
        .join()
        .expect("worker thread");
    }
    assert_eq!(EVENTS.load(Ordering::SeqCst), 5);
}
