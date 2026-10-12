// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pluggable byte and Arrow representation codecs for native OTAP.

mod arrow;
mod batching;
mod configuration;
mod decoder;
mod encoder;
mod error;
mod identity;
mod payload;
mod plan;
mod registry;
mod runtime;
mod view;

mod codecs;

pub use arrow::{
    ArrowCodecMetadata, ArrowCodecRegistration, PDATA_ARROW_CODEC_FACTORIES, PdataArrowDecoder,
    PdataArrowEncoder, ResolvedArrowCodec,
};
pub use batching::{
    BatchPlan, BatchSettings, BatchSizer, BatchingOutput, BatchingSupport, CodecBatches,
    PdataBatcher,
};
pub use configuration::{CodecPreparer, PreparedCodecFactory};
pub use decoder::{
    DecodeBudget, DecodePolicy, DecodeTask, DecodeValidation, PdataDecodeJob, PdataDecoder,
};
pub use encoder::{EncodeOutput, PdataEncoder};
pub use error::{CodecError, CodecOperation, RegistryError};
pub use identity::{EncodedPdata, EncodedPdataBatch, PdataEncoding};
pub use payload::{OtapPayload, PdataFormat, PdataPayload, PdataPayloadDecodeError};
pub use plan::{AcceptedEncodings, EncodePolicy, EncodingPlan};
pub use registry::{
    BatcherFactory, CodecBatcherRegistration, CodecCapabilities, CodecMetadata, CodecRegistration,
    CodecRegistry, ItemCounter, PDATA_CODEC_FACTORIES, ResolvedCodec,
};
pub use runtime::{CodecService, CodecServiceBuilder};
pub use view::{EncodedView, PdataView};

/// Registers a pdata codec in the process-wide link-time registry.
///
/// The registration expression must be const evaluable. The macro hides the
/// distributed-slice implementation so extension crates do not need to depend
/// on or use `linkme` directly. Its function-like form deliberately keeps the
/// registration as a typed [`CodecRegistration`] builder expression instead of
/// duplicating that API in a macro-specific syntax. The explicit identifier
/// names the generated static in compiler and linker diagnostics.
///
/// Registry validation, including duplicate-name handling, remains outside the
/// macro so registration order never becomes an implicit precedence policy.
#[macro_export]
macro_rules! register_pdata_codec {
    ($name:ident, $registration:expr $(,)?) => {
        #[allow(unsafe_code)]
        #[$crate::__private::distributed_slice($crate::PDATA_CODEC_FACTORIES)]
        #[linkme(crate = $crate::__private)]
        static $name: $crate::CodecRegistration = $registration;
    };
}

/// Registers a named, versioned Arrow representation codec at link time.
///
/// The registration expression must be const evaluable. Registry validation
/// rejects duplicate identities independently of linker order. Encode and
/// decode factories are optional; registration alone enables pass-through.
#[macro_export]
macro_rules! register_pdata_arrow_codec {
    ($name:ident, $registration:expr $(,)?) => {
        #[allow(unsafe_code)]
        #[$crate::__private::distributed_slice($crate::PDATA_ARROW_CODEC_FACTORIES)]
        #[linkme(crate = $crate::__private)]
        static $name: $crate::ArrowCodecRegistration = $registration;
    };
}

/// Implementation details used by exported macros.
#[doc(hidden)]
pub mod __private {
    pub use linkme::*;
}

/// Built-in codec identities and implementations.
pub mod builtins {
    pub use crate::codecs::otlp::{
        OTLP_ENCODING, OtlpBestEffortDecoder, OtlpEncoder, resolve_otlp,
    };
    #[cfg(feature = "syslog-cef")]
    pub use crate::codecs::syslog;
}

/// Reusable codec extension conformance checks.
#[cfg(any(test, feature = "testing"))]
pub mod testing;
