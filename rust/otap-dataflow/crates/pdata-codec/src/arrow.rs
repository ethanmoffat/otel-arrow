// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Codec contracts for named, versioned Arrow record-set representations.

use std::hash::{Hash, Hasher};

use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::{OtapArrowRecords, PdataArrowRecordSet, PdataRepresentation};

use crate::{CodecCapabilities, CodecError, CodecOperation};

/// Converts one complete pluggable Arrow record set to native OTAP.
///
/// Decoder instances belong to one pipeline runtime and may retain bounded
/// scratch state. The input remains owned by the caller so conversion failure
/// can preserve Ack/Nack and retry semantics. Implementations are `Send` so the
/// existing shared codec-service handle remains usable by both local and shared
/// engine variants; codec calls stay synchronous and do not cross an await.
pub trait PdataArrowDecoder: Send {
    /// Decodes one self-contained record set without retaining it.
    fn decode(&mut self, records: &PdataArrowRecordSet) -> Result<OtapArrowRecords, CodecError>;
}

/// Converts native OTAP to one complete pluggable Arrow record set.
///
/// Encoder instances belong to one pipeline runtime and may retain bounded
/// scratch state. Every successful result must use the representation identity
/// registered for the encoder. Implementations are `Send` for compatibility
/// with the existing shared codec-service handle; calls remain synchronous.
pub trait PdataArrowEncoder: Send {
    /// Encodes one native OTAP batch.
    fn encode(&mut self, records: OtapArrowRecords) -> Result<PdataArrowRecordSet, CodecError>;
}

/// Creates independent Arrow decoder state for one pipeline runtime.
pub type ArrowDecoderFactory = fn() -> Box<dyn PdataArrowDecoder>;

/// Creates independent Arrow encoder state for one pipeline runtime.
pub type ArrowEncoderFactory = fn() -> Box<dyn PdataArrowEncoder>;

/// Immutable metadata for one pluggable Arrow representation.
#[derive(Debug)]
pub struct ArrowCodecMetadata {
    representation: PdataRepresentation,
    signals: &'static [SignalType],
}

impl ArrowCodecMetadata {
    /// Declares a representation identity, version, and supported signals.
    #[must_use]
    pub const fn new(representation: PdataRepresentation, signals: &'static [SignalType]) -> Self {
        Self {
            representation,
            signals,
        }
    }

    /// Stable representation identity and version.
    #[must_use]
    pub const fn representation(&self) -> &PdataRepresentation {
        &self.representation
    }

    /// Signals understood by this codec.
    #[must_use]
    pub const fn signals(&self) -> &'static [SignalType] {
        self.signals
    }
}

/// Link-time registration for a pluggable Arrow representation.
#[derive(Debug)]
pub struct ArrowCodecRegistration {
    pub(crate) metadata: &'static ArrowCodecMetadata,
    pub(crate) decoder: Option<ArrowDecoderFactory>,
    pub(crate) encoder: Option<ArrowEncoderFactory>,
}

impl ArrowCodecRegistration {
    /// Registers an identity with pass-through capability and no OTAP conversion.
    ///
    /// Conversion in either direction is optional. A representation that is
    /// meaningful only to dedicated processors or exporters may remain
    /// pass-through-only.
    #[must_use]
    pub const fn new(metadata: &'static ArrowCodecMetadata) -> Self {
        Self {
            metadata,
            decoder: None,
            encoder: None,
        }
    }

    /// Adds conversion from the Arrow representation to native OTAP.
    #[must_use]
    pub const fn with_decoder(mut self, decoder: ArrowDecoderFactory) -> Self {
        self.decoder = Some(decoder);
        self
    }

    /// Adds conversion from native OTAP to the Arrow representation.
    #[must_use]
    pub const fn with_encoder(mut self, encoder: ArrowEncoderFactory) -> Self {
        self.encoder = Some(encoder);
        self
    }
}

/// Trusted Arrow codec extensions compiled into this binary.
#[allow(unsafe_code)]
#[linkme::distributed_slice]
pub static PDATA_ARROW_CODEC_FACTORIES: [ArrowCodecRegistration];

/// One immutable Arrow registration from a validated registry.
#[derive(Clone, Copy, Debug)]
pub struct ResolvedArrowCodec(pub(crate) &'static ArrowCodecRegistration);

impl PartialEq for ResolvedArrowCodec {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.0, other.0)
    }
}

impl Eq for ResolvedArrowCodec {}

impl Hash for ResolvedArrowCodec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::from_ref(self.0).hash(state);
    }
}

impl ResolvedArrowCodec {
    /// Immutable metadata and canonical representation identity.
    #[must_use]
    pub const fn metadata(self) -> &'static ArrowCodecMetadata {
        self.0.metadata
    }

    /// Canonical representation identity and version.
    #[must_use]
    pub const fn representation(self) -> &'static PdataRepresentation {
        self.0.metadata.representation()
    }

    /// Registered conversion capabilities.
    #[must_use]
    pub const fn capabilities(self) -> CodecCapabilities {
        CodecCapabilities::new(self.0.decoder.is_some(), self.0.encoder.is_some())
    }

    /// Whether conversion to native OTAP is registered.
    #[must_use]
    pub const fn can_decode(self) -> bool {
        self.0.decoder.is_some()
    }

    /// Whether conversion from native OTAP is registered.
    #[must_use]
    pub const fn can_encode(self) -> bool {
        self.0.encoder.is_some()
    }

    /// Validates pass-through support for a signal.
    pub fn require_passthrough(self, signal: SignalType) -> Result<(), CodecError> {
        self.require(signal, CodecOperation::PassThrough, true)
    }

    /// Validates decoder support for a signal.
    pub fn require_decoder(self, signal: SignalType) -> Result<(), CodecError> {
        self.require(signal, CodecOperation::Decode, self.can_decode())
    }

    /// Validates encoder support for a signal.
    pub fn require_encoder(self, signal: SignalType) -> Result<(), CodecError> {
        self.require(signal, CodecOperation::Encode, self.can_encode())
    }

    fn require(
        self,
        signal: SignalType,
        operation: CodecOperation,
        factory_present: bool,
    ) -> Result<(), CodecError> {
        if factory_present && self.metadata().signals().contains(&signal) {
            Ok(())
        } else {
            Err(CodecError::UnsupportedArrow {
                representation: self.representation().clone(),
                operation,
                signal,
            })
        }
    }

    pub(crate) fn create_decoder(self) -> Result<Box<dyn PdataArrowDecoder>, CodecError> {
        self.0.decoder.map(|factory| factory()).ok_or_else(|| {
            CodecError::UnsupportedArrowCodecOperation {
                representation: self.representation().clone(),
                operation: CodecOperation::Decode,
            }
        })
    }

    pub(crate) fn create_encoder(self) -> Result<Box<dyn PdataArrowEncoder>, CodecError> {
        self.0.encoder.map(|factory| factory()).ok_or_else(|| {
            CodecError::UnsupportedArrowCodecOperation {
                representation: self.representation().clone(),
                operation: CodecOperation::Encode,
            }
        })
    }
}
