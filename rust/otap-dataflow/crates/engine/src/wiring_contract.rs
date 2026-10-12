// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Node wiring contracts used to validate connection-level topology constraints.

use crate::error::Error;
use crate::node::NodeName;
use otel_arrow_dfe_config::PortName;
use std::fmt;

bitflags::bitflags! {
    /// Coarse representation families used for static pipeline compatibility.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct PdataRepresentationSet: u8 {
        /// Existing native OTAP Arrow records and OTLP protobuf bytes.
        const STANDARD = 0b0000_0001;
        /// Named, versioned pluggable Arrow record sets.
        const PLUGGABLE_ARROW = 0b0000_0010;
        /// Every representation family currently known by the engine.
        const ALL = Self::STANDARD.bits() | Self::PLUGGABLE_ARROW.bits();
    }
}

impl fmt::Display for PdataRepresentationSet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        if self.contains(Self::STANDARD) {
            formatter.write_str("standard OTAP/OTLP")?;
            first = false;
        }
        if self.contains(Self::PLUGGABLE_ARROW) {
            if !first {
                formatter.write_str(", ")?;
            }
            formatter.write_str("pluggable Arrow")?;
            first = false;
        }
        if first {
            formatter.write_str("none")?;
        }
        Ok(())
    }
}

/// How a node determines the representations emitted on its outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PdataOutputContract {
    /// The node always produces the declared representation families.
    Produces(PdataRepresentationSet),
    /// The node preserves every admitted representation unchanged.
    PreservesInput,
}

/// Static pdata compatibility contract for one node type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PdataWiringContract {
    accepted_inputs: PdataRepresentationSet,
    output: PdataOutputContract,
}

impl PdataWiringContract {
    /// Existing OTAP-specific behavior: accept and produce only standard pdata.
    pub const STANDARD: Self = Self {
        accepted_inputs: PdataRepresentationSet::STANDARD,
        output: PdataOutputContract::Produces(PdataRepresentationSet::STANDARD),
    };

    /// Representation-agnostic behavior: accept and preserve every family.
    pub const PASSTHROUGH: Self = Self {
        accepted_inputs: PdataRepresentationSet::ALL,
        output: PdataOutputContract::PreservesInput,
    };

    /// Source behavior for a future receiver producing pluggable Arrow records.
    pub const PLUGGABLE_ARROW_SOURCE: Self = Self {
        accepted_inputs: PdataRepresentationSet::empty(),
        output: PdataOutputContract::Produces(PdataRepresentationSet::PLUGGABLE_ARROW),
    };

    /// Sink behavior for a component that accepts pluggable Arrow records only.
    pub const PLUGGABLE_ARROW_SINK: Self = Self {
        accepted_inputs: PdataRepresentationSet::PLUGGABLE_ARROW,
        output: PdataOutputContract::Produces(PdataRepresentationSet::empty()),
    };

    /// Sink behavior for a component that discards or handles every representation.
    pub const ACCEPT_ALL: Self = Self {
        accepted_inputs: PdataRepresentationSet::ALL,
        output: PdataOutputContract::Produces(PdataRepresentationSet::empty()),
    };

    /// Creates an explicit pdata contract.
    #[must_use]
    pub const fn new(accepted_inputs: PdataRepresentationSet, output: PdataOutputContract) -> Self {
        Self {
            accepted_inputs,
            output,
        }
    }

    /// Representation families accepted on node inputs.
    #[must_use]
    pub const fn accepted_inputs(self) -> PdataRepresentationSet {
        self.accepted_inputs
    }

    /// Computes possible output representations for the current inputs.
    #[must_use]
    pub const fn outputs_for(self, inputs: PdataRepresentationSet) -> PdataRepresentationSet {
        match self.output {
            PdataOutputContract::Produces(outputs) => outputs,
            PdataOutputContract::PreservesInput => inputs,
        }
    }
}

/// Per-output fanout rule for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFanoutRule {
    /// No destination-count limit per output.
    #[default]
    Unrestricted,
    /// The number of destinations per output must be <= this limit.
    AtMostPerOutput(usize),
}

/// Contract describing wiring constraints for a node type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WiringContract {
    /// Constraint on per-output destination fanout.
    pub output_fanout: OutputFanoutRule,
    /// Static compatibility with pdata representation families.
    pub pdata: PdataWiringContract,
}

impl WiringContract {
    /// Unrestricted wiring contract (no per-output destination limit).
    pub const UNRESTRICTED: Self = Self {
        output_fanout: OutputFanoutRule::Unrestricted,
        pdata: PdataWiringContract::STANDARD,
    };

    /// Creates an unrestricted wiring contract.
    #[must_use]
    pub const fn unrestricted() -> Self {
        Self::UNRESTRICTED
    }

    /// Creates a contract with a per-output destination cap.
    #[must_use]
    pub const fn at_most_per_output(max: usize) -> Self {
        Self {
            output_fanout: OutputFanoutRule::AtMostPerOutput(max),
            pdata: PdataWiringContract::STANDARD,
        }
    }

    /// Replaces the pdata compatibility portion of this wiring contract.
    #[must_use]
    pub const fn with_pdata(mut self, pdata: PdataWiringContract) -> Self {
        self.pdata = pdata;
        self
    }

    /// Validates a source output against this contract.
    pub fn validate_output_destinations(
        &self,
        node: &NodeName,
        output: &PortName,
        destinations: &[NodeName],
    ) -> Result<(), Error> {
        match self.output_fanout {
            OutputFanoutRule::Unrestricted => Ok(()),
            OutputFanoutRule::AtMostPerOutput(max) if destinations.len() <= max => Ok(()),
            OutputFanoutRule::AtMostPerOutput(max) => Err(Error::InvalidNodeWiring {
                node: node.clone(),
                output: output.clone(),
                max_destinations: max,
                actual_destinations: destinations.to_vec(),
            }),
        }
    }
}

impl Default for WiringContract {
    fn default() -> Self {
        Self::UNRESTRICTED
    }
}
