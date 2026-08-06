//! Exchange-neutral orchestration for the rolling maker grid.
//!
//! The engine is deliberately a single-owner state machine. Exchange streams
//! and timers feed one task, and only that task mutates the desired grid and
//! live-order registry.

#![forbid(unsafe_code)]

mod config;
mod engine;
mod error;
mod registry;

pub use config::{EngineConfig, EngineConfigError};
pub use engine::{EnginePhase, MakerEngine};
pub use error::EngineError;
pub use registry::{OrderRegistry, RegisteredOrder, RegistryError, RegistryState};
