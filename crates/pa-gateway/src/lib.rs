//! Embed shared Prime Agent sessions in an application's own identity,
//! storage, and execution infrastructure. See the crate README for integration.

mod daemon;
mod error;
mod gateway;
mod memory;
mod ports;

#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "debug")]
pub mod debug;

pub use daemon::{DaemonEndpoint, DaemonRuntime};
pub use error::{Error, Result};
pub use gateway::Gateway;
pub use memory::MemoryStore;
pub use ports::{EventStream, Runtime, SessionStore, WorkspacePolicy};
