//! Agent entrypoints composed by Gateway.
pub mod activator;
pub mod managed;
pub mod request;
pub use adx_agent_core::error::{Error, Result};

pub mod discovery;
pub mod local;
