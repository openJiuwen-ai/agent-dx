//! E2B management API boundary types and adapters.

mod config;
mod dto;
pub mod http;
pub(crate) mod keys;
mod network;
mod templates;

pub use config::{Config, TemplateRef};
pub use dto::{
    ConnectRequest, CreateRequest, ErrorBody, PauseRequest, SnapshotRequest, TimeoutRequest,
    ValidationError,
};
