//! E2B management API boundary types and adapters.

mod config;
mod dto;
pub mod http;
pub(crate) mod keys;
mod network;

pub use config::{Config, Template, TemplateCatalog};
pub use dto::{
    ConnectRequest, CreateRequest, ErrorBody, PauseRequest, SnapshotRequest, TimeoutRequest,
    ValidationError,
};
