//! Public management API, directly using typed Capsule RPCs.
#![allow(clippy::result_large_err)]
pub mod clients;
pub mod config;
pub mod contract;
pub mod edge;
pub mod errors;
pub mod http;
pub mod operations;
pub mod ownership;

mod capsule_directory;
mod directory;
