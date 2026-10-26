//! Public management API, directly using typed Environment RPCs.
#![allow(clippy::result_large_err)]
pub mod activator;
pub mod clients;
pub mod config;
pub mod contract;
pub mod environment_service;
pub mod errors;
pub mod http;
pub mod ingress;
pub mod metrics;
pub mod operations;
pub mod ownership;

mod directory;
mod environment_directory;

mod volumes;
