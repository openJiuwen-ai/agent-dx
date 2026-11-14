//! Public management API, directly using typed Environment RPCs.
#![allow(clippy::result_large_err)]
// Reuse the socket HTTP fixture from integration tests in internal client tests.
#[cfg(test)]
extern crate self as adx_apiserver;

pub mod activator;
pub mod clients;
pub mod config;
pub mod contract;
pub mod e2b;
pub mod environment_service;
pub mod errors;
pub mod http;
pub mod ingress;
pub mod metrics;
pub mod operations;
pub mod ownership;
pub mod workspace_affinity;

mod directory;
mod environment_directory;
mod golden_preparation;

mod volumes;
