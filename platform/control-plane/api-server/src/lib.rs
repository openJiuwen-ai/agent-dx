//! Public management API, directly using typed Instance RPCs.
#![allow(clippy::result_large_err)]
pub mod clients;
pub mod config;
pub mod contract;
pub mod http;
pub mod operations;
pub mod ownership;

mod directory;
mod instance_directory;
