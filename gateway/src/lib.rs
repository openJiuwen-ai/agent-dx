//! Shared protocol and relay primitives for the Rust data plane gateway.
//! The node binary is intentionally protocol agnostic after CONNECT admission:
//! HTTP, SSH, TLS and database sessions are all opaque TCP bytes.

pub mod client;
pub mod common;
pub mod config;
pub mod edge;
pub mod node;
