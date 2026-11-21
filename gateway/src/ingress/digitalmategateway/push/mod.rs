//! Huawei Push Kit egress. The gateway holds the project service account, signs
//! PS256 JWTs and delivers notifications to a fixed Huawei endpoint.
//! The private key and JWT never enter a sandbox; device bindings, notification
//! content and the delivery queue stay with the sandbox notification owner.
//! The caller is authenticated with the shared Gateway workload identity.
mod client;
pub mod http;
mod jwt;
mod startup;

pub(crate) use startup::PushConfig;

use super::composio::{Error, Scope};
use client::Client;

pub(crate) struct Push {
    client: Client,
    category: String,
}
