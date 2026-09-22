//! Authenticated Gateway/Activator contracts. No platform runtime state is persisted here.
use crate::{Environment, Scope, Service, TemplateVersion};
use serde::{Deserialize, Serialize};

pub const DEADLINE_HEADER: &str = "x-adx-deadline-ms";
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeRequest {
    pub scope: Scope,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateRequest {
    pub tenant: String,
    pub name: String,
    pub version: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishRequest {
    pub tenant: String,
    pub template: TemplateVersion,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub environment: Environment,
    pub service: Vec<Service>,
}
