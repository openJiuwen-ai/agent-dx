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
pub struct ActivationRequest {
    pub scope: Scope,
    /// Internal retry stays within the Environment selected by this incoming request.
    #[serde(default)]
    pub expected_generation: Option<String>,
    /// Force a Sandbox observation even if this Activator already activated this generation.
    #[serde(default, rename = "bypasscache")]
    pub bypass_cache: bool,
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

/// A bounded page of Environment metadata within one tenant/template version.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentList {
    pub tenant: String,
    pub template: String,
    pub version: String,
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    #[serde(default)]
    pub page_token: Option<String>,
}

pub const fn default_page_size() -> usize {
    50
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentPage {
    pub environments: Vec<Environment>,
    pub next_page_token: Option<String>,
}

impl EnvironmentList {
    pub fn validate(&self) -> crate::ValidationResult {
        for (label, value) in [
            ("tenant", &self.tenant),
            ("template", &self.template),
            ("version", &self.version),
        ] {
            crate::identifier(value, label)?;
        }
        if !(1..=crate::limits::ENVIRONMENT_PAGE_SIZE).contains(&self.page_size)
            || self.page_token.as_ref().is_some_and(|v| v.len() > 16384)
        {
            return Err("invalid Environment page size or token".into());
        }
        Ok(())
    }
}
