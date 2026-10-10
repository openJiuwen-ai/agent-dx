//! Authenticated Gateway/Activator contracts. No platform runtime state is persisted here.
use crate::{AgentBinding, Scope, Service, TemplateVersion};
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
    /// Internal retry stays within the AgentBinding selected by this incoming request.
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
    pub binding: AgentBinding,
    pub service: Vec<Service>,
}

/// A bounded page of AgentBinding metadata within one tenant/template version.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingList {
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
pub struct BindingPage {
    pub bindings: Vec<AgentBinding>,
    pub next_page_token: Option<String>,
}

impl BindingList {
    pub fn validate(&self) -> crate::ValidationResult {
        for (label, value) in [
            ("tenant", &self.tenant),
            ("template", &self.template),
            ("version", &self.version),
        ] {
            crate::identifier(value, label)?;
        }
        if !(1..=crate::limits::BINDING_PAGE_SIZE).contains(&self.page_size)
            || self.page_token.as_ref().is_some_and(|v| v.len() > 16384)
        {
            return Err("invalid AgentBinding page size or token".into());
        }
        Ok(())
    }
}

/// Internal-only preparation; user credentials never appear in returned bindings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareBindingRequest {
    pub scope: Scope,
    pub launch: crate::launch::LaunchConfig,
}
