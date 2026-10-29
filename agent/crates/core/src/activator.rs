//! Authenticated Gateway/Activator contracts. No platform runtime state is persisted here.
use crate::{AgentBinding, Scope, Service};
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
    /// Force a Environment observation even if this Activator already activated this generation.
    #[serde(default, rename = "bypasscache")]
    pub bypass_cache: bool,
    /// Raw form-encoded initialization. Parsed only when this Binding is absent.
    #[serde(default)]
    pub parameters: Option<Vec<u8>>,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSelector {
    pub protocol: crate::Protocol,
    pub port: Option<u16>,
}
impl ServiceSelector {
    /// Returns an error unless the selector identifies exactly one declared service.
    pub fn select(&self, services: &[Service]) -> Result<u16, String> {
        let mut ports = services
            .iter()
            .filter(|service| {
                service.protocol == self.protocol
                    && self.port.is_none_or(|port| port == service.port)
            })
            .map(|service| service.port);
        match (ports.next(), ports.next()) {
            (Some(port), None) => Ok(port),
            _ => Err("service must identify exactly one declared protocol/port".into()),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveRequest {
    pub activation: ActivationRequest,
    pub service: ServiceSelector,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedTarget {
    pub target: Target,
    pub port: u16,
}
/// One read-only Binding snapshot and the immutable template for its actual version.
/// Before first activation, binding is None and template is selected from the desired state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingView {
    pub binding: Option<AgentBinding>,
    pub template: crate::TemplateVersion,
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
    pub template: crate::TemplateDraft,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AliasRequest {
    pub tenant: String,
    pub template: String,
    pub alias: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AliasUpdate {
    pub config: crate::alias::AliasConfig,
    pub expected_revision: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AliasSnapshot {
    pub config: crate::alias::AliasConfig,
    pub revision: String,
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
    pub template_ref: crate::TemplateRef,
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
            ("tenant", self.tenant.as_str()),
            ("template", self.template.as_str()),
            ("template reference", self.template_ref.name()),
        ] {
            crate::identifier(value, label)?;
        }
        self.template_ref.validate()?;
        if !(1..=crate::limits::BINDING_PAGE_SIZE).contains(&self.page_size)
            || self.page_token.as_ref().is_some_and(|v| v.len() > 16384)
        {
            return Err("invalid AgentBinding page size or token".into());
        }
        Ok(())
    }
}
