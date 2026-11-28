//! Authenticated Gateway/Activator contracts. No platform runtime state is persisted here.
use crate::{AgentBinding, Scope, Service};
use serde::{Deserialize, Serialize};

pub const DEADLINE_HEADER: &str = "x-adx-deadline-ms";

/// Read-only catalog filters: exact versions, prefix template and alias names.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListPage {
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    #[serde(default)]
    pub page_token: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}
impl Default for ListPage {
    fn default() -> Self {
        Self { page_size: default_page_size(), page_token: None, name: None }
    }
}
impl ListPage {
    pub fn validate(&self) -> crate::ValidationResult {
        if !(1..=crate::limits::BINDING_PAGE_SIZE).contains(&self.page_size)
            || self.page_token.as_ref().is_some_and(|v| v.len() > 16384)
        { return Err("invalid catalog page size or token".into()); }
        if let Some(name) = &self.name { crate::identifier(name, "catalog filter")?; }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogRequest {
    pub tenant: String,
    #[serde(default)]
    pub template: Option<String>,
    pub page: ListPage,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateSummary {
    pub name: String,
    pub latest_version: String,
    pub published_at: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateSummaryPage {
    pub templates: Vec<TemplateSummary>,
    pub total: usize,
    pub next_page_token: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionSummary {
    pub version: String,
    pub published_at: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionSummaryPage {
    pub versions: Vec<VersionSummary>,
    pub total: usize,
    pub next_page_token: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasPage {
    pub aliases: Vec<AliasSnapshot>,
    pub total: usize,
    pub next_page_token: Option<String>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingCatalog {
    pub tenant: String,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub template_ref: Option<crate::TemplateRef>,
    #[serde(default)]
    pub ref_kind: Option<String>,
    #[serde(default)]
    pub binding_id: Option<String>,
    /// Match actual current or secondary version, independently of the stored reference.
    #[serde(default)]
    pub actual_version: Option<String>,
    #[serde(default)]
    pub page: ListPage,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindingCatalogPage {
    pub bindings: Vec<AgentBinding>,
    pub total: usize,
    pub next_page_token: Option<String>,
}
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
    /// From a trusted business gateway, never from the public Binding resolve body.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub reuse_subject_binding: bool,
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
/// 启动模式判定（Activator 在 resolve 响应回传；§7 M2-4 接口扩展）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationKind {
    /// 命中绑定缓存（未做任何 I/O）。
    BindingReuse,
    /// 新建绑定 → 环境冷启动。
    Cold,
    /// 既有绑定，环境从暂停恢复。
    Resumed,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedTarget {
    pub target: Target,
    pub port: u16,
    /// 启动模式；缺省（旧响应）按 `unknown` 处理。
    #[serde(default)]
    pub activation: ActivationKind,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_id: Option<String>,
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
