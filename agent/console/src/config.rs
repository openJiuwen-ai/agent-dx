//! Deployment-owned tenant directory and upstream credential references.
use serde::Deserialize;
use std::{collections::BTreeSet, path::Path};

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TenantType { Standard, Digitalmate }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint { pub base_url: String, pub timeout_ms: u64 }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ingress { pub base_url: String, pub execd_port: u16, pub connect_timeout_ms: u64 }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentModule { pub enabled: bool }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admin { pub admin_id: String, pub display_name: String, pub token_sha256: String }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingRule {
    pub kind: String,
    pub agent_type: Option<String>,
    pub template: String,
    #[serde(rename = "ref")]
    pub reference: Reference,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reference { pub kind: String, pub name: String }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountsApi { pub base_url: String, pub timeout_ms: u64, pub token_file: String }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Business { pub accounts_api: AccountsApi, pub bindings: Vec<BindingRule> }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tenant {
    pub tenant_id: String, pub display_name: String,
    #[serde(rename = "type")] pub tenant_type: TenantType,
    pub admin_token_sha256: String, pub agent: AgentModule,
    pub upstream_token_file: String, pub business: Option<Business>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedFiles { pub mappings_file: String, pub directory_entries_limit: usize }
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleConfig {
    pub upstream_ca_file: Option<String>,
    pub listen: String, pub public_origin: String,
    pub agent_api: Endpoint, pub apiserver: Endpoint, pub ingress: Ingress,
    pub shared_files: Option<SharedFiles>,
    pub cookie_secret_file: String,
    #[serde(default = "ttl")] pub session_ttl_seconds: u64,
    #[serde(default = "idle")] pub terminal_idle_timeout_seconds: u64,
    #[serde(default = "lost")] pub terminal_lost_timeout_seconds: u64,
    pub terminal_profiles_file: Option<String>,
    #[serde(default = "preview")] pub file_preview_bytes_limit: usize,
    #[serde(default)] pub audit_target: String,
    pub platform_admins: Vec<Admin>, pub tenants: Vec<Tenant>,
}
fn ttl() -> u64 { 28800 }
fn idle() -> u64 { 600 }
fn lost() -> u64 { 30 }
fn preview() -> usize { 262144 }
fn digest(value: &str) -> bool { value.len() == 64 && hex::decode(value).is_ok() }
impl ConsoleConfig {
    pub fn from_path(path: &str) -> Result<Self, String> {
        let raw = std::fs::read(path).map_err(|_| "console configuration unreadable")?;
        let config: Self = serde_json::from_slice(&raw).map_err(|_| "invalid console configuration")?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), String> {
        self.listen.parse::<std::net::SocketAddr>().map_err(|_| "invalid listen address")?;
        for endpoint in [&self.public_origin, &self.agent_api.base_url, &self.apiserver.base_url, &self.ingress.base_url] {
            let url = url::Url::parse(endpoint).map_err(|_| "invalid upstream URL")?;
            if url.scheme() != "https" || url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() || url.query().is_some() || url.fragment().is_some() {
                return Err("invalid upstream URL".into());
            }
        }
        let origin = url::Url::parse(&self.public_origin).map_err(|_| "invalid public origin")?;
        if origin.path() != "/" { return Err("public origin must not include a path".into()); }
        if self.terminal_idle_timeout_seconds == 0 || self.terminal_lost_timeout_seconds == 0 || self.file_preview_bytes_limit == 0 {
            return Err("positive terminal and preview limits required".into());
        }
        if self.session_ttl_seconds == 0 || self.ingress.execd_port == 0 || self.agent_api.timeout_ms == 0 || self.apiserver.timeout_ms == 0 {
            return Err("positive timeout and port required".into());
        }
        let mut ids = BTreeSet::new();
        for admin in &self.platform_admins {
            if !ids.insert(&admin.admin_id) || !digest(&admin.token_sha256) { return Err("invalid admin directory".into()); }
        }
        let mut ids = BTreeSet::new();
        for tenant in &self.tenants {
            adx_agent_core::identifier(&tenant.tenant_id, "tenant")?;
            if !ids.insert(&tenant.tenant_id) || !digest(&tenant.admin_token_sha256) || !tenant.agent.enabled || !Path::new(&tenant.upstream_token_file).is_absolute() {
                return Err("invalid tenant directory".into());
            }
            match (tenant.tenant_type, &tenant.business) {
                (TenantType::Standard, None) => {},
                (TenantType::Digitalmate, Some(business)) => {
                    let endpoint = &business.accounts_api;
                    let url = url::Url::parse(&endpoint.base_url).map_err(|_|"invalid Accounts API URL")?;
                    if url.scheme() != "https" || url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() || url.query().is_some() || url.fragment().is_some() || endpoint.timeout_ms == 0 || !Path::new(&endpoint.token_file).is_absolute() {
                        return Err("invalid Accounts API configuration".into());
                    }
                    let mut kinds = BTreeSet::new();
                    for binding in &business.bindings {
                        if !kinds.insert(&binding.kind) || !matches!(binding.kind.as_str(), "jiuwen" | "orbit" | "digitalmate")
                            || binding.kind == "jiuwen" && binding.agent_type.as_ref().is_none_or(|v| v.is_empty()) {
                            return Err("invalid business identity mapping".into());
                        }
                        adx_agent_core::identifier(&binding.template, "template")?;
                        match binding.reference.kind.as_str() {
                            "version" => { adx_agent_core::version_number(&binding.reference.name)?; },
                            "alias" => adx_agent_core::alias_name(&binding.reference.name)?,
                            _ => return Err("invalid reference kind".into()),
                        }
                    }
                },
                _ => return Err("tenant type and business configuration disagree".into()),
            }
        }
        Ok(())
    }
}
