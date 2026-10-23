//! Agent product state. No Platform, network or persistence dependencies.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub mod activator;
pub mod cache;
pub mod environment;
pub mod error;
pub mod lifecycle;
pub mod limits;
pub mod mounts;
pub mod network;
pub mod parameters;
pub mod probes;
pub mod target;
pub mod transport;

pub type ValidationResult = Result<(), String>;

/// Wall-clock deadline shared across Activator processes. Deployment clocks must be synchronized.
pub fn unix_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock precedes Unix epoch")
        .as_millis() as u64
}

pub fn identifier(value: &str, label: &str) -> ValidationResult {
    if value.trim().is_empty()
        || value.len() > limits::IDENTIFIER_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(format!(
            "{label} must be nonempty, at most 512 bytes and contain no control characters"
        ));
    }
    Ok(())
}

/// JSON tuple encoding avoids ambiguous concatenation, including user delimiters.
pub fn encode_key(parts: &[&str]) -> String {
    let bytes = serde_json::to_vec(parts).expect("string array serialization");
    let mut encoded = String::with_capacity(bytes.len() * 2);
    use std::fmt::Write;
    for byte in bytes {
        write!(encoded, "{byte:02x}").expect("write to String");
    }
    encoded
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub tenant: String,
    pub template: String,
    pub version: String,
    pub binding_id: String,
}

impl Scope {
    pub fn validate(&self) -> ValidationResult {
        for (label, value) in [
            ("tenant", &self.tenant),
            ("template", &self.template),
            ("version", &self.version),
            ("binding_id", &self.binding_id),
        ] {
            identifier(value, label)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Http,
    Ws,
    Ssh,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Service {
    pub protocol: Protocol,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub cpu_millis: u64,
    pub memory_mib: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_limit_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_mib: Option<u64>,
}

impl Resources {
    pub fn validate(&self) -> ValidationResult {
        if self.cpu_millis == 0 || self.memory_mib == 0 {
            return Err("cpu_millis and memory_mib must be positive".into());
        }
        let cpu_limit = self.cpu_limit_millis.unwrap_or(self.cpu_millis);
        let memory_limit = self.memory_limit_mib.unwrap_or(self.memory_mib);
        if cpu_limit < self.cpu_millis || memory_limit < self.memory_mib {
            return Err("resource limits must not be below requests".into());
        }
        memory_limit
            .checked_mul(1_048_576)
            .ok_or("memory limit exceeds representable byte count")?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemplateVersion {
    pub name: String,
    pub version: String,
    pub image: String,
    pub isolation_runtime: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub resources: Resources,
    #[serde(
        default,
        skip_serializing_if = "lifecycle::LifecyclePolicy::is_default"
    )]
    pub lifecycle: lifecycle::LifecyclePolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<mounts::Mount>,
    #[serde(default)]
    pub service: Vec<Service>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init_timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_probe: Option<probes::ReadinessProbe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<probes::Healthcheck>,
}

impl TemplateVersion {
    /// Render Local mount paths from immutable Binding parameters, or reject missing/unsafe values.
    pub fn render_mounts(
        &self,
        parameters: &mounts::Parameters,
    ) -> Result<Vec<mounts::Mount>, String> {
        mounts::validate_parameters(parameters)?;
        mounts::validate_mounts(&self.mounts)?;
        self.mounts
            .iter()
            .map(|mount| mount.render_validated(parameters))
            .collect()
    }
    pub fn render_env(
        &self,
        parameters: &mounts::Parameters,
    ) -> Result<BTreeMap<String, String>, String> {
        mounts::validate_parameters(parameters)?;
        self.env
            .iter()
            .map(|(key, value)| Ok((key.clone(), parameters::render(value, Some(parameters))?)))
            .collect()
    }
    pub fn validate(&self) -> ValidationResult {
        for (label, value) in [
            ("name", &self.name),
            ("version", &self.version),
            ("image", &self.image),
            ("isolation_runtime", &self.isolation_runtime),
        ] {
            identifier(value, label)?;
        }
        for (key, value) in &self.env {
            parameters::render(value, None)?;
            if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') {
                return Err("invalid environment variable".into());
            }
            if matches!(
                key.as_str(),
                "ADX_AGENT_EXECUTION_HASH"
                    | "ADX_ENVIRONMENT_ID"
                    | "ADX_RUNTIME_ID"
                    | "ADX_OWNERSHIP_GENERATION"
                    | "ADX_IMAGE_PROCESS_CONFIG"
                    | "EXECD_HTTP_TOKEN"
                    | "EXECD_HTTP_PORT"
                    | "EXECD_HTTP_ONLY"
            ) {
                return Err(format!("{key} is reserved for the platform"));
            }
        }
        let mut seen = BTreeSet::new();
        for service in &self.service {
            if service.port == 0 || !seen.insert((service.protocol, service.port)) {
                return Err("service ports must be nonzero and protocol/port pairs unique".into());
            }
        }
        mounts::validate_mounts(&self.mounts)?;
        self.lifecycle.validate()?;
        if self
            .init_timeout_seconds
            .is_some_and(|seconds| seconds == 0 || seconds > 86400)
        {
            return Err("init_timeout_seconds must be between 1 and 86400".into());
        }
        if let Some(probe) = &self.readiness_probe {
            probe.validate()?;
        }
        if let Some(probe) = &self.healthcheck {
            probe.validate()?;
        }
        self.resources.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingPhase {
    Active,
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentBinding {
    pub scope: Scope,
    pub generation: String,
    /// Stable Platform identity, including across retries and runtime pause/resume.
    pub environment_id: String,
    /// Product deletion intent only; Environment runtime state belongs to Platform.
    pub phase: BindingPhase,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: mounts::Parameters,
}

pub mod discovery;
