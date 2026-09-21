//! Agent product state. No Platform, network or persistence dependencies.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub mod activator;
pub mod error;
pub mod inline;
pub mod limits;
pub mod sandbox;
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
    pub environment_id: String,
}

impl Scope {
    pub fn validate(&self) -> ValidationResult {
        for (label, value) in [
            ("tenant", &self.tenant),
            ("template", &self.template),
            ("version", &self.version),
            ("environment_id", &self.environment_id),
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
}

impl Resources {
    pub fn validate(&self) -> ValidationResult {
        if self.cpu_millis == 0 || self.memory_mib == 0 {
            return Err("cpu_millis and memory_mib must be positive".into());
        }
        self.memory_mib
            .checked_mul(1_048_576)
            .ok_or("memory_mib exceeds representable byte count")?;
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
    pub entrypoint: Vec<String>,
    #[serde(default)]
    pub working_dir: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub resources: Resources,
    #[serde(default)]
    pub service: Vec<Service>,
}

impl TemplateVersion {
    pub fn validate(&self) -> ValidationResult {
        for (label, value) in [
            ("name", &self.name),
            ("version", &self.version),
            ("image", &self.image),
            ("isolation_runtime", &self.isolation_runtime),
        ] {
            identifier(value, label)?;
        }
        if self
            .entrypoint
            .first()
            .is_none_or(|arg| arg.trim().is_empty())
            || self.entrypoint.iter().any(|arg| arg.contains('\0'))
        {
            return Err("entrypoint requires a nonempty executable and NUL-free argv".into());
        }
        if let Some(cwd) = &self.working_dir {
            if !cwd.starts_with('/') || cwd.contains('\0') || cwd.split('/').any(|p| p == "..") {
                return Err(
                    "working_dir must be an absolute sandbox path without parent traversal".into(),
                );
            }
        }
        for (key, value) in &self.env {
            if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') {
                return Err("invalid environment variable".into());
            }
            if matches!(
                key.as_str(),
                "ADX_AGENT_EXECUTION_HASH"
                    | "ADX_CAPSULE_ID"
                    | "ADX_RUNTIME_ID"
                    | "ADX_OWNERSHIP_GENERATION"
                    | "ADX_IMAGE_PROCESS_CONFIG"
                    | "RRT_HTTP_TOKEN"
                    | "RRT_HTTP_PORT"
                    | "RRT_HTTP_ONLY"
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
        self.resources.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentPhase {
    Active,
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Environment {
    pub scope: Scope,
    pub generation: String,
    /// Stable Platform identity, including across retries and runtime pause/resume.
    pub sandbox_id: String,
    /// Product deletion intent only; Sandbox runtime state belongs to Platform.
    pub phase: EnvironmentPhase,
}
