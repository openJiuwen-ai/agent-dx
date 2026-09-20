//! Agent product state. No Platform, network or persistence dependencies.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub mod cache;
pub mod dispatcher;
pub mod inline;
pub mod limits;
pub mod routing;
pub mod sandbox;
pub mod transport;

pub type ValidationResult = Result<(), String>;

/// Wall-clock deadline shared across Dispatcher processes. Deployment clocks must be synchronized.
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
    pub session_id: String,
}

impl Scope {
    pub fn validate(&self) -> ValidationResult {
        for (label, value) in [
            ("tenant", &self.tenant),
            ("template", &self.template),
            ("version", &self.version),
            ("session_id", &self.session_id),
        ] {
            identifier(value, label)?;
        }
        Ok(())
    }
    pub fn key(&self) -> String {
        encode_key(&[
            &self.tenant,
            &self.template,
            &self.version,
            &self.session_id,
        ])
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
                    | "ADX_AGENT_SERVICE_PORTS"
                    | "ADX_AGENT_HAS_ENTRYPOINT"
                    | "ADX_INSTANCE_ID"
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
pub enum SessionPhase {
    Active,
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    pub scope: Scope,
    /// Distinguishes reuse of a public session ID; never reused after deletion.
    pub generation: String,
    pub phase: SessionPhase,
    /// Logical instances in this Session, updated atomically with instance state.
    pub instances: BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DesiredState {
    Running,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstancePhase {
    Creating,
    Ready,
    Failed,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Instance {
    pub id: String,
    pub tenant: String,
    pub scope: Scope,
    pub session_generation: String,
    pub sandbox_id: String,
    pub desired: DesiredState,
    pub phase: InstancePhase,
    pub status_message: Option<String>,
    /// Absolute creation deadline, fixed at reservation and never extended by recovery.
    pub create_deadline_ms: u64,
}

impl Instance {
    pub fn creation_expired(&self) -> bool {
        self.phase == InstancePhase::Creating && unix_time_millis() >= self.create_deadline_ms
    }

    /// ADX only reconciles unfinished creation/deletion. Substrate owns health after Ready.
    pub fn needs_reconciliation(&self) -> bool {
        self.phase != InstancePhase::Deleted
            && (self.desired == DesiredState::Deleted || self.phase == InstancePhase::Creating)
    }

    pub fn accepts_binding(&self, scope: &Scope) -> bool {
        self.tenant == scope.tenant
            && self.desired == DesiredState::Running
            && self.phase == InstancePhase::Ready
            && &self.scope == scope
    }
    pub fn validate(&self) -> ValidationResult {
        if self.create_deadline_ms == 0 {
            return Err("creation deadline must be positive".into());
        }
        identifier(&self.id, "instance id")?;
        identifier(&self.tenant, "tenant")?;
        identifier(&self.sandbox_id, "stable sandbox id")?;
        self.scope.validate()?;
        identifier(&self.session_generation, "session generation")?;
        if self.scope.tenant != self.tenant {
            return Err("instance tenant differs from session".into());
        }
        if self.phase == InstancePhase::Deleted && self.desired != DesiredState::Deleted {
            return Err("deleted instance cannot desire running".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AffinityBinding {
    pub scope: Scope,
    pub affinity_key: String,
    pub session_generation: String,
    pub instance_id: String,
}
