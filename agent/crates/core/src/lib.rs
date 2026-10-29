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
pub mod volumes;

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
    pub template_ref: TemplateRef,
    pub binding_id: String,
}

impl Scope {
    pub fn validate(&self) -> ValidationResult {
        for (label, value) in [
            ("tenant", self.tenant.as_str()),
            ("template", self.template.as_str()),
            ("template reference", self.template_ref.name()),
            ("binding_id", self.binding_id.as_str()),
        ] {
            identifier(value, label)?;
        }
        self.template_ref.validate()?;
        Ok(())
    }
}

/// The template reference is part of the logical identity; its resolved version is not.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemplateRef {
    Version(String),
    Alias(String),
}
impl TemplateRef {
    pub fn name(&self) -> &str {
        match self {
            Self::Version(value) | Self::Alias(value) => value,
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Version(_) => "version",
            Self::Alias(_) => "alias",
        }
    }
}

/// Published versions are canonical positive u64 decimal strings.
pub fn version_number(value: &str) -> Result<u64, String> {
    let number = value
        .parse::<u64>()
        .map_err(|_| "version must be a positive decimal u64".to_owned())?;
    if number == 0 || number.to_string() != value {
        return Err("version must be a canonical positive decimal u64".into());
    }
    Ok(number)
}
pub fn alias_name(value: &str) -> ValidationResult {
    identifier(value, "alias")?;
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("alias must not be purely numeric".into());
    }
    Ok(())
}
impl std::str::FromStr for TemplateRef {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let reference = if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
            Self::Version(value.into())
        } else {
            Self::Alias(value.into())
        };
        reference.validate()?;
        Ok(reference)
    }
}
impl TemplateRef {
    pub fn validate(&self) -> ValidationResult {
        match self {
            Self::Version(value) => version_number(value).map(|_| ()),
            Self::Alias(value) => alias_name(value),
        }
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
pub struct TemplateDraft {
    pub name: String,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<volumes::VolumeSpec>,
    #[serde(default)]
    pub service: Vec<Service>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init_timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_probe: Option<probes::ReadinessProbe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<probes::Healthcheck>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<volumes::VolumeSpec>,
    #[serde(default)]
    pub service: Vec<Service>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init_timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness_probe: Option<probes::ReadinessProbe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<probes::Healthcheck>,
}

impl TemplateDraft {
    pub fn validate(&self) -> ValidationResult {
        self.with_version("1".into()).validate()
    }
    /// Attach the server-assigned version to this immutable publication snapshot.
    pub fn with_version(&self, version: String) -> TemplateVersion {
        TemplateVersion {
            version,
            name: self.name.clone(),
            image: self.image.clone(),
            isolation_runtime: self.isolation_runtime.clone(),
            env: self.env.clone(),
            resources: self.resources.clone(),
            lifecycle: self.lifecycle.clone(),
            mounts: self.mounts.clone(),
            volumes: self.volumes.clone(),
            service: self.service.clone(),
            init_timeout_seconds: self.init_timeout_seconds,
            readiness_probe: self.readiness_probe.clone(),
            healthcheck: self.healthcheck.clone(),
        }
    }
}
impl From<&TemplateVersion> for TemplateDraft {
    fn from(template: &TemplateVersion) -> Self {
        Self {
            name: template.name.clone(),
            image: template.image.clone(),
            isolation_runtime: template.isolation_runtime.clone(),
            env: template.env.clone(),
            resources: template.resources.clone(),
            lifecycle: template.lifecycle.clone(),
            mounts: template.mounts.clone(),
            volumes: template.volumes.clone(),
            service: template.service.clone(),
            init_timeout_seconds: template.init_timeout_seconds,
            readiness_probe: template.readiness_probe.clone(),
            healthcheck: template.healthcheck.clone(),
        }
    }
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
    pub fn render_volumes(
        &self,
        parameters: &mounts::Parameters,
    ) -> Result<Vec<volumes::VolumeSpec>, String> {
        mounts::validate_parameters(parameters)?;
        let mut ids = BTreeSet::new();
        self.volumes
            .iter()
            .map(|volume| {
                let volume = volume.render(Some(parameters))?;
                if !ids.insert(volume.id.clone()) {
                    return Err("duplicate rendered Volume ID".into());
                }
                Ok(volume)
            })
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
        version_number(&self.version)?;
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
        if self.volumes.len() > 32 {
            return Err("at most 32 Volumes are allowed".into());
        }
        let mut volumes = BTreeSet::new();
        for volume in &self.volumes {
            volume.render(None)?;
            if !volumes.insert(&volume.id) {
                return Err("duplicate Volume ID".into());
            }
        }
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
    pub current: EnvironmentRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary: Option<SecondaryEnv>,
    /// Product deletion intent only; Environment runtime state belongs to Platform.
    pub phase: BindingPhase,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: mounts::Parameters,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub volumes: BTreeMap<String, String>,
    /// Creation request metadata, not Platform lifecycle or health state. Keyed by Env ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub creation_attempts: BTreeMap<String, CreationAttempt>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreationAttempt {
    pub number: u64,
    pub retry_after_unix_ms: u64,
    pub backoff_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentRef {
    pub version: String,
    pub environment_id: String,
}
impl EnvironmentRef {
    pub fn new(version: String, environment_id: String) -> Self {
        Self {
            version,
            environment_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecondaryIntent {
    Preparing,
    Retiring,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecondaryEnv {
    pub environment: EnvironmentRef,
    pub intent: SecondaryIntent,
}

pub mod alias;

pub mod discovery;
