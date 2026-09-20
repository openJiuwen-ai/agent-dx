//! The report-backed legacy create contract. Decoding preserves platform-dependent intent.
//! Successful validation is not a capability check; adapters must not discard unsupported fields.
use crate::{identifier, Resources, ValidationResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxType {
    Docker,
    Supervisor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rootfs {
    pub imageurl: String,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub ports: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mount {
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub readonly: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSpec {
    pub runtime: String,
    pub sandbox_type: SandboxType,
    #[serde(default)]
    pub rootfs: Option<Rootfs>,
    #[serde(default)]
    pub cmds: Vec<Vec<String>>,
    #[serde(default)]
    pub cpu: u64,
    #[serde(default)]
    pub memory: u64,
    #[serde(default)]
    pub probes: Option<Probes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRequest {
    pub name: String,
    pub namespace: String,
    pub runtime_spec: RuntimeSpec,
    /// In the presence of runtime_spec, the report explicitly gives inline precedence.
    #[serde(default)]
    pub urn: Option<String>,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub env_vars: BTreeMap<String, String>,
    #[serde(default)]
    pub mounts: Vec<Mount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Probes {
    #[serde(default)]
    pub startup: Option<Probe>,
    #[serde(default)]
    pub liveness: Option<Probe>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Probe {
    #[serde(default)]
    pub tcp_socket: Option<TcpSocket>,
    #[serde(default)]
    pub http_get: Option<HttpGet>,
    #[serde(default)]
    pub exec: Option<Exec>,
    #[serde(default)]
    pub initial_delay_seconds: Option<u32>,
    #[serde(default)]
    pub period_seconds: Option<u32>,
    #[serde(default)]
    pub timeout_seconds: Option<u32>,
    #[serde(default)]
    pub failure_threshold: Option<u32>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpSocket {
    pub port: u16,
    #[serde(default)]
    pub host: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpGet {
    pub port: u16,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub scheme: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Exec {
    pub command: Vec<String>,
}

fn argv(value: &[String]) -> ValidationResult {
    if value.first().is_none_or(|s| s.trim().is_empty()) || value.iter().any(|s| s.contains('\0')) {
        return Err("invalid command argv".into());
    }
    Ok(())
}
fn absolute(path: &str) -> bool {
    path.starts_with('/') && !path.contains('\0') && !path.split('/').any(|p| p == "..")
}
impl Probe {
    pub fn validate(&self) -> ValidationResult {
        if usize::from(self.tcp_socket.is_some())
            + usize::from(self.http_get.is_some())
            + usize::from(self.exec.is_some())
            != 1
        {
            return Err("probe requires exactly one action".into());
        }
        if self.tcp_socket.as_ref().is_some_and(|p| p.port == 0)
            || self.http_get.as_ref().is_some_and(|p| p.port == 0)
            || [
                self.period_seconds,
                self.timeout_seconds,
                self.failure_threshold,
            ]
            .contains(&Some(0))
        {
            return Err("probe ports, period, timeout and threshold must be positive".into());
        }
        if let Some(exec) = &self.exec {
            argv(&exec.command)?;
        }
        if let Some(http) = &self.http_get {
            if http
                .scheme
                .as_ref()
                .is_some_and(|s| s != "http" && s != "https")
                || http.path.as_ref().is_some_and(|p| !p.starts_with('/'))
            {
                return Err("invalid HTTP probe scheme or path".into());
            }
        }
        Ok(())
    }
}
impl CreateRequest {
    pub fn resources(&self) -> Resources {
        Resources {
            cpu_millis: if self.runtime_spec.cpu == 0 {
                1000
            } else {
                self.runtime_spec.cpu
            },
            memory_mib: if self.runtime_spec.memory == 0 {
                2048
            } else {
                self.runtime_spec.memory
            },
        }
    }
    pub fn validate(&self) -> ValidationResult {
        identifier(&self.name, "name")?;
        identifier(&self.namespace, "namespace")?;
        let spec = &self.runtime_spec;
        if !spec.runtime.eq_ignore_ascii_case("python3.11") {
            return Err("only report-backed python3.11 runtime compatibility is enabled".into());
        }
        if spec.cmds.len() > 1 {
            return Err(
                "multiple commands are outside the report-backed compatibility scope".into(),
            );
        }
        for command in &spec.cmds {
            argv(command)?;
        }
        if spec.sandbox_type == SandboxType::Docker
            && spec
                .rootfs
                .as_ref()
                .is_none_or(|r| r.imageurl.trim().is_empty())
        {
            return Err("docker requires rootfs.imageurl".into());
        }
        if let Some(rootfs) = &spec.rootfs {
            if rootfs.imageurl.contains('\0')
                || rootfs.user.as_ref().is_some_and(|u| u.contains('\0'))
            {
                return Err("invalid rootfs image or user".into());
            }
            for port in &rootfs.ports {
                if port
                    .strip_prefix("tcp:")
                    .and_then(|s| s.parse::<u16>().ok())
                    .is_none_or(|p| p == 0)
                {
                    return Err("rootfs.ports must use tcp:<1..65535>".into());
                }
            }
        }
        if self.workspace.as_ref().is_some_and(|p| !absolute(p))
            || self
                .mounts
                .iter()
                .any(|m| !absolute(&m.source) || !absolute(&m.target))
        {
            return Err(
                "workspace and mount paths must be absolute without parent traversal".into(),
            );
        }
        for (key, value) in &self.env_vars {
            if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') {
                return Err("invalid environment variable".into());
            }
        }
        if let Some(probes) = &spec.probes {
            for probe in [&probes.startup, &probes.liveness].into_iter().flatten() {
                probe.validate()?;
            }
        }
        self.resources().validate()
    }
}
