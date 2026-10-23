//! Workload probes configured by an immutable Agent template.
use crate::ValidationResult;
use serde::{Deserialize, Serialize};

fn default_path() -> String {
    "/".into()
}
const fn default_period_seconds() -> u64 {
    2
}
const fn default_timeout_seconds() -> u64 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpGetProbe {
    pub port: u16,
    #[serde(default = "default_path")]
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpSocketProbe {
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecProbe {
    pub command: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeAction<'a> {
    HttpGet { port: u16, path: &'a str },
    TcpSocket { port: u16 },
    Exec { command: &'a [String] },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadinessProbe {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_get: Option<HttpGetProbe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp_socket: Option<TcpSocketProbe>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec: Option<ExecProbe>,
    #[serde(default)]
    pub initial_delay_seconds: u64,
    #[serde(default = "default_period_seconds")]
    pub period_seconds: u64,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

impl ReadinessProbe {
    pub fn action(&self) -> Result<ProbeAction<'_>, String> {
        let actions = usize::from(self.http_get.is_some())
            + usize::from(self.tcp_socket.is_some())
            + usize::from(self.exec.is_some());
        if actions != 1 {
            return Err("readiness probe requires exactly one action".into());
        }
        if let Some(probe) = &self.http_get {
            return Ok(ProbeAction::HttpGet {
                port: probe.port,
                path: &probe.path,
            });
        }
        if let Some(probe) = &self.tcp_socket {
            return Ok(ProbeAction::TcpSocket { port: probe.port });
        }
        let probe = self
            .exec
            .as_ref()
            .ok_or_else(|| "readiness probe requires exactly one action".to_owned())?;
        Ok(ProbeAction::Exec {
            command: &probe.command,
        })
    }

    pub fn validate(&self) -> ValidationResult {
        if self.initial_delay_seconds > 86400
            || self.period_seconds == 0
            || self.period_seconds > 86400
            || self.timeout_seconds == 0
            || self.timeout_seconds > 3600
        {
            return Err("readiness probe timing is out of range".into());
        }
        match self.action()? {
            ProbeAction::HttpGet { port, path }
                if port == 0
                    || !path.starts_with('/')
                    || path.starts_with("//")
                    || path.len() > 2048
                    || path.bytes().any(|byte| byte <= b' ' || byte == 127) =>
            {
                Err("HTTP readiness probe requires a port and an origin-form path".into())
            }
            ProbeAction::TcpSocket { port: 0 } => {
                Err("readiness probe port must be positive".into())
            }
            ProbeAction::Exec { command }
                if command.is_empty()
                    || command[0].is_empty()
                    || command.len() > 256
                    || command.iter().any(|arg| arg.contains('\0'))
                    || command
                        .iter()
                        .try_fold(0usize, |size, arg| size.checked_add(arg.len()))
                        .is_none_or(|size| size > 16384) =>
            {
                Err("exec readiness probe requires a bounded nonempty argv without NUL".into())
            }
            _ => Ok(()),
        }
    }
}

/// Periodic workload health check evaluated by Platform, never by Activator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Healthcheck {
    #[serde(flatten)]
    pub probe: ReadinessProbe,
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
}
const fn default_failure_threshold() -> u32 {
    3
}
impl Healthcheck {
    pub fn validate(&self) -> ValidationResult {
        self.probe.validate()?;
        if self.failure_threshold == 0 {
            return Err("healthcheck failure_threshold must be positive".into());
        }
        Ok(())
    }
}
