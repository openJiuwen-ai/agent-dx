//! Stable configuration types for the process logging pipeline.

use std::{path::PathBuf, str::FromStr, time::Duration};

/// Human-readable or machine-readable local log format.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LogFormat {
    Text,
    #[default]
    Json,
}

impl FromStr for LogFormat {
    type Err = LoggingError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err(LoggingError::InvalidConfig(
                "log format must be `text` or `json`".to_string(),
            )),
        }
    }
}

/// Destination owned by the process logger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogOutput {
    Stderr,
    File(PathBuf),
}

/// Behavior when the bounded asynchronous queue is full.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OverflowPolicy {
    /// Protect the data path and report the number of dropped records once the
    /// queue can accept records again.
    #[default]
    DropAndReport,
    /// Preserve every record at the cost of applying backpressure to callers.
    Block,
}

impl FromStr for OverflowPolicy {
    type Err = LoggingError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "drop-and-report" | "drop_and_report" => Ok(Self::DropAndReport),
            "block" => Ok(Self::Block),
            _ => Err(LoggingError::InvalidConfig(
                "log overflow must be `drop-and-report` or `block`".to_string(),
            )),
        }
    }
}

/// Fully resolved logging settings. Config-file and CLI precedence is owned by
/// the server crate; this type only validates and runs the resulting values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoggingConfig {
    pub level: slog::Level,
    pub format: LogFormat,
    pub output: LogOutput,
    pub async_queue_capacity: usize,
    pub overflow: OverflowPolicy,
    pub max_file_size: u64,
    pub max_backups: usize,
    pub max_age: Duration,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: slog::Level::Info,
            format: LogFormat::Json,
            output: LogOutput::Stderr,
            async_queue_capacity: 10_240,
            overflow: OverflowPolicy::DropAndReport,
            max_file_size: 256 * 1024 * 1024,
            max_backups: 14,
            max_age: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

impl LoggingConfig {
    pub(crate) fn validate(&self) -> Result<(), LoggingError> {
        if self.async_queue_capacity == 0 {
            return Err(LoggingError::InvalidConfig(
                "log async queue capacity must be positive".to_string(),
            ));
        }
        if self.max_file_size == 0 {
            return Err(LoggingError::InvalidConfig(
                "log max file size must be positive".to_string(),
            ));
        }
        if self.max_age.is_zero() {
            return Err(LoggingError::InvalidConfig(
                "log max age must be positive".to_string(),
            ));
        }
        Ok(())
    }
}

/// Stable fields attached to every process log record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessIdentity {
    pub service_name: String,
    pub instance: String,
}

impl ProcessIdentity {
    #[must_use]
    pub fn new(service_name: impl Into<String>, instance: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            instance: instance.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoggingError {
    #[error("invalid logging configuration: {0}")]
    InvalidConfig(String),
    #[error("failed to initialize log output: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to redirect the Rust log facade because another logger is already installed")]
    Redirect,
}

/// Parse the public textual level accepted by config files and CLI flags.
pub fn parse_level(value: &str) -> Result<slog::Level, LoggingError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "trace" => Ok(slog::Level::Trace),
        "debug" => Ok(slog::Level::Debug),
        "info" => Ok(slog::Level::Info),
        "warn" | "warning" => Ok(slog::Level::Warning),
        "error" => Ok(slog::Level::Error),
        "critical" | "crit" => Ok(slog::Level::Critical),
        _ => Err(LoggingError::InvalidConfig(
            "log level must be trace/debug/info/warn/error/critical".to_string(),
        )),
    }
}
