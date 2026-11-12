use std::time::Duration;

/// Process-owned trace runtime configuration.
///
/// SDK users do not construct this type: the SDK deliberately obeys the
/// subscriber/exporter already chosen by its host application.
#[derive(Clone, Debug, PartialEq)]
pub struct TracingConfig {
    pub enabled: bool,
    /// Whether successful periodic lifecycle traffic should produce spans.
    ///
    /// Heartbeat/keepalive traffic is still executed and measured even when
    /// this is false. 健康周期默认仅记录指标；失败日志与错误指标仍保留，
    /// 诊断 Span 遵循同一采样策略，不承诺每次失败都导出 Trace。
    pub periodic_operations: bool,
    pub otlp_endpoint: String,
    pub sample_ratio: f64,
    pub queue_capacity: usize,
    pub max_export_batch_size: usize,
    pub batch_interval: Duration,
    pub export_timeout: Duration,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            periodic_operations: false,
            otlp_endpoint: "http://127.0.0.1:4317".to_string(),
            sample_ratio: 0.01,
            queue_capacity: 4_096,
            max_export_batch_size: 512,
            batch_interval: Duration::from_secs(5),
            export_timeout: Duration::from_secs(3),
        }
    }
}

impl TracingConfig {
    pub fn validate(&self) -> Result<(), TracingError> {
        if !(0.0..=1.0).contains(&self.sample_ratio) {
            return Err(TracingError::InvalidConfig(
                "sample_ratio must be between 0.0 and 1.0".to_string(),
            ));
        }
        if self.queue_capacity == 0 {
            return Err(TracingError::InvalidConfig(
                "queue_capacity must be positive".to_string(),
            ));
        }
        if self.max_export_batch_size == 0 || self.max_export_batch_size > self.queue_capacity {
            return Err(TracingError::InvalidConfig(
                "max_export_batch_size must be positive and no larger than queue_capacity"
                    .to_string(),
            ));
        }
        if self.batch_interval.is_zero() || self.export_timeout.is_zero() {
            return Err(TracingError::InvalidConfig(
                "batch_interval and export_timeout must be positive".to_string(),
            ));
        }
        Ok(())
    }
}

/// Stable identity attached to every Span exported by one process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessIdentity {
    pub service_name: String,
    pub instance: String,
}

impl ProcessIdentity {
    pub fn new(service_name: impl Into<String>, instance: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            instance: instance.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TracingError {
    #[error("invalid tracing configuration: {0}")]
    InvalidConfig(String),
    #[error("failed to build OTLP exporter: {0}")]
    Exporter(String),
    #[error("failed to install process tracing subscriber: {0}")]
    Subscriber(String),
    #[error("failed to start the OTLP runtime: {0}")]
    Runtime(String),
    #[error("failed to flush or stop tracing runtime: {0}")]
    Shutdown(String),
}
