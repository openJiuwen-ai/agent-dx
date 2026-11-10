//! Shared Prometheus mechanics for AFS processes.
//!
//! Business metrics belong to the backend that owns their meaning.
//! A [`Registry`] is always created by the embedding process; this crate never
//! installs a global registry or opens a network listener.

#![forbid(unsafe_code)]

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use prometheus::{Encoder, TextEncoder};
pub use prometheus::{
    Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts,
};

/// Process-owned collector registry plus shared metric handle bundles.
///
/// `prometheus` 0.14 owns counters/histograms.
#[derive(Clone)]
pub struct Registry {
    inner: prometheus::Registry,
    bundles: Arc<Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>>,
}

impl Registry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: prometheus::Registry::new(),
            bundles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register(
        &self,
        collector: Box<dyn prometheus::core::Collector>,
    ) -> Result<(), prometheus::Error> {
        self.inner.register(collector)
    }

    /// 注册并复用宿主 Registry 内的一组句柄；独立 Registry 之间不共享状态。
    ///
    /// 初始化只发生一次并与其它注册者串行；factory 不得递归调用本方法。
    /// factory 应先构造 Collector 再注册，失败可能留下已注册的部分 Collector。
    pub fn get_or_register<T>(
        &self,
        factory: impl FnOnce(&Self) -> Result<T, MetricsError>,
    ) -> Result<T, MetricsError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut bundles = self
            .bundles
            .lock()
            .map_err(|_| MetricsError::BundlePoisoned)?;
        if let Some(bundle) = bundles.get(&TypeId::of::<T>()) {
            return Ok(bundle
                .downcast_ref::<T>()
                .expect("bundle type identity")
                .clone());
        }
        let bundle = factory(self)?;
        bundles.insert(TypeId::of::<T>(), Box::new(bundle.clone()));
        Ok(bundle)
    }

    #[must_use]
    pub fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.inner.gather()
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Registry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Registry").finish_non_exhaustive()
    }
}

pub const OPENMETRICS_CONTENT_TYPE: &str =
    "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// Registration or text-encoding failure at process composition time.
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("metrics bundle initialization lock poisoned")]
    BundlePoisoned,
    #[error("Prometheus collector registration failed: {0}")]
    Prometheus(#[from] prometheus::Error),
    #[error("Prometheus text encoding produced invalid UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

/// Creates one explicit process-owned registry.
#[must_use]
pub fn registry() -> Registry {
    Registry::new()
}

/// Encodes one OpenMetrics snapshot.
pub fn encode_text(registry: &Registry) -> Result<String, MetricsError> {
    let mut buffer = Vec::new();
    TextEncoder::new().encode(&registry.gather(), &mut buffer)?;
    let prometheus = String::from_utf8(buffer)?;
    let mut output = String::with_capacity(prometheus.len() + 6);
    for line in prometheus.lines() {
        output.push_str(line);
        output.push('\n');
    }
    output.push_str("# EOF\n");
    Ok(output)
}

/// Self-observability for the process-owned trace exporter.
#[derive(Clone)]
pub struct TraceRuntimeMetrics {
    export_batches_total: IntCounterVec,
    exported_spans_total: IntCounter,
    dropped_spans_total: IntCounterVec,
    export_duration_seconds: Histogram,
    export_queue_depth: IntGauge,
    last_success_timestamp_seconds: Gauge,
}

impl TraceRuntimeMetrics {
    pub fn register(registry: &Registry) -> Result<Self, MetricsError> {
        let metrics = Self {
            export_batches_total: IntCounterVec::new(
                Opts::new(
                    "afs_trace_export_batches_total",
                    "Completed OTLP export batches.",
                ),
                &["result"],
            )?,
            exported_spans_total: IntCounter::new(
                "afs_trace_exported_spans_total",
                "Spans successfully exported through OTLP.",
            )?,
            dropped_spans_total: IntCounterVec::new(
                Opts::new(
                    "afs_trace_dropped_spans_total",
                    "Spans dropped before OTLP export.",
                ),
                &["reason"],
            )?,
            export_duration_seconds: Histogram::with_opts(
                HistogramOpts::new(
                    "afs_trace_export_duration_seconds",
                    "OTLP batch export latency in seconds.",
                )
                .buckets(latency_buckets()),
            )?,
            export_queue_depth: IntGauge::new(
                "afs_trace_export_queue_depth",
                "Sampled spans waiting in the process export queue.",
            )?,
            last_success_timestamp_seconds: Gauge::new(
                "afs_trace_last_success_timestamp_seconds",
                "Unix timestamp of the most recent successful OTLP export.",
            )?,
        };
        register_collector(registry, &metrics.export_batches_total)?;
        register_collector(registry, &metrics.exported_spans_total)?;
        register_collector(registry, &metrics.dropped_spans_total)?;
        register_collector(registry, &metrics.export_duration_seconds)?;
        register_collector(registry, &metrics.export_queue_depth)?;
        register_collector(registry, &metrics.last_success_timestamp_seconds)?;
        // A CounterVec does not expose a metric family until at least one label
        // set has been materialized. Freeze the bounded reason vocabulary here
        // so a healthy process and a queue-full process publish the same
        // contract (the value remains zero until a real drop occurs).
        metrics
            .dropped_spans_total
            .with_label_values(&["queue_full"])
            .inc_by(0);
        Ok(metrics)
    }

    pub fn record_export_batch(&self, spans: usize, elapsed: std::time::Duration, success: bool) {
        self.export_batches_total
            .with_label_values(&[if success { "ok" } else { "error" }])
            .inc();
        self.export_duration_seconds.observe(elapsed.as_secs_f64());
        if success {
            self.exported_spans_total.inc_by(spans as u64);
            if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                self.last_success_timestamp_seconds.set(now.as_secs_f64());
            }
        }
    }

    pub fn record_dropped_span(&self, reason: &'static str) {
        self.dropped_spans_total.with_label_values(&[reason]).inc();
    }

    pub fn set_queue_depth(&self, depth: usize) {
        self.export_queue_depth.set(depth as i64);
    }
}

/// Registers a cloneable collector in an explicit process-owned registry.
///
/// Business crates use this helper so they own metric meaning and labels while
/// this common crate owns the Prometheus registration mechanics.
pub fn register_collector<C>(registry: &Registry, collector: &C) -> Result<(), MetricsError>
where
    C: prometheus::core::Collector + Clone + 'static,
{
    registry.register(Box::new(collector.clone()))?;
    Ok(())
}

/// Latency buckets shared by RPC and business-operation histograms.
#[must_use]
pub fn latency_buckets() -> Vec<f64> {
    vec![
        0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
        2.5, 5.0,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_bundles_are_shared_across_clones() {
        let registry = registry();
        let first = registry
            .get_or_register::<IntCounter>(|registry| {
                let counter = IntCounter::new("afs_shared_test_total", "test counter")?;
                register_collector(registry, &counter)?;
                Ok(counter)
            })
            .unwrap();
        first.inc();
        let second = registry
            .clone()
            .get_or_register::<IntCounter>(|_| panic!("already registered"))
            .unwrap();
        assert_eq!(second.get(), 1);
        assert!(
            encode_text(&registry)
                .unwrap()
                .contains("afs_shared_test_total 1")
        );
    }

    #[test]
    fn trace_runtime_contract_exposes_drop_family_before_the_first_drop() {
        let registry = registry();
        let _metrics = TraceRuntimeMetrics::register(&registry).unwrap();
        let text = encode_text(&registry).unwrap();
        assert!(text.contains("afs_trace_dropped_spans_total{reason=\"queue_full\"} 0"));
    }
}
