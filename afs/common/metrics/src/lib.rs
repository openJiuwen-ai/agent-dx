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
    hash::{Hash, Hasher},
    sync::OnceLock,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use prometheus::{Encoder, TextEncoder};
pub use prometheus::{
    Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts,
};

/// Process-owned collector registry plus a small OpenMetrics exemplar sidecar.
///
/// `prometheus` 0.14 owns counters/histograms. The sidecar stores at most one
/// sampled trace per bounded histogram bucket and augments exposition only;
/// it is never consulted by a business operation.
#[derive(Clone)]
pub struct Registry {
    inner: prometheus::Registry,
    exemplars: Arc<Mutex<ExemplarStore>>,
    bundles: Arc<Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>>,
}

impl Registry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: prometheus::Registry::new(),
            exemplars: Arc::new(Mutex::new(HashMap::new())),
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

    #[must_use]
    pub fn exemplar_recorder(
        &self,
        metric: &'static str,
        labels: &'static [&'static str],
        buckets: &[f64],
    ) -> ExemplarRecorder {
        let mut label_order = (0..labels.len()).collect::<Vec<_>>();
        label_order.sort_unstable_by_key(|index| labels[*index]);
        ExemplarRecorder {
            metric,
            labels,
            buckets: Arc::from(buckets),
            label_order: Arc::from(label_order),
            store: Arc::clone(&self.exemplars),
        }
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

/// Trace identity attached to one selected histogram observation.
///
/// It is deliberately a fixed 16-byte value. `trace_id` is never a metric
/// label; hexadecimal encoding is deferred until OpenMetrics exposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricExemplar {
    trace_id: [u8; 16],
}

type ExemplarProvider = fn() -> Option<MetricExemplar>;
static EXEMPLAR_PROVIDER: OnceLock<ExemplarProvider> = OnceLock::new();

/// Installs the trace bridge without making Metrics depend on one tracing SDK.
/// Repeated installation is intentionally harmless for SDK/server composition.
pub fn install_exemplar_provider(provider: ExemplarProvider) {
    let _ = EXEMPLAR_PROVIDER.set(provider);
}

fn capture_exemplar() -> Option<MetricExemplar> {
    EXEMPLAR_PROVIDER.get().and_then(|provider| provider())
}

/// Typed handle used beside one existing `HistogramVec`.
///
/// Labels must be the same bounded labels used by that histogram. The trace ID
/// is attached to a bucket sample, never promoted to a Prometheus label.
#[derive(Clone)]
pub struct ExemplarRecorder {
    metric: &'static str,
    labels: &'static [&'static str],
    // 配置只读，Guard 克隆不再为未采样请求重复分配 bucket 数组。
    buckets: Arc<[f64]>,
    label_order: Arc<[usize]>,
    store: Arc<Mutex<ExemplarStore>>,
}

// 以稳定身份散列定位候选，再比较完整标签消解碰撞。热身后更新已有
// series 不复制标签、不排序、不编码 Trace ID；仅首次采样建立有界系列。
type ExemplarStore = HashMap<u64, Vec<(ExemplarSeries, ExemplarValue)>>;

impl ExemplarRecorder {
    pub fn record(&self, labels: &[&str], value: f64, exemplar: Option<MetricExemplar>) {
        let Some(exemplar) = exemplar.or_else(capture_exemplar) else {
            return;
        };
        if labels.len() != self.labels.len() {
            return;
        }
        let upper_bound = self
            .buckets
            .iter()
            .copied()
            .find(|bound| value <= *bound)
            .unwrap_or(f64::INFINITY);
        let ordered_labels = || {
            self.label_order
                .iter()
                .map(|index| (self.labels[*index], labels[*index]))
        };
        let mut identity = std::collections::hash_map::DefaultHasher::new();
        self.metric.hash(&mut identity);
        upper_bound.to_bits().hash(&mut identity);
        for label in ordered_labels() {
            label.hash(&mut identity);
        }
        let observed_at_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_millis() as u64);
        let value = ExemplarValue {
            trace_id: exemplar.trace_id(),
            observed_value: value,
            observed_at_millis,
        };
        if let Ok(mut store) = self.store.lock() {
            let entries = store.entry(identity.finish()).or_default();
            if let Some((_, previous)) = entries.iter_mut().find(|(series, _)| {
                series.metric == self.metric
                    && series.upper_bound_bits == upper_bound.to_bits()
                    && series.labels.len() == self.labels.len()
                    && series.labels.iter().zip(ordered_labels()).all(
                        |((name, value), (expected_name, expected_value))| {
                            name == expected_name && value == expected_value
                        },
                    )
            }) {
                *previous = value;
            } else {
                entries.push((
                    ExemplarSeries {
                        metric: self.metric,
                        labels: ordered_labels()
                            .map(|(name, value)| (name.to_owned(), value.to_owned()))
                            .collect(),
                        upper_bound_bits: upper_bound.to_bits(),
                    },
                    value,
                ));
            }
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ExemplarSeries {
    metric: &'static str,
    labels: Vec<(String, String)>,
    upper_bound_bits: u64,
}

#[derive(Clone, Debug)]
struct ExemplarValue {
    trace_id: [u8; 16],
    observed_value: f64,
    observed_at_millis: u64,
}

impl MetricExemplar {
    #[must_use]
    pub const fn new(trace_id: [u8; 16]) -> Self {
        Self { trace_id }
    }

    #[must_use]
    pub const fn trace_id(self) -> [u8; 16] {
        self.trace_id
    }
}

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

/// Encodes one OpenMetrics snapshot, including sampled histogram exemplars.
pub fn encode_text(registry: &Registry) -> Result<String, MetricsError> {
    let mut buffer = Vec::new();
    TextEncoder::new().encode(&registry.gather(), &mut buffer)?;
    let prometheus = String::from_utf8(buffer)?;
    let exemplars = registry.exemplars.lock().map_or_else(
        |_| HashMap::new(),
        |store| store.values().flatten().cloned().collect(),
    );
    let mut output = String::with_capacity(prometheus.len() + 128);
    for line in prometheus.lines() {
        output.push_str(line);
        if let Some(exemplar) = exemplar_for_line(line, &exemplars) {
            output.push_str(" # {trace_id=\"");
            output.push_str(&hex_trace_id(exemplar.trace_id));
            output.push_str("\"} ");
            output.push_str(&exemplar.observed_value.to_string());
            output.push(' ');
            output.push_str(&exemplar.observed_at_millis.to_string());
        }
        output.push('\n');
    }
    output.push_str("# EOF\n");
    Ok(output)
}

fn exemplar_for_line<'a>(
    line: &str,
    exemplars: &'a HashMap<ExemplarSeries, ExemplarValue>,
) -> Option<&'a ExemplarValue> {
    let (metric_and_labels, _) = line.split_once(' ')?;
    let (metric, raw_labels) = metric_and_labels.split_once('{')?;
    let metric = metric.strip_suffix("_bucket")?;
    let raw_labels = raw_labels.strip_suffix('}')?;
    let mut labels = Vec::new();
    let mut upper_bound = None;
    for pair in raw_labels.split(',') {
        let (name, value) = pair.split_once('=')?;
        let value = value.trim_matches('"');
        if name == "le" {
            upper_bound = Some(if value == "+Inf" {
                f64::INFINITY
            } else {
                value.parse::<f64>().ok()?
            });
        } else {
            labels.push((name.to_string(), value.to_string()));
        }
    }
    labels.sort_unstable();
    exemplars.get(&ExemplarSeries {
        metric: exemplars.keys().find(|key| key.metric == metric)?.metric,
        labels,
        upper_bound_bits: upper_bound?.to_bits(),
    })
}

fn hex_trace_id(bytes: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(32);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
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
    fn sampled_updates_reuse_bound_series_and_binary_identity() {
        let registry = registry();
        let recorder = registry.exemplar_recorder("test", &["z", "a"], &[1.0]);
        recorder.record(&["last", "first"], 0.1, Some(MetricExemplar::new([1; 16])));
        let original_labels = {
            let store = registry.exemplars.lock().unwrap();
            let (series, _) = &store.values().next().unwrap()[0];
            assert_eq!(series.labels[0].0, "a");
            series.labels.as_ptr()
        };
        for _ in 0..1000 {
            recorder.record(&["last", "first"], 0.2, Some(MetricExemplar::new([2; 16])));
        }
        let store = registry.exemplars.lock().unwrap();
        let entries = store.values().next().unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0.labels.as_ptr(), original_labels);
        assert_eq!(entries[0].1.trace_id, [2; 16]);
    }

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
