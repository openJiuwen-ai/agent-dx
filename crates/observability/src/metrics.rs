//! Read-only Prometheus exposition of allocation ledgers (not runtime usage).
use adx_core::{
    scheduling::{DeviceKind, DeviceLedger},
    ResourceLedger,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Text {
    output: String,
    declared: BTreeSet<String>,
}
impl Text {
    pub fn gauge(&mut self, name: &str, labels: &[(&str, String)], value: u64) {
        self.declare(name, None, "gauge");
        let labels = Self::braced(Self::render_labels(labels));
        self.output.push_str(&format!("{name}{labels} {value}\n"));
    }
    /// 浮点 gauge（core-seconds、lag）。首次出现时同时输出 HELP 与 TYPE。
    pub fn gauge_f64(&mut self, name: &str, help: &str, labels: &[(&str, String)], value: f64) {
        self.declare(name, Some(help), "gauge");
        let labels = Self::braced(Self::render_labels(labels));
        self.output.push_str(&format!("{name}{labels} {value}\n"));
    }
    /// Counter；`name` 必须已含 `_total` 后缀（debug 断言）。
    pub fn counter(&mut self, name: &str, help: &str, labels: &[(&str, String)], value: u64) {
        debug_assert!(
            name.ends_with("_total"),
            "counter family {name} must end with _total"
        );
        self.declare(name, Some(help), "counter");
        let labels = Self::braced(Self::render_labels(labels));
        self.output.push_str(&format!("{name}{labels} {value}\n"));
    }
    /// 浮点 counter（积分类 core·秒 / byte·秒）。同样要求 `_total` 后缀。
    pub fn counter_f64(&mut self, name: &str, help: &str, labels: &[(&str, String)], value: f64) {
        debug_assert!(
            name.ends_with("_total"),
            "counter family {name} must end with _total"
        );
        self.declare(name, Some(help), "counter");
        let labels = Self::braced(Self::render_labels(labels));
        self.output.push_str(&format!("{name}{labels} {value}\n"));
    }
    /// 直方图：`le` 标签来自冻结 `BucketFamily`，拼接在调用方标签之后。
    pub fn histogram(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, String)],
        family: &BucketFamily,
        snapshot: &HistogramSnapshot,
    ) {
        debug_assert_eq!(family.labels.len(), family.bounds.len());
        debug_assert_eq!(snapshot.buckets.len(), family.bounds.len());
        self.declare(name, Some(help), "histogram");
        let inner = Self::render_labels(labels);
        let with_le = |le: &str| {
            if inner.is_empty() {
                format!("{{le=\"{le}\"}}")
            } else {
                format!("{{{inner},le=\"{le}\"}}")
            }
        };
        for (label, value) in family.labels.iter().zip(&snapshot.buckets) {
            self.output
                .push_str(&format!("{name}_bucket{} {value}\n", with_le(label)));
        }
        self.output.push_str(&format!(
            "{name}_bucket{} {}\n",
            with_le("+Inf"),
            snapshot.count
        ));
        let labels = Self::braced(inner);
        self.output
            .push_str(&format!("{name}_count{labels} {}\n", snapshot.count));
        self.output
            .push_str(&format!("{name}_sum{labels} {:.6}\n", snapshot.sum_seconds));
    }
    pub fn finish(self) -> String {
        self.output
    }
    /// 每族只声明一次；HELP（若有）位于 TYPE 之前。
    fn declare(&mut self, name: &str, help: Option<&str>, kind: &str) {
        if self.declared.insert(name.into()) {
            if let Some(help) = help {
                self.output.push_str(&format!("# HELP {name} {help}\n"));
            }
            self.output.push_str(&format!("# TYPE {name} {kind}\n"));
        }
    }
    fn render_labels(labels: &[(&str, String)]) -> String {
        labels
            .iter()
            .map(|(key, value)| {
                let escaped = value
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n");
                format!("{key}=\"{escaped}\"")
            })
            .collect::<Vec<_>>()
            .join(",")
    }
    fn braced(inner: String) -> String {
        if inner.is_empty() {
            inner
        } else {
            format!("{{{inner}}}")
        }
    }
}

/// 冻结桶族：`le` 标签字符串与上界配对，杜绝 f64 Display 改变 `le` 值（D8）。
#[derive(Debug, Clone, Copy)]
pub struct BucketFamily {
    pub labels: &'static [&'static str],
    pub bounds: &'static [f64],
}

/// 全部新阶段/体验/流族共用（详设 §5.2.1）。
pub const B15: BucketFamily = BucketFamily {
    labels: &[
        "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10",
        "30", "120", "600",
    ],
    bounds: &[
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 120.0, 600.0,
    ],
};

/// 仅 `adx_business_task_duration_seconds`（B15 尾部加长任务桶）。
pub const B18: BucketFamily = BucketFamily {
    labels: &[
        "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10",
        "30", "120", "600", "900", "1800", "3600",
    ],
    bounds: &[
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 120.0, 600.0,
        900.0, 1800.0, 3600.0,
    ],
};

/// 直方图快照：累积桶计数、总次数、总秒数。
pub struct HistogramSnapshot {
    pub buckets: Vec<u64>,
    pub count: u64,
    pub sum_seconds: f64,
}

/// 与 API Server 同构的并发直方图：每个满足 `seconds <= bound` 的桶累积 +1。
/// sum 以纳秒整数累加、快照时换算秒，避免浮点累加误差。
pub struct AtomicHistogram<const N: usize> {
    buckets: [AtomicU64; N],
    count: AtomicU64,
    sum_nanos: AtomicU64,
}

impl<const N: usize> AtomicHistogram<N> {
    pub const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; N],
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
        }
    }
    /// 按冻结族累积一次观测；`family.bounds.len()` 必须等于 `N`。
    pub fn observe(&self, family: &BucketFamily, seconds: f64) {
        debug_assert_eq!(family.bounds.len(), N);
        self.count.fetch_add(1, Ordering::Relaxed);
        let nanos = seconds * 1e9;
        if nanos.is_finite() && nanos >= 0.0 && nanos <= u64::MAX as f64 {
            self.sum_nanos.fetch_add(nanos as u64, Ordering::Relaxed);
        }
        for (bucket, bound) in self.buckets.iter().zip(family.bounds) {
            if seconds <= *bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            buckets: self
                .buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
            count: self.count.load(Ordering::Relaxed),
            sum_seconds: self.sum_nanos.load(Ordering::Relaxed) as f64 / 1e9,
        }
    }
}

impl<const N: usize> Default for AtomicHistogram<N> {
    fn default() -> Self {
        Self::new()
    }
}

pub fn resources(
    out: &mut Text,
    prefix: &str,
    labels: &[(&str, String)],
    scalar: &ResourceLedger,
    devices: &DeviceLedger,
    accepting: bool,
    devices_fresh: bool,
) {
    let capacity = scalar.capacity();
    let used = scalar.used();
    for (resource, capacity, used) in [
        ("cpu_millis", capacity.cpu_millis, used.cpu_millis),
        ("memory_bytes", capacity.memory_bytes, used.memory_bytes),
        ("disk_bytes", capacity.disk_bytes, used.disk_bytes),
    ] {
        for (state, value) in [
            ("capacity", capacity),
            ("reserved", used),
            (
                "available",
                if accepting {
                    capacity.saturating_sub(used)
                } else {
                    0
                },
            ),
            ("overcommitted", used.saturating_sub(capacity)),
        ] {
            out.gauge(&format!("{prefix}_{state}_{resource}"), labels, value);
        }
    }
    // Include allocations whose device disappeared or became unhealthy, rather
    // than deriving reservations from capacity minus current free inventory.
    let mut models: BTreeMap<(DeviceKind, String), (u64, u64, u64)> = BTreeMap::new();
    for d in devices.inventory() {
        let row = models.entry((d.kind, d.model.clone())).or_default();
        row.0 += u64::from(d.healthy);
    }
    for d in devices.allocated() {
        models.entry((d.kind, d.model.clone())).or_default().1 += 1;
    }
    for d in devices.available() {
        models.entry((d.kind, d.model.clone())).or_default().2 += 1;
    }
    for ((kind, model), (capacity, reserved, free)) in models {
        let mut labels = labels.to_vec();
        labels.push((
            "kind",
            match kind {
                DeviceKind::Gpu => "gpu",
                DeviceKind::Npu => "npu",
            }
            .into(),
        ));
        labels.push(("model", model));
        for (state, value) in [
            ("capacity", capacity),
            ("reserved", reserved),
            (
                "available",
                if accepting && devices_fresh { free } else { 0 },
            ),
            ("overcommitted", reserved.saturating_sub(capacity)),
        ] {
            let mut labels = labels.clone();
            labels.push(("state", state.into()));
            out.gauge(&format!("{prefix}_devices"), &labels, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_core::scheduling::{Device, DeviceAllocation, DeviceRequest};
    #[test]
    fn missing_and_unhealthy_devices_keep_reservations_and_escape_models() {
        let mut ledger = DeviceLedger::default();
        let model = "M\"\\\n".to_string();
        ledger
            .update(vec![Device {
                id: 0,
                kind: DeviceKind::Gpu,
                model: model.clone(),
                healthy: true,
            }])
            .unwrap();
        ledger
            .reserve(
                "a",
                &[DeviceRequest {
                    kind: DeviceKind::Gpu,
                    model: None,
                    count: 1,
                }],
                &[DeviceAllocation {
                    id: 0,
                    kind: DeviceKind::Gpu,
                    model: model.clone(),
                }],
            )
            .unwrap();
        for inventory in [
            vec![Device {
                id: 0,
                kind: DeviceKind::Gpu,
                model: model.clone(),
                healthy: false,
            }],
            vec![],
        ] {
            ledger.update(inventory).unwrap();
            let mut out = Text::default();
            resources(
                &mut out,
                "test",
                &[],
                &ResourceLedger::new(Default::default()),
                &ledger,
                true,
                true,
            );
            let text = out.finish();
            assert!(text.contains("model=\"M\\\"\\\\\\n\",state=\"reserved\"} 1\n"));
            assert!(text.contains("state=\"available\"} 0\n"));
            assert!(text.contains("state=\"overcommitted\"} 1\n"));
            assert_eq!(text.matches("# TYPE test_devices gauge").count(), 1);
        }
    }

    /// U1：gauge 输出字节回归（既有调用方依赖逐字节兼容）。
    #[test]
    fn gauge_output_is_byte_compatible() {
        let mut text = Text::default();
        text.gauge("adx_test_value", &[], 7);
        text.gauge("adx_test_value", &[("k", "v".into())], 3);
        assert_eq!(
            text.finish(),
            "# TYPE adx_test_value gauge\nadx_test_value 7\nadx_test_value{k=\"v\"} 3\n"
        );
    }

    /// U2：counter 族名必须带 `_total`；HELP/TYPE 仅首次输出。
    #[test]
    fn counter_requires_total_suffix_and_declares_once() {
        let mut text = Text::default();
        text.counter(
            "adx_test_events_total",
            "help text",
            &[("r", "a".into())],
            2,
        );
        text.counter(
            "adx_test_events_total",
            "help text",
            &[("r", "b".into())],
            5,
        );
        assert_eq!(
            text.finish(),
            "# HELP adx_test_events_total help text\n# TYPE adx_test_events_total counter\n\
adx_test_events_total{r=\"a\"} 2\nadx_test_events_total{r=\"b\"} 5\n"
        );
    }

    #[test]
    #[should_panic(expected = "must end with _total")]
    fn counter_rejects_family_without_total_suffix() {
        let mut text = Text::default();
        text.counter("adx_test_events", "help", &[], 1);
    }

    /// U3：直方图累积计数、冻结 `le` 标签、+Inf/count/sum 口径。
    #[test]
    fn histogram_renders_cumulative_buckets_with_frozen_labels() {
        let mut text = Text::default();
        let histogram = AtomicHistogram::<15>::new();
        histogram.observe(&B15, 0.004);
        histogram.observe(&B15, 2.0);
        let snapshot = histogram.snapshot();
        text.histogram(
            "adx_test_stream_seconds",
            "help",
            &[("t", "http".into())],
            &B15,
            &snapshot,
        );
        let output = text.finish();
        assert!(
            output.starts_with(
                "# HELP adx_test_stream_seconds help\n# TYPE adx_test_stream_seconds histogram\n"
            ),
            "{output}"
        );
        assert!(output.contains("adx_test_stream_seconds_bucket{t=\"http\",le=\"0.001\"} 0\n"));
        assert!(output.contains("adx_test_stream_seconds_bucket{t=\"http\",le=\"0.005\"} 1\n"));
        assert!(output.contains("adx_test_stream_seconds_bucket{t=\"http\",le=\"2.5\"} 2\n"));
        assert!(output.contains("adx_test_stream_seconds_bucket{t=\"http\",le=\"+Inf\"} 2\n"));
        assert!(output.contains("adx_test_stream_seconds_count{t=\"http\"} 2\n"));
        assert!(output.contains("adx_test_stream_seconds_sum{t=\"http\"} 2.004000\n"));
    }

    /// U4：零快照渲染全部桶的 0（稠密导出全零组合的前提）。
    #[test]
    fn zero_histogram_snapshot_renders_all_zero_buckets() {
        let mut text = Text::default();
        let histogram = AtomicHistogram::<15>::default();
        let snapshot = histogram.snapshot();
        text.histogram("adx_test_zero_seconds", "help", &[], &B15, &snapshot);
        let output = text.finish();
        assert!(output.contains("adx_test_zero_seconds_bucket{le=\"0.001\"} 0\n"));
        assert!(output.contains("adx_test_zero_seconds_bucket{le=\"600\"} 0\n"));
        assert!(output.contains("adx_test_zero_seconds_bucket{le=\"+Inf\"} 0\n"));
        assert!(output.contains("adx_test_zero_seconds_count 0\n"));
        assert!(output.contains("adx_test_zero_seconds_sum 0.000000\n"));
    }

    /// U5：桶族标签与边界一一对应、可解析为正有限值。
    #[test]
    fn bucket_families_keep_labels_aligned_with_bounds() {
        assert_eq!(B15.labels.len(), 15);
        assert_eq!(B18.labels.len(), 18);
        for family in [B15, B18] {
            assert_eq!(family.labels.len(), family.bounds.len());
            for (label, bound) in family.labels.iter().zip(family.bounds) {
                let parsed: f64 = label.parse().unwrap();
                assert_eq!(parsed, *bound, "label {label} must match bound");
                assert!(parsed.is_finite() && parsed > 0.0);
            }
        }
    }

    /// 浮点 counter：HELP/TYPE counter、`_total` 后缀、f64 值。
    #[test]
    fn counter_f64_renders_help_and_value() {
        let mut text = Text::default();
        text.counter_f64(
            "adx_test_core_seconds_total",
            "help",
            &[("c", "runsc".into())],
            12.5,
        );
        assert_eq!(
            text.finish(),
            "# HELP adx_test_core_seconds_total help\n# TYPE adx_test_core_seconds_total counter\n\
adx_test_core_seconds_total{c=\"runsc\"} 12.5\n"
        );
    }

    /// gauge_f64 首次声明 HELP/TYPE，值为 f64 Display。
    #[test]
    fn gauge_f64_renders_help_and_value() {
        let mut text = Text::default();
        text.gauge_f64("adx_test_ratio", "help", &[], 0.5);
        assert_eq!(
            text.finish(),
            "# HELP adx_test_ratio help\n# TYPE adx_test_ratio gauge\nadx_test_ratio 0.5\n"
        );
    }
}
