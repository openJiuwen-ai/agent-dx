//! Sampled backend usage, exposed by the node for the deployment's collector.
use crate::Adxlet;
use adx_core::{EnvironmentRecord, EnvironmentState};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeUsage {
    pub cpu_usage_ns: u64,
    pub memory_usage_bytes: u64,
    pub memory_limit_bytes: u64,
}
#[derive(Clone)]
struct Sample {
    runtime: String,
    generation: u64,
    runtime_class: &'static str,
    usage: RuntimeUsage,
    sampled: Instant,
}
#[derive(Clone)]
pub(crate) struct Registration {
    environment: String,
    owner: Arc<()>,
}
struct Execution {
    runtime: String,
    generation: u64,
    runtime_class: &'static str,
}
struct Registered {
    owner: Arc<()>,
    execution: Option<Execution>,
}
#[derive(Default)]
struct Samples {
    registrations: BTreeMap<String, Registered>,
    samples: BTreeMap<String, Sample>,
    // Lifetime CPU totals survive removal of an Environment and runtime resets.
    cpu_ns: BTreeMap<&'static str, u128>,
}
#[derive(Default)]
pub(crate) struct Metrics(Mutex<Samples>);
impl Metrics {
    fn lock(&self) -> std::sync::MutexGuard<'_, Samples> {
        // This cache contains only rebuildable telemetry.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn register(&self, environment: &str) -> Registration {
        let registration = Registration {
            environment: environment.into(),
            owner: Arc::new(()),
        };
        self.lock().registrations.insert(
            environment.into(),
            Registered {
                owner: registration.owner.clone(),
                execution: None,
            },
        );
        registration
    }
    /// Eligibility and sample writes share this short, in-memory critical section.
    /// Old controllers cannot modify a replacement controller's registration.
    pub fn observe(&self, registration: &Registration, record: Option<&EnvironmentRecord>) {
        let mut samples = self.lock();
        let Some(current) = samples
            .registrations
            .get_mut(&registration.environment)
            .filter(|current| Arc::ptr_eq(&current.owner, &registration.owner))
        else {
            return;
        };
        current.execution = record
            .filter(|record| {
                record.state == EnvironmentState::Running
                    && record.spec.id == registration.environment
            })
            .map(|record| Execution {
                runtime: record.runtime.id.clone(),
                generation: record.assignment.generation,
                runtime_class: match record.spec.runtime_class.as_str() {
                    "runsc" => "runsc",
                    "firecracker" => "firecracker",
                    "runc" => "runc",
                    _ => "other",
                },
            });
    }
    pub fn record(
        &self,
        registration: &Registration,
        runtime: &str,
        generation: u64,
        usage: RuntimeUsage,
    ) -> bool {
        let mut samples = self.lock();
        let Some(execution) = samples
            .registrations
            .get(&registration.environment)
            .filter(|current| Arc::ptr_eq(&current.owner, &registration.owner))
            .and_then(|current| current.execution.as_ref())
            .filter(|execution| execution.runtime == runtime && execution.generation == generation)
        else {
            return false;
        };
        let runtime_class = execution.runtime_class;
        let environment = &registration.environment;
        let previous = samples
            .samples
            .get(environment)
            .filter(|sample| {
                sample.runtime == runtime
                    && sample.generation == generation
                    && sample.runtime_class == runtime_class
            })
            .map_or(0, |sample| sample.usage.cpu_usage_ns);
        let delta = usage
            .cpu_usage_ns
            .checked_sub(previous)
            .unwrap_or(usage.cpu_usage_ns);
        *samples.cpu_ns.entry(runtime_class).or_default() += u128::from(delta);
        samples.samples.insert(
            environment.into(),
            Sample {
                runtime: runtime.into(),
                generation,
                runtime_class,
                usage,
                sampled: Instant::now(),
            },
        );
        true
    }
    pub fn remove(&self, registration: &Registration) {
        let mut samples = self.lock();
        if let Some(current) = samples
            .registrations
            .get_mut(&registration.environment)
            .filter(|current| Arc::ptr_eq(&current.owner, &registration.owner))
        {
            current.execution = None;
            samples.samples.remove(&registration.environment);
        }
    }
    pub fn unregister(&self, registration: &Registration) {
        let mut samples = self.lock();
        if samples
            .registrations
            .get(&registration.environment)
            .is_some_and(|current| Arc::ptr_eq(&current.owner, &registration.owner))
        {
            samples.registrations.remove(&registration.environment);
            samples.samples.remove(&registration.environment);
        }
    }
    fn render(&self, detail: bool) -> String {
        let escape = |s: &str| {
            s.replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
        };
        // Copy eligible samples before formatting; exporters never hold the
        // registry lock while building a potentially large text response.
        let (samples, cpu_ns) = {
            let samples = self.lock();
            let current: BTreeMap<_, _> = samples
                .samples
                .iter()
                .filter(|(id, sample)| {
                    samples
                        .registrations
                        .get(*id)
                        .and_then(|current| current.execution.as_ref())
                        .is_some_and(|execution| {
                            execution.runtime == sample.runtime
                                && execution.generation == sample.generation
                        })
                })
                .map(|(id, sample)| (id.clone(), sample.clone()))
                .collect();
            (current, samples.cpu_ns.clone())
        };
        let mut output = String::new();
        if detail {
            for (id, sample) in &samples {
                let labels = format!(
                    "environment_id=\"{}\",runtime_id=\"{}\"",
                    escape(id),
                    escape(&sample.runtime)
                );
                for (name, value) in [
                    (
                        "adx_environment_cpu_usage_seconds_total",
                        sample.usage.cpu_usage_ns as f64 / 1e9,
                    ),
                    (
                        "adx_environment_memory_usage_bytes",
                        sample.usage.memory_usage_bytes as f64,
                    ),
                    (
                        "adx_environment_memory_limit_bytes",
                        sample.usage.memory_limit_bytes as f64,
                    ),
                    (
                        "adx_environment_stats_age_seconds",
                        sample.sampled.elapsed().as_secs_f64(),
                    ),
                ] {
                    output.push_str(&format!("{name}{{{labels}}} {value}\n"));
                }
            }
        } else {
            output.push_str("# TYPE adx_node_environment_cpu_usage_seconds_total counter\n");
            for (class, cpu) in &cpu_ns {
                let mut memory = 0u128;
                let mut limit = 0u128;
                let mut fresh = 0u64;
                let mut stale = 0u64;
                for sample in samples
                    .values()
                    .filter(|sample| sample.runtime_class == *class)
                {
                    if sample.sampled.elapsed().as_secs_f64() > 45.0 {
                        stale += 1;
                    } else {
                        fresh += 1;
                        memory += u128::from(sample.usage.memory_usage_bytes);
                        limit += u128::from(sample.usage.memory_limit_bytes);
                    }
                }
                output.push_str(&format!("adx_node_environment_cpu_usage_seconds_total{{runtime_class=\"{class}\"}} {}\n", *cpu as f64 / 1e9));
                for (name, value) in [
                    ("memory_usage_bytes", memory),
                    ("memory_limit_bytes", limit),
                    ("samples", u128::from(fresh)),
                    ("stale_samples", u128::from(stale)),
                ] {
                    output.push_str(&format!(
                        "adx_node_environment_{name}{{runtime_class=\"{class}\"}} {value}\n"
                    ));
                }
            }
        }
        output
    }
}
impl Adxlet {
    /// Opt-in short-retention resource samples, separate from the node summary.
    pub fn environment_metrics(&self) -> String {
        self.services.metrics.render(true)
    }
    pub fn metrics(&self) -> String {
        self.prune_retired_environments();
        let mut output = self.services.metrics.render(false);
        output.push_str(&adx_observability::trace::metrics());
        output.push_str(&adx_observability::process::metrics());
        let managed_environments = u64::try_from(
            self.environments
                .lock()
                .expect("shared state lock poisoned")
                .len(),
        )
        .unwrap_or(u64::MAX);
        let retired_environments = u64::try_from(
            self.retired_environments
                .lock()
                .expect("shared state lock poisoned")
                .len(),
        )
        .unwrap_or(u64::MAX);
        let lifecycle_ready =
            self.lifecycle_ready.try_read().is_ok_and(|ready| *ready) && !self.is_draining();
        let admission = self
            .services
            .admission
            .lock()
            .expect("shared state lock poisoned");
        let fresh = admission
            .valid_until
            .is_some_and(|until| tokio::time::Instant::now() < until);
        let devices_fresh = admission
            .devices_valid_until
            .is_some_and(|until| tokio::time::Instant::now() < until);
        let accepting = lifecycle_ready && fresh && !admission.maintenance && !admission.pressure;
        let mut text = adx_observability::metrics::Text::default();
        text.gauge("adx_node_accepting_allocations", &[], u64::from(accepting));
        text.gauge("adx_node_resource_observation_fresh", &[], u64::from(fresh));
        text.gauge(
            "adx_node_device_observation_fresh",
            &[],
            u64::from(devices_fresh),
        );
        text.gauge("adx_node_managed_environments", &[], managed_environments);
        text.gauge(
            "adx_node_retired_environment_tombstones",
            &[],
            retired_environments,
        );
        adx_observability::metrics::resources(
            &mut text,
            "adx_node",
            &[],
            &admission.ledger,
            &admission.devices,
            accepting,
            devices_fresh,
        );
        output.push_str(&text.finish());
        output
    }
}
pub async fn serve(manager: Arc<Adxlet>, bind: std::net::SocketAddr) -> std::io::Result<()> {
    use bytes::Bytes;
    use http_body_util::Full;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let manager = manager.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let valid = request.method() == hyper::Method::GET
                        && matches!(request.uri().path(), "/metrics" | "/metrics/environments");
                    let detail = request.uri().path() == "/metrics/environments";
                    let response = hyper::Response::builder()
                        .status(if valid { 200 } else { 404 })
                        .header("Content-Type", "text/plain; version=0.0.4")
                        .body(Full::new(Bytes::from(if valid {
                            if detail {
                                manager.environment_metrics()
                            } else {
                                manager.metrics()
                            }
                        } else {
                            String::new()
                        })))
                        .expect("static metrics response is valid");
                    async move { Ok::<_, std::convert::Infallible>(response) }
                },
            );
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), connection).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(cpu: u64, memory: u64) -> RuntimeUsage {
        RuntimeUsage {
            cpu_usage_ns: cpu,
            memory_usage_bytes: memory,
            memory_limit_bytes: 1024,
        }
    }

    fn running(environment: &str, runtime: &str, class: &str) -> EnvironmentRecord {
        EnvironmentRecord {
            spec: serde_json::from_value(serde_json::json!({
                "id":environment,"tenant_id":"tenant","image":"image","runtime_class":class,
                "priority":0,"resources":{"cpu_millis":1,"memory_bytes":1,"disk_bytes":0}
            }))
            .unwrap(),
            assignment: adx_core::Assignment {
                environment_id: environment.into(),
                node_id: "node".into(),
                shard_id: 0,
                generation: 1,
                devices: vec![],
            },
            runtime: adx_core::Runtime {
                id: runtime.into(),
                ip: None,
            },
            state: EnvironmentState::Running,
            revision: 1,
            resources_held: true,
            checkpoint: None,
            snapshot_source: None,
            execution_artifacts: vec![],
            last_operation: None,
            failure: None,
            restart_attempts: 0,
            restart_pending: false,
        }
    }
    fn register(metrics: &Metrics, record: &EnvironmentRecord) -> Registration {
        let registration = metrics.register(&record.spec.id);
        metrics.observe(&registration, Some(record));
        registration
    }
    #[test]
    fn aggregate_cpu_survives_runtime_removal_and_counter_resets() {
        let metrics = Metrics::default();
        let a = register(&metrics, &running("a", "a-1", "runsc"));
        let b = register(&metrics, &running("b", "b-1", "firecracker"));
        metrics.record(&a, "a-1", 1, usage(1_000_000_000, 100));
        metrics.record(&b, "b-1", 1, usage(2_000_000_000, 200));
        metrics.record(&a, "a-1", 1, usage(3_000_000_000, 110));
        metrics.remove(&a);
        metrics.observe(&a, Some(&running("a", "a-2", "runsc")));
        metrics.record(&a, "a-2", 1, usage(500_000_000, 120));
        metrics.record(&a, "a-2", 1, usage(250_000_000, 130));
        let aggregate = metrics.render(false);
        assert!(
            aggregate.contains(
                "adx_node_environment_cpu_usage_seconds_total{runtime_class=\"runsc\"} 3.75\n"
            ),
            "{aggregate}"
        );
        assert!(aggregate
            .contains("adx_node_environment_memory_usage_bytes{runtime_class=\"runsc\"} 130\n"));
        assert!(!aggregate.contains("environment_id="));
        let detail = metrics.render(true);
        assert!(detail.contains("environment_id=\"a\",runtime_id=\"a-2\""));
        assert!(!detail.contains("a-1"));
        assert!(!detail.contains("adx_node_environment_"));
    }

    #[test]
    fn stale_samples_are_not_current_resource_usage() {
        let metrics = Metrics::default();
        let a = register(&metrics, &running("a", "a-1", "runsc"));
        metrics.record(&a, "a-1", 1, usage(10, 100));
        metrics
            .0
            .lock()
            .unwrap()
            .samples
            .get_mut("a")
            .unwrap()
            .sampled = Instant::now() - std::time::Duration::from_secs(46);
        let aggregate = metrics.render(false);
        assert!(aggregate.contains("adx_node_environment_samples{runtime_class=\"runsc\"} 0\n"));
        assert!(
            aggregate.contains("adx_node_environment_stale_samples{runtime_class=\"runsc\"} 1\n")
        );
        assert!(aggregate
            .contains("adx_node_environment_memory_usage_bytes{runtime_class=\"runsc\"} 0\n"));
        assert!(metrics
            .render(true)
            .contains("adx_environment_stats_age_seconds"));
    }
    #[test]
    fn inactive_and_replaced_executions_reject_late_samples() {
        let metrics = Metrics::default();
        let record = running("a", "a-1", "runsc");
        let old = register(&metrics, &record);
        assert!(metrics.record(&old, "a-1", 1, usage(10, 100)));
        metrics.observe(&old, None);
        assert!(!metrics.record(&old, "a-1", 1, usage(20, 200)));
        assert!(!metrics.render(true).contains("memory_usage_bytes"));
        metrics.observe(&old, Some(&running("a", "a-2", "runsc")));
        assert!(!metrics.record(&old, "a-1", 1, usage(30, 300)));
        assert!(!metrics.record(&old, "a-2", 2, usage(30, 300)));
        let current = register(&metrics, &running("a", "a-2", "runsc"));
        assert!(metrics.record(&current, "a-2", 1, usage(40, 400)));
        metrics.observe(&old, Some(&record));
        metrics.remove(&old);
        metrics.unregister(&old);
        assert!(!metrics.record(&old, "a-1", 1, usage(50, 500)));
        assert!(metrics.render(true).contains(
            "adx_environment_memory_usage_bytes{environment_id=\"a\",runtime_id=\"a-2\"} 400"
        ));
        metrics.unregister(&current);
        assert!(!metrics.render(true).contains("environment_id"));
        assert!(metrics.0.lock().unwrap().registrations.is_empty());
    }
}
