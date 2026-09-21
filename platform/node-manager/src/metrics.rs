//! Sampled backend usage, exposed by the node for the deployment's collector.
use crate::NodeManager;
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
#[derive(Default)]
pub(crate) struct Metrics(Mutex<BTreeMap<String, (String, RuntimeUsage, Instant)>>);
impl Metrics {
    pub fn record(&self, instance: &str, runtime: &str, usage: RuntimeUsage) {
        self.0
            .lock()
            .expect("shared state lock poisoned")
            .insert(instance.into(), (runtime.into(), usage, Instant::now()));
    }
    pub fn remove(&self, instance: &str) {
        self.0
            .lock()
            .expect("shared state lock poisoned")
            .remove(instance);
    }
    fn render(&self) -> String {
        let escape = |s: &str| {
            s.replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
        };
        let mut output = String::new();
        for (id, (runtime, usage, sampled)) in
            self.0.lock().expect("shared state lock poisoned").iter()
        {
            let labels = format!(
                "instance_id=\"{}\",runtime_id=\"{}\"",
                escape(id),
                escape(runtime)
            );
            for (name, value) in [
                (
                    "adx_instance_cpu_usage_seconds_total",
                    usage.cpu_usage_ns as f64 / 1e9,
                ),
                (
                    "adx_instance_memory_usage_bytes",
                    usage.memory_usage_bytes as f64,
                ),
                (
                    "adx_instance_memory_limit_bytes",
                    usage.memory_limit_bytes as f64,
                ),
                (
                    "adx_instance_stats_age_seconds",
                    sampled.elapsed().as_secs_f64(),
                ),
            ] {
                output.push_str(&format!("{name}{{{labels}}} {value}\n"));
            }
        }
        output
    }
}
impl NodeManager {
    pub fn metrics(&self) -> String {
        let mut output = self.services.metrics.render();
        output.push_str(&adx_observability::trace::metrics());
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
pub async fn serve(manager: Arc<NodeManager>, bind: std::net::SocketAddr) -> std::io::Result<()> {
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
                        && request.uri().path() == "/metrics";
                    let response = hyper::Response::builder()
                        .status(if valid { 200 } else { 404 })
                        .header("Content-Type", "text/plain; version=0.0.4")
                        .body(Full::new(Bytes::from(if valid {
                            manager.metrics()
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
