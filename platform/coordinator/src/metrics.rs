//! Optional local Prometheus endpoint; deploy on a private monitoring interface.
use crate::rpc::CoordinatorRpc;
use adx_core::observation::{Outcome, ReasonClass};
use adx_observability::metrics::{AtomicHistogram, Text, B15};
use bytes::Bytes;
use http_body_util::Full;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

pub async fn serve(listener: TcpListener, coordinator: CoordinatorRpc) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let coordinator = coordinator.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let coordinator = coordinator.clone();
                    async move {
                        if request.method() == hyper::Method::GET
                            && request.uri().path() == "/observability/environments"
                        {
                            let result = coordinator
                                .interruption_details(request.uri().query().unwrap_or(""))
                                .await;
                            let (status, body) = match result {
                                Ok(body) => (200, body.to_string()),
                                Err(adx_core::Error::Invalid(message)) => {
                                    (400, serde_json::json!({"error":message}).to_string())
                                }
                                Err(_) => (
                                    503,
                                    serde_json::json!({"error":"observation snapshot unavailable"})
                                        .to_string(),
                                ),
                            };
                            let mut response = hyper::Response::new(Full::new(Bytes::from(body)));
                            *response.status_mut() = hyper::StatusCode::from_u16(status)
                                .unwrap_or(hyper::StatusCode::INTERNAL_SERVER_ERROR);
                            response.headers_mut().insert(
                                hyper::header::CONTENT_TYPE,
                                hyper::header::HeaderValue::from_static("application/json"),
                            );
                            return Ok::<_, std::convert::Infallible>(response);
                        }
                        let (status, body) = if request.method() != hyper::Method::GET
                            || !matches!(request.uri().path(), "/metrics" | "/metrics/environments")
                        {
                            (404, String::new())
                        } else {
                            let detail = request.uri().path() == "/metrics/environments";
                            let result = if detail {
                                coordinator.environment_metrics().await
                            } else {
                                coordinator.metrics().await
                            };
                            match result {
                                Ok(mut text) => {
                                    if !detail {
                                        text.push_str(&adx_observability::trace::metrics());
                                        text.push_str(&adx_observability::process::metrics());
                                    }
                                    (200, text)
                                }
                                Err(_) => (503, "metrics temporarily unavailable\n".into()),
                            }
                        };
                        Ok::<_, std::convert::Infallible>(
                            hyper::Response::builder()
                                .status(status)
                                .header("Content-Type", "text/plain; version=0.0.4")
                                .body(Full::new(Bytes::from(body)))
                                .expect("static metrics response is valid"),
                        )
                    }
                },
            );
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service);
            let _ = tokio::time::timeout(Duration::from_secs(10), connection).await;
        });
    }
}

/// Queue stages carried by `adx_platform_queue_wait_seconds` (detail design §6.5).
/// Coordinator produces `Schedule` only; `Node` is produced by adxlet and is
/// exported here as a zero baseline so the two sides can be summed by label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueueStage {
    Schedule,
}

impl QueueStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
        }
    }
    fn index(self) -> usize {
        match self {
            Self::Schedule => 0,
        }
    }
}

/// Lifecycle operations carried by the coordinator lifecycle families.
/// `Create` reuses the API Server family and is intentionally absent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LifecycleOperation {
    Resume,
}

impl LifecycleOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Resume => "resume",
        }
    }
    fn index(self) -> usize {
        match self {
            Self::Resume => 0,
        }
    }
}

const RUNTIME_CLASSES: [&str; 4] = ["runsc", "firecracker", "runc", "other"];
/// Queue outcomes produced by the coordinator: assignment, confirmed
/// cancellation, and the reserved "could not confirm" value.
const QUEUE_OUTCOMES: [Outcome; 3] = [Outcome::Success, Outcome::Cancelled, Outcome::Unknown];
const OUTCOMES: [Outcome; 4] = [
    Outcome::Success,
    Outcome::Failure,
    Outcome::Cancelled,
    Outcome::Unknown,
];
const REASON_CLASSES: [ReasonClass; 13] = [
    ReasonClass::Auth,
    ReasonClass::Budget,
    ReasonClass::Capacity,
    ReasonClass::Placement,
    ReasonClass::RuntimeStart,
    ReasonClass::Readiness,
    ReasonClass::Route,
    ReasonClass::ModelUpstream,
    ReasonClass::Business,
    ReasonClass::Transport,
    ReasonClass::Cancelled,
    ReasonClass::Unknown,
    ReasonClass::Other,
];

fn runtime_index(value: &str) -> usize {
    crate::storage::observability::runtime_class(value)
}
fn outcome_index(outcome: Outcome) -> usize {
    OUTCOMES
        .iter()
        .position(|value| *value == outcome)
        .unwrap_or(OUTCOMES.len() - 1)
}
fn reason_index(reason: ReasonClass) -> usize {
    REASON_CLASSES
        .iter()
        .position(|value| *value == reason)
        .unwrap_or(REASON_CLASSES.len() - 1)
}

/// Coordinator-owned platform observations (detail design §6.5, M1-1/M1-3).
///
/// Export is dense on purpose: the adxlet side exports `stage="node"` sparsely,
/// so summing both sides by label stays correct.
#[derive(Default)]
pub(crate) struct CoordinatorMetrics {
    /// stage (schedule/node) × runtime_class × outcome (success/cancelled/unknown).
    queue_wait: [[[AtomicHistogram<15>; 3]; 4]; 2],
    /// operation × outcome × runtime_class.
    lifecycle_duration: [[[AtomicHistogram<15>; 4]; 4]; 3],
    /// operation × outcome × reason_class.
    lifecycle_operations: [[[AtomicU64; 13]; 4]; 3],
}

impl CoordinatorMetrics {
    /// Record one pure queue wait sample. `local-first` assignments never reach
    /// this method, so they cannot fabricate a zero-wait observation.
    pub(crate) fn observe_queue_wait(
        &self,
        stage: QueueStage,
        outcome: Outcome,
        runtime_class: &str,
        waited: Duration,
    ) {
        let Some(outcome) = QUEUE_OUTCOMES.iter().position(|value| *value == outcome) else {
            return;
        };
        self.queue_wait[stage.index()][runtime_index(runtime_class)][outcome]
            .observe(&B15, waited.as_secs_f64());
    }

    /// Record one lifecycle operation sample. `elapsed` covers the whole durable
    /// operation (first accepted request to terminal result), not one RPC call.
    pub(crate) fn observe_lifecycle(
        &self,
        operation: LifecycleOperation,
        outcome: Outcome,
        reason: ReasonClass,
        runtime_class: &str,
        elapsed: Duration,
    ) {
        self.lifecycle_duration[operation.index()][outcome_index(outcome)]
            [runtime_index(runtime_class)]
        .observe(&B15, elapsed.as_secs_f64());
        self.lifecycle_operations[operation.index()][outcome_index(outcome)][reason_index(reason)]
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn render(&self, out: &mut Text) {
        // Only stages the Coordinator actually produces; adxlet owns `node`.
        for stage in [QueueStage::Schedule] {
            for (outcome, outcome_label) in QUEUE_OUTCOMES.iter().enumerate() {
                for (index, class) in RUNTIME_CLASSES.iter().enumerate() {
                    let labels = [
                        ("stage", stage.as_str().to_owned()),
                        ("outcome", outcome_label.as_str().to_owned()),
                        ("runtime_class", (*class).to_owned()),
                    ];
                    out.histogram(
                        "adx_platform_queue_wait_seconds",
                        "Pure queue wait before assignment or cancellation, by queue stage.",
                        &labels,
                        &B15,
                        &self.queue_wait[stage.index()][index][outcome].snapshot(),
                    );
                }
            }
        }
        // Pause/delete are adxlet-side operations; exporting zero baselines
        // here makes the same family lie when only the Coordinator is scraped.
        for operation in [LifecycleOperation::Resume] {
            for (outcome, outcome_label) in OUTCOMES.iter().enumerate() {
                for (index, class) in RUNTIME_CLASSES.iter().enumerate() {
                    let labels = [
                        ("operation", operation.as_str().to_owned()),
                        ("outcome", outcome_label.as_str().to_owned()),
                        ("runtime_class", (*class).to_owned()),
                    ];
                    out.histogram(
                        "adx_platform_lifecycle_duration_seconds",
                        "Whole lifecycle operation duration; one sample per operation identity.",
                        &labels,
                        &B15,
                        &self.lifecycle_duration[operation.index()][outcome][index].snapshot(),
                    );
                }
                for (reason, reason_label) in REASON_CLASSES.iter().enumerate() {
                    let labels = [
                        ("operation", operation.as_str().to_owned()),
                        ("outcome", outcome_label.as_str().to_owned()),
                        ("reason_class", reason_label.as_str().to_owned()),
                    ];
                    let value = self.lifecycle_operations[operation.index()][outcome][reason]
                        .load(Ordering::Relaxed);
                    out.counter(
                        "adx_platform_lifecycle_operations_total",
                        "Lifecycle operation results, including the coarse reason class.",
                        &labels,
                        value,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_and_lifecycle_families_render_dense_baselines() {
        let metrics = CoordinatorMetrics::default();
        metrics.observe_queue_wait(
            QueueStage::Schedule,
            Outcome::Success,
            "runsc",
            Duration::from_millis(15),
        );
        metrics.observe_lifecycle(
            LifecycleOperation::Resume,
            Outcome::Success,
            ReasonClass::Other,
            "runsc",
            Duration::from_secs(2),
        );
        let mut text = Text::default();
        metrics.render(&mut text);
        let text = text.finish();
        assert!(text.starts_with("# HELP adx_platform_queue_wait_seconds"));
        assert!(text.contains(
            "adx_platform_queue_wait_seconds_bucket{stage=\"schedule\",outcome=\"success\",runtime_class=\"runsc\",le=\"+Inf\"} 1\n"
        ));
        // adxlet owns stage="node"; the Coordinator must not fabricate it.
        assert!(!text.contains("stage=\"node\""));
        assert!(text.contains(
            "adx_platform_lifecycle_duration_seconds_count{operation=\"resume\",outcome=\"success\",runtime_class=\"runsc\"} 1\n"
        ));
        assert!(text.contains(
            "adx_platform_lifecycle_operations_total{operation=\"resume\",outcome=\"success\",reason_class=\"other\"} 1\n"
        ));
        // Only produced dimensions: 1 stage × 3 outcomes × 4 classes,
        // 1 operation × 4 outcomes × 4 classes, 1 operation × 4 × 13 reasons.
        assert_eq!(
            text.matches("adx_platform_queue_wait_seconds_count{")
                .count(),
            12
        );
        assert_eq!(
            text.matches("adx_platform_lifecycle_duration_seconds_count{")
                .count(),
            16
        );
        assert_eq!(
            text.matches("adx_platform_lifecycle_operations_total{")
                .count(),
            52
        );
        assert!(!text.contains("operation=\"pause\""));
        assert!(!text.contains("operation=\"delete\""));
    }

    #[test]
    fn queue_wait_ignores_outcomes_the_coordinator_cannot_produce() {
        let metrics = CoordinatorMetrics::default();
        metrics.observe_queue_wait(
            QueueStage::Schedule,
            Outcome::Failure,
            "runsc",
            Duration::from_secs(1),
        );
        let mut text = Text::default();
        metrics.render(&mut text);
        assert!(!text.finish().contains(
            "adx_platform_queue_wait_seconds_count{stage=\"schedule\",outcome=\"failure\""
        ));
    }
}
