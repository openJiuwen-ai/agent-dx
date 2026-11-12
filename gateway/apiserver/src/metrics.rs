//! Bounded creation request telemetry on a private monitoring listener.
use crate::environment_service::EnvironmentService;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Instant;
use tonic::{Code, Status};

// These are the bounded outputs of classify/rejection_reason, never raw errors.
const CREATE_REASONS: &[&str] = &[
    "capacity",
    "constraint",
    "dependency",
    "timeout",
    "probe_failed",
    "runtime_error",
    "conflict",
    "transport",
    "caller_cancelled",
    "unknown",
];
const REJECTION_REASONS: &[&str] = &[
    "invalid_argument",
    "unauthenticated",
    "permission_denied",
    "conflict",
    "dependency_unavailable",
    "unknown",
];

const BOUNDS: [f64; 12] = [
    0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0, 3600.0,
];

#[derive(Default)]
pub(crate) struct CreationMetrics {
    http: AtomicU64,
    embedded: AtomicU64,
    in_flight: AtomicU64,
    completed: Mutex<Completed>,
}
#[derive(Default)]
struct Completed {
    outcomes: [u64; 4],
    buckets: [u64; 12],
    count: u64,
    seconds: f64,
    successful_creates: Histogram,
    errors: BTreeMap<(&'static str, &'static str, &'static str), u64>,
    rejections: BTreeMap<&'static str, u64>,
    workspace_affinity_fallbacks: u64,
}
#[derive(Default)]
struct Histogram {
    buckets: [u64; 12],
    count: u64,
    seconds: f64,
}
pub(crate) struct Rejection<'a> {
    metrics: &'a CreationMetrics,
    reason: &'static str,
    active: bool,
}
impl Rejection<'_> {
    pub fn accepted(&mut self) {
        self.active = false;
    }
    pub fn status(&mut self, error: &Status) {
        self.reason = rejection_reason(error);
    }
}
impl Drop for Rejection<'_> {
    fn drop(&mut self) {
        if self.active {
            *self
                .metrics
                .completed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .rejections
                .entry(self.reason)
                .or_default() += 1;
        }
    }
}
fn rejection_reason(error: &Status) -> &'static str {
    match error.code() {
        Code::InvalidArgument => "invalid_argument",
        Code::Unauthenticated => "unauthenticated",
        Code::PermissionDenied => "permission_denied",
        Code::AlreadyExists | Code::FailedPrecondition => "conflict",
        Code::Unavailable | Code::DeadlineExceeded | Code::Unimplemented => {
            "dependency_unavailable"
        }
        _ => "unknown",
    }
}
pub(crate) struct Attempt<'a> {
    metrics: &'a CreationMetrics,
    started: Instant,
    outcome: usize,
    classification: (&'static str, &'static str),
    successful_create: Option<std::time::Duration>,
}
impl Attempt<'_> {
    pub fn successful_create(&mut self, elapsed: std::time::Duration) {
        self.successful_create = Some(elapsed);
    }
    pub fn elapsed(&self) -> std::time::Duration {
        self.started.elapsed()
    }
    pub fn outcome(&self) -> &'static str {
        ["success", "failure", "unknown", "cancelled"][self.outcome]
    }
    pub fn finish<T>(&mut self, result: &Result<T, Status>) {
        self.outcome = match result {
            Ok(_) => 0,
            Err(error) if unknown(error) => 2,
            Err(_) => 1,
        };
        if let Err(error) = result {
            self.classification = classify(error);
        }
    }
}
pub(crate) fn unknown(error: &Status) -> bool {
    if error
        .metadata()
        .get("adx-create-terminal")
        .is_some_and(|value| value == "true")
    {
        return false;
    }
    adx_protocol::deadline_failure(error).is_none()
        && adx_protocol::scheduling_failure(error).is_none()
        && serde_json::from_slice::<adx_core::probes::ProbeFailure>(error.details()).is_err()
        && matches!(
            error.code(),
            Code::Cancelled
                | Code::Unknown
                | Code::Unavailable
                | Code::DeadlineExceeded
                | Code::Internal
        )
}
impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        self.metrics.in_flight.fetch_sub(1, Ordering::Relaxed);
        let elapsed = self.started.elapsed().as_secs_f64();
        let mut completed = self
            .metrics
            .completed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        completed.outcomes[self.outcome] += 1;
        completed.count += 1;
        completed.seconds += elapsed;
        if self.outcome != 0 {
            *completed
                .errors
                .entry((self.outcome(), self.classification.0, self.classification.1))
                .or_default() += 1;
        }
        for (bound, count) in BOUNDS.iter().zip(&mut completed.buckets) {
            *count += u64::from(elapsed <= *bound);
        }
        if let Some(duration) = self.successful_create.filter(|_| self.outcome == 0) {
            let histogram = &mut completed.successful_creates;
            let seconds = duration.as_secs_f64();
            histogram.count += 1;
            histogram.seconds += seconds;
            for (bound, count) in BOUNDS.iter().zip(&mut histogram.buckets) {
                *count += u64::from(seconds <= *bound);
            }
        }
    }
}
impl CreationMetrics {
    pub fn rejection_guard(&self, active: bool) -> Rejection<'_> {
        Rejection {
            metrics: self,
            reason: "invalid_argument",
            active,
        }
    }
    pub fn rejection(&self, error: &Status) {
        let reason = rejection_reason(error);
        *self
            .completed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rejections
            .entry(reason)
            .or_default() += 1;
    }
    /// A Workspace ownership lookup was unavailable and scheduling proceeded
    /// without affinity (`workspace_affinity.on_error = fail_open`).
    pub fn workspace_affinity_fallback(&self) {
        self.completed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .workspace_affinity_fallbacks += 1;
    }
    pub fn http_request(&self) {
        self.http.fetch_add(1, Ordering::Relaxed);
    }
    pub fn embedded_request(&self) {
        self.embedded.fetch_add(1, Ordering::Relaxed);
    }
    pub fn attempt(&self) -> Attempt<'_> {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Attempt {
            metrics: self,
            started: Instant::now(),
            outcome: 3,
            classification: ("unknown", "unknown"),
            successful_create: None,
        }
    }
    pub fn render(&self) -> String {
        let completed = self
            .completed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut output = format!(
            "# TYPE adx_environment_create_requests_total counter\nadx_environment_create_requests_total{{source=\"http\"}} {}\nadx_environment_create_requests_total{{source=\"embedded\"}} {}\n# TYPE adx_environment_create_in_flight gauge\nadx_environment_create_in_flight {}\n# TYPE adx_environment_create_responses_total counter\n",
            self.http.load(Ordering::Relaxed), self.embedded.load(Ordering::Relaxed), self.in_flight.load(Ordering::Relaxed));
        for (outcome, count) in ["success", "failure", "unknown", "cancelled"]
            .iter()
            .zip(completed.outcomes)
        {
            output.push_str(&format!(
                "adx_environment_create_responses_total{{outcome=\"{outcome}\"}} {count}\n"
            ));
        }
        output.push_str("# TYPE adx_environment_create_duration_seconds histogram\n");
        for (bound, count) in BOUNDS.iter().zip(completed.buckets) {
            output.push_str(&format!(
                "adx_environment_create_duration_seconds_bucket{{le=\"{bound}\"}} {count}\n"
            ));
        }
        output.push_str(&format!("adx_environment_create_duration_seconds_bucket{{le=\"+Inf\"}} {}\nadx_environment_create_duration_seconds_count {}\nadx_environment_create_duration_seconds_sum {}\n", completed.count, completed.count, completed.seconds));
        let name = "adx_environment_create_success_call_duration_seconds";
        let histogram = &completed.successful_creates;
        output.push_str(&format!("# TYPE {name} histogram\n"));
        for (bound, count) in BOUNDS.iter().zip(histogram.buckets) {
            output.push_str(&format!("{name}_bucket{{le=\"{bound}\"}} {count}\n"));
        }
        output.push_str(&format!(
            "{name}_bucket{{le=\"+Inf\"}} {}\n{name}_count {}\n{name}_sum {}\n",
            histogram.count, histogram.count, histogram.seconds
        ));
        output.push_str("# TYPE adx_environment_create_errors_total counter\n");
        // Pre-export the classifier's finite domain so the first event has a
        // prior zero scrape. Cancellation before finish has one fixed tuple.
        for outcome in ["failure", "unknown"] {
            for stage in adx_core::observation::CreateStage::ALL {
                for reason in CREATE_REASONS {
                    render_error(&mut output, &completed, outcome, stage.as_str(), reason);
                }
            }
        }
        render_error(&mut output, &completed, "cancelled", "unknown", "unknown");
        output.push_str("# TYPE adx_environment_create_rejections_total counter\n");
        for reason in REJECTION_REASONS {
            let count = completed
                .rejections
                .get(reason)
                .copied()
                .unwrap_or_default();
            output.push_str(&format!(
                "adx_environment_create_rejections_total{{reason=\"{reason}\"}} {count}\n"
            ));
        }
        output.push_str(&format!(
            "# TYPE adx_environment_create_workspace_affinity_fallbacks_total counter\nadx_environment_create_workspace_affinity_fallbacks_total {}\n",
            completed.workspace_affinity_fallbacks
        ));
        output
    }
}

fn render_error(
    output: &mut String,
    completed: &Completed,
    outcome: &'static str,
    stage: &'static str,
    reason: &'static str,
) {
    let count = completed
        .errors
        .get(&(outcome, stage, reason))
        .copied()
        .unwrap_or_default();
    output.push_str(&format!("adx_environment_create_errors_total{{outcome=\"{outcome}\",stage=\"{stage}\",reason=\"{reason}\"}} {count}\n"));
}

fn classify(error: &Status) -> (&'static str, &'static str) {
    let stage = error
        .metadata()
        .get("adx-create-stage")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| match v {
            "scheduling" => Some("scheduling"),
            "runtime_prepare" => Some("runtime_prepare"),
            "runtime_start" => Some("runtime_start"),
            "readiness" => Some("readiness"),
            "route_activation" => Some("route_activation"),
            "state_publish" => Some("state_publish"),
            _ => None,
        });
    if adx_protocol::scheduling_failure(error).is_some() {
        return ("scheduling", "timeout");
    }
    if adx_protocol::deadline_failure(error).is_some() {
        return (stage.unwrap_or("unknown"), "timeout");
    }
    if serde_json::from_slice::<adx_core::probes::ProbeFailure>(error.details()).is_ok() {
        return ("readiness", "probe_failed");
    }
    let producer_reason = error
        .metadata()
        .get("adx-create-reason")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| match value {
            "capacity" => Some("capacity"),
            "constraint" => Some("constraint"),
            "dependency" => Some("dependency"),
            "timeout" => Some("timeout"),
            "probe_failed" => Some("probe_failed"),
            "runtime_error" => Some("runtime_error"),
            "conflict" => Some("conflict"),
            "transport" => Some("transport"),
            "caller_cancelled" => Some("caller_cancelled"),
            _ => None,
        });
    if let Some(reason) = producer_reason {
        return (stage.unwrap_or("unknown"), reason);
    }
    let reason = match error.code() {
        Code::AlreadyExists | Code::FailedPrecondition => "conflict",
        Code::ResourceExhausted => "capacity",
        Code::Unavailable | Code::DeadlineExceeded | Code::Unknown => "transport",
        _ => "unknown",
    };
    (stage.unwrap_or("unknown"), reason)
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    service: Arc<EnvironmentService>,
) -> std::io::Result<()> {
    use bytes::Bytes;
    use http_body_util::Full;
    loop {
        let (stream, _) = listener.accept().await?;
        let service = service.clone();
        tokio::spawn(async move {
            let handler = hyper::service::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let valid = request.method() == hyper::Method::GET
                        && request.uri().path() == "/metrics";
                    let mut response = hyper::Response::new(Full::new(Bytes::from(if valid {
                        service.metrics()
                    } else {
                        String::new()
                    })));
                    *response.status_mut() = if valid {
                        hyper::StatusCode::OK
                    } else {
                        hyper::StatusCode::NOT_FOUND
                    };
                    response.headers_mut().insert(
                        hyper::header::CONTENT_TYPE,
                        hyper::header::HeaderValue::from_static("text/plain; version=0.0.4"),
                    );
                    async move { Ok::<_, std::convert::Infallible>(response) }
                },
            );
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), handler);
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), connection).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostics_have_zero_baselines_before_the_first_event() {
        let metrics = CreationMetrics::default();
        let error_series = "adx_environment_create_errors_total{outcome=\"unknown\",stage=\"unknown\",reason=\"transport\"}";
        let rejection_series =
            "adx_environment_create_rejections_total{reason=\"unauthenticated\"}";
        let before = metrics.render();
        assert!(before.contains(&format!("{error_series} 0\n")), "{before}");
        assert!(
            before.contains(&format!("{rejection_series} 0\n")),
            "{before}"
        );
        {
            let mut attempt = metrics.attempt();
            attempt.finish::<()>(&Err(Status::unavailable("lost response")));
        }
        metrics.rejection(&Status::unauthenticated("missing credential"));
        let after = metrics.render();
        assert!(after.contains(&format!("{error_series} 1\n")));
        assert!(after.contains(&format!("{rejection_series} 1\n")));
        assert_eq!(before.lines().count(), after.lines().count());
        if let Some(directory) = std::env::var_os("ADX_TEST_METRIC_SAMPLES") {
            let directory = std::path::PathBuf::from(directory);
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(directory.join("apiserver-before.prom"), &before).unwrap();
            std::fs::write(directory.join("apiserver-after.prom"), &after).unwrap();
        }
        assert!(!before.contains("outcome=\"success\",stage="));
    }

    #[test]
    fn workspace_affinity_fallbacks_are_counted() {
        let metrics = CreationMetrics::default();
        let series = "adx_environment_create_workspace_affinity_fallbacks_total";
        assert!(metrics.render().contains(&format!("{series} 0\n")));
        metrics.workspace_affinity_fallback();
        assert!(metrics.render().contains(&format!("{series} 1\n")));
    }

    #[test]
    fn bounded_provenance_preserves_unknown_transport_and_confirmed_terminal() {
        let mut error = Status::unavailable("raw backend detail not a label");
        error
            .metadata_mut()
            .insert("adx-create-stage", "runtime_start".parse().unwrap());
        error
            .metadata_mut()
            .insert("adx-create-reason", "runtime_error".parse().unwrap());
        assert!(unknown(&error));
        assert_eq!(classify(&error), ("runtime_start", "runtime_error"));
        error
            .metadata_mut()
            .insert("adx-create-terminal", "true".parse().unwrap());
        assert!(!unknown(&error));
        error
            .metadata_mut()
            .insert("adx-create-stage", "arbitrary-runtime-id".parse().unwrap());
        error
            .metadata_mut()
            .insert("adx-create-reason", "raw-oom-stacktrace".parse().unwrap());
        assert_eq!(classify(&error), ("unknown", "transport"));
    }

    #[test]
    fn results_distinguish_unknown_from_confirmed_failure_and_cancellation() {
        let metrics = CreationMetrics::default();
        metrics.http_request();
        metrics.embedded_request();
        for result in [
            Ok(()),
            Err(Status::unavailable("lost response")),
            Err(Status::invalid_argument("invalid")),
        ] {
            let mut attempt = metrics.attempt();
            attempt.successful_create(std::time::Duration::from_secs(1));
            if result.is_ok() {
                assert!(metrics
                    .render()
                    .contains("adx_environment_create_success_call_duration_seconds_count 0\n"));
            }
            attempt.finish(&result);
        }
        let mut cancelled = metrics.attempt();
        cancelled.successful_create(std::time::Duration::from_secs(1));
        drop(cancelled);
        let text = metrics.render();
        for outcome in ["success", "failure", "unknown", "cancelled"] {
            assert!(
                text.contains(&format!("outcome=\"{outcome}\"}} 1\n")),
                "{text}"
            );
        }
        assert!(text.contains("adx_environment_create_in_flight 0\n"));
        assert!(text.contains("adx_environment_create_duration_seconds_count 4\n"));
        assert!(text.contains("adx_environment_create_success_call_duration_seconds_count 1\n"));
        assert!(text.contains("adx_environment_create_success_call_duration_seconds_sum 1\n"));
        assert!(!text.contains("environment_id"));
    }
}
