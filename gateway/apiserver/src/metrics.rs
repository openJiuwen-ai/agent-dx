//! Bounded creation request telemetry on a private monitoring listener.
use crate::environment_service::EnvironmentService;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Instant;
use tonic::{Code, Status};

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
}
pub(crate) struct Attempt<'a> {
    metrics: &'a CreationMetrics,
    started: Instant,
    outcome: usize,
}
impl Attempt<'_> {
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
    }
}
fn unknown(error: &Status) -> bool {
    adx_protocol::deadline_failure(error).is_none()
        && adx_protocol::scheduling_failure(error).is_none()
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
        for (bound, count) in BOUNDS.iter().zip(&mut completed.buckets) {
            *count += u64::from(elapsed <= *bound);
        }
    }
}
impl CreationMetrics {
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
        output
    }
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
            attempt.finish(&result);
        }
        drop(metrics.attempt());
        let text = metrics.render();
        for outcome in ["success", "failure", "unknown", "cancelled"] {
            assert!(
                text.contains(&format!("outcome=\"{outcome}\"}} 1\n")),
                "{text}"
            );
        }
        assert!(text.contains("adx_environment_create_in_flight 0\n"));
        assert!(text.contains("adx_environment_create_duration_seconds_count 4\n"));
        assert!(!text.contains("environment_id"));
    }
}
