use adx_observability::trace::{self, Trace};
use std::time::{Duration, Instant};
#[tokio::test]
async fn unavailable_exporter_is_bounded_and_sampling_can_disable_export() {
    // A bound non-listening TCP socket is not portable; reserve then release a local test port.
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/v1/traces", socket.local_addr().unwrap());
    drop(socket);
    std::env::set_var("ADX_TRACE_ENABLED", "false");
    let off = trace::init("sampling-test").unwrap();
    Trace::remote(
        "unsampled",
        Some("00-11111111111111111111111111111111-1111111111111111-01"),
        None,
    )
    .run(async {
        assert!(trace::traceparent().unwrap().ends_with("-00"));
    })
    .await;
    drop(off);
    std::env::set_var("ADX_TRACE_ENABLED", "true");
    std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", endpoint);
    std::env::set_var("OTEL_BSP_MAX_QUEUE_SIZE", "8");
    std::env::set_var("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "4");
    std::env::set_var("OTEL_BSP_SCHEDULE_DELAY", "10");
    let guard = trace::init("unavailable-test").unwrap();
    let started = Instant::now();
    for _ in 0..100 {
        Trace::child("bounded").run(async {}).await;
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "span submission blocked application"
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    let metrics = trace::metrics();
    let failed = metrics
        .lines()
        .find_map(|line| line.strip_prefix("adx_trace_export_failed_spans_total "))
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!(failed > 0, "export failure counter missing: {metrics}");
    let shutdown = Instant::now();
    drop(guard);
    assert!(shutdown.elapsed() < Duration::from_secs(5));
}
