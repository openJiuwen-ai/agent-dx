use adx_observability::trace::{self, Trace};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

#[tokio::test]
async fn correlation_ids_seed_deterministic_traces_and_traceparent_wins() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());

    assert_eq!(trace::correlation_id(Some("t"), Some("r")), Some("t"));
    assert_eq!(trace::correlation_id(None, Some("r")), Some("r"));
    assert_eq!(trace::correlation_id(Some("  "), Some("r")), Some("r"));
    assert_eq!(trace::correlation_id(None, None), None);

    let derived = trace::derived_trace_id("e2e-invoke-0007");
    assert_eq!(derived.len(), 32);
    assert_eq!(derived, trace::derived_trace_id("e2e-invoke-0007"));
    assert_ne!(derived, trace::derived_trace_id("e2e-invoke-0008"));
    assert_ne!(
        trace::derived_trace_id("00000000000000000000000000000000"),
        "00000000000000000000000000000000"
    );

    let empty_parent = Trace::http_with_correlation(
        "POST",
        "/api/agent",
        Some("  "),
        None,
        Some("e2e-invoke-0007"),
    )
    .run(async { trace::trace_id() })
    .await
    .expect("derived trace id");
    assert_eq!(empty_parent, derived);

    let from_correlation =
        Trace::http_with_correlation("POST", "/api/agent", None, None, Some("e2e-invoke-0007"))
            .run(async { trace::trace_id() })
            .await
            .expect("derived trace id");
    assert_eq!(from_correlation, derived);

    let parent = "00-11111111111111111111111111111111-2222222222222222-01";
    let (trace_id, traceparent) = Trace::http_with_correlation(
        "POST",
        "/api/agent",
        Some(parent),
        None,
        Some("e2e-invoke-0007"),
    )
    .run(async { (trace::trace_id(), trace::traceparent()) })
    .await;
    assert_eq!(
        trace_id.as_deref(),
        Some("11111111111111111111111111111111")
    );
    assert!(traceparent
        .unwrap()
        .contains("11111111111111111111111111111111"));

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let derived_span = spans
        .iter()
        .find(|span| span.span_context.trace_id().to_string() == derived)
        .expect("derived span");
    assert_ne!(derived_span.parent_span_id.to_string(), "0000000000000000");

    let forwarded = spans
        .iter()
        .find(|span| span.span_context.trace_id().to_string() == "11111111111111111111111111111111")
        .expect("forwarded span");
    assert_eq!(forwarded.parent_span_id.to_string(), "2222222222222222");

    let headers = Trace::remote("handshake", Some(parent), None)
        .run(async { trace::propagation_headers() })
        .await;
    assert!(headers
        .iter()
        .any(|(key, value)| key == "traceparent"
            && value.contains("11111111111111111111111111111111")));
    assert!(trace::propagation_headers().is_empty());
}
