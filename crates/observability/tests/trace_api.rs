use adx_observability::trace::{self, Trace};
use opentelemetry::trace::{SpanKind, Status};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

#[tokio::test]
async fn spans_support_events_errors_bounded_attributes_and_client_kind() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());

    let oversized = "x".repeat(4_000);
    Trace::client("outbound")
        .run(async {
            let trace = Trace::child("operation");
            trace.integer_attribute("retry.count", 3);
            trace.boolean_attribute("cache.hit", false);
            trace.attribute("error.message", oversized);
            trace.add_event("checkpoint", vec![("stage", "started".to_owned())]);
            trace.record_error("RuntimeError", "boom");
            assert!(trace.is_recording());
            assert_eq!(trace.trace_id().map(|value| value.len()), Some(32));
            assert_eq!(trace.span_id().map(|value| value.len()), Some(16));
            trace.end();
        })
        .await;

    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let client = spans
        .iter()
        .find(|span| span.name == "outbound")
        .expect("client span");
    assert_eq!(client.span_kind, SpanKind::Client);

    let operation = spans
        .iter()
        .find(|span| span.name == "operation")
        .expect("operation span");
    assert_eq!(operation.parent_span_id, client.span_context.span_id());
    assert_eq!(
        operation.span_context.trace_id(),
        client.span_context.trace_id()
    );
    assert!(operation
        .attributes
        .iter()
        .any(|attribute| attribute.key.as_str() == "retry.count"
            && attribute.value.to_string() == "3"));
    assert!(operation
        .attributes
        .iter()
        .any(|attribute| attribute.key.as_str() == "cache.hit"
            && attribute.value.to_string() == "false"));

    let message = operation
        .attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == "error.message")
        .map(|attribute| attribute.value.to_string())
        .expect("bounded attribute");
    assert!(message.len() <= trace::MAX_ATTRIBUTE_BYTES + 3);
    assert!(message.contains("..."));

    assert!(operation
        .events
        .iter()
        .any(|event| event.name == "checkpoint"));
    let exception = operation
        .events
        .iter()
        .find(|event| event.name == "exception")
        .expect("exception event");
    assert!(exception.attributes.iter().any(|attribute| {
        attribute.key.as_str() == "exception.type" && attribute.value.to_string() == "RuntimeError"
    }));
    assert!(matches!(&operation.status, Status::Error { .. }));
}
