use adx_observability::trace::{http_route, Trace};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

#[tokio::test]
async fn http_interface_names_keep_method_route_parent_and_hide_user_input() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    let parent = "00-11111111111111111111111111111111-2222222222222222-01";
    for (path, expected) in [
        (
            "/api/environment/v1/environments",
            "/api/environment/v1/environments",
        ),
        (
            "/api/environment/v1/environments/private-id/pause?token=secret",
            "/api/environment/v1/environments/{id}/pause",
        ),
        (
            "/api/environment/private-id/exec/invoke",
            "/api/environment/{id}/exec/invoke",
        ),
        (
            "/api/environment/v1/snapshots/private-snapshot",
            "/api/environment/v1/snapshots/{id}",
        ),
        ("/api/instances?instance_id=private-id", "/api/instances"),
        ("/invoke?path=private-file", "/invoke"),
        ("/download?path=private-file", "/download"),
        ("/global-scheduler/resources", "/global-scheduler/resources"),
        ("/api/admin/v1/keys/private-key", "/api/admin/v1/keys/{id}"),
        ("/unknown/private-id", "/unmatched"),
    ] {
        assert_eq!(http_route(path), expected);
        Trace::http("POST", http_route(path), Some(parent), None)
            .run(async {})
            .await;
    }
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 10);
    for span in spans {
        assert!(span.name.starts_with("POST /"));
        assert!(!span.name.contains("private") && !span.name.contains("secret"));
        assert_eq!(span.parent_span_id.to_string(), "2222222222222222");
        assert_eq!(
            span.span_context.trace_id().to_string(),
            "11111111111111111111111111111111"
        );
        assert!(span
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "http.request.method" && a.value.to_string() == "POST"));
        assert!(span
            .attributes
            .iter()
            .any(|a| a.key.as_str() == "http.route"
                && a.value.to_string() == span.name.trim_start_matches("POST ")));
    }
}
