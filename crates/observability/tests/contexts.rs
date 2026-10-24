use adx_observability::trace::{self, Trace};
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

#[tokio::test]
async fn queued_and_detached_work_keep_their_own_parents_and_end_on_cancel() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    let a = "00-11111111111111111111111111111111-1111111111111111-01";
    let b = "00-22222222222222222222222222222222-2222222222222222-01";
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    for parent in [a, b] {
        Trace::remote("request", Some(parent), None)
            .run(async {
                tx.send(Trace::child("queued_operation")).await.unwrap();
            })
            .await;
    }
    drop(tx);
    let mut tasks = Vec::new();
    while let Some(span) = rx.recv().await {
        tasks.push(tokio::spawn(span.run(async {
            tokio::task::yield_now().await;
            let before = trace::traceparent().unwrap();
            tokio::task::yield_now().await;
            assert_eq!(before, trace::traceparent().unwrap());
            Trace::child("execute")
                .run(async { trace::traceparent().unwrap() })
                .await
        })));
    }
    assert!(tasks.remove(0).await.unwrap().contains(&a[3..35]));
    assert!(tasks.remove(0).await.unwrap().contains(&b[3..35]));
    let cancelled = Trace::remote("cancelled", Some(a), None).run(std::future::pending::<()>());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(5), cancelled)
            .await
            .is_err()
    );
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 7);
    for span in spans.iter().filter(|s| s.name == "execute") {
        let parent = spans
            .iter()
            .find(|p| p.span_context.span_id() == span.parent_span_id)
            .unwrap();
        assert_eq!(parent.name, "queued_operation");
        assert_eq!(parent.span_context.trace_id(), span.span_context.trace_id());
    }
    assert!(
        trace::traceparent().is_none(),
        "no context leaked into caller task"
    );
    let parent = Trace::remote("captured_parent", Some(a), None);
    let detached = parent.scope(|| {
        trace::with_current_context(async {
            Trace::child("captured_child")
                .run(async { trace::traceparent().unwrap() })
                .await
        })
    });
    drop(parent);
    assert!(tokio::spawn(detached).await.unwrap().contains(&a[3..35]));
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let parent = spans.iter().find(|s| s.name == "captured_parent").unwrap();
    let child = spans.iter().find(|s| s.name == "captured_child").unwrap();
    assert_eq!(child.parent_span_id, parent.span_context.span_id());
    assert!(parent.end_time <= child.start_time);
    assert!(trace::traceparent().is_none());
    drop(provider.tracer("unused"));
}
