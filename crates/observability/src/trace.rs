//! W3C context and bounded OTLP export. Context is attached only while polling a future.
use opentelemetry::{
    global,
    propagation::TextMapPropagator,
    trace::{FutureExt, SpanKind, TraceContextExt, Tracer},
    Context, KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracerProvider},
    Resource,
};
use std::{collections::HashMap, future::Future, time::Duration};

pub struct Trace {
    context: Context,
}
impl Trace {
    fn start(name: &'static str, parent: Context, kind: SpanKind) -> Self {
        let span = global::tracer("adx")
            .span_builder(name)
            .with_kind(kind)
            .start_with_context(&global::tracer("adx"), &parent);
        Self {
            context: parent.with_span(span),
        }
    }
    pub fn remote(name: &'static str, parent: Option<&str>, state: Option<&str>) -> Self {
        let mut headers = HashMap::new();
        // Only W3C trace fields; never copy baggage, credentials, URLs or request bodies.
        if let Some(v) = parent.filter(|v| v.len() <= 512) {
            headers.insert("traceparent".into(), v.into());
        }
        if let Some(v) = state.filter(|v| v.len() <= 512) {
            headers.insert("tracestate".into(), v.into());
        }
        let context = TraceContextPropagator::new().extract_with_context(&Context::new(), &headers);
        Self::start(name, context, SpanKind::Server)
    }
    pub fn rpc<T>(name: &'static str, request: &tonic::Request<T>) -> Self {
        Self::remote(
            name,
            request
                .metadata()
                .get("traceparent")
                .and_then(|v| v.to_str().ok()),
            request
                .metadata()
                .get("tracestate")
                .and_then(|v| v.to_str().ok()),
        )
    }
    pub fn child(name: &'static str) -> Self {
        Self::start(name, Context::current(), SpanKind::Internal)
    }
    pub fn attribute(&self, key: &'static str, value: String) {
        self.context.span().set_attribute(KeyValue::new(key, value));
    }
    pub async fn run_result<T, E, F: Future<Output = Result<T, E>>>(
        self,
        future: F,
    ) -> Result<T, E> {
        self.run(async {
            let result = future.await;
            if result.is_err() {
                error();
            }
            result
        })
        .await
    }
    pub fn scope<T>(&self, f: impl FnOnce() -> T) -> T {
        let _guard = self.context.clone().attach();
        f()
    }
    pub async fn run<F: Future>(self, future: F) -> F::Output {
        future.with_context(self.context.clone()).await
    }
}
impl Drop for Trace {
    fn drop(&mut self) {
        self.context.span().end();
    }
}
pub fn traceparent() -> Option<String> {
    let mut headers = HashMap::new();
    TraceContextPropagator::new().inject_context(&Context::current(), &mut headers);
    headers.remove("traceparent")
}
pub fn inject<T>(value: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(value);
    inject_metadata(request.metadata_mut());
    request
}
pub fn inject_metadata(metadata: &mut tonic::metadata::MetadataMap) {
    let mut headers = HashMap::new();
    TraceContextPropagator::new().inject_context(&Context::current(), &mut headers);
    for key in ["traceparent", "tracestate"] {
        if let Some(value) = headers.get(key).and_then(|v| v.parse().ok()) {
            metadata.insert(key, value);
        }
    }
}
pub fn inject_headers(headers: &mut http::HeaderMap) {
    let mut values = HashMap::new();
    TraceContextPropagator::new().inject_context(&Context::current(), &mut values);
    for key in ["traceparent", "tracestate"] {
        if let Some(value) = values.get(key).and_then(|v| v.parse().ok()) {
            headers.insert(key, value);
        }
    }
}
pub fn attribute(key: &'static str, value: String) {
    Context::current()
        .span()
        .set_attribute(KeyValue::new(key, value));
}

pub fn error() {
    Context::current()
        .span()
        .set_status(opentelemetry::trace::Status::error("operation failed"));
}

pub struct TraceGuard(SdkTracerProvider);
impl Drop for TraceGuard {
    fn drop(&mut self) {
        if self
            .0
            .shutdown_with_timeout(Duration::from_secs(3))
            .is_err()
        {
            tracing::warn!("trace shutdown incomplete");
        }
    }
}
pub fn init(service: &str) -> Result<TraceGuard, Box<dyn std::error::Error + Send + Sync>> {
    let enabled = match std::env::var("ADX_TRACE_ENABLED")
        .as_deref()
        .unwrap_or("false")
    {
        "true" => true,
        "false" => false,
        _ => return Err("ADX_TRACE_ENABLED must be true or false".into()),
    };
    let ratio: f64 = std::env::var("ADX_TRACE_SAMPLE_RATIO")
        .unwrap_or_else(|_| "1".into())
        .parse()?;
    if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
        return Err("ADX_TRACE_SAMPLE_RATIO must be in [0,1]".into());
    }
    let sampler = if enabled {
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
    } else {
        Sampler::AlwaysOff
    };
    let mut builder = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_resource(
            Resource::builder_empty()
                .with_attributes([KeyValue::new("service.name", service.to_owned())])
                .build(),
        );
    if enabled {
        // The standard SDK batch processor bounds its queue (OTEL_BSP_MAX_QUEUE_SIZE).
        // Exporter runs on its worker thread, not on the Instance lifecycle task.
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_timeout(Duration::from_secs(2))
            .build()?;
        builder = builder.with_batch_exporter(CountedExporter(exporter));
    }
    let provider = builder.build();
    global::set_tracer_provider(provider.clone());
    Ok(TraceGuard(provider))
}

static EXPORTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static EXPORT_FAILED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[derive(Debug)]
struct CountedExporter(opentelemetry_otlp::SpanExporter);
impl opentelemetry_sdk::trace::SpanExporter for CountedExporter {
    async fn export(
        &self,
        spans: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        let count = spans.len() as u64;
        let result = self.0.export(spans).await;
        if result.is_ok() {
            EXPORTED.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        } else {
            EXPORT_FAILED.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.0.set_resource(resource);
    }
    fn shutdown_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }
}
pub fn metrics() -> String {
    format!("# TYPE adx_trace_exported_spans_total counter\nadx_trace_exported_spans_total {}\n# TYPE adx_trace_export_failed_spans_total counter\nadx_trace_export_failed_spans_total {}\n",EXPORTED.load(std::sync::atomic::Ordering::Relaxed),EXPORT_FAILED.load(std::sync::atomic::Ordering::Relaxed))
}
