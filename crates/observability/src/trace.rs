//! W3C context and bounded OTLP export. Context is attached only while polling a future.
use opentelemetry::{
    global,
    propagation::TextMapPropagator,
    trace::{
        FutureExt, SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags, TraceId,
        TraceState, Tracer,
    },
    Context, KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracerProvider},
    Resource,
};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, future::Future, time::Duration};

/// Attribute values are bounded so request bodies or user paths cannot inflate spans.
pub const MAX_ATTRIBUTE_BYTES: usize = 1024;

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index < value.len() && !value.is_char_boundary(index) {
        index += 1;
    }
    index
}

/// Keep the head and tail of oversized values, on UTF-8 boundaries.
fn bounded_attribute(value: String) -> String {
    if value.len() <= MAX_ATTRIBUTE_BYTES {
        return value;
    }
    let head_end = floor_char_boundary(&value, MAX_ATTRIBUTE_BYTES / 2);
    let tail_start = ceil_char_boundary(&value, value.len() - (MAX_ATTRIBUTE_BYTES / 2));
    format!("{}...{}", &value[..head_end], &value[tail_start..])
}

/// Pick the application correlation id that seeds a trace when no W3C
/// traceparent is present. X-Trace-Id wins over X-Request-Id.
pub fn correlation_id<'a>(
    trace_id: Option<&'a str>,
    request_id: Option<&'a str>,
) -> Option<&'a str> {
    [trace_id, request_id]
        .into_iter()
        .flatten()
        .find(|value| !value.trim().is_empty())
}

fn extract_context(parent: Option<&str>, state: Option<&str>) -> Context {
    let mut headers = HashMap::new();
    // Only W3C trace fields; never copy baggage, credentials, URLs or request bodies.
    if let Some(value) = parent.filter(|value| value.len() <= 512) {
        headers.insert("traceparent".into(), value.into());
    }
    if let Some(value) = state.filter(|value| value.len() <= 512) {
        headers.insert("tracestate".into(), value.into());
    }
    TraceContextPropagator::new().extract_with_context(&Context::new(), &headers)
}

fn derived_ids(correlation: &str) -> (TraceId, SpanId) {
    let digest = Sha256::digest(correlation.as_bytes());
    let trace_id = TraceId::from_hex(correlation)
        .ok()
        .filter(|trace_id| *trace_id != TraceId::INVALID)
        .unwrap_or_else(|| {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&digest[..16]);
            TraceId::from_bytes(bytes)
        });
    let mut span_bytes = [0u8; 8];
    span_bytes.copy_from_slice(&digest[16..24]);
    (trace_id, SpanId::from_bytes(span_bytes))
}

fn derived_context(correlation: &str) -> Context {
    let (trace_id, span_id) = derived_ids(correlation);
    // Mark the synthetic parent sampled so ParentBased sampling records the
    // request. A disabled provider still drops it through AlwaysOff.
    Context::new().with_remote_span_context(SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ))
}

/// Deterministic 32-hex trace id for a caller-supplied correlation id.
pub fn derived_trace_id(correlation: &str) -> String {
    derived_ids(correlation).0.to_string()
}

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
        Self::start(name, extract_context(parent, state), SpanKind::Server)
    }
    /// Root server span whose trace id is derived from an application
    /// correlation id. Used when the caller only sends X-Trace-Id/X-Request-Id.
    pub fn remote_derived(name: &'static str, correlation: &str) -> Self {
        Self::start(name, derived_context(correlation), SpanKind::Server)
    }
    /// Name HTTP spans by their bounded route template, never a concrete URL.
    pub fn http(
        method: &str,
        route: &'static str,
        parent: Option<&str>,
        state: Option<&str>,
    ) -> Self {
        Self::remote("http.request", parent, state).finish_http(method, route)
    }
    /// HTTP entry span that falls back to a deterministic trace id when only an
    /// application correlation id is available. A W3C traceparent always wins.
    pub fn http_with_correlation(
        method: &str,
        route: &'static str,
        parent: Option<&str>,
        state: Option<&str>,
        correlation: Option<&str>,
    ) -> Self {
        let parent = parent.filter(|value| !value.trim().is_empty());
        let trace = match (parent, correlation) {
            (Some(parent), _) => Self::remote("http.request", Some(parent), state),
            (None, Some(correlation)) => Self::remote_derived("http.request", correlation),
            (None, None) => Self::remote("http.request", None, state),
        };
        trace.finish_http(method, route)
    }
    fn finish_http(self, method: &str, route: &'static str) -> Self {
        let method = match method {
            "GET" | "POST" | "PUT" | "DELETE" | "PATCH" | "HEAD" | "OPTIONS" | "CONNECT" => method,
            _ => "OTHER",
        };
        self.context.span().update_name(format!("{method} {route}"));
        self.attribute(crate::semconv::HTTP_REQUEST_METHOD, method.to_owned());
        self.attribute(crate::semconv::HTTP_ROUTE, route.to_owned());
        self
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
    /// Client-side span that continues the current trace.
    pub fn client(name: &'static str) -> Self {
        Self::start(name, Context::current(), SpanKind::Client)
    }
    pub fn attribute(&self, key: &'static str, value: String) {
        self.context
            .span()
            .set_attribute(KeyValue::new(key, bounded_attribute(value)));
    }
    pub fn integer_attribute(&self, key: &'static str, value: i64) {
        self.context.span().set_attribute(KeyValue::new(key, value));
    }
    pub fn boolean_attribute(&self, key: &'static str, value: bool) {
        self.context.span().set_attribute(KeyValue::new(key, value));
    }
    pub fn add_event(&self, name: &'static str, attributes: Vec<(&'static str, String)>) {
        let attributes = attributes
            .into_iter()
            .map(|(key, value)| KeyValue::new(key, bounded_attribute(value)))
            .collect();
        self.context.span().add_event(name, attributes);
    }
    /// Mark this span as failed without adding an exception event.
    pub fn error_with(&self, message: impl Into<String>) {
        self.context
            .span()
            .set_status(Status::error(bounded_attribute(message.into())));
    }
    /// Mark the span as failed and record a bounded `exception` event.
    pub fn record_error(&self, kind: &'static str, message: impl Into<String>) {
        let message = bounded_attribute(message.into());
        let span = self.context.span();
        span.set_status(Status::error(message.clone()));
        span.add_event(
            "exception",
            vec![
                KeyValue::new("exception.type", kind.to_owned()),
                KeyValue::new("exception.message", message),
            ],
        );
    }
    /// True when the sampler records and exports this span.
    pub fn is_recording(&self) -> bool {
        self.context.span().is_recording()
    }
    pub fn trace_id(&self) -> Option<String> {
        let span_context = self.context.span().span_context().clone();
        span_context
            .is_valid()
            .then(|| span_context.trace_id().to_string())
    }
    pub fn span_id(&self) -> Option<String> {
        let span_context = self.context.span().span_context().clone();
        span_context
            .is_valid()
            .then(|| span_context.span_id().to_string())
    }
    /// Explicit end for callers that own a span beyond a single scope.
    /// OTel `end` is idempotent, so the later `Drop` is harmless.
    pub fn end(self) {
        self.context.span().end();
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
    /// Runs a fallible future under this span without ending it, so callers can
    /// still write terminal attributes (and a guard can still run) before the
    /// span is dropped.
    pub async fn scope_result<T, E, F: Future<Output = Result<T, E>>>(
        &self,
        future: F,
    ) -> Result<T, E> {
        let result = future.with_context(self.context.clone()).await;
        if result.is_err() {
            self.context
                .span()
                .set_status(Status::error("operation failed"));
        }
        result
    }
    /// An attribute that is written exactly once: `finish` records the actual
    /// value, while `Drop` records the fallback if the operation is cancelled
    /// or panics before finishing.
    pub fn attribute_guard(
        &self,
        key: &'static str,
        fallback: &'static str,
    ) -> AttributeGuard {
        AttributeGuard {
            context: self.context.clone(),
            key,
            fallback,
            done: false,
        }
    }
    pub fn scope<T>(&self, f: impl FnOnce() -> T) -> T {
        let _guard = self.context.clone().attach();
        f()
    }
    /// Runs an arbitrary future under this span without ending it.
    pub async fn scope_future<F: Future>(&self, future: F) -> F::Output {
        future.with_context(self.context.clone()).await
    }
    pub async fn run<F: Future>(self, future: F) -> F::Output {
        future.with_context(self.context.clone()).await
    }
}

/// Single-write attribute guard used for terminal outcome attributes. OTel
/// `set_attribute` appends instead of replacing on the current SDK, so writing
/// a seed value and later overwriting it produces duplicate keys.
pub struct AttributeGuard {
    context: Context,
    key: &'static str,
    fallback: &'static str,
    done: bool,
}
impl AttributeGuard {
    /// Writes the actual value and disarms the fallback. Call at most once.
    pub fn finish(&mut self, value: &str) {
        if self.done {
            return;
        }
        self.done = true;
        self.context
            .span()
            .set_attribute(KeyValue::new(self.key, bounded_attribute(value.to_owned())));
    }
}
impl Drop for AttributeGuard {
    fn drop(&mut self) {
        if !self.done {
            self.done = true;
            self.context.span().set_attribute(KeyValue::new(
                self.key,
                bounded_attribute(self.fallback.to_owned()),
            ));
        }
    }
}
impl Drop for Trace {
    fn drop(&mut self) {
        self.context.span().end();
    }
}
/// Capture the current context now and attach it only while polling the future.
/// Detached work can retain its parent without keeping the parent's span open.
pub fn with_current_context<F: Future>(future: F) -> impl Future<Output = F::Output> {
    future.with_context(Context::current())
}
/// Capture the current context for work that leaves the async task, such as a
/// blocking thread pool. Attach it with [`attach_context`] inside that work so
/// logs and child spans keep the request trace.
pub fn current_context() -> Context {
    Context::current()
}
/// Run synchronous work with a previously captured context attached. This is
/// the blocking counterpart of [`with_current_context`].
pub fn attach_context<T>(context: &Context, f: impl FnOnce() -> T) -> T {
    let _guard = context.clone().attach();
    f()
}
/// Trace id of the span currently attached to this task, if any.
pub fn trace_id() -> Option<String> {
    let context = Context::current();
    let span_context = context.span().span_context().clone();
    span_context
        .is_valid()
        .then(|| span_context.trace_id().to_string())
}
/// Trace and span ids for log correlation. Non-recording spans keep valid ids,
/// so request logs remain correlatable when export is disabled.
pub fn log_context() -> (String, String) {
    let context = Context::current();
    let span_context = context.span().span_context().clone();
    if span_context.is_valid() {
        (
            span_context.trace_id().to_string(),
            span_context.span_id().to_string(),
        )
    } else {
        (String::new(), String::new())
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
/// W3C propagation fields for protocols that carry key/value metadata
/// (WebSocket handshakes, JSON tunnel headers). Empty when no span is active.
pub fn propagation_headers() -> Vec<(String, String)> {
    let mut values = HashMap::new();
    TraceContextPropagator::new().inject_context(&Context::current(), &mut values);
    values.into_iter().collect()
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
        .set_attribute(KeyValue::new(key, bounded_attribute(value)));
}

pub fn error() {
    error_with("operation failed");
}

/// Mark the current span as failed with a bounded description.
pub fn error_with(message: impl Into<String>) {
    Context::current()
        .span()
        .set_status(Status::error(bounded_attribute(message.into())));
}

/// Mark the current span as failed and record a bounded `exception` event.
pub fn record_error(kind: &'static str, message: impl Into<String>) {
    let message = bounded_attribute(message.into());
    let context = Context::current();
    let span = context.span();
    span.set_status(Status::error(message.clone()));
    span.add_event(
        "exception",
        vec![
            KeyValue::new("exception.type", kind.to_owned()),
            KeyValue::new("exception.message", message),
        ],
    );
}

/// End the span currently attached to this task. Used when a span covers only
/// an establishment phase and the remaining work runs outside it.
pub fn end_current() {
    Context::current().span().end();
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
fn optional_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn resolved_service_name(service: &str) -> String {
    optional_env("ADX_SERVICE_NAME").unwrap_or_else(|| service.to_owned())
}

fn resolved_instance_id() -> String {
    optional_env("ADX_SERVICE_INSTANCE_ID").unwrap_or_else(|| {
        let host = optional_env("HOSTNAME").unwrap_or_else(|| "adx".to_owned());
        format!("{host}-{}", std::process::id())
    })
}

fn resource_attributes(
    service: &str,
    instance: &str,
    node: Option<&str>,
    environment: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut attributes = vec![
        ("service.name", service.to_owned()),
        ("service.instance.id", instance.to_owned()),
    ];
    if let Some(node) = node {
        attributes.push(("node.id", node.to_owned()));
    }
    if let Some(environment) = environment {
        attributes.push(("adx_env", environment.to_owned()));
    }
    attributes
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
    let service_name = resolved_service_name(service);
    let instance_id = resolved_instance_id();
    let node_id = optional_env("ADX_NODE_ID");
    let environment = optional_env("ADX_ENV");
    let resource = Resource::builder_empty()
        .with_attributes(
            resource_attributes(
                &service_name,
                &instance_id,
                node_id.as_deref(),
                environment.as_deref(),
            )
            .into_iter()
            .map(|(key, value)| KeyValue::new(key, value)),
        )
        .build();
    let mut builder = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_resource(resource);
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

/// Bounded HTTP route names shared by the public API and runtime forwarding.
/// Unknown paths and user-selected ports/files are never copied into span names.
pub fn http_route(path: &str) -> &'static str {
    let path = path.split('?').next().unwrap_or(path);
    match path {
        "/healthz" => return "/healthz",
        "/metrics" => return "/metrics",
        "/invoke" => return "/invoke",
        "/commands/watch" => return "/commands/watch",
        "/pty" => return "/pty",
        "/upload" => return "/upload",
        "/upload/status" => return "/upload/status",
        "/upload/commit" => return "/upload/commit",
        "/download" => return "/download",
        "/control/v1/checkpoint" => return "/control/v1/checkpoint",
        "/control/v1/status" => return "/control/v1/status",
        "/api/environment/create" => return "/api/environment/create",
        "/api/environment/v1/environments" => return "/api/environment/v1/environments",
        "/api/environment/v1/snapshots" => return "/api/environment/v1/snapshots",
        "/api/instances" => return "/api/instances",
        "/api/admin/v1/keys" => return "/api/admin/v1/keys",
        "/api/agent" => return "/api/agent",
        "/global-scheduler/resources" => return "/global-scheduler/resources",
        "/global-scheduler/scheduling_queue" => return "/global-scheduler/scheduling_queue",
        "/global-scheduler/node/localschedulingstatus" => {
            return "/global-scheduler/node/localschedulingstatus"
        }
        _ => (),
    }
    if path
        .strip_prefix("/api/environment/v1/snapshots/")
        .is_some_and(|id| !id.is_empty() && !id.contains('/'))
    {
        return "/api/environment/v1/snapshots/{id}";
    }
    if path
        .strip_prefix("/api/admin/v1/keys/")
        .is_some_and(|id| !id.is_empty() && !id.contains('/'))
    {
        return "/api/admin/v1/keys/{id}";
    }
    if let Some(rest) = path.strip_prefix("/api/environment/v1/environments/") {
        let (id, action) = rest.split_once('/').unwrap_or((rest, ""));
        if id.is_empty() {
            return "/unmatched";
        }
        return match action {
            "" => "/api/environment/v1/environments/{id}",
            "pause" => "/api/environment/v1/environments/{id}/pause",
            "resume" => "/api/environment/v1/environments/{id}/resume",
            "reload" => "/api/environment/v1/environments/{id}/reload",
            "network" => "/api/environment/v1/environments/{id}/network",
            "snapshots" => "/api/environment/v1/environments/{id}/snapshots",
            _ => "/unmatched",
        };
    }
    if let Some(rest) = path.strip_prefix("/api/environment/") {
        let (id, action) = rest.split_once('/').unwrap_or((rest, ""));
        if id.is_empty() {
            return "/unmatched";
        }
        return match action {
            "" => "/api/environment/{id}",
            "exec/invoke" => "/api/environment/{id}/exec/invoke",
            "exec/commands/watch" => "/api/environment/{id}/exec/commands/watch",
            "exec/pty" => "/api/environment/{id}/exec/pty",
            "exec/upload" => "/api/environment/{id}/exec/upload",
            "exec/upload/status" => "/api/environment/{id}/exec/upload/status",
            "exec/upload/commit" => "/api/environment/{id}/exec/upload/commit",
            "exec/download" => "/api/environment/{id}/exec/download",
            _ => "/unmatched",
        };
    }
    if let Some(rest) = path.strip_prefix("/api/agent/") {
        let (_, action) = rest.split_once('/').unwrap_or((rest, ""));
        return match action {
            "" => "/api/agent/{id}",
            "invoke" => "/api/agent/{id}/invoke",
            "files/upload" => "/api/agent/{id}/files/upload",
            "files/download" => "/api/agent/{id}/files/download",
            "files/list" => "/api/agent/{id}/files/list",
            "files/mkdir" => "/api/agent/{id}/files/mkdir",
            _ => "/unmatched",
        };
    }
    "/unmatched"
}

#[cfg(test)]
mod tests {
    use super::resource_attributes;

    #[test]
    fn resource_attributes_carry_service_instance_node_and_environment() {
        let attributes = resource_attributes("ingress", "node-1-42", Some("node-1"), Some("prod"));
        assert!(attributes
            .iter()
            .any(|(key, value)| *key == "service.name" && value == "ingress"));
        assert!(attributes
            .iter()
            .any(|(key, value)| *key == "service.instance.id" && value == "node-1-42"));
        assert!(attributes
            .iter()
            .any(|(key, value)| *key == "node.id" && value == "node-1"));
        assert!(attributes
            .iter()
            .any(|(key, value)| *key == "adx_env" && value == "prod"));

        let minimal = resource_attributes("adxlet", "host-7", None, None);
        assert_eq!(minimal.len(), 2);
    }
}
