use afs_metrics::MetricExemplar;
use opentelemetry::{
    Context,
    trace::{SpanContext, TraceContextExt},
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Transport-neutral parent context captured before an asynchronous boundary.
///
/// This belongs in an internal mailbox envelope, not in AFS business protobuf
/// messages. Cloning it clones small trace identity state, not payload bytes.
#[derive(Clone, Debug)]
pub struct TraceContext(Context);

impl TraceContext {
    #[must_use]
    pub fn current() -> Self {
        Self(current_otel_context())
    }

    #[must_use]
    pub fn from_remote(context: Context) -> Self {
        Self(context)
    }

    #[must_use]
    pub fn as_otel(&self) -> &Context {
        &self.0
    }

    /// Enter this parent while executing synchronous mailbox-owner code.
    #[must_use]
    pub fn attach(&self) -> opentelemetry::ContextGuard {
        self.0.clone().attach()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceCorrelation {
    pub trace_id: String,
    pub span_id: String,
}

#[must_use]
pub fn capture_current_context() -> TraceContext {
    TraceContext::current()
}

/// Returns IDs only for a valid sampled context.
///
/// The active `tracing` span is preferred. The thread-local OpenTelemetry
/// context is the fallback used by actor/mailbox owner loops.
#[must_use]
pub fn current_correlation() -> Option<TraceCorrelation> {
    let context = current_otel_context();
    let span = context.span();
    let span_context = span.span_context();
    valid_sampled(span_context).then(|| TraceCorrelation {
        trace_id: span_context.trace_id().to_string(),
        span_id: span_context.span_id().to_string(),
    })
}

#[must_use]
pub fn current_exemplar() -> Option<MetricExemplar> {
    let context = current_otel_context();
    let span = context.span();
    let span_context = span.span_context();
    valid_sampled(span_context).then(|| MetricExemplar::new(span_context.trace_id().to_bytes()))
}

/// Makes an extracted remote or mailbox Context the parent of a new span.
pub fn set_parent(span: &tracing::Span, parent: &TraceContext) {
    // No OpenTelemetry layer is a supported SDK-host configuration. In that
    // case `set_parent` reports that no OTel extension exists and tracing stays
    // a no-op; business execution must continue.
    let _ = span.set_parent(parent.0.clone());
}

pub fn record_ok(span: &tracing::Span) {
    span.record("result", "ok");
}

fn current_otel_context() -> Context {
    let span_context = tracing::Span::current().context();
    if span_context.span().span_context().is_valid() {
        span_context
    } else {
        Context::current()
    }
}

fn valid_sampled(span_context: &SpanContext) -> bool {
    span_context.is_valid() && span_context.is_sampled()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_subscriber_means_no_correlation_or_exemplar() {
        assert_eq!(current_correlation(), None);
        assert_eq!(current_exemplar(), None);
    }
}
