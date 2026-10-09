use opentelemetry::{
    Context,
    propagation::{Extractor, Injector, TextMapPropagator},
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tonic::{
    Request,
    metadata::{KeyRef, MetadataMap},
    service::{Interceptor, interceptor::InterceptedService},
    transport::Channel,
};

use crate::TraceContext;

struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(key) = key.parse::<tonic::metadata::MetadataKey<_>>() else {
            return;
        };
        let Ok(value) = value.parse() else {
            return;
        };
        self.0.insert(key, value);
    }
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key)?.to_str().ok()
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|key| match key {
                KeyRef::Ascii(key) => Some(key.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

pub fn inject_current_context(metadata: &mut MetadataMap) {
    let current = crate::capture_current_context();
    TraceContextPropagator::new()
        .inject_context(current.as_otel(), &mut MetadataInjector(metadata));
}

#[must_use]
pub fn request_with_current_context<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    inject_current_context(request.metadata_mut());
    request
}

#[must_use]
pub fn extract_remote_context(metadata: &MetadataMap) -> TraceContext {
    let context: Context = TraceContextPropagator::new().extract(&MetadataExtractor(metadata));
    TraceContext::from_remote(context)
}

/// Concrete channel type used by all generated AFS gRPC clients.
///
/// The interceptor mutates only transport metadata. Generated protobuf bodies,
/// public SDK parameters, and payload bytes remain unchanged.
pub type TracedChannel = InterceptedService<Channel, TraceContextInterceptor>;

#[derive(Clone, Copy, Debug, Default)]
pub struct TraceContextInterceptor;

impl Interceptor for TraceContextInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, tonic::Status> {
        inject_current_context(request.metadata_mut());
        Ok(request)
    }
}

#[must_use]
pub fn traced_channel(channel: Channel) -> TracedChannel {
    InterceptedService::new(channel, TraceContextInterceptor)
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };

    use super::*;

    #[test]
    fn metadata_round_trip_preserves_w3c_identity() {
        let span_context = SpanContext::new(
            TraceId::from(42),
            SpanId::from(7),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let context = Context::new().with_remote_span_context(span_context);
        let mut metadata = MetadataMap::new();
        TraceContextPropagator::new()
            .inject_context(&context, &mut MetadataInjector(&mut metadata));

        let extracted = extract_remote_context(&metadata);
        let span = extracted.as_otel().span();
        let actual = span.span_context();
        assert_eq!(actual.trace_id(), TraceId::from(42));
        assert_eq!(actual.span_id(), SpanId::from(7));
        assert!(actual.is_sampled());
        assert!(actual.is_remote());
    }
}
