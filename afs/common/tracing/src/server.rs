use std::{
    future::Future,
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tonic::codegen::http::{HeaderMap, Request};
use tower::{Layer, Service};
use tracing::Instrument as _;

use crate::{TraceContext, set_parent};

/// Tower layer installed once around a Tonic server.
///
/// Every generated service receives the same W3C parent extraction and one
/// transport span. Business handlers remain focused on DTO/domain conversion.
#[derive(Clone, Copy, Debug, Default)]
pub struct GrpcServerTraceLayer {
    periodic_operations: bool,
}

impl GrpcServerTraceLayer {
    #[must_use]
    pub const fn new(periodic_operations: bool) -> Self {
        Self {
            periodic_operations,
        }
    }
}

impl<S> Layer<S> for GrpcServerTraceLayer {
    type Service = GrpcServerTraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcServerTraceService {
            inner,
            periodic_operations: self.periodic_operations,
        }
    }
}

#[derive(Clone, Debug)]
pub struct GrpcServerTraceService<S> {
    inner: S,
    periodic_operations: bool,
}

impl<S, B> Service<Request<B>> for GrpcServerTraceService<S>
where
    S: Service<Request<B>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let method = request.uri().path();
        if !self.periodic_operations && is_periodic_method(method) {
            return Box::pin(self.inner.call(request));
        }
        let operation_name = grpc_operation_name(method);
        let span = tracing::info_span!(
            "afs.grpc.server",
            // `operation_name` is already a bounded `&'static str`, so the
            // OpenTelemetry layer records it through the string-value visitor.
            otel.name = operation_name,
            otel.kind = "server",
            rpc.system = "grpc",
            rpc.method = operation_name,
            transport.result = tracing::field::Empty,
        );
        // 无 Span 时无需解析远端父上下文。enabled 判断交给当前 Subscriber，
        // 不引入会屏蔽 SDK 宿主 Subscriber 的进程全局开关。
        if span.is_disabled() {
            return Box::pin(self.inner.call(request));
        }
        let parent = TraceContext::from_remote(
            TraceContextPropagator::new().extract(&HeaderExtractor(request.headers())),
        );
        set_parent(&span, &parent);
        let future = self.inner.call(request);
        let result_span = span.clone();
        Box::pin(
            async move {
                let result = future.await;
                // At this generic Tower boundary an HTTP/2 response can still
                // carry a business-level gRPC Status. Therefore this field is
                // deliberately named `transport.result`; handlers and typed
                // operation metrics own the business result.
                result_span.record(
                    "transport.result",
                    if result.is_ok() { "ok" } else { "error" },
                );
                result
            }
            .instrument(span),
        )
    }
}

fn is_periodic_method(method: &str) -> bool {
    method.ends_with("/Heartbeat")
}

fn grpc_operation_name(_method: &str) -> &'static str {
    "afs.grpc.server"
}

struct HeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key)?.to_str().ok()
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_routes_use_a_bounded_operation_name() {
        assert_eq!(
            grpc_operation_name("/future.Service/Method"),
            "afs.grpc.server"
        );
    }

    #[test]
    fn heartbeat_routes_are_periodic() {
        assert!(is_periodic_method("/future.Service/Heartbeat"));
        assert!(!is_periodic_method("/future.Service/Read"));
    }
}
