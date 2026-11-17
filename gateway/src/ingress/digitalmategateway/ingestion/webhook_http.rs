//! Fixed provider webhook entrypoint. A successful reply follows PostgreSQL
//! commit; database and commit ambiguity remain retryable provider failures.
use super::super::composio::{WebhookError, WebhookVerifier};
use super::{Error, Inbox};
use crate::ingress::server::ProxyBody;
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub const WEBHOOK_PATH: &str = "/api/digitalmate/ingestion/v1/composio/webhook";

pub struct WebhookHttp {
    verifier: Arc<WebhookVerifier>,
    inbox: Inbox,
}

impl WebhookHttp {
    pub fn new(verifier: Arc<WebhookVerifier>, inbox: Inbox) -> Self {
        Self { verifier, inbox }
    }

    pub(crate) async fn handle(&self, request: Request<Incoming>) -> Response<ProxyBody> {
        if request.uri().path() != WEBHOOK_PATH || request.uri().query().is_some() {
            return response(StatusCode::NOT_FOUND, "not_found");
        }
        if request.method() != Method::POST {
            return response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
        }
        let (parts, body) = request.into_parts();
        let bytes = match Limited::new(body, 1_048_576).collect().await {
            Ok(value) => value.to_bytes(),
            Err(_) => return response(StatusCode::PAYLOAD_TOO_LARGE, "invalid_body"),
        };
        let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(value) => value.as_secs(),
            Err(_) => return response(StatusCode::SERVICE_UNAVAILABLE, "clock_unavailable"),
        };
        let proof = match self.verifier.verify(&parts.headers, &bytes, now) {
            Ok(proof) => proof,
            Err(
                WebhookError::Signature | WebhookError::Timestamp | WebhookError::InvalidHeaders,
            ) => {
                return response(StatusCode::UNAUTHORIZED, "invalid_webhook");
            }
            Err(_) => return response(StatusCode::BAD_REQUEST, "invalid_webhook"),
        };
        match self.inbox.receive(proof).await {
            Ok(_) => response(StatusCode::OK, "durable"),
            Err(Error::ConflictingInput) => response(StatusCode::CONFLICT, "event_conflict"),
            Err(Error::Refused) => response(StatusCode::FORBIDDEN, "subscription_refused"),
            Err(Error::Capacity) => response(StatusCode::SERVICE_UNAVAILABLE, "capacity_exhausted"),
            Err(_) => response(StatusCode::SERVICE_UNAVAILABLE, "store_unavailable"),
        }
    }
}

fn response(status: StatusCode, code: &'static str) -> Response<ProxyBody> {
    let body = Full::new(Bytes::from_static(code.as_bytes()))
        .map_err(|error| match error {})
        .boxed_unsync();
    let mut result = Response::new(body);
    *result.status_mut() = status;
    result
        .headers_mut()
        .insert("cache-control", http::HeaderValue::from_static("no-store"));
    if status == StatusCode::SERVICE_UNAVAILABLE {
        result
            .headers_mut()
            .insert("retry-after", http::HeaderValue::from_static("5"));
    }
    result
}
