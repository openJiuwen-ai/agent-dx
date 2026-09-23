//! Shared management envelope; authentication and business routing stay in their own adapters.
use super::server::ProxyBody;
use adx_agent_api::{request::RequestContext, Error};
use adx_agent_core::limits;
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use std::{future::Future, sync::Arc, time::Duration};

pub(super) async fn management<B, F, Fut>(
    request: Request<B>,
    timeout: Duration,
    handler: F,
) -> Response<ProxyBody>
where
    F: FnOnce(Request<B>, Arc<RequestContext>) -> Fut,
    Fut: Future<Output = Result<serde_json::Value, Error>>,
{
    let ctx = Arc::new(RequestContext::new(timeout));
    let trace = match request.headers().get("x-trace-id") {
        Some(value) => match value.to_str() {
            Ok(value)
                if !value.trim().is_empty()
                    && value.len() <= limits::IDENTIFIER_BYTES
                    && !value.chars().any(char::is_control) =>
            {
                value.to_owned()
            }
            _ => return error_response(Error::Invalid("invalid X-Trace-Id".into()), None),
        },
        None => uuid::Uuid::new_v4().to_string(),
    };
    match ctx.run(handler(request, ctx.clone())).await {
        Ok(value) => json_response(StatusCode::OK, &value, Some(&trace)),
        Err(error) => error_response(error, Some(&trace)),
    }
}

pub(super) fn error_response(
    error: Error,
    trace: Option<&str>,
) -> Response<super::server::ProxyBody> {
    let status = match &error {
        Error::Invalid(_) => StatusCode::BAD_REQUEST,
        Error::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
        Error::NotFound => StatusCode::NOT_FOUND,
        Error::Conflict(_) => StatusCode::CONFLICT,
        Error::NotReady(_) | Error::Unavailable(_) | Error::OutcomeUnknown(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
    };
    json_response(
        status,
        &serde_json::json!({"code":status.as_u16(),"message":error.to_string(),"error":error}),
        trace,
    )
}
pub(super) fn json_response(
    status: StatusCode,
    value: &serde_json::Value,
    trace: Option<&str>,
) -> Response<super::server::ProxyBody> {
    let mut response = Response::builder()
        .status(status)
        .header("content-type", "application/json");
    if let Some(trace) = trace {
        response = response.header("x-trace-id", trace);
    }
    response
        .body(
            Full::new(Bytes::from(
                serde_json::to_vec(value).expect("serializing a JSON Value cannot fail"),
            ))
            .map_err(
                |never: std::convert::Infallible| -> Box<dyn std::error::Error + Send + Sync> {
                    match never {}
                },
            )
            .boxed_unsync(),
        )
        .expect("response uses fixed status and content type and a validated trace header")
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body(response: Response<ProxyBody>) -> serde_json::Value {
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn management_envelope_preserves_trace_and_classifies_deadline_errors() {
        for write in [false, true] {
            let request = Request::builder()
                .header("x-trace-id", "test-trace")
                .body(())
                .unwrap();
            let response = management(request, Duration::from_secs(1), |_, ctx| async move {
                if write {
                    ctx.start_write();
                }
                std::future::pending::<Result<serde_json::Value, Error>>().await
            })
            .await;
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()["x-trace-id"], "test-trace");
            let value = body(response).await;
            assert_eq!(value["code"], 503);
            assert_eq!(
                value["error"]["kind"],
                if write {
                    "outcome_unknown"
                } else {
                    "unavailable"
                }
            );
        }
        let response = management(Request::new(()), Duration::from_secs(1), |_, _| async {
            Ok(serde_json::json!({"ok":true}))
        })
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        uuid::Uuid::parse_str(response.headers()["x-trace-id"].to_str().unwrap()).unwrap();
        assert_eq!(body(response).await, serde_json::json!({"ok":true}));
    }

    #[tokio::test]
    async fn invalid_trace_is_rejected_before_business_execution() {
        let called = std::sync::atomic::AtomicBool::new(false);
        let request = Request::builder()
            .header("x-trace-id", " ")
            .body(())
            .unwrap();
        let response = management(request, Duration::from_secs(1), |_, _| async {
            called.store(true, std::sync::atomic::Ordering::Relaxed);
            Ok(serde_json::Value::Null)
        })
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body(response).await["error"]["kind"], "invalid");
        assert!(!called.load(std::sync::atomic::Ordering::Relaxed));
    }
}
