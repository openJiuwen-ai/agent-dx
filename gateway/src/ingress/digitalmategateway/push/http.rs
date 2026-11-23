//! Bounded Push contract on the DigitalMateGateway business entrypoint.
use super::super::connector_owner;
use super::{Error, Push, Scope};
use crate::ingress::{accounts, processing::ProcessingDeadline};
use ::http::{header, request::Parts, Method, Response, StatusCode};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Body;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeMap, future::Future, time::Duration};
use tokio::time::{timeout_at, Instant};

pub const SEND_PATH: &str = "/api/digitalmate/push/v1/send";
const MAX_REQUEST_BYTES: usize = 16_384;
const MAX_CLICK_BYTES: usize = 2048;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendRequest {
    event_id: String,
    title: String,
    body: String,
    profile_id: String,
    device_token: String,
    click_target: BTreeMap<String, String>,
    ttl_seconds: u16,
}

pub(crate) async fn handle<B>(
    gateway: Option<&Push>,
    parts: &Parts,
    body: B,
    authenticate: impl Future<Output = accounts::Result<Scope>>,
) -> Response<Full<Bytes>>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    // Shared Ingress consumes the header and carries its deadline in the request extensions.
    if parts
        .extensions
        .get::<ProcessingDeadline>()
        .is_some_and(|deadline| deadline.caller_supplied)
    {
        return not_sent(
            StatusCode::BAD_REQUEST,
            "unsupported_processing_timeout",
            false,
        );
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    let prepare = async {
        if parts.method != Method::POST {
            return Err(not_sent(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                false,
            ));
        }
        if parts.uri.query().is_some() || parts.headers.contains_key(header::ORIGIN) {
            return Err(not_sent(StatusCode::BAD_REQUEST, "invalid_request", false));
        }
        if !connector_owner::bearer(&parts.headers)
            .is_ok_and(|token| token.starts_with("dm-privacy-v1."))
        {
            return Err(not_sent(StatusCode::UNAUTHORIZED, "unauthorized", false));
        }
        let scope = authenticate.await.map_err(|error| {
            let status = error.status();
            not_sent(status, "workload_identity_denied", status.is_server_error())
        })?;
        let content_types = parts.headers.get_all(header::CONTENT_TYPE);
        let mut values = content_types.iter();
        let content_type = values.next().and_then(|value| value.to_str().ok());
        if values.next().is_some()
            || !content_type.is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
            })
        {
            return Err(not_sent(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "invalid_request",
                false,
            ));
        }
        let gateway = gateway
            .ok_or_else(|| not_sent(StatusCode::SERVICE_UNAVAILABLE, "not_configured", true))?;
        let bytes = Limited::new(body, MAX_REQUEST_BYTES)
            .collect()
            .await
            .map_err(|_| not_sent(StatusCode::PAYLOAD_TOO_LARGE, "invalid_request", false))?
            .to_bytes();
        let huawei = huawei_request(&bytes, &gateway.category).map_err(failure)?;
        let request = gateway
            .client
            .prepare(&scope, &huawei)
            .await
            .map_err(failure)?;
        Ok((&gateway.client, request))
    };
    let (client, request) = match timeout_at(deadline, prepare).await {
        Ok(Ok(ready)) if Instant::now() < deadline => ready,
        Ok(Err(response)) => return response,
        _ => return not_sent(StatusCode::SERVICE_UNAVAILABLE, "preparation_timeout", true),
    };
    match client.dispatch(request, deadline).await {
        Ok(value) => response(StatusCode::OK, value),
        Err(error) => failure(error),
    }
}

fn huawei_request(bytes: &[u8], category: &str) -> Result<Value, Error> {
    let request: SendRequest = serde_json::from_slice(bytes).map_err(|_| Error::InvalidRequest)?;
    if request.event_id.is_empty()
        || request.event_id.len() > 64
        || !request
            .event_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_+=/-".contains(&byte))
        || [request.title.as_str(), request.body.as_str()]
            .iter()
            .any(|text| text.trim().is_empty() || text.chars().count() > 180)
        || !(8..=64).contains(&request.profile_id.chars().count())
        || !(16..=4096).contains(&request.device_token.chars().count())
        || request.profile_id.chars().any(char::is_control)
        || request.device_token.chars().any(char::is_control)
        || !(1..=3600).contains(&request.ttl_seconds)
        || !["workspace", "principal"]
            .iter()
            .all(|key| request.click_target.contains_key(*key))
        || request
            .click_target
            .iter()
            .any(|(key, value)| key.trim().is_empty() || value.trim().is_empty())
    {
        return Err(Error::InvalidRequest);
    }
    let click = serde_json::to_string(&request.click_target).map_err(|_| Error::InvalidRequest)?;
    if click.len() > MAX_CLICK_BYTES {
        return Err(Error::InvalidRequest);
    }
    Ok(json!({
        "payload": {"notification": {
            "category": category,
            "title": request.title,
            "body": request.body,
            "profileId": request.profile_id,
            "foregroundShow": false,
            "appMessageId": request.event_id,
            "clickAction": {"actionType": 0, "data": {"digitalmatePushTarget": click}}
        }},
        "target": {"token": [request.device_token]},
        "pushOptions": {"ttl": request.ttl_seconds}
    }))
}

fn failure(error: Error) -> Response<Full<Bytes>> {
    match error {
        Error::InvalidRequest => not_sent(StatusCode::BAD_REQUEST, "invalid_request", false),
        Error::Forbidden
        | Error::AuthorizationInactive
        | Error::AuthorizationPending
        | Error::AccountMismatch
        | Error::ReauthRequired
        | Error::ConsentRequired => not_sent(StatusCode::FORBIDDEN, "forbidden", false),
        Error::RateLimited { retry_after } => {
            let mut result = not_sent(StatusCode::TOO_MANY_REQUESTS, "rate_limited", true);
            if let Some(seconds) = retry_after {
                if let Ok(value) = seconds.min(3600).to_string().parse() {
                    result.headers_mut().insert(header::RETRY_AFTER, value);
                }
            }
            result
        }
        Error::NotConfigured | Error::UpstreamUnavailable => {
            not_sent(StatusCode::SERVICE_UNAVAILABLE, "unavailable", true)
        }
        Error::NotFound => not_sent(StatusCode::NOT_FOUND, "not_found", false),
        Error::InvalidResponse | Error::ResponseTooLarge => {
            not_sent(StatusCode::BAD_GATEWAY, "invalid_dependency_response", true)
        }
        Error::OutcomeUnknown => response(StatusCode::CONFLICT, json!({"result":"unknown"})),
    }
}

fn not_sent(status: StatusCode, code: &'static str, retryable: bool) -> Response<Full<Bytes>> {
    response(
        status,
        json!({"result":"not_sent","error":code,"retryable":retryable}),
    )
}

fn response(status: StatusCode, value: Value) -> Response<Full<Bytes>> {
    let mut result = Response::new(Full::new(Bytes::from(value.to_string())));
    *result.status_mut() = status;
    for (name, value) in [
        (header::CONTENT_TYPE, "application/json"),
        (header::CACHE_CONTROL, "no-store"),
        (header::REFERRER_POLICY, "no-referrer"),
        (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        (
            header::CONTENT_SECURITY_POLICY,
            "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
        ),
    ] {
        result
            .headers_mut()
            .insert(name, ::http::HeaderValue::from_static(value));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{huawei_request, Error};
    use serde_json::{json, Value};

    pub(super) fn body() -> Value {
        json!({
            "event_id": "hive_abc",
            "title": "Beebou",
            "body": "发来一条消息",
            "profile_id": "profile-1",
            "device_token": "token-1234567890123456",
            "click_target": {"kind":"hive_home","workspace":"local","principal":"user"},
            "ttl_seconds": 60
        })
    }

    #[test]
    fn builds_one_system_notification_with_a_fixed_click_action() {
        let value = huawei_request(body().to_string().as_bytes(), "WORK").unwrap();
        assert_eq!(value["target"]["token"], json!(["token-1234567890123456"]));
        assert_eq!(value["payload"]["notification"]["foregroundShow"], false);
        assert_eq!(
            value["payload"]["notification"]["clickAction"]["actionType"],
            0
        );
    }

    #[test]
    fn rejects_raw_provider_fields_and_invalid_targets() {
        let mut request = body();
        request["huawei"] = json!({});
        assert!(matches!(
            huawei_request(request.to_string().as_bytes(), "WORK"),
            Err(Error::InvalidRequest)
        ));
        let mut request = body();
        request["device_token"] = json!(["t1", "t2"]);
        assert!(huawei_request(request.to_string().as_bytes(), "WORK").is_err());
        let mut request = body();
        request["ttl_seconds"] = json!(3601);
        assert!(huawei_request(request.to_string().as_bytes(), "WORK").is_err());
    }
}

#[cfg(test)]
mod entry_tests {
    use super::super::super::composio::Budget;
    use super::super::{client::Client, jwt::Signer};
    use super::*;
    use ::http::Request;
    use std::sync::Arc;

    struct Exhausted;
    #[async_trait::async_trait]
    impl Budget for Exhausted {
        async fn acquire(&self, _: &Scope) -> Result<(), Error> {
            Err(Error::RateLimited {
                retry_after: Some(42),
            })
        }
    }

    fn gateway() -> Push {
        let signer = Signer::from_pem(
            include_str!("../../../../tests/fixtures/huawei-test-key.pem"),
            "test".into(),
            "issuer".into(),
        )
        .unwrap();
        Push {
            client: Client::new("123".into(), Arc::new(signer), Arc::new(Exhausted)).unwrap(),
            category: "WORK".into(),
        }
    }

    fn request() -> Request<Full<Bytes>> {
        Request::post(SEND_PATH)
            .header(header::AUTHORIZATION, "Bearer dm-privacy-v1.fixture")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(super::tests::body().to_string())))
            .unwrap()
    }

    async fn invoke(
        gateway: Option<&Push>,
        request: Request<Full<Bytes>>,
        scope: accounts::Result<Scope>,
    ) -> (StatusCode, ::http::HeaderMap, Value) {
        let (parts, body) = request.into_parts();
        let reply = handle(gateway, &parts, body, std::future::ready(scope)).await;
        let (parts, body) = reply.into_parts();
        let value = serde_json::from_slice(&body.collect().await.unwrap().to_bytes()).unwrap();
        (parts.status, parts.headers, value)
    }

    fn scope() -> accounts::Result<Scope> {
        Ok(
            Scope::from_authenticated_foreground("tenant".into(), "user".into(), "w-bound".into())
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn business_route_validates_before_dispatch() {
        use super::super::super::entrypoint::DigitalMateGatewayApi;
        assert!(DigitalMateGatewayApi::matches(SEND_PATH));
        let gateway = gateway();
        let mut method = request();
        *method.method_mut() = Method::GET;
        let mut origin = request();
        origin
            .headers_mut()
            .insert(header::ORIGIN, "https://web.example".parse().unwrap());
        let mut prefix = request();
        prefix.headers_mut().insert(
            header::AUTHORIZATION,
            "Bearer dm-connector-v1.fixture".parse().unwrap(),
        );
        let mut content_type = request();
        content_type
            .headers_mut()
            .insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
        let oversize = request().map(|_| Full::new(Bytes::from(vec![0; MAX_REQUEST_BYTES + 1])));
        for (req, status) in [
            (method, 405),
            (origin, 400),
            (prefix, 401),
            (content_type, 415),
            (oversize, 413),
        ] {
            let (actual, _, value) = invoke(Some(&gateway), req, scope()).await;
            assert_eq!(actual.as_u16(), status);
            assert_eq!(value["result"], "not_sent");
            assert_eq!(value["retryable"], false);
        }
        let (status, _, value) = invoke(
            Some(&gateway),
            request(),
            Err(accounts::Error::Unauthorized),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(value["retryable"], false);
        let (status, _, value) = invoke(None, request(), scope()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(value["error"], "not_configured");
        let (status, headers, value) = invoke(Some(&gateway), request(), scope()).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(headers[header::RETRY_AFTER], "42");
        assert_eq!(
            value,
            json!({"result":"not_sent","retryable":true,"error":"rate_limited"})
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_before_dispatch_allows_retry() {
        let (parts, body) = request().into_parts();
        let reply = handle(None, &parts, body, std::future::pending()).await;
        assert_eq!(reply.status(), StatusCode::SERVICE_UNAVAILABLE);
        let value: Value =
            serde_json::from_slice(&reply.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(
            value,
            json!({"result":"not_sent","retryable":true,"error":"preparation_timeout"})
        );
    }
}
