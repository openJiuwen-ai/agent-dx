//! Bounded wire adapter. The actual authenticated entry supplies Scope and the
//! real Authority; this module neither authenticates JSON nor creates attempts.
use super::types::ReadAccess;
use super::{Composio, Error, GmailRequest, MailRequest, OutlookRequest, Scope};
use ::http::{header, Method, Request, Response, StatusCode};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Body;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

pub const PREFIX: &str = "/api/digitalmate/connectors/v1/composio";
pub const CALLBACK_PATH: &str = "/api/digitalmate/connectors/v1/composio/callback";
pub const VALIDATE_PATH: &str = "/api/digitalmate/connectors/v1/composio/validate";
pub const SUBSCRIPTION_PATH: &str = "/api/digitalmate/connectors/v1/composio/subscription";
const MAX_REQUEST_BYTES: usize = 16_384;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    registration: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Validation {
    connection_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Confirmation {
    registration: String,
    session_uri: String,
}

#[derive(Deserialize)]
#[serde(tag = "provider", rename_all = "lowercase", deny_unknown_fields)]
enum Mail {
    Gmail {
        connection_id: String,
        request: Gmail,
        access: Option<ReadAccess>,
    },
    Outlook {
        connection_id: String,
        request: Outlook,
        access: Option<ReadAccess>,
    },
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Gmail {
    Search {
        query: String,
        limit: u16,
        page: String,
    },
    Read {
        message_id: String,
        body: bool,
    },
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Outlook {
    Search {
        #[serde(default)]
        query: String,
        sender: String,
        since: String,
        until: String,
        limit: u16,
        page: String,
    },
    Read {
        message_id: String,
        body: bool,
    },
}

impl Mail {
    fn into_request(self) -> (String, MailRequest, Option<ReadAccess>) {
        match self {
            Self::Gmail {
                connection_id,
                request,
                access,
            } => (
                connection_id,
                MailRequest::Gmail(match request {
                    Gmail::Search { query, limit, page } => {
                        GmailRequest::Search { query, limit, page }
                    }
                    Gmail::Read { message_id, body } => GmailRequest::Read { message_id, body },
                }),
                access,
            ),
            Self::Outlook {
                connection_id,
                request,
                access,
            } => (
                connection_id,
                MailRequest::Outlook(match request {
                    Outlook::Search {
                        query,
                        sender,
                        since,
                        until,
                        limit,
                        page,
                    } => OutlookRequest::Search {
                        query,
                        sender,
                        since,
                        until,
                        limit,
                        page,
                    },
                    Outlook::Read { message_id, body } => OutlookRequest::Read { message_id, body },
                }),
                access,
            ),
        }
    }
}

/// Candidate route match; production must mount only with real owner adapters.
pub fn matches(path: &str) -> bool {
    path.strip_prefix(PREFIX).is_some_and(|suffix| {
        matches!(
            suffix,
            "/start" | "/state" | "/confirm" | "/cancel" | "/mail" | "/validate" | "/subscription"
        )
    })
}

/// Only for an entry that has authenticated the user and admitted this exact
/// operation. Scope alone is identity, not task/tool permission. Authority must
/// perform current-attempt / connection / operation checks before and after I/O.
pub async fn handle<B>(
    gateway: &Composio,
    scope: &Scope,
    request: Request<B>,
) -> Response<Full<Bytes>>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    if !matches(request.uri().path()) {
        return error_response(StatusCode::NOT_FOUND, "not_found");
    }
    if request.method() != Method::POST {
        return error_response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
    }
    if request.uri().query().is_some() || request.headers().contains_key(header::ORIGIN) {
        return error_response(StatusCode::BAD_REQUEST, "invalid_request");
    }
    let content_type_headers = request.headers().get_all(header::CONTENT_TYPE);
    let mut content_types = content_type_headers.iter();
    let content_type = content_types.next().and_then(|v| v.to_str().ok());
    if content_types.next().is_some()
        || !content_type.is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"))
        })
    {
        return error_response(StatusCode::UNSUPPORTED_MEDIA_TYPE, "invalid_request");
    }
    let path = request.uri().path().to_owned();
    let bytes = match Limited::new(request.into_body(), MAX_REQUEST_BYTES)
        .collect()
        .await
    {
        Ok(value) => Zeroizing::new(value.to_bytes().to_vec()),
        Err(_) => return error_response(StatusCode::PAYLOAD_TOO_LARGE, "invalid_request"),
    };
    let result = dispatch(gateway, scope, &path, &bytes).await;
    match result {
        Ok(value) => response(StatusCode::OK, value),
        Err(Error::AuthorizationPending) if path.ends_with("/state") => {
            response(StatusCode::OK, json!({"phase":"waiting"}))
        }
        Err(error) => failure(error),
    }
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(bytes).map_err(|_| Error::InvalidRequest)
}

fn reference(value: &str) -> Result<(), Error> {
    if value.is_empty() || !super::types::bounded(value, 256) {
        Err(Error::InvalidRequest)
    } else {
        Ok(())
    }
}

async fn dispatch(
    gateway: &Composio,
    scope: &Scope,
    path: &str,
    bytes: &[u8],
) -> Result<Value, Error> {
    match path.strip_prefix(PREFIX) {
        Some("/subscription") => {
            let request: Validation = decode(bytes)?;
            reference(&request.connection_id)?;
            match gateway
                .synchronize_subscription(scope, &request.connection_id)
                .await?
            {
                super::super::ingestion::SubscriptionOutcome::Enabled(binding) => {
                    Ok(json!({"state":"enabled", "binding":binding}))
                }
                super::super::ingestion::SubscriptionOutcome::Disabled => {
                    Ok(json!({"state":"disabled"}))
                }
            }
        }
        Some("/validate") => {
            let request: Validation = decode(bytes)?;
            reference(&request.connection_id)?;
            gateway
                .validate_account(scope, &request.connection_id)
                .await
        }
        Some("/confirm") => {
            let request: Confirmation = decode(bytes)?;
            reference(&request.registration)?;
            let session_uri = Zeroizing::new(request.session_uri);
            let account = gateway
                .confirm(scope, &request.registration, &session_uri)
                .await?;
            Ok(
                json!({"phase":"active", "account_id":account.account_id, "subject_id":account.subject_id}),
            )
        }
        Some("/mail") => {
            let request: Mail = decode(bytes)?;
            let (connection_id, operation, access) = request.into_request();
            reference(&connection_id)?;
            let scope = match access {
                Some(access) => scope
                    .clone()
                    .with_read_access(access, hex::encode(Sha256::digest(bytes))),
                None => scope.clone(),
            };
            gateway.mail(&scope, &connection_id, operation).await
        }
        Some(operation @ ("/start" | "/state" | "/cancel")) => {
            let request: Registration = decode(bytes)?;
            reference(&request.registration)?;
            match operation {
                "/start" => {
                    let started = gateway.start(scope, &request.registration).await?;
                    Ok(
                        json!({"phase":"waiting", "redirect_url":started.redirect_url, "account_id":started.account_id, "expires_at":started.expires_at}),
                    )
                }
                "/state" => {
                    let account = gateway
                        .reconcile_authorization(scope, &request.registration)
                        .await?;
                    Ok(
                        json!({"phase":"active", "account_id":account.account_id, "subject_id":account.subject_id}),
                    )
                }
                _ => {
                    gateway.cancel(scope, &request.registration).await?;
                    Ok(json!({"phase":"cancelled"}))
                }
            }
        }
        _ => Err(Error::InvalidRequest),
    }
}

fn failure(error: Error) -> Response<Full<Bytes>> {
    let status = match error {
        Error::InvalidRequest => StatusCode::BAD_REQUEST,
        Error::Forbidden | Error::AccountMismatch | Error::ConsentRequired => StatusCode::FORBIDDEN,
        Error::ReauthRequired => StatusCode::UNAUTHORIZED,
        Error::AuthorizationInactive | Error::OutcomeUnknown => StatusCode::CONFLICT,
        Error::AuthorizationPending => StatusCode::ACCEPTED,
        Error::NotFound => StatusCode::NOT_FOUND,
        Error::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
        Error::InvalidResponse | Error::ResponseTooLarge => StatusCode::BAD_GATEWAY,
        Error::NotConfigured | Error::UpstreamUnavailable => StatusCode::SERVICE_UNAVAILABLE,
    };
    let mut result = response(status, json!({"error":error.to_string()}));
    if let Error::RateLimited {
        retry_after: Some(seconds),
    } = error
    {
        if let Ok(value) = seconds.min(3600).to_string().parse() {
            result.headers_mut().insert(header::RETRY_AFTER, value);
        }
    }
    result
}

fn error_response(status: StatusCode, code: &'static str) -> Response<Full<Bytes>> {
    response(status, json!({"error":code}))
}

fn secure_response(
    status: StatusCode,
    content_type: &'static str,
    bytes: Bytes,
) -> Response<Full<Bytes>> {
    let mut result = Response::new(Full::new(bytes));
    *result.status_mut() = status;
    for (name, value) in [
        (header::CONTENT_TYPE, content_type),
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

fn response(status: StatusCode, value: Value) -> Response<Full<Bytes>> {
    secure_response(status, "application/json", Bytes::from(value.to_string()))
}

/// Public browser landing carries no user identity, reads no callback fields and
/// performs no external effect. The app must intercept its exact configured HTTPS
/// verifier URL and confirm through the authenticated native channel. This page
/// does not claim success or provide a system-browser login/handoff fallback.
pub fn callback_landing() -> Response<Full<Bytes>> {
    secure_response(StatusCode::OK, "text/html; charset=utf-8", Bytes::from_static(
        "<!doctype html><html lang=\"zh-CN\"><meta charset=\"utf-8\"><meta name=\"referrer\" content=\"no-referrer\"><title>DigitalMate</title><p>请返回 DigitalMate 继续完成邮箱授权。</p></html>".as_bytes(),
    ))
}
