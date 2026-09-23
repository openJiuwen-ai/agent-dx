//! Authentication for the Frontend inline compatibility protocol only.
use super::auth::{validate_with_iam, AuthError, AuthenticatedIdentity};
use base64::{engine::general_purpose, Engine};
use http::Request;
use serde::Deserialize;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) struct InlineAuth {
    iam_address: String,
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    exp: i64,
}

impl InlineAuth {
    pub fn new(iam_address: String) -> Result<Self, AuthError> {
        // The existing IAM capability uses HTTP on the trusted internal network.
        let url = url::Url::parse(&format!(
            "http://{}",
            iam_address.trim().trim_start_matches("http://")
        ))
        .map_err(|_| AuthError::Invalid("invalid inline IAM address".into()))?;
        if url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(AuthError::Invalid(
                "inline IAM address must be an HTTP origin".into(),
            ));
        }
        let host = url
            .host()
            .ok_or_else(|| AuthError::Invalid("missing IAM host".into()))?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| AuthError::Invalid("missing IAM port".into()))?;
        Ok(Self {
            iam_address: format!("{host}:{port}"),
        })
    }

    pub async fn authenticate_management<B>(
        &self,
        request: &Request<B>,
    ) -> Result<String, AuthError> {
        self.authenticate(request, true)
            .await
            .map(|identity| identity.tenant_id)
    }
    pub async fn authenticate_data<B>(
        &self,
        request: &Request<B>,
    ) -> Result<AuthenticatedIdentity, AuthError> {
        self.authenticate(request, false).await
    }
    async fn authenticate<B>(
        &self,
        request: &Request<B>,
        management: bool,
    ) -> Result<AuthenticatedIdentity, AuthError> {
        let token = management_token(request)
            .or_else(|| {
                if management {
                    return None;
                }
                request
                    .headers()
                    .get("sec-websocket-protocol")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|value| value.split(',').map(str::trim).find(|v| !v.is_empty()))
                    .map(str::to_owned)
            })
            .ok_or(AuthError::Missing)?;

        let parts: Vec<_> = token.split('.').collect();
        let [header, payload, _signature] = parts.as_slice() else {
            return Err(AuthError::Invalid("expected a JWT".into()));
        };
        let _: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&decode(header)?)
                .map_err(|_| AuthError::Invalid("invalid JWT header".into()))?;
        let claims: Claims = serde_json::from_slice(&decode(payload)?)
            .map_err(|_| AuthError::Invalid("invalid JWT claims".into()))?;
        if management && (claims.sub.trim().is_empty() || claims.role != "developer") {
            return Err(AuthError::Invalid(
                "inline management requires a developer tenant".into(),
            ));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if claims.exp > 0 && now > claims.exp.unsigned_abs() {
            return Err(AuthError::Expired);
        }
        let trace = request
            .headers()
            .get("x-trace-id")
            .or_else(|| request.headers().get("x-request-id"))
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        // Decoding the claims above is not authentication. IAM verifies the original token.
        validate_with_iam(&self.iam_address, &token, trace).await?;
        Ok(AuthenticatedIdentity {
            tenant_id: if claims.sub.is_empty() {
                "default".into()
            } else {
                claims.sub
            },
            expires_at_unix: (claims.exp > 0).then_some(claims.exp),
        })
    }
}

fn decode(value: &str) -> Result<Vec<u8>, AuthError> {
    general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .or_else(|_| general_purpose::STANDARD.decode(value))
        .map_err(|_| AuthError::Invalid("invalid JWT encoding".into()))
}

fn management_token<B>(request: &Request<B>) -> Option<String> {
    if let Some(token) = request
        .headers()
        .get("x-auth")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
    {
        return Some(token.into());
    }
    if let Some((_, token)) =
        url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes())
            .find(|(key, _)| key == "token")
    {
        if !token.is_empty() {
            return Some(token.into_owned());
        }
    }
    for cookie in request.headers().get_all(http::header::COOKIE) {
        for part in cookie.to_str().ok()?.split(';') {
            if let Some(("iam_token", value)) = part.trim().split_once('=') {
                let value = value.trim_matches('"').replace('+', " ");
                if !value.is_empty() {
                    return percent_encoding::percent_decode_str(&value)
                        .decode_utf8()
                        .ok()
                        .map(|v| v.into_owned());
                }
            }
        }
    }
    None
}
