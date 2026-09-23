use base64::Engine;
use bytes::Bytes;
use http::{header, Request, StatusCode};
use http_body_util::Empty;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::time::timeout;

const IAM_TIMEOUT: Duration = Duration::from_secs(3);
const HEADER_X_AUTH: &str = "x-auth";

#[derive(Debug, Clone)]
pub struct IngressAuthenticator {
    verifier: Option<Arc<dyn CredentialVerifier>>,
    required: bool,
    validate_iam: bool,
    iam_address: String,
    cache_ttl: Duration,
    cache: Arc<Mutex<HashMap<String, Instant>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedIdentity {
    pub tenant_id: String,
    pub expires_at_unix: Option<i64>,
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing bearer token")]
    Missing,
    #[error("ambiguous credentials: {0}")]
    Ambiguous(String),
    #[error("invalid bearer token: {0}")]
    Invalid(String),
    #[error("bearer token is expired")]
    Expired,
    #[error("IAM validation failed: {0}")]
    IamRejected(String),
    #[error("IAM service is unavailable: {0}")]
    IamUnavailable(String),
}

#[derive(Deserialize)]
struct Claims {
    sub: String,
    #[serde(default)]
    exp: i64,
}

impl AuthError {
    pub fn status(&self) -> StatusCode {
        match self {
            Self::IamUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Ambiguous(_) => StatusCode::BAD_REQUEST,
            Self::Missing | Self::Invalid(_) | Self::Expired | Self::IamRejected(_) => {
                StatusCode::UNAUTHORIZED
            }
        }
    }
}

#[async_trait::async_trait]
pub trait CredentialVerifier: Send + Sync + std::fmt::Debug {
    async fn verify(&self, key: &str) -> Result<AuthenticatedIdentity, AuthError>;
}
impl IngressAuthenticator {
    pub fn with_verifier(verifier: Arc<dyn CredentialVerifier>) -> Self {
        let mut auth = Self::disabled();
        auth.required = true;
        auth.verifier = Some(verifier);
        auth
    }

    pub fn new(
        required: bool,
        validate_iam: bool,
        iam_address: impl Into<String>,
        cache_ttl: Duration,
    ) -> Result<Self, AuthError> {
        let iam_address = iam_address.into();
        if required && validate_iam && iam_address.trim().is_empty() {
            return Err(AuthError::Invalid(
                "IAM address is required when IAM validation is enabled".into(),
            ));
        }
        Ok(Self {
            verifier: None,
            required,
            validate_iam,
            iam_address,
            cache_ttl,
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn disabled() -> Self {
        Self::new(false, false, "", Duration::ZERO).expect("disabled auth is valid")
    }

    pub async fn authenticate_request<B>(&self, request: &Request<B>) -> Result<String, AuthError> {
        self.authenticate_request_with_policy(request, true).await
    }

    pub async fn authenticate_request_with_policy<B>(
        &self,
        request: &Request<B>,
        required: bool,
    ) -> Result<String, AuthError> {
        self.authenticate_request_identity_with_policy(request, required)
            .await
            .map(|identity| identity.tenant_id)
    }

    pub async fn authenticate_request_identity_with_policy<B>(
        &self,
        request: &Request<B>,
        required: bool,
    ) -> Result<AuthenticatedIdentity, AuthError> {
        if let Some(identity) = request.extensions().get::<AuthenticatedIdentity>() {
            if identity
                .expires_at_unix
                .is_some_and(|expires| now_unix_seconds() > expires)
            {
                return Err(AuthError::Expired);
            }
            if required && identity.tenant_id.is_empty() {
                return Err(AuthError::Missing);
            }
            return Ok(identity.clone());
        }
        if !self.required {
            return Ok(AuthenticatedIdentity {
                tenant_id: String::new(),
                expires_at_unix: None,
            });
        }
        let Some(token) = request_token(request)? else {
            return if required {
                Err(AuthError::Missing)
            } else {
                Ok(AuthenticatedIdentity {
                    tenant_id: String::new(),
                    expires_at_unix: None,
                })
            };
        };
        self.authenticate_token_identity(token, request_id(request), required)
            .await
    }

    pub async fn authenticate_token(
        &self,
        token: &str,
        request_id: &str,
    ) -> Result<String, AuthError> {
        self.authenticate_token_with_policy(token, request_id, true)
            .await
    }

    pub async fn authenticate_token_with_policy(
        &self,
        token: &str,
        request_id: &str,
        required: bool,
    ) -> Result<String, AuthError> {
        self.authenticate_token_identity(token, request_id, required)
            .await
            .map(|identity| identity.tenant_id)
    }

    async fn authenticate_token_identity(
        &self,
        token: &str,
        request_id: &str,
        required: bool,
    ) -> Result<AuthenticatedIdentity, AuthError> {
        if !self.required {
            return Ok(AuthenticatedIdentity {
                tenant_id: String::new(),
                expires_at_unix: None,
            });
        }
        if token.trim().is_empty() {
            return if required {
                Err(AuthError::Missing)
            } else {
                Ok(AuthenticatedIdentity {
                    tenant_id: String::new(),
                    expires_at_unix: None,
                })
            };
        }
        if let Some(verifier) = &self.verifier {
            return verifier.verify(token).await;
        }
        let claims = parse_claims(token)?;
        if claims.sub.trim().is_empty() {
            return Err(AuthError::Invalid("JWT subject is empty".into()));
        }
        if claims.exp > 0 && now_unix_seconds() > claims.exp {
            return Err(AuthError::Expired);
        }
        if self.validate_iam && !self.cached(token) {
            validate_with_iam(&self.iam_address, token, request_id).await?;
            self.cache
                .lock()
                .unwrap()
                .insert(token.to_owned(), Instant::now());
        }
        Ok(AuthenticatedIdentity {
            tenant_id: claims.sub,
            expires_at_unix: (claims.exp > 0).then_some(claims.exp),
        })
    }

    fn cached(&self, token: &str) -> bool {
        let mut cache = self.cache.lock().unwrap();
        cache.retain(|_, validated_at| validated_at.elapsed() < self.cache_ttl);
        cache.contains_key(token)
    }
}

fn request_token<B>(request: &Request<B>) -> Result<Option<&str>, AuthError> {
    let authorization = unique_header(request, header::AUTHORIZATION, "Authorization")?
        .map(parse_bearer_token)
        .transpose()?;
    let x_auth = unique_header(request, HEADER_X_AUTH, "X-Auth")?
        .map(|value| {
            let token = value.trim();
            if token.is_empty() {
                Err(AuthError::Invalid("X-Auth token is empty".into()))
            } else {
                Ok(token)
            }
        })
        .transpose()?;

    match (authorization, x_auth) {
        (Some(authorization), Some(x_auth)) if authorization != x_auth => Err(
            AuthError::Ambiguous("Authorization and X-Auth contain different tokens".into()),
        ),
        (Some(token), _) | (_, Some(token)) => Ok(Some(token)),
        (None, None) => Ok(None),
    }
}

fn unique_header<'a, B>(
    request: &'a Request<B>,
    name: impl http::header::AsHeaderName,
    display_name: &str,
) -> Result<Option<&'a str>, AuthError> {
    let mut values = request.headers().get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AuthError::Ambiguous(format!(
            "multiple {display_name} headers"
        )));
    }
    value
        .to_str()
        .map(Some)
        .map_err(|_| AuthError::Invalid(format!("{display_name} is not valid ASCII")))
}

fn parse_bearer_token(value: &str) -> Result<&str, AuthError> {
    let (scheme, token) = value
        .split_once(' ')
        .ok_or_else(|| AuthError::Invalid("Authorization must use Bearer scheme".into()))?;
    let token = token.trim();
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return Err(AuthError::Invalid(
            "Authorization must contain a non-empty Bearer token".into(),
        ));
    }
    Ok(token)
}

fn parse_claims(token: &str) -> Result<Claims, AuthError> {
    let mut parts = token.split('.');
    let _header = parts.next();
    let payload = parts
        .next()
        .ok_or_else(|| AuthError::Invalid("JWT payload is missing".into()))?;
    if parts.next().is_none() || parts.next().is_some() {
        return Err(AuthError::Invalid("JWT must contain three segments".into()));
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .map_err(|error| AuthError::Invalid(format!("decode JWT payload: {error}")))?;
    serde_json::from_slice(&payload)
        .map_err(|error| AuthError::Invalid(format!("decode JWT claims: {error}")))
}

fn request_id<B>(request: &Request<B>) -> &str {
    request
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
}

fn now_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(super) async fn validate_with_iam(
    iam_address: &str,
    token: &str,
    request_id: &str,
) -> Result<(), AuthError> {
    let endpoint = iam_address
        .trim()
        .trim_start_matches("http://")
        .trim_end_matches('/');
    if endpoint.is_empty() || endpoint.contains('/') {
        return Err(AuthError::IamUnavailable(
            "IAM address must be an HTTP host:port".into(),
        ));
    }
    let stream = timeout(IAM_TIMEOUT, TcpStream::connect(endpoint))
        .await
        .map_err(|_| AuthError::IamUnavailable("connect timeout".into()))?
        .map_err(|error| AuthError::IamUnavailable(error.to_string()))?;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|error| AuthError::IamUnavailable(error.to_string()))?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(%error, "IAM validation connection closed");
        }
    });
    let request = Request::builder()
        .method("GET")
        .uri("/iam-server/v1/token/auth")
        .header(header::HOST, endpoint)
        .header("x-auth", token)
        .header("x-request-id", request_id)
        .body(Empty::<Bytes>::new())
        .map_err(|error| AuthError::IamUnavailable(error.to_string()))?;
    let response = timeout(IAM_TIMEOUT, sender.send_request(request))
        .await
        .map_err(|_| AuthError::IamUnavailable("request timeout".into()))?
        .map_err(|error| AuthError::IamUnavailable(error.to_string()))?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(AuthError::IamRejected(response.status().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(payload: &str) -> String {
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
        format!("e30.{payload}.signature")
    }

    #[tokio::test]
    async fn extracts_subject_from_bearer_token() {
        let auth = IngressAuthenticator::new(true, false, "", Duration::from_secs(30)).unwrap();
        let request = Request::builder()
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", token(r#"{"sub":"tenant-a","exp":0}"#)),
            )
            .body(())
            .unwrap();
        assert_eq!(
            auth.authenticate_request(&request).await.unwrap(),
            "tenant-a"
        );
    }

    #[tokio::test]
    async fn accepts_x_auth_compatibility_header() {
        let auth = IngressAuthenticator::new(true, false, "", Duration::from_secs(30)).unwrap();
        let request = Request::builder()
            .header("X-Auth", token(r#"{"sub":"tenant-a","exp":0}"#))
            .body(())
            .unwrap();
        assert_eq!(
            auth.authenticate_request(&request).await.unwrap(),
            "tenant-a"
        );
    }

    #[tokio::test]
    async fn accepts_matching_authorization_and_x_auth() {
        let auth = IngressAuthenticator::new(true, false, "", Duration::from_secs(30)).unwrap();
        let token = token(r#"{"sub":"tenant-a","exp":0}"#);
        let request = Request::builder()
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header("X-Auth", &token)
            .body(())
            .unwrap();
        assert_eq!(
            auth.authenticate_request(&request).await.unwrap(),
            "tenant-a"
        );
    }

    #[tokio::test]
    async fn rejects_conflicting_authorization_and_x_auth() {
        let auth = IngressAuthenticator::new(true, false, "", Duration::from_secs(30)).unwrap();
        let request = Request::builder()
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", token(r#"{"sub":"tenant-a","exp":0}"#)),
            )
            .header("X-Auth", token(r#"{"sub":"tenant-b","exp":0}"#))
            .body(())
            .unwrap();
        let error = auth.authenticate_request(&request).await.unwrap_err();
        assert!(matches!(error, AuthError::Ambiguous(_)));
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_identity_headers_without_bearer_token() {
        let auth = IngressAuthenticator::new(true, false, "", Duration::from_secs(30)).unwrap();
        let request = Request::builder()
            .header("x-adx-instance-id", "spoofed")
            .body(())
            .unwrap();
        assert!(matches!(
            auth.authenticate_request(&request).await,
            Err(AuthError::Missing)
        ));
    }

    #[tokio::test]
    async fn optional_auth_accepts_anonymous_but_validates_supplied_token() {
        let auth = IngressAuthenticator::new(true, false, "", Duration::from_secs(30)).unwrap();
        let anonymous = Request::new(());
        assert_eq!(
            auth.authenticate_request_with_policy(&anonymous, false)
                .await
                .unwrap(),
            ""
        );

        let authenticated = Request::builder()
            .header("X-Auth", token(r#"{"sub":"tenant-a","exp":0}"#))
            .body(())
            .unwrap();
        assert_eq!(
            auth.authenticate_request_with_policy(&authenticated, false)
                .await
                .unwrap(),
            "tenant-a"
        );

        let invalid = Request::builder()
            .header(header::AUTHORIZATION, "Bearer invalid")
            .body(())
            .unwrap();
        assert!(matches!(
            auth.authenticate_request_with_policy(&invalid, false).await,
            Err(AuthError::Invalid(_))
        ));
    }
}
