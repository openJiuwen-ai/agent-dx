//! Workload grants share the existing Agent metadata authority and Redis repository.
use crate::{Activator, Error, Result};
use adx_agent_core::{
    activator::DEADLINE_HEADER,
    service_grants::*,
    transport::{capped_deadline, remaining_time, ServiceAuth},
};
use axum::{
    extract::{DefaultBodyLimit, Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn now() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|v| v.as_secs())
        .map_err(|_| Error::Unavailable("clock unavailable".into()))
}

impl Activator {
    pub async fn issue_service_grant(
        &self,
        request: &IssueServiceGrant,
    ) -> Result<IssuedServiceGrant> {
        if request.ttl_seconds == 0 || request.ttl_seconds > MAX_GRANT_SECONDS {
            return Err(Error::Invalid("invalid service credential lifetime".into()));
        }
        let timestamp = now()?;
        let expires = timestamp
            .checked_add(request.ttl_seconds)
            .ok_or_else(|| Error::Invalid("invalid service credential lifetime".into()))?;
        let credential = format!(
            "adxcg_{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let digest = credential_digest(&credential).map_err(Error::Invalid)?;
        let context = self
            .state
            .create_service_grant(
                &request.scope,
                &request.expected_generation,
                &digest,
                &request.purpose,
                expires,
                timestamp,
            )
            .await?;
        Ok(IssuedServiceGrant {
            credential,
            credential_digest: digest,
            context,
            expires_at_unix_seconds: expires,
        })
    }
    pub async fn verify_service_grant(
        &self,
        request: &VerifyServiceGrant,
    ) -> Result<ServiceGrantDecision> {
        let Ok(digest) = credential_digest(&request.credential) else {
            return Ok(ServiceGrantDecision { allowed: false });
        };
        Ok(ServiceGrantDecision {
            allowed: self
                .state
                .authorize_service_grant(&digest, &request.expected, now()?)
                .await?,
        })
    }
    pub async fn revoke_service_grant(&self, request: &RevokeServiceGrant) -> Result<()> {
        Ok(self
            .state
            .revoke_service_grant(&request.credential_digest)
            .await?)
    }
}

#[derive(Clone)]
struct GrantService {
    activator: Arc<Activator>,
    issuer: ServiceAuth,
    verifier: ServiceAuth,
    timeout: Duration,
}

async fn authorize(State(state): State<GrantService>, request: Request, next: Next) -> Response {
    let verification = request.uri().path() == "/internal/adx/v1/service-grants/verify";
    let authorization = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if !(state.issuer.accepts(authorization)
        || verification && state.verifier.accepts(authorization))
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let caller_deadline = match request.headers().get(DEADLINE_HEADER) {
        Some(value) => match value.to_str().ok().and_then(|v| v.parse().ok()) {
            Some(deadline) => Some(deadline),
            None => {
                return crate::server::failure(Error::Invalid("invalid request deadline".into()))
            }
        },
        None => None,
    };
    let duration = remaining_time(capped_deadline(caller_deadline, state.timeout));
    match tokio::time::timeout(duration, next.run(request)).await {
        Ok(response) => response,
        Err(_) => crate::server::failure(if verification {
            Error::Unavailable("service authorization timed out".into())
        } else {
            Error::OutcomeUnknown(
                "service credential write outcome unknown; do not blindly repeat issuance".into(),
            )
        }),
    }
}

/// Issuance/revocation use the existing private control credential; verification has a separate read-only credential.
pub fn router(
    activator: Arc<Activator>,
    issuer_token: &str,
    verification_token: &str,
    timeout: Duration,
) -> Result<Router> {
    if issuer_token == verification_token || timeout.is_zero() {
        return Err(Error::Invalid(
            "separate service grant verifier credential and positive timeout required".into(),
        ));
    }
    let state = GrantService {
        activator,
        issuer: ServiceAuth::new(issuer_token).map_err(Error::Invalid)?,
        verifier: ServiceAuth::new(verification_token).map_err(Error::Invalid)?,
        timeout,
    };
    Ok(Router::new()
        .route("/internal/adx/v1/service-grants/issue", post(issue))
        .route("/internal/adx/v1/service-grants/verify", post(verify))
        .route("/internal/adx/v1/service-grants/revoke", post(revoke))
        .layer(DefaultBodyLimit::max(
            adx_agent_core::limits::SERVICE_GRANT_JSON_BYTES,
        ))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize))
        .with_state(state))
}
async fn issue(
    State(state): State<GrantService>,
    Json(request): Json<IssueServiceGrant>,
) -> Response {
    match state.activator.issue_service_grant(&request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => crate::server::failure(error),
    }
}
async fn verify(
    State(state): State<GrantService>,
    Json(request): Json<VerifyServiceGrant>,
) -> Response {
    match state.activator.verify_service_grant(&request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => crate::server::failure(error),
    }
}
async fn revoke(
    State(state): State<GrantService>,
    Json(request): Json<RevokeServiceGrant>,
) -> Response {
    match state.activator.revoke_service_grant(&request).await {
        Ok(()) => Json(()).into_response(),
        Err(error) => crate::server::failure(error),
    }
}
