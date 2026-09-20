//! Internal service API. The caller is a Gateway that already authenticated the tenant.
use crate::*;
use adx_agent_core::{
    limits,
    transport::{RequestProgress, ServiceAuth},
};
use axum::{
    extract::{DefaultBodyLimit, Extension, Request, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
struct Service {
    dispatcher: Arc<Dispatcher>,
    auth: ServiceAuth,
    ready: Arc<AtomicBool>,
}
pub use adx_agent_core::dispatcher::{ReleaseInstanceRequest, ScopeRequest};
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Invalid(_) => StatusCode::BAD_REQUEST,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::NotReady(_) | Self::Unavailable(_) | Self::OutcomeUnknown(_) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
        };
        (status, Json(self)).into_response()
    }
}
async fn authorize(State(service): State<Service>, request: Request, next: Next) -> Response {
    if !service.auth.accepts(
        request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    ) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let progress = Arc::new(RequestProgress::default());
    let mut request = request;
    request.extensions_mut().insert(progress.clone());
    match tokio::time::timeout(service.dispatcher.request_timeout(), next.run(request)).await {
        Ok(response) => response,
        Err(_) => if progress.may_have_written() {
            Error::OutcomeUnknown(
                "request deadline exceeded; inspect the original Session/affinity state".into(),
            )
        } else {
            Error::Unavailable("Dispatcher request timed out before execution".into())
        }
        .into_response(),
    }
}
pub fn router(dispatcher: Arc<Dispatcher>, token: &str, ready: Arc<AtomicBool>) -> Result<Router> {
    let state = Service {
        dispatcher,
        auth: ServiceAuth::new(token).map_err(Error::Invalid)?,
        ready,
    };
    Ok(Router::new()
        .route("/internal/adx/v1/resolve", post(resolve))
        .route("/internal/adx/v1/release", post(release))
        .route("/internal/adx/v1/release-instance", post(release_instance))
        .layer(DefaultBodyLimit::max(limits::INTERNAL_REQUEST_BYTES))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize))
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(health))
        .with_state(state))
}
async fn health(State(s): State<Service>) -> StatusCode {
    if s.ready.load(Ordering::Acquire) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn resolve(
    State(s): State<Service>,
    Extension(progress): Extension<Arc<RequestProgress>>,
    headers: HeaderMap,
    Json(body): Json<ResolveRequest>,
) -> Result<Json<Target>> {
    let deadline_ms = headers
        .get(adx_agent_core::dispatcher::DEADLINE_HEADER)
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .ok_or_else(|| Error::Invalid("invalid Dispatcher deadline".into()))
        })
        .transpose()?;
    progress.start_write(); // Resolve may reserve an instance or bind affinity.
    s.dispatcher
        .resolve_with_deadline(&body, deadline_ms)
        .await
        .map(Json)
}
async fn release(
    State(s): State<Service>,
    Extension(progress): Extension<Arc<RequestProgress>>,
    Json(body): Json<ScopeRequest>,
) -> Result<StatusCode> {
    progress.start_write();
    s.dispatcher.release(&body.scope).await?;
    Ok(StatusCode::ACCEPTED)
}
async fn release_instance(
    State(s): State<Service>,
    Extension(progress): Extension<Arc<RequestProgress>>,
    Json(body): Json<ReleaseInstanceRequest>,
) -> Result<StatusCode> {
    progress.start_write();
    s.dispatcher
        .release_instance(&body.scope, &body.instance_id)
        .await?;
    Ok(StatusCode::ACCEPTED)
}
