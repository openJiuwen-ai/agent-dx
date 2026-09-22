//! Private service API. Authenticated Gateway supplies the verified tenant scope.
use crate::*;
use adx_agent_core::{
    activator::*,
    limits,
    transport::{RequestProgress, ServiceAuth},
};
use axum::{
    extract::{DefaultBodyLimit, Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use std::time::Duration;

#[derive(Clone)]
struct Service {
    activator: Arc<Activator>,
    auth: ServiceAuth,
    timeout: Duration,
}
pub(crate) fn failure(error: Error) -> Response {
    let status = match error {
        Error::Invalid(_) => StatusCode::BAD_REQUEST,
        Error::NotFound => StatusCode::NOT_FOUND,
        Error::Conflict(_) => StatusCode::CONFLICT,
        Error::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, Json(error)).into_response()
}
async fn authorize(State(s): State<Service>, request: Request, next: Next) -> Response {
    if !s.auth.accepts(
        request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
    ) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let remaining = match request.headers().get(DEADLINE_HEADER) {
        Some(value) => match value.to_str().ok().and_then(|v| v.parse::<u64>().ok()) {
            Some(deadline) => {
                Duration::from_millis(deadline.saturating_sub(adx_agent_core::unix_time_millis()))
                    .min(s.timeout)
            }
            None => return failure(Error::Invalid("invalid request deadline".into())),
        },
        None => s.timeout,
    };
    if remaining.is_zero() {
        return failure(Error::Unavailable("request deadline expired".into()));
    }
    let progress = Arc::new(RequestProgress::default());
    let mut request = request;
    request.extensions_mut().insert(progress.clone());
    match tokio::time::timeout(remaining, next.run(request)).await {
        Ok(response) => response,
        Err(_) => failure(if progress.may_have_written() {
            Error::OutcomeUnknown(
                "operation response timed out; reuse the original identity".into(),
            )
        } else {
            Error::Unavailable("read timed out".into())
        }),
    }
}
pub fn router(activator: Arc<Activator>, token: &str, timeout: Duration) -> Result<Router> {
    if timeout.is_zero() {
        return Err(Error::Invalid("timeout must be positive".into()));
    }
    let s = Service {
        activator,
        auth: ServiceAuth::new(token).map_err(Error::Invalid)?,
        timeout,
    };
    Ok(Router::new()
        .route("/internal/adx/v1/templates/publish", post(publish))
        .route("/internal/adx/v1/templates/get", post(template))
        .route("/internal/adx/v1/environments/create", post(create))
        .route("/internal/adx/v1/environments/get", post(environment))
        .route("/internal/adx/v1/environments/list", post(environments))
        .route("/internal/adx/v1/environments/delete", post(delete))
        .route("/internal/adx/v1/environments/activate", post(activate))
        .layer(DefaultBodyLimit::max(limits::HTTP_JSON_BYTES))
        .route_layer(middleware::from_fn_with_state(s.clone(), authorize))
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(|| async { StatusCode::NO_CONTENT }))
        .with_state(s))
}
async fn publish(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(r): Json<PublishRequest>,
) -> Response {
    p.start_write();
    match s.activator.publish(&r.tenant, &r.template).await {
        Ok(()) => Json(()).into_response(),
        Err(e) => failure(e),
    }
}
async fn template(State(s): State<Service>, Json(r): Json<TemplateRequest>) -> Response {
    match s.activator.template(&r.tenant, &r.name, &r.version).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => failure(e),
    }
}
async fn create(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(r): Json<ScopeRequest>,
) -> Response {
    p.start_write();
    match s.activator.create_environment(r.scope).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => failure(e),
    }
}
async fn environment(State(s): State<Service>, Json(r): Json<ScopeRequest>) -> Response {
    match s.activator.environment(&r.scope).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => failure(e),
    }
}
async fn environments(State(s): State<Service>, Json(r): Json<EnvironmentList>) -> Response {
    match s.activator.list_environments(&r).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => failure(e),
    }
}
async fn delete(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(r): Json<ScopeRequest>,
) -> Response {
    p.start_write();
    match s.activator.delete_environment(&r.scope).await {
        Ok(()) => Json(()).into_response(),
        Err(e) => failure(e),
    }
}
async fn activate(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(r): Json<ActivationRequest>,
) -> Response {
    p.start_write();
    match s
        .activator
        .activate(&r.scope, r.expected_generation.as_deref())
        .await
    {
        Ok(v) => Json(v).into_response(),
        Err(e) => failure(e),
    }
}
