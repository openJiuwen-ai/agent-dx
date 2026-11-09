//! Private service API. Authenticated Gateway supplies the verified tenant scope.
use crate::*;
use adx_agent_core::pool::*;
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
    let caller_deadline = match request.headers().get(DEADLINE_HEADER) {
        Some(value) => match value.to_str().ok().and_then(|v| v.parse::<u64>().ok()) {
            Some(deadline) => Some(deadline),
            None => return failure(Error::Invalid("invalid request deadline".into())),
        },
        None => None,
    };
    let deadline = adx_agent_core::transport::capped_deadline(caller_deadline, s.timeout);
    let remaining = adx_agent_core::transport::remaining_time(deadline);
    if remaining.is_zero() {
        return failure(Error::Unavailable("request deadline expired".into()));
    }
    let progress = Arc::new(RequestProgress::default());
    let mut request = request;
    request.extensions_mut().insert(progress.clone());
    request.extensions_mut().insert(deadline);
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
        .route("/internal/adx/v1/aliases/get", post(alias))
        .route("/internal/adx/v1/aliases/put", post(update_alias))
        .route("/internal/adx/v1/bindings/get", post(binding))
        .route("/internal/adx/v1/bindings/list", post(bindings))
        .route("/internal/adx/v1/bindings/delete", post(delete))
        .route("/internal/adx/v1/bindings/activate", post(activate))
        .route("/internal/adx/v1/bindings/resolve", post(resolve))
        .route("/internal/adx/v1/bindings/view", post(binding_view))
        .route("/internal/adx/v1/pools/put", post(put_pool))
        .route("/internal/adx/v1/pools/get", post(get_pool))
        .route("/internal/adx/v1/pools/status", post(pool_status))
        .route("/internal/adx/v1/pools/drain", post(drain_pool))
        .route("/internal/adx/v1/pools/reserve", post(reserve_pool))
        .route(
            "/internal/adx/v1/pools/pending_keys",
            post(pending_pool_keys),
        )
        .route(
            "/internal/adx/v1/pools/key_confirmed",
            post(confirm_pool_key),
        )
        .route("/internal/adx/v1/pools/claim", post(claim_pool))
        .route("/internal/adx/v1/pools/claims/get", post(lookup_pool_claim))
        .route("/internal/adx/v1/pools/commit", post(commit_pool))
        .route("/internal/adx/v1/pools/abort", post(abort_pool))
        .route("/internal/adx/v1/pools/retired", post(retired_pool_keys))
        .route(
            "/internal/adx/v1/pools/retired/ack",
            post(ack_retired_pool_key),
        )
        .layer(DefaultBodyLimit::max(limits::HTTP_JSON_BYTES))
        .route_layer(middleware::from_fn_with_state(s.clone(), authorize))
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route(
            "/health/ready",
            get(|State(s): State<Service>| async move {
                if s.activator.ready() {
                    StatusCode::NO_CONTENT
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .with_state(s))
}
async fn publish(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(r): Json<PublishRequest>,
) -> Response {
    p.start_write();
    match s.activator.publish(&r.tenant, &r.template).await {
        Ok(template) => Json(template).into_response(),
        Err(e) => failure(e),
    }
}
async fn template(State(s): State<Service>, Json(r): Json<TemplateRequest>) -> Response {
    match s.activator.template(&r.tenant, &r.name, &r.version).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => failure(e),
    }
}
async fn alias(State(s): State<Service>, Json(r): Json<AliasRequest>) -> Response {
    match s.activator.alias_config(&r).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn update_alias(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(r): Json<AliasUpdate>,
) -> Response {
    p.start_write();
    match s.activator.update_alias(&r).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn binding(State(s): State<Service>, Json(r): Json<ScopeRequest>) -> Response {
    match s.activator.binding(&r.scope).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => failure(e),
    }
}
async fn bindings(State(s): State<Service>, Json(r): Json<BindingList>) -> Response {
    match s.activator.list_bindings(&r).await {
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
    match s.activator.delete_binding(&r.scope).await {
        Ok(()) => Json(()).into_response(),
        Err(e) => failure(e),
    }
}
async fn activate(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    axum::Extension(deadline): axum::Extension<u64>,
    Json(r): Json<ActivationRequest>,
) -> Response {
    p.start_write();
    match s
        .activator
        .activate_with_parameters(
            &r.scope,
            r.expected_generation.as_deref(),
            deadline,
            r.bypass_cache,
            r.parameters.as_deref(),
        )
        .await
    {
        Ok(v) => Json(v).into_response(),
        Err(e) => failure(e),
    }
}

async fn resolve(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    axum::Extension(deadline): axum::Extension<u64>,
    Json(request): Json<ResolveRequest>,
) -> Response {
    p.start_write();
    match s.activator.resolve(&request, deadline).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn binding_view(State(s): State<Service>, Json(request): Json<ScopeRequest>) -> Response {
    match s.activator.binding_view(&request.scope).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}

async fn put_pool(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(request): Json<PoolPutRequest>,
) -> Response {
    p.start_write();
    match s.activator.put_pool(&request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn get_pool(State(s): State<Service>, Json(request): Json<PoolRequest>) -> Response {
    match s.activator.pool(&request.tenant, &request.pool_id).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn pool_status(State(s): State<Service>, Json(request): Json<PoolRequest>) -> Response {
    match s
        .activator
        .pool_status(&request.tenant, &request.pool_id)
        .await
    {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn drain_pool(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(request): Json<PoolRequest>,
) -> Response {
    p.start_write();
    match s
        .activator
        .drain_pool(&request.tenant, &request.pool_id)
        .await
    {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn reserve_pool(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(request): Json<ReserveRequest>,
) -> Response {
    p.start_write();
    match s.activator.reserve_pool_slot(&request).await {
        Ok(Some(value)) => Json(value).into_response(),
        Ok(None) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => failure(error),
    }
}
async fn pending_pool_keys(
    State(s): State<Service>,
    Json(request): Json<PendingKeysRequest>,
) -> Response {
    match s.activator.pending_pool_keys(&request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn confirm_pool_key(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(request): Json<KeyConfirmedRequest>,
) -> Response {
    p.start_write();
    match s.activator.confirm_pool_key(&request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn claim_pool(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(request): Json<ClaimRequest>,
) -> Response {
    p.start_write();
    match s.activator.claim_pool(&request).await {
        Ok(outcome @ ClaimOutcome::Aborted { .. }) => {
            (StatusCode::CONFLICT, Json(outcome)).into_response()
        }
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}

async fn lookup_pool_claim(
    State(s): State<Service>,
    Json(request): Json<ClaimLookupRequest>,
) -> Response {
    match s.activator.lookup_pool_claim(&request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn commit_pool(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(request): Json<CommitRequest>,
) -> Response {
    p.start_write();
    match s.activator.commit_pool(&request).await {
        Ok(outcome @ ClaimOutcome::Aborted { .. }) => {
            (StatusCode::CONFLICT, Json(outcome)).into_response()
        }
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn abort_pool(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(request): Json<AbortRequest>,
) -> Response {
    p.start_write();
    match s.activator.abort_pool(&request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn retired_pool_keys(
    State(s): State<Service>,
    Json(request): Json<RetiredRequest>,
) -> Response {
    match s.activator.retired_pool_keys(&request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn ack_retired_pool_key(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(request): Json<AckRetiredRequest>,
) -> Response {
    p.start_write();
    match s.activator.ack_retired_pool_key(&request).await {
        Ok(()) => Json(()).into_response(),
        Err(error) => failure(error),
    }
}
