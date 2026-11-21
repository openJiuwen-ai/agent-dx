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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

/// The Activator instance, set once initialization (store connection,
/// environment reachability, cache construction) completes. Its presence is
/// the readiness latch: `/health/ready` reports 204 only while it is set and
/// the process is not draining. Runtime dependency failures after
/// initialization never unset it — a shared-dependency outage must not pull
/// every replica out of selection at once; callers retry instead.
pub type SharedActivator = Arc<OnceLock<Arc<Activator>>>;

#[derive(Clone)]
struct Service {
    activator: SharedActivator,
    auth: ServiceAuth,
    timeout: Duration,
    /// Set when the process begins shutting down: new work is rejected with
    /// an explicit error so Gateway callers retry another member, while
    /// already-received requests finish within the shutdown budget.
    draining: Arc<AtomicBool>,
}
impl Service {
    fn activator(&self) -> Result<Arc<Activator>> {
        self.activator
            .get()
            .cloned()
            .ok_or_else(|| Error::Unavailable("Activator initialization in progress".into()))
    }
    fn ready(&self) -> bool {
        self.activator.get().is_some() && !self.draining.load(Ordering::Acquire)
    }
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
    // Draining rejects new work before it is accepted: requests that slipped
    // in during endpoint-removal propagation get an explicit, retryable
    // error instead of being cut off by process exit. Already-accepted
    // requests (past this middleware) continue within the shutdown budget.
    if s.draining.load(Ordering::Acquire) {
        return failure(Error::Unavailable(
            "Activator is draining; retry another member".into(),
        ));
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
    // Backward-compatible entry point: the supplied Activator is treated as
    // initialized and not draining, so callers that construct one directly
    // (tests, embedded hosts) keep the previous always-ready behavior.
    let slot: SharedActivator = Arc::new(OnceLock::new());
    let _ = slot.set(activator);
    router_with_readiness(slot, Arc::new(AtomicBool::new(false)), token, timeout)
}

/// Build the service router around an initialization slot and a draining
/// flag, both owned by the process main. See [`SharedActivator`].
pub fn router_with_readiness(
    activator: SharedActivator,
    draining: Arc<AtomicBool>,
    token: &str,
    timeout: Duration,
) -> Result<Router> {
    if timeout.is_zero() {
        return Err(Error::Invalid("timeout must be positive".into()));
    }
    let s = Service {
        activator,
        auth: ServiceAuth::new(token).map_err(Error::Invalid)?,
        timeout,
        draining,
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
        .layer(DefaultBodyLimit::max(limits::HTTP_JSON_BYTES))
        .route_layer(middleware::from_fn_with_state(s.clone(), authorize))
        .route("/health/live", get(|| async { StatusCode::NO_CONTENT }))
        .route("/health/ready", get(ready))
        .with_state(s))
}

async fn ready(State(s): State<Service>) -> StatusCode {
    if s.ready() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn publish(
    State(s): State<Service>,
    axum::Extension(p): axum::Extension<Arc<RequestProgress>>,
    Json(r): Json<PublishRequest>,
) -> Response {
    p.start_write();
    let Ok(activator) = s.activator() else {
        return failure(Error::Unavailable(
            "Activator initialization in progress".into(),
        ));
    };
    match activator.publish(&r.tenant, &r.template).await {
        Ok(version) => Json(version).into_response(),
        Err(e) => failure(e),
    }
}
async fn template(State(s): State<Service>, Json(r): Json<TemplateRequest>) -> Response {
    let Ok(activator) = s.activator() else {
        return failure(Error::Unavailable(
            "Activator initialization in progress".into(),
        ));
    };
    match activator.template(&r.tenant, &r.name, &r.version).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => failure(e),
    }
}
async fn alias(State(s): State<Service>, Json(r): Json<AliasRequest>) -> Response {
    let Ok(activator) = s.activator() else {
        return failure(Error::Unavailable(
            "Activator initialization in progress".into(),
        ));
    };
    match activator.alias_config(&r).await {
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
    let Ok(activator) = s.activator() else {
        return failure(Error::Unavailable(
            "Activator initialization in progress".into(),
        ));
    };
    match activator.update_alias(&r).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn binding(State(s): State<Service>, Json(r): Json<ScopeRequest>) -> Response {
    let Ok(activator) = s.activator() else {
        return failure(Error::Unavailable(
            "Activator initialization in progress".into(),
        ));
    };
    match activator.binding(&r.scope).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => failure(e),
    }
}
async fn bindings(State(s): State<Service>, Json(r): Json<BindingList>) -> Response {
    let Ok(activator) = s.activator() else {
        return failure(Error::Unavailable(
            "Activator initialization in progress".into(),
        ));
    };
    match activator.list_bindings(&r).await {
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
    let Ok(activator) = s.activator() else {
        return failure(Error::Unavailable(
            "Activator initialization in progress".into(),
        ));
    };
    match activator.delete_binding(&r.scope).await {
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
    let Ok(activator) = s.activator() else {
        return failure(Error::Unavailable(
            "Activator initialization in progress".into(),
        ));
    };
    match activator
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
    let Ok(activator) = s.activator() else {
        return failure(Error::Unavailable(
            "Activator initialization in progress".into(),
        ));
    };
    match activator.resolve(&request, deadline).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
async fn binding_view(State(s): State<Service>, Json(request): Json<ScopeRequest>) -> Response {
    let Ok(activator) = s.activator() else {
        return failure(Error::Unavailable(
            "Activator initialization in progress".into(),
        ));
    };
    match activator.binding_view(&request.scope).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => failure(error),
    }
}
