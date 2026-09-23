use super::auth::IngressAuthenticator;
use super::connector::{DataPlaneL4Connector, H2ConnectStream};
use super::http_pool::{
    BackendHttpPool, BackendHttpPoolConfig, BackendHttpPoolError, BackendHttpPoolKey,
};
use super::path::parse_direct_path;
use super::resolver::{AccessKind, IngressRouteResolver, ResolveError, RouteHandle};
use super::reverse_proxy::{ProxyRoute, ReverseProxy, ReverseProxyConfig};
use crate::common::listener::accept_with_backoff;
use crate::common::protocol::{
    H_ACCESS_KIND, H_INSTANCE_ID, H_TARGET_IP, H_TARGET_PORT, H_WORKLOAD_ID,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{header, Request, Response, StatusCode};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::fmt::Write as _;
use std::io;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;

type BoxError = Box<dyn Error + Send + Sync>;
pub(super) type ProxyBody = UnsyncBoxBody<Bytes, BoxError>;
const L4_COPY_BUFFER_SIZE: usize = 64 * 1024;
const COMMAND_WATCH_PATH: &str = "/api/sandbox/v1/commands/watch";
const EXECD_COMMAND_PORT: u16 = 50090;
const SLOW_REQUEST_LOG_THRESHOLD_US: u64 = 100_000;
const REQUEST_DURATION_BUCKETS_US: [(&str, u64); 7] = [
    ("0.001", 1_000),
    ("0.002", 2_000),
    ("0.005", 5_000),
    ("0.010", 10_000),
    ("0.025", 25_000),
    ("0.050", 50_000),
    ("0.100", 100_000),
];

#[derive(Debug, Clone, Copy)]
pub struct CommandWatchConfig {
    pub max_subscriptions_per_connection: usize,
    pub queue_capacity: usize,
    pub max_frame_bytes: usize,
    pub ping_interval: std::time::Duration,
}

impl Default for CommandWatchConfig {
    fn default() -> Self {
        Self {
            max_subscriptions_per_connection: 4096,
            queue_capacity: 256,
            max_frame_bytes: 1024 * 1024,
            ping_interval: std::time::Duration::from_secs(20),
        }
    }
}

#[derive(Default)]
struct CommandWatchMetrics {
    connections: std::sync::atomic::AtomicU64,
    subscriptions: std::sync::atomic::AtomicU64,
    sandboxes: std::sync::atomic::AtomicU64,
    reconnects: std::sync::atomic::AtomicU64,
    auth_failures: std::sync::atomic::AtomicU64,
    downstream_streams: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
struct IngressRequestMetrics {
    in_flight: AtomicU64,
    total: AtomicU64,
    responses_2xx: AtomicU64,
    responses_3xx: AtomicU64,
    responses_4xx: AtomicU64,
    responses_5xx: AtomicU64,
    responses_other: AtomicU64,
    duration_us_total: AtomicU64,
    duration_buckets: [AtomicU64; REQUEST_DURATION_BUCKETS_US.len()],
    duration_overflow: AtomicU64,
}

impl IngressRequestMetrics {
    fn observe(&self, status: StatusCode, duration_us: u64) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.duration_us_total
            .fetch_add(duration_us, Ordering::Relaxed);
        match status.as_u16() / 100 {
            2 => &self.responses_2xx,
            3 => &self.responses_3xx,
            4 => &self.responses_4xx,
            5 => &self.responses_5xx,
            _ => &self.responses_other,
        }
        .fetch_add(1, Ordering::Relaxed);
        if let Some(index) = REQUEST_DURATION_BUCKETS_US
            .iter()
            .position(|(_, upper)| duration_us <= *upper)
        {
            self.duration_buckets[index].fetch_add(1, Ordering::Relaxed);
        } else {
            self.duration_overflow.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn prometheus(&self) -> String {
        let total = self.total.load(Ordering::Relaxed);
        let mut output = format!(
            "data_plane_ingress_http_requests_in_flight {}\n\
data_plane_ingress_http_requests_total {}\n\
data_plane_ingress_http_responses_total{{class=\"2xx\"}} {}\n\
data_plane_ingress_http_responses_total{{class=\"3xx\"}} {}\n\
data_plane_ingress_http_responses_total{{class=\"4xx\"}} {}\n\
data_plane_ingress_http_responses_total{{class=\"5xx\"}} {}\n\
data_plane_ingress_http_responses_total{{class=\"other\"}} {}\n",
            self.in_flight.load(Ordering::Relaxed),
            total,
            self.responses_2xx.load(Ordering::Relaxed),
            self.responses_3xx.load(Ordering::Relaxed),
            self.responses_4xx.load(Ordering::Relaxed),
            self.responses_5xx.load(Ordering::Relaxed),
            self.responses_other.load(Ordering::Relaxed),
        );
        let mut cumulative = 0;
        for ((label, _), bucket) in REQUEST_DURATION_BUCKETS_US
            .iter()
            .zip(self.duration_buckets.iter())
        {
            cumulative += bucket.load(Ordering::Relaxed);
            let _ = writeln!(
                output,
                "data_plane_ingress_http_request_duration_seconds_bucket{{le=\"{label}\"}} {cumulative}"
            );
        }
        let _ = writeln!(
            output,
            "data_plane_ingress_http_request_duration_seconds_bucket{{le=\"+Inf\"}} {total}"
        );
        let _ = writeln!(
            output,
            "data_plane_ingress_http_request_duration_seconds_sum {:.6}",
            self.duration_us_total.load(Ordering::Relaxed) as f64 / 1_000_000.0
        );
        let _ = writeln!(
            output,
            "data_plane_ingress_http_request_duration_seconds_count {total}"
        );
        output
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressSecurity {
    Plaintext,
    Tls,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaticRoute {
    Exact(String),
    Prefix(String),
}

impl StaticRoute {
    pub fn matches(&self, path: &str) -> bool {
        match self {
            Self::Exact(route) => path == route,
            Self::Prefix(route) => path
                .strip_prefix(route)
                .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with('/')),
        }
    }
}

impl FromStr for StaticRoute {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        let (kind, path) = value.split_once(':').ok_or_else(|| {
            format!("static route {value:?} must use exact:/path or prefix:/path")
        })?;
        if !path.starts_with('/') || path.contains('?') || path.contains('#') {
            return Err(format!(
                "static route path {path:?} must be an absolute URI path"
            ));
        }
        match kind.trim().to_ascii_lowercase().as_str() {
            "exact" => Ok(Self::Exact(path.to_owned())),
            "prefix" if path == "/" => Err(
                "prefix:/ would capture the whole ingress; use explicit prefixes instead".into(),
            ),
            "prefix" => Ok(Self::Prefix(path.trim_end_matches('/').to_owned())),
            _ => Err(format!(
                "static route {value:?} has unsupported match kind {kind:?}"
            )),
        }
    }
}

pub fn parse_static_routes(value: &str) -> Result<Vec<StaticRoute>, String> {
    let routes = value
        .split(',')
        .map(str::trim)
        .filter(|route| !route.is_empty())
        .map(StaticRoute::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    if routes.is_empty() {
        return Err("at least one control-plane static route is required".into());
    }
    Ok(routes)
}

#[derive(Clone)]
pub struct Ingress {
    #[cfg(feature = "agent-api")]
    pub(super) agent_api: Option<Arc<super::agent_api::AgentApi>>,
    #[cfg(feature = "agent-api")]
    inline_api: Option<Arc<super::inline_api::InlineApi>>,
    #[cfg(feature = "agent-api")]
    sandbox_api: Option<Arc<super::sandbox_api::SandboxApi>>,
    resolver: Arc<IngressRouteResolver>,
    connector: DataPlaneL4Connector,
    http_pool: BackendHttpPool,
    authenticator: IngressAuthenticator,
    default_direct_port: u16,
    default_tunnel_port: u16,
    frontend_address: String,
    reverse_proxy: ReverseProxy,
    proxy_routes: Arc<Vec<ProxyRoute>>,
    control_plane_routes: Arc<Vec<StaticRoute>>,
    draining: Arc<AtomicBool>,
    active_sessions: Arc<AtomicUsize>,
    sessions: Arc<Mutex<HashMap<Uuid, ActiveSession>>>,
    allowed_client_networks: Arc<Vec<ipnet::IpNet>>,
    allow_any_client: bool,
    forced_close_total: Arc<std::sync::atomic::AtomicU64>,
    command_watch: CommandWatchHub,
    command_watch_config: CommandWatchConfig,
    command_watch_metrics: Arc<CommandWatchMetrics>,
    request_metrics: Arc<IngressRequestMetrics>,
}

impl Ingress {
    pub fn new(
        resolver: Arc<IngressRouteResolver>,
        connector: DataPlaneL4Connector,
        authenticator: IngressAuthenticator,
        default_direct_port: u16,
        default_tunnel_port: u16,
        frontend_address: impl Into<String>,
        control_plane_routes: Vec<StaticRoute>,
    ) -> Self {
        Self {
            #[cfg(feature = "agent-api")]
            agent_api: None,
            #[cfg(feature = "agent-api")]
            inline_api: None,
            #[cfg(feature = "agent-api")]
            sandbox_api: None,
            resolver,
            connector,
            http_pool: BackendHttpPool::new(BackendHttpPoolConfig::default()),
            authenticator,
            default_direct_port,
            default_tunnel_port,
            frontend_address: frontend_address.into(),
            reverse_proxy: ReverseProxy::new(ReverseProxyConfig::default()),
            proxy_routes: Arc::new(Vec::new()),
            control_plane_routes: Arc::new(control_plane_routes),
            draining: Arc::new(AtomicBool::new(false)),
            active_sessions: Arc::new(AtomicUsize::new(0)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            allowed_client_networks: Arc::new(Vec::new()),
            allow_any_client: false,
            forced_close_total: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            command_watch: CommandWatchHub::default(),
            command_watch_config: CommandWatchConfig::default(),
            command_watch_metrics: Arc::new(CommandWatchMetrics::default()),
            request_metrics: Arc::new(IngressRequestMetrics::default()),
        }
    }

    #[cfg(feature = "agent-api")]
    pub fn with_agent_api(mut self, api: Arc<super::agent_api::AgentApi>) -> Self {
        self.agent_api = Some(api);
        self
    }

    #[cfg(feature = "agent-api")]
    pub fn with_inline_api(mut self, api: Arc<super::inline_api::InlineApi>) -> Self {
        self.inline_api = Some(api);
        self
    }

    #[cfg(feature = "agent-api")]
    pub fn with_sandbox_api(mut self, api: Arc<super::sandbox_api::SandboxApi>) -> Self {
        self.sandbox_api = Some(api);
        self
    }

    pub fn with_command_watch_config(mut self, config: CommandWatchConfig) -> Self {
        self.command_watch_config = config;
        self.command_watch = CommandWatchHub::new(config, Arc::clone(&self.command_watch_metrics));
        self
    }

    pub fn with_backend_http_pool_config(mut self, config: BackendHttpPoolConfig) -> Self {
        self.http_pool = BackendHttpPool::new(config);
        self
    }

    pub fn with_reverse_proxy_config(mut self, config: ReverseProxyConfig) -> Self {
        self.reverse_proxy = ReverseProxy::new(config);
        self
    }

    pub fn with_proxy_routes(mut self, routes: Vec<ProxyRoute>) -> Self {
        self.proxy_routes = Arc::new(routes);
        self
    }

    fn application_route<B>(&self, request: &Request<B>) -> Option<&ProxyRoute> {
        self.proxy_routes
            .iter()
            .filter(|route| route.matches(request))
            .max_by_key(|route| (route.host.is_some(), route.path_prefix.len()))
    }

    pub fn with_client_acl(mut self, networks: Vec<ipnet::IpNet>, allow_any_client: bool) -> Self {
        self.allowed_client_networks = Arc::new(networks);
        self.allow_any_client = allow_any_client;
        self
    }

    pub fn peer_allowed(&self, peer: std::net::IpAddr) -> bool {
        self.allow_any_client
            || self
                .allowed_client_networks
                .iter()
                .any(|network| network.contains(&peer))
    }

    pub fn ready(&self) -> bool {
        self.resolver.ready() && !self.draining.load(Ordering::Acquire)
    }

    pub fn start_drain(&self) {
        self.draining.store(true, Ordering::Release);
        self.http_pool.clear();
    }

    pub fn active_sessions(&self) -> usize {
        self.active_sessions.load(Ordering::Relaxed)
    }

    pub fn physical_connections(&self) -> usize {
        self.connector.physical_connection_count()
    }

    pub fn backend_http_pool_metrics(&self) -> super::http_pool::BackendHttpPoolMetrics {
        self.http_pool.metrics()
    }

    pub(super) async fn resolve_route(
        &self,
        instance_id: &str,
        target_port: u16,
        access_kind: AccessKind,
        request_id: String,
    ) -> Result<RouteHandle, IngressOpenError> {
        if self.draining.load(Ordering::Acquire) {
            return Err(IngressOpenError::Draining);
        }
        let request_id = if request_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            request_id
        };
        Ok(self
            .resolver
            .resolve(instance_id, target_port, access_kind, request_id)
            .await?)
    }

    async fn open_resolved_stream(
        &self,
        route: RouteHandle,
        tenant_id: &str,
    ) -> Result<IngressStream, IngressOpenError> {
        self.authorize(&route, tenant_id)?;
        self.open_authorized_stream(route)
            .await
            .map_err(IngressOpenError::Connect)
    }

    pub(super) async fn open_authorized_stream(
        &self,
        route: RouteHandle,
    ) -> io::Result<IngressStream> {
        self.open_authorized_stream_with_activity(route, false)
            .await
    }

    async fn open_authorized_stream_with_activity(
        &self,
        route: RouteHandle,
        passive: bool,
    ) -> io::Result<IngressStream> {
        if self.draining.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Ingress is draining",
            ));
        }
        let session_id = Uuid::new_v4();
        let (cancel, cancelled) = watch::channel(false);
        let guard = SessionGuard {
            session_id,
            active_sessions: self.active_sessions.clone(),
            sessions: self.sessions.clone(),
        };
        self.active_sessions.fetch_add(1, Ordering::Relaxed);
        self.sessions.lock().unwrap().insert(
            session_id,
            ActiveSession {
                instance_id: route.target.instance_id.clone(),
                workload_id: route.target.workload_id.clone(),
                relay_address: route.relay_address.clone(),
                target_ip: route.target.target_ip,
                cancel,
            },
        );
        if !self.resolver.route_is_current(&route) {
            drop(guard);
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "route changed while opening the stream",
            ));
        }
        let stream = self
            .connector
            .connect_stream_with_activity(
                &route.relay_address,
                &route.target,
                cancelled.clone(),
                passive,
            )
            .await?;
        Ok(IngressStream {
            stream,
            cancelled,
            _guard: guard,
        })
    }

    pub async fn open_l4_stream(
        &self,
        instance_id: &str,
        target_port: u16,
        access_kind: AccessKind,
        token: &str,
        request_id: String,
    ) -> Result<IngressStream, IngressOpenError> {
        let route = self
            .resolve_route(instance_id, target_port, access_kind, request_id.clone())
            .await?;
        let tenant_id = self
            .authenticator
            .authenticate_token_with_policy(
                token,
                &request_id,
                self.auth_required(access_kind, &route),
            )
            .await?;
        self.open_resolved_stream(route, &tenant_id).await
    }

    fn auth_required(&self, access_kind: AccessKind, route: &RouteHandle) -> bool {
        effective_auth_required(access_kind, route)
    }

    pub async fn run_route_reconciler(
        self: Arc<Self>,
        mut changes: broadcast::Receiver<super::route_store::RouteChange>,
    ) {
        loop {
            match changes.recv().await {
                Ok(change) => self.apply_route_change(change),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        skipped,
                        "route change consumer lagged; closing all active sessions"
                    );
                    self.cancel_all_sessions();
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    }

    fn apply_route_change(&self, change: super::route_store::RouteChange) {
        {
            let sessions = self.sessions.lock().unwrap();
            for session in sessions
                .values()
                .filter(|session| session.instance_id == change.instance_id)
            {
                let still_current = change.current.as_ref().is_some_and(|route| {
                    route.environment_status.code == 3
                        && route.sandbox_id == session.workload_id
                        && route.relay_address == session.relay_address
                        && route.sandbox_ip.parse().ok() == Some(session.target_ip)
                });
                if !still_current && !*session.cancel.borrow() {
                    self.forced_close_total.fetch_add(1, Ordering::Relaxed);
                    let _ = session.cancel.send(true);
                }
            }
        }
        self.http_pool.invalidate_instance(&change.instance_id);
    }

    fn cancel_all_sessions(&self) {
        self.http_pool.clear();
        for session in self.sessions.lock().unwrap().values() {
            if !*session.cancel.borrow() {
                self.forced_close_total.fetch_add(1, Ordering::Relaxed);
                let _ = session.cancel.send(true);
            }
        }
    }

    pub(super) fn authorize(
        &self,
        route: &RouteHandle,
        tenant_id: &str,
    ) -> Result<(), IngressOpenError> {
        if tenant_id.is_empty() {
            return Ok(());
        }
        if tenant_id.is_empty() || route.tenant_id.is_empty() || tenant_id != route.tenant_id {
            return Err(IngressOpenError::Forbidden);
        }
        Ok(())
    }

    pub async fn serve_http(
        self: Arc<Self>,
        listener: TcpListener,
        shutdown: watch::Receiver<bool>,
    ) -> io::Result<()> {
        self.serve_http_inner(listener, None, IngressSecurity::Plaintext, shutdown)
            .await
    }

    pub async fn serve_http_tls(
        self: Arc<Self>,
        listener: TcpListener,
        acceptor: TlsAcceptor,
        shutdown: watch::Receiver<bool>,
    ) -> io::Result<()> {
        self.serve_http_inner(listener, Some(acceptor), IngressSecurity::Tls, shutdown)
            .await
    }

    async fn serve_http_inner(
        self: Arc<Self>,
        listener: TcpListener,
        tls_acceptor: Option<TlsAcceptor>,
        ingress_security: IngressSecurity,
        mut shutdown: watch::Receiver<bool>,
    ) -> io::Result<()> {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                (stream, peer) = accept_with_backoff(&listener, "ingress-ingress") => {
                    if !self.peer_allowed(peer.ip()) {
                        tracing::warn!(
                            target: "adx_audit",
                            event = "ingress_acl",
                            decision = "deny",
                            peer = %peer,
                            reason = "source_outside_allowed_cidrs",
                            "Ingress ingress denied"
                        );
                        continue;
                    }
                    let gateway = self.clone();
                    let tls_acceptor = tls_acceptor.clone();
                    tokio::spawn(async move {
                        match tls_acceptor {
                            Some(acceptor) => {
                                if let Some(stream) = accept_ingress_tls(acceptor, stream, peer).await {
                                    gateway
                                        .serve_http_connection(stream, peer, ingress_security)
                                        .await;
                                }
                            }
                            None => gateway
                                .serve_http_connection(stream, peer, ingress_security)
                                .await,
                        }
                    });
                }
            }
        }
    }

    async fn serve_http_connection<T>(
        self: Arc<Self>,
        stream: T,
        peer: std::net::SocketAddr,
        ingress_security: IngressSecurity,
    ) where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let gateway = self;
        let service =
            service_fn(move |request| gateway.clone().handle_http(request, ingress_security, peer));
        if let Err(error) = hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .with_upgrades()
            .await
        {
            tracing::debug!(%peer, %error, "Ingress HTTP connection closed");
        }
    }

    pub async fn serve_health(
        self: Arc<Self>,
        listener: TcpListener,
        mut shutdown: watch::Receiver<bool>,
    ) -> io::Result<()> {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                (stream, peer) = accept_with_backoff(&listener, "ingress-health") => {
                    let gateway = self.clone();
                    tokio::spawn(async move {
                        let service = service_fn(move |request: Request<Incoming>| {
                            let response = gateway.health_response(request.uri().path());
                            async move { Ok::<_, Infallible>(response) }
                        });
                        if let Err(error) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await
                        {
                            tracing::debug!(%peer, %error, "Ingress health connection closed");
                        }
                    });
                }
            }
        }
    }

    async fn handle_http(
        self: Arc<Self>,
        mut request: Request<Incoming>,
        ingress_security: IngressSecurity,
        peer: std::net::SocketAddr,
    ) -> Result<Response<ProxyBody>, Infallible> {
        let trace = adx_observability::trace::Trace::remote(
            "ingress.http",
            request
                .headers()
                .get("traceparent")
                .and_then(|v| v.to_str().ok()),
            request
                .headers()
                .get("tracestate")
                .and_then(|v| v.to_str().ok()),
        );
        trace
            .run(async {
                adx_observability::trace::inject_headers(request.headers_mut());
                let response = self
                    .handle_http_inner(request, ingress_security, peer)
                    .await?;
                if response.status().is_server_error() {
                    adx_observability::trace::error();
                }
                Ok(response)
            })
            .await
    }

    async fn handle_http_inner(
        self: Arc<Self>,
        mut request: Request<Incoming>,
        ingress_security: IngressSecurity,
        peer: std::net::SocketAddr,
    ) -> Result<Response<ProxyBody>, Infallible> {
        let _in_flight = AtomicMetricGuard::new(&self.request_metrics.in_flight);
        let started = Instant::now();
        let request_id = ensure_request_id(&mut request);
        let method = request.method().to_string();
        let path = request.uri().path().to_owned();
        let (access_kind, instance_id, target_port) = self.access_fields(&request);
        adx_observability::trace::attribute("instance.id", instance_id.clone());
        adx_observability::trace::attribute("http.request.method", method.clone());
        #[cfg(feature = "agent-api")]
        let agent_data_error = self
            .prepare_agent_data(&mut request, ingress_security)
            .await
            .err();
        #[cfg(not(feature = "agent-api"))]
        let agent_data_error: Option<Response<ProxyBody>> = None;
        #[cfg(feature = "agent-api")]
        let environment_notice = request
            .extensions()
            .get::<super::agent_access::EnvironmentNotice>()
            .cloned();
        #[cfg(feature = "agent-api")]
        let agent_forward = environment_notice.is_some()
            || request
                .extensions()
                .get::<super::agent_access::InlineForward>()
                .is_some();
        #[cfg(not(feature = "agent-api"))]
        let agent_forward = false;
        let response = if let Some(response) = agent_data_error {
            response
        } else if agent_forward {
            // A selected Sandbox is already a data-plane target. Its ID must never
            // be interpreted again as a public management or configured proxy path.
            self.proxy_direct(request, ingress_security).await
        } else {
            match request.uri().path() {
                #[cfg(feature = "agent-api")]
                path if super::inline_api::InlineApi::matches(path)
                    && self.inline_api.is_some() =>
                {
                    if ingress_security == IngressSecurity::Plaintext {
                        tls_required()
                    } else {
                        self.inline_api
                            .as_ref()
                            .expect("matched configured inline API")
                            .management(request, &self)
                            .await
                    }
                }
                #[cfg(feature = "agent-api")]
                path if super::agent_api::AgentApi::matches(path) && self.agent_api.is_some() => {
                    if ingress_security == IngressSecurity::Plaintext {
                        tls_required()
                    } else {
                        match self
                            .authenticator
                            .authenticate_request_with_policy(&request, true)
                            .await
                        {
                            Ok(tenant) if !tenant.is_empty() => {
                                self.agent_api
                                    .as_ref()
                                    .expect("configured Agent API")
                                    .management(request, &tenant)
                                    .await
                            }
                            Ok(_) => response_text(
                                StatusCode::UNAUTHORIZED,
                                "tenant authentication required",
                            ),
                            Err(error) => response_text(error.status(), &error.to_string()),
                        }
                    }
                }
                #[cfg(feature = "agent-api")]
                path if super::sandbox_api::SandboxApi::matches(path)
                    && self.sandbox_api.is_some() =>
                {
                    if ingress_security == IngressSecurity::Plaintext {
                        tls_required()
                    } else {
                        let api = self.sandbox_api.as_ref().expect("configured Sandbox API");
                        if api.service_authorized(request.headers()) {
                            api.handle(request, None).await
                        } else {
                            match self
                                .authenticator
                                .authenticate_request_with_policy(&request, true)
                                .await
                            {
                                Ok(tenant) if !tenant.is_empty() => {
                                    api.handle(request, Some(&tenant)).await
                                }
                                Ok(_) => response_text(
                                    StatusCode::UNAUTHORIZED,
                                    "tenant authentication required",
                                ),
                                Err(error) => response_text(error.status(), &error.to_string()),
                            }
                        }
                    }
                }
                COMMAND_WATCH_PATH => {
                    self.handle_command_watch(&mut request, ingress_security)
                        .await
                }
                _ if request.method() == http::Method::CONNECT => {
                    self.handle_connect(&mut request, ingress_security, peer)
                        .await
                }
                _ if self.application_route(&request).is_some() => {
                    if ingress_security == IngressSecurity::Plaintext {
                        tls_required()
                    } else {
                        let route = self.application_route(&request).expect("route matched");
                        self.reverse_proxy.proxy(request, route, peer).await
                    }
                }
                path if self.is_control_plane_path(path) => {
                    self.proxy_control_plane(request, ingress_security, peer)
                        .await
                }
                _ => self.proxy_direct(request, ingress_security).await,
            }
        };
        #[cfg(feature = "agent-api")]
        let response = {
            let mut response = response;
            if let Some(notice) = environment_notice {
                notice.apply(response.headers_mut());
            }
            response
        };
        let status = response.status();
        let duration_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.request_metrics.observe(status, duration_us);
        let security = ingress_security_name(ingress_security);
        if should_log_access(status, duration_us) {
            tracing::info!(
                target: "adx_access",
                event = "request",
                request_id = %request_id,
                peer = %peer,
                method = %method,
                path = %path,
                ingress_security = security,
                access_kind = access_kind,
                instance_id = %instance_id,
                target_port,
                status = status.as_u16(),
                duration_us,
                "Ingress access"
            );
        }
        if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
            tracing::warn!(
                target: "adx_audit",
                event = "authorization",
                decision = "deny",
                request_id = %request_id,
                peer = %peer,
                method = %method,
                path = %path,
                access_kind = access_kind,
                instance_id = %instance_id,
                target_port,
                status = status.as_u16(),
                "Ingress authorization denied"
            );
        }
        Ok(response)
    }

    #[cfg(feature = "agent-api")]
    async fn prepare_agent_data<B>(
        &self,
        request: &mut Request<B>,
        ingress_security: IngressSecurity,
    ) -> Result<(), Response<ProxyBody>> {
        use super::agent_access::{AccessRequest, InlineForward};
        use adx_agent_core::target::Target;
        let Some(access) = AccessRequest::parse(request)
            .map_err(|error| super::agent_response::error_response(error, None))?
        else {
            return Ok(());
        };
        if ingress_security == IngressSecurity::Plaintext {
            return Err(tls_required());
        }
        if let Target::Instance(id) = &access.target {
            let api = self
                .inline_api
                .as_ref()
                .ok_or_else(|| response_text(StatusCode::NOT_FOUND, "inline access is disabled"))?;
            let identity = api
                .authenticate_data(request)
                .await
                .map_err(|_| response_text(StatusCode::UNAUTHORIZED, "authentication failed"))?;
            let port = access.port.unwrap_or(18092);
            *request.uri_mut() = format!("/{id}/{port}{}", access.backend_uri)
                .parse()
                .map_err(|_| response_text(StatusCode::BAD_REQUEST, "invalid instance target"))?;
            if access.protocol == adx_agent_core::Protocol::Http {
                request.headers_mut().remove(header::AUTHORIZATION);
                if request
                    .headers()
                    .get("x-forwarded-proto")
                    .is_none_or(|v| v.is_empty())
                {
                    request
                        .headers_mut()
                        .insert("x-forwarded-proto", http::HeaderValue::from_static("https"));
                }
            }
            request.extensions_mut().insert(identity);
            request.extensions_mut().insert(InlineForward {
                backend_uri: access.backend_uri,
            });
        } else {
            let api = self.agent_api.as_ref().ok_or_else(|| {
                response_text(StatusCode::NOT_FOUND, "managed access is disabled")
            })?;
            let identity = self
                .authenticator
                .authenticate_request_identity_with_policy(request, true)
                .await
                .map_err(|error| response_text(error.status(), &error.to_string()))?;
            if identity.tenant_id.is_empty() {
                return Err(response_text(
                    StatusCode::UNAUTHORIZED,
                    "tenant authentication required",
                ));
            }
            let tenant = identity.tenant_id.clone();
            request.extensions_mut().insert(identity);
            api.prepare_data(request, &tenant, access)
                .await
                .map_err(|error| super::agent_response::error_response(error, None))?;
        }
        Ok(())
    }

    fn access_fields<B>(&self, request: &Request<B>) -> (&'static str, String, u16) {
        if request.method() == http::Method::CONNECT {
            let target = external_connect_target(request).ok();
            let access_kind = external_access_kind(request)
                .ok()
                .map(AccessKind::as_str)
                .unwrap_or("invalid-connect");
            return (
                access_kind,
                target
                    .as_ref()
                    .map(|value| value.0.clone())
                    .unwrap_or_default(),
                target.map(|value| value.1).unwrap_or_default(),
            );
        }
        if self.application_route(request).is_some() {
            return ("reverse-proxy", String::new(), 0);
        }
        if self.is_control_plane_path(request.uri().path()) {
            return ("control-plane", String::new(), 0);
        }
        parse_direct_path(
            request.uri().path(),
            self.default_direct_port,
            self.default_tunnel_port,
        )
        .map(|parsed| {
            (
                parsed.access_kind.as_str(),
                parsed.instance_id,
                parsed.target_port,
            )
        })
        .unwrap_or(("unmatched", String::new(), 0))
    }

    fn is_control_plane_path(&self, path: &str) -> bool {
        self.control_plane_routes
            .iter()
            .any(|route| route.matches(path))
    }

    async fn proxy_control_plane(
        &self,
        request: Request<Incoming>,
        ingress_security: IngressSecurity,
        peer: std::net::SocketAddr,
    ) -> Response<ProxyBody> {
        if ingress_security == IngressSecurity::Plaintext {
            return tls_required();
        }
        let route = ProxyRoute {
            name: "apiserver".into(),
            path_prefix: "/".into(),
            upstream: format!("http://{}", self.frontend_address),
            strip_prefix: false,
            host: None,
        };
        self.reverse_proxy.proxy(request, &route, peer).await
    }

    fn health_response(&self, path: &str) -> Response<ProxyBody> {
        match path {
            "/healthz" => plain(StatusCode::OK, "ok"),
            "/readyz" if self.ready() => plain(StatusCode::OK, "ready"),
            "/readyz" => plain(StatusCode::SERVICE_UNAVAILABLE, "not ready"),
            "/metrics" => plain(StatusCode::OK, &{
                let pool = self.http_pool.metrics();
                format!(
                    "data_plane_ingress_ready {}\ndata_plane_ingress_route_cache_entries {}\ndata_plane_ingress_route_watch_revision {}\ndata_plane_ingress_route_watch_lag_seconds {}\ndata_plane_ingress_route_point_get_total {}\ndata_plane_ingress_active_sessions {}\ndata_plane_ingress_h2_physical_connections {}\ndata_plane_ingress_route_forced_close_total {}\ndata_plane_ingress_backend_http_idle_connections {}\ndata_plane_ingress_backend_http_opened_total {}\ndata_plane_ingress_backend_http_reused_total {}\ndata_plane_ingress_backend_http_discarded_total {}\ndata_plane_ingress_backend_http_acquire_timeouts_total {}\n{}command_watch_connections {}\ncommand_watch_subscriptions {}\ncommand_watch_sandboxes {}\ncommand_watch_reconnects {}\ncommand_watch_auth_failures {}\ncommand_watch_downstream_streams {}\n",
                    usize::from(self.ready()),
                    self.resolver.cache_len(),
                    self.resolver.watch_revision(),
                    self.resolver.watch_lag_seconds(),
                    self.resolver.point_get_total(),
                    self.active_sessions(),
                    self.physical_connections(),
                    self.forced_close_total.load(Ordering::Relaxed),
                    pool.idle_connections,
                    pool.opened_total,
                    pool.reused_total,
                    pool.discarded_total,
                    pool.acquire_timeouts_total,
                    self.request_metrics.prometheus(),
                    self.command_watch_metrics.connections.load(Ordering::Relaxed),
                    self.command_watch_metrics.subscriptions.load(Ordering::Relaxed),
                    self.command_watch_metrics.sandboxes.load(Ordering::Relaxed),
                    self.command_watch_metrics.reconnects.load(Ordering::Relaxed),
                    self.command_watch_metrics.auth_failures.load(Ordering::Relaxed),
                    self.command_watch_metrics.downstream_streams.load(Ordering::Relaxed),
                )
            }),
            _ => plain(StatusCode::NOT_FOUND, "not found"),
        }
    }

    async fn handle_command_watch(
        self: &Arc<Self>,
        request: &mut Request<Incoming>,
        ingress_security: IngressSecurity,
    ) -> Response<ProxyBody> {
        if request.method() != http::Method::GET {
            return plain(StatusCode::METHOD_NOT_ALLOWED, "command watch requires GET");
        }
        if ingress_security != IngressSecurity::Tls {
            return tls_required();
        }
        if !request
            .headers()
            .get(header::UPGRADE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
        {
            return plain(StatusCode::BAD_REQUEST, "websocket upgrade is required");
        }
        let key = match request
            .headers()
            .get("sec-websocket-key")
            .and_then(|value| value.to_str().ok())
        {
            Some(key) => key.to_owned(),
            None => return plain(StatusCode::BAD_REQUEST, "missing sec-websocket-key"),
        };
        let identity = match self
            .authenticator
            .authenticate_request_identity_with_policy(request, true)
            .await
        {
            Ok(tenant_id) => tenant_id,
            Err(error) => {
                self.command_watch_metrics
                    .auth_failures
                    .fetch_add(1, Ordering::Relaxed);
                return plain(error.status(), &error.to_string());
            }
        };
        let upgraded = hyper::upgrade::on(request);
        let gateway = Arc::clone(self);
        tokio::spawn(async move {
            match upgraded.await {
                Ok(upgraded) => {
                    let websocket = tokio_tungstenite::WebSocketStream::from_raw_socket(
                        TokioIo::new(upgraded),
                        tokio_tungstenite::tungstenite::protocol::Role::Server,
                        None,
                    )
                    .await;
                    if let Err(error) = gateway
                        .run_external_command_watch(
                            websocket,
                            identity.tenant_id,
                            identity.expires_at_unix,
                        )
                        .await
                    {
                        tracing::debug!(%error, "external command watch closed");
                    }
                }
                Err(error) => tracing::debug!(%error, "command watch upgrade failed"),
            }
        });
        let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
        Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header(header::CONNECTION, "Upgrade")
            .header(header::UPGRADE, "websocket")
            .header("sec-websocket-accept", accept)
            .body(empty_body())
            .expect("valid websocket response")
    }

    async fn run_external_command_watch<S>(
        self: Arc<Self>,
        mut websocket: tokio_tungstenite::WebSocketStream<S>,
        tenant_id: String,
        expires_at_unix: Option<i64>,
    ) -> Result<(), BoxError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let (outbound_tx, mut outbound_rx) =
            mpsc::channel::<WatchState>(self.command_watch_config.queue_capacity);
        let _connection = AtomicMetricGuard::new(&self.command_watch_metrics.connections);
        let mut subscriptions: HashMap<(String, String), ExternalSubscription> = HashMap::new();
        let mut version_negotiated = false;
        let expiry = expires_at_unix.map(|expires_at| {
            let remaining = expires_at.saturating_sub(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
            );
            tokio::time::sleep(std::time::Duration::from_secs(remaining.max(0) as u64))
        });
        tokio::pin!(expiry);
        loop {
            tokio::select! {
                _ = async {
                    if let Some(expiry) = expiry.as_mut().as_pin_mut() {
                        expiry.await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    websocket.close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                        code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy,
                        reason: "JWT expired".into(),
                    })).await?;
                    break;
                }
                incoming = websocket.next() => {
                    let Some(incoming) = incoming else { break; };
                    match incoming? {
                        tokio_tungstenite::tungstenite::Message::Text(text) => {
                            if text.len() > self.command_watch_config.max_frame_bytes {
                                return Err("command watch frame exceeds configured limit".into());
                            }
                            let message: ExternalWatchRequest = serde_json::from_str(&text)?;
                            if !version_negotiated {
                                if message.protocol_version != Some(1) {
                                    websocket.close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                        code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Protocol,
                                        reason: "command watch version 1 is required".into(),
                                    })).await?;
                                    break;
                                }
                                version_negotiated = true;
                                websocket.send(tokio_tungstenite::tungstenite::Message::Text(
                                    serde_json::json!({
                                        "op": "ready",
                                        "protocolVersion": 1,
                                        "capabilities": ["multiplexed-command-watch", "sandbox-downstream-aggregation"]
                                    }).to_string()
                                )).await?;
                            }
                            for command in message.commands {
                                let key = (command.sandbox_id.clone(), command.command_id.clone());
                                match message.op.as_str() {
                                    "subscribe" if !subscriptions.contains_key(&key) => {
                                        if subscriptions.len() >= self.command_watch_config.max_subscriptions_per_connection {
                                            outbound_tx.send(WatchState::rejected(command, "command watch subscription limit reached".into())).await?;
                                            continue;
                                        }
                                        match self.subscribe_external_command(&tenant_id, &command, outbound_tx.clone()).await {
                                            Ok(subscription) => {
                                                self.command_watch_metrics.subscriptions.fetch_add(1, Ordering::Relaxed);
                                                subscriptions.insert(key, subscription);
                                            }
                                            Err(error) => {
                                                outbound_tx.send(WatchState::rejected(command, error.to_string())).await?;
                                            }
                                        }
                                    }
                                    "unsubscribe" => {
                                        if let Some(subscription) = subscriptions.remove(&key) {
                                            subscription.stop().await;
                                            self.command_watch_metrics.subscriptions.fetch_sub(1, Ordering::Relaxed);
                                        }
                                    }
                                    "subscribe" => {}
                                    _ => return Err("unsupported command watch operation".into()),
                                }
                            }
                        }
                        tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                            websocket.send(tokio_tungstenite::tungstenite::Message::Pong(payload)).await?;
                        }
                        tokio_tungstenite::tungstenite::Message::Close(_) => break,
                        _ => {}
                    }
                }
                Some(state) = outbound_rx.recv() => {
                    if state.status == "RECONNECT" {
                        websocket.close(None).await?;
                        break;
                    }
                    websocket.send(tokio_tungstenite::tungstenite::Message::Text(serde_json::to_string(&state)?)).await?;
                }
            }
        }
        for (_, subscription) in subscriptions {
            subscription.stop().await;
            self.command_watch_metrics
                .subscriptions
                .fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn subscribe_external_command(
        &self,
        tenant_id: &str,
        command: &WatchCommandRef,
        outbound: mpsc::Sender<WatchState>,
    ) -> Result<ExternalSubscription, IngressOpenError> {
        let route = self
            .resolve_route(
                &command.sandbox_id,
                EXECD_COMMAND_PORT,
                AccessKind::Direct,
                Uuid::new_v4().to_string(),
            )
            .await?;
        if let Err(error) = self.authorize(&route, tenant_id) {
            self.command_watch_metrics
                .auth_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }
        let mut subscription = self
            .command_watch
            .subscribe(self, route, command.command_id.clone())
            .await
            .map_err(IngressOpenError::Connect)?;
        let sandbox_id = command.sandbox_id.clone();
        let command_id = command.command_id.clone();
        let task = tokio::spawn(async move {
            loop {
                match subscription.events.recv().await {
                    Ok(state) if state.command_id == command_id => {
                        let _ = outbound.send(state.with_sandbox(&sandbox_id)).await;
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let _ = outbound
                            .send(WatchState {
                                op: "state",
                                sandbox_id: sandbox_id.clone(),
                                command_id: command_id.clone(),
                                status: "RECONNECT".into(),
                                state_version: 0,
                                error: None,
                            })
                            .await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        let _ = outbound
                            .send(WatchState {
                                op: "state",
                                sandbox_id: sandbox_id.clone(),
                                command_id: command_id.clone(),
                                status: "RECONNECT".into(),
                                state_version: 0,
                                error: None,
                            })
                            .await;
                        break;
                    }
                }
            }
        });
        Ok(ExternalSubscription {
            control: subscription.control,
            command_id: command.command_id.clone(),
            task,
        })
    }

    async fn handle_connect(
        &self,
        request: &mut Request<Incoming>,
        ingress_security: IngressSecurity,
        peer: std::net::SocketAddr,
    ) -> Response<ProxyBody> {
        let (instance_id, target_port) = match external_connect_target(request) {
            Ok(target) => target,
            Err(message) => return plain(StatusCode::BAD_REQUEST, message),
        };
        let access_kind = match external_access_kind(request) {
            Ok(kind) => kind,
            Err(message) => return plain(StatusCode::BAD_REQUEST, message),
        };
        if ingress_security == IngressSecurity::Plaintext && request_has_credentials(request) {
            return plain(
                StatusCode::BAD_REQUEST,
                "credentials are not accepted on the plaintext listener",
            );
        }
        let request_id = header_string(request, "x-request-id");
        let route = match self
            .resolve_route(&instance_id, target_port, access_kind, request_id.clone())
            .await
        {
            Ok(route) => route,
            Err(error) => return error_response(error),
        };
        let auth_required = self.auth_required(access_kind, &route);
        if ingress_security == IngressSecurity::Plaintext && auth_required {
            return tls_required();
        }
        let tenant_id = if ingress_security == IngressSecurity::Plaintext {
            String::new()
        } else {
            match self
                .authenticator
                .authenticate_request_with_policy(request, auth_required)
                .await
            {
                Ok(tenant_id) => tenant_id,
                Err(error) => return plain(error.status(), &error.to_string()),
            }
        };
        match self.open_resolved_stream(route, &tenant_id).await {
            Ok(mut upstream) => {
                let cancelled = upstream.cancelled.clone();
                let on_upgrade = hyper::upgrade::on(request);
                let stream_started = Instant::now();
                let access_kind = access_kind.as_str();
                tokio::spawn(async move {
                    let relay = async {
                        match on_upgrade.await {
                            Ok(upgraded) => {
                                let mut upgraded = TokioIo::new(upgraded);
                                tokio::io::copy_bidirectional_with_sizes(
                                    &mut upgraded,
                                    &mut upstream,
                                    L4_COPY_BUFFER_SIZE,
                                    L4_COPY_BUFFER_SIZE,
                                )
                                .await
                            }
                            Err(error) => Err(io::Error::other(error.to_string())),
                        }
                    };
                    let result = relay_until_cancelled(relay, Some(cancelled)).await;
                    let duration_ms =
                        u64::try_from(stream_started.elapsed().as_millis()).unwrap_or(u64::MAX);
                    match result {
                        Ok((bytes_up, bytes_down)) => tracing::info!(
                            target: "adx_access",
                            event = "stream_close",
                            request_id = %request_id,
                            peer = %peer,
                            access_kind,
                            instance_id = %instance_id,
                            target_port,
                            outcome = "complete",
                            bytes_up,
                            bytes_down,
                            duration_ms,
                            "Ingress CONNECT stream closed"
                        ),
                        Err(error) => tracing::warn!(
                            target: "adx_access",
                            event = "stream_close",
                            request_id = %request_id,
                            peer = %peer,
                            access_kind,
                            instance_id = %instance_id,
                            target_port,
                            outcome = "error",
                            error = %error,
                            duration_ms,
                            "Ingress CONNECT stream closed"
                        ),
                    }
                });
                empty(StatusCode::OK)
            }
            Err(error) => error_response(error),
        }
    }

    async fn proxy_direct(
        &self,
        request: Request<Incoming>,
        ingress_security: IngressSecurity,
    ) -> Response<ProxyBody> {
        #[cfg(feature = "agent-api")]
        if let Some(api) = &self.agent_api {
            if api.can_retry_data(&request) {
                return self
                    .proxy_managed_direct(request, ingress_security, api)
                    .await;
            }
        }
        self.proxy_direct_attempt(request, ingress_security)
            .await
            .unwrap_or_else(|(response, _)| response)
    }

    #[cfg(feature = "agent-api")]
    async fn proxy_managed_direct(
        &self,
        mut request: Request<Incoming>,
        ingress_security: IngressSecurity,
        api: &super::agent_api::AgentApi,
    ) -> Response<ProxyBody> {
        loop {
            // Preserve only managed selection metadata. Ordinary fixed-ID forwarding keeps its
            // original path with no header copying. The body is never cloned or buffered.
            let uri = request.uri().clone();
            let headers = request.headers().clone();
            match self.proxy_direct_attempt(request, ingress_security).await {
                Ok(response) => return response,
                Err((response, mut unconsumed)) => {
                    *unconsumed.uri_mut() = uri;
                    *unconsumed.headers_mut() = headers;
                    match api.retry_data(&mut unconsumed).await {
                        Ok(true) => {
                            request = unconsumed;
                            continue;
                        }
                        Ok(false) => return response,
                        Err(error) => return super::agent_response::error_response(error, None),
                    }
                }
            }
        }
    }
    async fn proxy_direct_attempt(
        &self,
        mut request: Request<Incoming>,
        ingress_security: IngressSecurity,
    ) -> Result<Response<ProxyBody>, (Response<ProxyBody>, Request<Incoming>)> {
        let Some(parsed) = parse_direct_path(
            request.uri().path(),
            self.default_direct_port,
            self.default_tunnel_port,
        ) else {
            return Ok(plain(StatusCode::NOT_FOUND, "route not found"));
        };
        if ingress_security == IngressSecurity::Plaintext {
            if parsed.access_kind == AccessKind::Direct {
                return Ok(tls_required());
            }
            if request_has_credentials(&request) {
                return Ok(plain(
                    StatusCode::BAD_REQUEST,
                    "credentials are not accepted on the plaintext listener",
                ));
            }
        }
        #[cfg(feature = "agent-api")]
        let inline_forward = request
            .extensions()
            .get::<super::agent_access::InlineForward>()
            .cloned();
        #[cfg(feature = "agent-api")]
        let legacy = inline_forward.is_some();
        #[cfg(not(feature = "agent-api"))]
        let legacy = false;
        let request_id = header_string(&request, "x-request-id");
        let route = match self
            .resolve_route(
                &parsed.instance_id,
                parsed.target_port,
                parsed.access_kind,
                request_id,
            )
            .await
        {
            Ok(route) => route,
            Err(error) => {
                return Err((
                    if legacy {
                        inline_forward_error(error)
                    } else {
                        error_response(error)
                    },
                    request,
                ))
            }
        };
        let auth_required = self.auth_required(parsed.access_kind, &route);
        if ingress_security == IngressSecurity::Plaintext && auth_required {
            return Ok(tls_required());
        }
        let tenant_id = if ingress_security == IngressSecurity::Plaintext {
            String::new()
        } else {
            match self
                .authenticator
                .authenticate_request_with_policy(&request, auth_required)
                .await
            {
                Ok(tenant_id) => tenant_id,
                Err(error) => return Ok(plain(error.status(), &error.to_string())),
            }
        };
        let tenant_id = if legacy && tenant_id == "0" {
            route.tenant_id.clone()
        } else {
            tenant_id
        };
        let query = request
            .uri()
            .query()
            .map(strip_token_query)
            .unwrap_or_default();
        let query = if query.is_empty() {
            String::new()
        } else {
            format!("?{query}")
        };
        let uri = format!("{}{query}", parsed.stripped_path)
            .parse()
            .expect("stripped route path is a valid origin-form URI");
        *request.uri_mut() = uri;
        if !legacy {
            request.headers_mut().remove(H_INSTANCE_ID);
            request.headers_mut().remove(H_WORKLOAD_ID);
            request.headers_mut().remove(H_TARGET_IP);
            request.headers_mut().remove(H_TARGET_PORT);
            request.headers_mut().remove("x-auth");
            request.headers_mut().remove(header::AUTHORIZATION);
        }
        #[cfg(feature = "agent-api")]
        if let Some(forward) = inline_forward {
            *request.uri_mut() = forward.backend_uri;
        }
        let upgrade_requested = request.headers().contains_key(header::UPGRADE);
        if upgrade_requested {
            let stream = match self.open_resolved_stream(route, &tenant_id).await {
                Ok(stream) => stream,
                Err(error) => {
                    return Err((
                        if legacy {
                            inline_forward_error(error)
                        } else {
                            error_response(error)
                        },
                        request,
                    ))
                }
            };
            let cancelled = stream.cancelled.clone();
            return Ok(proxy_http(request, stream, "sandbox", Some(cancelled)).await);
        }

        if let Err(error) = self.authorize(&route, &tenant_id) {
            return Ok(error_response(error));
        }
        if !self.resolver.route_is_current(&route) {
            return Err((error_response(IngressOpenError::RouteChanged), request));
        }
        strip_hop_by_hop_headers(request.headers_mut());
        let key = BackendHttpPoolKey {
            relay_address: route.relay_address.clone(),
            instance_id: route.target.instance_id.clone(),
            workload_id: route.target.workload_id.clone(),
            target_ip: route.target.target_ip,
            target_port: route.target.target_port,
        };
        match self
            .http_pool
            .send_recoverable(key, request, || self.open_authorized_stream(route))
            .await
        {
            Ok(mut response) => {
                strip_hop_by_hop_headers(response.headers_mut());
                Ok(response.map(|body| {
                    body.map_err(|error| -> BoxError { Box::new(error) })
                        .boxed_unsync()
                }))
            }
            Err((error, Some(request))) => Err((backend_pool_error_response(error), request)),
            Err((error, None)) => Ok(backend_pool_error_response(error)),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExternalWatchRequest {
    #[serde(default)]
    protocol_version: Option<u32>,
    op: String,
    #[serde(default)]
    commands: Vec<WatchCommandRef>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WatchCommandRef {
    sandbox_id: String,
    command_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WatchState {
    op: &'static str,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    sandbox_id: String,
    command_id: String,
    status: String,
    state_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl WatchState {
    fn with_sandbox(mut self, sandbox_id: &str) -> Self {
        self.sandbox_id = sandbox_id.to_owned();
        self
    }

    fn rejected(command: WatchCommandRef, error: String) -> Self {
        Self {
            op: "state",
            sandbox_id: command.sandbox_id,
            command_id: command.command_id,
            status: "REJECTED".into(),
            state_version: 0,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WatchKey {
    sandbox_id: String,
}

#[derive(Clone)]
struct CommandWatchHub {
    entries: Arc<tokio::sync::Mutex<HashMap<WatchKey, WatchEntry>>>,
    config: CommandWatchConfig,
    metrics: Arc<CommandWatchMetrics>,
}

impl Default for CommandWatchHub {
    fn default() -> Self {
        Self::new(
            CommandWatchConfig::default(),
            Arc::new(CommandWatchMetrics::default()),
        )
    }
}

#[derive(Clone)]
struct WatchEntry {
    control: mpsc::Sender<HubControl>,
    events: broadcast::Sender<WatchState>,
}

enum HubControl {
    Add(String),
    Remove(String),
}

struct HubSubscription {
    control: mpsc::Sender<HubControl>,
    events: broadcast::Receiver<WatchState>,
}

impl CommandWatchHub {
    fn new(config: CommandWatchConfig, metrics: Arc<CommandWatchMetrics>) -> Self {
        Self {
            entries: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            config,
            metrics,
        }
    }
    async fn subscribe(
        &self,
        gateway: &Ingress,
        route: RouteHandle,
        command_id: String,
    ) -> io::Result<HubSubscription> {
        let key = WatchKey {
            sandbox_id: route.target.instance_id.clone(),
        };
        let mut entries = self.entries.lock().await;
        let entry = if let Some(entry) = entries.get(&key) {
            entry.clone()
        } else {
            let stream = gateway
                .open_authorized_stream_with_activity(route, true)
                .await?;
            let (websocket, _) =
                tokio_tungstenite::client_async("ws://execd/commands/watch", stream)
                    .await
                    .map_err(|error| io::Error::other(error.to_string()))?;
            let (control, controls) = mpsc::channel(self.config.queue_capacity);
            let (events, _) = broadcast::channel(self.config.queue_capacity);
            let entry = WatchEntry {
                control,
                events: events.clone(),
            };
            entries.insert(key.clone(), entry.clone());
            self.metrics.sandboxes.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .downstream_streams
                .fetch_add(1, Ordering::Relaxed);
            let all_entries = Arc::clone(&self.entries);
            let metrics = Arc::clone(&self.metrics);
            let ping_interval = self.config.ping_interval;
            tokio::spawn(async move {
                run_downstream_watch(websocket, controls, events, ping_interval).await;
                all_entries.lock().await.remove(&key);
                metrics.sandboxes.fetch_sub(1, Ordering::Relaxed);
                metrics.downstream_streams.fetch_sub(1, Ordering::Relaxed);
                metrics.reconnects.fetch_add(1, Ordering::Relaxed);
            });
            entry
        };
        let receiver = entry.events.subscribe();
        entry
            .control
            .send(HubControl::Add(command_id))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "command watch closed"))?;
        Ok(HubSubscription {
            control: entry.control,
            events: receiver,
        })
    }
}

async fn run_downstream_watch<S>(
    mut websocket: tokio_tungstenite::WebSocketStream<S>,
    mut controls: mpsc::Receiver<HubControl>,
    events: broadcast::Sender<WatchState>,
    ping_interval: std::time::Duration,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut refs = HashMap::<String, usize>::new();
    let mut ping = tokio::time::interval(ping_interval);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            control = controls.recv() => {
                let Some(control) = control else { break; };
                let (op, command_id) = match control {
                    HubControl::Add(command_id) => {
                        let count = refs.entry(command_id.clone()).or_default();
                        *count += 1;
                        // Re-send subscribe for every external subscriber. EXECD
                        // treats it idempotently but replays the current state,
                        // so a late subscriber cannot miss an already-terminal
                        // snapshot while this shared downstream is still open.
                        ("subscribe", command_id)
                    }
                    HubControl::Remove(command_id) => {
                        let Some(count) = refs.get_mut(&command_id) else { continue; };
                        *count -= 1;
                        if *count != 0 { continue; }
                        refs.remove(&command_id);
                        ("unsubscribe", command_id)
                    }
                };
                let message = serde_json::json!({"op": op, "commandIds": [command_id]});
                if websocket.send(tokio_tungstenite::tungstenite::Message::Text(message.to_string())).await.is_err() { break; }
                if refs.is_empty() && op == "unsubscribe" { break; }
            }
            incoming = websocket.next() => {
                let Some(Ok(incoming)) = incoming else { break; };
                match incoming {
                    tokio_tungstenite::tungstenite::Message::Text(text) => {
                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                            let state = WatchState {
                                op: "state",
                                sandbox_id: String::new(),
                                command_id: value.get("commandId").and_then(|v| v.as_str()).unwrap_or("").to_owned(),
                                status: value.get("status").and_then(|v| v.as_str()).unwrap_or("NOT_FOUND").to_owned(),
                                state_version: value.get("stateVersion").and_then(|v| v.as_u64()).unwrap_or(0),
                                error: None,
                            };
                            let _ = events.send(state);
                        }
                    }
                    tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                        let reply = websocket.send(tokio_tungstenite::tungstenite::Message::Pong(payload)).await;
                        if reply.is_err() { break; }
                    }
                    tokio_tungstenite::tungstenite::Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = ping.tick() => {
                if websocket.send(tokio_tungstenite::tungstenite::Message::Ping(Vec::new())).await.is_err() { break; }
            }
        }
    }
}

struct AtomicMetricGuard<'a>(&'a std::sync::atomic::AtomicU64);

impl<'a> AtomicMetricGuard<'a> {
    fn new(metric: &'a std::sync::atomic::AtomicU64) -> Self {
        metric.fetch_add(1, Ordering::Relaxed);
        Self(metric)
    }
}

impl Drop for AtomicMetricGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct ExternalSubscription {
    control: mpsc::Sender<HubControl>,
    command_id: String,
    task: tokio::task::JoinHandle<()>,
}

impl ExternalSubscription {
    async fn stop(self) {
        self.task.abort();
        let _ = self.control.send(HubControl::Remove(self.command_id)).await;
    }
}

fn strip_hop_by_hop_headers(headers: &mut http::HeaderMap) {
    let connection_headers = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| http::HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect::<Vec<_>>();
    for name in connection_headers {
        headers.remove(name);
    }
    for name in [
        header::CONNECTION,
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
    ] {
        headers.remove(name);
    }
    headers.remove("keep-alive");
    headers.remove("proxy-connection");
}

fn backend_pool_error_response(error: BackendHttpPoolError) -> Response<ProxyBody> {
    match error {
        BackendHttpPoolError::Saturated => plain(
            StatusCode::TOO_MANY_REQUESTS,
            "sandbox HTTP pool is saturated",
        ),
        BackendHttpPoolError::Closed => plain(
            StatusCode::SERVICE_UNAVAILABLE,
            "sandbox HTTP pool is closed",
        ),
        BackendHttpPoolError::Connect(error) => error_response(IngressOpenError::Connect(error)),
        BackendHttpPoolError::Handshake(error) | BackendHttpPoolError::Request(error) => {
            plain(StatusCode::BAD_GATEWAY, &error.to_string())
        }
    }
}

// Cancellation must surround the entire relay: after one direction reaches
// EOF, copy_bidirectional can wait solely on the client and never poll the
// upstream stream's cancellation-aware I/O again.
async fn relay_until_cancelled<F>(
    relay: F,
    cancelled: Option<watch::Receiver<bool>>,
) -> io::Result<(u64, u64)>
where
    F: std::future::Future<Output = io::Result<(u64, u64)>>,
{
    tokio::select! {
        biased;
        _ = wait_for_route_cancellation(cancelled) => Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "CONNECT stream cancelled by route change",
        )),
        result = relay => result,
    }
}

async fn wait_for_route_cancellation(cancelled: Option<watch::Receiver<bool>>) {
    let Some(mut cancelled) = cancelled else {
        std::future::pending::<()>().await;
        return;
    };
    loop {
        if *cancelled.borrow() {
            return;
        }
        if cancelled.changed().await.is_err() {
            return;
        }
    }
}

async fn proxy_http<T>(
    mut request: Request<Incoming>,
    stream: T,
    upstream_name: &'static str,
    cancelled: Option<watch::Receiver<bool>>,
) -> Response<ProxyBody>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let upgrade_requested = request.headers().contains_key(header::UPGRADE);
    let downstream_upgrade = upgrade_requested.then(|| hyper::upgrade::on(&mut request));
    let (mut sender, connection) =
        match hyper::client::conn::http1::handshake(TokioIo::new(stream)).await {
            Ok(parts) => parts,
            Err(error) => return plain(StatusCode::BAD_GATEWAY, &error.to_string()),
        };
    let connection_cancelled = cancelled.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = wait_for_route_cancellation(connection_cancelled) => {}
            result = connection.with_upgrades() => {
                if let Err(error) = result {
                    tracing::debug!(%error, upstream = upstream_name, "upstream HTTP connection closed");
                }
            }
        }
    });
    let mut response = match sender.send_request(request).await {
        Ok(response) => response,
        Err(error) => return plain(StatusCode::BAD_GATEWAY, &error.to_string()),
    };
    if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        if let Some(downstream_upgrade) = downstream_upgrade {
            let upstream_upgrade = hyper::upgrade::on(&mut response);
            tokio::spawn(async move {
                let relay = async move {
                    let downstream = downstream_upgrade
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))?;
                    let upstream = upstream_upgrade
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))?;
                    let mut downstream = TokioIo::new(downstream);
                    let mut upstream = TokioIo::new(upstream);
                    tokio::io::copy_bidirectional_with_sizes(
                        &mut downstream,
                        &mut upstream,
                        L4_COPY_BUFFER_SIZE,
                        L4_COPY_BUFFER_SIZE,
                    )
                    .await
                };
                let _ = relay_until_cancelled(relay, cancelled).await;
            });
        }
    }
    response.map(|body| {
        body.map_err(|error| -> BoxError { Box::new(error) })
            .boxed_unsync()
    })
}

fn external_connect_target<B>(request: &Request<B>) -> Result<(String, u16), &'static str> {
    let uri_authority = request.uri().authority().cloned();
    let host_authority = request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<http::uri::Authority>().ok());
    if let (Some(uri), Some(host)) = (&uri_authority, &host_authority) {
        if uri != host {
            return Err("CONNECT authority and Host header disagree");
        }
    }
    // RFC 9110 CONNECT uses authority-form. Some HTTP reverse proxies preserve
    // that authority in Host while normalizing the forwarded URI to origin-form.
    // Host is therefore the standards-compatible fallback, not a private route
    // header. Ambiguous requests were rejected above.
    let authority = uri_authority
        .or(host_authority)
        .ok_or("CONNECT target must be <instance-id>:<target-port>")?;
    let instance_id = authority.host().trim();
    if instance_id.is_empty() {
        return Err("CONNECT instance id is required");
    }
    let target_port = authority
        .port_u16()
        .filter(|port| *port != 0)
        .ok_or("CONNECT target port must be in 1..65535")?;
    Ok((instance_id.to_owned(), target_port))
}

fn external_access_kind<B>(request: &Request<B>) -> Result<AccessKind, &'static str> {
    let value = request
        .headers()
        .get(H_ACCESS_KIND)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if value.trim().is_empty() {
        return Ok(AccessKind::PortForwarding);
    }
    let kind = value
        .parse::<AccessKind>()
        .map_err(|_| "invalid X-Adx-Access-Kind")?;
    if kind == AccessKind::Direct {
        return Err("direct access does not use CONNECT");
    }
    Ok(kind)
}

fn effective_auth_required(access_kind: AccessKind, route: &RouteHandle) -> bool {
    match access_kind {
        AccessKind::Direct => true,
        AccessKind::Tunnel => route.tunnel_security_mode.token_required(false),
        AccessKind::PortForwarding | AccessKind::Ssh => {
            route.port_forward_auth_mode == crate::common::route::DataPlaneAuthMode::Token
        }
    }
}

fn request_has_credentials<B>(request: &Request<B>) -> bool {
    request.headers().contains_key(header::AUTHORIZATION)
        || request.headers().contains_key("x-auth")
        || request.uri().query().is_some_and(|query| {
            query.split('&').any(|field| {
                field
                    .split_once('=')
                    .map(|(name, _)| name.eq_ignore_ascii_case("token"))
                    .unwrap_or(false)
            })
        })
}

fn tls_required() -> Response<ProxyBody> {
    plain(
        StatusCode::UPGRADE_REQUIRED,
        "this route requires the Ingress TLS listener",
    )
}

#[derive(Debug, thiserror::Error)]
pub enum IngressOpenError {
    #[error(transparent)]
    Resolve(#[from] ResolveError),
    #[error(transparent)]
    Auth(#[from] super::auth::AuthError),
    #[error("tenant is not authorized for this workload")]
    Forbidden,
    #[error("Ingress is draining")]
    Draining,
    #[error("route changed while opening the stream")]
    RouteChanged,
    #[error("Relay connection failed: {0}")]
    Connect(#[from] io::Error),
}

struct ActiveSession {
    instance_id: String,
    workload_id: String,
    relay_address: String,
    target_ip: std::net::IpAddr,
    cancel: watch::Sender<bool>,
}

struct SessionGuard {
    session_id: Uuid,
    active_sessions: Arc<AtomicUsize>,
    sessions: Arc<Mutex<HashMap<Uuid, ActiveSession>>>,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.sessions.lock().unwrap().remove(&self.session_id);
        self.active_sessions.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct IngressStream {
    stream: H2ConnectStream,
    cancelled: watch::Receiver<bool>,
    _guard: SessionGuard,
}

impl AsyncRead for IngressStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(context, buffer)
    }
}

impl AsyncWrite for IngressStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.stream).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.stream).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.stream).poll_shutdown(context)
    }
}

fn header_string(request: &Request<Incoming>, name: &str) -> String {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn ensure_request_id(request: &mut Request<Incoming>) -> String {
    let current = header_string(request, "x-request-id");
    if !current.is_empty() {
        return current;
    }
    let generated = Uuid::new_v4().to_string();
    request.headers_mut().insert(
        "x-request-id",
        http::HeaderValue::from_str(&generated).expect("UUID is a valid HTTP header value"),
    );
    generated
}

fn should_log_access(status: StatusCode, duration_us: u64) -> bool {
    !status.is_success() || duration_us >= SLOW_REQUEST_LOG_THRESHOLD_US
}

fn ingress_security_name(security: IngressSecurity) -> &'static str {
    match security {
        IngressSecurity::Plaintext => "plaintext",
        IngressSecurity::Tls => "tls",
    }
}

async fn accept_ingress_tls(
    acceptor: TlsAcceptor,
    stream: TcpStream,
    peer: std::net::SocketAddr,
) -> Option<tokio_rustls::server::TlsStream<TcpStream>> {
    // TCP health checks may close without sending a ClientHello. Peek without
    // consuming bytes so rustls still receives the complete TLS record.
    let error = match stream.peek(&mut [0_u8; 1]).await {
        Ok(0) => return None,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::UnexpectedEof
            ) =>
        {
            tracing::debug!(
                event = "tls_handshake",
                peer = %peer,
                error = %error,
                "Ingress TLS peer closed before sending data"
            );
            return None;
        }
        Err(error) => error,
        Ok(_) => match acceptor.accept(stream).await {
            Ok(stream) => return Some(stream),
            Err(error) => error,
        },
    };
    tracing::warn!(
        target: "adx_audit",
        event = "tls_handshake",
        decision = "deny",
        peer = %peer,
        error = %error,
        "Ingress TLS handshake denied"
    );
    None
}

fn strip_token_query(query: &str) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(
            url::form_urlencoded::parse(query.as_bytes())
                .filter(|(name, _)| !name.eq_ignore_ascii_case("token")),
        )
        .finish()
}

fn inline_forward_error(error: IngressOpenError) -> Response<ProxyBody> {
    let status = match error {
        IngressOpenError::Forbidden => StatusCode::FORBIDDEN,
        IngressOpenError::Auth(_) => StatusCode::UNAUTHORIZED,
        IngressOpenError::Resolve(ResolveError::EnvironmentStatus { .. }) => StatusCode::CONFLICT,
        _ => StatusCode::BAD_GATEWAY,
    };
    plain(status, &error.to_string())
}

fn error_response(error: IngressOpenError) -> Response<ProxyBody> {
    let status = match &error {
        IngressOpenError::Resolve(ResolveError::NotFound) => StatusCode::NOT_FOUND,
        IngressOpenError::Resolve(ResolveError::NotReady | ResolveError::Unavailable(_))
        | IngressOpenError::Draining
        | IngressOpenError::RouteChanged => StatusCode::SERVICE_UNAVAILABLE,
        IngressOpenError::Resolve(ResolveError::EnvironmentStatus { .. }) => StatusCode::CONFLICT,
        IngressOpenError::Resolve(ResolveError::MissingEndpoint) => StatusCode::BAD_GATEWAY,
        IngressOpenError::Connect(error) => match error.kind() {
            io::ErrorKind::WouldBlock => StatusCode::TOO_MANY_REQUESTS,
            io::ErrorKind::TimedOut => StatusCode::GATEWAY_TIMEOUT,
            io::ErrorKind::PermissionDenied => StatusCode::FORBIDDEN,
            io::ErrorKind::InvalidData => StatusCode::CONFLICT,
            io::ErrorKind::NotConnected => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::BAD_GATEWAY,
        },
        IngressOpenError::Forbidden => StatusCode::FORBIDDEN,
        IngressOpenError::Auth(error) => error.status(),
    };
    plain(status, &error.to_string())
}

fn empty(status: StatusCode) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .body(
            Empty::<Bytes>::new()
                .map_err(|never| -> BoxError { match never {} })
                .boxed_unsync(),
        )
        .unwrap()
}

fn empty_body() -> ProxyBody {
    Empty::<Bytes>::new()
        .map_err(|never| -> BoxError { match never {} })
        .boxed_unsync()
}

fn plain(status: StatusCode, body: &str) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(
            Full::new(Bytes::copy_from_slice(body.as_bytes()))
                .map_err(|never| -> BoxError { match never {} })
                .boxed_unsync(),
        )
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ConnectTarget;
    use crate::common::route::{DataPlaneAuthMode, DataPlaneSecurityMode};

    #[derive(Clone)]
    struct TlsTestLog(Arc<Mutex<Vec<u8>>>);

    impl io::Write for TlsTestLog {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn ingress_test_acceptor() -> TlsAcceptor {
        adx_transport::install_crypto_provider();
        let certs = rustls_pemfile::certs(
            &mut &include_bytes!("../../tests/fixtures/ingress-cert.pem")[..],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let key = rustls_pemfile::private_key(
            &mut &include_bytes!("../../tests/fixtures/ingress-key.pem")[..],
        )
        .unwrap()
        .unwrap();
        TlsAcceptor::from(Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap(),
        ))
    }

    async fn ingress_test_pair() -> (TcpStream, TcpStream, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, peer) = listener.accept().await.unwrap();
        (client, server, peer)
    }

    async fn failed_ingress_log(payload: &[u8], reset: bool) -> String {
        let log = TlsTestLog(Arc::new(Mutex::new(Vec::new())));
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        exercise_failed_ingress(payload, reset).await;
        let bytes = log.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    async fn exercise_failed_ingress(payload: &[u8], reset: bool) {
        use tokio::io::AsyncWriteExt;
        let (mut client, server, peer) = ingress_test_pair().await;
        client.write_all(payload).await.unwrap();
        if reset {
            use std::os::fd::AsRawFd;
            let linger = libc::linger {
                l_onoff: 1,
                l_linger: 0,
            };
            // SAFETY: client owns this valid descriptor, linger points to a fully initialized
            // value for the duration of setsockopt, and the kernel does not retain the pointer.
            // Closing with SO_LINGER=0 exercises a real TCP RST, not an EOF.
            let result = unsafe {
                libc::setsockopt(
                    client.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_LINGER,
                    &linger as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&linger) as libc::socklen_t,
                )
            };
            assert_eq!(result, 0, "{}", io::Error::last_os_error());
        } else {
            client.shutdown().await.unwrap();
        }
        drop(client);
        let accepted = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            accept_ingress_tls(ingress_test_acceptor(), server, peer),
        )
        .await
        .unwrap();
        assert!(accepted.is_none());
    }

    #[tokio::test]
    async fn ingress_tls_empty_fin_is_silent() {
        let log = failed_ingress_log(b"", false).await;
        assert!(log.is_empty(), "{log}");
    }

    #[tokio::test]
    async fn ingress_tls_empty_rst_is_debug() {
        let log = failed_ingress_log(b"", true).await;
        assert!(log.contains("DEBUG"), "{log}");
        assert!(log.contains("closed before sending data"), "{log}");
        assert!(
            !log.contains("WARN") && !log.contains("decision=\"deny\""),
            "{log}"
        );
    }

    #[tokio::test]
    async fn ingress_tls_partial_record_eof_stays_warn() {
        let log = failed_ingress_log(b"\x16", false).await;
        assert!(
            log.contains("WARN") && log.contains("Ingress TLS handshake denied"),
            "{log}"
        );
        assert!(!log.contains("closed before sending data"), "{log}");
    }

    #[tokio::test]
    async fn ingress_tls_invalid_record_stays_warn() {
        let log = failed_ingress_log(b"GET / HTTP/1.1\r\n\r\n", false).await;
        assert!(
            log.contains("WARN") && log.contains("Ingress TLS handshake denied"),
            "{log}"
        );
    }

    #[test]
    fn ingress_tls_file_logging_respects_level() {
        const CHILD_ENV: &str = "ADX_TEST_INGRESS_LOG_CHILD";
        if std::env::var_os(CHILD_ENV).is_some() {
            // Use the real logger and file writers, including the independent
            // access/audit layer. A separate process isolates global tracing.
            let guard = adx_observability::logging::init("ingress-frontend", true).unwrap();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    exercise_failed_ingress(b"", false).await;
                    exercise_failed_ingress(b"", true).await;
                    exercise_failed_ingress(b"\x16", false).await;
                    exercise_failed_ingress(b"GET / HTTP/1.0\r\n\r\n", false).await;
                    tracing::info!(target: "adx_access", "request-access-marker");
                    tracing::warn!(target: "adx_audit", "authorization-denied-marker");
                });
            guard.shutdown().unwrap();
            return;
        }
        for level in ["info", "debug"] {
            let directory = std::env::temp_dir()
                .join(format!("adx-ingress-log-{}-{level}", std::process::id()));
            std::fs::create_dir(&directory).unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "ingress::server::tests::ingress_tls_file_logging_respects_level",
                    "--nocapture",
                ])
                .env(CHILD_ENV, "1")
                .env("RUST_LOG", level)
                .env("ADX_DATA_PLANE_LOG_DIR", &directory)
                .env("ADX_DATA_PLANE_LOG_STDOUT", "true")
                .env("ADX_DATA_PLANE_INGRESS_ACCESS_LOG_ENABLED", "true")
                .env("ADX_DATA_PLANE_INGRESS_AUDIT_LOG_ENABLED", "true")
                .output()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            let general = std::fs::read_to_string(directory.join("ingress-frontend.log")).unwrap();
            let access =
                std::fs::read_to_string(directory.join("ingress-frontend-access.log")).unwrap();
            let stdout = String::from_utf8(output.stdout).unwrap();
            let message = "closed before sending data";
            assert!(!access.contains(message), "{access}");
            assert_eq!(
                general.matches(message).count(),
                usize::from(level == "debug"),
                "{general}"
            );
            assert_eq!(
                stdout.matches(message).count(),
                usize::from(level == "debug"),
                "{stdout}"
            );
            assert_eq!(
                access.matches("Ingress TLS handshake denied").count(),
                2,
                "{access}"
            );
            assert!(access.contains("request-access-marker"), "{access}");
            assert!(access.contains("authorization-denied-marker"), "{access}");
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn ingress_tls_success_preserves_client_hello_and_application_data() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let acceptor = ingress_test_acceptor();
        let mut roots = rustls::RootCertStore::empty();
        for cert in
            rustls_pemfile::certs(&mut &include_bytes!("../../tests/fixtures/ingress-cert.pem")[..])
        {
            roots.add(cert.unwrap()).unwrap();
        }
        let connector = tokio_rustls::TlsConnector::from(Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ));
        let (client, server, peer) = ingress_test_pair().await;
        let exchange = async {
            tokio::join!(
                async {
                    let mut stream = accept_ingress_tls(acceptor, server, peer).await.unwrap();
                    let mut body = [0; 4];
                    stream.read_exact(&mut body).await.unwrap();
                    assert_eq!(&body, b"ping");
                    stream.write_all(b"pong").await.unwrap();
                },
                async {
                    let mut stream = connector
                        .connect("localhost".try_into().unwrap(), client)
                        .await
                        .unwrap();
                    stream.write_all(b"ping").await.unwrap();
                    let mut body = [0; 4];
                    stream.read_exact(&mut body).await.unwrap();
                    assert_eq!(&body, b"pong");
                }
            );
        };
        tokio::time::timeout(std::time::Duration::from_secs(3), exchange)
            .await
            .unwrap();
    }

    fn route(tunnel: DataPlaneSecurityMode, port_forward: DataPlaneAuthMode) -> RouteHandle {
        RouteHandle {
            access_kind: AccessKind::Direct,
            relay_address: "node:8443".into(),
            tenant_id: "tenant-a".into(),
            tunnel_security_mode: tunnel,
            port_forward_auth_mode: port_forward,
            target: ConnectTarget {
                instance_id: "instance-a".into(),
                workload_id: "sandbox-a".into(),
                target_ip: "10.88.0.2".parse().unwrap(),
                target_port: 22,
                request_id: "request-a".into(),
            },
        }
    }

    #[test]
    fn request_metrics_aggregate_status_and_latency_without_request_labels() {
        let metrics = IngressRequestMetrics::default();
        metrics.observe(StatusCode::OK, 900);
        metrics.observe(StatusCode::BAD_REQUEST, 1_500);
        metrics.observe(StatusCode::SERVICE_UNAVAILABLE, 200_000);

        let output = metrics.prometheus();
        assert!(output.contains("data_plane_ingress_http_requests_total 3\n"));
        assert!(output.contains("data_plane_ingress_http_responses_total{class=\"2xx\"} 1\n"));
        assert!(output.contains("data_plane_ingress_http_responses_total{class=\"4xx\"} 1\n"));
        assert!(output.contains("data_plane_ingress_http_responses_total{class=\"5xx\"} 1\n"));
        assert!(output
            .contains("data_plane_ingress_http_request_duration_seconds_bucket{le=\"0.001\"} 1\n"));
        assert!(output
            .contains("data_plane_ingress_http_request_duration_seconds_bucket{le=\"+Inf\"} 3\n"));
    }

    #[test]
    fn access_log_keeps_errors_and_slow_successes_only() {
        assert!(!should_log_access(StatusCode::OK, 99_999));
        assert!(should_log_access(StatusCode::OK, 100_000));
        assert!(should_log_access(StatusCode::BAD_REQUEST, 10));
        assert!(should_log_access(StatusCode::INTERNAL_SERVER_ERROR, 10));
    }

    #[test]
    fn route_security_selects_authentication() {
        let route = route(DataPlaneSecurityMode::TlsToken, DataPlaneAuthMode::Token);
        assert!(effective_auth_required(AccessKind::Tunnel, &route));
        assert!(effective_auth_required(AccessKind::PortForwarding, &route));
        assert!(effective_auth_required(AccessKind::Ssh, &route));
        assert!(effective_auth_required(AccessKind::Direct, &route));
    }

    #[test]
    fn tunnel_and_port_forward_default_to_no_authentication() {
        let route = route(DataPlaneSecurityMode::Inherit, DataPlaneAuthMode::None);
        assert!(!effective_auth_required(AccessKind::Tunnel, &route));
        assert!(!effective_auth_required(AccessKind::PortForwarding, &route));
        assert!(!effective_auth_required(AccessKind::Ssh, &route));
        assert!(effective_auth_required(AccessKind::Direct, &route));
    }

    #[test]
    fn detects_credentials_that_must_not_cross_plaintext() {
        let authorization = Request::builder()
            .header(header::AUTHORIZATION, "Bearer secret")
            .body(())
            .unwrap();
        let query = Request::builder()
            .uri("/tunnel/i?token=secret")
            .body(())
            .unwrap();
        let anonymous = Request::builder().uri("/tunnel/i").body(()).unwrap();
        assert!(request_has_credentials(&authorization));
        assert!(request_has_credentials(&query));
        assert!(!request_has_credentials(&anonymous));
    }

    #[test]
    fn configured_control_plane_paths_do_not_capture_data_plane_routes() {
        let routes = parse_static_routes(
            "exact:/,exact:/healthz,prefix:/api/sandbox,prefix:/serverless/v1/functions,prefix:/terminal,prefix:/global-scheduler",
        )
        .unwrap();
        for path in [
            "/",
            "/api/sandbox/v1/sandboxes",
            "/serverless/v1/functions/demo",
            "/terminal/session",
            "/global-scheduler/traefik/config",
            "/healthz",
        ] {
            assert!(routes.iter().any(|route| route.matches(path)), "{path}");
        }
        for path in [
            "/direct/instance-a",
            "/tunnel/instance-a",
            "/instance-a/8080/healthz",
            "/api/sandbox-escape",
        ] {
            assert!(!routes.iter().any(|route| route.matches(path)), "{path}");
        }
    }

    #[test]
    fn rejects_invalid_static_route_specs() {
        assert!(parse_static_routes("").is_err());
        assert!(parse_static_routes("/api/sandbox").is_err());
        assert!(parse_static_routes("glob:/api/*").is_err());
        assert!(parse_static_routes("prefix:https://frontend").is_err());
        assert!(parse_static_routes("prefix:/").is_err());
    }

    #[test]
    fn connect_target_uses_standard_authority_form() {
        let request = Request::builder()
            .method(http::Method::CONNECT)
            .uri("instance-a:2222")
            .body(())
            .unwrap();
        assert_eq!(
            external_connect_target(&request).unwrap(),
            ("instance-a".into(), 2222)
        );
        assert_eq!(
            external_access_kind(&request).unwrap(),
            AccessKind::PortForwarding
        );
    }

    #[test]
    fn connect_target_uses_standard_host_fallback_after_reverse_proxy() {
        let request = Request::builder()
            .method(http::Method::CONNECT)
            .uri("/")
            .header(header::HOST, "instance-a:2222")
            .body(())
            .unwrap();
        assert_eq!(
            external_connect_target(&request).unwrap(),
            ("instance-a".into(), 2222)
        );
    }

    #[test]
    fn connect_target_rejects_ambiguous_authorities() {
        let request = Request::builder()
            .method(http::Method::CONNECT)
            .uri("instance-a:2222")
            .header(header::HOST, "instance-b:2222")
            .body(())
            .unwrap();
        assert!(external_connect_target(&request).is_err());
    }

    #[test]
    fn connect_access_kind_is_explicit_and_rejects_direct() {
        let request = Request::builder()
            .method(http::Method::CONNECT)
            .uri("instance-a:22")
            .header(H_ACCESS_KIND, "ssh")
            .body(())
            .unwrap();
        assert_eq!(external_access_kind(&request).unwrap(), AccessKind::Ssh);

        let request = Request::builder()
            .method(http::Method::CONNECT)
            .uri("instance-a:50090")
            .header(H_ACCESS_KIND, "direct")
            .body(())
            .unwrap();
        assert!(external_access_kind(&request).is_err());
    }
}

#[cfg(test)]
mod reverse_proxy_tests;

#[cfg(feature = "agent-api")]
fn response_text(status: StatusCode, message: &str) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .body(
            Full::new(Bytes::from(message.to_owned()))
                .map_err(|never: Infallible| -> BoxError { match never {} })
                .boxed_unsync(),
        )
        .expect("static response")
}

#[cfg(all(test, feature = "agent-api"))]
#[path = "agent_api_tests.rs"]
mod agent_api_tests;
