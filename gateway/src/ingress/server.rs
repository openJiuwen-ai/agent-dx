use super::auth::IngressAuthenticator;
use super::connector::{DataPlaneL4Connector, H2ConnectStream};
use super::e2b_route::{
    is_management_path as is_e2b_management_path, parse_target as parse_e2b_target,
    E2bAuthorizationError, E2bTarget,
};
use super::http_pool::{
    BackendHttpPool, BackendHttpPoolConfig, BackendHttpPoolError, BackendHttpPoolKey,
};
use super::path::{parse_direct_path, parse_port_host_route, ParsedDirectPath};
use super::processing::{ProcessingDeadline, TIMEOUT_HEADER};
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
use hyper_util::rt::{TokioExecutor, TokioIo};
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

struct DeadlineBody {
    body: ProxyBody,
    timer: Pin<Box<tokio::time::Sleep>>,
    finished: bool,
}
impl DeadlineBody {
    fn new(body: ProxyBody, deadline: tokio::time::Instant) -> Self {
        Self {
            body,
            timer: Box::pin(tokio::time::sleep_until(deadline)),
            finished: false,
        }
    }
}
impl hyper::body::Body for DeadlineBody {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, BoxError>>> {
        use std::future::Future;
        if self.finished {
            return Poll::Ready(None);
        }
        if self.timer.as_mut().poll(cx).is_ready() {
            self.finished = true;
            return Poll::Ready(Some(Err(Box::new(io::Error::new(
                io::ErrorKind::TimedOut,
                "processing deadline exceeded after forwarding",
            )))));
        }
        let result = Pin::new(&mut self.body).poll_frame(cx);
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            self.finished = true;
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.finished || self.body.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

/// 全流观测的请求分类（详设 §6.2：有界枚举，值集冻结）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteClass {
    EnvironmentApi,
    CommandWatch,
    Jiuwen,
    InternalHealth,
    Unmatched,
}
impl RouteClass {
    const COUNT: usize = 5;
    const ALL: [Self; Self::COUNT] = [
        Self::EnvironmentApi,
        Self::CommandWatch,
        Self::Jiuwen,
        Self::InternalHealth,
        Self::Unmatched,
    ];
    fn as_str(self) -> &'static str {
        match self {
            Self::EnvironmentApi => "environment_api",
            Self::CommandWatch => "command_watch",
            Self::Jiuwen => "jiuwen",
            Self::InternalHealth => "internal_health",
            Self::Unmatched => "unmatched",
        }
    }
}

/// 全流观测的传输形态（`ws`/`connect` 的样本由连接生命周期挂点记录）。
/// `pub(super)`：reverse_proxy 的升级拷贝任务需要构造取值（M1-9）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Transport {
    Http,
    Sse,
    Ws,
    Connect,
}
impl Transport {
    const COUNT: usize = 4;
    const ALL: [Self; Self::COUNT] = [Self::Http, Self::Sse, Self::Ws, Self::Connect];
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Sse => "sse",
            Self::Ws => "ws",
            Self::Connect => "connect",
        }
    }
}

/// 流中断的粗分类（详设：不设 backend_closed——无法可靠区分后端正常关闭与协议错误）。
/// `pub(super)`：reverse_proxy 的升级拷贝任务需要构造取值（M1-9）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AbortReason {
    ClientClosed,
    Deadline,
    ProtocolError,
    Unknown,
}
impl AbortReason {
    const COUNT: usize = 4;
    const ALL: [Self; Self::COUNT] = [
        Self::ClientClosed,
        Self::Deadline,
        Self::ProtocolError,
        Self::Unknown,
    ];
    fn as_str(self) -> &'static str {
        match self {
            Self::ClientClosed => "client_closed",
            Self::Deadline => "deadline",
            Self::ProtocolError => "protocol_error",
            Self::Unknown => "unknown",
        }
    }
}

/// 一次请求的全流观察者：随所有权移动，由流的唯一所有者记录一次（详设 §6.2）。
/// `Clone` 仅为满足 `http::Extensions::insert` 的边界；运行时始终只有一份。
#[derive(Clone)]
pub(super) struct StreamObserver {
    metrics: Arc<IngressRequestMetrics>,
    route_class: RouteClass,
    started: Instant,
}
impl StreamObserver {
    fn new(metrics: &Arc<IngressRequestMetrics>, route_class: RouteClass) -> Self {
        Self {
            metrics: Arc::clone(metrics),
            route_class,
            started: Instant::now(),
        }
    }
    fn wrap_body(self, response: Response<ProxyBody>, transport: Transport) -> Response<ProxyBody> {
        let (parts, body) = response.into_parts();
        let body = ObservedBody {
            body,
            observer: Some((self, transport)),
        }
        .boxed_unsync();
        Response::from_parts(parts, body)
    }
    pub(super) fn record(
        self,
        transport: Transport,
        outcome: adx_core::observation::Outcome,
        abort: Option<AbortReason>,
    ) {
        self.metrics.observe_stream(
            self.route_class,
            transport,
            outcome,
            self.started.elapsed().as_secs_f64(),
            abort,
        );
    }
}

/// 全流观察体：最外层包装（在 DeadlineBody 之外），结束/错误/丢弃时恰好记录一次。
struct ObservedBody {
    body: ProxyBody,
    observer: Option<(StreamObserver, Transport)>,
}
impl hyper::body::Body for ObservedBody {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, BoxError>>> {
        let result = Pin::new(&mut self.body).poll_frame(cx);
        match &result {
            Poll::Ready(None) => {
                if let Some((observer, transport)) = self.observer.take() {
                    observer.record(transport, adx_core::observation::Outcome::Success, None);
                }
            }
            // 已知长度的体（Full）在最后一帧后 is_end_stream 为真，hyper 可能不再
            // poll 到 None——在帧成功且体已标尽时即记录，避免误记 client_closed。
            Poll::Ready(Some(Ok(_))) if self.body.is_end_stream() => {
                if let Some((observer, transport)) = self.observer.take() {
                    observer.record(transport, adx_core::observation::Outcome::Success, None);
                }
            }
            Poll::Ready(Some(Err(error))) => {
                if let Some((observer, transport)) = self.observer.take() {
                    let reason = abort_reason_of(error);
                    observer.record(
                        transport,
                        adx_core::observation::Outcome::Failure,
                        Some(reason),
                    );
                }
            }
            _ => {}
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}
impl Drop for ObservedBody {
    fn drop(&mut self) {
        // 未记录即被丢弃：下游在流结束前断开（详设：client_closed）。
        if let Some((observer, transport)) = self.observer.take() {
            observer.record(
                transport,
                adx_core::observation::Outcome::Cancelled,
                Some(AbortReason::ClientClosed),
            );
        }
    }
}

fn abort_reason_of(error: &BoxError) -> AbortReason {
    if error
        .downcast_ref::<io::Error>()
        .is_some_and(|error| error.kind() == io::ErrorKind::TimedOut)
    {
        AbortReason::Deadline
    } else {
        AbortReason::ProtocolError
    }
}

fn transport_of(response: &Response<ProxyBody>) -> Transport {
    let event_stream = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().starts_with("text/event-stream"));
    if event_stream {
        Transport::Sse
    } else {
        Transport::Http
    }
}

/// lifecycle 请求的内层早退响应同样计入全流（观察者已从 extensions 取出）。
fn observed(
    observer: Option<StreamObserver>,
    response: Response<ProxyBody>,
) -> Response<ProxyBody> {
    match observer {
        Some(observer) => observer.wrap_body(response, Transport::Http),
        None => response,
    }
}

fn resuming_response(request_id: &str, instance_id: &str) -> Response<ProxyBody> {
    let body = serde_json::json!({"error": {
        "code": "ENV_RESUMING", "message": "Environment readiness is pending; retry the same request",
        "retry": "same_operation", "outcome": "not_started", "requestForwarded": false,
        "requestId": request_id, "instanceId": instance_id,
    }});
    let mut response = plain(StatusCode::SERVICE_UNAVAILABLE, &body.to_string());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, http::HeaderValue::from_static("1"));
    response.headers_mut().insert(
        "x-adx-error-code",
        http::HeaderValue::from_static("ENV_RESUMING"),
    );
    response
}

fn processing_timeout_response(
    deadline: &ProcessingDeadline,
    request_id: &str,
    instance_id: &str,
) -> Response<ProxyBody> {
    if deadline.is_waiting_resume() && !deadline.is_forwarded() {
        return resuming_response(request_id, instance_id);
    }
    let body = serde_json::json!({"error": {
        "code": "PROCESSING_DEADLINE_EXCEEDED", "message": "Request processing deadline exceeded",
        "retry": if deadline.is_forwarded() { "never" } else { "same_operation" },
        "outcome": if deadline.is_forwarded() { "unknown" } else { "not_started" },
        "requestForwarded": deadline.is_forwarded(), "requestId": request_id, "instanceId": instance_id,
    }});
    let mut response = plain(StatusCode::GATEWAY_TIMEOUT, &body.to_string());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    response
}
const L4_COPY_BUFFER_SIZE: usize = 64 * 1024;
const COMMAND_WATCH_PATH: &str = "/api/environment/v1/commands/watch";
const EXECD_COMMAND_PORT: u16 = 50090;
const SLOW_REQUEST_LOG_THRESHOLD_US: u64 = 100_000;
const REQUEST_DURATION_BUCKETS_US: [(&str, u64); 16] = [
    ("0.001", 1_000),
    ("0.002", 2_000),
    ("0.005", 5_000),
    ("0.010", 10_000),
    ("0.025", 25_000),
    ("0.050", 50_000),
    ("0.100", 100_000),
    ("0.250", 250_000),
    ("0.500", 500_000),
    ("1.000", 1_000_000),
    ("2.500", 2_500_000),
    ("5.000", 5_000_000),
    ("10.000", 10_000_000),
    ("30.000", 30_000_000),
    ("60.000", 60_000_000),
    ("120.000", 120_000_000),
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
    environments: std::sync::atomic::AtomicU64,
    reconnects: std::sync::atomic::AtomicU64,
    auth_failures: std::sync::atomic::AtomicU64,
    downstream_streams: std::sync::atomic::AtomicU64,
}

/// 全流族存储：[route_class][transport][outcome] 的 B15 直方图＋中断计数。
/// outcome 维度长度 4 ＝ `adx_core::observation::Outcome` 变体数（单测锁定）。
#[derive(Default)]
struct FullStreamMetrics {
    duration: [[[adx_observability::metrics::AtomicHistogram<15>; 4]; Transport::COUNT];
        RouteClass::COUNT],
    aborted: [[AtomicU64; AbortReason::COUNT]; RouteClass::COUNT],
}

#[derive(Default)]
struct IngressRequestMetrics {
    traffic: Arc<crate::common::traffic::TrafficMeter>,
    responses_1xx: AtomicU64,
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
    full_stream: FullStreamMetrics,
}

impl IngressRequestMetrics {
    fn observe(&self, status: StatusCode, duration_us: u64) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.duration_us_total
            .fetch_add(duration_us, Ordering::Relaxed);
        match status.as_u16() / 100 {
            1 => &self.responses_1xx,
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

    /// 每条流恰好一次：全流样本＋（若有）中断计数。
    fn observe_stream(
        &self,
        route_class: RouteClass,
        transport: Transport,
        outcome: adx_core::observation::Outcome,
        seconds: f64,
        abort: Option<AbortReason>,
    ) {
        self.full_stream.duration[route_class as usize][transport as usize][outcome as usize]
            .observe(&adx_observability::metrics::B15, seconds);
        if let Some(reason) = abort {
            self.full_stream.aborted[route_class as usize][reason as usize]
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn prometheus(&self, full_stream: bool) -> String {
        let total = self.total.load(Ordering::Relaxed);
        let mut output = format!(
            "# TYPE data_plane_ingress_http_requests_in_flight gauge\ndata_plane_ingress_http_requests_in_flight {}\n\
# TYPE data_plane_ingress_http_requests_total counter\ndata_plane_ingress_http_requests_total {}\n\
# TYPE data_plane_ingress_http_responses_total counter\ndata_plane_ingress_http_responses_total{{class=\"2xx\"}} {}\n\
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
        let (received, sent) = self.traffic.snapshot();
        let _ = writeln!(
            output,
            "data_plane_ingress_http_responses_total{{class=\"1xx\"}} {}",
            self.responses_1xx.load(Ordering::Relaxed)
        );
        let _ = writeln!(output, "# TYPE data_plane_ingress_frontend_bytes_total counter\ndata_plane_ingress_frontend_bytes_total{{direction=\"received\"}} {received}\ndata_plane_ingress_frontend_bytes_total{{direction=\"sent\"}} {sent}");
        let _ = writeln!(
            output,
            "# TYPE data_plane_ingress_http_request_duration_seconds histogram"
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
        // 全流族稠密导出（含全零组合），经共享渲染器声明 HELP/TYPE。
        // 开关关闭时不导出。
        if full_stream {
            let mut families = adx_observability::metrics::Text::default();
            for route_class in RouteClass::ALL {
                for transport in Transport::ALL {
                    for outcome in adx_core::observation::Outcome::ALL {
                        let snapshot = self.full_stream.duration[route_class as usize]
                            [transport as usize][*outcome as usize]
                            .snapshot();
                        families.histogram(
                            "data_plane_ingress_full_stream_duration_seconds",
                            "Full stream lifetime from request headers to body or tunnel end",
                            &[
                                ("route_class", route_class.as_str().to_owned()),
                                ("transport", transport.as_str().to_owned()),
                                ("outcome", outcome.as_str().to_owned()),
                            ],
                            &adx_observability::metrics::B15,
                            &snapshot,
                        );
                    }
                }
                for reason in AbortReason::ALL {
                    families.counter(
                        "data_plane_ingress_stream_aborted_total",
                        "Streams that ended before clean completion",
                        &[
                            ("route_class", route_class.as_str().to_owned()),
                            ("reason", reason.as_str().to_owned()),
                        ],
                        self.full_stream.aborted[route_class as usize][reason as usize]
                            .load(Ordering::Relaxed),
                    );
                }
            }
            output.push_str(&families.finish());
        }
        output
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressSecurity {
    Plaintext,
    /// Explicit listener opt-in; authentication remains mandatory on private APIs.
    AuthenticatedHttp,
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
    pub(super) execd_access: Option<Arc<super::environment_files::ExecdAccess>>,
    #[cfg(feature = "agent-api")]
    pub(super) agent_api: Option<Arc<super::agent_api::AgentApi>>,
    #[cfg(feature = "agent-api")]
    digitalmategateway_api:
        Option<Arc<super::digitalmategateway::entrypoint::DigitalMateGatewayApi>>,
    resolver: Arc<IngressRouteResolver>,
    connector: DataPlaneL4Connector,
    http_pool: BackendHttpPool,
    authenticator: IngressAuthenticator,
    default_direct_port: u16,
    default_tunnel_port: u16,
    port_host_domain: Option<String>,
    e2b_enabled: bool,
    e2b_allow_http: bool,
    full_stream: bool,
    experience_metrics: Arc<super::experience::ExperienceMetrics>,
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
    /// 业务观测聚合器（M2-5，§6.10）；未启用时 None。
    #[cfg(feature = "agent-api")]
    observation_aggregator: Option<Arc<super::observability::Aggregator>>,
    /// 业务事件发射器（M2-4b，§6.4）；未启用时 None。
    #[cfg(feature = "agent-api")]
    observation_sink: Option<Arc<super::observability::ObservationSink>>,
    /// 观测接收端点状态（M2-2）；仅用于接收质量族渲染。
    #[cfg(feature = "agent-api")]
    observation_ingest: Option<Arc<super::observability::IngestState>>,
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
            execd_access: None,
            #[cfg(feature = "agent-api")]
            agent_api: None,
            #[cfg(feature = "agent-api")]
            digitalmategateway_api: None,
            resolver,
            connector,
            http_pool: BackendHttpPool::new(BackendHttpPoolConfig::default()),
            authenticator,
            default_direct_port,
            default_tunnel_port,
            port_host_domain: None,
            e2b_enabled: false,
            e2b_allow_http: false,
            full_stream: true,
            experience_metrics: Arc::new(super::experience::ExperienceMetrics::default()),
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
            #[cfg(feature = "agent-api")]
            observation_aggregator: None,
            #[cfg(feature = "agent-api")]
            observation_sink: None,
            #[cfg(feature = "agent-api")]
            observation_ingest: None,
        }
    }

    /// 观测接收端点状态（M2-2）：用于 `/metrics` 的接收质量族。
    #[cfg(feature = "agent-api")]
    pub fn with_observation_ingest(
        mut self,
        ingest: Option<Arc<super::observability::IngestState>>,
    ) -> Self {
        self.observation_ingest = ingest;
        self
    }

    /// 业务观测聚合器（M2-5）：启用后 `/metrics` 追加业务导出与质量族。
    #[cfg(feature = "agent-api")]
    pub fn with_observation_aggregator(
        mut self,
        aggregator: Option<Arc<super::observability::Aggregator>>,
    ) -> Self {
        self.observation_aggregator = aggregator;
        self
    }

    /// 业务事件发射器（M2-4b）：挂点到 `/metrics` 的发射质量族，锚点携带。
    #[cfg(feature = "agent-api")]
    pub fn with_observation_sink(
        mut self,
        sink: Option<Arc<super::observability::ObservationSink>>,
    ) -> Self {
        self.observation_sink = sink;
        self
    }

    #[cfg(feature = "agent-api")]
    pub fn with_digitalmategateway_api(
        mut self,
        api: Arc<super::digitalmategateway::entrypoint::DigitalMateGatewayApi>,
    ) -> Self {
        self.digitalmategateway_api = Some(api);
        self
    }

    #[cfg(feature = "agent-api")]
    pub fn with_agent_api(mut self, api: Arc<super::agent_api::AgentApi>) -> Self {
        self.agent_api = Some(api);
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

    pub fn with_port_host_domain(mut self, domain: Option<String>) -> Self {
        self.port_host_domain = domain;
        self
    }

    pub fn with_e2b_enabled(mut self, enabled: bool) -> Self {
        self.e2b_enabled = enabled;
        self
    }

    /// Opt in to E2B routes over unencrypted HTTP. Native transport policies remain unchanged.
    pub fn with_e2b_allow_http(mut self, allow: bool) -> Self {
        self.e2b_allow_http = allow;
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

    /// 全流观测开关；关闭时仅保留请求族计数。
    pub fn with_full_stream_observation(mut self, enabled: bool) -> Self {
        self.full_stream = enabled;
        self
    }

    /// 体验族锚点；template 超基数上限由存储归 `other`。
    /// 事件发射器（若启用）随锚点下发，`userRef` 在这里做 HMAC。
    #[cfg(feature = "agent-api")]
    pub(crate) fn experience_anchor(
        &self,
        template_release: &str,
        started: tokio::time::Instant,
        tenant_ref: &str,
        user_id: &str,
    ) -> Option<super::experience::ExperienceAnchor> {
        Some(super::experience::ExperienceAnchor::new(
            Arc::clone(&self.experience_metrics),
            self.observation_sink.clone(),
            template_release,
            started,
            tenant_ref,
            user_id,
        ))
    }

    /// 渲染体验族四族。
    pub(crate) fn render_experience(&self) -> String {
        let mut text = adx_observability::metrics::Text::default();
        self.experience_metrics.render(&mut text);
        text.finish()
    }

    /// 业务事件发射器（M2-4b/M2-4c）：适配器动作事件使用。
    #[cfg(feature = "agent-api")]
    pub(crate) fn observation_sink(&self) -> Option<Arc<super::observability::ObservationSink>> {
        self.observation_sink.clone()
    }

    /// 渲染业务观测族（聚合导出 + 发射/接收质量）；未启用时返回空串。
    #[cfg(feature = "agent-api")]
    pub(crate) fn render_observation(&self) -> String {
        if self.observation_aggregator.is_none()
            && self.observation_sink.is_none()
            && self.observation_ingest.is_none()
        {
            return String::new();
        }
        let mut text = adx_observability::metrics::Text::default();
        if let Some(aggregator) = &self.observation_aggregator {
            aggregator.render(&mut text);
        }
        if let Some(sink) = &self.observation_sink {
            sink.render(&mut text);
        }
        if let Some(ingest) = &self.observation_ingest {
            ingest.render(&mut text);
        }
        text.finish()
    }

    /// 资源利用率不在此导出：`adx_environment_*` 已由 Coordinator/adxlet 采集，
    /// 目录 × 分配 × 用量的 join 由既有 Prometheus 与 `adx-environments` 看板
    /// 完成（含 `adx_env`/`runtime_id`/`stats_age` 全键与新鲜度校验），
    /// ingress 不再重复实现一遍。
    #[cfg(feature = "agent-api")]
    pub fn with_execd_access(mut self, access: Arc<super::environment_files::ExecdAccess>) -> Self {
        self.execd_access = Some(access);
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

    pub(super) async fn open_resolved_stream(
        &self,
        route: RouteHandle,
        tenant_id: &str,
    ) -> Result<IngressStream, IngressOpenError> {
        self.authorize(&route, tenant_id)?;
        let route = self.prepare_route(route, tenant_id).await?;
        self.open_authorized_stream(route)
            .await
            .map_err(IngressOpenError::Connect)
    }
    pub(super) async fn prepare_route(
        &self,
        route: RouteHandle,
        tenant_id: &str,
    ) -> Result<RouteHandle, IngressOpenError> {
        self.authorize(&route, tenant_id)?;
        if !route.needs_resume {
            return Ok(route);
        }
        let ready = self.resolver.wake(&route).await?;
        self.authorize(&ready, tenant_id)?;
        if ready.tunnel_security_mode != route.tunnel_security_mode
            || ready.port_forward_auth_mode != route.port_forward_auth_mode
            || ready.private_service_ports != route.private_service_ports
            || !match (&route.e2b, &ready.e2b) {
                (None, None) => true,
                (Some(previous), Some(current)) => e2b_session_policy_unchanged(previous, current),
                _ => false,
            }
        {
            return Err(IngressOpenError::Forbidden);
        }
        Ok(ready)
    }

    pub(super) async fn open_authorized_stream(
        &self,
        route: RouteHandle,
    ) -> io::Result<IngressStream> {
        if route
            .private_service_ports
            .contains(&route.target.target_port)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private service requires an approved HTTP policy",
            ));
        }
        self.open_authorized_stream_with_activity(route, false)
            .await
    }

    // Called only after the HTTP service policy has authorized this request.
    async fn open_service_http_stream(&self, route: RouteHandle) -> io::Result<IngressStream> {
        self.open_authorized_stream_with_activity(route, false)
            .await
    }

    async fn open_authorized_stream_with_activity(
        &self,
        route: RouteHandle,
        passive: bool,
    ) -> io::Result<IngressStream> {
        if passive && route.needs_resume {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "passive watch cannot wake a environment",
            ));
        }
        let error = match self.open_current_stream(route.clone(), passive).await {
            Ok(stream) => return Ok(stream),
            Err(error) => error,
        };
        if self.draining.load(Ordering::Acquire)
            || !matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::NotConnected
            )
        {
            return Err(error);
        }
        // CONNECT was rejected before any application bytes were sent. Wait
        // only for a replacement binding, then open once against that execution.
        let replacement = match self.resolver.replacement(&route).await {
            Ok(replacement) => replacement,
            Err(_) if !passive => match self.resolver.wake(&route).await {
                Ok(route) => route,
                Err(error @ (ResolveError::Resuming | ResolveError::ResumeRejected(_))) => {
                    return Err(io::Error::other(error));
                }
                Err(ResolveError::Unavailable(message)) => {
                    return Err(io::Error::new(io::ErrorKind::NotConnected, message));
                }
                Err(ResolveError::NotReady) => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "resume service is not ready",
                    ));
                }
                Err(_) => return Err(error),
            },
            Err(_) => return Err(error),
        };
        if replacement.tenant_id != route.tenant_id
            || replacement.tunnel_security_mode != route.tunnel_security_mode
            || replacement.port_forward_auth_mode != route.port_forward_auth_mode
            || replacement.private_service_ports != route.private_service_ports
            || replacement.e2b != route.e2b
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "route authorization changed while opening the stream",
            ));
        }
        self.open_current_stream(replacement, passive).await
    }

    #[cfg(feature = "agent-api")]
    pub(super) fn subscribe_route_changes(
        &self,
    ) -> broadcast::Receiver<super::route_store::RouteChange> {
        self.resolver.subscribe_changes()
    }

    #[cfg(feature = "agent-api")]
    pub(super) fn route_is_current(&self, route: &RouteHandle) -> bool {
        self.resolver.route_is_current(route)
    }

    /// Open exactly the admitted execution. File transfers must not follow a
    /// replacement route between chunks or between upload and commit.
    #[cfg(feature = "agent-api")]
    pub(super) async fn open_pinned_stream(&self, route: RouteHandle) -> io::Result<IngressStream> {
        self.open_current_stream(route, false).await
    }

    async fn open_current_stream(
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
        {
            let mut sessions = self
                .sessions
                .lock()
                .map_err(|_| io::Error::other("active session lock poisoned"))?;
            // Hold the session lock across the snapshot and registration: a later
            // revocation must not be consumed before this session becomes visible.
            let revision = self
                .resolver
                .current_route_revision(&route)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotConnected, IngressOpenError::RouteChanged)
                })?;
            sessions.insert(
                session_id,
                ActiveSession {
                    instance_id: route.target.instance_id.clone(),
                    workload_id: route.target.workload_id.clone(),
                    relay_address: route.relay_address.clone(),
                    target_ip: route.target.target_ip,
                    revision,
                    route: route.clone(),
                    cancel,
                },
            );
            self.active_sessions.fetch_add(1, Ordering::Relaxed);
        }
        let guard = SessionGuard {
            session_id,
            active_sessions: self.active_sessions.clone(),
            sessions: self.sessions.clone(),
        };

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
        if route.e2b.is_some() {
            return Err(IngressOpenError::Forbidden);
        }
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
                // Queued events predating admission cannot revoke a newer session.
                .filter(|session| {
                    session.instance_id == change.instance_id && session.revision < change.revision
                })
            {
                let still_current = change.current.as_ref().is_some_and(|route| {
                    route.environment_status.code == 3
                        && route.environment_id == session.workload_id
                        && route.relay_address == session.relay_address
                        && route.environment_ip.parse().ok() == Some(session.target_ip)
                        && session_can_continue(session, route)
                });
                if !still_current && !*session.cancel.borrow() {
                    self.forced_close_total.fetch_add(1, Ordering::Relaxed);
                    let _ = session.cancel.send(true);
                }
            }
        }
        self.http_pool
            .invalidate_instance(&change.instance_id, change.revision);
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
        // Only a protocol-aware HTTP service handler may admit a private guest
        // listener. Native forwarding and opaque streams never open these ports.
        if route
            .private_service_ports
            .contains(&route.target.target_port)
        {
            return Err(IngressOpenError::Forbidden);
        }
        if route.e2b.is_some() && native_execd_route(route) && tenant_id.is_empty() {
            return Err(IngressOpenError::Forbidden);
        }
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
        self.serve_http_inner(listener, None, IngressSecurity::Plaintext, None, shutdown)
            .await
    }

    pub async fn serve_http_tls(
        self: Arc<Self>,
        listener: TcpListener,
        acceptor: TlsAcceptor,
        shutdown: watch::Receiver<bool>,
    ) -> io::Result<()> {
        self.serve_http_inner(
            listener,
            Some(acceptor),
            IngressSecurity::Tls,
            None,
            shutdown,
        )
        .await
    }

    pub async fn serve_listener(
        self: Arc<Self>,
        listener: TcpListener,
        options: super::ListenerConfig,
        acceptor: Option<TlsAcceptor>,
        shutdown: watch::Receiver<bool>,
    ) -> io::Result<()> {
        options
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        if options.tls != acceptor.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "listener TLS configuration mismatch",
            ));
        }
        let security = if options.tls {
            IngressSecurity::Tls
        } else if options.allow_http_credentials {
            IngressSecurity::AuthenticatedHttp
        } else {
            IngressSecurity::Plaintext
        };
        self.serve_http_inner(
            listener,
            acceptor,
            security,
            Some(Arc::new(options)),
            shutdown,
        )
        .await
    }

    async fn serve_http_inner(
        self: Arc<Self>,
        listener: TcpListener,
        tls_acceptor: Option<TlsAcceptor>,
        ingress_security: IngressSecurity,
        options: Option<Arc<super::ListenerConfig>>,
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
                    let options = options.clone();
                    tokio::spawn(async move {
                        match tls_acceptor {
                            Some(acceptor) => {
                                if let Some(stream) = accept_ingress_tls(acceptor, stream, peer).await {
                                    gateway
                                        .serve_http_connection_with_options(stream, peer, ingress_security, options)
                                        .await;
                                }
                            }
                            None => gateway
                                .serve_http_connection_with_options(stream, peer, ingress_security, options)
                                .await,
                        }
                    });
                }
            }
        }
    }

    #[cfg(test)]
    async fn serve_http_connection<T>(
        self: Arc<Self>,
        stream: T,
        peer: std::net::SocketAddr,
        ingress_security: IngressSecurity,
    ) where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        self.serve_http_connection_with_options(stream, peer, ingress_security, None)
            .await;
    }

    async fn serve_http_connection_with_options<T>(
        self: Arc<Self>,
        stream: T,
        peer: std::net::SocketAddr,
        ingress_security: IngressSecurity,
        options: Option<Arc<super::ListenerConfig>>,
    ) where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let stream = crate::common::traffic::MeteredIo::new(stream, &self.request_metrics.traffic);
        let gateway = self;
        let service = service_fn(move |request: Request<Incoming>| {
            let gateway = gateway.clone();
            let options = options.clone();
            async move {
                if options
                    .as_ref()
                    .is_some_and(|options| !options.allows(request.method(), request.uri().path()))
                {
                    return Ok(plain(
                        StatusCode::FORBIDDEN,
                        "route is not allowed on this listener",
                    ));
                }
                gateway.handle_http(request, ingress_security, peer).await
            }
        });
        // Official E2B transports may use cleartext HTTP/2. Protocol detection
        // does not change the listener's security identity or route admission.
        if let Err(error) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
            .serve_connection_with_upgrades(TokioIo::new(stream), service)
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
                            // health 口请求同样计入全流（internal_health / http）。
                            let response = if gateway.full_stream {
                                StreamObserver::new(
                                    &gateway.request_metrics,
                                    RouteClass::InternalHealth,
                                )
                                .wrap_body(response, Transport::Http)
                            } else {
                                response
                            };
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
        let started = Instant::now();
        let processing = match ProcessingDeadline::from_headers(request.headers()) {
            Ok(deadline) => deadline,
            Err(message) => {
                // 早 400 也是一次请求：入口拒绝计入请求族。
                self.request_metrics
                    .observe(StatusCode::BAD_REQUEST, elapsed_us(started));
                let response = plain(StatusCode::BAD_REQUEST, message);
                let response = match self.new_stream_observer(&request) {
                    Some(observer) => observer.wrap_body(response, Transport::Http),
                    None => response,
                };
                return Ok(response);
            }
        };
        let budgeted = request.headers().contains_key(TIMEOUT_HEADER);
        request.headers_mut().remove(TIMEOUT_HEADER);
        request.extensions_mut().insert(processing.clone());
        let request_id = ensure_request_id(&mut request);
        let instance_id = self.access_fields(&request).1;
        let route = match self.access_fields(&request).0 {
            "port-forwarding" => "/{environment}/{port}/*",
            "tunnel" => "/tunnel/*",
            "direct" => self
                .direct_route(&request)
                .map(|route| adx_observability::trace::http_route(&route.stripped_path))
                .unwrap_or("/unmatched"),
            "reverse-proxy" => "/application/*",
            _ => adx_observability::trace::http_route(request.uri().path()),
        };
        let correlation = adx_observability::trace::correlation_id(
            request
                .headers()
                .get("x-trace-id")
                .and_then(|v| v.to_str().ok()),
            request
                .headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok()),
        );
        let trace = adx_observability::trace::Trace::http_with_correlation(
            request.method().as_str(),
            route,
            request
                .headers()
                .get("traceparent")
                .and_then(|v| v.to_str().ok()),
            request
                .headers()
                .get("tracestate")
                .and_then(|v| v.to_str().ok()),
            correlation,
        );
        trace.attribute(adx_observability::semconv::COMPONENT, "ingress".to_owned());
        let trace_id_header = if trace.is_recording() {
            trace.trace_id()
        } else {
            None
        };
        // `self` 会被下方闭包移入 handle_http_inner；出口观测点需独立持有指标句柄。
        let request_metrics = Arc::clone(&self.request_metrics);
        let is_connect = request.method() == http::Method::CONNECT;
        let mut stream_observer = self.new_stream_observer(&request);
        // WS（command-watch）与 CONNECT 的流挂在连接生命周期上：观察者随请求传入
        // 内层处理器，由会话/隧道任务在关闭时记录（详设 §6.2 挂点表）。
        let lifecycle_stream = is_connect
            || stream_observer
                .as_ref()
                .is_some_and(|observer| observer.route_class == RouteClass::CommandWatch);
        if lifecycle_stream {
            if let Some(observer) = stream_observer.take() {
                request.extensions_mut().insert(observer);
            }
        }
        // 带升级头的请求：克隆一份观察者随请求传入（M1-9）。101 时外层不包体，
        // 由升级拷贝任务记录；非 101 时外层包原件记录，extensions 份被取出后
        // 静默丢弃——StreamObserver 无 Drop，丢弃即不记录，两路互斥、恰好一次。
        if request.headers().contains_key(header::UPGRADE) {
            if let Some(observer) = stream_observer.as_ref() {
                request.extensions_mut().insert(observer.clone());
            }
        }
        trace
            .run(async {
                adx_observability::trace::inject_headers(request.headers_mut());
                let handled = self.handle_http_inner(request, ingress_security, peer);
                let (mut response, deadline_exceeded) = if budgeted {
                    match tokio::time::timeout_at(processing.at, handled).await {
                        Ok(response) => (response?, false),
                        Err(_) => (
                            processing_timeout_response(&processing, &request_id, &instance_id),
                            true,
                        ),
                    }
                } else {
                    (handled.await?, false)
                };
                if response
                    .headers()
                    .get("x-adx-error-code")
                    .is_some_and(|v| v == "ENV_RESUMING")
                {
                    response = resuming_response(&request_id, &instance_id);
                }
                if budgeted
                    && processing.is_forwarded()
                    && !deadline_exceeded
                    && response.status() != StatusCode::SWITCHING_PROTOCOLS
                {
                    let (parts, body) = response.into_parts();
                    response = Response::from_parts(
                        parts,
                        DeadlineBody::new(body, processing.at).boxed_unsync(),
                    );
                }
                if let Some(observer) = stream_observer {
                    // 101 无 body 可包：其全流样本由升级拷贝任务记录（观察者已随
                    // extensions 传入，M1-9）；lifecycle 请求（WS/CONNECT）同样已移交。
                    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
                        let transport = transport_of(&response);
                        response = observer.wrap_body(response, transport);
                    }
                }
                // 唯一观测点。早 400 分支在入口单独观测；其余路径——
                // 含外层超时 504 与 ENV_RESUMING 重写后的最终状态——在此恰好一次。
                request_metrics.observe(response.status(), elapsed_us(started));
                adx_observability::trace::attribute(
                    "http.response.status_code",
                    response.status().as_u16().to_string(),
                );
                if response.status().is_server_error() {
                    adx_observability::trace::error();
                }
                if let Some(trace_id) = trace_id_header.as_deref() {
                    if let Ok(value) = trace_id.parse() {
                        response.headers_mut().insert("x-trace-id", value);
                    }
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
        adx_observability::trace::attribute("request.id", request_id.clone());
        let method = request.method().to_string();
        let path = request.uri().path().to_owned();
        let (access_kind, instance_id, target_port) = self.access_fields(&request);
        let host_route = self.port_host_route(&request).is_some();
        adx_observability::trace::attribute("instance.id", instance_id.clone());
        adx_observability::trace::attribute("http.request.method", method.clone());
        #[cfg(feature = "agent-api")]
        let agent_data_error = if host_route {
            None
        } else {
            self.prepare_agent_data(&mut request, ingress_security)
                .await
                .err()
        };
        #[cfg(not(feature = "agent-api"))]
        let agent_data_error: Option<Response<ProxyBody>> = None;
        #[cfg(feature = "agent-api")]
        let binding_notice = request
            .extensions()
            .get::<super::agent_access::BindingNotice>()
            .cloned();
        #[cfg(feature = "agent-api")]
        let agent_forward = binding_notice.is_some()
            || request
                .extensions()
                .get::<super::agent_access::InstanceAccess>()
                .is_some();
        #[cfg(not(feature = "agent-api"))]
        let agent_forward = false;
        let response = if let Some(response) = agent_data_error {
            response
        } else if let Some(target) = match self.e2b_target(&request) {
            Ok(target) => target,
            Err(_) => return Ok(plain(StatusCode::BAD_REQUEST, "invalid E2B sandbox target")),
        } {
            self.proxy_e2b(request, ingress_security, target).await
        } else if host_route || agent_forward {
            // A selected Environment is already a data-plane target. Its ID must never
            // be interpreted again as a public management or configured proxy path.
            self.proxy_direct(request, ingress_security).await
        } else {
            match request.uri().path() {
                #[cfg(feature = "agent-api")]
                path if super::digitalmategateway::entrypoint::DigitalMateGatewayApi::matches(
                    path,
                ) && self.digitalmategateway_api.is_some() =>
                {
                    self.digitalmategateway_api
                        .as_ref()
                        .expect("configured DigitalMateGateway API")
                        .handle(request, self.clone())
                        .await
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
                        self.reverse_proxy
                            .proxy_with_tls(
                                request,
                                route,
                                peer,
                                ingress_security == IngressSecurity::Tls,
                            )
                            .await
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
            if let Some(notice) = binding_notice {
                notice.apply(response.headers_mut());
            }
            response
        };
        let status = response.status();
        let duration_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        // M0：observe 已迁往 handle_http 外层出口；此处仅供访问日志。
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
        use super::agent_access::AccessRequest;
        let Some(access) = AccessRequest::parse(request)
            .map_err(|error| super::agent_response::error_response(error, None))?
        else {
            return Ok(());
        };
        if ingress_security == IngressSecurity::Plaintext {
            return Err(tls_required());
        }
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
        if let adx_agent_core::target::Target::Instance(id) = &access.target {
            let port = access.port.ok_or_else(|| {
                response_text(
                    StatusCode::BAD_REQUEST,
                    "instance access requires an explicit port",
                )
            })?;
            request
                .headers_mut()
                .remove(adx_agent_core::parameters::HEADER);
            request
                .extensions_mut()
                .insert(super::agent_access::InstanceAccess(ParsedDirectPath {
                    instance_id: id.clone(),
                    target_port: port,
                    stripped_path: access.backend_uri.path().into(),
                    access_kind: AccessKind::PortForwarding,
                }));
            replace_request_path(request, &access.backend_uri.to_string())
                .map_err(|_| response_text(StatusCode::BAD_REQUEST, "invalid backend URI"))?;
            return Ok(());
        }
        let api = self
            .agent_api
            .as_ref()
            .ok_or_else(|| response_text(StatusCode::NOT_FOUND, "managed access is disabled"))?;
        api.prepare_data(request, &tenant, access)
            .await
            .map_err(|error| super::agent_response::error_response(error, None))?;
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
        if let Some(parsed) = self.port_host_route(request) {
            return (
                parsed.access_kind.as_str(),
                parsed.instance_id,
                parsed.target_port,
            );
        }
        if self.application_route(request).is_some() {
            return ("reverse-proxy", String::new(), 0);
        }
        if self.is_control_plane_path(request.uri().path()) {
            return ("control-plane", String::new(), 0);
        }
        self.direct_route(request)
            .map(|parsed| {
                (
                    parsed.access_kind.as_str(),
                    parsed.instance_id,
                    parsed.target_port,
                )
            })
            .unwrap_or(("unmatched", String::new(), 0))
    }

    /// 全流观测的请求分类（详设 §6.2 路由分类表；与内层 dispatch 的路径判定同源）。
    fn route_class_of<B>(&self, request: &Request<B>) -> RouteClass {
        let path = request.uri().path();
        if path == COMMAND_WATCH_PATH {
            return RouteClass::CommandWatch;
        }
        #[cfg(feature = "agent-api")]
        if self.digitalmategateway_api.is_some()
            && super::digitalmategateway::entrypoint::DigitalMateGatewayApi::matches(path)
        {
            return RouteClass::Jiuwen;
        }
        if self.access_fields(request).0 == "unmatched" {
            RouteClass::Unmatched
        } else {
            RouteClass::EnvironmentApi
        }
    }

    /// 观测开启时为请求创建全流观察者；关闭时返回 None（保留请求族计数）。
    fn new_stream_observer<B>(&self, request: &Request<B>) -> Option<StreamObserver> {
        self.full_stream
            .then(|| StreamObserver::new(&self.request_metrics, self.route_class_of(request)))
    }

    fn port_host_route<B>(&self, request: &Request<B>) -> Option<ParsedDirectPath> {
        if request.method() == http::Method::CONNECT {
            return None;
        }
        let domain = self.port_host_domain.as_deref()?;
        let host = request
            .headers()
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .or_else(|| request.uri().authority().map(http::uri::Authority::as_str))?;
        parse_port_host_route(host, request.uri().path(), domain)
    }

    fn e2b_target<B>(&self, request: &Request<B>) -> Result<Option<E2bTarget>, ()> {
        if !self.e2b_enabled || request.method() == http::Method::CONNECT {
            return Ok(None);
        }
        // The native <id>-<port> grammar overlaps E2B's <port>-<id> form.
        // Preserve native Host routing unless an explicit E2B selector is present;
        // parsing below still requires both headers and agreement with the Host.
        if self.port_host_route(request).is_some()
            && !request.headers().contains_key("e2b-sandbox-id")
            && !request.headers().contains_key("e2b-sandbox-port")
        {
            return Ok(None);
        }
        let Some(domain) = self.port_host_domain.as_deref() else {
            return Ok(None);
        };
        parse_e2b_target(request, domain).map_err(|_| ())
    }

    fn direct_route<B>(&self, request: &Request<B>) -> Option<ParsedDirectPath> {
        #[cfg(feature = "agent-api")]
        if let Some(access) = request
            .extensions()
            .get::<super::agent_access::InstanceAccess>()
        {
            return Some(access.0.clone());
        }
        self.port_host_route(request).or_else(|| {
            parse_direct_path(
                request.uri().path(),
                self.default_direct_port,
                self.default_tunnel_port,
            )
        })
    }

    fn is_control_plane_path(&self, path: &str) -> bool {
        self.control_plane_routes
            .iter()
            .any(|route| route.matches(path))
            || (self.e2b_enabled && is_e2b_management_path(path))
    }

    fn e2b_http_allowed(&self) -> bool {
        self.e2b_enabled && self.e2b_allow_http
    }

    fn e2b_http_management_allowed(&self, path: &str) -> bool {
        // Only canonical E2B paths may cross the plaintext management boundary.
        // Do not let upstream URI normalization turn an admitted E2B prefix
        // into a native or administrator path.
        self.e2b_http_allowed()
            && is_e2b_management_path(path)
            && !path.contains(['%', '\\'])
            && !path.contains("//")
            && !path.split('/').any(|segment| matches!(segment, "." | ".."))
    }

    async fn proxy_control_plane(
        &self,
        request: Request<Incoming>,
        ingress_security: IngressSecurity,
        peer: std::net::SocketAddr,
    ) -> Response<ProxyBody> {
        if (ingress_security == IngressSecurity::Plaintext
            || (ingress_security != IngressSecurity::Tls
                && is_e2b_management_path(request.uri().path())))
            && !self.e2b_http_management_allowed(request.uri().path())
        {
            return tls_required();
        }
        let route = ProxyRoute {
            name: "apiserver".into(),
            path_prefix: "/".into(),
            upstream: format!("http://{}", self.frontend_address),
            strip_prefix: false,
            host: None,
        };
        self.reverse_proxy
            .proxy_with_tls(
                request,
                &route,
                peer,
                ingress_security == IngressSecurity::Tls,
            )
            .await
    }

    fn health_response(&self, path: &str) -> Response<ProxyBody> {
        match path {
            "/healthz" => plain(StatusCode::OK, "ok"),
            "/readyz" if self.ready() => plain(StatusCode::OK, "ready"),
            "/readyz" => plain(StatusCode::SERVICE_UNAVAILABLE, "not ready"),
            "/metrics" => plain(StatusCode::OK, &{
                let pool = self.http_pool.metrics();
                // Embedded Ingress exports the API Server's owning PID only once.
                let process_metrics = adx_observability::process::metrics();
                let experience = self.render_experience();
                #[cfg(feature = "agent-api")]
                let observation = self.render_observation();
                #[cfg(not(feature = "agent-api"))]
                let observation = String::new();
                format!(
                    "data_plane_ingress_ready {}\n\
                     data_plane_ingress_route_cache_entries {}\n\
                     data_plane_ingress_route_watch_revision {}\n\
                     data_plane_ingress_route_watch_lag_seconds {}\n\
                     data_plane_ingress_route_point_get_total {}\n\
                     data_plane_ingress_active_sessions {}\n\
                     data_plane_ingress_h2_physical_connections {}\n\
                     data_plane_ingress_route_forced_close_total {}\n\
                     data_plane_ingress_backend_http_idle_connections {}\n\
                     data_plane_ingress_backend_http_opened_total {}\n\
                     data_plane_ingress_backend_http_reused_total {}\n\
                     data_plane_ingress_backend_http_discarded_total {}\n\
                     data_plane_ingress_backend_http_acquire_timeouts_total {}\n\
                     {}command_watch_connections {}\n\
                     command_watch_subscriptions {}\n\
                     command_watch_environments {}\n\
                     command_watch_reconnects {}\n\
                     command_watch_auth_failures {}\n\
                     command_watch_downstream_streams {}\n\
                     {process_metrics}\n\
                     {experience}{observation}",
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
                    self.request_metrics.prometheus(self.full_stream),
                    self.command_watch_metrics
                        .connections
                        .load(Ordering::Relaxed),
                    self.command_watch_metrics
                        .subscriptions
                        .load(Ordering::Relaxed),
                    self.command_watch_metrics
                        .environments
                        .load(Ordering::Relaxed),
                    self.command_watch_metrics
                        .reconnects
                        .load(Ordering::Relaxed),
                    self.command_watch_metrics
                        .auth_failures
                        .load(Ordering::Relaxed),
                    self.command_watch_metrics
                        .downstream_streams
                        .load(Ordering::Relaxed),
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
        let observer = request.extensions_mut().remove::<StreamObserver>();
        if request.method() != http::Method::GET {
            return observed(
                observer,
                plain(StatusCode::METHOD_NOT_ALLOWED, "command watch requires GET"),
            );
        }
        if ingress_security == IngressSecurity::Plaintext {
            return observed(observer, tls_required());
        }
        if !request
            .headers()
            .get(header::UPGRADE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
        {
            return observed(
                observer,
                plain(StatusCode::BAD_REQUEST, "websocket upgrade is required"),
            );
        }
        let key = match request
            .headers()
            .get("sec-websocket-key")
            .and_then(|value| value.to_str().ok())
        {
            Some(key) => key.to_owned(),
            None => {
                return observed(
                    observer,
                    plain(StatusCode::BAD_REQUEST, "missing sec-websocket-key"),
                )
            }
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
                return observed(observer, plain(error.status(), &error.to_string()));
            }
        };
        let upgraded = hyper::upgrade::on(request);
        let gateway = Arc::clone(self);
        // The upgrade task outlives the entry span. Capture its context now so
        // the session-close logs remain correlated with the originating request.
        tokio::spawn(adx_observability::trace::with_current_context(async move {
            match upgraded.await {
                Ok(upgraded) => {
                    let websocket = tokio_tungstenite::WebSocketStream::from_raw_socket(
                        TokioIo::new(upgraded),
                        tokio_tungstenite::tungstenite::protocol::Role::Server,
                        None,
                    )
                    .await;
                    let result = gateway
                        .run_external_command_watch(
                            websocket,
                            identity.tenant_id,
                            identity.expires_at_unix,
                        )
                        .await;
                    if let Err(error) = &result {
                        tracing::debug!(%error, "external command watch closed");
                    }
                    if let Some(observer) = observer {
                        // 正常关闭＝unknown（关闭套接字不是完成一次交换的证据）；
                        // 异常关闭＝failure，中断原因不可区分记 unknown（详设挂点表）。
                        match result {
                            Ok(()) => observer.record(
                                Transport::Ws,
                                adx_core::observation::Outcome::Unknown,
                                None,
                            ),
                            Err(_) => observer.record(
                                Transport::Ws,
                                adx_core::observation::Outcome::Failure,
                                Some(AbortReason::Unknown),
                            ),
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(%error, "command watch upgrade failed");
                    if let Some(observer) = observer {
                        observer.record(
                            Transport::Ws,
                            adx_core::observation::Outcome::Failure,
                            Some(AbortReason::ClientClosed),
                        );
                    }
                }
            }
        }));
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
                                        "capabilities": ["multiplexed-command-watch", "environment-downstream-aggregation"]
                                    }).to_string()
                                )).await?;
                            }
                            for command in message.commands {
                                let key = (command.environment_id.clone(), command.command_id.clone());
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
                                                outbound_tx.send(WatchState::subscription_failure(command, error)).await?;
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
                &command.environment_id,
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
            .subscribe(self, route, command.command_id.clone(), tenant_id)
            .await
            .map_err(IngressOpenError::Connect)?;
        let environment_id = command.environment_id.clone();
        let command_id = command.command_id.clone();
        let task = tokio::spawn(async move {
            loop {
                match subscription.events.recv().await {
                    Ok(state) if state.command_id == command_id => {
                        let _ = outbound.send(state.with_environment(&environment_id)).await;
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let _ = outbound
                            .send(WatchState {
                                op: "state",
                                environment_id: environment_id.clone(),
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
                                environment_id: environment_id.clone(),
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
        let mut route = match self
            .resolve_route(&instance_id, target_port, access_kind, request_id.clone())
            .await
        {
            Ok(route) => route,
            Err(error) => return error_response(error),
        };
        route.processing = request.extensions().get::<ProcessingDeadline>().cloned();
        if route.e2b.is_some() && !native_execd_route(&route) {
            return plain(
                StatusCode::FORBIDDEN,
                "use the HTTP service entrypoint for this sandbox",
            );
        }
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
                let on_upgrade = hyper::upgrade::on(&mut *request);
                let stream_started = Instant::now();
                let access_kind = access_kind.as_str();
                let observer = request.extensions_mut().remove::<StreamObserver>();
                // The CONNECT byte pump outlives the entry span; keep the
                // captured context so the stream-close log carries trace_id.
                tokio::spawn(adx_observability::trace::with_current_context(async move {
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
                        Ok((bytes_up, bytes_down)) => {
                            tracing::info!(
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
                            );
                            if let Some(observer) = observer {
                                observer.record(
                                    Transport::Connect,
                                    adx_core::observation::Outcome::Success,
                                    None,
                                );
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
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
                            );
                            if let Some(observer) = observer {
                                // 异常关闭＝unknown，原因不可区分（详设挂点表）。
                                observer.record(
                                    Transport::Connect,
                                    adx_core::observation::Outcome::Unknown,
                                    Some(AbortReason::Unknown),
                                );
                            }
                        }
                    }
                }));
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
        if request.headers().contains_key(header::UPGRADE) {
            if let Some((selection, backend_uri)) = api.take_service_selection(&mut request) {
                let stream = match api.service_access().connect_service(self, selection).await {
                    Ok(stream) => stream,
                    Err(super::agent_service::ServiceAccessError::Agent(error)) => {
                        return super::agent_response::error_response(error, None);
                    }
                    Err(super::agent_service::ServiceAccessError::Ingress(error)) => {
                        return error_response(error);
                    }
                };
                let query = backend_uri
                    .query()
                    .map(strip_token_query)
                    .unwrap_or_default();
                let uri = if query.is_empty() {
                    backend_uri.path().to_owned()
                } else {
                    format!("{}?{query}", backend_uri.path())
                };
                let Ok(uri) = uri.parse() else {
                    return response_text(StatusCode::BAD_REQUEST, "invalid backend URI");
                };
                *request.uri_mut() = uri;
                for name in [
                    H_INSTANCE_ID,
                    H_WORKLOAD_ID,
                    H_TARGET_IP,
                    H_TARGET_PORT,
                    "x-auth",
                    "authorization",
                ] {
                    request.headers_mut().remove(name);
                }
                let cancelled = stream.cancelled.clone();
                return proxy_http(request, stream, "environment", Some(cancelled)).await;
            }
        }
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
        let Some(parsed) = self.direct_route(&request) else {
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
        let request_id = header_string(&request, "x-request-id");
        let mut route = match self
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
                return Err((error_response(error), request));
            }
        };
        route.processing = request.extensions().get::<ProcessingDeadline>().cloned();
        if route.e2b.is_some() && !native_execd_route(&route) {
            let query = request
                .uri()
                .query()
                .map(|value| format!("?{value}"))
                .unwrap_or_default();
            if replace_request_path(&mut request, &format!("{}{query}", parsed.stripped_path))
                .is_err()
            {
                return Ok(plain(StatusCode::BAD_REQUEST, "invalid service path"));
            }
            return Ok(self
                .proxy_service_route(request, ingress_security, route)
                .await);
        }
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
        {
            request.headers_mut().remove(H_INSTANCE_ID);
            request.headers_mut().remove(H_WORKLOAD_ID);
            request.headers_mut().remove(H_TARGET_IP);
            request.headers_mut().remove(H_TARGET_PORT);
            request.headers_mut().remove("x-auth");
            request.headers_mut().remove(header::AUTHORIZATION);
        }
        if route.e2b.is_some() {
            strip_service_credentials(&mut request, route.target.target_port);
        }
        #[cfg(feature = "agent-api")]
        if parsed.access_kind == AccessKind::Direct {
            if let Some(access) = &self.execd_access {
                if parsed.target_port == access.port {
                    if let Err(error) = access
                        .apply(&tenant_id, &parsed.instance_id, &mut request)
                        .await
                    {
                        let status = match error {
                            super::environment_files::ReadError::NotFound => StatusCode::NOT_FOUND,
                            _ => StatusCode::BAD_GATEWAY,
                        };
                        return Ok(plain(status, "Environment file access unavailable"));
                    }
                }
            }
        }
        if matches!(request.uri().path(), "/healthz" | "/metrics") {
            if let Err(error) = self.authorize(&route, &tenant_id) {
                return Ok(error_response(error));
            }
            if route.needs_resume {
                return Ok(plain(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Environment is paused",
                ));
            }
            // Probe connections stay passive even when Relay rejects a stale
            // binding. Do not pool them with business connections or demand wake.
            let stream = match self.open_authorized_stream_with_activity(route, true).await {
                Ok(stream) => stream,
                Err(error) => {
                    let failure = IngressOpenError::Connect(error);
                    record_ingress_error(&failure);
                    return Ok(error_response(failure));
                }
            };
            let cancelled = stream.cancelled.clone();
            return Ok(proxy_http(request, stream, "environment", Some(cancelled)).await);
        }

        let upgrade_requested = request.headers().contains_key(header::UPGRADE);
        if upgrade_requested {
            let stream = match self.open_resolved_stream(route, &tenant_id).await {
                Ok(stream) => stream,
                Err(error) => {
                    record_ingress_error(&error);
                    return Err((error_response(error), request));
                }
            };
            let cancelled = stream.cancelled.clone();
            return Ok(proxy_http(request, stream, "environment", Some(cancelled)).await);
        }

        if let Err(error) = self.authorize(&route, &tenant_id) {
            return Ok(error_response(error));
        }
        let mut route = match self.prepare_route(route, &tenant_id).await {
            Ok(route) => route,
            Err(error) => {
                record_ingress_error(&error);
                return Err((error_response(error), request));
            }
        };
        strip_hop_by_hop_headers(request.headers_mut());
        let mut retried = false;
        loop {
            let Some(route_revision) = self.resolver.current_route_revision(&route) else {
                return Err((error_response(IngressOpenError::RouteChanged), request));
            };
            let key = BackendHttpPoolKey {
                route_revision,
                relay_address: route.relay_address.clone(),
                instance_id: route.target.instance_id.clone(),
                workload_id: route.target.workload_id.clone(),
                target_ip: route.target.target_ip,
                target_port: route.target.target_port,
            };
            match self
                .http_pool
                .send_recoverable(
                    key,
                    request,
                    || self.open_authorized_stream(route.clone()),
                    || {
                        if !self.ready()
                            || self.resolver.current_route_revision(&route) != Some(route_revision)
                        {
                            return Err(io::Error::new(
                                io::ErrorKind::NotConnected,
                                IngressOpenError::RouteChanged,
                            ));
                        }
                        Ok(())
                    },
                )
                .await
            {
                Ok(mut response) => {
                    response.headers_mut().remove("x-adx-error-code");
                    strip_hop_by_hop_headers(response.headers_mut());
                    return Ok(response.map(|body| {
                        body.map_err(|error| -> BoxError { Box::new(error) })
                            .boxed_unsync()
                    }));
                }
                Err((error, Some(unconsumed))) => {
                    let route_changed = matches!(&error, BackendHttpPoolError::Closed)
                        || matches!(&error, BackendHttpPoolError::Connect(error)
                            if matches!(error.get_ref().and_then(|e| e.downcast_ref::<IngressOpenError>()),
                                Some(IngressOpenError::RouteChanged)));
                    if retried || !self.ready() || !route_changed {
                        return Err((backend_pool_error_response(error), unconsumed));
                    }
                    // Only an untouched request can follow the current route. Keep
                    // its authorization scope and deadline, and retry at most once.
                    retried = true;
                    let mut replacement = match self
                        .resolve_route(
                            &route.target.instance_id,
                            route.target.target_port,
                            route.access_kind,
                            route.target.request_id.clone(),
                        )
                        .await
                    {
                        Ok(route) => route,
                        Err(error) => return Err((error_response(error), unconsumed)),
                    };
                    if replacement.tenant_id != route.tenant_id
                        || replacement.tunnel_security_mode != route.tunnel_security_mode
                        || replacement.port_forward_auth_mode != route.port_forward_auth_mode
                        || replacement.private_service_ports != route.private_service_ports
                        || replacement.e2b != route.e2b
                    {
                        return Err((error_response(IngressOpenError::Forbidden), unconsumed));
                    }
                    replacement.processing = route.processing.clone();
                    route = match self.prepare_route(replacement, &tenant_id).await {
                        Ok(route) => route,
                        Err(error) => return Err((error_response(error), unconsumed)),
                    };
                    request = unconsumed;
                }
                Err((error, None)) => return Ok(backend_pool_error_response(error)),
            }
        }
    }

    async fn proxy_e2b(
        &self,
        request: Request<Incoming>,
        ingress_security: IngressSecurity,
        target: E2bTarget,
    ) -> Response<ProxyBody> {
        if ingress_security != IngressSecurity::Tls && !self.e2b_http_allowed() {
            return tls_required();
        }
        let request_id = header_string(&request, "x-request-id");
        let route = match self
            .resolve_route(
                &target.sandbox_id,
                target.port,
                AccessKind::PortForwarding,
                request_id,
            )
            .await
        {
            Ok(route) => route,
            Err(error) => return error_response(error),
        };
        self.proxy_service_route(request, ingress_security, route)
            .await
    }

    async fn proxy_service_route(
        &self,
        mut request: Request<Incoming>,
        ingress_security: IngressSecurity,
        route: RouteHandle,
    ) -> Response<ProxyBody> {
        if ingress_security != IngressSecurity::Tls && !self.e2b_http_allowed() {
            return tls_required();
        }
        // An opaque tunnel could carry unchecked envd control requests. Execd
        // is available only through its separately authenticated native entry.
        if request.method() == http::Method::CONNECT {
            return e2b_policy_error(E2bAuthorizationError::Forbidden);
        }
        let target = E2bTarget {
            sandbox_id: route.target.instance_id.clone(),
            port: route.target.target_port,
        };
        let Some(policy) = route.e2b.as_ref() else {
            return plain(
                StatusCode::NOT_FOUND,
                "E2B routing is not enabled for this sandbox",
            );
        };
        if let Err(error) = policy.authorize(&request, &target) {
            return e2b_policy_error(error);
        }
        if !self.resolver.route_is_current(&route) {
            return error_response(IngressOpenError::RouteChanged);
        }
        strip_service_credentials(&mut request, target.port);
        if policy.envd_gateway_auth
            && target.port == super::e2b_route::ENVD_PORT
            && strip_gateway_envd_credentials(&mut request).is_err()
        {
            return plain(StatusCode::BAD_REQUEST, "invalid envd file URL");
        }
        let upgrade_requested = request.headers().contains_key(header::UPGRADE);
        if upgrade_requested {
            let stream = match self.open_service_http_stream(route).await {
                Ok(stream) => stream,
                Err(error) => return error_response(IngressOpenError::Connect(error)),
            };
            let cancelled = stream.cancelled.clone();
            return proxy_http(request, stream, "envd-or-application", Some(cancelled)).await;
        }
        strip_hop_by_hop_headers(request.headers_mut());
        let Some(route_revision) = self.resolver.current_route_revision(&route) else {
            return error_response(IngressOpenError::RouteChanged);
        };
        let key = BackendHttpPoolKey {
            route_revision,
            relay_address: route.relay_address.clone(),
            instance_id: route.target.instance_id.clone(),
            workload_id: route.target.workload_id.clone(),
            target_ip: route.target.target_ip,
            target_port: route.target.target_port,
        };
        match self
            .http_pool
            .send_recoverable(
                key,
                request,
                || self.open_service_http_stream(route.clone()),
                || {
                    if !self.ready()
                        || self.resolver.current_route_revision(&route) != Some(route_revision)
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::NotConnected,
                            IngressOpenError::RouteChanged,
                        ));
                    }
                    Ok(())
                },
            )
            .await
        {
            Ok(mut response) => {
                strip_hop_by_hop_headers(response.headers_mut());
                response.map(|body| {
                    body.map_err(|error| -> BoxError { Box::new(error) })
                        .boxed_unsync()
                })
            }
            Err((error, _)) => backend_pool_error_response(error),
        }
    }
}

fn strip_service_credentials<B>(request: &mut Request<B>, port: u16) {
    // All selectors are consumed at the ingress trust boundary. The traffic
    // credential never reaches the guest; X-Access-Token reaches only envd.
    for name in [
        H_INSTANCE_ID,
        H_WORKLOAD_ID,
        H_TARGET_IP,
        H_TARGET_PORT,
        H_ACCESS_KIND,
        "e2b-sandbox-id",
        "e2b-sandbox-port",
        "e2b-traffic-access-token",
    ] {
        request.headers_mut().remove(name);
    }
    if port != super::e2b_route::ENVD_PORT {
        request.headers_mut().remove("x-access-token");
    }
}

fn e2b_policy_error(error: E2bAuthorizationError) -> Response<ProxyBody> {
    let message = match error {
        E2bAuthorizationError::Unavailable => "E2B sandbox route is unavailable",
        E2bAuthorizationError::Forbidden => "E2B route is forbidden",
        E2bAuthorizationError::Unauthorized => "E2B token is invalid or missing",
    };
    plain(error.status(), message)
}

fn strip_gateway_envd_credentials<B>(request: &mut Request<B>) -> Result<(), http::Error> {
    request.headers_mut().remove("x-access-token");
    if request.uri().path() == "/files" {
        if let Some(query) = request.uri().query() {
            let pairs: Vec<_> = url::form_urlencoded::parse(query.as_bytes())
                .filter(|(name, _)| !matches!(name.as_ref(), "signature" | "signature_expiration"))
                .collect();
            let mut remaining = url::form_urlencoded::Serializer::new(String::new());
            for (name, value) in pairs {
                remaining.append_pair(&name, &value);
            }
            let remaining = remaining.finish();
            let path_and_query = if remaining.is_empty() {
                "/files".to_owned()
            } else {
                format!("/files?{remaining}")
            };
            replace_request_path(request, &path_and_query)?;
        }
    }
    Ok(())
}

fn replace_request_path<B>(request: &mut Request<B>, path: &str) -> Result<(), http::Error> {
    // HTTP/2 carries Host in :authority. Path and credential rewrites must
    // preserve the original scheme and authority before shared forwarding.
    let mut parts = request.uri().clone().into_parts();
    parts.path_and_query = Some(path.parse()?);
    *request.uri_mut() = http::Uri::from_parts(parts)?;
    Ok(())
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
    environment_id: String,
    command_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WatchState {
    op: &'static str,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment_id: String,
    command_id: String,
    status: String,
    state_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl WatchState {
    fn with_environment(mut self, environment_id: &str) -> Self {
        self.environment_id = environment_id.to_owned();
        self
    }

    fn subscription_failure(command: WatchCommandRef, error: IngressOpenError) -> Self {
        // The command already exists. A temporary downstream failure should
        // reconnect its subscription, never reject it or start another command.
        let retryable = match &error {
            IngressOpenError::Resolve(
                ResolveError::NotReady | ResolveError::Unavailable(_) | ResolveError::Resuming,
            )
            | IngressOpenError::Draining
            | IngressOpenError::RouteChanged => true,
            IngressOpenError::Connect(error) => !matches!(
                error.kind(),
                io::ErrorKind::PermissionDenied | io::ErrorKind::InvalidData
            ),
            _ => false,
        };
        let mut state = Self::rejected(command, error.to_string());
        if retryable {
            state.status = "RECONNECT".into();
        }
        state
    }

    fn rejected(command: WatchCommandRef, error: String) -> Self {
        Self {
            op: "state",
            environment_id: command.environment_id,
            command_id: command.command_id,
            status: "REJECTED".into(),
            state_version: 0,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WatchKey {
    environment_id: String,
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
        _tenant_id: &str,
    ) -> io::Result<HubSubscription> {
        let key = WatchKey {
            environment_id: route.target.instance_id.clone(),
        };
        let mut entries = self.entries.lock().await;
        let entry = if let Some(entry) = entries.get(&key) {
            entry.clone()
        } else {
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            let request = "ws://execd/commands/watch"
                .into_client_request()
                .map_err(|error| io::Error::other(error.to_string()))?;
            #[cfg(feature = "agent-api")]
            let request = {
                let mut request = request;
                if let Some(access) = &gateway.execd_access {
                    access
                        .apply(_tenant_id, &route.target.instance_id, &mut request)
                        .await
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::PermissionDenied,
                                "Environment runtime access unavailable",
                            )
                        })?;
                }
                request
            };
            let stream = gateway
                .open_authorized_stream_with_activity(route, true)
                .await?;
            let (websocket, _) = tokio_tungstenite::client_async(request, stream)
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
            let (control, controls) = mpsc::channel(self.config.queue_capacity);
            let (events, _) = broadcast::channel(self.config.queue_capacity);
            let entry = WatchEntry {
                control,
                events: events.clone(),
            };
            entries.insert(key.clone(), entry.clone());
            self.metrics.environments.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .downstream_streams
                .fetch_add(1, Ordering::Relaxed);
            let all_entries = Arc::clone(&self.entries);
            let metrics = Arc::clone(&self.metrics);
            let ping_interval = self.config.ping_interval;
            tokio::spawn(async move {
                // Retire the shared entry before waiting for a close reply so
                // a new subscriber never joins a socket already shutting down.
                let retire = async {
                    all_entries.lock().await.remove(&key);
                    metrics.environments.fetch_sub(1, Ordering::Relaxed);
                    metrics.downstream_streams.fetch_sub(1, Ordering::Relaxed);
                    metrics.reconnects.fetch_add(1, Ordering::Relaxed);
                };
                run_downstream_watch(websocket, controls, events, ping_interval, retire).await;
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
    retire: impl std::future::Future<Output = ()>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut graceful = false;
    let mut refs = HashMap::<String, usize>::new();
    let mut ping = tokio::time::interval(ping_interval);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            control = controls.recv() => {
                let Some(control) = control else { graceful = true; break; };
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
                if refs.is_empty() && op == "unsubscribe" { graceful = true; break; }
            }
            incoming = websocket.next() => {
                let Some(Ok(incoming)) = incoming else { break; };
                match incoming {
                    tokio_tungstenite::tungstenite::Message::Text(text) => {
                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                            let state = WatchState {
                                op: "state",
                                environment_id: String::new(),
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
                    tokio_tungstenite::tungstenite::Message::Close(_) => {
                        // Tungstenite queues the peer's close acknowledgement.
                        let _ = websocket.flush().await;
                        break;
                    }
                    _ => {}
                }
            }
            _ = ping.tick() => {
                if websocket.send(tokio_tungstenite::tungstenite::Message::Ping(Vec::new())).await.is_err() { break; }
            }
        }
    }
    retire.await;
    if graceful {
        // A normal unsubscribe must not look like an abruptly reset runtime
        // connection. Bound the handshake so a lost peer cannot retain a task.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            websocket.close(None).await?;
            while let Some(message) = websocket.next().await {
                if matches!(message?, tokio_tungstenite::tungstenite::Message::Close(_)) {
                    break;
                }
            }
            Ok::<_, tokio_tungstenite::tungstenite::Error>(())
        })
        .await;
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
            "environment HTTP pool is saturated",
        ),
        // Closed is returned only before send_request, so retry is unambiguous.
        BackendHttpPoolError::Closed => error_response(IngressOpenError::RouteChanged),
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

pub(super) async fn wait_for_route_cancellation(cancelled: Option<watch::Receiver<bool>>) {
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

pub(super) async fn proxy_http<T>(
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
    // M1-9：升级请求的观察者随 extensions 传入（外层插入克隆），发出前取出。
    let observer = request.extensions_mut().remove::<StreamObserver>();
    let (mut sender, connection) =
        match hyper::client::conn::http1::handshake(TokioIo::new(stream)).await {
            Ok(parts) => parts,
            Err(error) => return plain(StatusCode::BAD_GATEWAY, &error.to_string()),
        };
    let connection_cancelled = cancelled.clone();
    // The connection driver outlives the request span; retain its context so
    // the close log can be correlated.
    tokio::spawn(adx_observability::trace::with_current_context(async move {
        tokio::select! {
            _ = wait_for_route_cancellation(connection_cancelled) => {}
            result = connection.with_upgrades() => {
                if let Err(error) = result {
                    tracing::debug!(%error, upstream = upstream_name, "upstream HTTP connection closed");
                }
            }
        }
    }));
    if let Some(processing) = request.extensions().get::<ProcessingDeadline>() {
        processing.forwarded();
    }
    let mut response = match sender.send_request(request).await {
        Ok(response) => response,
        Err(error) => return plain(StatusCode::BAD_GATEWAY, &error.to_string()),
    };
    response.headers_mut().remove("x-adx-error-code");
    if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        if let Some(downstream_upgrade) = downstream_upgrade {
            let upstream_upgrade = hyper::upgrade::on(&mut response);
            tokio::spawn(async move {
                // 升级失败按侧别携带错误类别：下游→NotConnected（client_closed）、
                // 上游→其他（protocol_error）；路由取消由 relay_until_cancelled 以
                // ConnectionAborted 返回（详设 M1-9 记录规则）。
                let relay = async move {
                    let downstream = downstream_upgrade.await.map_err(|error| {
                        io::Error::new(io::ErrorKind::NotConnected, error.to_string())
                    })?;
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
                let result = relay_until_cancelled(relay, cancelled).await;
                if let Some(observer) = observer {
                    // 正常结束＝unknown（关 socket 不是完成交换的证据）；
                    // success 对代理升级永不导出（详设 M1-9）。
                    match result {
                        Ok(_) => observer.record(
                            Transport::Ws,
                            adx_core::observation::Outcome::Unknown,
                            None,
                        ),
                        Err(error) if error.kind() == io::ErrorKind::ConnectionAborted => observer
                            .record(
                                Transport::Ws,
                                adx_core::observation::Outcome::Failure,
                                Some(AbortReason::Unknown),
                            ),
                        Err(error) if error.kind() == io::ErrorKind::NotConnected => observer
                            .record(
                                Transport::Ws,
                                adx_core::observation::Outcome::Failure,
                                Some(AbortReason::ClientClosed),
                            ),
                        Err(_) => observer.record(
                            Transport::Ws,
                            adx_core::observation::Outcome::Failure,
                            Some(AbortReason::ProtocolError),
                        ),
                    }
                }
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
    revision: u64,
    instance_id: String,
    workload_id: String,
    relay_address: String,
    target_ip: std::net::IpAddr,
    route: RouteHandle,
    cancel: watch::Sender<bool>,
}

fn session_can_continue(session: &ActiveSession, route: &crate::common::route::RouteInfo) -> bool {
    route.environment_status.code == 3
        && route.environment_id == session.workload_id
        && route.relay_address == session.relay_address
        && route.environment_ip.parse().ok() == Some(session.target_ip)
        && session.route.private_service_ports == route.private_service_ports
        && match (&session.route.e2b, &route.e2b) {
            (None, None) => true,
            (Some(previous), Some(current)) => e2b_session_policy_unchanged(previous, current),
            _ => false,
        }
}

fn native_execd_route(route: &RouteHandle) -> bool {
    route.access_kind == AccessKind::Direct
        && route.target.target_port == super::e2b_route::EXECD_PORT
}

fn e2b_session_policy_unchanged(
    previous: &super::e2b_route::E2bRouteConfig,
    current: &super::e2b_route::E2bRouteConfig,
) -> bool {
    current.envd_ready
        && previous.envd_token_sha256 == current.envd_token_sha256
        && previous.envd_gateway_auth == current.envd_gateway_auth
        && previous.envd_signing_key == current.envd_signing_key
        && previous.traffic_token_sha256 == current.traffic_token_sha256
        && previous.allow_public_traffic == current.allow_public_traffic
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

impl IngressStream {
    #[cfg(feature = "agent-api")]
    pub(super) fn cancellation(&self) -> watch::Receiver<bool> {
        self.cancelled.clone()
    }
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
    let current = header_string(request, "x-adx-request-id");
    let current = if current.is_empty() {
        header_string(request, "x-request-id")
    } else {
        current
    };
    if !current.is_empty() {
        if let Ok(value) = http::HeaderValue::from_str(&current) {
            request.headers_mut().insert("x-request-id", value);
        }
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
        IngressSecurity::AuthenticatedHttp => "authenticated_http",
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

/// Connection and route failures are server-side failures; record an OTel
/// exception on the entry span. Client errors (404, auth, forbidden) stay
/// Unset per OTel conventions.
fn record_ingress_error(error: &IngressOpenError) {
    match error {
        IngressOpenError::Connect(error) => {
            adx_observability::trace::record_error("connection", error.to_string())
        }
        IngressOpenError::Resolve(ResolveError::Unavailable(message)) => {
            adx_observability::trace::record_error("resolve", message.clone())
        }
        IngressOpenError::Resolve(ResolveError::MissingEndpoint) => {
            adx_observability::trace::record_error("resolve", error.to_string())
        }
        _ => {}
    }
}

fn error_response(error: IngressOpenError) -> Response<ProxyBody> {
    // RouteChanged is emitted only before CONNECT or the business send. Keep
    // that typed evidence so clients can retry; never classify by message text.
    if matches!(
        &error,
        IngressOpenError::Resolve(ResolveError::Resuming) | IngressOpenError::RouteChanged
    ) || matches!(&error, IngressOpenError::Connect(e) if e.get_ref().and_then(|e| e.downcast_ref::<ResolveError>()) == Some(&ResolveError::Resuming)
        || matches!(e.get_ref().and_then(|e| e.downcast_ref::<IngressOpenError>()), Some(IngressOpenError::RouteChanged)))
    {
        return resuming_response("", "");
    }
    let rejected = match &error {
        IngressOpenError::Resolve(ResolveError::ResumeRejected(message)) => Some(message.as_str()),
        IngressOpenError::Connect(error) => match error
            .get_ref()
            .and_then(|e| e.downcast_ref::<ResolveError>())
        {
            Some(ResolveError::ResumeRejected(message)) => Some(message.as_str()),
            _ => None,
        },
        _ => None,
    };
    if let Some(message) = rejected {
        let body = serde_json::json!({"error": {"code": "ENV_UNAVAILABLE", "message": message,
            "retry": "never", "outcome": "not_started", "requestForwarded": false}});
        let mut response = plain(StatusCode::CONFLICT, &body.to_string());
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        return response;
    }
    let status = match &error {
        IngressOpenError::Resolve(ResolveError::ResumeRejected(_)) => StatusCode::CONFLICT,
        IngressOpenError::Resolve(ResolveError::NotFound) => StatusCode::NOT_FOUND,
        IngressOpenError::Resolve(
            ResolveError::NotReady | ResolveError::Unavailable(_) | ResolveError::Resuming,
        )
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

fn elapsed_us(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
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
    use crate::ingress::{H2PoolConfig, RouteStore};

    #[test]
    fn full_stream_storage_dimensions_match_label_enums() {
        // FullStreamMetrics 的数组维度与枚举值集必须一致，否则稠密导出错位。
        assert_eq!(adx_core::observation::Outcome::ALL.len(), 4);
        assert_eq!(Transport::COUNT, Transport::ALL.len());
        assert_eq!(RouteClass::COUNT, RouteClass::ALL.len());
        assert_eq!(AbortReason::COUNT, AbortReason::ALL.len());
    }

    #[tokio::test]
    async fn preforward_route_change_is_retryable_but_other_connection_errors_are_not() {
        for error in [
            IngressOpenError::RouteChanged,
            IngressOpenError::Connect(io::Error::new(
                io::ErrorKind::NotConnected,
                IngressOpenError::RouteChanged,
            )),
        ] {
            let response = error_response(error);
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                response.headers().get("x-adx-error-code").unwrap(),
                "ENV_RESUMING"
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["error"]["requestForwarded"], false);
        }
        // Words alone are not evidence that no application bytes were sent.
        let response = error_response(IngressOpenError::Connect(io::Error::new(
            io::ErrorKind::NotConnected,
            "route changed while opening the stream",
        )));
        assert!(response.headers().get("x-adx-error-code").is_none());
    }

    #[test]
    fn watch_subscription_failure_reconnects_transient_transport_but_rejects_permissions() {
        let command = || WatchCommandRef {
            environment_id: "environment".into(),
            command_id: "command".into(),
        };
        for error in [
            IngressOpenError::Connect(io::Error::other("HTTP error: 503 Service Unavailable")),
            IngressOpenError::Resolve(ResolveError::NotReady),
            IngressOpenError::RouteChanged,
        ] {
            assert_eq!(
                WatchState::subscription_failure(command(), error).status,
                "RECONNECT"
            );
        }
        for error in [
            IngressOpenError::Forbidden,
            IngressOpenError::Resolve(ResolveError::NotFound),
            IngressOpenError::Connect(io::Error::new(io::ErrorKind::PermissionDenied, "denied")),
            IngressOpenError::Connect(io::Error::new(
                io::ErrorKind::InvalidData,
                "identity conflict",
            )),
        ] {
            assert_eq!(
                WatchState::subscription_failure(command(), error).status,
                "REJECTED"
            );
        }
    }

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
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        TlsAcceptor::from(Arc::new(config))
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
            needs_resume: false,
            processing: None,
            access_kind: AccessKind::Direct,
            relay_address: "node:8443".into(),
            tenant_id: "tenant-a".into(),
            tunnel_security_mode: tunnel,
            port_forward_auth_mode: port_forward,
            private_service_ports: Vec::new(),
            e2b: None,
            target: ConnectTarget {
                instance_id: "instance-a".into(),
                workload_id: "environment-a".into(),
                target_ip: "10.88.0.2".parse().unwrap(),
                target_port: 22,
                request_id: "request-a".into(),
            },
        }
    }

    #[tokio::test]
    async fn health_metrics_include_business_and_single_process_resources() {
        let ingress = Ingress::new(
            Arc::new(IngressRouteResolver::new(Arc::new(
                super::super::RouteStore::new(),
            ))),
            DataPlaneL4Connector::new(super::super::H2PoolConfig::default()),
            IngressAuthenticator::new(true, false, "", std::time::Duration::from_secs(30)).unwrap(),
            50090,
            8765,
            "127.0.0.1:18888",
            Vec::new(),
        );
        let response = ingress.health_response("/metrics");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("data_plane_ingress_ready 0\n"));
        assert!(body.contains("data_plane_ingress_http_requests_total 0\n"));

        #[cfg(target_os = "linux")]
        {
            let pid = std::process::id();
            for metric in [
                "process_memory_usage_bytes",
                "process_memory_virtual_bytes",
                "process_open_file_descriptors",
                "process_threads",
                "process_uptime_seconds",
            ] {
                let prefix = format!("{metric}{{process_pid=\"{pid}\"}} ");
                let samples: Vec<_> = body
                    .lines()
                    .filter(|line| line.starts_with(&prefix))
                    .collect();
                assert_eq!(samples.len(), 1, "one sample per owning process: {metric}");
                let value: f64 = samples[0].strip_prefix(&prefix).unwrap().parse().unwrap();
                assert!(
                    value.is_finite() && value >= 0.0,
                    "invalid {metric}: {value}"
                );
            }
            for state in ["user", "system"] {
                let prefix = format!(
                    "process_cpu_time_seconds_total{{process_pid=\"{pid}\",state=\"{state}\"}} "
                );
                assert_eq!(
                    body.lines()
                        .filter(|line| line.starts_with(&prefix))
                        .count(),
                    1
                );
            }
        }
    }

    fn e2b_route_config() -> super::super::e2b_route::E2bRouteConfig {
        super::super::e2b_route::E2bRouteConfig {
            envd_token_sha256: "a".repeat(64),
            traffic_token_sha256: "b".repeat(64),
            allow_public_traffic: false,
            envd_ready: true,
            envd_gateway_auth: false,
            envd_signing_key: String::new(),
        }
    }

    #[test]
    fn e2b_lease_renewal_keeps_session_policy_but_revocation_does_not() {
        let current = e2b_route_config();
        let renewed = current.clone();
        assert!(e2b_session_policy_unchanged(&current, &renewed));

        let mut revoked = renewed.clone();
        revoked.traffic_token_sha256 = "c".repeat(64);
        assert!(!e2b_session_policy_unchanged(&renewed, &revoked));

        let mut unavailable = renewed;
        unavailable.envd_ready = false;
        assert!(!e2b_session_policy_unchanged(&current, &unavailable));
    }

    #[test]
    fn e2b_management_paths_require_a_configured_domain() {
        let store = Arc::new(RouteStore::new());
        let ingress = Ingress::new(
            Arc::new(IngressRouteResolver::new(store)),
            DataPlaneL4Connector::new(H2PoolConfig::default()),
            IngressAuthenticator::disabled(),
            EXECD_COMMAND_PORT,
            8765,
            "127.0.0.1:8080",
            Vec::new(),
        );
        assert!(!ingress.is_control_plane_path("/v2/sandboxes"));
        let ingress = ingress.with_port_host_domain(Some("sandbox.example.test".into()));
        assert!(!ingress.is_control_plane_path("/v2/sandboxes"));
        let ingress = ingress.with_e2b_enabled(true);
        assert!(ingress.is_control_plane_path("/v2/sandboxes"));
        assert!(ingress.is_control_plane_path("/templates/snapshot-a"));
        assert!(!ingress.is_control_plane_path("/snapshots-old"));
    }

    #[test]
    fn request_metrics_aggregate_status_and_latency_without_request_labels() {
        let metrics = IngressRequestMetrics::default();
        metrics.observe(StatusCode::OK, 900);
        metrics.observe(StatusCode::BAD_REQUEST, 1_500);
        metrics.observe(StatusCode::SERVICE_UNAVAILABLE, 200_000);

        let output = metrics.prometheus(true);
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
    fn request_metrics_keep_slow_requests_in_finite_latency_buckets() {
        let metrics = IngressRequestMetrics::default();
        metrics.observe(StatusCode::OK, 200_000);
        metrics.observe(StatusCode::OK, 2_000_000);
        metrics.observe(StatusCode::OK, 200_000_000);

        let output = metrics.prometheus(true);
        assert!(output
            .contains("data_plane_ingress_http_request_duration_seconds_bucket{le=\"0.250\"} 1\n"));
        assert!(output
            .contains("data_plane_ingress_http_request_duration_seconds_bucket{le=\"2.500\"} 2\n"));
        assert!(output.contains(
            "data_plane_ingress_http_request_duration_seconds_bucket{le=\"120.000\"} 2\n"
        ));
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
            "exact:/,exact:/healthz,prefix:/api/environment,prefix:/serverless/v1/functions,prefix:/terminal,prefix:/global-scheduler",
        )
        .unwrap();
        for path in [
            "/",
            "/api/environment/v1/environments",
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
            "/api/environment-escape",
        ] {
            assert!(!routes.iter().any(|route| route.matches(path)), "{path}");
        }
    }

    #[test]
    fn rejects_invalid_static_route_specs() {
        assert!(parse_static_routes("").is_err());
        assert!(parse_static_routes("/api/environment").is_err());
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

#[cfg(all(test, feature = "mock-e2e"))]
mod route_recovery_tests;

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

#[cfg(test)]
mod processing_tests;

#[cfg(test)]
mod access_policy_tests;

#[cfg(test)]
mod watch_close_contract_tests {
    use super::*;

    #[tokio::test]
    async fn last_command_subscription_closes_downstream_websocket_cleanly() {
        use tokio_tungstenite::{
            tungstenite::{protocol::Role, Message},
            WebSocketStream,
        };
        let (client, peer) = tokio::io::duplex(4096);
        let client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let mut peer = WebSocketStream::from_raw_socket(peer, Role::Server, None).await;
        let (send, receive) = mpsc::channel(4);
        let (events, _) = broadcast::channel(4);
        let task = tokio::spawn(run_downstream_watch(
            client,
            receive,
            events,
            std::time::Duration::from_secs(60),
            async {},
        ));
        send.send(HubControl::Add("command-a".into()))
            .await
            .unwrap();
        loop {
            match peer.next().await.unwrap().unwrap() {
                Message::Text(_) => break,
                Message::Ping(payload) => peer.send(Message::Pong(payload)).await.unwrap(),
                _ => (),
            }
        }
        send.send(HubControl::Remove("command-a".into()))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                match peer
                    .next()
                    .await
                    .expect("close frame")
                    .expect("clean close handshake")
                {
                    Message::Close(_) => {
                        peer.flush().await.unwrap();
                        break;
                    }
                    Message::Ping(payload) => peer.send(Message::Pong(payload)).await.unwrap(),
                    _ => (),
                }
            }
        })
        .await
        .unwrap();
        task.await.unwrap();
    }
}

#[cfg(test)]
#[path = "server/e2b_http_tests.rs"]
mod e2b_http_tests;
