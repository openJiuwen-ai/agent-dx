//! Agent transport composition. Management JSON is separate from unchanged data forwarding.
use adx_agent_api::{
    dispatcher::DispatcherClient,
    managed::ManagedService,
    management::{InlineProfile, InlineService, Options},
    Error,
};
use adx_agent_core::{inline::CreateRequest, sandbox::Sandbox, Protocol, Scope, TemplateVersion};
use adx_agent_core::{limits, transport::RequestProgress};
use adx_agent_store::{AgentState, RedisRepository};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    InlineOnly,
    Both,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatcherConfig {
    pub token_env: String,
    pub timeout_seconds: u64,
    pub ca_path: Option<String>,
    #[serde(default)]
    pub allow_plaintext: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub mode: Mode,
    pub redis_url: Option<String>,
    pub namespace: Option<String>,
    pub inline_profiles: Vec<InlineProfile>,
    pub backend_timeout_seconds: u64,
    pub max_inflight: usize,
    pub dispatcher: Option<DispatcherConfig>,
}
pub struct AgentApi {
    pub inline: Arc<InlineService>,
    pub managed: Option<Arc<ManagedService>>,
    pub dispatcher: Option<Arc<DispatcherClient>>,
}
impl AgentApi {
    pub async fn new(
        config: AgentConfig,
        sandbox: Arc<dyn Sandbox>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let inline = Arc::new(InlineService::new(
            sandbox,
            Options {
                profiles: config.inline_profiles,
                backend_timeout: Duration::from_secs(config.backend_timeout_seconds),
                max_inflight: config.max_inflight,
            },
        )?);
        let (managed, dispatcher) = match (config.mode, config.dispatcher) {
            (Mode::InlineOnly, None) => (None, None),
            (Mode::InlineOnly, Some(_)) => {
                return Err("inline_only cannot configure Dispatcher".into())
            }
            (Mode::Both, None) => return Err("both mode requires Dispatcher settings".into()),
            (Mode::Both, Some(settings)) => {
                if Duration::from_secs(settings.timeout_seconds)
                    <= limits::CREATE_TIMEOUT + limits::DISPATCHER_FINISH_TIMEOUT
                {
                    return Err("Dispatcher timeout_seconds must exceed the default creation/Resolve deadline plus 10 seconds (70 seconds); increase it further when configuring a longer creation timeout".into());
                }
                let token = std::env::var(&settings.token_env)
                    .map_err(|_| "Dispatcher token environment variable missing")?;
                let ca = settings.ca_path.map(std::fs::read).transpose()?;
                let repository = Arc::new(
                    RedisRepository::connect(
                        config
                            .redis_url
                            .as_deref()
                            .ok_or("both mode requires redis_url")?,
                        config
                            .namespace
                            .as_deref()
                            .ok_or("both mode requires namespace")?,
                        Duration::from_secs(3),
                    )
                    .await?,
                );
                let dispatcher = Arc::new(DispatcherClient::new(
                    repository.clone(),
                    token,
                    Duration::from_secs(settings.timeout_seconds),
                    ca.as_deref(),
                    settings.allow_plaintext,
                )?);
                let managed = Arc::new(ManagedService::new(
                    AgentState::new(repository),
                    dispatcher.clone(),
                ));
                (Some(managed), Some(dispatcher))
            }
        };
        Ok(Self {
            inline,
            managed,
            dispatcher,
        })
    }
    pub fn matches(path: &str) -> bool {
        path == "/api/agent" || path.starts_with("/api/agent/")
    }
    pub async fn management(
        &self,
        request: Request<hyper::body::Incoming>,
        tenant: &str,
    ) -> Response<super::server::ProxyBody> {
        let trace = match request.headers().get("x-trace-id") {
            Some(value) => match value.to_str() {
                Ok(v)
                    if !v.trim().is_empty()
                        && v.len() <= limits::IDENTIFIER_BYTES
                        && !v.chars().any(char::is_control) =>
                {
                    v.to_owned()
                }
                _ => return error_response(Error::Invalid("invalid X-Trace-Id".into()), None),
            },
            None => uuid::Uuid::new_v4().to_string(),
        };
        let progress = RequestProgress::default();
        let budget = if request.uri().path().starts_with("/api/agent/v2/") {
            self.dispatcher
                .as_ref()
                .map(|client| client.request_budget())
                .unwrap_or(limits::CREATE_TIMEOUT + limits::DISPATCHER_FINISH_TIMEOUT)
        } else {
            limits::AGENT_REQUEST_TIMEOUT
        };
        let result =
            tokio::time::timeout(budget, self.management_inner(request, tenant, &progress))
                .await
                .unwrap_or_else(|_| {
                    Err(if progress.may_have_written() {
                        Error::OutcomeUnknown(
                            "Agent operation timed out; inspect the original identity".into(),
                        )
                    } else {
                        Error::Unavailable("Agent read or request admission timed out".into())
                    })
                });
        match result {
            Ok(value) => json_response(StatusCode::OK, &value, Some(&trace)),
            Err(error) => error_response(error, Some(&trace)),
        }
    }
    async fn management_inner(
        &self,
        request: Request<hyper::body::Incoming>,
        tenant: &str,
        progress: &RequestProgress,
    ) -> Result<serde_json::Value, Error> {
        if request.uri().path().starts_with("/api/agent/v2/") {
            return self.managed_management(request, tenant, progress).await;
        }
        let (parts, body) = request.into_parts();
        let suffix = parts
            .uri
            .path()
            .strip_prefix("/api/agent")
            .ok_or(Error::NotFound)?;
        let id = if suffix.is_empty() {
            None
        } else {
            Some(
                suffix
                    .strip_prefix('/')
                    .filter(|s| uuid::Uuid::parse_str(s).is_ok())
                    .ok_or(Error::NotFound)?,
            )
        };
        if parts.uri.query().is_some() {
            return Err(Error::Invalid(
                "inline management does not accept query parameters".into(),
            ));
        }
        match (parts.method, id) {
            (http::Method::POST, None) => {
                let raw = http_body_util::Limited::new(body, limits::HTTP_JSON_BYTES)
                    .collect()
                    .await
                    .map_err(|_| {
                        Error::Invalid("invalid or oversized Agent create request".into())
                    })?
                    .to_bytes();
                let request: CreateRequest = serde_json::from_slice(&raw).map_err(|_| {
                    Error::Invalid("invalid report-backed inline create JSON".into())
                })?;
                progress.start_write();
                Ok(
                    serde_json::to_value(self.inline.create(tenant, request).await?)
                        .expect("created response"),
                )
            }
            (http::Method::GET, Some(id)) => {
                Ok(serde_json::json!({"code":200,"instance":self.inline.get(tenant,id).await?}))
            }
            (http::Method::DELETE, Some(id)) => {
                progress.start_write();
                self.inline.kill(tenant, id).await?;
                Ok(serde_json::json!({"code":200,"status":"deleted"}))
            }
            _ => Err(Error::NotFound),
        }
    }
}
pub(super) fn error_response(
    error: Error,
    trace: Option<&str>,
) -> Response<super::server::ProxyBody> {
    let status = match &error {
        Error::Invalid(_) => StatusCode::BAD_REQUEST,
        Error::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
        Error::NotFound => StatusCode::NOT_FOUND,
        Error::Conflict(_) => StatusCode::CONFLICT,
        Error::NotReady(_) | Error::Unavailable(_) | Error::OutcomeUnknown(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
    };
    json_response(
        status,
        &serde_json::json!({"code":status.as_u16(),"message":error.to_string(),"error":error}),
        trace,
    )
}
fn json_response(
    status: StatusCode,
    value: &serde_json::Value,
    trace: Option<&str>,
) -> Response<super::server::ProxyBody> {
    let mut response = Response::builder()
        .status(status)
        .header("content-type", "application/json");
    if let Some(trace) = trace {
        response = response.header("x-trace-id", trace);
    }
    response
        .body(
            Full::new(Bytes::from(
                serde_json::to_vec(value).expect("Agent response"),
            ))
            .map_err(
                |never: std::convert::Infallible| -> Box<dyn std::error::Error + Send + Sync> {
                    match never {}
                },
            )
            .boxed_unsync(),
        )
        .expect("validated Agent response")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionInput {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveInput {
    protocol: Protocol,
    port: Option<u16>,
    affinity_key: Option<String>,
    #[serde(default)]
    bypass_cache: bool,
}
fn segment(value: &str) -> Result<String, Error> {
    let value = percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| Error::Invalid("invalid UTF-8 path identifier".into()))?
        .into_owned();
    if value.is_empty()
        || value.len() > limits::IDENTIFIER_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(Error::Invalid("invalid path identifier".into()));
    }
    Ok(value)
}
impl AgentApi {
    fn managed(&self) -> Result<&ManagedService, Error> {
        self.managed
            .as_deref()
            .ok_or_else(|| Error::Unavailable("managed Agent mode is disabled".into()))
    }
    async fn managed_management(
        &self,
        request: Request<hyper::body::Incoming>,
        tenant: &str,
        progress: &RequestProgress,
    ) -> Result<serde_json::Value, Error> {
        let (parts, body) = request.into_parts();
        if parts.uri.query().is_some() {
            return Err(Error::Invalid(
                "managed APIs do not accept query parameters".into(),
            ));
        }
        let path: Vec<_> = parts
            .uri
            .path()
            .strip_prefix("/api/agent/v2/")
            .ok_or(Error::NotFound)?
            .split('/')
            .collect();
        let managed = self.managed()?;
        if path == ["templates"] && parts.method == http::Method::POST {
            let input: TemplateVersion = decode_body(body).await?;
            progress.start_write();
            managed.publish(tenant, &input).await?;
            return Ok(serde_json::json!({"template":input}));
        }
        let ["templates", name, "versions", version, rest @ ..] = path.as_slice() else {
            return Err(Error::NotFound);
        };
        let name = segment(name)?;
        let version = segment(version)?;
        if rest.is_empty() && parts.method == http::Method::GET {
            return Ok(
                serde_json::json!({"template":managed.template(tenant,&name,&version).await?}),
            );
        }
        let ["sessions", session, action @ ..] = rest else {
            return Err(Error::NotFound);
        };
        let scope = Scope {
            tenant: tenant.into(),
            template: name,
            version,
            session_id: segment(session)?,
        };
        scope.validate().map_err(Error::Invalid)?;
        match (parts.method, action) {
            (http::Method::PUT, []) => {
                let bytes = read_body(body).await?;
                if !bytes.is_empty() {
                    serde_json::from_slice::<SessionInput>(&bytes).map_err(|_| {
                        Error::Invalid("Session creation accepts no options".into())
                    })?;
                }
                progress.start_write();
                managed.create_session(scope.clone()).await?;
                Ok(serde_json::json!({"session":managed.session(&scope).await?}))
            }
            (http::Method::GET, []) => {
                Ok(serde_json::json!({"session":managed.session(&scope).await?}))
            }
            (http::Method::DELETE, []) => {
                progress.start_write();
                managed.release(&scope).await?;
                match managed.session(&scope).await {
                    Ok(session) => Ok(serde_json::json!({"status":"deleting","session":session})),
                    Err(Error::NotFound) => Ok(serde_json::json!({"status":"deleted"})),
                    Err(error) => Err(error),
                }
            }
            (http::Method::GET, ["instances"]) => {
                Ok(serde_json::json!({"instances":managed.instances(&scope).await?}))
            }
            (http::Method::GET, ["instances", id]) => {
                Ok(serde_json::json!({"instance":managed.instance(&scope,&segment(id)?).await?}))
            }
            (http::Method::DELETE, ["instances", id]) => {
                let id = segment(id)?;
                progress.start_write();
                managed.release_capsule(&scope, &id).await?;
                Ok(serde_json::json!({"status":"release_requested"}))
            }
            (http::Method::POST, ["resolve"]) => {
                let input: ResolveInput = decode_body(body).await?;
                progress.start_write(); // Resolve may cold-start or bind affinity.
                let (target, port) = managed
                    .resolve_with_cache(
                        &scope,
                        input.affinity_key,
                        input.protocol,
                        input.port,
                        input.bypass_cache,
                    )
                    .await?;
                Ok(
                    serde_json::json!({"instance_id":target.instance_id,"sandbox_id":target.sandbox_id,"port":port,"protocol":input.protocol}),
                )
            }
            _ => Err(Error::NotFound),
        }
    }
    pub fn matches_data(path: &str) -> bool {
        path.starts_with("/agent/v2/")
    }
    /// Rewrite a managed HTTP/WS request into the existing fixed Sandbox forwarding path.
    /// SSH clients use the resolve endpoint then the existing Sandbox-ID CONNECT/tunnel interface.
    pub fn can_retry_data<B>(&self, request: &Request<B>) -> bool {
        request.extensions().get::<SelectionRetry>().is_some()
    }
    /// Consume retry metadata: each incoming business request can bypass selection once at most.
    pub async fn retry_data<B>(&self, request: &mut Request<B>) -> Result<bool, Error> {
        let Some(retry) = request.extensions_mut().remove::<SelectionRetry>() else {
            return Ok(false);
        };
        let (target, port) = self
            .managed()?
            .resolve_with_cache(
                &retry.scope,
                retry.affinity,
                retry.protocol,
                Some(retry.port),
                true,
            )
            .await?;
        let uri = format!("/{}/{port}/{}", target.sandbox_id, retry.tail_and_query);
        *request.uri_mut() = uri
            .parse()
            .map_err(|_| Error::Unavailable("invalid resolved forwarding path".into()))?;
        Ok(true)
    }
    pub async fn prepare_data<B>(
        &self,
        request: &mut Request<B>,
        tenant: &str,
    ) -> Result<(), Error> {
        let mut parts = request
            .uri()
            .path()
            .strip_prefix("/agent/v2/")
            .ok_or(Error::NotFound)?
            .splitn(6, '/');
        let name = segment(parts.next().ok_or(Error::NotFound)?)?;
        let version = segment(parts.next().ok_or(Error::NotFound)?)?;
        let session_id = segment(parts.next().ok_or(Error::NotFound)?)?;
        let protocol = match parts.next() {
            Some("http") => Protocol::Http,
            Some("ws") => Protocol::Ws,
            _ => return Err(Error::NotFound),
        };
        let port = parts
            .next()
            .and_then(|p| p.parse::<u16>().ok())
            .filter(|p| *p > 0)
            .ok_or_else(|| Error::Invalid("invalid service port".into()))?;
        let tail = parts.next().unwrap_or("").to_owned();
        let websocket = request
            .headers()
            .get("upgrade")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
        if websocket != (protocol == Protocol::Ws) {
            return Err(Error::Invalid(
                "HTTP/WS route does not match the upgrade request".into(),
            ));
        }
        let affinity = request
            .headers()
            .get("x-adx-affinity-key")
            .map(|v| {
                v.to_str()
                    .map(str::to_owned)
                    .map_err(|_| Error::Invalid("invalid affinity header".into()))
            })
            .transpose()?;
        let scope = Scope {
            tenant: tenant.into(),
            template: name,
            version,
            session_id,
        };
        let (target, port) = self
            .managed()?
            .resolve(&scope, affinity.clone(), protocol, Some(port))
            .await?;
        let path = format!("/{}/{port}/{tail}", target.sandbox_id);
        let uri = if let Some(query) = request.uri().query() {
            format!("{path}?{query}")
        } else {
            path
        };
        *request.uri_mut() = uri
            .parse()
            .map_err(|_| Error::Unavailable("invalid resolved forwarding path".into()))?;
        let tail_and_query = if let Some(query) = request.uri().query() {
            format!("{tail}?{query}")
        } else {
            tail
        };
        request.extensions_mut().insert(SelectionRetry {
            scope,
            affinity,
            protocol,
            port,
            tail_and_query,
        });
        request.headers_mut().remove("x-adx-affinity-key");
        Ok(())
    }
}
async fn decode_body<T: serde::de::DeserializeOwned>(
    body: hyper::body::Incoming,
) -> Result<T, Error> {
    let bytes = read_body(body).await?;
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::Invalid("invalid managed request JSON".into()))
}
async fn read_body(body: hyper::body::Incoming) -> Result<Bytes, Error> {
    let bytes = http_body_util::Limited::new(body, limits::HTTP_JSON_BYTES)
        .collect()
        .await
        .map_err(|_| Error::Invalid("invalid or oversized managed request".into()))?
        .to_bytes();
    Ok(bytes)
}

#[derive(Clone)]
struct SelectionRetry {
    scope: Scope,
    affinity: Option<String>,
    protocol: Protocol,
    port: u16,
    tail_and_query: String,
}
