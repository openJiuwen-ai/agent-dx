//! SSH terminal entrypoint shared by inline instances and managed Environments.
use super::{server::EdgeStream, AccessKind, EdgeFrontend};
use adx_agent_api::managed::ManagedService;
use adx_agent_core::{
    target::{SshRoute, Target},
    Protocol, Scope,
};
use russh::{
    client,
    keys::{self, HashAlg, PrivateKey, PrivateKeyWithHashAlg, PublicKey, PublicKeyOrCertificate},
    server, Channel, ChannelMsg,
};
use serde::Deserialize;
use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{watch, Semaphore},
    task::JoinSet,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid SSH configuration or target: {0}")]
    Invalid(String),
    #[error("SSH access denied")]
    Forbidden,
    #[error("SSH activation or connection timed out; retry using the same Environment")]
    Timeout,
    #[error("SSH protocol failed: {0}")]
    Protocol(#[from] russh::Error),
    #[error("SSH I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("SSH target unavailable: {0}")]
    Target(String),
}
type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyGrant {
    pub public_key: String,
    pub tenant_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SshConfig {
    pub bind: SocketAddr,
    pub host_key: PathBuf,
    pub backend_key: PathBuf,
    pub backend_user: String,
    pub backend_host_keys: Vec<String>,
    #[serde(default)]
    pub inline_authorized_keys: Vec<KeyGrant>,
    #[serde(default)]
    pub agent_authorized_keys: Vec<KeyGrant>,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_seconds: u64,
    #[serde(default = "default_auth_timeout")]
    pub auth_timeout_seconds: u64,
    #[serde(default = "default_connections")]
    pub max_connections: usize,
}
fn default_connect_timeout() -> u64 {
    60
}
fn default_auth_timeout() -> u64 {
    15
}
fn default_connections() -> usize {
    256
}

struct Grants(BTreeMap<String, String>);
impl Grants {
    fn load(values: Vec<KeyGrant>) -> Result<Self> {
        let mut grants = BTreeMap::new();
        for value in values {
            adx_agent_core::identifier(&value.tenant_id, "SSH tenant").map_err(Error::Invalid)?;
            let key = PublicKey::from_openssh(&value.public_key)
                .map_err(|_| Error::Invalid("invalid authorized public key".into()))?;
            if grants
                .insert(
                    key.fingerprint(HashAlg::Sha256).to_string(),
                    value.tenant_id,
                )
                .is_some()
            {
                return Err(Error::Invalid("duplicate authorized public key".into()));
            }
        }
        Ok(Self(grants))
    }
    fn tenant(&self, key: &PublicKey) -> Option<&str> {
        self.0
            .get(&key.fingerprint(HashAlg::Sha256).to_string())
            .map(String::as_str)
    }
}
struct Settings {
    inline: Grants,
    agent: Grants,
    backend_key: Arc<PrivateKey>,
    backend_user: String,
    backend_host_keys: Vec<PublicKey>,
    connect_timeout: Duration,
    auth_timeout: Duration,
    max_connections: usize,
}

/// Optional listener assembled by the shared standalone/embedded Edge service.
pub struct SshListener {
    listener: TcpListener,
    server_config: Arc<server::Config>,
    settings: Arc<Settings>,
    gateway: Arc<EdgeFrontend>,
}
impl SshListener {
    /// Validates credentials and host-key pins before binding. Never disables host verification.
    pub async fn bind(config: SshConfig, gateway: Arc<EdgeFrontend>) -> Result<Self> {
        if config.connect_timeout_seconds == 0
            || config.auth_timeout_seconds == 0
            || config.max_connections == 0
            || config.max_connections > Semaphore::MAX_PERMITS
        {
            return Err(Error::Invalid(
                "SSH timeouts and max_connections must be positive".into(),
            ));
        }
        for seconds in [config.connect_timeout_seconds, config.auth_timeout_seconds] {
            if tokio::time::Instant::now()
                .checked_add(Duration::from_secs(seconds))
                .is_none()
            {
                return Err(Error::Invalid("SSH timeout is too large".into()));
            }
        }
        adx_agent_core::identifier(&config.backend_user, "SSH backend user")
            .map_err(Error::Invalid)?;
        let host_key = keys::load_secret_key(&config.host_key, None)
            .map_err(|_| Error::Invalid("cannot load SSH host private key".into()))?;
        let backend_key = keys::load_secret_key(&config.backend_key, None)
            .map_err(|_| Error::Invalid("cannot load backend SSH private key".into()))?;
        let backend_host_keys = config
            .backend_host_keys
            .iter()
            .map(|key| {
                PublicKey::from_openssh(key)
                    .map_err(|_| Error::Invalid("invalid backend SSH host key".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        let inline = Grants::load(config.inline_authorized_keys)?;
        let agent = Grants::load(config.agent_authorized_keys)?;
        if backend_host_keys.is_empty() || inline.0.is_empty() && agent.0.is_empty() {
            return Err(Error::Invalid(
                "SSH requires authorized clients and backend host-key pins".into(),
            ));
        }
        if !agent.0.is_empty() && gateway.agent_api.is_none() {
            return Err(Error::Invalid(
                "managed SSH requires ADX_AGENT_CONFIG".into(),
            ));
        }
        let server_config = Arc::new(server::Config {
            keys: vec![host_key],
            methods: russh::MethodSet::from(&[russh::MethodKind::PublicKey][..]),
            auth_rejection_time: Duration::from_secs(1),
            auth_rejection_time_initial: Some(Duration::ZERO),
            ..Default::default()
        });
        Ok(Self {
            listener: TcpListener::bind(config.bind).await?,
            server_config,
            gateway,
            settings: Arc::new(Settings {
                inline,
                agent,
                backend_key: Arc::new(backend_key),
                backend_user: config.backend_user,
                backend_host_keys,
                connect_timeout: Duration::from_secs(config.connect_timeout_seconds),
                auth_timeout: Duration::from_secs(config.auth_timeout_seconds),
                max_connections: config.max_connections,
            }),
        })
    }
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }
    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let slots = Arc::new(Semaphore::new(self.settings.max_connections));
        let mut connections = JoinSet::new();
        tracing::info!(address = %self.listener.local_addr()?, "SSH terminal listener serving");
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                Some(result) = connections.join_next(), if !connections.is_empty() => {
                    if let Err(error) = result { tracing::warn!(%error, "SSH connection task failed"); }
                }
                accepted = self.listener.accept() => {
                    let (stream, peer) = accepted?;
                    if !self.gateway.peer_allowed(peer.ip()) || !self.gateway.ready() { continue; }
                    let Ok(permit) = slots.clone().try_acquire_owned() else { continue };
                    let (authenticated, mut auth_rx) = watch::channel(false);
                    let handler = Connection {
                        gateway: self.gateway.clone(), settings: self.settings.clone(), target: None,
                        authenticated, terminal: JoinSet::new(), opened: false, pty_requested: false, shell_requested: false,
                    };
                    let config = self.server_config.clone();
                    let mut stop = shutdown.clone();
                    let deadline = tokio::time::Instant::now() + self.settings.auth_timeout;
                    connections.spawn(async move {
                        let _permit = permit;
                        let session = tokio::select! {
                            _ = stop.changed() => return,
                            result = tokio::time::timeout_at(deadline, server::run_stream(config, stream, handler)) => match result {
                                Ok(Ok(session)) => session,
                                _ => return,
                            },
                        };
                        let handle = session.handle();
                        tokio::pin!(session);
                        let mut accepted = false;
                        loop {
                            tokio::select! {
                                result = &mut session => {
                                    if let Err(error) = result { tracing::debug!(%error, "SSH connection closed"); }
                                    return;
                                }
                                _ = auth_rx.changed(), if !accepted => { accepted = *auth_rx.borrow(); }
                                _ = tokio::time::sleep_until(deadline), if !accepted => break,
                                _ = stop.changed() => break,
                            }
                        }
                        let _ = handle.disconnect(russh::Disconnect::ByApplication, "SSH connection closing".into(), "".into()).await;
                        let _ = session.await;
                    });
                }
            }
        }
        while connections.join_next().await.is_some() {}
        Ok(())
    }
}

#[derive(Clone)]
enum SessionTarget {
    Inline {
        id: String,
        tenant: String,
        port: u16,
        trace: String,
    },
    Managed {
        scope: Scope,
        port: Option<u16>,
        trace: String,
    },
}
impl SessionTarget {
    fn new(route: &SshRoute, tenant: &str) -> Result<Self> {
        let trace = route
            .trace
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        if let Target::Instance(id) = &route.target {
            return Ok(Self::Inline {
                id: id.clone(),
                tenant: tenant.into(),
                port: route.port.unwrap_or(22),
                trace,
            });
        }
        let scope = ManagedService::environment_scope(tenant, &route.target)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        Ok(Self::Managed {
            scope,
            port: route.port,
            trace,
        })
    }
    fn notice(&self) -> Option<String> {
        let Self::Managed { scope, .. } = self else {
            return None;
        };
        let urn = Target::Environment {
            name: scope.template.clone(),
            version: scope.version.clone(),
            id: scope.environment_id.clone(),
        };
        Some(format!(
            "Environment ID: {}\r\nEnvironment URN: {urn}\r\n",
            scope.environment_id
        ))
    }
    async fn connect(
        &self,
        gateway: &EdgeFrontend,
        ctx: &adx_agent_api::request::RequestContext,
    ) -> Result<EdgeStream> {
        let (id, tenant, port, trace, inline) = match self {
            Self::Inline {
                id,
                tenant,
                port,
                trace,
            } => (id.clone(), tenant.as_str(), *port, trace, true),
            Self::Managed { scope, port, trace } => {
                let api = gateway.agent_api.as_ref().ok_or(Error::Forbidden)?;
                let (target, port) = api
                    .managed
                    .resolve(ctx, scope, Protocol::Ssh, *port)
                    .await
                    .map_err(|e| Error::Target(e.to_string()))?;
                (
                    target.environment.sandbox_id,
                    scope.tenant.as_str(),
                    port,
                    trace,
                    false,
                )
            }
        };
        let route = gateway
            .resolve_route(&id, port, AccessKind::Ssh, trace.clone())
            .await
            .map_err(|e| Error::Target(e.to_string()))?;
        // Legacy system tenant "0" can reach inline instances across tenants; managed access remains tenant-scoped.
        if !(inline && tenant == "0") && (route.tenant_id.is_empty() || route.tenant_id != tenant) {
            return Err(Error::Forbidden);
        }
        gateway
            .open_authorized_stream(route)
            .await
            .map_err(Error::Io)
    }
}
struct Connection {
    gateway: Arc<EdgeFrontend>,
    settings: Arc<Settings>,
    target: Option<SessionTarget>,
    authenticated: watch::Sender<bool>,
    terminal: JoinSet<()>,
    opened: bool,
    pty_requested: bool,
    shell_requested: bool,
}
impl Connection {
    fn credential<'a>(&'a self, user: &str, key: &PublicKey) -> Option<(SshRoute, &'a str)> {
        let route = user.parse::<SshRoute>().ok()?;
        let grants = if matches!(route.target, Target::Instance(_)) {
            &self.settings.inline
        } else {
            &self.settings.agent
        };
        Some((route, grants.tenant(key)?))
    }
}
impl server::Handler for Connection {
    type Error = Error;
    async fn auth_publickey_offered(
        &mut self,
        user: &str,
        key: &PublicKey,
    ) -> Result<server::Auth> {
        Ok(if self.credential(user, key).is_some() {
            server::Auth::Accept
        } else {
            server::Auth::reject()
        })
    }
    async fn auth_publickey(&mut self, user: &str, key: &PublicKey) -> Result<server::Auth> {
        self.target = None;
        if let Some((route, tenant)) = self.credential(user, key) {
            self.target = Some(SessionTarget::new(&route, tenant)?);
            Ok(server::Auth::Accept)
        } else {
            Ok(server::Auth::reject())
        }
    }
    async fn auth_succeeded(&mut self, _: &mut server::Session) -> Result<()> {
        let _ = self.authenticated.send(true);
        Ok(())
    }
    // Reply while russh is handling this exact request. Its wants_reply flag is per channel;
    // deferring acknowledgements to the relay task loses replies for pipelined requests.
    async fn pty_request(
        &mut self,
        channel: russh::ChannelId,
        _term: &str,
        _cols: u32,
        _rows: u32,
        _width: u32,
        _height: u32,
        _modes: &[(russh::Pty, u32)],
        session: &mut server::Session,
    ) -> Result<()> {
        if self.pty_requested || self.shell_requested {
            session.channel_failure(channel)?;
        } else {
            self.pty_requested = true;
            session.channel_success(channel)?;
        }
        Ok(())
    }
    async fn shell_request(
        &mut self,
        channel: russh::ChannelId,
        session: &mut server::Session,
    ) -> Result<()> {
        if !self.pty_requested || self.shell_requested {
            session.channel_failure(channel)?;
        } else {
            self.shell_requested = true;
            session.channel_success(channel)?;
        }
        Ok(())
    }
    async fn env_request(
        &mut self,
        channel: russh::ChannelId,
        _name: &str,
        _value: &str,
        session: &mut server::Session,
    ) -> Result<()> {
        session.channel_failure(channel)?;
        Ok(())
    }
    async fn exec_request(
        &mut self,
        channel: russh::ChannelId,
        _data: &[u8],
        session: &mut server::Session,
    ) -> Result<()> {
        session.channel_failure(channel)?;
        Ok(())
    }
    async fn subsystem_request(
        &mut self,
        channel: russh::ChannelId,
        _name: &str,
        session: &mut server::Session,
    ) -> Result<()> {
        session.channel_failure(channel)?;
        Ok(())
    }
    async fn x11_request(
        &mut self,
        channel: russh::ChannelId,
        _single: bool,
        _protocol: &str,
        _cookie: &str,
        _screen: u32,
        session: &mut server::Session,
    ) -> Result<()> {
        session.channel_failure(channel)?;
        Ok(())
    }
    async fn channel_open_session(
        &mut self,
        channel: Channel<server::Msg>,
        reply: server::ChannelOpenHandle,
        session: &mut server::Session,
    ) -> Result<()> {
        let Some(target) = self.target.clone() else {
            return Ok(());
        };
        if self.opened {
            return Ok(());
        }
        self.opened = true;
        reply.accept().await;
        let gateway = self.gateway.clone();
        let settings = self.settings.clone();
        let handle = session.handle();
        self.terminal.spawn(async move {
            let id = channel.id();
            if let Err(error) = terminal(channel, handle.clone(), target, gateway, settings).await {
                tracing::debug!(%error, "SSH terminal closed");
                // Connection diagnostics are channel stderr; only the interactive shell receives the Environment notice.
                let _ = handle
                    .extended_data(id, 1, format!("ADX: {error}\r\n"))
                    .await;
                let _ = handle.exit_status_request(id, 1).await;
                let _ = handle.eof(id).await;
                let _ = handle.close(id).await;
            }
        });
        Ok(())
    }
}

struct BackendVerifier {
    keys: Vec<PublicKey>,
}
impl client::Handler for BackendVerifier {
    type Error = Error;
    async fn check_server_key(&mut self, key: &PublicKeyOrCertificate) -> Result<bool> {
        Ok(
            matches!(key, PublicKeyOrCertificate::PublicKey { key, .. } if self.keys.iter().any(|pin| pin.key_data() == key.key_data())),
        )
    }
}
struct PtyRequest {
    term: String,
    cols: u32,
    rows: u32,
    width: u32,
    height: u32,
    modes: Vec<(russh::Pty, u32)>,
}

async fn terminal(
    mut front: Channel<server::Msg>,
    handle: server::Handle,
    target: SessionTarget,
    gateway: Arc<EdgeFrontend>,
    settings: Arc<Settings>,
) -> Result<()> {
    let mut pty = None;
    // No lifecycle side effects until a client actually requests an interactive shell.
    loop {
        match front.wait().await {
            Some(ChannelMsg::RequestPty {
                term,
                col_width,
                row_height,
                pix_width,
                pix_height,
                terminal_modes,
                ..
            }) if pty.is_none() => {
                pty = Some(PtyRequest {
                    term,
                    cols: col_width,
                    rows: row_height,
                    width: pix_width,
                    height: pix_height,
                    modes: terminal_modes,
                });
            }
            Some(ChannelMsg::RequestShell { .. }) if pty.is_some() => break,
            Some(ChannelMsg::Eof | ChannelMsg::Close) | None => return Ok(()),
            Some(ChannelMsg::WindowAdjusted { .. }) => {}
            _ => {}
        }
    }
    let pty = pty.ok_or_else(|| Error::Invalid("interactive SSH requires a terminal".into()))?;
    let ctx = adx_agent_api::request::RequestContext::new(settings.connect_timeout);
    let deadline = ctx.deadline();
    // Validate the managed service before announcing an Environment or submitting a create.
    if let SessionTarget::Managed { scope, port, .. } = &target {
        let api = gateway.agent_api.as_ref().ok_or(Error::Forbidden)?;
        let template = tokio::time::timeout_at(
            deadline,
            api.managed
                .template(&ctx, &scope.tenant, &scope.template, &scope.version),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|e| Error::Target(e.to_string()))?;
        ManagedService::select_service(&template, Protocol::Ssh, *port)
            .map_err(|e| Error::Invalid(e.to_string()))?;
    }
    let connected = async {
        if let Some(notice) = target.notice() {
            front.data(notice.as_bytes()).await?;
        }
        let stream = target.connect(&gateway, &ctx).await?;
        let mut backend = client::connect_stream(
            Arc::new(client::Config::default()),
            stream,
            BackendVerifier {
                keys: settings.backend_host_keys.clone(),
            },
        )
        .await?;
        let hash = backend.best_supported_rsa_hash().await?.flatten();
        if !backend
            .authenticate_publickey(
                &settings.backend_user,
                PrivateKeyWithHashAlg::new(settings.backend_key.clone(), hash),
            )
            .await?
            .success()
        {
            return Err(Error::Forbidden);
        }
        let mut channel = backend.channel_open_session().await?;
        channel
            .request_pty(
                true, &pty.term, pty.cols, pty.rows, pty.width, pty.height, &pty.modes,
            )
            .await?;
        require_success(&mut channel).await?;
        channel.request_shell(true).await?;
        // Shell output may arrive alongside its acknowledgement; relay handles both in wire order.
        Ok::<_, Error>((backend, channel))
    };
    let (backend, back) = tokio::time::timeout_at(deadline, connected)
        .await
        .map_err(|_| Error::Timeout)??;
    let result = relay(front, back, handle).await;
    let _ = backend
        .disconnect(russh::Disconnect::ByApplication, "terminal closed", "")
        .await;
    result
}
async fn require_success(channel: &mut Channel<client::Msg>) -> Result<()> {
    loop {
        match channel.wait().await {
            Some(ChannelMsg::Success) => return Ok(()),
            Some(ChannelMsg::WindowAdjusted { .. }) => {}
            _ => return Err(Error::Target("backend rejected terminal allocation".into())),
        }
    }
}
async fn relay(
    front: Channel<server::Msg>,
    back: Channel<client::Msg>,
    handle: server::Handle,
) -> Result<()> {
    let id = front.id();
    let (mut front_read, front_write) = front.split();
    let (mut back_read, back_write) = back.split();
    // Separate directions keep channel-window backpressure from blocking window updates in the other direction.
    let upload = async {
        while let Some(message) = front_read.wait().await {
            match message {
                ChannelMsg::Data { data } => back_write.data(&data[..]).await?,
                ChannelMsg::WindowChange {
                    col_width,
                    row_height,
                    pix_width,
                    pix_height,
                } => {
                    back_write
                        .window_change(col_width, row_height, pix_width, pix_height)
                        .await?
                }
                ChannelMsg::Signal { signal } => back_write.signal(signal).await?,
                ChannelMsg::Eof => {
                    back_write.eof().await?;
                }
                ChannelMsg::Close => {
                    back_write.close().await?;
                    break;
                }
                ChannelMsg::WindowAdjusted { .. } => {}
                _ => {}
            }
        }
        Ok::<_, Error>(())
    };
    let download = async {
        while let Some(message) = back_read.wait().await {
            match message {
                ChannelMsg::Data { data } => front_write.data(&data[..]).await?,
                ChannelMsg::ExtendedData { data, ext } => {
                    front_write.extended_data(ext, &data[..]).await?
                }
                ChannelMsg::ExitStatus { exit_status } => {
                    front_write.exit_status(exit_status).await?
                }
                ChannelMsg::ExitSignal {
                    signal_name,
                    core_dumped,
                    error_message,
                    lang_tag,
                } => {
                    let _ = handle
                        .exit_signal_request(id, signal_name, core_dumped, error_message, lang_tag)
                        .await;
                }
                ChannelMsg::Eof => {
                    front_write.eof().await?;
                }
                ChannelMsg::Close => break,
                ChannelMsg::Failure => return Err(Error::Target("backend rejected shell".into())),
                _ => {}
            }
        }
        front_write.close().await?;
        Ok::<_, Error>(())
    };
    tokio::select! { result = upload => result, result = download => result }
}

#[cfg(test)]
mod tests;
