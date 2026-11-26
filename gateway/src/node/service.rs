//! One service lifecycle for standalone and embedded Relay deployment.
use super::{bind_route_control, serve_health, ActivityTracker, Relay};
use crate::common::{listener::accept_with_backoff, protocol::GatewayPolicy};
use crate::config::{IngressNodeSecurityMode, RelayConfig};
use std::{future::Future, path::PathBuf, sync::Arc};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::watch,
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;
type ServiceError = Box<dyn std::error::Error + Send + Sync>;

pub struct RelayService {
    config: RelayConfig,
    listener: TcpListener,
    health: TcpListener,
    route: UnixListener,
    gateway: Arc<Relay>,
    tracker: Arc<ActivityTracker>,
    tls: Option<Arc<TlsAcceptor>>,
}
impl RelayService {
    /// Bind every listener before the owner advertises readiness. Bindings start
    /// unavailable and only the Adxlet's complete sync opens admission.
    pub async fn bind(config: RelayConfig) -> Result<Self, ServiceError> {
        Self::bind_standalone(config, None).await
    }
    /// Bind an optional generation-specific control socket without changing the
    /// activity publisher endpoint. Explicit paths must be new absolute paths;
    /// their cleanup belongs to the caller after this generation has exited.
    pub async fn bind_standalone(
        config: RelayConfig,
        route_socket: Option<&std::path::Path>,
    ) -> Result<Self, ServiceError> {
        if let Some(path) = route_socket {
            if !path.is_absolute() {
                return Err("Relay route socket must be an absolute nonempty path".into());
            }
        }
        adx_transport::install_crypto_provider();
        let dir = config
            .activity_uds_dir
            .as_ref()
            .filter(|p| !p.is_empty())
            .ok_or("Relay control socket directory required")?;
        let tls = if config.ingress_security_mode == IngressNodeSecurityMode::Mtls {
            if config.mtls_client_ca.is_empty() {
                return Err("Relay mTLS client CA required".into());
            }
            Some(Arc::new(load_tls_acceptor(
                &config.tls_cert,
                &config.tls_key,
                &config.mtls_client_ca,
            )?))
        } else {
            None
        };
        let listener = TcpListener::bind(config.bind).await?;
        let health = TcpListener::bind(config.health_bind).await?;
        let route = match route_socket {
            Some(path) => super::route_control::bind_generation_route_control(path).await?,
            None => {
                bind_route_control(
                    std::path::Path::new(dir)
                        .join("route.sock")
                        .to_str()
                        .ok_or("invalid route path")?,
                )
                .await?
            }
        };
        let tracker = Arc::new(ActivityTracker::new(config.gateway_epoch.clone()));
        let gateway = Arc::new(
            Relay::new(GatewayPolicy::new(config.allowed_target_networks.clone()))
                .with_max_active_streams(config.max_streams)
                .with_activity_tracker(tracker.clone())
                .with_route_enforcement(),
        );
        gateway.set_bindings_ready(false);
        Ok(Self {
            config,
            listener,
            health,
            route,
            gateway,
            tracker,
            tls,
        })
    }
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
    pub fn health_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.health.local_addr()
    }
    pub async fn serve(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), ServiceError> {
        self.serve_inner(shutdown, false).await
    }
    /// Standalone process lifecycle; embedded owners deliberately use `serve`.
    pub async fn serve_standalone(
        self,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<(), ServiceError> {
        self.serve_inner(shutdown, true).await
    }
    async fn serve_inner(
        self,
        shutdown: impl Future<Output = ()> + Send,
        standalone: bool,
    ) -> Result<(), ServiceError> {
        let Self {
            config,
            listener,
            health,
            route,
            gateway,
            tracker,
            tls,
        } = self;
        let (stop, stopped) = watch::channel(false);
        let mut services = JoinSet::<Result<(), ServiceError>>::new();
        let bindings = Arc::new(super::route_control::BindingService::new(gateway.clone()));
        let upgrade = standalone.then(|| super::upgrade::UpgradeService::new(bindings.clone()));
        let mut accepted = upgrade.as_ref().map(|service| service.subscribe());
        let mut route_stop = stopped.clone();
        let route_upgrade = upgrade.clone();
        services.spawn(async move {
            let shutdown = async move {
                let _ = route_stop.changed().await;
            };
            if let Some(route_upgrade) = route_upgrade {
                super::route_control::serve_standalone_control_until(
                    bindings,
                    route_upgrade,
                    route,
                    shutdown,
                )
                .await?;
            } else {
                super::route_control::serve_binding_control_until(bindings, route, shutdown)
                    .await?;
            }
            Ok(())
        });
        let health_gateway = gateway.clone();
        let (health_stop, health_stopped) = watch::channel(false);
        let mut health_task =
            tokio::spawn(async move { serve_health(health_gateway, health, health_stopped).await });
        let mut health_joined = false;
        let path = std::path::Path::new(config.activity_uds_dir.as_ref().unwrap())
            .join("adxlet.sock")
            .to_string_lossy()
            .into_owned();
        let mut activity_stop = stop.subscribe();
        services.spawn(async move {
            tokio::select! {_=super::run_activity_publisher(tracker,path,config.activity_interval)=>{},_=activity_stop.changed()=>{}}
            Ok(())
        });
        let mut connections = JoinSet::new();
        tokio::pin!(shutdown);
        tracing::info!(bind=%listener.local_addr()?,health=%config.health_bind,"Relay serving");
        let mut upgrading = false;
        let mut shutdown_completed = false;
        let mut failure = loop {
            tokio::select! {
                _=&mut shutdown=>{shutdown_completed=true;break None;},
                result=&mut health_task=>{
                    health_joined = true;
                    break Some(format!("Relay health service stopped: {result:?}"));
                },
                _=async {
                    match accepted.as_mut() {
                        Some(accepted) => { let _ = accepted.changed().await; }
                        None => std::future::pending::<()>().await,
                    }
                }=>{
                    upgrading = true;
                    break None;
                },
                ended=services.join_next()=>break Some(format!("Relay background service stopped: {ended:?}")),
                // Reap finished connection tasks while the service is running.
                _=connections.join_next(),if !connections.is_empty()=>{},
                (stream,peer)=accept_with_backoff(&listener,"node-h2")=>{
                    if !config.peer_allowed(peer.ip()) {
                        tracing::warn!(target: "adx_audit", event="ingress_acl", decision="deny", %peer,
                            reason="source_outside_allowed_cidrs", "Relay peer denied");
                        continue
                    }
                    let gateway=gateway.clone();let tls=tls.clone();
                    connections.spawn(async move {
                        let result=match tls {
                            Some(acceptor)=>match acceptor.accept(stream).await {
                                Ok(stream)=>gateway.serve_h2(stream).await,
                                Err(error)=>{tracing::debug!(%peer,%error,"Relay TLS handshake rejected");return;}
                            },
                            None=>gateway.serve_h2(stream).await,
                        };
                        if let Err(error)=result{tracing::debug!(%peer,%error,"Relay H2 connection closed");}
                    });
                }
            }
        };
        // Serialize shutdown with BeginDrain: no accepted task may be left unconsumed.
        if let Some(upgrade) = &upgrade {
            let has_task = upgrade.close_admission().await;
            if has_task && !upgrading {
                failure
                    .get_or_insert_with(|| "Relay upgrade interrupted by service shutdown".into());
            }
            upgrading = has_task;
        }
        gateway.start_drain();
        // This owner drops the TCP listener; an RPC acknowledgement is not release evidence.
        drop(listener);
        let mut deadline = tokio::time::Instant::now() + config.drain_timeout;
        if upgrading && !health_joined {
            let _ = health_stop.send(true);
            match tokio::time::timeout(std::time::Duration::from_secs(1), &mut health_task).await {
                Ok(Ok(Ok(()))) => {
                    health_joined = true;
                    deadline = tokio::time::Instant::now() + super::upgrade::DRAIN_LIMIT;
                    let unix_deadline = std::time::SystemTime::now()
                        .checked_add(super::upgrade::DRAIN_LIMIT)
                        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                        .and_then(|duration| u64::try_from(duration.as_millis()).ok());
                    if let Some(unix_deadline) = unix_deadline {
                        if let Some(upgrade) = &upgrade {
                            upgrade.released(unix_deadline).await;
                        }
                    } else {
                        failure = Some("Relay drain deadline is not representable".into());
                    }
                }
                Ok(result) => {
                    health_joined = true;
                    failure = Some(format!("Relay health release failed: {result:?}"));
                }
                Err(error) => {
                    failure = Some(format!("Relay health release timed out: {error}"));
                }
            }
        }
        let mut interrupted = upgrading && shutdown_completed;
        while !interrupted && gateway.active_streams() > 0 && tokio::time::Instant::now() < deadline
        {
            tokio::select! {
                _=tokio::time::sleep(std::time::Duration::from_millis(50))=>{},
                _=&mut shutdown, if upgrading && !shutdown_completed=>{interrupted=true;break;},
                ended=services.join_next()=>{
                    failure=Some(format!("Relay background service stopped during drain: {ended:?}"));
                    break;
                },
            }
        }
        let forced = gateway.active_streams() > 0;
        connections.shutdown().await;
        if upgrading {
            let phase = if failure.is_some() {
                "failed"
            } else if interrupted {
                "interrupted"
            } else if forced {
                "forced"
            } else {
                "completed"
            };
            if let Some(upgrade) = &upgrade {
                upgrade.finish(phase).await;
            }
            tracing::info!(phase, "Relay upgrade stream drain ended");
            if failure.is_none() && (interrupted || forced) {
                failure = Some(format!(
                    "Relay upgrade drain {phase}; not natural completion"
                ));
            }
        }
        let _ = health_stop.send(true);
        let _ = stop.send(true);
        let joined = async {
            if !health_joined {
                (&mut health_task).await??;
            }
            while let Some(result) = services.join_next().await {
                result??;
            }
            Ok::<(), ServiceError>(())
        };
        // A stuck local RPC must not keep its owning process alive indefinitely.
        let cleanup = tokio::time::timeout(
            if upgrading {
                std::time::Duration::from_secs(1)
            } else {
                config.drain_timeout.max(std::time::Duration::from_secs(1))
            },
            joined,
        )
        .await;
        // Dropping/aborting the owner also closes persistent control streams.
        health_task.abort();
        services.abort_all();
        cleanup??;
        if let Some(error) = failure {
            return Err(error.into());
        }
        Ok(())
    }
}

fn load_tls_acceptor(
    cert_path: &str,
    key_path: &str,
    mtls_client_ca: &str,
) -> Result<TlsAcceptor, Box<dyn std::error::Error + Send + Sync>> {
    adx_transport::tls::http_server_acceptor(
        cert_path,
        key_path,
        (!mtls_client_ca.is_empty()).then(|| PathBuf::from(mtls_client_ca)),
        vec![b"h2".to_vec()],
    )
}
