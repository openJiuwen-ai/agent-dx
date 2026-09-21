//! One service lifecycle for standalone and embedded Node Proxy deployment.
use super::{bind_route_control, serve_health, ActivityTracker, NodeProxy};
use crate::common::{listener::accept_with_backoff, protocol::GatewayPolicy};
use crate::config::{EdgeNodeSecurityMode, NodeProxyConfig};
use std::{future::Future, path::PathBuf, sync::Arc};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::watch,
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;
type ServiceError = Box<dyn std::error::Error + Send + Sync>;

pub struct NodeProxyService {
    config: NodeProxyConfig,
    listener: TcpListener,
    health: TcpListener,
    route: UnixListener,
    gateway: Arc<NodeProxy>,
    tracker: Arc<ActivityTracker>,
    tls: Option<Arc<TlsAcceptor>>,
}
impl NodeProxyService {
    /// Bind every listener before the owner advertises readiness. Bindings start
    /// unavailable and only the Node Manager's complete sync opens admission.
    pub async fn bind(config: NodeProxyConfig) -> Result<Self, ServiceError> {
        adx_transport::install_crypto_provider();
        let dir = config
            .activity_uds_dir
            .as_ref()
            .filter(|p| !p.is_empty())
            .ok_or("Node Proxy control socket directory required")?;
        let tls = if config.edge_security_mode == EdgeNodeSecurityMode::Mtls {
            if config.mtls_client_ca.is_empty() {
                return Err("Node Proxy mTLS client CA required".into());
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
        let route = bind_route_control(
            std::path::Path::new(dir)
                .join("route.sock")
                .to_str()
                .ok_or("invalid route path")?,
        )
        .await?;
        let tracker = Arc::new(ActivityTracker::new(config.gateway_epoch.clone()));
        let gateway = Arc::new(
            NodeProxy::new(GatewayPolicy::new(config.allowed_target_networks.clone()))
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
    pub async fn serve(
        self,
        shutdown: impl Future<Output = ()> + Send,
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
        let route_gateway = gateway.clone();
        let mut route_stop = stopped.clone();
        services.spawn(async move {
            super::route_control::serve_route_control_until(route_gateway, route, async move {
                let _ = route_stop.changed().await;
            })
            .await?;
            Ok(())
        });
        let health_gateway = gateway.clone();
        services.spawn(async move {
            serve_health(health_gateway, health, stopped).await?;
            Ok(())
        });
        let path = std::path::Path::new(config.activity_uds_dir.as_ref().unwrap())
            .join("node-manager.sock")
            .to_string_lossy()
            .into_owned();
        let mut activity_stop = stop.subscribe();
        services.spawn(async move {
            tokio::select! {_=super::run_activity_publisher(tracker,path,config.activity_interval)=>{},_=activity_stop.changed()=>{}}
            Ok(())
        });
        let mut connections = JoinSet::new();
        tokio::pin!(shutdown);
        tracing::info!(bind=%listener.local_addr()?,health=%config.health_bind,"Node Proxy serving");
        let failure = loop {
            tokio::select! {
                _=&mut shutdown=>break None,
                ended=services.join_next()=>break Some(format!("Node Proxy background service stopped: {ended:?}")),
                // Reap finished connection tasks while the service is running.
                _=connections.join_next(),if !connections.is_empty()=>{},
                (stream,peer)=accept_with_backoff(&listener,"node-h2")=>{
                    if !config.peer_allowed(peer.ip()) {
                        tracing::warn!(target: "adx_audit", event="edge_acl", decision="deny", %peer,
                            reason="source_outside_allowed_cidrs", "Node Proxy peer denied");
                        continue
                    }
                    let gateway=gateway.clone();let tls=tls.clone();
                    connections.spawn(async move {
                        let result=match tls {
                            Some(acceptor)=>match acceptor.accept(stream).await {
                                Ok(stream)=>gateway.serve_h2(stream).await,
                                Err(error)=>{tracing::debug!(%peer,%error,"Node Proxy TLS handshake rejected");return;}
                            },
                            None=>gateway.serve_h2(stream).await,
                        };
                        if let Err(error)=result{tracing::debug!(%peer,%error,"Node Proxy H2 connection closed");}
                    });
                }
            }
        };
        gateway.start_drain();
        let deadline = tokio::time::Instant::now() + config.drain_timeout;
        while gateway.active_streams() > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        connections.shutdown().await;
        let _ = stop.send(true);
        let joined = async {
            while let Some(result) = services.join_next().await {
                result??;
            }
            Ok::<(), ServiceError>(())
        };
        // A stuck local RPC must not keep its owning process alive indefinitely.
        tokio::time::timeout(
            config.drain_timeout.max(std::time::Duration::from_secs(1)),
            joined,
        )
        .await??;
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
