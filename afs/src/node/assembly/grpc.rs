//! Node TCP gRPC server (段 18, T8): TLS incoming, the trusted Peer
//! certificates, the control/data/chunks/owner_files services and the serve
//! task.
//!
//! T8 is fatal class: `serve_with_incoming_shutdown` returning `Err` shuts the
//! Node down. Stop: `services.stop` drives graceful shutdown of the server.

use super::common::{Backends, LocalEndpoints};
#[cfg(feature = "ownerfs")]
use super::ownerfs;
#[cfg(test)]
use crate::node::diagnostics;
use crate::{
    config::Config,
    node::{Node, rpc},
    runtime::{BoxError, Services, cancelled},
};
use std::sync::Arc;

const OWNER_ONLY_TCP_MAX_FRAME_SIZE: u32 = 256 * 1024;

fn configure_node_tcp_server(
    grpc_config: &afs_transport::grpc::GrpcConfig,
    ownerfs: bool,
    dfs: bool,
) -> tonic::transport::Server {
    let server = grpc_config.configure_server(tonic::transport::Server::builder());
    if ownerfs && !dfs {
        server.max_frame_size(OWNER_ONLY_TCP_MAX_FRAME_SIZE)
    } else {
        server
    }
}

/// Build every gRPC service and start the serve task as one service.
///
/// Fallible steps keep their original order: TLS security, incoming TLS,
/// trusted certificates, the control service, the DFS chunk service, then the
/// OwnerFs `owner_files` service. An error returns before the task starts.
pub(super) fn start(
    cfg: &Config,
    grpc: tokio::net::TcpListener,
    state: &Arc<Node>,
    endpoints: &LocalEndpoints,
    backends: &Backends,
    registered_node_epoch: u64,
    services: &mut Services,
) -> Result<(), BoxError> {
    let sessions = &endpoints.sessions;
    #[cfg(feature = "ownerfs")]
    let owner_rdma_sessions = &endpoints.owner_rdma_sessions;
    let diagnostic_storage = endpoints.diagnostic_storage.clone();
    #[cfg(feature = "dfs")]
    let timeout = backends.timeout;
    #[cfg(feature = "dfs")]
    let dfs_meta = &backends.dfs.meta;
    #[cfg(feature = "dfs")]
    let local_chunk_store = &backends.dfs.storage.local_chunk_store;
    #[cfg(feature = "dfs")]
    let dfs_replica_plane = &backends.dfs.replica_plane;
    #[cfg(not(feature = "dfs"))]
    let _ = (registered_node_epoch, backends);
    #[cfg(not(any(feature = "ownerfs", feature = "dfs")))]
    let _ = state;
    let stop = services.stop.subscribe();
    let grpc_config = afs_transport::grpc::GrpcConfig::default();
    let grpc_security = afs_transport::grpc::SecurityManager::new(cfg.tls_config())?;
    let grpc_server = configure_node_tcp_server(&grpc_config, cfg.ownerfs, cfg.dfs);
    let incoming =
        grpc_config.configure_tcp_incoming(tonic::transport::server::TcpIncoming::from(grpc));
    let incoming = grpc_security.incoming(incoming)?;
    // 业务 Handler 在 Node，公共 transport 只提供 builder 配置和低层搬运机制。
    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    let dfs_trusted = cfg
        .trusted_node_certs
        .iter()
        .map(|(node_id, path)| Ok((node_id.clone(), crate::config::read_certificate_der(path)?)))
        .collect::<afs_error::Result<Vec<_>>>()?;
    let control = rpc::control::NodeControlService::new(sessions.clone()).into_server();
    // One bound on long advisory lock waits across both backends' control services.
    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    let lock_waits = rpc::control::BlockingLockWaits::default();
    #[cfg(feature = "dfs")]
    let dfs_owner_control = if let Some(dfs) = state.dfs.as_ref() {
        rpc::control::DfsOwnerControlService::with_dfs_owner(
            lock_waits.clone(),
            dfs.clone(),
            Arc::new(rpc::data::MtlsPeerAuthenticator::new(dfs_trusted.clone())?),
        )
    } else {
        rpc::control::DfsOwnerControlService::new(lock_waits.clone())
    }
    .into_server();
    #[cfg(feature = "ownerfs")]
    let owner_control =
        ownerfs::owner_control_server(state, &dfs_trusted, owner_rdma_sessions, lock_waits)?;
    let data = rpc::data::make_data_server(diagnostic_storage, sessions.clone());
    #[cfg(feature = "dfs")]
    let dfs_read_authorizer: Arc<dyn rpc::data::DfsReadAuthorizer> = match dfs_meta.as_ref() {
        Some(meta) => Arc::new(rpc::data::CachedDfsReadAuthorizer::new(
            meta.clone(),
            cfg.id.clone(),
            registered_node_epoch,
        )),
        None => Arc::new(rpc::data::DenyDfsReadAuthorizer),
    };
    #[cfg(feature = "dfs")]
    let dfs_replica_authorizer: Arc<dyn rpc::data::DfsReplicaAuthorizer> = match dfs_meta.clone() {
        Some(meta) => Arc::new(rpc::data::MetaReplicaAuthorizer {
            meta,
            node_id: cfg.id.clone(),
            node_epoch: registered_node_epoch,
        }),
        None => Arc::new(rpc::data::DenyDfsReplicaAuthorizer),
    };
    #[cfg(feature = "dfs")]
    let dfs_chunks = rpc::data::make_dfs_chunks_server_with_transport(
        local_chunk_store.clone(),
        Arc::new(rpc::data::MtlsPeerAuthenticator::new(dfs_trusted.clone())?),
        dfs_read_authorizer,
        dfs_replica_authorizer,
        Some(dfs_replica_plane.clone()),
        timeout,
        rpc::data::DfsChunkTransportResources {
            rdma_sessions: sessions.clone(),
            payload_metrics: Some(rpc::data::DfsPayloadMetrics::register(
                &state.observability.registry,
            )?),
        },
    );
    #[cfg(feature = "ownerfs")]
    let owner_files =
        ownerfs::owner_files_server(cfg, state, &backends.metrics.owner_rpc, owner_rdma_sessions)?;
    #[cfg(feature = "dfs")]
    let grpc_state = state.clone();
    services.spawn(async move {
        let router = grpc_server
            .layer(afs_tracing::GrpcServerTraceLayer::default())
            .add_service(control)
            .add_service(data);
        #[cfg(feature = "ownerfs")]
        let router = router.add_service(owner_control).add_service(owner_files);
        #[cfg(feature = "dfs")]
        let router = {
            let dfs_owner_files = if let Some(dfs) = grpc_state.dfs.as_ref() {
                rpc::data::make_dfs_owner_files_server(rpc::data::DfsOwnerFilesService::new(
                    dfs.clone(),
                    Arc::new(rpc::data::MtlsPeerAuthenticator::new(dfs_trusted.clone())?),
                ))
            } else {
                rpc::data::make_dfs_owner_files_server(rpc::data::DfsOwnerFilesService::default())
            };
            router
                .add_service(dfs_owner_control)
                .add_service(dfs_chunks)
                .add_service(dfs_owner_files)
        };
        router
            .serve_with_incoming_shutdown(incoming, cancelled(stop))
            .await
            .map_err(Into::into)
    });
    Ok(())
}

#[cfg(test)]
mod node_tcp_server_settings_tests {
    use super::{
        OWNER_ONLY_TCP_MAX_FRAME_SIZE, configure_node_tcp_server,
        diagnostics::DiagnosticStorage,
        rpc::{control::RdmaSessionRegistry, data::make_data_server},
    };
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_stream::wrappers::TcpListenerStream;

    const DEFAULT_HTTP2_MAX_FRAME_SIZE: u32 = 16 * 1024;
    const HTTP2_SETTINGS_MAX_FRAME_SIZE: u16 = 0x5;

    #[tokio::test]
    async fn node_tcp_server_advertises_owner_only_frame_size_policy() {
        for (ownerfs, dfs, expected) in [
            (true, false, OWNER_ONLY_TCP_MAX_FRAME_SIZE),
            (false, true, DEFAULT_HTTP2_MAX_FRAME_SIZE),
            (true, true, DEFAULT_HTTP2_MAX_FRAME_SIZE),
            (false, false, DEFAULT_HTTP2_MAX_FRAME_SIZE),
        ] {
            let actual = advertised_http2_max_frame_size(ownerfs, dfs).await;
            assert_eq!(actual, expected, "ownerfs={ownerfs} dfs={dfs}");
        }
    }

    async fn advertised_http2_max_frame_size(ownerfs: bool, dfs: bool) -> u32 {
        let temp = tempfile::tempdir().expect("tempdir");
        let storage = DiagnosticStorage::new(temp.path().join("data")).expect("storage");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("local addr");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let grpc_config = afs_transport::grpc::GrpcConfig::default();
        let server = tokio::spawn(async move {
            let result = configure_node_tcp_server(&grpc_config, ownerfs, dfs)
                .add_service(make_data_server(
                    std::sync::Arc::new(storage),
                    RdmaSessionRegistry::default(),
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await;
            assert!(result.is_ok(), "server failed: {result:?}");
        });

        let mut stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        stream
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .expect("write preface");
        stream
            .write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0])
            .await
            .expect("write empty settings");
        let advertised = read_settings_max_frame_size(&mut stream, Duration::from_secs(3)).await;
        drop(stream);
        let _ = shutdown_tx.send(());
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .expect("server shutdown timeout")
            .expect("server join");
        advertised
    }

    async fn read_settings_max_frame_size(
        stream: &mut tokio::net::TcpStream,
        timeout: Duration,
    ) -> u32 {
        loop {
            let mut header = [0u8; 9];
            tokio::time::timeout(timeout, stream.read_exact(&mut header))
                .await
                .expect("settings frame timeout")
                .expect("read frame header");
            let length =
                ((header[0] as usize) << 16) | ((header[1] as usize) << 8) | header[2] as usize;
            let frame_type = header[3];
            let stream_id = u32::from_be_bytes([header[5] & 0x7f, header[6], header[7], header[8]]);
            let mut payload = vec![0; length];
            tokio::time::timeout(timeout, stream.read_exact(&mut payload))
                .await
                .expect("settings payload timeout")
                .expect("read frame payload");
            if frame_type != 4 || stream_id != 0 {
                continue;
            }
            for setting in payload.chunks_exact(6) {
                let id = u16::from_be_bytes([setting[0], setting[1]]);
                if id == HTTP2_SETTINGS_MAX_FRAME_SIZE {
                    return u32::from_be_bytes([setting[2], setting[3], setting[4], setting[5]]);
                }
            }
            return DEFAULT_HTTP2_MAX_FRAME_SIZE;
        }
    }
}
