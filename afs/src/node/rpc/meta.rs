//! Node owns its generated Meta caller; common transport only configures it.
//!
//! 调用端直接使用 Proto 生成的 MetaClient，再应用 common/transport/grpc 的配置。
//! 这是 Node 的内部控制 caller，不是对外的本机高性能 client/ SDK。
//! 示例每次显式 ping 建连；未来节点注册/watch 的连接复用由相应业务模块管理。
#[cfg(feature = "ownerfs")]
use crate::node::vfs::ownerfs::{
    catalog::LocalRootRecord,
    root::{
        OwnerRootInventory, PreparedRoot, PresentedRootAccess, RootGrant, RootId, RootLocation,
        RootMeta, RootReservation, RootRight,
    },
};
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
use afs_error::CLIENT_PROTOCOL_VIOLATION;
use afs_error::{
    CLIENT_ARGUMENT_INVALID, CLIENT_CONNECTION_UNAVAILABLE, CLIENT_DEADLINE_EXCEEDED, Error, Result,
};
#[cfg(feature = "ownerfs")]
use afs_protocol::meta::{
    AbortRootRequest, AcquireRootRequest, ActivateRootRequest, DeactivateRootRequest,
    ListOwnerRootsRequest, LookupNodeRequest, LookupRootRequest, PollRootCommandBatchRequest,
    RecoverRootRequest, ReserveConflictPolicy, ReserveRootRequest, ValidateRootAccessRequest,
    owner_roots_client::OwnerRootsClient,
};
use afs_protocol::meta::{
    MetaBackendPersistence, PingRequest, RegisterNodeReply, RegisterNodeRequest,
    meta_client::MetaClient,
};
use afs_transport::grpc::{GrpcConfig, SecurityManager, TlsConfig};
use std::error::Error as StdError;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "dfs")]
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
#[cfg(feature = "ownerfs")]
use tonic::transport::Channel;
use tonic::transport::Endpoint;

fn map_registration_connect_error(error: tonic::transport::Error) -> Error {
    if let Some(io_error) = transport_error_source::<std::io::Error>(&error) {
        let code = match io_error.kind() {
            std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput => {
                CLIENT_ARGUMENT_INVALID
            }
            std::io::ErrorKind::TimedOut => CLIENT_DEADLINE_EXCEEDED,
            _ => CLIENT_CONNECTION_UNAVAILABLE,
        };
        return Error::coded(code, transport_error_message(&error));
    }
    Error::coded(
        CLIENT_CONNECTION_UNAVAILABLE,
        transport_error_message(&error),
    )
}

fn transport_error_source<T: StdError + 'static>(error: &tonic::transport::Error) -> Option<&T> {
    let mut source = StdError::source(error);
    while let Some(inner) = source {
        if let Some(typed) = inner.downcast_ref::<T>() {
            return Some(typed);
        }
        source = inner.source();
    }
    None
}

fn transport_error_message(error: &tonic::transport::Error) -> String {
    let mut message = error.to_string();
    let mut source = StdError::source(error);
    while let Some(inner) = source {
        message.push_str(": ");
        message.push_str(&inner.to_string());
        source = inner.source();
    }
    message
}
pub async fn ping(
    endpoint: &str,
    node_id: &str,
    timeout: Duration,
    tls: TlsConfig,
) -> afs_error::Result<String> {
    let config = GrpcConfig {
        connect_timeout: timeout,
        request_timeout: timeout,
        ..Default::default()
    };
    let endpoint = config.configure_client(
        Endpoint::from_shared(endpoint.to_owned())
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?,
    );
    let channel = SecurityManager::new(tls)
        .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?
        .configure_client(endpoint)
        .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?
        .connect()
        .await
        .map_err(|e| Error::coded(CLIENT_CONNECTION_UNAVAILABLE, e.to_string()))?;
    let mut client = MetaClient::new(afs_tracing::traced_channel(channel))
        .max_encoding_message_size(config.max_encoding_message_bytes)
        .max_decoding_message_size(config.max_decoding_message_bytes);
    let reply = client
        .ping(afs_tracing::request_with_current_context(PingRequest {
            node_id: node_id.into(),
        }))
        .await
        .map_err(afs_transport::grpc::error_status::status_to_error)?;
    Ok(reply.into_inner().message)
}

#[cfg(test)]
mod ping_tls_tests {
    use super::*;

    #[tokio::test]
    async fn ping_applies_tls_policy_before_connecting() {
        let error = ping(
            "https://127.0.0.1:1",
            "node-a",
            Duration::from_millis(10),
            TlsConfig::MutualTls {
                ca_certificate: "missing-ca.pem".into(),
                identity_certificate: "missing-client.pem".into(),
                identity_private_key: "missing-client-key.pem".into(),
                server_name: String::new(),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), CLIENT_ARGUMENT_INVALID);
        assert!(error.to_string().contains("server_name"));
    }
}

#[cfg(test)]
mod registration_lease_tests {
    use super::*;
    use std::time::Instant;

    fn reply(remaining_ms: u64, absolute_expiry_ms: u64) -> RegisterNodeReply {
        RegisterNodeReply {
            lease_epoch: 7,
            expires_at_unix_ms: absolute_expiry_ms,
            lease_remaining_ms: remaining_ms,
            ..Default::default()
        }
    }

    #[test]
    fn registration_lease_ignores_cross_machine_wall_clock_offsets() {
        let sent = Instant::now();
        let received = sent + Duration::from_secs(2);
        for absolute_expiry in [0, 1, u64::MAX] {
            let registration =
                checked_registration_readiness(reply(30_000, absolute_expiry), sent, received)
                    .unwrap();
            assert_eq!(
                registration.session_deadline,
                sent + Duration::from_secs(30)
            );
            assert_eq!(
                registration.session_deadline.duration_since(received),
                Duration::from_secs(28)
            );
        }
    }

    #[test]
    fn registration_lease_rejects_missing_ttl_or_zero_epoch() {
        let sent = Instant::now();
        assert!(checked_registration_readiness(reply(0, u64::MAX), sent, sent).is_err());
        let mut missing_epoch = reply(30_000, u64::MAX);
        missing_epoch.lease_epoch = 0;
        assert!(checked_registration_readiness(missing_epoch, sent, sent).is_err());
    }

    #[test]
    fn registration_lease_deducts_slow_response_and_rejects_exhausted_ttl() {
        let sent = Instant::now();
        let deadline = sent + Duration::from_secs(5);
        let registration = checked_registration_readiness(
            reply(5_000, u64::MAX),
            sent,
            sent + Duration::from_secs(4),
        )
        .unwrap();
        assert_eq!(registration.session_deadline, deadline);
        for received in [deadline, deadline + Duration::from_secs(1)] {
            assert!(
                checked_registration_readiness(reply(5_000, u64::MAX), sent, received).is_err()
            );
        }
    }

    #[test]
    fn registration_lease_does_not_exceed_requested_ttl() {
        let sent = Instant::now();
        let registration =
            checked_registration_readiness(reply(u64::MAX, u64::MAX), sent, sent).unwrap();
        assert_eq!(
            registration.session_deadline,
            sent + Duration::from_secs(30)
        );
    }
}

#[cfg(test)]
mod registration_connect_tests {
    use super::*;
    use crate::meta::{Meta, rpc::MetaRpc};
    use afs_protocol::meta::{NodeDescriptor, NodeEndpoint};
    use std::{fs, path::Path, process::Command, sync::Arc};
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

    #[tokio::test]
    async fn registration_rejects_known_permanent_tls_transport_error() {
        let tls = openssl_tls_fixture();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener address");
        let server_tls = ServerTlsConfig::new()
            .client_ca_root(Certificate::from_pem(
                fs::read(&tls.ca).expect("read CA certificate"),
            ))
            .identity(Identity::from_pem(
                fs::read(&tls.server_cert).expect("read server certificate"),
                fs::read(&tls.server_key).expect("read server key"),
            ));
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls)
                .expect("server TLS")
                .add_service(afs_protocol::meta::meta_server::MetaServer::new(MetaRpc(
                    Arc::new(Meta::new(
                        "meta-test".into(),
                        crate::runtime::Observability::new().unwrap(),
                    )),
                )))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("test Meta TLS server");
        });

        let endpoint = format!("https://{addr}");
        let error = register_node_with_readiness(
            &endpoint,
            node_descriptor(&endpoint),
            Duration::from_secs(2),
            TlsConfig::MutualTls {
                ca_certificate: tls.ca.clone(),
                identity_certificate: tls.client_cert.clone(),
                identity_private_key: tls.client_key.clone(),
                server_name: "wrong.local".into(),
            },
        )
        .await
        .expect_err("server_name mismatch must fail during TLS handshake");

        server.abort();
        let _ = server.await;
        assert!(error.message().contains("wrong.local"), "{error}");
        assert_eq!(error.code(), CLIENT_ARGUMENT_INVALID);
        assert_eq!(error.kind(), afs_error::ErrorKind::InvalidArgument);
    }

    #[tokio::test]
    async fn registration_connect_refused_remains_retryable_unavailable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener address");
        drop(listener);

        let error = register_node_with_readiness(
            &format!("http://{addr}"),
            NodeDescriptor {
                node_id: "node-a".into(),
                endpoint: Some(NodeEndpoint {
                    grpc_addr: format!("http://{addr}"),
                    data_addr: format!("http://{addr}"),
                    rest_addr: "http://127.0.0.1:0".into(),
                }),
                labels: Default::default(),
                capabilities: Vec::new(),
                session_id: "session-a".into(),
                storage_devices: Vec::new(),
            },
            Duration::from_millis(50),
            TlsConfig::Disabled,
        )
        .await
        .expect_err("closed listener must fail to connect");

        assert_eq!(error.code(), CLIENT_CONNECTION_UNAVAILABLE);
        assert_eq!(error.kind(), afs_error::ErrorKind::Unavailable);
    }

    struct TlsFixture {
        _temp: tempfile::TempDir,
        ca: std::path::PathBuf,
        server_cert: std::path::PathBuf,
        server_key: std::path::PathBuf,
        client_cert: std::path::PathBuf,
        client_key: std::path::PathBuf,
    }

    fn openssl_tls_fixture() -> TlsFixture {
        let temp = tempfile::tempdir().expect("create TLS fixture directory");
        let dir = temp.path();
        run_openssl(
            dir,
            &[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-sha256",
                "-subj",
                "/CN=AFS Test CA",
                "-keyout",
                "ca-key.pem",
                "-out",
                "ca.pem",
            ],
        );
        write_ext(
            dir,
            "server.ext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
        );
        signed_cert(dir, "server", "/CN=localhost", "server.ext");
        write_ext(dir, "client.ext", "extendedKeyUsage=clientAuth\n");
        signed_cert(dir, "client", "/CN=node-a", "client.ext");

        TlsFixture {
            ca: dir.join("ca.pem"),
            server_cert: dir.join("server.pem"),
            server_key: dir.join("server-key.pem"),
            client_cert: dir.join("client.pem"),
            client_key: dir.join("client-key.pem"),
            _temp: temp,
        }
    }

    fn signed_cert(dir: &Path, name: &str, subject: &str, ext: &str) {
        let key = format!("{name}-key.pem");
        let csr = format!("{name}.csr");
        let cert = format!("{name}.pem");
        run_openssl(
            dir,
            &[
                "req", "-new", "-newkey", "rsa:2048", "-nodes", "-sha256", "-subj", subject,
                "-keyout", &key, "-out", &csr,
            ],
        );
        run_openssl(
            dir,
            &[
                "x509",
                "-req",
                "-days",
                "1",
                "-sha256",
                "-in",
                &csr,
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca-key.pem",
                "-CAcreateserial",
                "-out",
                &cert,
                "-extfile",
                ext,
            ],
        );
    }

    fn write_ext(dir: &Path, name: &str, content: &str) {
        fs::write(dir.join(name), content).expect("write certificate extension file");
    }

    fn run_openssl(dir: &Path, args: &[&str]) {
        let output = Command::new("openssl")
            .args(args)
            .current_dir(dir)
            .output()
            .expect("run openssl");
        assert!(
            output.status.success(),
            "openssl {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn node_descriptor(endpoint: &str) -> NodeDescriptor {
        NodeDescriptor {
            node_id: "node-a".into(),
            endpoint: Some(NodeEndpoint {
                grpc_addr: endpoint.into(),
                data_addr: endpoint.into(),
                rest_addr: "http://127.0.0.1:0".into(),
            }),
            labels: Default::default(),
            capabilities: Vec::new(),
            session_id: "session-a".into(),
            storage_devices: Vec::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetaPersistenceCapability {
    Unknown,
    Volatile,
    Persistent,
}

impl From<MetaBackendPersistence> for MetaPersistenceCapability {
    fn from(value: MetaBackendPersistence) -> Self {
        match value {
            MetaBackendPersistence::Persistent => Self::Persistent,
            MetaBackendPersistence::Volatile => Self::Volatile,
            MetaBackendPersistence::Unknown | MetaBackendPersistence::Unspecified => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeRegistrationReadiness {
    pub lease_epoch: u64,
    pub expires_at_unix_ms: u64,
    /// Conservative local cutoff, independent of Node/Meta wall-clock skew.
    pub session_deadline: Instant,
    pub meta_backend_persistence: MetaPersistenceCapability,
    pub meta_backend_healthy: bool,
    pub meta_persistence_ready: bool,
    pub meta_persistence_detail: String,
}

pub fn registration_readiness_from_reply(reply: RegisterNodeReply) -> NodeRegistrationReadiness {
    registration_readiness_from_reply_at(reply, Instant::now())
}

const NODE_REGISTRATION_LEASE_SECONDS: u64 = 30;

fn registration_readiness_from_reply_at(
    reply: RegisterNodeReply,
    request_started_at: Instant,
) -> NodeRegistrationReadiness {
    let remaining = Duration::from_millis(reply.lease_remaining_ms)
        .min(Duration::from_secs(NODE_REGISTRATION_LEASE_SECONDS));
    let persistence = MetaBackendPersistence::try_from(reply.meta_backend_persistence)
        .unwrap_or(MetaBackendPersistence::Unspecified)
        .into();
    let meta_persistence_ready = persistence == MetaPersistenceCapability::Persistent
        && reply.meta_backend_healthy
        && reply.meta_persistence_ready;
    NodeRegistrationReadiness {
        lease_epoch: reply.lease_epoch,
        expires_at_unix_ms: reply.expires_at_unix_ms,
        session_deadline: request_started_at + remaining,
        meta_backend_persistence: persistence,
        meta_backend_healthy: reply.meta_backend_healthy,
        meta_persistence_ready,
        meta_persistence_detail: reply.meta_persistence_detail,
    }
}

fn checked_registration_readiness(
    reply: RegisterNodeReply,
    request_started_at: Instant,
    received_at: Instant,
) -> Result<NodeRegistrationReadiness> {
    if reply.lease_epoch == 0 {
        return Err(Error::coded(
            CLIENT_ARGUMENT_INVALID,
            "Meta returned a zero Node registration epoch",
        ));
    }
    let registration = registration_readiness_from_reply_at(reply, request_started_at);
    if registration.session_deadline <= received_at {
        return Err(Error::coded(
            afs_error::IO_PERMISSION_DENIED,
            "Meta Node registration lifetime is missing or expired",
        ));
    }
    Ok(registration)
}

/// 同步 VFS/FUSE 回调的 Meta adapter。调用方必须在 FUSE 线程或
/// `spawn_blocking` 中，不能在 Tokio worker 上直接调用 RootMeta 方法。
/// 创建/授权/恢复是慢路径；根内操作不经过本模块。
#[cfg(feature = "ownerfs")]
pub struct GrpcRootMeta {
    channel: Channel,
    runtime: tokio::runtime::Handle,
    node_id: String,
    session_id: String,
    timeout: Duration,
    sequence: AtomicU64,
}

#[cfg(feature = "ownerfs")]
impl GrpcRootMeta {
    pub fn new(
        endpoint: &str,
        node_id: String,
        session_id: String,
        timeout: Duration,
        tls: TlsConfig,
    ) -> Result<Self> {
        let endpoint = GrpcConfig {
            connect_timeout: timeout,
            request_timeout: timeout,
            ..Default::default()
        }
        .configure_client(
            Endpoint::from_shared(endpoint.to_owned())
                .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?,
        );
        let endpoint = SecurityManager::new(tls)
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?
            .configure_client(endpoint)
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?;
        let channel = endpoint.connect_lazy();
        Ok(Self {
            channel,
            runtime: tokio::runtime::Handle::current(),
            node_id,
            session_id,
            timeout,
            sequence: AtomicU64::new(1),
        })
    }

    fn request_id(&self) -> String {
        crate::meta::store::timed_request_id(format!(
            "{}-{}",
            self.session_id,
            self.sequence.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn client(&self) -> OwnerRootsClient<Channel> {
        OwnerRootsClient::new(self.channel.clone())
    }

    pub(crate) fn poll_root_command_batch(&self, after_revision: u64) -> Result<RootCommandBatch> {
        let mut client = self.client();
        let reply = self
            .run(client.poll_root_command_batch(PollRootCommandBatchRequest {
                node_id: self.node_id.clone(),
                session_id: self.session_id.clone(),
                after_revision,
            }))?
            .into_inner();
        root_command_batch_from_wire(after_revision, reply)
    }

    /// 节点位置来自 Meta 注册表，而非调用者在数据请求中自报的地址。
    /// 返回完整 URI，供 OwnerFs 在根授权缓存未命中时建立 P2P 连接。
    pub fn lookup_node_endpoint(&self, node_id: &str) -> Result<String> {
        self.lookup_node_location(node_id)
            .map(|(endpoint, _)| endpoint)
    }

    /// Returns the registered endpoint and Node incarnation for connection reuse.
    pub fn lookup_node_location(&self, node_id: &str) -> Result<(String, u64)> {
        let mut client = MetaClient::new(self.channel.clone());
        let reply = self
            .run(client.lookup_node(LookupNodeRequest {
                request_id: self.request_id(),
                node_id: node_id.to_owned(),
            }))?
            .into_inner();
        if !reply.found {
            return Err(Error::coded(
                afs_error::NODE_VFS_NOT_FOUND,
                "Home Node is not registered",
            ));
        }
        let node_epoch = reply.lease_epoch;
        let node = required(reply.node, "LookupNode.node")?;
        let endpoint = required(node.endpoint, "LookupNode.node.endpoint")?.grpc_addr;
        if endpoint.is_empty() {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "Home Node has no gRPC endpoint",
            ));
        }
        Ok((endpoint, node_epoch))
    }

    fn run<T>(
        &self,
        future: impl std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    ) -> Result<T> {
        self.runtime.block_on(async {
            tokio::time::timeout(self.timeout, future)
                .await
                .map_err(|_| {
                    Error::coded(
                        afs_error::CLIENT_DEADLINE_EXCEEDED,
                        "Meta control request timed out",
                    )
                })?
                .map_err(afs_transport::grpc::error_status::status_to_error)
        })
    }
}

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| {
        Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            format!("Meta reply missing {field}"),
        )
    })
}

#[cfg(feature = "ownerfs")]
fn right_wire(right: RootRight) -> i32 {
    use afs_protocol::meta::RootRight as Wire;
    (match right {
        RootRight::Lookup => Wire::Lookup,
        RootRight::Read => Wire::Read,
        RootRight::Write => Wire::Write,
        RootRight::Admin => Wire::Admin,
    }) as i32
}

#[cfg(feature = "ownerfs")]
fn remote_session_rights(requested: RootRight) -> Vec<i32> {
    let mut rights = vec![
        right_wire(RootRight::Lookup),
        right_wire(RootRight::Read),
        right_wire(RootRight::Write),
    ];
    if requested == RootRight::Admin {
        rights.push(right_wire(RootRight::Admin));
    }
    rights
}

#[cfg(feature = "ownerfs")]
fn grant_from_wire(value: afs_protocol::meta::RootAccess) -> Result<RootGrant> {
    use afs_protocol::meta::RootRight as Wire;
    let rights = value
        .rights
        .into_iter()
        .map(|right| match Wire::try_from(right).ok() {
            Some(Wire::Lookup) => Ok(RootRight::Lookup),
            Some(Wire::Read) => Ok(RootRight::Read),
            Some(Wire::Write) => Ok(RootRight::Write),
            Some(Wire::Admin) => Ok(RootRight::Admin),
            _ => Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "Meta returned invalid root right",
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    if value.root_id.is_empty() || value.fencing_token.is_empty() || rights.is_empty() {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "Meta returned incomplete root access",
        ));
    }
    Ok(RootGrant {
        id: RootId(value.root_id),
        epoch: value.root_epoch,
        home_node_id: value.home_node_id,
        home_session_id: value.home_session_id,
        holder_node_id: value.holder_node_id,
        session_id: value.session_id,
        access_generation: value.access_generation,
        rights,
        fencing_token: value.fencing_token,
    })
}

#[cfg(feature = "ownerfs")]
fn reservation_wire(value: &RootReservation) -> afs_protocol::meta::RootReservation {
    afs_protocol::meta::RootReservation {
        root_id: value.id.0.clone(),
        root_epoch: value.epoch,
        home_node_id: value.home_node_id.clone(),
        session_id: value.session_id.clone(),
        create_intent_id: value.create_intent_id.clone(),
        prepare_token: value.prepare_token.clone(),
    }
}

#[cfg(feature = "ownerfs")]
impl RootMeta for GrpcRootMeta {
    fn lookup_node_endpoint(&self, node_id: &str) -> Result<String> {
        GrpcRootMeta::lookup_node_endpoint(self, node_id)
    }

    fn reserve_root(&self, id: &RootId, create_intent_id: &str) -> Result<RootReservation> {
        let request = ReserveRootRequest {
            request_id: self.request_id(),
            root_id: id.0.clone(),
            preferred_home_node_id: self.node_id.clone(),
            session_id: self.session_id.clone(),
            rights: [
                RootRight::Lookup,
                RootRight::Read,
                RootRight::Write,
                RootRight::Admin,
            ]
            .map(right_wire)
            .to_vec(),
            expected_root_epoch: 0,
            parent_root_id: String::new(),
            create_intent_id: create_intent_id.to_owned(),
            conflict_policy: ReserveConflictPolicy::ReuseIfSameIntent as i32,
        };
        let mut client = self.client();
        let reply = self.run(client.reserve_root(request))?.into_inner();
        let value = required(reply.reservation, "ReserveRoot.reservation")?;
        Ok(RootReservation {
            id: RootId(value.root_id),
            epoch: value.root_epoch,
            home_node_id: value.home_node_id,
            session_id: value.session_id,
            create_intent_id: value.create_intent_id,
            prepare_token: value.prepare_token,
        })
    }

    fn activate_root(&self, prepared: &PreparedRoot) -> Result<RootGrant> {
        let mut client = self.client();
        let reply = self
            .run(client.activate_root(ActivateRootRequest {
                request_id: self.request_id(),
                reservation: Some(reservation_wire(prepared.reservation())),
                local_prepare_id: prepared.local_prepare_id().to_owned(),
                parent_fsync_generation: prepared.parent_fsync_generation(),
                parent_fsync_complete: true,
            }))?
            .into_inner();
        grant_from_wire(required(reply.access, "ActivateRoot.access")?)
    }

    fn abort_root(&self, reservation: &RootReservation) -> Result<()> {
        let mut client = self.client();
        self.run(client.abort_root(AbortRootRequest {
            request_id: self.request_id(),
            root_id: reservation.id.0.clone(),
            root_epoch: reservation.epoch,
            session_id: reservation.session_id.clone(),
            create_intent_id: reservation.create_intent_id.clone(),
            reason: "local root preparation failed".into(),
            prepare_token: reservation.prepare_token.clone(),
        }))?;
        Ok(())
    }

    fn deactivate_root(
        &self,
        root_id: &RootId,
        epoch: u64,
        home_node_id: &str,
        session_id: &str,
    ) -> Result<()> {
        let mut client = self.client();
        self.run(client.deactivate_root(DeactivateRootRequest {
            request_id: self.request_id(),
            root_id: root_id.0.clone(),
            root_epoch: epoch,
            home_node_id: home_node_id.to_owned(),
            home_session_id: session_id.to_owned(),
            reason: "root directory deleted".into(),
        }))?;
        Ok(())
    }

    fn lookup_root(&self, id: &RootId) -> Result<Option<RootLocation>> {
        let mut client = self.client();
        let reply = self
            .run(client.lookup_root(LookupRootRequest {
                request_id: self.request_id(),
                root_id: id.0.clone(),
            }))?
            .into_inner();
        if !reply.found {
            return Ok(None);
        }
        let value = required(reply.location, "LookupRoot.location")?;
        Ok(Some(RootLocation {
            id: RootId(value.root_id),
            epoch: value.root_epoch,
            home_node_id: value.home_node_id,
            home_session_id: value.home_session_id,
        }))
    }

    fn list_owner_roots(&self, home_node_id: &str) -> Result<OwnerRootInventory> {
        let mut client = self.client();
        let reply = self
            .run(client.list_owner_roots(ListOwnerRootsRequest {
                request_id: self.request_id(),
                home_node_id: home_node_id.to_owned(),
            }))?
            .into_inner();
        Ok(OwnerRootInventory {
            active: reply
                .active_roots
                .into_iter()
                .map(|value| RootLocation {
                    id: RootId(value.root_id),
                    epoch: value.root_epoch,
                    home_node_id: value.home_node_id,
                    home_session_id: value.home_session_id,
                })
                .collect(),
            pending: reply
                .pending_reservations
                .into_iter()
                .map(|value| RootReservation {
                    id: RootId(value.root_id),
                    epoch: value.root_epoch,
                    home_node_id: value.home_node_id,
                    session_id: value.session_id,
                    create_intent_id: value.create_intent_id,
                    prepare_token: value.prepare_token,
                })
                .collect(),
        })
    }

    fn acquire_root(&self, id: &RootId, right: RootRight) -> Result<RootGrant> {
        let mut client = self.client();
        let reply = self
            .run(client.acquire_root(AcquireRootRequest {
                request_id: self.request_id(),
                root_id: id.0.clone(),
                requester_node_id: self.node_id.clone(),
                session_id: self.session_id.clone(),
                rights: remote_session_rights(right),
                expected_root_epoch: 0,
                expected_access_generation: 0,
            }))?
            .into_inner();
        grant_from_wire(required(reply.access, "AcquireRoot.access")?)
    }

    fn current_node_session(&self, node_id: &str) -> Result<Option<String>> {
        let mut client = MetaClient::new(self.channel.clone());
        let reply = self
            .run(client.lookup_node(LookupNodeRequest {
                request_id: self.request_id(),
                node_id: node_id.to_owned(),
            }))?
            .into_inner();
        if !reply.found {
            return Ok(None);
        }
        let node = required(reply.node, "LookupNode.node")?;
        if node.session_id.is_empty() {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "LookupNode returned an empty process session",
            ));
        }
        Ok(Some(node.session_id))
    }

    fn validate_root_access(
        &self,
        presented: &PresentedRootAccess,
        authenticated_peer_node_id: &str,
    ) -> Result<RootGrant> {
        // observed_peer_node_id 必须由传输认证层给出，不能照抄请求体里的 holder。
        let mut client = self.client();
        let reply = self
            .run(client.validate_root_access(ValidateRootAccessRequest {
                request_id: self.request_id(),
                presented_access: Some(afs_protocol::meta::PresentedRootAccess {
                    root_id: presented.id.0.clone(),
                    root_epoch: presented.epoch,
                    home_node_id: presented.home_node_id.clone(),
                    holder_node_id: presented.holder_node_id.clone(),
                    session_id: presented.session_id.clone(),
                    access_generation: presented.access_generation,
                    fencing_token: presented.fencing_token.clone(),
                    home_session_id: presented.home_session_id.clone(),
                }),
                observed_peer_node_id: authenticated_peer_node_id.to_owned(),
                validator_home_node_id: self.node_id.clone(),
                validator_home_session_id: self.session_id.clone(),
            }))?
            .into_inner();
        grant_from_wire(required(reply.access, "ValidateRootAccess.access")?)
    }

    fn recover_root(&self, record: &LocalRootRecord, new_session_id: &str) -> Result<RootGrant> {
        let mut client = self.client();
        let reply = self
            .run(client.recover_root(RecoverRootRequest {
                request_id: self.request_id(),
                root_id: record.id.0.clone(),
                expected_root_epoch: record.epoch,
                home_node_id: self.node_id.clone(),
                home_session_id: new_session_id.to_owned(),
                local_prepare_id: record.local_prepare_id.clone(),
            }))?
            .into_inner();
        grant_from_wire(required(reply.access, "RecoverRoot.access")?)
    }
}

/// 新 Node 会话注册完成之前不能挂载或服务 OwnerFs 根。
pub async fn register_node(
    endpoint: &str,
    node: afs_protocol::meta::NodeDescriptor,
    timeout: Duration,
    tls: TlsConfig,
) -> Result<u64> {
    register_node_with_readiness(endpoint, node, timeout, tls)
        .await
        .map(|registration| registration.lease_epoch)
}

/// Registers the Node and returns Meta persistence readiness as observed by the
/// server. Missing persistence fields decode as unknown/false. A missing or
/// exhausted relative session lifetime fails closed, including on heartbeat.
pub async fn register_node_with_readiness(
    endpoint: &str,
    node: afs_protocol::meta::NodeDescriptor,
    timeout: Duration,
    tls: TlsConfig,
) -> Result<NodeRegistrationReadiness> {
    static REGISTER_SEQUENCE: AtomicU64 = AtomicU64::new(1);
    // Include connection setup and the full response in the conservative lease
    // budget. Both startup and heartbeat consume this exact monotonic cutoff.
    let request_started_at = Instant::now();
    let config = GrpcConfig {
        connect_timeout: timeout,
        request_timeout: timeout,
        ..Default::default()
    };
    let endpoint = config.configure_client(
        Endpoint::from_shared(endpoint.to_owned())
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?,
    );
    let channel = SecurityManager::new(tls)
        .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?
        .configure_client(endpoint)
        .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?
        .connect()
        .await
        .map_err(map_registration_connect_error)?;
    let mut client = MetaClient::new(afs_tracing::traced_channel(channel));
    let reply = client
        .register_node(RegisterNodeRequest {
            request_id: crate::meta::store::timed_request_id(format!(
                "register-{}-{}",
                node.session_id,
                REGISTER_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            )),
            node: Some(node),
            lease_seconds: NODE_REGISTRATION_LEASE_SECONDS,
        })
        .await
        .map_err(afs_transport::grpc::error_status::status_to_error)?
        .into_inner();
    checked_registration_readiness(reply, request_started_at, Instant::now())
}

/// Synchronous adapter used by the DFS Backend on FUSE worker threads.
#[cfg(feature = "dfs")]
pub struct GrpcDfsMeta {
    channel: afs_tracing::TracedChannel,
    runtime: tokio::runtime::Handle,
    node_id: String,
    session_id: String,
    namespace_id: crate::dfs::NamespaceId,
    timeout: Duration,
    sequence: AtomicU64,
    placement: Mutex<Option<Arc<crate::dfs::PlacementSnapshot>>>,
    read_work: Option<crate::node::read_work::ReadWorkMetrics>,
}

#[cfg(feature = "dfs")]
impl GrpcDfsMeta {
    pub fn new(
        endpoint: &str,
        node_id: String,
        session_id: String,
        namespace_id: crate::dfs::NamespaceId,
        timeout: Duration,
        tls: TlsConfig,
    ) -> afs_error::Result<Self> {
        let endpoint = GrpcConfig {
            connect_timeout: timeout,
            request_timeout: timeout,
            ..Default::default()
        }
        .configure_client(
            Endpoint::from_shared(endpoint.to_owned())
                .map_err(|error| Error::coded(CLIENT_ARGUMENT_INVALID, error.to_string()))?,
        );
        let endpoint = SecurityManager::new(tls)
            .map_err(|error| Error::coded(CLIENT_ARGUMENT_INVALID, error.to_string()))?
            .configure_client(endpoint)
            .map_err(|error| Error::coded(CLIENT_ARGUMENT_INVALID, error.to_string()))?;
        Ok(Self {
            channel: afs_tracing::traced_channel(endpoint.connect_lazy()),
            runtime: tokio::runtime::Handle::current(),
            node_id,
            session_id,
            namespace_id,
            timeout,
            sequence: AtomicU64::new(1),
            placement: Mutex::new(None),
            read_work: None,
        })
    }

    pub(crate) fn with_read_work_metrics(
        mut self,
        metrics: crate::node::read_work::ReadWorkMetrics,
    ) -> Self {
        self.read_work = Some(metrics);
        self
    }

    fn record_read_rpc(&self, method: &'static str) {
        if let Some(metrics) = &self.read_work {
            metrics.read_rpc(method);
        }
    }

    // Covers request preparation, caller queue/transport wait, reply decoding
    // and domain conversion. It is a client adapter interval, not server CPU.
    fn read_diagnostic<T>(
        &self,
        stage: crate::node::read_work::ReadDiagnosticStage,
        call: impl FnOnce() -> afs_error::Result<T>,
    ) -> afs_error::Result<T> {
        use crate::node::read_work::ReadDiagnosticStage;
        let diagnostic = self
            .read_work
            .as_ref()
            .and_then(|metrics| metrics.diagnostic(stage));
        let phase = match stage {
            ReadDiagnosticStage::MetaPlacementSnapshot => Some("meta_placement_rpc"),
            ReadDiagnosticStage::MetaCreate => Some("meta_create_rpc"),
            ReadDiagnosticStage::MetaOpenWrite => Some("meta_open_write_rpc"),
            ReadDiagnosticStage::MetaResolveWriteAuthority => Some("meta_resolve_write_rpc"),
            ReadDiagnosticStage::MetaRenewWriteLease => Some("meta_renew_write_lease_rpc"),
            ReadDiagnosticStage::MetaSyncInodeMetadata => Some("meta_sync_inode_metadata_rpc"),
            _ => None,
        };
        let _coarse = phase.and_then(|phase| {
            self.read_work
                .as_ref()
                .and_then(|metrics| metrics.coarse_diagnostic(phase, 0))
        });
        let result = call();
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish(result.is_ok());
        }
        result
    }

    fn request_id(&self) -> String {
        crate::meta::store::timed_request_id(format!(
            "{}-dfs-{}",
            self.session_id,
            self.sequence.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn run<T>(
        &self,
        future: impl std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    ) -> afs_error::Result<T> {
        self.run_with_timeout(future, self.timeout)
    }

    fn run_with_timeout<T>(
        &self,
        future: impl std::future::Future<Output = std::result::Result<T, tonic::Status>>,
        timeout: Duration,
    ) -> afs_error::Result<T> {
        let timeout = timeout.min(self.timeout);
        self.runtime.block_on(async {
            tokio::time::timeout(timeout, future)
                .await
                .map_err(|_| {
                    Error::coded(
                        afs_error::CLIENT_DEADLINE_EXCEEDED,
                        "DFS Meta request timed out",
                    )
                })?
                .map_err(Self::dfs_meta_status_to_error)
        })
    }

    #[cfg(feature = "dfs")]
    fn dfs_meta_status_to_error(status: tonic::Status) -> Error {
        if Self::status_source_is_timeout_expired(&status) {
            return Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "DFS Meta request timed out",
            );
        }
        afs_transport::grpc::error_status::status_to_error(status)
    }

    fn status_source_is_timeout_expired(status: &tonic::Status) -> bool {
        let mut source = std::error::Error::source(status);
        while let Some(error) = source {
            if error.is::<tonic::TimeoutExpired>() {
                return true;
            }
            source = error.source();
        }
        false
    }

    fn client(
        &self,
    ) -> afs_protocol::meta::dfs_meta_client::DfsMetaClient<afs_tracing::TracedChannel> {
        afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone())
    }

    fn root_client(
        &self,
    ) -> afs_protocol::meta::meta_client::MetaClient<afs_tracing::TracedChannel> {
        afs_protocol::meta::meta_client::MetaClient::new(self.channel.clone())
    }

    pub fn validate_read_grants(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaValidateReadGrants,
            || {
                self.record_read_rpc("validate_read_grants");
                let validations = request
                    .validations
                    .into_iter()
                    .map(|validation| {
                        let grant = validation.grant;
                        afs_protocol::meta::DfsReadValidation {
                            grant: Some(afs_protocol::meta::DfsReadGrant {
                                namespace_id: grant.namespace_id.0,
                                file_version_id: grant.file_version_id.0,
                                layout_root_id: grant.layout_root_id.0,
                                caller_node_id: grant.caller_node_id,
                                caller_node_epoch: grant.caller_node_epoch,
                                expires_at_unix_ms: grant.expires_at_unix_ms,
                                fence: grant.fence,
                                token: grant.token,
                            }),
                            chunk_id: validation.chunk_id.0,
                            copy_id: validation.copy_id.0,
                            chunk_offset: validation.chunk_offset,
                            length: validation.length,
                        }
                    })
                    .collect();
                let reply = self
                    .run(self.client().validate_read_grants(
                        afs_protocol::meta::ValidateDfsReadGrantsRequest {
                            receiver_node_id: request.receiver_node_id,
                            receiver_node_epoch: request.receiver_node_epoch,
                            peer_node_id: request.peer_node_id,
                            validations,
                        },
                    ))?
                    .into_inner();
                reply
                    .authorizations
                    .into_iter()
                    .map(|entry| {
                        let validation =
                            required(entry.validation, "ReadAuthorization.validation")?;
                        Ok(crate::dfs::DfsAuthorizedRead {
                            validation: crate::dfs::DfsReadValidation {
                                grant: domain_dfs_read_grant(required(
                                    validation.grant,
                                    "ReadAuthorization.grant",
                                )?),
                                chunk_id: crate::dfs::ChunkId::new(validation.chunk_id),
                                copy_id: crate::dfs::CopyId::new(validation.copy_id),
                                chunk_offset: validation.chunk_offset,
                                length: validation.length,
                            },
                            allowed_ranges: entry
                                .allowed_ranges
                                .into_iter()
                                .map(|range| (range.offset, range.length))
                                .collect(),
                            expires_at_unix_ms: entry.expires_at_unix_ms,
                        })
                    })
                    .collect()
            },
        )
    }

    pub fn validate_external_read_grants(
        &self,
        request: crate::dfs::ValidateExternalReadGrants,
    ) -> afs_error::Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
        let reply = self
            .run(
                self.client().validate_external_read_grants(
                    afs_tracing::request_with_current_context(
                        afs_protocol::meta::ValidateExternalReadGrantsRequest {
                            caller_id: request.caller_id,
                            validations: request
                                .validations
                                .into_iter()
                                .map(wire_read_validation)
                                .collect(),
                        },
                    ),
                ),
            )?
            .into_inner();
        reply
            .authorizations
            .into_iter()
            .map(domain_external_authorized_read)
            .collect()
    }

    pub fn authorize_current_external_read(
        &self,
        request: crate::dfs::AuthorizeCurrentExternalRead,
    ) -> afs_error::Result<Option<crate::dfs::AuthorizeCurrentExternalReadReply>> {
        let response = self.runtime.block_on(async {
            tokio::time::timeout(
                self.timeout,
                self.client().authorize_current_external_read(
                    afs_tracing::request_with_current_context(
                        afs_protocol::meta::AuthorizeCurrentExternalReadRequest {
                            caller_id: request.caller_id,
                            inode_id: request.inode_id.0,
                            namespace_id: request.namespace_id.0,
                            expected_head_version_id: request.expected_head_version.0,
                            layout_root_id: request.layout_root_id.0,
                            validations: request
                                .validations
                                .into_iter()
                                .map(wire_read_validation)
                                .collect(),
                            missing_sources: request
                                .missing_sources
                                .into_iter()
                                .map(|missing| afs_protocol::meta::DfsMissingReadSource {
                                    chunk_id: missing.chunk_id.0,
                                    chunk_offset: missing.chunk_offset,
                                    length: missing.length,
                                })
                                .collect(),
                        },
                    ),
                ),
            )
            .await
        });
        let reply = match response {
            Err(_) => {
                return Err(Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "DFS Meta request timed out",
                ));
            }
            Ok(Err(status)) if current_external_read_is_unimplemented(&status) => return Ok(None),
            Ok(Err(status)) => return Err(Self::dfs_meta_status_to_error(status)),
            Ok(Ok(response)) => response.into_inner(),
        };
        domain_authorize_current_external_read_reply(reply).map(Some)
    }

    pub fn authorize_external_version_read(
        &self,
        request: crate::dfs::AuthorizeExternalVersionRead,
    ) -> afs_error::Result<crate::dfs::AuthorizeExternalVersionReadReply> {
        let response = self.runtime.block_on(async {
            tokio::time::timeout(
                self.timeout,
                self.client().authorize_external_version_read(
                    afs_tracing::request_with_current_context(
                        afs_protocol::meta::AuthorizeExternalVersionReadRequest {
                            caller_id: request.caller_id,
                            inode_id: request.inode_id.0,
                            namespace_id: request.namespace_id.0,
                            file_version_id: request.file_version_id.0,
                            layout_root_id: request.layout_root_id.0,
                            requests: request
                                .requests
                                .into_iter()
                                .map(|request| afs_protocol::meta::DfsMissingReadSource {
                                    chunk_id: request.chunk_id.0,
                                    chunk_offset: request.chunk_offset,
                                    length: request.length,
                                })
                                .collect(),
                        },
                    ),
                ),
            )
            .await
        });
        let reply = match response {
            Err(_) => {
                return Err(Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "DFS Meta request timed out",
                ));
            }
            Ok(Err(status)) => return Err(Self::dfs_meta_status_to_error(status)),
            Ok(Ok(response)) => response.into_inner(),
        };
        domain_authorize_external_version_read_reply(reply)
    }

    pub fn validate_replica_write(
        &self,
        request: crate::dfs::ValidateReplicaWriteRequest,
    ) -> afs_error::Result<crate::dfs::ReplicaWriteGrant> {
        let reply = self
            .run(
                self.client()
                    .validate_replica_write(wire_validate_replica_write_request(request)),
            )?
            .into_inner();
        domain_replica_write_grant(required(reply.grant, "ValidateReplicaWrite.grant")?)
    }

    pub fn claim_replication_task(
        &self,
        request: crate::dfs::ClaimReplicationTask,
    ) -> afs_error::Result<Option<crate::dfs::ReplicationClaim>> {
        let reply = self
            .run(self.client().claim_replication_task(
                afs_protocol::meta::ClaimDfsReplicationTaskRequest {
                    caller_id: request.caller_id,
                    caller_session_id: request.caller_session_id,
                    caller_node_epoch: request.caller_node_epoch,
                    operation_id: request.operation_id.0,
                    lease_seconds: request.lease_seconds,
                },
            ))?
            .into_inner();
        if !reply.claimed {
            return Ok(None);
        }
        required(reply.claim, "ClaimReplicationTask.claim")
            .and_then(domain_replication_claim)
            .map(Some)
    }

    pub fn report_replication_task(
        &self,
        request: crate::dfs::ReportReplicationTask,
    ) -> afs_error::Result<crate::dfs::ReplicationTask> {
        let reply = self
            .run(
                self.client().report_replication_task(
                    afs_protocol::meta::ReportDfsReplicationTaskRequest {
                        caller_id: request.caller_id,
                        caller_session_id: request.caller_session_id,
                        caller_node_epoch: request.caller_node_epoch,
                        operation_id: request.operation_id.0,
                        claim: Some(wire_replication_claim(&request.claim)),
                        durable_acks: request
                            .durable_acks
                            .into_iter()
                            .map(wire_replica_ack)
                            .collect(),
                        error: request.error,
                        source_invalid: request.source_invalid,
                    },
                ),
            )?
            .into_inner();
        required(reply.task, "ReportReplicationTask.task").and_then(domain_replication_task)
    }

    pub fn report_chunk_corruption(
        &self,
        request: crate::dfs::ReportChunkCorruption,
    ) -> afs_error::Result<()> {
        self.run(self.client().report_chunk_corruption(
            afs_protocol::meta::ReportDfsChunkCorruptionRequest {
                caller_id: request.caller_id,
                caller_session_id: request.caller_session_id,
                caller_node_epoch: request.caller_node_epoch,
                operation_id: request.operation_id.0,
                chunk_id: request.chunk_id.0,
                device_id: request.device_id,
                device_epoch: request.device_epoch,
                catalog_revision: request.catalog_revision,
            },
        ))?;
        Ok(())
    }

    pub fn report_corrupt_chunk(
        &self,
        request: crate::dfs::ReportChunkCorruption,
    ) -> afs_error::Result<()> {
        self.report_chunk_corruption(request)
    }
}

#[cfg(feature = "dfs")]
impl crate::node::replication::PlacementProvider for GrpcDfsMeta {
    fn snapshot(&self) -> afs_error::Result<Arc<crate::dfs::PlacementSnapshot>> {
        if let Some(snapshot) = self
            .placement
            .lock()
            .map_err(|_| {
                Error::coded(
                    afs_error::CLIENT_PROTOCOL_VIOLATION,
                    "DFS placement cache lock poisoned",
                )
            })?
            .clone()
        {
            return Ok(snapshot);
        }
        self.refresh(0)
    }

    fn refresh(
        &self,
        minimum_revision: u64,
    ) -> afs_error::Result<Arc<crate::dfs::PlacementSnapshot>> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaPlacementSnapshot,
            || {
                let reply = self
                    .run(self.client().get_placement_snapshot(
                        afs_protocol::meta::GetDfsPlacementSnapshotRequest {
                            caller_id: self.node_id.clone(),
                            minimum_revision,
                        },
                    ))?
                    .into_inner();
                let snapshot = Arc::new(domain_placement_snapshot(required(
                    reply.snapshot,
                    "GetPlacementSnapshot.snapshot",
                )?)?);
                *self.placement.lock().map_err(|_| {
                    Error::coded(
                        afs_error::CLIENT_PROTOCOL_VIOLATION,
                        "DFS placement cache lock poisoned",
                    )
                })? = Some(snapshot.clone());
                Ok(snapshot)
            },
        )
    }
}

#[cfg(feature = "dfs")]
impl crate::node::replication::ReplicationTaskAuthority for GrpcDfsMeta {
    fn claim(
        &self,
        request: crate::dfs::ClaimReplicationTask,
    ) -> afs_error::Result<Option<crate::dfs::ReplicationClaim>> {
        GrpcDfsMeta::claim_replication_task(self, request)
    }

    fn report(
        &self,
        request: crate::dfs::ReportReplicationTask,
    ) -> afs_error::Result<crate::dfs::ReplicationTask> {
        GrpcDfsMeta::report_replication_task(self, request)
    }

    fn report_corrupt_chunk(
        &self,
        request: crate::dfs::ReportChunkCorruption,
    ) -> afs_error::Result<()> {
        GrpcDfsMeta::report_corrupt_chunk(self, request)
    }
}

#[cfg(feature = "dfs")]
impl crate::node::dfs_read::ReadSourceProvider for GrpcDfsMeta {
    fn sources_for(
        &self,
        request: crate::dfs::DfsChunkSourcesRequest,
    ) -> afs_error::Result<crate::dfs::DfsChunkSourcesReply> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaGetChunkSources,
            || {
                self.record_read_rpc("get_chunk_sources");
                let reply = self
                    .run(
                        self.client()
                            .get_chunk_sources(afs_tracing::request_with_current_context(
                                afs_protocol::meta::GetDfsChunkSourcesRequest {
                                    caller_id: request.caller_id,
                                    namespace_id: request.namespace_id.0,
                                    file_version_id: request.file_version_id.0,
                                    layout_root_id: request.layout_root_id.0,
                                    chunk_ids: request
                                        .chunk_ids
                                        .into_iter()
                                        .map(|chunk_id| chunk_id.0)
                                        .collect(),
                                },
                            )),
                    )?
                    .into_inner();
                Ok(crate::dfs::DfsChunkSourcesReply {
                    revision: reply.revision,
                    chunks: reply
                        .chunks
                        .into_iter()
                        .map(domain_dfs_chunk_sources)
                        .collect::<afs_error::Result<Vec<_>>>()?,
                })
            },
        )
    }
}

#[cfg(feature = "dfs")]
impl crate::node::dfs_read::ExternalReadAuthorizer for GrpcDfsMeta {
    fn authorize_current_external_read(
        &self,
        request: crate::dfs::AuthorizeCurrentExternalRead,
    ) -> afs_error::Result<Option<crate::dfs::AuthorizeCurrentExternalReadReply>> {
        GrpcDfsMeta::authorize_current_external_read(self, request)
    }

    fn authorize_external_version_read(
        &self,
        request: crate::dfs::AuthorizeExternalVersionRead,
    ) -> afs_error::Result<Option<crate::dfs::AuthorizeExternalVersionReadReply>> {
        GrpcDfsMeta::authorize_external_version_read(self, request).map(Some)
    }

    fn validate_external_read_grants(
        &self,
        caller_id: &str,
        validations: Vec<crate::dfs::DfsReadValidation>,
    ) -> afs_error::Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
        GrpcDfsMeta::validate_external_read_grants(
            self,
            crate::dfs::ValidateExternalReadGrants {
                caller_id: caller_id.to_owned(),
                validations,
            },
        )
    }
}

#[cfg(feature = "dfs")]
impl crate::node::vfs::dfs::DfsMeta for GrpcDfsMeta {
    fn lookup(
        &self,
        parent: &crate::dfs::InodeId,
        name: &[u8],
    ) -> afs_error::Result<Option<crate::dfs::InodeRecord>> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaLookup,
            || {
                let reply = self
                    .run(self.client().lookup(afs_protocol::meta::DfsLookupRequest {
                        namespace_id: self.namespace_id.0.clone(),
                        parent_inode_id: parent.0.clone(),
                        name: name.to_vec(),
                    }))?
                    .into_inner();
                if !reply.found {
                    return Ok(None);
                }
                required(reply.inode, "DfsLookup.inode")
                    .and_then(domain_dfs_inode)
                    .map(Some)
            },
        )
    }

    fn create(
        &self,
        operation_id: &crate::dfs::OperationId,
        parent: &crate::dfs::InodeId,
        name: &[u8],
        attributes: crate::dfs::InodeAttributes,
    ) -> afs_error::Result<(crate::dfs::InodeRecord, crate::dfs::WriteLease)> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaCreate,
            || {
                let reply = self
                    .run(self.client().create(afs_protocol::meta::DfsCreateRequest {
                        caller_id: self.node_id.clone(),
                        operation_id: operation_id.0.clone(),
                        namespace_id: self.namespace_id.0.clone(),
                        parent_inode_id: parent.0.clone(),
                        name: name.to_vec(),
                        attributes: Some(wire_dfs_attributes(attributes)),
                        owner_session_id: self.session_id.clone(),
                        lease_seconds: crate::node::vfs::dfs::DFS_WRITE_LEASE_SECONDS,
                    }))?
                    .into_inner();
                Ok((
                    domain_dfs_inode(required(reply.inode, "DfsCreate.inode")?)?,
                    domain_dfs_write_lease(required(reply.write_lease, "DfsCreate.write_lease")?),
                ))
            },
        )
    }

    fn mkdir(
        &self,
        operation_id: &crate::dfs::OperationId,
        parent: &crate::dfs::InodeId,
        name: &[u8],
        attributes: crate::dfs::InodeAttributes,
        caller: crate::dfs::CallerContext,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().mkdir(afs_protocol::meta::DfsMkdirRequest {
                caller_id: self.node_id.clone(),
                operation_id: operation_id.0.clone(),
                namespace_id: self.namespace_id.0.clone(),
                parent_inode_id: parent.0.clone(),
                name: name.to_vec(),
                attributes: Some(wire_dfs_attributes(attributes)),
                caller: Some(wire_dfs_caller_context(caller)),
            }))?
            .into_inner();
        required(reply.inode, "DfsMkdir.inode").and_then(domain_dfs_inode)
    }

    fn read_dir(
        &self,
        parent: &crate::dfs::InodeId,
    ) -> afs_error::Result<Vec<crate::dfs::DentryRecord>> {
        let reply = self
            .run(
                self.client()
                    .read_dir(afs_protocol::meta::DfsReadDirRequest {
                        namespace_id: self.namespace_id.0.clone(),
                        parent_inode_id: parent.0.clone(),
                    }),
            )?
            .into_inner();
        reply.entries.into_iter().map(domain_dfs_dentry).collect()
    }

    fn unlink(
        &self,
        operation_id: &crate::dfs::OperationId,
        parent: &crate::dfs::InodeId,
        name: &[u8],
        caller: crate::dfs::CallerContext,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().unlink(afs_protocol::meta::DfsUnlinkRequest {
                caller_id: self.node_id.clone(),
                operation_id: operation_id.0.clone(),
                namespace_id: self.namespace_id.0.clone(),
                parent_inode_id: parent.0.clone(),
                name: name.to_vec(),
                caller: Some(wire_dfs_caller_context(caller)),
            }))?
            .into_inner();
        required(reply.inode, "DfsUnlink.inode").and_then(domain_dfs_inode)
    }

    fn rmdir(
        &self,
        operation_id: &crate::dfs::OperationId,
        parent: &crate::dfs::InodeId,
        name: &[u8],
        caller: crate::dfs::CallerContext,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().rmdir(afs_protocol::meta::DfsRmdirRequest {
                caller_id: self.node_id.clone(),
                operation_id: operation_id.0.clone(),
                namespace_id: self.namespace_id.0.clone(),
                parent_inode_id: parent.0.clone(),
                name: name.to_vec(),
                caller: Some(wire_dfs_caller_context(caller)),
            }))?
            .into_inner();
        required(reply.inode, "DfsRmdir.inode").and_then(domain_dfs_inode)
    }

    fn rename(
        &self,
        operation_id: &crate::dfs::OperationId,
        old_parent: &crate::dfs::InodeId,
        old_name: &[u8],
        new_parent: &crate::dfs::InodeId,
        new_name: &[u8],
        mode: crate::dfs::RenameMode,
        caller: crate::dfs::CallerContext,
    ) -> afs_error::Result<crate::dfs::RenameOutcome> {
        let mode = match mode {
            crate::dfs::RenameMode::NoReplace => afs_protocol::meta::DfsRenameMode::NoReplace,
            crate::dfs::RenameMode::Replace => afs_protocol::meta::DfsRenameMode::Replace,
        };
        let reply = self
            .run(self.client().rename(afs_protocol::meta::DfsRenameRequest {
                caller_id: self.node_id.clone(),
                operation_id: operation_id.0.clone(),
                namespace_id: self.namespace_id.0.clone(),
                old_parent_inode_id: old_parent.0.clone(),
                old_name: old_name.to_vec(),
                new_parent_inode_id: new_parent.0.clone(),
                new_name: new_name.to_vec(),
                mode: mode as i32,
                caller: Some(wire_dfs_caller_context(caller)),
            }))?
            .into_inner();
        Ok(crate::dfs::RenameOutcome {
            inode: domain_dfs_inode(required(reply.inode, "DfsRename.inode")?)?,
            replaced_inode: reply.replaced_inode.map(domain_dfs_inode).transpose()?,
        })
    }

    fn link(
        &self,
        operation_id: &crate::dfs::OperationId,
        inode_id: &crate::dfs::InodeId,
        expected_inode_revision: u64,
        parent: &crate::dfs::InodeId,
        name: &[u8],
        caller: crate::dfs::CallerContext,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().link(afs_protocol::meta::DfsLinkRequest {
                caller_id: self.node_id.clone(),
                operation_id: operation_id.0.clone(),
                namespace_id: self.namespace_id.0.clone(),
                existing_inode_id: inode_id.0.clone(),
                expected_inode_revision,
                parent_inode_id: parent.0.clone(),
                name: name.to_vec(),
                caller: Some(wire_dfs_caller_context(caller)),
            }))?
            .into_inner();
        required(reply.inode, "DfsLink.inode").and_then(domain_dfs_inode)
    }

    fn symlink(
        &self,
        request: crate::dfs::SymlinkRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(
                self.client()
                    .symlink(afs_protocol::meta::DfsSymlinkRequest {
                        caller_id: request.caller_id,
                        operation_id: request.operation_id.0,
                        namespace_id: request.namespace_id.0,
                        parent_inode_id: request.parent_inode_id.0,
                        name: request.name,
                        target: request.target,
                        attributes: Some(wire_dfs_attributes(request.attributes)),
                        caller: Some(wire_dfs_caller_context(request.caller)),
                    }),
            )?
            .into_inner();
        required(reply.inode, "DfsSymlink.inode").and_then(domain_dfs_inode)
    }

    fn mknod(
        &self,
        request: crate::dfs::MknodRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().mknod(afs_protocol::meta::DfsMknodRequest {
                caller_id: request.caller_id,
                operation_id: request.operation_id.0,
                namespace_id: request.namespace_id.0,
                parent_inode_id: request.parent_inode_id.0,
                name: request.name,
                special_node: Some(wire_dfs_special_node(request.kind)),
                attributes: Some(wire_dfs_attributes(request.attributes)),
                caller: Some(wire_dfs_caller_context(request.caller)),
            }))?
            .into_inner();
        required(reply.inode, "DfsMknod.inode").and_then(domain_dfs_inode)
    }

    fn read_link(&self, request: crate::dfs::ReadLinkRequest) -> afs_error::Result<Vec<u8>> {
        let reply = self
            .run(
                self.client()
                    .read_link(afs_protocol::meta::DfsReadLinkRequest {
                        namespace_id: request.namespace_id.0,
                        inode_id: request.inode_id.0,
                    }),
            )?
            .into_inner();
        Ok(reply.target)
    }

    fn open_write(
        &self,
        inode_id: &crate::dfs::InodeId,
    ) -> afs_error::Result<(crate::dfs::InodeRecord, crate::dfs::WriteLease)> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaOpenWrite,
            || {
                let reply = self
                    .run(
                        self.client()
                            .open_write(afs_protocol::meta::OpenDfsWriteRequest {
                                caller_id: self.node_id.clone(),
                                owner_session_id: self.session_id.clone(),
                                operation_id: self.request_id(),
                                inode_id: inode_id.0.clone(),
                                lease_seconds: crate::node::vfs::dfs::DFS_WRITE_LEASE_SECONDS,
                            }),
                    )?
                    .into_inner();
                Ok((
                    domain_dfs_inode(required(reply.inode, "OpenWrite.inode")?)?,
                    domain_dfs_write_lease(required(reply.write_lease, "OpenWrite.write_lease")?),
                ))
            },
        )
    }

    fn resolve_lock_authority(
        &self,
        inode_id: &crate::dfs::InodeId,
    ) -> afs_error::Result<(crate::dfs::InodeRecord, crate::dfs::WriteLease)> {
        let reply = self
            .run(self.client().resolve_lock_authority(
                afs_protocol::meta::ResolveDfsLockAuthorityRequest {
                    caller_id: self.node_id.clone(),
                    owner_session_id: self.session_id.clone(),
                    operation_id: self.request_id(),
                    inode_id: inode_id.0.clone(),
                    lease_seconds: crate::node::vfs::dfs::DFS_WRITE_LEASE_SECONDS,
                },
            ))?
            .into_inner();
        Ok((
            domain_dfs_inode(required(reply.inode, "ResolveLockAuthority.inode")?)?,
            domain_dfs_write_lease(required(
                reply.write_lease,
                "ResolveLockAuthority.write_lease",
            )?),
        ))
    }

    fn resolve_write_authority(
        &self,
        inode_id: &crate::dfs::InodeId,
    ) -> afs_error::Result<(crate::dfs::InodeRecord, crate::dfs::WriteLease)> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaResolveWriteAuthority,
            || {
                let reply = self
                    .run(self.client().resolve_write_authority(
                        afs_protocol::meta::ResolveDfsWriteAuthorityRequest {
                            caller_id: self.node_id.clone(),
                            owner_session_id: self.session_id.clone(),
                            operation_id: self.request_id(),
                            inode_id: inode_id.0.clone(),
                            lease_seconds: crate::node::vfs::dfs::DFS_WRITE_LEASE_SECONDS,
                        },
                    ))?
                    .into_inner();
                Ok((
                    domain_dfs_inode(required(reply.inode, "ResolveWriteAuthority.inode")?)?,
                    domain_dfs_write_lease(required(
                        reply.write_lease,
                        "ResolveWriteAuthority.write_lease",
                    )?),
                ))
            },
        )
    }

    fn renew_write_lease(
        &self,
        lease: crate::dfs::WriteLease,
    ) -> afs_error::Result<crate::dfs::WriteLease> {
        self.renew_write_lease_with_timeout(lease, self.timeout)
    }

    fn renew_write_lease_with_timeout(
        &self,
        lease: crate::dfs::WriteLease,
        timeout: Duration,
    ) -> afs_error::Result<crate::dfs::WriteLease> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaRenewWriteLease,
            || {
                let reply = self
                    .run_with_timeout(
                        self.client().renew_write_lease(
                            afs_protocol::meta::RenewDfsWriteLeaseRequest {
                                caller_id: self.node_id.clone(),
                                owner_session_id: self.session_id.clone(),
                                operation_id: self.request_id(),
                                current_lease: Some(wire_dfs_write_lease(lease)),
                                lease_seconds: crate::node::vfs::dfs::DFS_WRITE_LEASE_SECONDS,
                            },
                        ),
                        timeout,
                    )?
                    .into_inner();
                Ok(domain_dfs_write_lease(required(
                    reply.write_lease,
                    "RenewWriteLease.write_lease",
                )?))
            },
        )
    }

    fn get_inode(
        &self,
        inode_id: &crate::dfs::InodeId,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaGetInode,
            || {
                self.record_read_rpc("get_inode");
                let reply = self
                    .run(
                        self.client()
                            .get_inode(afs_tracing::request_with_current_context(
                                afs_protocol::meta::GetDfsInodeRequest {
                                    inode_id: inode_id.0.clone(),
                                },
                            )),
                    )?
                    .into_inner();
                required(reply.inode, "GetDfsInode.inode").and_then(domain_dfs_inode)
            },
        )
    }

    fn get_file_version(
        &self,
        version_id: &crate::dfs::FileVersionId,
    ) -> afs_error::Result<(crate::dfs::FileVersion, crate::dfs::LayoutRoot)> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaGetFileVersion,
            || {
                self.record_read_rpc("get_file_version");
                let reply = self
                    .run(self.client().get_file_version(
                        afs_tracing::request_with_current_context(
                            afs_protocol::meta::GetFileVersionRequest {
                                version_id: version_id.0.clone(),
                            },
                        ),
                    ))?
                    .into_inner();
                Ok((
                    domain_dfs_version(required(reply.version, "GetFileVersion.version")?),
                    domain_dfs_layout(required(reply.layout, "GetFileVersion.layout")?),
                ))
            },
        )
    }

    fn sync_inode_metadata(
        &self,
        sync: crate::dfs::SyncInodeMetadata,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        self.sync_inode_metadata_with_timeout(sync, self.timeout)
    }

    fn sync_inode_metadata_with_timeout(
        &self,
        sync: crate::dfs::SyncInodeMetadata,
        timeout: Duration,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaSyncInodeMetadata,
            || {
                let reply = self
                    .run_with_timeout(
                        self.client().sync_inode_metadata(
                            afs_protocol::meta::SyncDfsInodeMetadataRequest {
                                caller_id: self.node_id.clone(),
                                operation_id: sync.operation_id.0,
                                inode_id: sync.inode_id.0,
                                write_lease: Some(wire_dfs_write_lease(sync.write_lease)),
                                expected_inode_revision: sync.expected_inode_revision,
                                expected_head_version_id: sync
                                    .expected_head_version
                                    .map_or_else(String::new, |id| id.0),
                                metadata_delta: Some(wire_dfs_metadata_delta(sync.metadata_delta)),
                            },
                        ),
                        timeout,
                    )?
                    .into_inner();
                required(reply.inode, "SyncInodeMetadata.inode").and_then(domain_dfs_inode)
            },
        )
    }

    fn set_inode_attributes(
        &self,
        request: crate::dfs::SetInodeAttrRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(self.client().set_inode_attributes(
                afs_protocol::meta::DfsSetInodeAttributesRequest {
                    caller_id: request.caller_id,
                    operation_id: request.operation_id.0,
                    caller: Some(wire_dfs_caller_context(request.caller)),
                    inode_id: request.inode_id.0,
                    expected_inode_revision: request.expected_inode_revision,
                    update: Some(wire_dfs_attr_update(request.update)),
                },
            ))?
            .into_inner();
        required(reply.inode, "SetInodeAttributes.inode").and_then(domain_dfs_inode)
    }

    fn get_xattr(&self, request: crate::dfs::GetXattrRequest) -> afs_error::Result<Vec<u8>> {
        let reply = self
            .run(
                self.client()
                    .get_xattr(afs_protocol::meta::DfsGetXattrRequest {
                        caller: Some(wire_dfs_caller_context(request.caller)),
                        inode_id: request.inode_id.0,
                        name: request.name,
                    }),
            )?
            .into_inner();
        Ok(reply.value)
    }

    fn list_xattr(&self, request: crate::dfs::ListXattrRequest) -> afs_error::Result<Vec<Vec<u8>>> {
        let reply = self
            .run(
                self.client()
                    .list_xattr(afs_protocol::meta::DfsListXattrRequest {
                        caller: Some(wire_dfs_caller_context(request.caller)),
                        inode_id: request.inode_id.0,
                    }),
            )?
            .into_inner();
        Ok(reply.names)
    }

    fn set_xattr(
        &self,
        request: crate::dfs::SetXattrRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let mode = match request.mode {
            crate::dfs::XattrSetMode::Upsert => afs_protocol::meta::DfsXattrSetMode::Upsert,
            crate::dfs::XattrSetMode::Create => afs_protocol::meta::DfsXattrSetMode::Create,
            crate::dfs::XattrSetMode::Replace => afs_protocol::meta::DfsXattrSetMode::Replace,
        };
        let reply = self
            .run(
                self.client()
                    .set_xattr(afs_protocol::meta::DfsSetXattrRequest {
                        caller_id: request.caller_id,
                        operation_id: request.operation_id.0,
                        caller: Some(wire_dfs_caller_context(request.caller)),
                        inode_id: request.inode_id.0,
                        expected_inode_revision: request.expected_inode_revision,
                        name: request.name,
                        value: request.value,
                        mode: mode as i32,
                    }),
            )?
            .into_inner();
        required(reply.inode, "SetXattr.inode").and_then(domain_dfs_inode)
    }

    fn remove_xattr(
        &self,
        request: crate::dfs::RemoveXattrRequest,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run(
                self.client()
                    .remove_xattr(afs_protocol::meta::DfsRemoveXattrRequest {
                        caller_id: request.caller_id,
                        operation_id: request.operation_id.0,
                        caller: Some(wire_dfs_caller_context(request.caller)),
                        inode_id: request.inode_id.0,
                        expected_inode_revision: request.expected_inode_revision,
                        name: request.name,
                    }),
            )?
            .into_inner();
        required(reply.inode, "RemoveXattr.inode").and_then(domain_dfs_inode)
    }

    fn lookup_node_location(
        &self,
        node_id: &str,
    ) -> afs_error::Result<Option<crate::node::vfs::dfs::DfsNodeLocation>> {
        self.read_diagnostic(
            crate::node::read_work::ReadDiagnosticStage::MetaLookupNodeLocation,
            || {
                let reply =
                    self.run(self.root_client().lookup_node(
                        afs_protocol::meta::LookupNodeRequest {
                            request_id: self.request_id(),
                            node_id: node_id.to_owned(),
                        },
                    ))?
                    .into_inner();
                if !reply.found {
                    return Ok(None);
                }
                let node = required(reply.node, "LookupNode.node")?;
                let endpoint = required(node.endpoint, "LookupNode.node.endpoint")?;
                Ok(Some(crate::node::vfs::dfs::DfsNodeLocation {
                    node_id: node.node_id,
                    node_epoch: reply.lease_epoch,
                    data_endpoint: endpoint.data_addr,
                }))
            },
        )
    }

    fn current_node_session(&self, node_id: &str) -> afs_error::Result<Option<String>> {
        self.current_node_session_with_timeout(node_id, self.timeout)
    }

    fn current_node_session_with_timeout(
        &self,
        node_id: &str,
        timeout: Duration,
    ) -> afs_error::Result<Option<String>> {
        let reply = self
            .run_with_timeout(
                self.root_client()
                    .lookup_node(afs_protocol::meta::LookupNodeRequest {
                        request_id: self.request_id(),
                        node_id: node_id.to_owned(),
                    }),
                timeout,
            )?
            .into_inner();
        if !reply.found {
            return Ok(None);
        }
        let node = required(reply.node, "LookupNode.node")?;
        if node.session_id.is_empty() {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "LookupNode returned an empty process session",
            ));
        }
        Ok(Some(node.session_id))
    }

    fn commit_file_version(
        &self,
        commit: crate::dfs::CommitFileVersion,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        self.commit_file_version_with_timeout(commit, self.timeout)
    }

    fn commit_file_version_with_timeout(
        &self,
        commit: crate::dfs::CommitFileVersion,
        timeout: Duration,
    ) -> afs_error::Result<crate::dfs::InodeRecord> {
        let reply = self
            .run_with_timeout(
                self.client()
                    .commit_file_version(afs_tracing::request_with_current_context(
                        afs_protocol::meta::CommitFileVersionRequest {
                            gc_fence: commit.gc_fence.map(crate::dfs::GcFence::wire),
                            caller_id: self.node_id.clone(),
                            operation_id: commit.operation_id.0,
                            inode_id: commit.inode_id.0,
                            write_lease: Some(wire_dfs_write_lease(commit.write_lease)),
                            expected_inode_revision: commit.expected_inode_revision,
                            expected_head_version_id: commit
                                .expected_head_version
                                .map_or_else(String::new, |id| id.0),
                            version: Some(wire_dfs_version(commit.file_version)),
                            layout: Some(wire_dfs_layout(commit.layout_root)),
                            chunk_receipts: commit
                                .chunk_receipts
                                .into_iter()
                                .map(wire_chunk_receipt)
                                .collect(),
                            metadata_delta: Some(wire_dfs_metadata_delta(commit.metadata_delta)),
                            clone_source: commit.clone_source.map(|source| {
                                afs_protocol::meta::DfsCloneFileRange {
                                    version_id: source.version.0,
                                    source_offset: source.source_offset,
                                    destination_offset: source.destination_offset,
                                    length: source.length,
                                }
                            }),
                        },
                    )),
                timeout,
            )?
            .into_inner();
        required(reply.inode, "CommitFileVersion.inode").and_then(domain_dfs_inode)
    }
}

#[cfg(feature = "dfs")]
fn wire_dfs_caller_context(
    caller: crate::dfs::CallerContext,
) -> afs_protocol::meta::DfsCallerContext {
    afs_protocol::meta::DfsCallerContext {
        uid: caller.uid,
        gid: caller.gid,
        supplementary_gids: caller.supplementary_gids,
    }
}

#[cfg(feature = "dfs")]
fn wire_dfs_attr_update(
    update: crate::dfs::InodeAttrUpdate,
) -> afs_protocol::meta::DfsInodeAttributeUpdate {
    afs_protocol::meta::DfsInodeAttributeUpdate {
        mode: update.mode,
        uid: update.uid,
        gid: update.gid,
        atime_unix_ms: update.atime_unix_ms,
        mtime_unix_ms: update.mtime_unix_ms,
        ctime_unix_ms: update.ctime_unix_ms,
        timestamps_now: update.timestamps_now,
    }
}

#[cfg(feature = "dfs")]
fn domain_dfs_dentry(
    entry: afs_protocol::meta::DfsDentryRecord,
) -> afs_error::Result<crate::dfs::DentryRecord> {
    Ok(crate::dfs::DentryRecord {
        name: entry.name,
        inode: domain_dfs_inode(required(entry.inode, "DfsDentryRecord.inode")?)?,
    })
}

#[cfg(feature = "dfs")]
fn wire_dfs_attributes(
    attributes: crate::dfs::InodeAttributes,
) -> afs_protocol::meta::DfsInodeAttributes {
    afs_protocol::meta::DfsInodeAttributes {
        mode: attributes.mode,
        uid: attributes.uid,
        gid: attributes.gid,
        nlink: attributes.nlink,
        atime_unix_ms: attributes.atime_unix_ms,
        mtime_unix_ms: attributes.mtime_unix_ms,
        ctime_unix_ms: attributes.ctime_unix_ms,
    }
}

#[cfg(feature = "dfs")]
fn domain_dfs_inode(
    inode: afs_protocol::meta::DfsInodeRecord,
) -> afs_error::Result<crate::dfs::InodeRecord> {
    let attributes = required(inode.attributes, "DfsInodeRecord.attributes")?;
    let kind = match afs_protocol::meta::DfsInodeKind::try_from(inode.kind) {
        Ok(afs_protocol::meta::DfsInodeKind::Regular) => crate::dfs::InodeKind::Regular,
        Ok(afs_protocol::meta::DfsInodeKind::Directory) => crate::dfs::InodeKind::Directory,
        Ok(afs_protocol::meta::DfsInodeKind::Symlink) => crate::dfs::InodeKind::Symlink,
        Ok(afs_protocol::meta::DfsInodeKind::Special) => crate::dfs::InodeKind::Special(
            domain_dfs_special_node(required(inode.special_node, "DfsInodeRecord.special_node")?)?,
        ),
        _ => {
            return Err(Error::coded(
                afs_error::CLIENT_PROTOCOL_VIOLATION,
                "DfsInodeRecord has an invalid inode kind",
            ));
        }
    };
    Ok(crate::dfs::InodeRecord {
        namespace_id: crate::dfs::NamespaceId::new(inode.namespace_id),
        inode_id: crate::dfs::InodeId::new(inode.inode_id),
        kind,
        attributes: crate::dfs::InodeAttributes {
            mode: attributes.mode,
            uid: attributes.uid,
            gid: attributes.gid,
            nlink: attributes.nlink,
            atime_unix_ms: attributes.atime_unix_ms,
            mtime_unix_ms: attributes.mtime_unix_ms,
            ctime_unix_ms: attributes.ctime_unix_ms,
        },
        head_version: (!inode.head_version_id.is_empty())
            .then(|| crate::dfs::FileVersionId::new(inode.head_version_id)),
        symlink_target: (!inode.symlink_target.is_empty()).then_some(inode.symlink_target),
        xattrs: inode
            .xattrs
            .into_iter()
            .map(|record| (record.name, record.value))
            .collect(),
        revision: inode.revision,
    })
}

#[cfg(feature = "dfs")]
fn domain_dfs_special_node(
    special: afs_protocol::meta::DfsSpecialNode,
) -> afs_error::Result<crate::dfs::SpecialNodeKind> {
    match afs_protocol::meta::DfsSpecialNodeKind::try_from(special.kind) {
        Ok(afs_protocol::meta::DfsSpecialNodeKind::Fifo) if special.rdev == 0 => {
            Ok(crate::dfs::SpecialNodeKind::Fifo)
        }
        Ok(afs_protocol::meta::DfsSpecialNodeKind::Socket) if special.rdev == 0 => {
            Ok(crate::dfs::SpecialNodeKind::Socket)
        }
        Ok(afs_protocol::meta::DfsSpecialNodeKind::BlockDevice) => {
            Ok(crate::dfs::SpecialNodeKind::BlockDevice { rdev: special.rdev })
        }
        Ok(afs_protocol::meta::DfsSpecialNodeKind::CharDevice) => {
            Ok(crate::dfs::SpecialNodeKind::CharDevice { rdev: special.rdev })
        }
        _ => Err(Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "DfsSpecialNode has an invalid kind/rdev combination",
        )),
    }
}

#[cfg(feature = "dfs")]
fn wire_dfs_special_node(kind: crate::dfs::SpecialNodeKind) -> afs_protocol::meta::DfsSpecialNode {
    let (kind, rdev) = match kind {
        crate::dfs::SpecialNodeKind::Fifo => (afs_protocol::meta::DfsSpecialNodeKind::Fifo, 0),
        crate::dfs::SpecialNodeKind::Socket => (afs_protocol::meta::DfsSpecialNodeKind::Socket, 0),
        crate::dfs::SpecialNodeKind::BlockDevice { rdev } => {
            (afs_protocol::meta::DfsSpecialNodeKind::BlockDevice, rdev)
        }
        crate::dfs::SpecialNodeKind::CharDevice { rdev } => {
            (afs_protocol::meta::DfsSpecialNodeKind::CharDevice, rdev)
        }
    };
    afs_protocol::meta::DfsSpecialNode {
        kind: kind.into(),
        rdev,
    }
}

#[cfg(feature = "dfs")]
fn wire_dfs_version(version: crate::dfs::FileVersion) -> afs_protocol::meta::DfsFileVersion {
    afs_protocol::meta::DfsFileVersion {
        version_id: version.id.0,
        inode_id: version.inode_id.0,
        parent_version_id: version.parent_version.map_or_else(String::new, |id| id.0),
        length: version.length,
        layout_root_id: version.layout_root.0,
        created_at_unix_ms: version.created_at_unix_ms,
    }
}

#[cfg(feature = "dfs")]
fn domain_dfs_version(version: afs_protocol::meta::DfsFileVersion) -> crate::dfs::FileVersion {
    crate::dfs::FileVersion {
        id: crate::dfs::FileVersionId::new(version.version_id),
        inode_id: crate::dfs::InodeId::new(version.inode_id),
        parent_version: (!version.parent_version_id.is_empty())
            .then(|| crate::dfs::FileVersionId::new(version.parent_version_id)),
        length: version.length,
        layout_root: crate::dfs::LayoutRootId::new(version.layout_root_id),
        created_at_unix_ms: version.created_at_unix_ms,
    }
}

#[cfg(feature = "dfs")]
fn wire_dfs_layout(layout: crate::dfs::LayoutRoot) -> afs_protocol::meta::DfsLayoutRoot {
    afs_protocol::meta::DfsLayoutRoot {
        layout_root_id: layout.id.0,
        file_length: layout.file_length,
        inline_extents: layout
            .inline_extents
            .into_iter()
            .map(|extent| afs_protocol::meta::DfsExtent {
                file_offset: extent.file_offset,
                length: extent.length,
                chunk_id: extent.chunk_id.0,
                chunk_offset: extent.chunk_offset,
            })
            .collect(),
    }
}

#[cfg(feature = "dfs")]
fn domain_dfs_layout(layout: afs_protocol::meta::DfsLayoutRoot) -> crate::dfs::LayoutRoot {
    crate::dfs::LayoutRoot {
        id: crate::dfs::LayoutRootId::new(layout.layout_root_id),
        file_length: layout.file_length,
        inline_extents: layout
            .inline_extents
            .into_iter()
            .map(|extent| crate::dfs::Extent {
                file_offset: extent.file_offset,
                length: extent.length,
                chunk_id: crate::dfs::ChunkId::new(extent.chunk_id),
                chunk_offset: extent.chunk_offset,
            })
            .collect(),
    }
}

#[cfg(feature = "dfs")]
fn wire_dfs_write_lease(lease: crate::dfs::WriteLease) -> afs_protocol::meta::DfsWriteLease {
    afs_protocol::meta::DfsWriteLease {
        inode_id: lease.inode_id.0,
        owner_node_id: lease.owner_node_id,
        owner_session_id: lease.owner_session_id,
        lease_epoch: lease.lease_epoch,
        expires_at_unix_ms: lease.expires_at_unix_ms,
    }
}

#[cfg(feature = "dfs")]
fn domain_dfs_write_lease(lease: afs_protocol::meta::DfsWriteLease) -> crate::dfs::WriteLease {
    crate::dfs::WriteLease {
        inode_id: crate::dfs::InodeId::new(lease.inode_id),
        owner_node_id: lease.owner_node_id,
        owner_session_id: lease.owner_session_id,
        lease_epoch: lease.lease_epoch,
        expires_at_unix_ms: lease.expires_at_unix_ms,
    }
}

#[cfg(feature = "dfs")]
fn wire_dfs_metadata_delta(
    delta: crate::dfs::CommitMetadataDelta,
) -> afs_protocol::meta::DfsCommitMetadataDelta {
    afs_protocol::meta::DfsCommitMetadataDelta {
        mode: match delta.mode {
            crate::dfs::CommitMetadataMode::DataOnly => {
                afs_protocol::meta::DfsCommitMetadataMode::DataOnly as i32
            }
            crate::dfs::CommitMetadataMode::Full => {
                afs_protocol::meta::DfsCommitMetadataMode::Full as i32
            }
        },
        mtime_unix_ms: delta.mtime_unix_ms.unwrap_or_default(),
        ctime_unix_ms: delta.ctime_unix_ms.unwrap_or_default(),
        kill_suidgid: delta.kill_suidgid,
    }
}

#[cfg(feature = "dfs")]
fn wire_validate_replica_write_request(
    request: crate::dfs::ValidateReplicaWriteRequest,
) -> afs_protocol::meta::ValidateDfsReplicaWriteRequest {
    afs_protocol::meta::ValidateDfsReplicaWriteRequest {
        requester_node_id: request.requester_node_id,
        requester_node_epoch: request.requester_node_epoch,
        initiator_node_id: request.initiator_node_id,
        initiator_node_epoch: request.initiator_node_epoch,
        operation_id: request.operation_id.0,
        chunk_id: request.chunk_id.0,
        chunk_length: request.chunk_length,
        content_digest: request.content_digest.bytes.to_vec(),
        content_digest_algorithm: wire_digest_algorithm(request.content_digest.algorithm),
        placement_revision: request.placement_revision,
        placement_epoch: request.placement_epoch,
        replica_group_id: request.replica_group_id.0,
        target_index: request.target_index,
        ordered_targets: request
            .ordered_targets
            .into_iter()
            .map(wire_replica_target)
            .collect(),
        repair_claim: request.repair_claim.as_ref().map(wire_replication_claim),
    }
}

#[cfg(feature = "dfs")]
fn wire_replica_target(target: crate::dfs::ReplicaTarget) -> afs_protocol::meta::DfsReplicaTarget {
    afs_protocol::meta::DfsReplicaTarget {
        node_id: target.node_id,
        node_epoch: target.node_epoch,
        data_endpoint: target.data_endpoint,
        device: Some(afs_protocol::meta::DfsStorageDevice {
            device_id: target.device.device_id,
            device_epoch: target.device.device_epoch,
            catalog_revision: target.device.catalog_revision,
            failure_domain: target.device.failure_domain,
        }),
    }
}

#[cfg(feature = "dfs")]
fn wire_chunk_receipt(receipt: crate::dfs::ChunkReceipt) -> afs_protocol::meta::DfsChunkReceipt {
    afs_protocol::meta::DfsChunkReceipt {
        operation_id: receipt.operation_id.0,
        chunk_id: receipt.chunk.id.0,
        chunk_length: receipt.chunk.length,
        content_digest: receipt.chunk.content_digest.bytes.to_vec(),
        content_digest_algorithm: wire_digest_algorithm(receipt.chunk.content_digest.algorithm),
        placement_revision: receipt.placement_revision,
        placement_epoch: receipt.placement_epoch,
        replica_group_id: receipt.replica_group_id.0,
        external_commit: receipt.external_commit.map(|external| {
            afs_protocol::meta::DfsExternalCommitReceipt {
                store_id: external.store_id,
                generation: external.generation,
                object_key: external.object_key,
                persisted_bytes: external.persisted_bytes,
                verified_digest: external.verified_digest.bytes.to_vec(),
                verified_digest_algorithm: wire_digest_algorithm(
                    external.verified_digest.algorithm,
                ),
            }
        }),
        durable_acks: receipt
            .durable_acks
            .into_iter()
            .map(wire_replica_ack)
            .collect(),
    }
}

#[cfg(feature = "dfs")]
fn wire_replica_ack(ack: crate::dfs::ReplicaAck) -> afs_protocol::meta::DfsReplicaAck {
    afs_protocol::meta::DfsReplicaAck {
        operation_id: ack.operation_id.0,
        chunk_id: ack.chunk_id.0,
        placement_revision: ack.placement_revision,
        placement_epoch: ack.placement_epoch,
        node_id: ack.node_id,
        node_epoch: ack.node_epoch,
        device_id: ack.device_id,
        device_epoch: ack.device_epoch,
        catalog_revision: ack.catalog_revision,
        persisted_bytes: ack.persisted_bytes,
        verified_digest: ack.verified_digest.bytes.to_vec(),
        verified_digest_algorithm: wire_digest_algorithm(ack.verified_digest.algorithm),
    }
}

#[cfg(feature = "dfs")]
fn wire_replication_config(
    replication: crate::dfs::ReplicationConfig,
) -> afs_protocol::meta::DfsReplicationConfig {
    afs_protocol::meta::DfsReplicationConfig {
        desired_copies: u32::from(replication.desired_copies),
        sync_required_copies: u32::from(replication.sync_required_copies),
        min_distinct_nodes: u32::from(replication.min_distinct_nodes),
        min_distinct_failure_domains: u32::from(replication.min_distinct_failure_domains),
        local_copy: match replication.local_copy {
            crate::dfs::LocalCopyPolicy::Required => {
                afs_protocol::meta::DfsLocalCopyPolicy::Required as i32
            }
            crate::dfs::LocalCopyPolicy::Preferred => {
                afs_protocol::meta::DfsLocalCopyPolicy::Preferred as i32
            }
            crate::dfs::LocalCopyPolicy::NotRequired => {
                afs_protocol::meta::DfsLocalCopyPolicy::NotRequired as i32
            }
        },
    }
}

#[cfg(feature = "dfs")]
fn wire_replica_group(group: crate::dfs::ReplicaGroup) -> afs_protocol::meta::DfsReplicaGroup {
    afs_protocol::meta::DfsReplicaGroup {
        replica_group_id: group.id.0,
        placement_epoch: group.placement_epoch,
        targets: group.targets.into_iter().map(wire_replica_target).collect(),
    }
}

#[cfg(feature = "dfs")]
fn wire_chunk_object(chunk: crate::dfs::ChunkObject) -> afs_protocol::meta::DfsChunkObject {
    afs_protocol::meta::DfsChunkObject {
        chunk_id: chunk.id.0,
        length: chunk.length,
        content_digest: chunk.content_digest.bytes.to_vec(),
        content_digest_algorithm: wire_digest_algorithm(chunk.content_digest.algorithm),
    }
}

#[cfg(feature = "dfs")]
pub(crate) fn wire_replication_claim(
    claim: &crate::dfs::ReplicationClaim,
) -> afs_protocol::meta::DfsReplicationClaim {
    afs_protocol::meta::DfsReplicationClaim {
        task_id: claim.task_id.0.clone(),
        operation_id: claim.operation_id.0.clone(),
        worker_node_id: claim.worker_node_id.clone(),
        worker_node_epoch: claim.worker_node_epoch,
        worker_session_id: claim.worker_session_id.clone(),
        expires_at_unix_ms: claim.expires_at_unix_ms,
        fence: claim.fence,
        chunk: Some(wire_chunk_object(claim.chunk.clone())),
        source_copy_id: claim.source_copy_id.0.clone(),
        placement_revision: claim.placement_revision,
        replica_group: Some(wire_replica_group(claim.replica_group.clone())),
        replication: Some(wire_replication_config(claim.replication.clone())),
    }
}

#[cfg(feature = "dfs")]
fn wire_digest_algorithm(algorithm: crate::dfs::DigestAlgorithm) -> i32 {
    match algorithm {
        crate::dfs::DigestAlgorithm::Blake3 => {
            afs_protocol::meta::DfsDigestAlgorithm::Blake3 as i32
        }
        crate::dfs::DigestAlgorithm::Crc32cSegmentsV1 => {
            afs_protocol::meta::DfsDigestAlgorithm::Crc32cSegmentsV1 as i32
        }
    }
}

#[cfg(feature = "dfs")]
fn domain_digest_algorithm(value: i32) -> afs_error::Result<crate::dfs::DigestAlgorithm> {
    match afs_protocol::meta::DfsDigestAlgorithm::try_from(value) {
        Ok(afs_protocol::meta::DfsDigestAlgorithm::Blake3) => {
            Ok(crate::dfs::DigestAlgorithm::Blake3)
        }
        Ok(afs_protocol::meta::DfsDigestAlgorithm::Crc32cSegmentsV1) => {
            Ok(crate::dfs::DigestAlgorithm::Crc32cSegmentsV1)
        }
        _ => Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "DFS digest algorithm is invalid",
        )),
    }
}

#[cfg(feature = "dfs")]
fn domain_replica_write_grant(
    grant: afs_protocol::meta::DfsReplicaWriteGrant,
) -> afs_error::Result<crate::dfs::ReplicaWriteGrant> {
    let digest: [u8; 32] = grant.content_digest.try_into().map_err(|_| {
        Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "DfsReplicaWriteGrant digest must contain 32 bytes",
        )
    })?;
    Ok(crate::dfs::ReplicaWriteGrant {
        requester_node_id: grant.requester_node_id,
        requester_node_epoch: grant.requester_node_epoch,
        initiator_node_id: grant.initiator_node_id,
        initiator_node_epoch: grant.initiator_node_epoch,
        operation_id: crate::dfs::OperationId::new(grant.operation_id),
        chunk_id: crate::dfs::ChunkId::new(grant.chunk_id),
        chunk_length: grant.chunk_length,
        content_digest: crate::dfs::ContentDigest {
            algorithm: domain_digest_algorithm(grant.content_digest_algorithm)?,
            bytes: digest,
        },
        placement_revision: grant.placement_revision,
        placement_epoch: grant.placement_epoch,
        replica_group_id: crate::dfs::ReplicaGroupId::new(grant.replica_group_id),
        target_index: grant.target_index,
        replication: domain_replication_config(required(
            grant.replication,
            "DfsReplicaWriteGrant.replication",
        )?)?,
        replica_group: domain_replica_group(required(
            grant.replica_group,
            "DfsReplicaWriteGrant.replica_group",
        )?)?,
        expires_at_unix_ms: grant.expires_at_unix_ms,
        fence: grant.fence,
        token: grant.token,
    })
}

#[cfg(feature = "dfs")]
fn domain_chunk_object(
    chunk: afs_protocol::meta::DfsChunkObject,
) -> afs_error::Result<crate::dfs::ChunkObject> {
    let digest: [u8; 32] = chunk.content_digest.try_into().map_err(|_| {
        Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "DfsChunkObject digest must contain 32 bytes",
        )
    })?;
    Ok(crate::dfs::ChunkObject {
        id: crate::dfs::ChunkId::new(chunk.chunk_id),
        length: chunk.length,
        content_digest: crate::dfs::ContentDigest {
            algorithm: domain_digest_algorithm(chunk.content_digest_algorithm)?,
            bytes: digest,
        },
        encoding: crate::dfs::ChunkEncoding::Raw,
    })
}

#[cfg(feature = "dfs")]
pub(crate) fn domain_replication_claim(
    claim: afs_protocol::meta::DfsReplicationClaim,
) -> afs_error::Result<crate::dfs::ReplicationClaim> {
    Ok(crate::dfs::ReplicationClaim {
        task_id: crate::dfs::ReplicationTaskId::new(claim.task_id),
        operation_id: crate::dfs::OperationId::new(claim.operation_id),
        worker_node_id: claim.worker_node_id,
        worker_node_epoch: claim.worker_node_epoch,
        worker_session_id: claim.worker_session_id,
        expires_at_unix_ms: claim.expires_at_unix_ms,
        fence: claim.fence,
        chunk: domain_chunk_object(required(claim.chunk, "DfsReplicationClaim.chunk")?)?,
        source_copy_id: crate::dfs::CopyId::new(claim.source_copy_id),
        placement_revision: claim.placement_revision,
        replica_group: domain_replica_group(required(
            claim.replica_group,
            "DfsReplicationClaim.replica_group",
        )?)?,
        replication: domain_replication_config(required(
            claim.replication,
            "DfsReplicationClaim.replication",
        )?)?,
    })
}

#[cfg(feature = "dfs")]
fn domain_replication_task(
    task: afs_protocol::meta::DfsReplicationTask,
) -> afs_error::Result<crate::dfs::ReplicationTask> {
    let state = match afs_protocol::meta::DfsReplicationTaskState::try_from(task.state) {
        Ok(afs_protocol::meta::DfsReplicationTaskState::Pending) => {
            crate::dfs::ReplicationTaskState::Pending
        }
        Ok(afs_protocol::meta::DfsReplicationTaskState::Running) => {
            crate::dfs::ReplicationTaskState::Running
        }
        Ok(afs_protocol::meta::DfsReplicationTaskState::RetryWaiting) => {
            crate::dfs::ReplicationTaskState::RetryWaiting
        }
        Ok(afs_protocol::meta::DfsReplicationTaskState::Completed) => {
            crate::dfs::ReplicationTaskState::Completed
        }
        Ok(afs_protocol::meta::DfsReplicationTaskState::BlockedNoSource) => {
            crate::dfs::ReplicationTaskState::BlockedNoSource
        }
        _ => {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "DfsReplicationTask has an invalid state",
            ));
        }
    };
    Ok(crate::dfs::ReplicationTask {
        id: crate::dfs::ReplicationTaskId::new(task.task_id),
        chunk_id: crate::dfs::ChunkId::new(task.chunk_id),
        placement_epoch: task.placement_epoch,
        desired_copies: u16::try_from(task.desired_copies).map_err(|_| {
            Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "DfsReplicationTask desired_copies exceeds u16",
            )
        })?,
        existing_copies: task
            .existing_copy_ids
            .into_iter()
            .map(crate::dfs::CopyId::new)
            .collect(),
        state,
        attempt: task.attempt,
        next_retry_unix_ms: task.next_retry_unix_ms,
        last_error: task.last_error,
        claim: task
            .claim
            .map(domain_replication_claim)
            .transpose()?
            .map(Box::new),
    })
}

#[cfg(feature = "dfs")]
fn domain_replication_config(
    replication: afs_protocol::meta::DfsReplicationConfig,
) -> afs_error::Result<crate::dfs::ReplicationConfig> {
    let local_copy = match afs_protocol::meta::DfsLocalCopyPolicy::try_from(replication.local_copy)
    {
        Ok(afs_protocol::meta::DfsLocalCopyPolicy::Required) => {
            crate::dfs::LocalCopyPolicy::Required
        }
        Ok(afs_protocol::meta::DfsLocalCopyPolicy::Preferred) => {
            crate::dfs::LocalCopyPolicy::Preferred
        }
        Ok(afs_protocol::meta::DfsLocalCopyPolicy::NotRequired) => {
            crate::dfs::LocalCopyPolicy::NotRequired
        }
        _ => {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "DFS replication config has an invalid local-copy policy",
            ));
        }
    };
    let replication = crate::dfs::ReplicationConfig {
        desired_copies: u16::try_from(replication.desired_copies)
            .map_err(|_| Error::coded(CLIENT_PROTOCOL_VIOLATION, "desired_copies exceeds u16"))?,
        sync_required_copies: u16::try_from(replication.sync_required_copies).map_err(|_| {
            Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "sync_required_copies exceeds u16",
            )
        })?,
        min_distinct_nodes: u16::try_from(replication.min_distinct_nodes).map_err(|_| {
            Error::coded(CLIENT_PROTOCOL_VIOLATION, "min_distinct_nodes exceeds u16")
        })?,
        min_distinct_failure_domains: u16::try_from(replication.min_distinct_failure_domains)
            .map_err(|_| {
                Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "min_distinct_failure_domains exceeds u16",
                )
            })?,
        local_copy,
    };
    if !replication.is_valid() {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "Meta returned an invalid DFS replication config",
        ));
    }
    Ok(replication)
}

#[cfg(feature = "dfs")]
fn domain_replica_group(
    group: afs_protocol::meta::DfsReplicaGroup,
) -> afs_error::Result<crate::dfs::ReplicaGroup> {
    Ok(crate::dfs::ReplicaGroup {
        id: crate::dfs::ReplicaGroupId::new(group.replica_group_id),
        placement_epoch: group.placement_epoch,
        targets: group
            .targets
            .into_iter()
            .map(domain_replica_target)
            .collect::<afs_error::Result<Vec<_>>>()?,
    })
}

#[cfg(feature = "dfs")]
fn domain_replica_target(
    target: afs_protocol::meta::DfsReplicaTarget,
) -> afs_error::Result<crate::dfs::ReplicaTarget> {
    let device = required(target.device, "DfsReplicaTarget.device")?;
    Ok(crate::dfs::ReplicaTarget {
        node_id: target.node_id,
        node_epoch: target.node_epoch,
        data_endpoint: target.data_endpoint,
        device: crate::dfs::StorageDeviceDescriptor {
            device_id: device.device_id,
            device_epoch: device.device_epoch,
            catalog_revision: device.catalog_revision,
            failure_domain: device.failure_domain,
        },
    })
}

#[cfg(feature = "dfs")]
fn domain_placement_snapshot(
    snapshot: afs_protocol::meta::DfsPlacementSnapshot,
) -> afs_error::Result<crate::dfs::PlacementSnapshot> {
    Ok(crate::dfs::PlacementSnapshot {
        revision: snapshot.revision,
        replication: domain_replication_config(required(
            snapshot.replication,
            "DfsPlacementSnapshot.replication",
        )?)?,
        replica_groups: snapshot
            .replica_groups
            .into_iter()
            .map(domain_replica_group)
            .collect::<afs_error::Result<Vec<_>>>()?,
    })
}

#[cfg(feature = "dfs")]
fn wire_read_validation(
    validation: crate::dfs::DfsReadValidation,
) -> afs_protocol::meta::DfsReadValidation {
    let grant = validation.grant;
    afs_protocol::meta::DfsReadValidation {
        grant: Some(afs_protocol::meta::DfsReadGrant {
            namespace_id: grant.namespace_id.0,
            file_version_id: grant.file_version_id.0,
            layout_root_id: grant.layout_root_id.0,
            caller_node_id: grant.caller_node_id,
            caller_node_epoch: grant.caller_node_epoch,
            expires_at_unix_ms: grant.expires_at_unix_ms,
            fence: grant.fence,
            token: grant.token,
        }),
        chunk_id: validation.chunk_id.0,
        copy_id: validation.copy_id.0,
        chunk_offset: validation.chunk_offset,
        length: validation.length,
    }
}

#[cfg(feature = "dfs")]
fn current_external_read_is_unimplemented(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unimplemented
}

#[cfg(feature = "dfs")]
fn domain_authorize_current_external_read_reply(
    reply: afs_protocol::meta::AuthorizeCurrentExternalReadReply,
) -> afs_error::Result<crate::dfs::AuthorizeCurrentExternalReadReply> {
    let inode = domain_dfs_inode(required(reply.inode, "AuthorizeCurrentExternalRead.inode")?)?;
    if reply.head_changed {
        if !reply.authorizations.is_empty() {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "head-changed current external read must not include authorizations",
            ));
        }
        return Ok(crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(
            inode,
        ));
    }
    let authorizations = reply
        .authorizations
        .into_iter()
        .map(domain_external_authorized_read)
        .collect::<afs_error::Result<Vec<_>>>()?;
    Ok(crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
        inode,
        authorizations,
    })
}

#[cfg(feature = "dfs")]
fn domain_authorize_external_version_read_reply(
    reply: afs_protocol::meta::AuthorizeExternalVersionReadReply,
) -> afs_error::Result<crate::dfs::AuthorizeExternalVersionReadReply> {
    let authorizations = reply
        .authorizations
        .into_iter()
        .map(domain_external_authorized_read)
        .collect::<afs_error::Result<Vec<_>>>()?;
    if !authorizations.is_empty() && reply.remaining_validity_ms == 0 {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "external version read authorization must include a positive remaining validity",
        ));
    }
    if reply.remaining_validity_ms > 5_000 {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "external version read validity exceeds the supported bound",
        ));
    }
    Ok(crate::dfs::AuthorizeExternalVersionReadReply {
        authorizations,
        remaining_validity_ms: reply.remaining_validity_ms,
    })
}

#[cfg(feature = "dfs")]
fn domain_external_authorized_read(
    entry: afs_protocol::meta::DfsExternalAuthorizedRead,
) -> afs_error::Result<crate::dfs::DfsExternalAuthorizedRead> {
    let authorized = required(entry.authorized, "ExternalReadAuthorization.authorized")?;
    let validation = required(
        authorized.validation,
        "ExternalReadAuthorization.validation",
    )?;
    let chunk = domain_chunk_object(required(entry.chunk, "ExternalReadAuthorization.chunk")?)?;
    let location = domain_dfs_copy_location(required(
        entry.location,
        "ExternalReadAuthorization.location",
    )?)?;
    let crate::dfs::CopyLocation::External {
        store_id,
        object_key,
        object_revision,
    } = &location
    else {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "external authorization requires external location",
        ));
    };
    let expected_key = crate::dfs::external_object_key(store_id, object_revision, &chunk.id)
        .map_err(|error| Error::coded(CLIENT_PROTOCOL_VIOLATION, error.to_string()))?;
    if chunk.id.0 != validation.chunk_id || *object_key != expected_key {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "external authorization identity mismatch",
        ));
    }
    Ok(crate::dfs::DfsExternalAuthorizedRead {
        authorized: crate::dfs::DfsAuthorizedRead {
            validation: crate::dfs::DfsReadValidation {
                grant: domain_dfs_read_grant(required(
                    validation.grant,
                    "ExternalReadAuthorization.grant",
                )?),
                chunk_id: crate::dfs::ChunkId::new(validation.chunk_id),
                copy_id: crate::dfs::CopyId::new(validation.copy_id),
                chunk_offset: validation.chunk_offset,
                length: validation.length,
            },
            allowed_ranges: authorized
                .allowed_ranges
                .into_iter()
                .map(|range| (range.offset, range.length))
                .collect(),
            expires_at_unix_ms: authorized.expires_at_unix_ms,
        },
        chunk,
        location,
    })
}

#[cfg(feature = "dfs")]
fn domain_dfs_chunk_sources(
    value: afs_protocol::meta::DfsChunkSources,
) -> afs_error::Result<crate::dfs::ChunkSources> {
    let chunk_id = crate::dfs::ChunkId::new(value.chunk_id);
    let chunk = value.chunk.map(domain_chunk_object).transpose()?;
    if chunk.as_ref().is_some_and(|object| object.id != chunk_id) {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "chunk source object identity mismatch",
        ));
    }
    if value.allowed_ranges.len() > 128
        || value.allowed_ranges.iter().any(|range| {
            range.length == 0
                || chunk.as_ref().is_none_or(|object| {
                    range
                        .offset
                        .checked_add(range.length)
                        .is_none_or(|end| end > object.length)
                })
        })
    {
        return Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "chunk source grant ranges exceed the bounded object identity",
        ));
    }
    Ok(crate::dfs::ChunkSources {
        chunk_id,
        chunk,
        sources: value
            .sources
            .into_iter()
            .map(domain_dfs_source_candidate)
            .collect::<afs_error::Result<Vec<_>>>()?,
        allowed_ranges: value
            .allowed_ranges
            .into_iter()
            .map(|range| (range.offset, range.length))
            .collect(),
    })
}

#[cfg(feature = "dfs")]
fn domain_dfs_source_candidate(
    value: afs_protocol::meta::DfsSourceCandidate,
) -> afs_error::Result<crate::dfs::SourceCandidate> {
    let location =
        domain_dfs_copy_location(required(value.location, "DfsSourceCandidate.location")?)?;
    match &location {
        crate::dfs::CopyLocation::Node { .. }
            if value.data_endpoint.as_deref().is_none_or(str::is_empty) =>
        {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "Node source requires a Peer endpoint",
            ));
        }
        crate::dfs::CopyLocation::External { .. } if value.data_endpoint.is_some() => {
            return Err(Error::coded(
                CLIENT_PROTOCOL_VIOLATION,
                "External source cannot carry a Peer endpoint",
            ));
        }
        _ => {}
    }
    Ok(crate::dfs::SourceCandidate {
        copy_id: crate::dfs::CopyId::new(value.copy_id),
        chunk_id: crate::dfs::ChunkId::new(value.chunk_id),
        role: domain_dfs_copy_role(value.role)?,
        state: domain_dfs_copy_state(value.state)?,
        location,
        data_endpoint: value.data_endpoint,
        load_hint: value.load_hint,
        read_grant: domain_dfs_read_grant(required(
            value.read_grant,
            "DfsSourceCandidate.read_grant",
        )?),
    })
}

#[cfg(feature = "dfs")]
fn domain_dfs_copy_role(value: i32) -> afs_error::Result<crate::dfs::CopyRole> {
    match afs_protocol::meta::DfsCopyRole::try_from(value) {
        Ok(afs_protocol::meta::DfsCopyRole::DurableReplica) => {
            Ok(crate::dfs::CopyRole::DurableReplica)
        }
        Ok(afs_protocol::meta::DfsCopyRole::VerifiedCache) => {
            Ok(crate::dfs::CopyRole::VerifiedCache)
        }
        Ok(afs_protocol::meta::DfsCopyRole::ExternalCommitted) => {
            Ok(crate::dfs::CopyRole::ExternalCommitted)
        }
        _ => Err(Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "DfsSourceCandidate has an invalid copy role",
        )),
    }
}

#[cfg(feature = "dfs")]
fn domain_dfs_copy_state(value: i32) -> afs_error::Result<crate::dfs::CopyState> {
    match afs_protocol::meta::DfsCopyState::try_from(value) {
        Ok(afs_protocol::meta::DfsCopyState::Ready) => Ok(crate::dfs::CopyState::Ready),
        Ok(afs_protocol::meta::DfsCopyState::Corrupt) => Ok(crate::dfs::CopyState::Corrupt),
        Ok(afs_protocol::meta::DfsCopyState::Deleting) => Ok(crate::dfs::CopyState::Deleting),
        _ => Err(Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "DfsSourceCandidate has an invalid copy state",
        )),
    }
}

#[cfg(feature = "dfs")]
fn domain_dfs_copy_location(
    value: afs_protocol::meta::DfsCopyLocation,
) -> afs_error::Result<crate::dfs::CopyLocation> {
    match value.location {
        Some(afs_protocol::meta::dfs_copy_location::Location::Node(node)) => {
            Ok(crate::dfs::CopyLocation::Node {
                node_id: node.node_id,
                node_epoch: node.node_epoch,
                device_id: node.device_id,
                device_epoch: node.device_epoch,
                catalog_revision: node.catalog_revision,
            })
        }
        Some(afs_protocol::meta::dfs_copy_location::Location::External(external)) => {
            Ok(crate::dfs::CopyLocation::External {
                store_id: external.store_id,
                object_key: external.object_key,
                object_revision: external.object_revision,
            })
        }
        None => Err(Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "DfsCopyLocation is empty",
        )),
    }
}

#[cfg(feature = "dfs")]
fn domain_dfs_read_grant(value: afs_protocol::meta::DfsReadGrant) -> crate::dfs::DfsReadGrant {
    crate::dfs::DfsReadGrant {
        namespace_id: crate::dfs::NamespaceId::new(value.namespace_id),
        file_version_id: crate::dfs::FileVersionId::new(value.file_version_id),
        layout_root_id: crate::dfs::LayoutRootId::new(value.layout_root_id),
        caller_node_id: value.caller_node_id,
        caller_node_epoch: value.caller_node_epoch,
        expires_at_unix_ms: value.expires_at_unix_ms,
        fence: value.fence,
        token: value.token,
    }
}

#[cfg(feature = "ownerfs")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootCommandType {
    RevokeAccess,
}

#[cfg(feature = "ownerfs")]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RootCommand {
    pub(crate) command_id: String,
    pub(crate) command_type: RootCommandType,
    pub(crate) revision: u64,
    pub(crate) root_id: String,
    pub(crate) root_epoch: u64,
    pub(crate) home_node_id: String,
    pub(crate) home_session_id: String,
    pub(crate) access_generation: u64,
}

#[cfg(feature = "ownerfs")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RootCommandRecoveryReason {
    WatchCompacted,
    NodeSessionRestarted,
    BackendLeaderChanged,
}

#[cfg(feature = "ownerfs")]
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RootCommandBatch {
    Events {
        start_revision: u64,
        next_revision: u64,
        commands: Vec<RootCommand>,
    },
    Compacted {
        requested_after: u64,
        compacted_to: u64,
        recovery_resume_after: u64,
        recovery_reason: RootCommandRecoveryReason,
    },
    Unsupported {
        message: String,
    },
}

#[cfg(feature = "ownerfs")]
fn root_command_batch_from_wire(
    requested_after: u64,
    value: afs_protocol::meta::RootCommandBatchReply,
) -> Result<RootCommandBatch> {
    match value.result {
        Some(afs_protocol::meta::root_command_batch_reply::Result::Events(events)) => {
            if events.start_revision != requested_after {
                return Err(Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "RootCommandBatchEvents start revision does not match request",
                ));
            }
            if events.next_revision == 0 || events.next_revision <= events.start_revision {
                return Err(Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "RootCommandBatchEvents has invalid next revision",
                ));
            }
            let mut seen_ids = std::collections::BTreeSet::new();
            let mut last_revision = events.start_revision;
            let commands = events
                .commands
                .into_iter()
                .map(|command| {
                    if command.command_id.is_empty() || !seen_ids.insert(command.command_id.clone())
                    {
                        return Err(Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommand has an empty or duplicate command id",
                        ));
                    }
                    if command.revision < last_revision {
                        return Err(Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommand revisions are not ordered",
                        ));
                    }
                    last_revision = command.revision;
                    if command.revision <= events.start_revision
                        || command.revision >= events.next_revision
                    {
                        return Err(Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommand revision is outside the batch range",
                        ));
                    }
                    let command_type =
                        afs_protocol::meta::RootCommandType::try_from(command.command_type)
                            .map_err(|_| {
                                Error::coded(
                                    CLIENT_PROTOCOL_VIOLATION,
                                    "RootCommand has an unknown command type",
                                )
                            })?;
                    let command_type = match command_type {
                        afs_protocol::meta::RootCommandType::RevokeAccess => {
                            RootCommandType::RevokeAccess
                        }
                        afs_protocol::meta::RootCommandType::InvalidateCache => {
                            return Err(Error::coded(
                                CLIENT_PROTOCOL_VIOLATION,
                                "RootCommand InvalidateCache is not implemented by OwnerFs",
                            ));
                        }
                        afs_protocol::meta::RootCommandType::Unspecified => {
                            return Err(Error::coded(
                                CLIENT_PROTOCOL_VIOLATION,
                                "RootCommand has unspecified command type",
                            ));
                        }
                    };
                    let access = command.access.ok_or_else(|| {
                        Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommand is missing root access",
                        )
                    })?;
                    if access.root_id.is_empty()
                        || access.root_epoch == 0
                        || access.home_node_id.is_empty()
                        || access.home_session_id.is_empty()
                        || access.access_generation == 0
                    {
                        return Err(Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommand has invalid root identity",
                        ));
                    }
                    Ok(RootCommand {
                        command_id: command.command_id,
                        command_type,
                        revision: command.revision,
                        root_id: access.root_id,
                        root_epoch: access.root_epoch,
                        home_node_id: access.home_node_id,
                        home_session_id: access.home_session_id,
                        access_generation: access.access_generation,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(RootCommandBatch::Events {
                start_revision: events.start_revision,
                next_revision: events.next_revision,
                commands,
            })
        }
        Some(afs_protocol::meta::root_command_batch_reply::Result::Compacted(compacted)) => {
            let cursor = compacted.recovery_cursor.ok_or_else(|| {
                Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "RootCommandBatchCompacted is missing recovery cursor",
                )
            })?;
            // This private consumer accepts forward compacted-history diagnostics
            // only. Other recovery shapes stay unsupported, never authoritative.
            if compacted.requested_after != requested_after
                || compacted.compacted_to <= compacted.requested_after
                || cursor.resume_after <= compacted.requested_after
            {
                return Err(Error::coded(
                    CLIENT_PROTOCOL_VIOLATION,
                    "RootCommandBatchCompacted has invalid diagnostic cursor",
                ));
            }
            let recovery_reason =
                match afs_protocol::meta::RootCommandRecoveryReason::try_from(cursor.reason) {
                    Ok(afs_protocol::meta::RootCommandRecoveryReason::WatchCompacted) => {
                        RootCommandRecoveryReason::WatchCompacted
                    }
                    Ok(afs_protocol::meta::RootCommandRecoveryReason::NodeSessionRestarted) => {
                        RootCommandRecoveryReason::NodeSessionRestarted
                    }
                    Ok(afs_protocol::meta::RootCommandRecoveryReason::BackendLeaderChanged) => {
                        RootCommandRecoveryReason::BackendLeaderChanged
                    }
                    Ok(afs_protocol::meta::RootCommandRecoveryReason::Unspecified) | Err(_) => {
                        return Err(Error::coded(
                            CLIENT_PROTOCOL_VIOLATION,
                            "RootCommandBatchCompacted has invalid recovery reason",
                        ));
                    }
                };
            Ok(RootCommandBatch::Compacted {
                requested_after: compacted.requested_after,
                compacted_to: compacted.compacted_to,
                recovery_resume_after: cursor.resume_after,
                recovery_reason,
            })
        }
        Some(afs_protocol::meta::root_command_batch_reply::Result::Unsupported(unsupported)) => {
            Ok(RootCommandBatch::Unsupported {
                message: unsupported.message,
            })
        }
        None => Err(Error::coded(
            CLIENT_PROTOCOL_VIOLATION,
            "RootCommandBatchReply is empty",
        )),
    }
}

#[cfg(all(test, feature = "ownerfs"))]
mod n2b2_root_command_batch_tests {
    use super::*;

    fn access() -> afs_protocol::meta::RootAccess {
        afs_protocol::meta::RootAccess {
            root_id: "root-a".into(),
            root_epoch: 3,
            home_node_id: "home-a".into(),
            holder_node_id: "holder-a".into(),
            session_id: "holder-session-a".into(),
            access_generation: 5,
            rights: vec![afs_protocol::meta::RootRight::Lookup as i32],
            fencing_token: "fence-a".into(),
            home_session_id: "home-session-a".into(),
        }
    }

    #[test]
    fn root_command_batch_adapter_uses_next_minus_one_cursor_and_exact_access_fields() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 14,
                        commands: vec![afs_protocol::meta::RootCommand {
                            command_id: "cmd-a".into(),
                            command_type: afs_protocol::meta::RootCommandType::RevokeAccess as i32,
                            access: Some(access()),
                            revision: 12,
                        }],
                    },
                ),
            ),
        };

        let batch = root_command_batch_from_wire(11, reply).unwrap();

        assert_eq!(
            batch,
            RootCommandBatch::Events {
                start_revision: 11,
                next_revision: 14,
                commands: vec![RootCommand {
                    command_id: "cmd-a".into(),
                    command_type: RootCommandType::RevokeAccess,
                    revision: 12,
                    root_id: "root-a".into(),
                    root_epoch: 3,
                    home_node_id: "home-a".into(),
                    home_session_id: "home-session-a".into(),
                    access_generation: 5,
                }],
            }
        );
    }

    #[test]
    fn root_command_batch_adapter_compaction_is_non_authorizing() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Compacted(
                    afs_protocol::meta::RootCommandBatchCompacted {
                        requested_after: 8,
                        compacted_to: 20,
                        recovery_cursor: Some(afs_protocol::meta::RootCommandRecoveryCursor {
                            resume_after: 20,
                            reason: afs_protocol::meta::RootCommandRecoveryReason::WatchCompacted
                                as i32,
                        }),
                    },
                ),
            ),
        };

        let batch = root_command_batch_from_wire(8, reply).unwrap();

        assert_eq!(
            batch,
            RootCommandBatch::Compacted {
                requested_after: 8,
                compacted_to: 20,
                recovery_resume_after: 20,
                recovery_reason: RootCommandRecoveryReason::WatchCompacted,
            }
        );
    }

    #[test]
    fn root_command_batch_adapter_rejects_malformed_compaction_diagnostics() {
        use afs_protocol::meta::RootCommandRecoveryReason;
        for (requested_after, compacted_to, resume_after, reason) in [
            (8, 20, 19, RootCommandRecoveryReason::Unspecified as i32),
            (8, 20, 19, 99),
            (8, 8, 19, RootCommandRecoveryReason::WatchCompacted as i32),
            (8, 20, 8, RootCommandRecoveryReason::WatchCompacted as i32),
            (8, 20, 0, RootCommandRecoveryReason::WatchCompacted as i32),
        ] {
            let reply = afs_protocol::meta::RootCommandBatchReply {
                result: Some(
                    afs_protocol::meta::root_command_batch_reply::Result::Compacted(
                        afs_protocol::meta::RootCommandBatchCompacted {
                            requested_after,
                            compacted_to,
                            recovery_cursor: Some(afs_protocol::meta::RootCommandRecoveryCursor {
                                resume_after,
                                reason,
                            }),
                        },
                    ),
                ),
            };
            assert_eq!(
                root_command_batch_from_wire(8, reply).unwrap_err().code(),
                CLIENT_PROTOCOL_VIOLATION
            );
        }
    }

    #[test]
    fn root_command_batch_adapter_unsupported_is_non_authorizing() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Unsupported(
                    afs_protocol::meta::RootCommandBatchUnsupported {
                        message: "batch polling is disabled".into(),
                    },
                ),
            ),
        };

        let batch = root_command_batch_from_wire(8, reply).unwrap();

        assert_eq!(
            batch,
            RootCommandBatch::Unsupported {
                message: "batch polling is disabled".into(),
            }
        );
    }

    #[test]
    fn root_command_batch_adapter_rejects_empty_and_invalid_event_batches() {
        let error = root_command_batch_from_wire(
            14,
            afs_protocol::meta::RootCommandBatchReply { result: None },
        )
        .expect_err("empty reply must not authorize anything");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);

        let error = root_command_batch_from_wire(
            14,
            afs_protocol::meta::RootCommandBatchReply {
                result: Some(
                    afs_protocol::meta::root_command_batch_reply::Result::Events(
                        afs_protocol::meta::RootCommandBatchEvents {
                            start_revision: 14,
                            next_revision: 14,
                            commands: Vec::new(),
                        },
                    ),
                ),
            },
        )
        .expect_err("non-advancing events cannot produce a durable cursor");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);

        let error = root_command_batch_from_wire(
            14,
            afs_protocol::meta::RootCommandBatchReply {
                result: Some(
                    afs_protocol::meta::root_command_batch_reply::Result::Events(
                        afs_protocol::meta::RootCommandBatchEvents {
                            start_revision: 14,
                            next_revision: 16,
                            commands: vec![afs_protocol::meta::RootCommand {
                                command_id: "already-observed".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(access()),
                                revision: 14,
                            }],
                        },
                    ),
                ),
            },
        )
        .expect_err("start_revision is the exclusive previously observed revision");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);
    }

    #[test]
    fn root_command_batch_adapter_accepts_empty_idle_progress_without_new_command() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 14,
                        next_revision: 15,
                        commands: Vec::new(),
                    },
                ),
            ),
        };

        assert_eq!(
            root_command_batch_from_wire(14, reply).unwrap(),
            RootCommandBatch::Events {
                start_revision: 14,
                next_revision: 15,
                commands: Vec::new(),
            }
        );
    }

    #[test]
    fn root_command_batch_adapter_rejects_request_start_mismatch() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 15,
                        next_revision: 16,
                        commands: Vec::new(),
                    },
                ),
            ),
        };

        let error = root_command_batch_from_wire(14, reply)
            .expect_err("batch must identify the requested exclusive start");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);
    }

    #[test]
    fn root_command_batch_adapter_rejects_unimplemented_command_type() {
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 14,
                        commands: vec![afs_protocol::meta::RootCommand {
                            command_id: "cmd-cache".into(),
                            command_type: afs_protocol::meta::RootCommandType::InvalidateCache
                                as i32,
                            access: Some(access()),
                            revision: 12,
                        }],
                    },
                ),
            ),
        };

        let error = root_command_batch_from_wire(11, reply)
            .expect_err("known but unimplemented commands must fail close");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);
    }

    #[test]
    fn root_command_batch_adapter_rejects_duplicate_or_unordered_commands() {
        let mut first = access();
        first.root_id = "root-a".into();
        let mut second = access();
        second.root_id = "root-b".into();
        let duplicate = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 14,
                        commands: vec![
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-dup".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(first.clone()),
                                revision: 12,
                            },
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-dup".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(second.clone()),
                                revision: 12,
                            },
                        ],
                    },
                ),
            ),
        };
        assert_eq!(
            root_command_batch_from_wire(11, duplicate)
                .unwrap_err()
                .code(),
            CLIENT_PROTOCOL_VIOLATION
        );

        let unordered = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 15,
                        commands: vec![
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-later".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(first),
                                revision: 13,
                            },
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-earlier".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(second),
                                revision: 12,
                            },
                        ],
                    },
                ),
            ),
        };
        assert_eq!(
            root_command_batch_from_wire(11, unordered)
                .unwrap_err()
                .code(),
            CLIENT_PROTOCOL_VIOLATION
        );
    }

    #[test]
    fn root_command_batch_adapter_accepts_distinct_commands_at_same_revision() {
        let mut first = access();
        first.root_id = "root-a".into();
        let mut second = access();
        second.root_id = "root-b".into();
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 14,
                        commands: vec![
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-a".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(first),
                                revision: 12,
                            },
                            afs_protocol::meta::RootCommand {
                                command_id: "cmd-b".into(),
                                command_type: afs_protocol::meta::RootCommandType::RevokeAccess
                                    as i32,
                                access: Some(second),
                                revision: 12,
                            },
                        ],
                    },
                ),
            ),
        };

        let RootCommandBatch::Events { commands, .. } =
            root_command_batch_from_wire(11, reply).unwrap()
        else {
            panic!("events batch expected");
        };
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].revision, commands[1].revision);
    }

    #[test]
    fn root_command_batch_adapter_rejects_zero_home_tuple() {
        let mut invalid = access();
        invalid.root_epoch = 0;
        let reply = afs_protocol::meta::RootCommandBatchReply {
            result: Some(
                afs_protocol::meta::root_command_batch_reply::Result::Events(
                    afs_protocol::meta::RootCommandBatchEvents {
                        start_revision: 11,
                        next_revision: 14,
                        commands: vec![afs_protocol::meta::RootCommand {
                            command_id: "cmd-a".into(),
                            command_type: afs_protocol::meta::RootCommandType::RevokeAccess as i32,
                            access: Some(invalid),
                            revision: 12,
                        }],
                    },
                ),
            ),
        };

        let error = root_command_batch_from_wire(11, reply)
            .expect_err("nonzero root identity is required for revocation");
        assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);
    }
}

#[cfg(all(test, feature = "dfs"))]
mod tests {
    use super::*;
    use crate::node::vfs::dfs::DfsMeta;

    #[test]
    fn chunk_source_ranges_wire_roundtrip_and_legacy_fallback() {
        use prost::Message;

        let chunk = current_read_authorization().chunk.unwrap();
        let wire = afs_protocol::meta::DfsChunkSources {
            chunk_id: chunk.chunk_id.clone(),
            chunk: Some(chunk),
            sources: Vec::new(),
            allowed_ranges: vec![afs_protocol::meta::dfs_authorized_read::Range {
                offset: 1,
                length: 2,
            }],
        };
        let decoded =
            afs_protocol::meta::DfsChunkSources::decode(wire.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, wire);
        assert_eq!(
            domain_dfs_chunk_sources(decoded).unwrap().allowed_ranges,
            vec![(1, 2)]
        );

        let mut legacy_wire = wire;
        legacy_wire.allowed_ranges.clear();
        let legacy =
            afs_protocol::meta::DfsChunkSources::decode(legacy_wire.encode_to_vec().as_slice())
                .unwrap();
        assert!(
            domain_dfs_chunk_sources(legacy)
                .unwrap()
                .allowed_ranges
                .is_empty()
        );
        let old_json = serde_json::json!({ "chunk_id": "legacy", "chunk": null, "sources": [] });
        let legacy: crate::dfs::ChunkSources = serde_json::from_value(old_json).unwrap();
        assert!(legacy.allowed_ranges.is_empty());
    }

    #[test]
    fn chunk_source_ranges_wire_rejects_unbounded_or_invalid_hints() {
        let chunk = current_read_authorization().chunk.unwrap();
        let range =
            |offset, length| afs_protocol::meta::dfs_authorized_read::Range { offset, length };
        for allowed_ranges in [
            vec![range(0, 1); 129],
            vec![range(0, 0)],
            vec![range(u64::MAX, 1)],
            vec![range(3, 2)],
        ] {
            let error = domain_dfs_chunk_sources(afs_protocol::meta::DfsChunkSources {
                chunk_id: chunk.chunk_id.clone(),
                chunk: Some(chunk.clone()),
                sources: Vec::new(),
                allowed_ranges,
            })
            .unwrap_err();
            assert_eq!(error.code(), CLIENT_PROTOCOL_VIOLATION);
        }
        assert!(
            domain_dfs_chunk_sources(afs_protocol::meta::DfsChunkSources {
                chunk_id: chunk.chunk_id,
                chunk: None,
                sources: Vec::new(),
                allowed_ranges: vec![range(0, 1)],
            })
            .is_err()
        );
    }

    #[test]
    fn external_authorization_wire_rejects_node_receiver_and_changed_object_key() {
        let chunk_id = crate::dfs::ChunkId::new(format!("blake3-{}-4", "a".repeat(64)));
        let key = crate::dfs::external_object_key("filesystem", "g1", &chunk_id).unwrap();
        let valid = afs_protocol::meta::DfsExternalAuthorizedRead {
            authorized: Some(afs_protocol::meta::DfsAuthorizedRead {
                validation: Some(afs_protocol::meta::DfsReadValidation {
                    grant: Some(afs_protocol::meta::DfsReadGrant::default()),
                    chunk_id: chunk_id.0.clone(),
                    copy_id: "external-copy".into(),
                    chunk_offset: 0,
                    length: 4,
                }),
                allowed_ranges: vec![afs_protocol::meta::dfs_authorized_read::Range {
                    offset: 0,
                    length: 4,
                }],
                expires_at_unix_ms: 10,
            }),
            chunk: Some(afs_protocol::meta::DfsChunkObject {
                chunk_id: chunk_id.0,
                length: 4,
                content_digest: vec![0xaa; 32],
                content_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3 as i32,
            }),
            location: Some(afs_protocol::meta::DfsCopyLocation {
                location: Some(afs_protocol::meta::dfs_copy_location::Location::External(
                    afs_protocol::meta::dfs_copy_location::External {
                        store_id: "filesystem".into(),
                        object_key: key,
                        object_revision: "g1".into(),
                    },
                )),
            }),
        };
        assert!(domain_external_authorized_read(valid.clone()).is_ok());
        let mut missing_identity = valid.clone();
        missing_identity.chunk = None;
        assert!(domain_external_authorized_read(missing_identity).is_err());
        let mut wrong_key = valid.clone();
        if let Some(afs_protocol::meta::dfs_copy_location::Location::External(external)) =
            wrong_key.location.as_mut().unwrap().location.as_mut()
        {
            external.object_key.push_str("-alias");
        }
        assert!(domain_external_authorized_read(wrong_key).is_err());
        let mut node_receiver = valid;
        node_receiver.location = Some(afs_protocol::meta::DfsCopyLocation {
            location: Some(afs_protocol::meta::dfs_copy_location::Location::Node(
                afs_protocol::meta::dfs_copy_location::Node::default(),
            )),
        });
        assert!(domain_external_authorized_read(node_receiver).is_err());
    }

    fn current_read_inode(head: &str) -> afs_protocol::meta::DfsInodeRecord {
        afs_protocol::meta::DfsInodeRecord {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            kind: afs_protocol::meta::DfsInodeKind::Regular as i32,
            attributes: Some(afs_protocol::meta::DfsInodeAttributes {
                mode: 0o640,
                uid: 1000,
                gid: 1000,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            }),
            head_version_id: head.into(),
            revision: 1,
            symlink_target: Vec::new(),
            xattrs: Vec::new(),
            special_node: None,
        }
    }

    fn current_read_authorization() -> afs_protocol::meta::DfsExternalAuthorizedRead {
        let chunk_id = crate::dfs::ChunkId::new(format!("blake3-{}-4", "b".repeat(64)));
        let key = crate::dfs::external_object_key("filesystem", "g1", &chunk_id).unwrap();
        afs_protocol::meta::DfsExternalAuthorizedRead {
            authorized: Some(afs_protocol::meta::DfsAuthorizedRead {
                validation: Some(afs_protocol::meta::DfsReadValidation {
                    grant: Some(afs_protocol::meta::DfsReadGrant::default()),
                    chunk_id: chunk_id.0.clone(),
                    copy_id: "external-copy".into(),
                    chunk_offset: 0,
                    length: 4,
                }),
                allowed_ranges: vec![afs_protocol::meta::dfs_authorized_read::Range {
                    offset: 0,
                    length: 4,
                }],
                expires_at_unix_ms: 10,
            }),
            chunk: Some(afs_protocol::meta::DfsChunkObject {
                chunk_id: chunk_id.0,
                length: 4,
                content_digest: vec![0xbb; 32],
                content_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3 as i32,
            }),
            location: Some(afs_protocol::meta::DfsCopyLocation {
                location: Some(afs_protocol::meta::dfs_copy_location::Location::External(
                    afs_protocol::meta::dfs_copy_location::External {
                        store_id: "filesystem".into(),
                        object_key: key,
                        object_revision: "g1".into(),
                    },
                )),
            }),
        }
    }

    #[test]
    fn current_external_reply_rejects_head_changed_authorizations() {
        let reply = afs_protocol::meta::AuthorizeCurrentExternalReadReply {
            inode: Some(current_read_inode("next")),
            head_changed: true,
            authorizations: vec![current_read_authorization()],
        };
        assert_eq!(
            domain_authorize_current_external_read_reply(reply)
                .unwrap_err()
                .code(),
            CLIENT_PROTOCOL_VIOLATION
        );
    }

    #[test]
    fn current_external_reply_requires_inode() {
        let reply = afs_protocol::meta::AuthorizeCurrentExternalReadReply {
            inode: None,
            head_changed: false,
            authorizations: Vec::new(),
        };
        assert_eq!(
            domain_authorize_current_external_read_reply(reply)
                .unwrap_err()
                .code(),
            CLIENT_PROTOCOL_VIOLATION
        );
    }

    #[test]
    fn current_external_reply_decodes_authorized_and_head_changed_shapes() {
        let authorized = afs_protocol::meta::AuthorizeCurrentExternalReadReply {
            inode: Some(current_read_inode("version")),
            head_changed: false,
            authorizations: vec![current_read_authorization()],
        };
        assert!(matches!(
            domain_authorize_current_external_read_reply(authorized).unwrap(),
            crate::dfs::AuthorizeCurrentExternalReadReply::Authorized { authorizations, .. }
                if authorizations.len() == 1
        ));

        let head_changed = afs_protocol::meta::AuthorizeCurrentExternalReadReply {
            inode: Some(current_read_inode("next")),
            head_changed: true,
            authorizations: Vec::new(),
        };
        assert!(matches!(
            domain_authorize_current_external_read_reply(head_changed).unwrap(),
            crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode)
                if inode.head_version == Some(crate::dfs::FileVersionId::new("next"))
        ));
    }

    #[test]
    fn external_version_reply_requires_bounded_positive_validity() {
        let valid = afs_protocol::meta::AuthorizeExternalVersionReadReply {
            authorizations: vec![current_read_authorization()],
            remaining_validity_ms: 5_000,
        };
        assert!(matches!(
            domain_authorize_external_version_read_reply(valid).unwrap(),
            crate::dfs::AuthorizeExternalVersionReadReply {
                authorizations,
                remaining_validity_ms: 5_000
            } if authorizations.len() == 1
        ));
        for remaining_validity_ms in [0, 5_001] {
            let reply = afs_protocol::meta::AuthorizeExternalVersionReadReply {
                authorizations: vec![current_read_authorization()],
                remaining_validity_ms,
            };
            assert_eq!(
                domain_authorize_external_version_read_reply(reply)
                    .unwrap_err()
                    .code(),
                CLIENT_PROTOCOL_VIOLATION
            );
        }
    }

    #[test]
    fn current_external_unsupported_filter_is_exact_tonic_unimplemented() {
        assert!(current_external_read_is_unimplemented(
            &tonic::Status::unimplemented("old meta")
        ));
        assert!(!current_external_read_is_unimplemented(
            &tonic::Status::permission_denied("no")
        ));
        assert!(!current_external_read_is_unimplemented(
            &tonic::Status::deadline_exceeded("slow")
        ));
    }

    use std::time::Instant;
    use tokio::net::TcpListener;

    async fn hanging_endpoint() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut sockets = Vec::new();
            while let Ok((socket, _peer)) = listener.accept().await {
                sockets.push(socket);
            }
        });
        (format!("http://{addr}"), task)
    }

    fn test_meta(endpoint: &str, timeout: Duration) -> GrpcDfsMeta {
        GrpcDfsMeta::new(
            endpoint,
            "node-a".into(),
            "session-a".into(),
            crate::dfs::NamespaceId::new("default"),
            timeout,
            TlsConfig::Disabled,
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_rpc_metrics_count_real_adapter_invocations_even_on_failure() {
        let (endpoint, server) = hanging_endpoint().await;
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::read_work::ReadWorkMetrics::register(&registry).unwrap();
        let meta = test_meta(&endpoint, Duration::from_millis(30)).with_read_work_metrics(metrics);
        tokio::task::spawn_blocking(move || {
            use crate::node::{dfs_read::ReadSourceProvider, vfs::dfs::DfsMeta};
            assert!(
                meta.get_inode(&crate::dfs::InodeId::new("missing"))
                    .is_err()
            );
            assert!(
                meta.get_file_version(&crate::dfs::FileVersionId::new("missing"))
                    .is_err()
            );
            assert!(
                meta.sources_for(crate::dfs::DfsChunkSourcesRequest {
                    caller_id: "node-a".into(),
                    namespace_id: crate::dfs::NamespaceId::new("default"),
                    file_version_id: crate::dfs::FileVersionId::new("missing"),
                    layout_root_id: crate::dfs::LayoutRootId::new("missing"),
                    chunk_ids: Vec::new(),
                })
                .is_err()
            );
            assert!(
                meta.validate_read_grants(crate::dfs::ValidateDfsReadGrants {
                    receiver_node_id: "node-a".into(),
                    receiver_node_epoch: 1,
                    peer_node_id: "node-b".into(),
                    validations: Vec::new(),
                })
                .is_err()
            );
        })
        .await
        .unwrap();
        server.abort();
        let families = registry.gather();
        let calls = families
            .iter()
            .find(|family| family.name() == "afs_dfs_read_rpc_calls_total")
            .unwrap();
        assert_eq!(calls.get_metric().len(), 5);
        for metric in calls.get_metric() {
            let method = metric
                .get_label()
                .iter()
                .find(|label| label.name() == "method")
                .unwrap()
                .value();
            let expected = if method == "peer_read_ranges" {
                0.0
            } else {
                1.0
            };
            assert_eq!(metric.get_counter().get_value(), expected, "{method}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_read_diagnostics_cover_full_failed_meta_calls() {
        use crate::node::read_work::{ReadDiagnosticStage, ReadWorkMetrics};
        let (endpoint, server) = hanging_endpoint().await;
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let meta =
            test_meta(&endpoint, Duration::from_millis(30)).with_read_work_metrics(metrics.clone());
        tokio::task::spawn_blocking(move || {
            use crate::node::{dfs_read::ReadSourceProvider, vfs::dfs::DfsMeta};
            assert_eq!(
                meta.get_inode(&crate::dfs::InodeId::new("missing"))
                    .unwrap_err()
                    .code(),
                afs_error::CLIENT_DEADLINE_EXCEEDED
            );
            assert_eq!(
                meta.get_file_version(&crate::dfs::FileVersionId::new("missing"))
                    .unwrap_err()
                    .code(),
                afs_error::CLIENT_DEADLINE_EXCEEDED
            );
            assert!(
                meta.sources_for(crate::dfs::DfsChunkSourcesRequest {
                    caller_id: "node-a".into(),
                    namespace_id: crate::dfs::NamespaceId::new("default"),
                    file_version_id: crate::dfs::FileVersionId::new("missing"),
                    layout_root_id: crate::dfs::LayoutRootId::new("missing"),
                    chunk_ids: Vec::new(),
                })
                .is_err()
            );
            assert!(
                meta.validate_read_grants(crate::dfs::ValidateDfsReadGrants {
                    receiver_node_id: "node-a".into(),
                    receiver_node_epoch: 1,
                    peer_node_id: "node-b".into(),
                    validations: Vec::new(),
                })
                .is_err()
            );
            assert!(
                meta.lookup(&crate::dfs::InodeId::new("root"), b"missing")
                    .is_err()
            );
            assert!(meta.lookup_node_location("missing").is_err());
        })
        .await
        .unwrap();
        server.abort();
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        let expected = [
            ReadDiagnosticStage::MetaGetInode,
            ReadDiagnosticStage::MetaGetFileVersion,
            ReadDiagnosticStage::MetaGetChunkSources,
            ReadDiagnosticStage::MetaValidateReadGrants,
            ReadDiagnosticStage::MetaLookup,
            ReadDiagnosticStage::MetaLookupNodeLocation,
        ];
        assert_eq!(snapshot.records.len(), expected.len());
        for (record, stage) in snapshot.records.iter().zip(expected) {
            assert_eq!(record.stage, stage);
            assert!(record.completed && !record.success);
            assert!(record.end_ns > record.start_ns);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_write_diagnostics_cover_controls_and_keep_cached_placement_local() {
        use crate::node::read_work::{ReadDiagnosticStage, ReadWorkMetrics};
        let (endpoint, server) = hanging_endpoint().await;
        for enabled in [false, true] {
            let registry = afs_metrics::Registry::new();
            let metrics = ReadWorkMetrics::register(&registry)
                .unwrap()
                .with_read_diagnostics(enabled);
            let meta = test_meta(&endpoint, Duration::from_millis(30))
                .with_read_work_metrics(metrics.clone());
            tokio::task::spawn_blocking(move || {
                use crate::node::{replication::PlacementProvider, vfs::dfs::DfsMeta};
                let cached = Arc::new(crate::dfs::PlacementSnapshot {
                    revision: 9,
                    replication: crate::dfs::ReplicationConfig::local_single_copy(),
                    replica_groups: Vec::new(),
                });
                *meta.placement.lock().unwrap() = Some(cached.clone());
                assert!(Arc::ptr_eq(&meta.snapshot().unwrap(), &cached));
                if let Some(snapshot) = meta.read_work.as_ref().unwrap().diagnostics_snapshot() {
                    assert!(
                        snapshot.records.is_empty(),
                        "cached placement is local work"
                    );
                }
                // Refresh really invokes the transport; its failure leaves the
                // previously cached placement usable without another RPC.
                assert!(meta.refresh(10).is_err());
                assert!(Arc::ptr_eq(&meta.snapshot().unwrap(), &cached));
                assert!(
                    meta.create(
                        &crate::dfs::OperationId::new("diagnostic-create"),
                        &crate::dfs::InodeId::new("root"),
                        b"missing",
                        crate::dfs::InodeAttributes {
                            mode: 0o644,
                            uid: 1,
                            gid: 1,
                            nlink: 1,
                            atime_unix_ms: 0,
                            mtime_unix_ms: 0,
                            ctime_unix_ms: 0,
                        },
                    )
                    .is_err()
                );
                let inode = crate::dfs::InodeId::new("missing");
                assert!(meta.open_write(&inode).is_err());
                assert!(meta.resolve_write_authority(&inode).is_err());
                assert!(meta.renew_write_lease(test_lease()).is_err());
                assert!(meta.sync_inode_metadata(test_metadata_sync()).is_err());
            })
            .await
            .unwrap();
            match metrics.diagnostics_snapshot() {
                None => assert!(!enabled),
                Some(snapshot) => {
                    let expected = [
                        (
                            ReadDiagnosticStage::MetaPlacementSnapshot,
                            "meta_placement_rpc",
                        ),
                        (ReadDiagnosticStage::MetaCreate, "meta_create_rpc"),
                        (ReadDiagnosticStage::MetaOpenWrite, "meta_open_write_rpc"),
                        (
                            ReadDiagnosticStage::MetaResolveWriteAuthority,
                            "meta_resolve_write_rpc",
                        ),
                        (
                            ReadDiagnosticStage::MetaRenewWriteLease,
                            "meta_renew_write_lease_rpc",
                        ),
                        (
                            ReadDiagnosticStage::MetaSyncInodeMetadata,
                            "meta_sync_inode_metadata_rpc",
                        ),
                    ];
                    assert_eq!(snapshot.records.len(), expected.len());
                    assert_eq!(snapshot.overflow, 0);
                    for (record, (stage, phase)) in snapshot.records.iter().zip(expected) {
                        assert_eq!(record.stage, stage);
                        assert!(record.completed && !record.success);
                        assert!(record.end_ns >= record.start_ns);
                        let aggregate = snapshot
                            .coarse_phases
                            .iter()
                            .find(|entry| entry.phase == phase)
                            .unwrap();
                        assert_eq!(aggregate.calls, 1, "{phase}");
                        assert!(aggregate.last_end_ns >= aggregate.first_start_ns);
                    }
                }
            }
        }
        server.abort();
    }

    fn test_lease() -> crate::dfs::WriteLease {
        crate::dfs::WriteLease {
            inode_id: crate::dfs::InodeId::new("inode:test"),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            expires_at_unix_ms: u64::MAX,
        }
    }

    fn test_metadata_sync() -> crate::dfs::SyncInodeMetadata {
        crate::dfs::SyncInodeMetadata {
            operation_id: crate::dfs::OperationId::new("op:sync"),
            inode_id: crate::dfs::InodeId::new("inode:test"),
            write_lease: test_lease(),
            expected_inode_revision: 1,
            expected_head_version: None,
            metadata_delta: crate::dfs::CommitMetadataDelta {
                mode: crate::dfs::CommitMetadataMode::Full,
                mtime_unix_ms: Some(1),
                ctime_unix_ms: Some(1),
                kill_suidgid: false,
            },
        }
    }

    fn test_commit() -> crate::dfs::CommitFileVersion {
        let layout_id = crate::dfs::LayoutRootId::new("layout:test");
        crate::dfs::CommitFileVersion {
            gc_fence: None,
            operation_id: crate::dfs::OperationId::new("op:commit"),
            inode_id: crate::dfs::InodeId::new("inode:test"),
            write_lease: test_lease(),
            expected_inode_revision: 1,
            expected_head_version: None,
            file_version: crate::dfs::FileVersion {
                id: crate::dfs::FileVersionId::new("version:test"),
                inode_id: crate::dfs::InodeId::new("inode:test"),
                parent_version: None,
                length: 0,
                layout_root: layout_id.clone(),
                created_at_unix_ms: 1,
            },
            layout_root: crate::dfs::LayoutRoot {
                id: layout_id,
                file_length: 0,
                inline_extents: Vec::new(),
            },
            chunk_receipts: Vec::new(),
            clone_source: None,
            metadata_delta: crate::dfs::CommitMetadataDelta {
                mode: crate::dfs::CommitMetadataMode::Full,
                mtime_unix_ms: Some(1),
                ctime_unix_ms: Some(1),
                kill_suidgid: false,
            },
        }
    }

    fn test_digest(byte: u8) -> crate::dfs::ContentDigest {
        crate::dfs::ContentDigest {
            algorithm: crate::dfs::DigestAlgorithm::Blake3,
            bytes: [byte; 32],
        }
    }

    fn test_replica_target(node: &str, index: u64) -> crate::dfs::ReplicaTarget {
        crate::dfs::ReplicaTarget {
            node_id: node.into(),
            node_epoch: index + 10,
            data_endpoint: format!("https://{node}:19000"),
            device: crate::dfs::StorageDeviceDescriptor {
                device_id: format!("device-{node}"),
                device_epoch: index + 20,
                catalog_revision: index + 30,
                failure_domain: format!("rack-{index}"),
            },
        }
    }

    fn test_replication_claim() -> crate::dfs::ReplicationClaim {
        crate::dfs::ReplicationClaim {
            task_id: crate::dfs::ReplicationTaskId::new("task:claim"),
            operation_id: crate::dfs::OperationId::new("op:claim"),
            worker_node_id: "node-a".into(),
            worker_node_epoch: 11,
            worker_session_id: "session-a".into(),
            expires_at_unix_ms: 123_456,
            fence: 9,
            chunk: crate::dfs::ChunkObject {
                id: crate::dfs::ChunkId::new("chunk:claim"),
                length: 4096,
                content_digest: test_digest(7),
                encoding: crate::dfs::ChunkEncoding::Raw,
            },
            source_copy_id: crate::dfs::CopyId::new("copy:source"),
            placement_revision: 77,
            replica_group: crate::dfs::ReplicaGroup {
                id: crate::dfs::ReplicaGroupId::new("group:claim"),
                placement_epoch: 88,
                targets: vec![
                    test_replica_target("node-a", 1),
                    test_replica_target("node-b", 2),
                ],
            },
            replication: crate::dfs::ReplicationConfig {
                desired_copies: 2,
                sync_required_copies: 2,
                min_distinct_nodes: 2,
                min_distinct_failure_domains: 1,
                local_copy: crate::dfs::LocalCopyPolicy::Preferred,
            },
        }
    }

    #[test]
    fn replication_claim_wire_round_trip_preserves_authority_identity() {
        let claim = test_replication_claim();
        let wire = wire_replication_claim(&claim);

        assert_eq!(wire.task_id, "task:claim");
        assert_eq!(wire.operation_id, "op:claim");
        assert_eq!(wire.worker_node_id, "node-a");
        assert_eq!(wire.worker_session_id, "session-a");
        assert_eq!(wire.source_copy_id, "copy:source");

        let decoded = domain_replication_claim(wire).unwrap();
        assert_eq!(decoded, claim);
    }

    #[test]
    fn replication_claim_rejects_invalid_chunk_digest_length() {
        let claim = test_replication_claim();
        let mut wire = wire_replication_claim(&claim);
        wire.chunk.as_mut().unwrap().content_digest = vec![1, 2, 3];

        let error = domain_replication_claim(wire).expect_err("invalid digest should fail");
        assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
    }

    #[test]
    fn replication_task_rejects_unspecified_state() {
        let task = afs_protocol::meta::DfsReplicationTask {
            task_id: "task:bad".into(),
            chunk_id: "chunk:bad".into(),
            placement_epoch: 1,
            desired_copies: 1,
            existing_copy_ids: Vec::new(),
            state: afs_protocol::meta::DfsReplicationTaskState::Unspecified as i32,
            attempt: 0,
            next_retry_unix_ms: 0,
            last_error: None,
            claim: None,
        };

        let error = domain_replication_task(task).expect_err("unspecified state should fail");
        assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
    }

    #[test]
    fn dfs_meta_typed_tonic_timeout_is_client_deadline() {
        let mut status = tonic::Status::cancelled("Timeout expired");
        status.set_source(std::sync::Arc::new(tonic::TimeoutExpired(())));

        let error = GrpcDfsMeta::dfs_meta_status_to_error(status);

        assert_eq!(error.code(), afs_error::CLIENT_DEADLINE_EXCEEDED);
        assert_eq!(error.kind(), afs_error::ErrorKind::DeadlineExceeded);
    }

    #[test]
    fn dfs_meta_plain_cancelled_remains_remote_cancelled() {
        let error =
            GrpcDfsMeta::dfs_meta_status_to_error(tonic::Status::cancelled("remote cancelled"));

        assert_eq!(error.code(), afs_error::CLIENT_REMOTE_STATUS);
        assert_eq!(error.kind(), afs_error::ErrorKind::Cancelled);
        assert_eq!(error.message(), "remote cancelled");
    }

    #[test]
    fn dfs_meta_rich_business_error_identity_is_unchanged() {
        let original = Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, "invalid argument");
        let status = afs_transport::grpc::error_status::error_to_status(original.clone());

        let error = GrpcDfsMeta::dfs_meta_status_to_error(status);

        assert_eq!(error, original);
    }

    async fn assert_deadline_elapsed(
        label: &'static str,
        operation: impl FnOnce() -> afs_error::Result<()> + Send + 'static,
    ) {
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(operation).await.unwrap();
        let elapsed = started.elapsed();
        let error = result.expect_err(label);
        assert_eq!(
            error.code(),
            afs_error::CLIENT_DEADLINE_EXCEEDED,
            "{label}: unexpected error {error:?}"
        );
        assert!(
            elapsed < Duration::from_millis(300),
            "{label} should honor the focused timeout, elapsed={elapsed:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_lock_resolver_honors_configured_deadline() {
        let (endpoint, listener) = hanging_endpoint().await;
        let meta = test_meta(&endpoint, Duration::from_millis(40));
        assert_deadline_elapsed("lock resolver configured deadline", move || {
            DfsMeta::resolve_lock_authority(&meta, &crate::dfs::InodeId::new("inode:test"))
                .map(|_| ())
        })
        .await;
        listener.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grpc_dfs_meta_focused_timeouts_bound_hanging_rpc() {
        let (endpoint, listener) = hanging_endpoint().await;
        let configured_long = Duration::from_secs(1);
        let focused = Duration::from_millis(40);

        let meta = std::sync::Arc::new(test_meta(&endpoint, configured_long));
        let lease_meta = meta.clone();
        assert_deadline_elapsed("renew lease timeout", move || {
            DfsMeta::renew_write_lease_with_timeout(lease_meta.as_ref(), test_lease(), focused)
                .map(|_| ())
        })
        .await;

        let sync_meta = meta.clone();
        assert_deadline_elapsed("metadata sync timeout", move || {
            DfsMeta::sync_inode_metadata_with_timeout(
                sync_meta.as_ref(),
                test_metadata_sync(),
                focused,
            )
            .map(|_| ())
        })
        .await;

        let commit_meta = meta.clone();
        assert_deadline_elapsed("file commit timeout", move || {
            DfsMeta::commit_file_version_with_timeout(commit_meta.as_ref(), test_commit(), focused)
                .map(|_| ())
        })
        .await;

        let configured_short = std::sync::Arc::new(test_meta(&endpoint, focused));
        assert_deadline_elapsed("configured timeout remains a cap", move || {
            DfsMeta::renew_write_lease_with_timeout(
                configured_short.as_ref(),
                test_lease(),
                configured_long,
            )
            .map(|_| ())
        })
        .await;

        listener.abort();
    }
}

#[cfg(feature = "dfs")]
impl crate::node::gc::GcControl for GrpcDfsMeta {
    fn retain(
        &self,
        id: &str,
        inode: &crate::dfs::InodeId,
        version: Option<&crate::dfs::FileVersionId>,
        publication: bool,
    ) -> Result<()> {
        self.update_gc(id, Some(inode), version, publication, Vec::new(), false)
    }
    fn retain_fenced(
        &self,
        id: &str,
        inode: &crate::dfs::InodeId,
        version: Option<&crate::dfs::FileVersionId>,
    ) -> Result<crate::dfs::GcFence> {
        self.update_gc_with_fence(id, Some(inode), version, true, Vec::new(), false, true)?
            .ok_or_else(|| {
                Error::coded(
                    afs_error::NODE_VFS_UNIMPLEMENTED,
                    "Meta did not return a GC fence",
                )
            })
    }
    fn release(&self, id: &str) -> Result<()> {
        self.update_gc(id, None, None, false, Vec::new(), true)
    }
    fn reserve_publication(
        &self,
        operation: &crate::dfs::OperationId,
        chunks: Vec<crate::dfs::ChunkObject>,
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.update_gc(
                &format!("publish:{}", operation.0),
                None,
                None,
                true,
                chunks.clone(),
                false,
            ) {
                Err(error)
                    if error.code() == afs_error::META_DFS_GC_BUSY && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(50))
                }
                result => return result,
            }
        }
    }
    fn collect(
        &self,
        completed: u64,
        inventory: Vec<crate::dfs::ChunkObject>,
    ) -> Result<crate::dfs::GcState> {
        let mut client =
            afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone());
        let reply = self
            .run(
                client.collect_garbage(afs_protocol::meta::CollectDfsGarbageRequest {
                    node_id: self.node_id.clone(),
                    session_id: self.session_id.clone(),
                    completed_generation: completed,
                    inventory: inventory.into_iter().map(wire_chunk_object).collect(),
                }),
            )?
            .into_inner();
        Ok(crate::dfs::GcState {
            generation: reply.generation,
            chunks: reply
                .chunk_ids
                .into_iter()
                .map(crate::dfs::ChunkId::new)
                .collect(),
            waiting_nodes: if reply.pending_for_node {
                vec![self.node_id.clone()]
            } else {
                Vec::new()
            },
        })
    }
}
#[cfg(feature = "dfs")]
impl GrpcDfsMeta {
    fn update_gc(
        &self,
        id: &str,
        inode: Option<&crate::dfs::InodeId>,
        version: Option<&crate::dfs::FileVersionId>,
        publication: bool,
        chunks: Vec<crate::dfs::ChunkObject>,
        release: bool,
    ) -> Result<()> {
        self.update_gc_with_fence(id, inode, version, publication, chunks, release, false)
            .map(|_| ())
    }

    #[allow(clippy::too_many_arguments)] // Mirrors the existing fixed GC request shape.
    fn update_gc_with_fence(
        &self,
        id: &str,
        inode: Option<&crate::dfs::InodeId>,
        version: Option<&crate::dfs::FileVersionId>,
        publication: bool,
        chunks: Vec<crate::dfs::ChunkObject>,
        release: bool,
        capture_gc_fence: bool,
    ) -> Result<Option<crate::dfs::GcFence>> {
        let mut client =
            afs_protocol::meta::dfs_meta_client::DfsMetaClient::new(self.channel.clone());
        let reply = self
            .run(
                client.update_gc_reference(afs_protocol::meta::UpdateDfsGcReferenceRequest {
                    capture_gc_fence,
                    node_id: self.node_id.clone(),
                    session_id: self.session_id.clone(),
                    reference_id: id.into(),
                    admission_deadline_unix_ms: if inode.is_some() && !release {
                        crate::meta::store::now_unix_ms().saturating_add(30_000)
                    } else {
                        0
                    },
                    inode_id: inode.map(|id| id.0.clone()).unwrap_or_default(),
                    version_id: version.map(|id| id.0.clone()).unwrap_or_default(),
                    publication,
                    chunks: chunks.into_iter().map(wire_chunk_object).collect(),
                    release,
                }),
            )?
            .into_inner();
        reply
            .gc_fence
            .map(crate::dfs::GcFence::from_wire)
            .transpose()
    }
}

#[cfg(all(test, feature = "dfs"))]
mod crc32c_wire_tests {
    use super::*;
    #[test]
    fn crc32c_meta_descriptor_roundtrip() {
        let chunk = crate::dfs::ChunkObject {
            id: crate::dfs::ChunkId::new("crc32c-v1-00112233445566778899aabbccddeeff"),
            length: 32769,
            content_digest: crate::dfs::ContentDigest {
                algorithm: crate::dfs::DigestAlgorithm::Crc32cSegmentsV1,
                bytes: [7; 32],
            },
            encoding: crate::dfs::ChunkEncoding::Raw,
        };
        assert_eq!(
            domain_chunk_object(wire_chunk_object(chunk.clone())).unwrap(),
            chunk
        );
        assert_eq!(wire_digest_algorithm(chunk.content_digest.algorithm), 2);
        assert!(domain_digest_algorithm(99).is_err());
    }
}
