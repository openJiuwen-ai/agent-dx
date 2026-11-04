//! Meta connection errors, ping and node registration.

use super::*;

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
    let security = SecurityManager::new(tls)
        .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?;
    let endpoint = config.configure_client(
        Endpoint::from_shared(security.endpoint_uri(endpoint))
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?,
    );
    let channel = security
        .connect(endpoint)
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

pub(super) fn checked_registration_readiness(
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
    let security = SecurityManager::new(tls)
        .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?;
    let endpoint = config.configure_client(
        Endpoint::from_shared(security.endpoint_uri(endpoint))
            .map_err(|e| Error::coded(CLIENT_ARGUMENT_INVALID, e.to_string()))?,
    );
    let channel = security
        .connect(endpoint)
        .await
        .map_err(|error| match error {
            afs_transport::GrpcError::Transport(source) => map_registration_connect_error(source),
            other => Error::coded(CLIENT_ARGUMENT_INVALID, other.to_string()),
        })?;
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
