//! afs-node：近计算部署的单一节点进程。
//!
//! FUSE、SDK/REST、节点间 RPC 接入同一个 Node，内容经 P2P 直达数据节点。
//! 不另建 Home 进程，不保留 NFS 后端。OwnerFs/DFS 共用 Backend 与 FUSE 实现，
//! 但使用独立 mount、session、数据表示、缓存、一致性与恢复语义。
//! 阻塞 I/O/设备等待不可占住异步执行线程；调度和局部保护归各业务模块。

//!
//! 阅读启动顺序：run → Backend/Storage/会话表 → 本机 UDS → 可选 FUSE → TCP gRPC/REST。
//! gRPC 的控制与数据 service 共用 TCP listener；SDK 使用另一条本机 UDS listener。
//! OwnerFs 与 DFS 都通过 FUSE→Backend 进入各自的真实文件路径；
//! REST diagnostics/SDK→Storage 仍是独立诊断链，不属于任何文件系统的数据面。

pub mod api;
pub mod chunk;
#[cfg(feature = "dfs")]
pub mod dfs_read;
pub mod fuse;
#[cfg(feature = "ownerfs")]
mod native_workspace;
#[cfg(feature = "dfs")]
pub mod replication;
pub mod rpc;
pub mod storage;
pub mod vfs;

use crate::{
    config::Config,
    runtime::{BoxError, Observability, Services, cancelled},
};
use rpc::meta::{MetaPersistenceCapability, NodeRegistrationReadiness};
use std::{
    os::unix::ffi::OsStrExt,
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

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

/// B-side slow-path connector. The root grant and Home location stay in the
/// OwnerFs business layer; this adapter only turns Meta's authenticated node
/// endpoint into a cached OwnerFiles transport client.
#[cfg(feature = "ownerfs")]
struct OwnerFilesFactory {
    meta: Arc<rpc::meta::GrpcRootMeta>,
    peers: Arc<rpc::peer::PeerConnectionPool>,
    runtime: tokio::runtime::Handle,
    metrics: rpc::OwnerRpcMetrics,
    data_mode: rpc::peer::DataMode,
    rdma_device: Option<String>,
    timeout: std::time::Duration,
    rdma_admission: Arc<tokio::sync::Semaphore>,
}

#[cfg(feature = "ownerfs")]
impl vfs::ownerfs::RemoteFilesFactory for OwnerFilesFactory {
    fn supports_advisory_locks(&self) -> bool {
        true
    }

    fn supports_killpriv_v2(&self) -> bool {
        true
    }

    fn connect(
        &self,
        home_node_id: &str,
    ) -> afs_error::Result<Arc<dyn vfs::ownerfs::remote::RemoteFiles>> {
        let (uri, node_epoch) = self.meta.lookup_node_location(home_node_id)?;
        let channel = self.runtime.block_on(self.peers.owner_files_channel(
            home_node_id,
            node_epoch,
            &uri,
        ))?;
        let long_wait_channel =
            self.runtime
                .block_on(self.peers.long_wait_channel(home_node_id, node_epoch, &uri))?;
        Ok(Arc::new(
            rpc::peer::owner_files_client_from_channel_with_runtime_and_metrics(
                channel,
                self.runtime.clone(),
                Some(self.metrics.clone()),
            )
            .with_data_transport(self.data_mode, self.rdma_device.clone(), self.timeout)
            .with_rdma_admission(self.rdma_admission.clone())
            .with_long_wait_channel(long_wait_channel),
        ))
    }
}

/// REST 持有的进程级共享对象，不是另一个 Home 服务进程。
/// OwnerFs 和 DFS 分别绑定自己的 FUSE session；诊断 Storage 与 RDMA 会话表独立。
pub struct Node {
    pub config: Config,
    pub observability: Observability,
    /// 每次进程启动生成的新会话，旧远端句柄不能跨此边界复用。
    pub session_id: String,
    pub readiness: Arc<NodeReadiness>,
    #[cfg(feature = "ownerfs")]
    pub ownerfs: Option<Arc<vfs::ownerfs::OwnerFs>>,
    #[cfg(feature = "dfs")]
    pub dfs: Option<Arc<vfs::dfs::DistributedFs>>,
}

const READINESS_SAMPLER_INTERVAL: Duration = Duration::from_secs(15);
const READINESS_OBSERVATION_STALE_AFTER: Duration = Duration::from_secs(30);
const READINESS_SAMPLER_BLOCKED_AFTER: Duration = Duration::from_secs(15);
const READINESS_SAMPLER_SHUTDOWN_WAIT: Duration = Duration::from_secs(5);
const RDMA_ACCEPTED_PORT: &str = "1";

#[derive(Debug, Clone)]
pub struct ReadinessObservation {
    ready: bool,
    observed_at: Option<Instant>,
    source: &'static str,
    error: Option<String>,
}

impl ReadinessObservation {
    fn unknown(source: &'static str) -> Self {
        Self {
            ready: false,
            observed_at: None,
            source,
            error: Some("no readiness observation has completed".into()),
        }
    }

    fn from_sample(ready: bool, source: &'static str, error: Option<String>) -> Self {
        Self {
            ready,
            observed_at: Some(Instant::now()),
            source,
            error,
        }
    }

    #[must_use]
    pub fn ready(&self) -> bool {
        self.ready && !self.stale()
    }

    #[must_use]
    pub fn observed(&self) -> bool {
        self.observed_at.is_some()
    }

    #[must_use]
    pub fn stale(&self) -> bool {
        self.observed_at
            .is_none_or(|observed_at| observed_at.elapsed() > READINESS_OBSERVATION_STALE_AFTER)
    }

    #[must_use]
    pub fn age_ms(&self) -> Option<u128> {
        self.observed_at
            .map(|observed_at| observed_at.elapsed().as_millis())
    }

    #[must_use]
    pub fn source(&self) -> &'static str {
        self.source
    }

    #[must_use]
    pub fn error(&self) -> Option<String> {
        if self.observed_at.is_none() {
            return Some("no readiness observation has completed".into());
        }
        if self.stale() {
            return Some("readiness observation is stale".into());
        }
        self.error.clone()
    }
}

#[derive(Debug)]
pub struct NodeReadiness {
    meta_required: bool,
    allow_volatile_meta: AtomicBool,
    registered_epoch: Option<u64>,
    node_registration_ready: AtomicBool,
    node_registration_error: Mutex<Option<String>>,
    node_registration_last_success: Mutex<Option<Instant>>,
    meta_persistence_usable: AtomicBool,
    meta_persistent_ready: AtomicBool,
    meta_backend_healthy: AtomicBool,
    meta_backend_persistence: Mutex<MetaPersistenceCapability>,
    meta_persistence_error: Mutex<Option<String>>,
    ownerfs_mount: Option<AfsMountIdentity>,
    dfs_mount: Option<AfsMountIdentity>,
    data_device: Mutex<ReadinessObservation>,
    rdma_required: bool,
    rdma_configured: bool,
    rdma_device: Option<String>,
    rdma_available: Mutex<ReadinessObservation>,
    sampler_in_flight: AtomicBool,
    sampler_started_at: Mutex<Option<Instant>>,
    sampler_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Debug, Clone)]
pub struct AfsMountIdentity {
    path: PathBuf,
    mount_id: Option<String>,
    expected_source: String,
    capture_error: Option<String>,
}

impl AfsMountIdentity {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn mount_id(&self) -> Option<&str> {
        self.mount_id.as_deref()
    }

    #[must_use]
    pub fn expected_source(&self) -> &str {
        &self.expected_source
    }

    #[must_use]
    pub fn capture_error(&self) -> Option<&str> {
        self.capture_error.as_deref()
    }
}

fn capture_afs_mount_identity(path: PathBuf, expected_source: &str) -> AfsMountIdentity {
    let normalized = normalize_mount_path(&path);
    match current_visible_mount_id(&normalized, expected_source) {
        Ok(Some(mount_id)) => AfsMountIdentity {
            path: normalized,
            mount_id: Some(mount_id),
            expected_source: expected_source.to_owned(),
            capture_error: None,
        },
        Ok(None) => AfsMountIdentity {
            path: normalized,
            mount_id: None,
            expected_source: expected_source.to_owned(),
            capture_error: Some(
                "configured path was not the visible AFS FUSE mount at readiness initialization"
                    .into(),
            ),
        },
        Err(error) => AfsMountIdentity {
            path: normalized,
            mount_id: None,
            expected_source: expected_source.to_owned(),
            capture_error: Some(error),
        },
    }
}

fn normalize_mount_path(path: &Path) -> PathBuf {
    let mut normalized = if path.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"))
    };
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Path::new("/")),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    normalized
}

fn current_visible_mount_id(path: &Path, expected_source: &str) -> Result<Option<String>, String> {
    let mountinfo = std::fs::read("/proc/self/mountinfo").map_err(|error| error.to_string())?;
    Ok(current_visible_afs_mount_id_in_mountinfo(
        &mountinfo,
        path,
        expected_source,
    ))
}

pub(crate) fn current_visible_afs_mount_id_in_mountinfo(
    mountinfo: &[u8],
    path: &Path,
    expected_source: &str,
) -> Option<String> {
    let encoded = mountinfo_path_bytes(path);
    let top = mountinfo
        .split(|byte| *byte == b'\n')
        .filter_map(|line| parse_exact_mountinfo_line(line, &encoded))
        .next_back()?;
    let fuse = top.fs_type == b"fuse" || top.fs_type.starts_with(b"fuse.");
    if fuse && top.source == expected_source.as_bytes() {
        Some(String::from_utf8_lossy(top.mount_id).into_owned())
    } else {
        None
    }
}

struct MountinfoEntry<'a> {
    mount_id: &'a [u8],
    fs_type: &'a [u8],
    source: &'a [u8],
}

fn parse_exact_mountinfo_line<'a>(
    line: &'a [u8],
    encoded_path: &[u8],
) -> Option<MountinfoEntry<'a>> {
    let split = line.windows(3).position(|window| window == b" - ")?;
    let fields = &line[..split];
    let fs_fields = &line[split + 3..];
    let mount_id = fields.split(|byte| *byte == b' ').next()?;
    let mount_point = fields.split(|byte| *byte == b' ').nth(4)?;
    if mount_point != encoded_path {
        return None;
    }
    let mut fs_iter = fs_fields.split(|byte| *byte == b' ');
    let fs_type = fs_iter.next()?;
    let source = fs_iter.next()?;
    Some(MountinfoEntry {
        mount_id,
        fs_type,
        source,
    })
}

fn mountinfo_path_bytes(path: &Path) -> Vec<u8> {
    path.as_os_str()
        .as_bytes()
        .iter()
        .flat_map(|byte| match *byte {
            b' ' | b'\t' | b'\n' | b'\\' => format!("\\{:03o}", byte).into_bytes(),
            other => vec![other],
        })
        .collect()
}

#[cfg(test)]
mod readiness_mount_tests {
    use super::current_visible_afs_mount_id_in_mountinfo;
    use std::path::Path;

    #[test]
    fn visible_mount_identity_uses_top_exact_path_before_source_validation() {
        let mountinfo = b"41 30 0:31 / /mnt/afs rw,relatime - fuse afs-ownerfs rw\n42 30 0:32 / /mnt/afs rw,relatime - fuse wrong-source rw\n";

        assert_eq!(
            current_visible_afs_mount_id_in_mountinfo(
                mountinfo,
                Path::new("/mnt/afs"),
                "afs-ownerfs"
            ),
            None
        );
    }

    #[test]
    fn visible_mount_identity_accepts_top_exact_afs_mount() {
        let mountinfo = b"41 30 0:31 / /mnt/afs rw,relatime - fuse wrong-source rw\n42 30 0:32 / /mnt/afs rw,relatime - fuse.afs afs-ownerfs rw\n";

        assert_eq!(
            current_visible_afs_mount_id_in_mountinfo(
                mountinfo,
                Path::new("/mnt/afs"),
                "afs-ownerfs"
            ),
            Some("42".to_owned())
        );
    }
}

#[cfg(test)]
mod node_tcp_server_settings_tests {
    use super::{
        OWNER_ONLY_TCP_MAX_FRAME_SIZE, configure_node_tcp_server,
        rpc::{control::RdmaSessionRegistry, data::make_data_server},
        storage::Storage,
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
        let storage = Storage::new(temp.path().join("data")).expect("storage");
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

impl NodeReadiness {
    #[must_use]
    pub fn new(
        meta_required: bool,
        registered_epoch: Option<u64>,
        registered_at: Option<Instant>,
        ownerfs_mount: Option<PathBuf>,
        dfs_mount: Option<PathBuf>,
        data_mode: &str,
        rdma_device: Option<&str>,
    ) -> Self {
        let rdma_required = data_mode == "rdma";
        let rdma_configured = rdma_device.is_some();
        Self {
            meta_required,
            allow_volatile_meta: AtomicBool::new(false),
            registered_epoch,
            node_registration_ready: AtomicBool::new(!meta_required || registered_epoch.is_some()),
            node_registration_error: Mutex::new(None),
            node_registration_last_success: Mutex::new(if !meta_required {
                Some(Instant::now())
            } else {
                registered_at
            }),
            meta_persistence_usable: AtomicBool::new(!meta_required),
            meta_persistent_ready: AtomicBool::new(!meta_required),
            meta_backend_healthy: AtomicBool::new(!meta_required),
            meta_backend_persistence: Mutex::new(if meta_required {
                MetaPersistenceCapability::Unknown
            } else {
                MetaPersistenceCapability::Persistent
            }),
            meta_persistence_error: Mutex::new(if meta_required {
                Some("Meta persistence capability has not been observed".into())
            } else {
                None
            }),
            ownerfs_mount: ownerfs_mount
                .map(|path| capture_afs_mount_identity(path, "afs-ownerfs")),
            dfs_mount: dfs_mount.map(|path| capture_afs_mount_identity(path, "afs-dfs")),
            data_device: Mutex::new(ReadinessObservation::unknown("data_dir internal sampler")),
            rdma_required,
            rdma_configured,
            rdma_device: rdma_device.map(str::to_owned),
            rdma_available: Mutex::new(if rdma_configured {
                ReadinessObservation::unknown("RDMA sysfs sampler")
            } else {
                ReadinessObservation::from_sample(false, "RDMA is not configured", None)
            }),
            sampler_in_flight: AtomicBool::new(false),
            sampler_started_at: Mutex::new(None),
            sampler_handle: Mutex::new(None),
        }
    }

    pub fn note_registration_success(&self, meta_persistence_usable: bool) {
        self.note_registration_success_with_persistence(
            meta_persistence_usable,
            Some("Meta persistence capability was not reported by heartbeat".into()),
        );
    }

    pub fn set_allow_volatile_meta(&self, allow: bool) {
        self.allow_volatile_meta.store(allow, Ordering::Release);
    }

    pub fn note_registration_success_with_persistence(
        &self,
        meta_persistence_usable: bool,
        meta_persistence_error: Option<String>,
    ) {
        self.node_registration_ready.store(true, Ordering::Release);
        *self.node_registration_error.lock().unwrap() = None;
        *self.node_registration_last_success.lock().unwrap() = Some(Instant::now());
        *self.meta_backend_persistence.lock().unwrap() = if meta_persistence_usable {
            MetaPersistenceCapability::Persistent
        } else {
            MetaPersistenceCapability::Unknown
        };
        self.meta_backend_healthy
            .store(meta_persistence_usable, Ordering::Release);
        self.meta_persistent_ready
            .store(meta_persistence_usable, Ordering::Release);
        self.note_meta_persistence(
            meta_persistence_usable,
            if meta_persistence_usable {
                None
            } else {
                meta_persistence_error.or_else(|| {
                    Some("Meta persistence capability was not reported by heartbeat".into())
                })
            },
        );
    }

    pub fn note_registration_capability(&self, registration: &NodeRegistrationReadiness) {
        self.node_registration_ready.store(true, Ordering::Release);
        *self.node_registration_error.lock().unwrap() = None;
        *self.node_registration_last_success.lock().unwrap() = Some(Instant::now());
        self.note_registration_meta_capability(registration);
    }

    pub fn note_initial_registration_capability(&self, registration: &NodeRegistrationReadiness) {
        self.note_registration_meta_capability(registration);
    }

    fn note_registration_meta_capability(&self, registration: &NodeRegistrationReadiness) {
        *self.meta_backend_persistence.lock().unwrap() = registration.meta_backend_persistence;
        self.meta_backend_healthy
            .store(registration.meta_backend_healthy, Ordering::Release);
        self.meta_persistent_ready
            .store(registration.meta_persistence_ready, Ordering::Release);
        let usable = self.meta_registration_is_functionally_ready(registration);
        self.note_meta_persistence(
            usable,
            if usable {
                None
            } else {
                Some(registration.meta_persistence_detail.clone())
            },
        );
    }

    fn meta_registration_is_functionally_ready(
        &self,
        registration: &NodeRegistrationReadiness,
    ) -> bool {
        match registration.meta_backend_persistence {
            MetaPersistenceCapability::Persistent => {
                registration.meta_backend_healthy && registration.meta_persistence_ready
            }
            MetaPersistenceCapability::Volatile => {
                self.allow_volatile_meta.load(Ordering::Acquire)
                    && registration.meta_backend_healthy
            }
            MetaPersistenceCapability::Unknown => false,
        }
    }

    pub fn note_meta_persistence(&self, usable: bool, error: Option<String>) {
        self.meta_persistence_usable
            .store(usable, Ordering::Release);
        *self.meta_persistence_error.lock().unwrap() = error;
    }

    pub fn note_registration_failure(&self, error: impl Into<String>) {
        let error = error.into();
        self.node_registration_ready.store(false, Ordering::Release);
        *self.node_registration_error.lock().unwrap() = Some(error.clone());
        self.meta_backend_healthy.store(false, Ordering::Release);
        self.meta_persistent_ready.store(false, Ordering::Release);
        self.note_meta_persistence(false, Some(error));
    }

    #[must_use]
    pub fn meta_required(&self) -> bool {
        self.meta_required
    }

    #[must_use]
    pub fn allow_volatile_meta(&self) -> bool {
        self.allow_volatile_meta.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn registered_epoch(&self) -> Option<u64> {
        self.registered_epoch
    }

    #[must_use]
    pub fn node_registration_ready(&self) -> bool {
        if !self.meta_required {
            return true;
        }
        self.node_registration_ready.load(Ordering::Acquire)
            && self
                .node_registration_last_success
                .lock()
                .unwrap()
                .is_some_and(|observed_at| {
                    observed_at.elapsed() <= READINESS_OBSERVATION_STALE_AFTER
                })
    }

    #[must_use]
    pub fn node_registration_error(&self) -> Option<String> {
        if self.node_registration_ready.load(Ordering::Acquire)
            && self
                .node_registration_last_success
                .lock()
                .unwrap()
                .is_some_and(|observed_at| {
                    observed_at.elapsed() > READINESS_OBSERVATION_STALE_AFTER
                })
        {
            return Some("node registration heartbeat is stale".into());
        }
        self.node_registration_error.lock().unwrap().clone()
    }

    #[must_use]
    pub fn node_registration_age_ms(&self) -> Option<u128> {
        self.node_registration_last_success
            .lock()
            .unwrap()
            .map(|observed_at| observed_at.elapsed().as_millis())
    }

    #[must_use]
    pub fn meta_persistence_usable(&self) -> bool {
        !self.meta_required || self.meta_persistence_usable.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn meta_persistent_ready(&self) -> bool {
        !self.meta_required || self.meta_persistent_ready.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn meta_backend_healthy(&self) -> bool {
        !self.meta_required || self.meta_backend_healthy.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn meta_backend_persistence(&self) -> MetaPersistenceCapability {
        *self.meta_backend_persistence.lock().unwrap()
    }

    #[must_use]
    pub fn meta_persistence_error(&self) -> Option<String> {
        if self.meta_persistence_usable() {
            None
        } else {
            self.meta_persistence_error.lock().unwrap().clone()
        }
    }

    #[must_use]
    pub fn ownerfs_mount(&self) -> Option<&AfsMountIdentity> {
        self.ownerfs_mount.as_ref()
    }

    #[must_use]
    pub fn dfs_mount(&self) -> Option<&AfsMountIdentity> {
        self.dfs_mount.as_ref()
    }

    #[must_use]
    pub fn data_device_snapshot(&self) -> ReadinessObservation {
        self.data_device.lock().unwrap().clone()
    }

    #[must_use]
    pub fn rdma_required(&self) -> bool {
        self.rdma_required
    }

    #[must_use]
    pub fn rdma_configured(&self) -> bool {
        self.rdma_configured
    }

    #[must_use]
    pub fn rdma_device(&self) -> Option<&str> {
        self.rdma_device.as_deref()
    }

    #[must_use]
    pub fn rdma_available_snapshot(&self) -> ReadinessObservation {
        self.rdma_available.lock().unwrap().clone()
    }

    pub fn note_resource_observation(
        &self,
        data_ready: bool,
        data_error: Option<String>,
        rdma_available: bool,
        rdma_error: Option<String>,
    ) {
        *self.data_device.lock().unwrap() =
            ReadinessObservation::from_sample(data_ready, "data_dir internal sampler", data_error);
        *self.rdma_available.lock().unwrap() =
            ReadinessObservation::from_sample(rdma_available, "RDMA sysfs sampler", rdma_error);
    }

    pub fn force_sampler_in_flight_for_tests(&self) {
        self.sampler_in_flight.store(true, Ordering::Release);
        *self.sampler_started_at.lock().unwrap() =
            Some(Instant::now() - READINESS_SAMPLER_BLOCKED_AFTER - Duration::from_secs(1));
    }

    pub fn force_stale_registration_for_tests(&self) {
        if let Some(observed_at) = self.node_registration_last_success.lock().unwrap().as_mut() {
            *observed_at =
                Instant::now() - READINESS_OBSERVATION_STALE_AFTER - Duration::from_secs(1);
        }
    }

    pub fn force_stale_resource_observation_for_tests(&self) {
        if let Some(observed_at) = self.data_device.lock().unwrap().observed_at.as_mut() {
            *observed_at =
                Instant::now() - READINESS_OBSERVATION_STALE_AFTER - Duration::from_secs(1);
        }
        if let Some(observed_at) = self.rdma_available.lock().unwrap().observed_at.as_mut() {
            *observed_at =
                Instant::now() - READINESS_OBSERVATION_STALE_AFTER - Duration::from_secs(1);
        }
    }

    #[must_use]
    pub fn start_resource_sample(self: &Arc<Self>, data_dir: PathBuf) -> bool {
        if self
            .sampler_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.note_blocked_sampler_if_needed();
            return false;
        }
        *self.sampler_started_at.lock().unwrap() = Some(Instant::now());
        self.drop_finished_sampler_handle();
        let readiness = self.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let (data_ready, data_error) = sample_data_device(&data_dir);
            let (rdma_available, rdma_error) = sample_rdma_sysfs(
                readiness.rdma_required,
                readiness.rdma_configured,
                readiness.rdma_device.as_deref(),
            );
            readiness.note_resource_observation(data_ready, data_error, rdma_available, rdma_error);
            readiness.finish_sampler();
        });
        *self.sampler_handle.lock().unwrap() = Some(handle);
        true
    }

    fn finish_sampler(&self) {
        *self.sampler_started_at.lock().unwrap() = None;
        self.sampler_in_flight.store(false, Ordering::Release);
    }

    fn drop_finished_sampler_handle(&self) {
        let mut guard = self.sampler_handle.lock().unwrap();
        if guard
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            guard.take();
        }
    }

    pub async fn shutdown_resource_sampler(&self) -> afs_error::Result<()> {
        self.shutdown_resource_sampler_with_timeout(READINESS_SAMPLER_SHUTDOWN_WAIT)
            .await
    }

    pub async fn shutdown_resource_sampler_with_timeout(
        &self,
        timeout: Duration,
    ) -> afs_error::Result<()> {
        let Some(mut handle) = self.sampler_handle.lock().unwrap().take() else {
            return Ok(());
        };
        if handle.is_finished() {
            handle
                .await
                .map_err(|error| afs_error::Error::coded(afs_error::IO_OTHER, error.to_string()))?;
            return Ok(());
        }
        match tokio::time::timeout(timeout, &mut handle).await {
            Ok(result) => result
                .map_err(|error| afs_error::Error::coded(afs_error::IO_OTHER, error.to_string())),
            Err(_) => {
                *self.sampler_handle.lock().unwrap() = Some(handle);
                self.note_blocked_sampler_if_needed();
                Err(afs_error::Error::coded(
                    afs_error::IO_UNAVAILABLE,
                    "readiness sampler did not finish before shutdown timeout",
                ))
            }
        }
    }

    #[doc(hidden)]
    pub fn start_blocked_resource_sample_for_tests(
        self: &Arc<Self>,
    ) -> tokio::sync::oneshot::Sender<()> {
        assert!(
            self.sampler_in_flight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        );
        *self.sampler_started_at.lock().unwrap() =
            Some(Instant::now() - READINESS_SAMPLER_BLOCKED_AFTER - Duration::from_secs(1));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let readiness = self.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let _ = rx.blocking_recv();
            readiness.note_resource_observation(true, None, false, None);
            readiness.finish_sampler();
        });
        *self.sampler_handle.lock().unwrap() = Some(handle);
        tx
    }

    pub fn note_blocked_sampler_if_needed(&self) {
        let blocked = self
            .sampler_started_at
            .lock()
            .unwrap()
            .is_some_and(|started| started.elapsed() > READINESS_SAMPLER_BLOCKED_AFTER);
        if blocked {
            *self.data_device.lock().unwrap() = ReadinessObservation::from_sample(
                false,
                "data_dir internal sampler",
                Some("readiness sampler is still running".into()),
            );
            *self.rdma_available.lock().unwrap() = ReadinessObservation::from_sample(
                false,
                "RDMA sysfs sampler",
                Some("readiness sampler is still running".into()),
            );
        }
    }
}

fn sample_data_device(data_dir: &Path) -> (bool, Option<String>) {
    let internal = data_dir.join(".afs-internal-health");
    if let Err(error) = std::fs::create_dir_all(&internal) {
        return (false, Some(error.to_string()));
    }
    let probe = internal.join("probe");
    match std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&probe)
    {
        Ok(mut file) => {
            use std::io::Write;
            if let Err(error) = file.write_all(b"ok").and_then(|()| file.sync_all()) {
                return (false, Some(error.to_string()));
            }
            if let Err(error) = std::fs::remove_file(&probe) {
                return (false, Some(error.to_string()));
            }
            (true, None)
        }
        Err(error) => (false, Some(error.to_string())),
    }
}

fn sample_rdma_sysfs(
    rdma_required: bool,
    rdma_configured: bool,
    rdma_device: Option<&str>,
) -> (bool, Option<String>) {
    sample_rdma_sysfs_at(
        Path::new("/sys/class/infiniband"),
        rdma_required,
        rdma_configured,
        rdma_device,
    )
}

#[doc(hidden)]
pub fn sample_rdma_sysfs_at(
    infiniband_root: &Path,
    rdma_required: bool,
    rdma_configured: bool,
    rdma_device: Option<&str>,
) -> (bool, Option<String>) {
    if !rdma_configured {
        return if rdma_required {
            (false, Some("rdma_device is not configured".into()))
        } else {
            (false, None)
        };
    }
    let Some(device) = rdma_device else {
        return (false, Some("rdma_device is not configured".into()));
    };
    let state_path = infiniband_root
        .join(device)
        .join("ports")
        .join(RDMA_ACCEPTED_PORT)
        .join("state");
    match std::fs::read_to_string(&state_path) {
        Ok(state)
            if state
                .split_once(':')
                .map_or(state.trim(), |(_, token)| token.trim())
                == "ACTIVE" =>
        {
            (true, None)
        }
        Ok(state) => (
            false,
            Some(format!(
                "RDMA device {device} accepted port {RDMA_ACCEPTED_PORT} is not ACTIVE: {}",
                state.trim()
            )),
        ),
        Err(error) => (
            false,
            Some(format!(
                "RDMA device {device} accepted port {RDMA_ACCEPTED_PORT} state is not readable at {}: {error}",
                state_path.display()
            )),
        ),
    }
}

/// 组装并持有 Node 的所有入口。启动失败清理已经建立的资源，正常退出卸载本进程挂载。
pub async fn run(cfg: Config, obs: Observability) -> Result<(), BoxError> {
    run_node(cfg, obs, || {}).await
}

pub async fn run_with_shutdown(
    cfg: Config,
    obs: Observability,
    shutdown: crate::runtime::ShutdownTrigger,
) -> Result<(), BoxError> {
    run_node(cfg, obs, move || shutdown.arm()).await
}

async fn run_node(
    cfg: Config,
    obs: Observability,
    on_shutdown: impl FnOnce(),
) -> Result<(), BoxError> {
    #[cfg(feature = "ownerfs")]
    let owner_rpc_metrics = rpc::OwnerRpcMetrics::register(&obs.registry)?;
    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    let fuse_request_metrics = fuse::FuseRequestMetrics::register(&obs.registry)?;
    // Bind all TCP ingress before spawning services. A failed bind cannot leave a half-ready Node.
    let grpc = tokio::net::TcpListener::bind(cfg.grpc_listen).await?;
    let rest = tokio::net::TcpListener::bind(cfg.rest_listen).await?;
    // OwnerFs 生产路径必须先建立 Meta 会话和本机普通文件后端，不能挂载
    // 这里的 session ID 来自 Linux 内核随机源，也用于隔离 DFS 操作身份。
    let session_id = std::fs::read_to_string("/proc/sys/kernel/random/uuid")?
        .trim()
        .to_owned();
    let timeout = std::time::Duration::from_millis(cfg.timeout_ms);
    let needs_meta = cfg.ownerfs || cfg.dfs;
    let meta_endpoint = if needs_meta {
        Some(cfg.meta_endpoint.as_deref().ok_or_else(|| {
            afs_error::Error::coded(
                afs_error::CONFIG_INVALID,
                "OwnerFs and DFS require meta_endpoint",
            )
        })?)
    } else {
        None
    };
    let advertised = if needs_meta {
        Some(match cfg.advertise_endpoint.clone() {
            Some(value) => value,
            None if !cfg.grpc_listen.ip().is_unspecified() => format!("http://{}", cfg.grpc_listen),
            None => {
                return Err(afs_error::Error::coded(
                    afs_error::CONFIG_INVALID,
                    "Node listening on an unspecified address requires advertise_endpoint",
                )
                .into());
            }
        })
    } else {
        None
    };
    #[cfg(feature = "dfs")]
    let local_chunk_store = if cfg.dfs {
        Some(Arc::new(chunk::LocalChunkStore::open(
            cfg.data_dir.join("dfs"),
            cfg.id.clone(),
        )?))
    } else {
        None
    };
    let node_descriptor = meta_endpoint.map(|endpoint| {
        let advertised = advertised
            .clone()
            .expect("needs_meta sets advertised endpoint");
        let mut capabilities = Vec::new();
        if cfg.ownerfs {
            capabilities.push("ownerfs".into());
        }
        if cfg.dfs {
            capabilities.push("dfs".into());
        }
        (
            endpoint,
            afs_protocol::meta::NodeDescriptor {
                node_id: cfg.id.clone(),
                endpoint: Some(afs_protocol::meta::NodeEndpoint {
                    grpc_addr: advertised.clone(),
                    data_addr: advertised,
                    rest_addr: format!("http://{}", cfg.rest_listen),
                }),
                labels: std::collections::HashMap::new(),
                capabilities,
                session_id: session_id.clone(),
                storage_devices: {
                    #[cfg(feature = "dfs")]
                    {
                        local_chunk_store
                            .as_ref()
                            .map(|store| store.device_descriptor())
                            .transpose()
                            .expect("local ChunkStore was opened before Node registration")
                            .into_iter()
                            .map(|device| afs_protocol::meta::DfsStorageDevice {
                                device_id: device.device_id,
                                device_epoch: device.device_epoch,
                                catalog_revision: device.catalog_revision,
                                failure_domain: device.failure_domain,
                            })
                            .collect()
                    }
                    #[cfg(not(feature = "dfs"))]
                    {
                        Vec::new()
                    }
                },
            },
        )
    });
    let (registered_node_epoch, registered_node_at, registered_readiness) =
        if let Some((endpoint, descriptor)) = &node_descriptor {
            let registration = rpc::meta::register_node_with_readiness(
                endpoint,
                descriptor.clone(),
                timeout,
                cfg.tls_config(),
            )
            .await?;
            (
                registration.lease_epoch,
                Some(Instant::now()),
                Some(registration),
            )
        } else {
            (0, None, None)
        };
    #[cfg(not(feature = "dfs"))]
    let _ = registered_node_epoch;

    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    let peer_connections = Arc::new(rpc::peer::PeerConnectionPool::new(
        afs_transport::grpc::GrpcConfig {
            connect_timeout: timeout,
            request_timeout: timeout,
            ..Default::default()
        },
        cfg.tls_config(),
        64,
    )?);

    #[cfg(feature = "ownerfs")]
    let mut workspace_bind_root_control = None;
    #[cfg(feature = "ownerfs")]
    let ownerfs_instance = if cfg.ownerfs {
        let endpoint = meta_endpoint.expect("OwnerFs checked meta_endpoint");
        let root_meta = Arc::new(rpc::meta::GrpcRootMeta::new(
            endpoint,
            cfg.id.clone(),
            session_id.clone(),
            timeout,
            cfg.tls_config(),
        )?);
        let disk = Arc::new(storage::LocalFs::open(cfg.data_dir.join("ownerfs"))?);
        let recovery_disk = disk.clone();
        let recovery_node_id = cfg.id.clone();
        let recovery_session_id = session_id.clone();
        let owner_data_mode = match cfg.data_mode.as_str() {
            "grpc" => rpc::peer::DataMode::Grpc,
            "rdma" => rpc::peer::DataMode::Rdma,
            _ => rpc::peer::DataMode::Auto,
        };
        if owner_data_mode == rpc::peer::DataMode::Rdma && cfg.rdma_device.is_none() {
            return Err(afs_error::Error::coded(
                afs_error::CONFIG_INVALID,
                "OwnerFs RDMA requires rdma_device",
            )
            .into());
        }
        #[cfg(feature = "rdma")]
        if owner_data_mode == rpc::peer::DataMode::Rdma {
            let device = cfg
                .rdma_device
                .clone()
                .expect("OwnerFs RDMA checked rdma_device");
            tokio::task::spawn_blocking(move || afs_transport::rdma::RdmaEndpoint::open(&device))
                .await?
                .map_err(|error| {
                    afs_error::Error::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, error.to_string())
                })?;
        }
        let remote_factory = Arc::new(OwnerFilesFactory {
            meta: root_meta.clone(),
            peers: peer_connections.clone(),
            runtime: tokio::runtime::Handle::current(),
            metrics: owner_rpc_metrics.clone(),
            data_mode: owner_data_mode,
            rdma_device: cfg.rdma_device.clone(),
            timeout,
            rdma_admission: Arc::new(tokio::sync::Semaphore::new(
                rpc::peer::OWNER_RDMA_MAX_CLIENT_WINDOWS,
            )),
        });
        let recovery_root_meta = root_meta.clone();
        let roots = Arc::new(
            tokio::task::spawn_blocking(move || {
                vfs::ownerfs::root::RootManager::open(
                    recovery_node_id,
                    recovery_session_id,
                    recovery_root_meta,
                    recovery_disk,
                )
            })
            .await??,
        );
        if workspace_bind_root_commands_enabled(
            cfg.experimental_ownerfs_workspace_bind,
            cfg.experimental_native_workspace,
        ) {
            workspace_bind_root_control = Some((root_meta, roots.clone()));
        }
        let owner = if cfg.experimental_native_workspace || cfg.experimental_ownerfs_workspace_bind
        {
            vfs::ownerfs::OwnerFs::new_local_native_eligible_with_remote(
                roots,
                disk,
                remote_factory,
            )
        } else {
            vfs::ownerfs::OwnerFs::new_local_with_remote(roots, disk, remote_factory)
        };
        Some(Arc::new(owner))
    } else {
        None
    };

    #[cfg(feature = "dfs")]
    let dfs_meta = if cfg.dfs {
        Some(Arc::new(rpc::meta::GrpcDfsMeta::new(
            meta_endpoint.expect("DFS checked meta_endpoint"),
            cfg.id.clone(),
            session_id.clone(),
            crate::dfs::NamespaceId::new("default"),
            timeout,
            cfg.tls_config(),
        )?))
    } else {
        None
    };
    #[cfg(feature = "dfs")]
    let dfs_data_mode = parse_data_mode(&cfg.data_mode);
    #[cfg(feature = "dfs")]
    let dfs_rdma_pool = if cfg.dfs {
        if let Some(device) = dfs_rdma_startup_device(dfs_data_mode, cfg.rdma_device.as_deref())? {
            let peers = peer_connections.clone();
            Some(Arc::new(
                tokio::task::spawn_blocking(move || {
                    rpc::peer::DfsRdmaPool::new(peers, device, timeout)
                })
                .await??,
            ))
        } else {
            None
        }
    } else {
        None
    };
    #[cfg(feature = "dfs")]
    let dfs_replica_plane = rpc::peer::make_replica_data_plane(
        if cfg.dfs {
            dfs_data_mode
        } else {
            rpc::peer::DataMode::Grpc
        },
        peer_connections.clone(),
        timeout,
        dfs_rdma_pool.clone(),
    )?;
    #[cfg(feature = "dfs")]
    let dfs_instance = if cfg.dfs {
        let namespace = crate::dfs::NamespaceId::new("default");
        let meta = dfs_meta
            .as_ref()
            .expect("DFS Meta adapter was constructed")
            .clone();
        let chunks = local_chunk_store
            .as_ref()
            .expect("DFS local ChunkStore was opened before registration")
            .clone();
        let chunk_store = Arc::new(replication::DfsChunkStore::new_with_epoch(
            cfg.id.clone(),
            registered_node_epoch,
            chunks.clone(),
            meta.clone(),
            dfs_replica_plane.clone(),
        ));
        let read_config = dfs_read::DfsReadConfig {
            max_ops_per_batch: cfg.dfs_read_max_ops_per_batch,
            max_inflight_bytes: cfg.dfs_read_max_inflight_bytes,
            source_cache_ttl: std::time::Duration::from_millis(cfg.dfs_read_source_cache_ttl_ms),
        };
        read_config.validate()?;
        let read_engine = Arc::new(dfs_read::DfsReadEngine::new(
            namespace.clone(),
            cfg.id.clone(),
            chunks,
            meta.clone(),
            rpc::peer::make_chunk_transfer(
                dfs_data_mode,
                peer_connections.clone(),
                timeout,
                dfs_rdma_pool.clone(),
            )?,
            read_config,
        ));
        let remote_factory = Arc::new(rpc::peer::GrpcDfsOwnerFactory::new(
            peer_connections.clone(),
            timeout,
        ));
        Some(Arc::new(
            vfs::dfs::DistributedFs::new(
                namespace,
                cfg.id.clone(),
                session_id.clone(),
                meta,
                chunk_store,
                read_engine,
            )
            .with_remote_owner_factory(remote_factory),
        ))
    } else {
        None
    };

    // diagnostics is a separate test object directory, not an OwnerFs or DFS data path.
    let diagnostic_storage = Arc::new(storage::Storage::new(cfg.data_dir.join("diagnostics"))?);
    let sessions = rpc::control::RdmaSessionRegistry::new(cfg.rdma_device.clone());
    #[cfg(feature = "ownerfs")]
    let owner_rdma_sessions = rpc::control::RdmaSessionRegistry::new(cfg.rdma_device.clone());
    let local = api::local::serve_local_api_with_options(
        diagnostic_storage.clone(),
        &cfg.uds_path,
        api::local::LocalApiOptions {
            metrics_registry: Some(obs.registry.clone()),
        },
    )
    .await?;

    #[cfg(feature = "ownerfs")]
    let mounted_ownerfs = match (&cfg.ownerfs_mount, &ownerfs_instance) {
        (Some(path), Some(ownerfs)) => match fuse::mount_ownerfs_with_metrics(
            ownerfs.clone(),
            path,
            fuse_request_metrics.clone(),
        ) {
            Ok(session) => Some(session),
            Err(error) => {
                local.shutdown().await?;
                return Err(error.into());
            }
        },
        (Some(_), None) => {
            local.shutdown().await?;
            return Err(afs_error::Error::coded(
                afs_error::CONFIG_INVALID,
                "ownerfs_mount requires the OwnerFs backend",
            )
            .into());
        }
        (None, _) => None,
    };

    #[cfg(feature = "dfs")]
    let mounted_dfs = match (&cfg.dfs_mount, &dfs_instance) {
        (Some(path), Some(dfs)) => {
            match fuse::mount_dfs_with_metrics(dfs.clone(), path, fuse_request_metrics.clone()) {
                Ok(session) => Some(session),
                Err(error) => {
                    #[cfg(feature = "ownerfs")]
                    drop(mounted_ownerfs);
                    local.shutdown().await?;
                    return Err(error.into());
                }
            }
        }
        (Some(_), None) => {
            #[cfg(feature = "ownerfs")]
            drop(mounted_ownerfs);
            local.shutdown().await?;
            return Err(afs_error::Error::coded(
                afs_error::CONFIG_INVALID,
                "dfs_mount requires the DFS backend",
            )
            .into());
        }
        (None, _) => None,
    };

    let readiness = Arc::new(NodeReadiness::new(
        needs_meta,
        if needs_meta {
            Some(registered_node_epoch)
        } else {
            None
        },
        registered_node_at,
        cfg.ownerfs_mount.clone(),
        cfg.dfs_mount.clone(),
        &cfg.data_mode,
        cfg.rdma_device.as_deref(),
    ));
    readiness.set_allow_volatile_meta(cfg.allow_volatile_meta);
    if let Some(registration) = &registered_readiness {
        readiness.note_initial_registration_capability(registration);
    }

    let state = Arc::new(Node {
        config: cfg.clone(),
        observability: obs,
        session_id,
        readiness: readiness.clone(),
        #[cfg(feature = "ownerfs")]
        ownerfs: ownerfs_instance,
        #[cfg(feature = "dfs")]
        dfs: dfs_instance,
    });
    let mut services = Services::new();
    {
        let stop = services.stop.subscribe();
        let readiness = readiness.clone();
        let data_dir = cfg.data_dir.clone();
        services.spawn(async move {
            let mut tick = tokio::time::interval(READINESS_SAMPLER_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let shutdown = cancelled(stop);
            tokio::pin!(shutdown);
            loop {
                let _ = readiness.start_resource_sample(data_dir.clone());
                tokio::select! {
                    _ = &mut shutdown => return readiness.shutdown_resource_sampler().await.map_err(Into::into),
                    _ = tick.tick() => {}
                }
            }
        });
    }
    if let Some((endpoint, descriptor)) = node_descriptor {
        let endpoint = endpoint.to_owned();
        let timeout = std::time::Duration::from_millis(cfg.timeout_ms);
        let tls = cfg.tls_config();
        let stop = services.stop.subscribe();
        let readiness = readiness.clone();
        services.spawn(async move {
            // The Meta lease lasts longer than one refresh interval. A brief
            // Meta restart must not tear down the FUSE mount and all open FDs
            // just because one heartbeat raced the restart. Retry within the
            // lease window; fail closed if control cannot be restored in time.
            let mut last_success = tokio::time::Instant::now();
            let mut next_delay = std::time::Duration::from_secs(10);
            let shutdown = cancelled(stop);
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = tokio::time::sleep(next_delay) => {
                        match rpc::meta::register_node_with_readiness(
                            &endpoint,
                            descriptor.clone(),
                            timeout,
                            tls.clone(),
                        )
                        .await
                        {
                            Ok(registration) => {
                                if registration.lease_epoch != registered_node_epoch {
                                    readiness.note_registration_failure(
                                        "Node registration epoch changed; the running authority must stop",
                                    );
                                    return Err(afs_error::Error::coded(
                                        afs_error::IO_PERMISSION_DENIED,
                                        "Node registration epoch changed; the running authority must stop",
                                    ).into());
                                }
                                readiness.note_registration_capability(&registration);
                                last_success = tokio::time::Instant::now();
                                next_delay = std::time::Duration::from_secs(10);
                            }
                            Err(error) => {
                                readiness.note_registration_failure(error.to_string());
                                if !heartbeat_error_is_retryable(&error)
                                    || last_success.elapsed() >= std::time::Duration::from_secs(25)
                                {
                                    return Err(error.into());
                                }
                                afs_logging::warn!("node.meta_heartbeat_retry"; "error" => error.to_string());
                                next_delay = std::time::Duration::from_secs(1);
                            }
                        }
                    }
                }
            }
        });
    }
    #[cfg(feature = "ownerfs")]
    if let Some(ownerfs) = state.ownerfs.clone() {
        let stop = services.stop.subscribe();
        services.spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
            tick.tick().await;
            let shutdown = cancelled(stop);
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = tick.tick() => {
                        let fs = ownerfs.clone();
                        match tokio::task::spawn_blocking(move || fs.reap_expired_peer_sessions()).await {
                            Ok(Ok(count)) if count > 0 => {
                                afs_logging::info!("ownerfs.peer_handles_reaped"; "count" => count);
                            }
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => {
                                // Unknown Meta state is never evidence that a peer died.
                                afs_logging::warn!("ownerfs.peer_reaper_retry"; "error" => error.to_string());
                            }
                            Err(error) => {
                                afs_logging::warn!("ownerfs.peer_reaper_worker_failed"; "error" => error.to_string());
                            }
                        }
                    }
                }
            }
        });
    }
    #[cfg(feature = "dfs")]
    if let Some(dfs) = state.dfs.clone() {
        let stop = services.stop.subscribe();
        services.spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            tick.tick().await;
            let shutdown = cancelled(stop);
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = tick.tick() => {
                        let fs = dfs.clone();
                        match tokio::task::spawn_blocking(move || {
                            let mut first_error = None;
                            let committed = match fs.writeback_pending() {
                                Ok(count) => count,
                                Err(error) => {
                                    first_error.get_or_insert(error);
                                    0
                                }
                            };
                            let remote_releases = match fs.retry_pending_remote_releases() {
                                Ok(count) => count,
                                Err(error) => {
                                    first_error.get_or_insert(error);
                                    0
                                }
                            };
                            let owner_handles = match fs.reap_expired_peer_owner_handles() {
                                Ok(count) => count,
                                Err(error) => {
                                    first_error.get_or_insert(error);
                                    0
                                }
                            };
                            let lock_sessions = match fs.reap_expired_peer_lock_sessions() {
                                Ok(count) => count,
                                Err(error) => {
                                    first_error.get_or_insert(error);
                                    0
                                }
                            };
                            if let Some(error) = first_error {
                                return Err(error);
                            }
                            Ok::<_, afs_error::Error>((committed, remote_releases, owner_handles, lock_sessions))
                        }).await {
                            Ok(Ok((committed, remote_releases, owner_handles, lock_sessions)))
                                if committed > 0 || remote_releases > 0 || owner_handles > 0 || lock_sessions > 0 =>
                            {
                                if committed > 0 {
                                    afs_logging::info!("dfs.background_versions_committed"; "count" => committed);
                                }
                                if remote_releases > 0 {
                                    afs_logging::info!("dfs.remote_releases_retried"; "count" => remote_releases);
                                }
                                if owner_handles > 0 {
                                    afs_logging::info!("dfs.peer_owner_handles_reaped"; "count" => owner_handles);
                                }
                                if lock_sessions > 0 {
                                    afs_logging::info!("dfs.peer_lock_sessions_reaped"; "count" => lock_sessions);
                                }
                            }
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => {
                                afs_logging::warn!("dfs.background_retry"; "error" => error.to_string());
                            }
                            Err(error) => {
                                afs_logging::warn!("dfs.background_worker_failed"; "error" => error.to_string());
                            }
                        }
                    }
                }
            }
        });
    }
    // local API 自己持有 JoinHandle；这里监控它，避免 UDS 已死而 TCP 健康检查仍成功。
    let local_task = local
        .abort_handle()
        .expect("local API task exists after startup");
    let stop = services.stop.subscribe();
    services.spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
        tokio::pin! {let shutdown=cancelled(stop);}
        loop {
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = tick.tick() => if local_task.is_finished() {
                    return Err(std::io::Error::other("local SDK service exited unexpectedly").into());
                }
            }
        }
    });
    let stop = services.stop.subscribe();
    let grpc_config = afs_transport::grpc::GrpcConfig::default();
    let grpc_security = afs_transport::grpc::SecurityManager::new(cfg.tls_config())?;
    let grpc_server = grpc_security.configure_server(configure_node_tcp_server(
        &grpc_config,
        cfg.ownerfs,
        cfg.dfs,
    ))?;
    let incoming =
        grpc_config.configure_tcp_incoming(tonic::transport::server::TcpIncoming::from(grpc));
    // 业务 Handler 在 Node，公共 transport 只提供 builder 配置和低层搬运机制。
    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    let dfs_trusted = cfg
        .trusted_node_certs
        .iter()
        .map(|(node_id, path)| Ok((node_id.clone(), crate::config::read_certificate_der(path)?)))
        .collect::<afs_error::Result<Vec<_>>>()?;
    #[cfg(feature = "dfs")]
    let control = if let Some(dfs) = state.dfs.as_ref() {
        rpc::control::NodeControlService::with_dfs_owner(
            sessions.clone(),
            dfs.clone(),
            Arc::new(rpc::data::MtlsPeerAuthenticator::new(dfs_trusted.clone())?),
        )
    } else {
        rpc::control::NodeControlService::new(sessions.clone())
    };
    #[cfg(not(feature = "dfs"))]
    let control = rpc::control::NodeControlService::new(sessions.clone());
    #[cfg(feature = "ownerfs")]
    let control = if let Some(owner) = state.ownerfs.as_ref() {
        control
            .with_owner_locks(
                owner.peer_executor()?,
                Arc::new(rpc::data::MtlsPeerAuthenticator::new(dfs_trusted.clone())?),
            )
            .with_owner_rdma_registry(owner_rdma_sessions.clone())
    } else {
        control
    };
    let control = control.into_server();
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
    let owner_files = if let Some(ownerfs) = state.ownerfs.as_ref() {
        let trusted = cfg
            .trusted_node_certs
            .iter()
            .map(|(node_id, path)| {
                Ok((node_id.clone(), crate::config::read_certificate_der(path)?))
            })
            .collect::<afs_error::Result<Vec<_>>>()?;
        let authenticator = Arc::new(rpc::data::MtlsPeerAuthenticator::new(trusted)?);
        let handler = rpc::data::make_owner_files_handler(ownerfs.peer_executor()?);
        rpc::data::make_owner_files_server_with_handler_metrics_and_rdma(
            handler,
            authenticator,
            owner_rpc_metrics.clone(),
            owner_rdma_sessions.clone(),
        )
    } else {
        rpc::data::make_owner_files_server()
    };
    #[cfg(feature = "dfs")]
    let grpc_state = state.clone();
    services.spawn(async move {
        let router = grpc_server
            .layer(afs_tracing::GrpcServerTraceLayer::default())
            .add_service(control)
            .add_service(data);
        #[cfg(feature = "ownerfs")]
        let router = router.add_service(owner_files);
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
            router.add_service(dfs_chunks).add_service(dfs_owner_files)
        };
        router
            .serve_with_incoming_shutdown(incoming, cancelled(stop))
            .await
            .map_err(Into::into)
    });
    #[cfg(feature = "dfs")]
    if let (Some(meta), Some(local)) = (dfs_meta, local_chunk_store) {
        let worker = Arc::new(replication::ReplicationWorker::new(
            cfg.id.clone(),
            registered_node_epoch,
            state.session_id.clone(),
            local,
            meta,
            dfs_replica_plane,
        ));
        let stop = services.stop.subscribe();
        services.spawn(async move {
            let shutdown = cancelled(stop);
            tokio::pin!(shutdown);
            let mut delay = std::time::Duration::from_secs(1);
            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = tokio::time::sleep(delay) => {}
                }
                let worker = worker.clone();
                // The worker owns an exact pending request until its Meta
                // outcome is confirmed. A started blocking transfer is not
                // detached on a tick or replaced by another task.
                match tokio::task::spawn_blocking(move || worker.run_once()).await {
                    Ok(Ok(Some(task))) => {
                        afs_logging::info!("dfs.replication_task_reported";
                            "task" => task.id.0,
                            "state" => format!("{:?}", task.state),
                            "attempt" => task.attempt);
                        delay = std::time::Duration::from_millis(1);
                    }
                    Ok(Ok(None)) => delay = std::time::Duration::from_secs(1),
                    Ok(Err(error)) => {
                        afs_logging::warn!("dfs.replication_task_retry"; "error" => error.to_string());
                        delay = std::time::Duration::from_secs(1);
                    }
                    Err(error) => {
                        // A panic can invalidate the worker's exact state.
                        // Fail the service instead of launching a duplicate.
                        return Err(error.into());
                    }
                }
            }
        });
    }
    #[cfg(feature = "dfs")]
    let dfs_for_drain = state.dfs.clone();
    let stop = services.stop.subscribe();
    let rest_state = state.clone();
    services.spawn(async move {
        axum::serve(rest, api::rest::router(rest_state))
            .with_graceful_shutdown(cancelled(stop))
            .await
            .map_err(Into::into)
    });
    let stop = services.stop.subscribe();
    #[cfg(feature = "ownerfs")]
    let owner_rdma_cleanup = owner_rdma_sessions.clone();
    services.spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tokio::pin! {let shutdown=cancelled(stop);}
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    sessions.cleanup_expired().await;
                    #[cfg(feature = "ownerfs")]
                    owner_rdma_cleanup.cleanup_expired().await;
                }
                _ = &mut shutdown => break,
            }
        }
        Ok(())
    });
    #[cfg(feature = "ownerfs")]
    let mut native_workspace_startup_error = None;
    #[cfg(feature = "ownerfs")]
    let mut native_workspace_worker = None;
    #[cfg(feature = "ownerfs")]
    if cfg.experimental_native_workspace {
        let owner = state
            .ownerfs
            .clone()
            .expect("native config requires OwnerFs");
        let mount = cfg
            .ownerfs_mount
            .clone()
            .expect("native config requires mount");
        let native = cfg
            .native_workspace
            .clone()
            .expect("native config requires settings");
        let startup = tokio::task::spawn_blocking(move || {
            native_workspace::NativeWorkspace::start(owner, mount, native)
        })
        .await;
        (native_workspace_worker, native_workspace_startup_error) =
            register_native_workspace_startup(&mut services, startup);
    }
    #[cfg(feature = "ownerfs")]
    let mut workspace_bind_worker = None;
    #[cfg(feature = "ownerfs")]
    if cfg.experimental_ownerfs_workspace_bind {
        let owner = state
            .ownerfs
            .clone()
            .expect("workspace bind requires OwnerFs");
        let mount = cfg
            .ownerfs_mount
            .clone()
            .expect("workspace bind requires mount");
        let workspace = cfg
            .ownerfs_workspace_bind
            .as_ref()
            .expect("workspace bind requires settings")
            .workspace
            .clone();
        let startup = tokio::task::spawn_blocking(move || {
            WorkspaceBindWorker::start(owner, mount, workspace)
        })
        .await;
        match startup {
            Ok(Ok((worker, ready))) => {
                let monitor = worker.monitor(services.stop.subscribe());
                services.spawn(async move { monitor.await.map_err(Into::into) });
                workspace_bind_worker = Some(worker);
                if let Err(error) = ready {
                    native_workspace_startup_error = Some(error.into());
                }
            }
            Ok(Err(error)) => native_workspace_startup_error = Some(error.into()),
            Err(error) => native_workspace_startup_error = Some(error.into()),
        }
    }
    #[cfg(feature = "ownerfs")]
    if native_workspace_startup_error.is_none()
        && let Some((meta, roots)) = workspace_bind_root_control
    {
        let stop = services.stop.subscribe();
        services.spawn(run_workspace_bind_root_commands(meta, roots, stop));
    }
    #[cfg(feature = "ownerfs")]
    let mut shutdown_error = if let Some(error) = native_workspace_startup_error {
        Some(drain_started_services(services, on_shutdown, error).await)
    } else {
        afs_logging::info!("node.ready";"grpc"=>cfg.grpc_listen.to_string(),"rest"=>cfg.rest_listen.to_string(),"uds"=>cfg.uds_path.display().to_string(),"ownerfs"=>cfg.ownerfs,"dfs"=>cfg.dfs);
        services.run_with_shutdown(on_shutdown).await.err()
    };
    #[cfg(not(feature = "ownerfs"))]
    let mut shutdown_error = {
        afs_logging::info!("node.ready";"grpc"=>cfg.grpc_listen.to_string(),"rest"=>cfg.rest_listen.to_string(),"uds"=>cfg.uds_path.display().to_string(),"ownerfs"=>cfg.ownerfs,"dfs"=>cfg.dfs);
        services.run_with_shutdown(on_shutdown).await.err()
    };
    // A Services timeout may abort its observer, never the owned mount worker.
    // Keep FUSE alive until that same worker proves normal native closure. The
    // process-wide watchdog bounds a permanently busy or unresolved claim.
    #[cfg(feature = "ownerfs")]
    if let Some(worker) = native_workspace_worker {
        let closure = tokio::task::spawn_blocking(move || worker.shutdown()).await;
        let error: Option<BoxError> = match closure {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error.into()),
            Err(error) => Some(error.into()),
        };
        if let Some(error) = error {
            afs_logging::error!("ownerfs.workspace_closure_failed"; "error" => error.to_string());
            remember_shutdown_error(&mut shutdown_error, error);
            // No successful closure proof (including a worker panic): keep
            // FUSE owned until the process watchdog reports failure124.
            std::future::pending::<()>().await;
        }
    }
    #[cfg(feature = "ownerfs")]
    if let Some(worker) = workspace_bind_worker {
        let closure = tokio::task::spawn_blocking(move || worker.shutdown()).await;
        let error: Option<BoxError> = match closure {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error.into()),
            Err(error) => Some(error.into()),
        };
        if let Some(error) = error {
            afs_logging::error!("ownerfs.workspace_bind_closure_failed"; "error" => error.to_string());
            remember_shutdown_error(&mut shutdown_error, error);
            std::future::pending::<()>().await;
        }
    }
    // Stop FUSE admission and observe its thread cleanup before the final dirty drain.
    // Dropping BackgroundSession alone detaches the thread and cannot prove this boundary.
    #[cfg(feature = "dfs")]
    if let Some(mounted) = mounted_dfs {
        match tokio::task::spawn_blocking(move || mounted.join()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                afs_logging::error!("node.fuse_closure_failed"; "error" => error.to_string());
                remember_shutdown_error(&mut shutdown_error, error.into());
                std::future::pending::<()>().await;
            }
            Err(error) => {
                afs_logging::error!("node.fuse_closure_failed"; "error" => error.to_string());
                remember_shutdown_error(&mut shutdown_error, error.into());
                std::future::pending::<()>().await;
            }
        }
    }
    #[cfg(feature = "ownerfs")]
    if let Some(mounted) = mounted_ownerfs {
        match tokio::task::spawn_blocking(move || mounted.join()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                afs_logging::error!("node.fuse_closure_failed"; "error" => error.to_string());
                remember_shutdown_error(&mut shutdown_error, error.into());
                std::future::pending::<()>().await;
            }
            Err(error) => {
                afs_logging::error!("node.fuse_closure_failed"; "error" => error.to_string());
                remember_shutdown_error(&mut shutdown_error, error.into());
                std::future::pending::<()>().await;
            }
        }
    }
    #[cfg(feature = "dfs")]
    if let Some(dfs) = dfs_for_drain {
        match tokio::task::spawn_blocking(move || {
            let mut first_error = None;
            let committed = match dfs.drain() {
                Ok(count) => count,
                Err(error) => {
                    first_error.get_or_insert(error);
                    0
                }
            };
            let remote_releases = match dfs.drain_pending_remote_releases() {
                Ok(count) => count,
                Err(error) => {
                    first_error.get_or_insert(error);
                    0
                }
            };
            let owner_handles = match dfs.reap_expired_peer_owner_handles() {
                Ok(count) => count,
                Err(error) => {
                    first_error.get_or_insert(error);
                    0
                }
            };
            let lock_sessions = match dfs.reap_expired_peer_lock_sessions() {
                Ok(count) => count,
                Err(error) => {
                    first_error.get_or_insert(error);
                    0
                }
            };
            if let Some(error) = first_error {
                return Err(error);
            }
            Ok::<_, afs_error::Error>((committed, remote_releases, owner_handles, lock_sessions))
        })
        .await
        {
            Ok(Ok((committed, remote_releases, owner_handles, lock_sessions)))
                if committed > 0
                    || remote_releases > 0
                    || owner_handles > 0
                    || lock_sessions > 0 =>
            {
                if committed > 0 {
                    afs_logging::info!("dfs.node_drain_versions_committed"; "count" => committed);
                }
                if remote_releases > 0 {
                    afs_logging::info!("dfs.node_drain_remote_releases"; "count" => remote_releases);
                }
                if owner_handles > 0 {
                    afs_logging::info!("dfs.node_drain_owner_handles_reaped"; "count" => owner_handles);
                }
                if lock_sessions > 0 {
                    afs_logging::info!("dfs.node_drain_lock_sessions_reaped"; "count" => lock_sessions);
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                afs_logging::error!("dfs.node_drain_incomplete"; "error" => error.to_string());
                remember_shutdown_error(&mut shutdown_error, error.into());
            }
            Err(error) => {
                afs_logging::error!("dfs.node_drain_worker_failed"; "error" => error.to_string());
                remember_shutdown_error(&mut shutdown_error, error.into());
            }
        }
    }
    match tokio::time::timeout(std::time::Duration::from_secs(10), local.shutdown()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => remember_shutdown_error(&mut shutdown_error, error.into()),
        Err(error) => remember_shutdown_error(&mut shutdown_error, error.into()),
    }
    if let Some(error) = shutdown_error {
        afs_logging::error!("node.shutdown_failed"; "error" => error.to_string());
        return Err(error);
    }
    Ok(())
}

#[cfg(feature = "ownerfs")]
fn workspace_bind_root_commands_enabled(host: bool, managed: bool) -> bool {
    host || managed
}

/// In-session progress only: no persisted cursor or revoke ACK is produced.
/// A matching command rejects admission before Services starts normal closure.
#[cfg(feature = "ownerfs")]
fn process_workspace_bind_root_commands(
    after_revision: u64,
    batch: rpc::meta::RootCommandBatch,
    mut reject: impl FnMut(&rpc::meta::RootCommand) -> afs_error::Result<bool>,
) -> afs_error::Result<u64> {
    let rpc::meta::RootCommandBatch::Events {
        start_revision,
        next_revision,
        commands,
    } = batch
    else {
        return Err(afs_error::Error::coded(
            afs_error::NODE_OWNER_GRANT_UNAVAILABLE,
            "workspace bind RootCommand control requires recovery or is unsupported",
        ));
    };
    if start_revision != after_revision || next_revision <= start_revision {
        return Err(afs_error::Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "workspace bind RootCommand batch does not match the requested cursor",
        ));
    }
    for command in commands {
        if reject(&command)? {
            afs_logging::info!("ownerfs.workspace_bind_root_command_rejected";
                "command_id" => command.command_id,
                "revision" => command.revision,
                "root_id" => command.root_id,
                "root_epoch" => command.root_epoch,
                "access_generation" => command.access_generation);
            return Err(afs_error::Error::coded(
                afs_error::NODE_OWNER_GRANT_UNAVAILABLE,
                "workspace bind Home admission rejected by Meta RootCommand",
            ));
        }
        afs_logging::info!("ownerfs.workspace_bind_root_command_ignored";
            "command_id" => command.command_id,
            "revision" => command.revision,
            "root_id" => command.root_id);
    }
    // Filtered events still advance through the complete Meta history batch.
    Ok(next_revision - 1)
}

#[cfg(feature = "ownerfs")]
async fn run_workspace_bind_root_commands(
    meta: Arc<rpc::meta::GrpcRootMeta>,
    roots: Arc<vfs::ownerfs::root::RootManager>,
    stop: tokio::sync::watch::Receiver<bool>,
) -> Result<(), BoxError> {
    let shutdown = cancelled(stop.clone());
    tokio::pin!(shutdown);
    let mut after_revision = 0;
    let mut delay = Duration::ZERO;
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
        }
        let meta = meta.clone();
        // Join this exact blocking RPC before another poll or shutdown. Do not
        // race/cancel it on a tick and leave detached duplicate control calls.
        let result =
            tokio::task::spawn_blocking(move || meta.poll_root_command_batch(after_revision))
                .await?;
        if *stop.borrow() {
            return Ok(());
        }
        let next = result.and_then(|batch| {
            process_workspace_bind_root_commands(after_revision, batch, |command| {
                roots.revoke_matching_home(&vfs::ownerfs::root::HomeRootRevocation {
                    command_id: command.command_id.clone(),
                    root_id: vfs::ownerfs::root::RootId(command.root_id.clone()),
                    root_epoch: command.root_epoch,
                    home_node_id: command.home_node_id.clone(),
                    home_session_id: command.home_session_id.clone(),
                    access_generation: command.access_generation,
                })
            })
        });
        match next {
            Ok(next) => {
                delay = if next > after_revision {
                    Duration::ZERO
                } else {
                    Duration::from_millis(250)
                };
                after_revision = next;
            }
            Err(error) => {
                // Host ON deliberately fails closed on any untrusted control
                // result. Ordinary OFF retains its existing heartbeat policy.
                afs_logging::error!("ownerfs.workspace_bind_root_command_control_failed";
                    "after_revision" => after_revision,
                    "error" => error.to_string());
                return Err(error.into());
            }
        }
    }
}

#[cfg(feature = "ownerfs")]
struct WorkspaceBindWorker {
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<std::io::Result<()>>>,
    finished: tokio::sync::watch::Receiver<Option<(Option<i32>, String)>>,
}

#[cfg(feature = "ownerfs")]
impl WorkspaceBindWorker {
    // Even a failed activation returns the original worker to Node: attachment
    // may have succeeded before its postcondition failed. Never drop its claim.
    fn start(
        owner: Arc<vfs::ownerfs::OwnerFs>,
        mount: PathBuf,
        workspace: String,
    ) -> std::io::Result<(Self, std::io::Result<()>)> {
        use std::{ffi::OsStr, io, sync::mpsc, thread};
        use vfs::ownerfs::bind_mount::AuthorizedWorkspaceBind;
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (finished_tx, finished) = tokio::sync::watch::channel(None);
        // Inherit the Node startup namespace. No unshare/setns or runc here.
        let worker = thread::Builder::new()
            .name("afs-workspace-bind".into())
            .spawn(move || {
                let mut bind =
                    match AuthorizedWorkspaceBind::prepare(owner, &mount, OsStr::new(&workspace)) {
                        Ok(bind) => bind,
                        Err(error) => {
                            let _ = ready_tx.send(Err(io::Error::other(error.to_string())));
                            return Ok(());
                        }
                    };
                let mut failure = bind.activate().err();
                let _ = ready_tx.send(match &failure {
                    Some(error) => Err(io::Error::other(error.to_string())),
                    None => Ok(()),
                });
                while failure.is_none() && !worker_stop.load(Ordering::Acquire) {
                    if let Err(error) = bind.verify_current() {
                        failure = Some(error);
                        break;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
                // Notify before closure, so an unresolved claim cannot prevent
                // Node from starting its process-wide shutdown watchdog.
                if let Some(error) = &failure {
                    let _ = finished_tx.send(Some((error.raw_os_error(), error.to_string())));
                }
                loop {
                    match bind.detach() {
                        Ok(()) => return Ok(()),
                        Err(error) if error.raw_os_error() == Some(libc::EBUSY) => {
                            thread::sleep(Duration::from_millis(50));
                        }
                        Err(error) => {
                            let _ =
                                finished_tx.send(Some((error.raw_os_error(), error.to_string())));
                            // Retain both original descriptors/authority and FUSE;
                            // a terminal identity error is not successful teardown.
                            loop {
                                thread::park();
                            }
                        }
                    }
                }
            })?;
        let ready = ready_rx
            .recv()
            .unwrap_or_else(|error| Err(io::Error::other(error)));
        Ok((
            Self {
                stop,
                worker: Some(worker),
                finished,
            },
            ready,
        ))
    }

    fn monitor(
        &self,
        stop: tokio::sync::watch::Receiver<bool>,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send + 'static {
        let worker_stop = self.stop.clone();
        let mut finished = self.finished.clone();
        async move {
            tokio::select! {
                biased;
                _ = cancelled(stop) => {
                    worker_stop.store(true, Ordering::Release);
                    Ok(())
                }
                _ = finished.changed() => Err(match finished.borrow().as_ref() {
                    Some((Some(errno), _)) => std::io::Error::from_raw_os_error(*errno),
                    Some((None, message)) => std::io::Error::other(message.clone()),
                    None => std::io::Error::other("workspace bind worker exited unexpectedly"),
                }),
            }
        }
    }

    fn shutdown(mut self) -> std::io::Result<()> {
        self.stop.store(true, Ordering::Release);
        self.worker
            .take()
            .expect("owned workspace bind worker")
            .join()
            .map_err(|_| std::io::Error::other("workspace bind worker panicked"))?
    }
}

#[cfg(feature = "ownerfs")]
impl Drop for WorkspaceBindWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

#[cfg(all(test, feature = "ownerfs"))]
mod workspace_bind_root_command_tests {
    use super::*;
    use rpc::meta::{RootCommand, RootCommandBatch, RootCommandRecoveryReason, RootCommandType};

    #[test]
    fn workspace_bind_root_command_default_off_does_not_start_control() {
        assert!(!workspace_bind_root_commands_enabled(false, false));
    }

    #[test]
    fn workspace_bind_root_command_host_only_receives_control() {
        assert!(workspace_bind_root_commands_enabled(true, false));
    }

    #[test]
    fn workspace_bind_root_command_managed_only_receives_control() {
        assert!(workspace_bind_root_commands_enabled(false, true));
    }

    fn command(id: &str, revision: u64) -> RootCommand {
        RootCommand {
            command_id: id.into(),
            command_type: RootCommandType::RevokeAccess,
            revision,
            root_id: "workspace".into(),
            root_epoch: 7,
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            access_generation: 3,
        }
    }

    fn events(start: u64, next: u64, commands: Vec<RootCommand>) -> RootCommandBatch {
        RootCommandBatch::Events {
            start_revision: start,
            next_revision: next,
            commands,
        }
    }

    #[test]
    fn workspace_bind_root_command_filtered_and_idle_batches_preserve_full_progress() {
        let reject = |_: &RootCommand| panic!("no command should be applied");
        assert_eq!(
            process_workspace_bind_root_commands(10, events(10, 21, vec![]), reject).unwrap(),
            20
        );
        assert_eq!(
            process_workspace_bind_root_commands(20, events(20, 21, vec![]), reject).unwrap(),
            20
        );
    }

    #[test]
    fn workspace_bind_root_command_unrelated_tuples_are_processed_before_progress() {
        let mut seen = Vec::new();
        let next = process_workspace_bind_root_commands(
            10,
            events(10, 31, vec![command("old", 12), command("foreign", 20)]),
            |c| {
                seen.push(c.command_id.clone());
                Ok(false)
            },
        )
        .unwrap();
        assert_eq!(next, 30);
        assert_eq!(seen, ["old", "foreign"]);
    }

    #[test]
    fn workspace_bind_root_command_matching_refusal_stops_without_ack_or_further_admission() {
        let mut seen = Vec::new();
        let error = process_workspace_bind_root_commands(
            10,
            events(
                10,
                31,
                vec![
                    command("old", 12),
                    command("matching", 20),
                    command("later", 21),
                ],
            ),
            |c| {
                seen.push(c.command_id.clone());
                Ok(c.command_id == "matching")
            },
        )
        .unwrap_err();
        assert_eq!(seen, ["old", "matching"]);
        assert_eq!(error.code(), afs_error::NODE_OWNER_GRANT_UNAVAILABLE);
        assert!(
            error
                .to_string()
                .contains("Home admission rejected by Meta RootCommand")
        );
    }

    #[test]
    fn workspace_bind_root_command_invalid_or_replayed_cursor_cannot_touch_grants() {
        for (start, next) in [(9, 31), (10, 10), (10, 0)] {
            let error = process_workspace_bind_root_commands(
                10,
                events(start, next, vec![command("matching", 12)]),
                |_| panic!("invalid batch cannot touch a grant"),
            )
            .unwrap_err();
            assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
        }
    }

    #[test]
    fn workspace_bind_root_command_compaction_and_unsupported_do_not_restore_authority() {
        for batch in [
            RootCommandBatch::Compacted {
                requested_after: 10,
                compacted_to: 20,
                recovery_resume_after: 20,
                recovery_reason: RootCommandRecoveryReason::WatchCompacted,
            },
            RootCommandBatch::Unsupported {
                message: "not supported".into(),
            },
        ] {
            let error = process_workspace_bind_root_commands(10, batch, |_| {
                panic!("non-authorizing control result")
            })
            .unwrap_err();
            assert_eq!(error.code(), afs_error::NODE_OWNER_GRANT_UNAVAILABLE);
        }
    }

    #[test]
    fn workspace_bind_root_command_refusal_error_is_propagated_without_progress() {
        let error = process_workspace_bind_root_commands(
            10,
            events(10, 31, vec![command("matching", 12)]),
            |_| {
                Err(afs_error::Error::coded(
                    afs_error::NODE_OWNER_INVALID_GRANT,
                    "poisoned grant lock",
                ))
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_OWNER_INVALID_GRANT);
    }
}

#[cfg(all(test, feature = "ownerfs"))]
mod workspace_bind_worker_tests {
    use super::*;

    #[tokio::test]
    async fn workspace_bind_observer_reports_failure_without_joining_owned_worker() {
        let stop = Arc::new(AtomicBool::new(false));
        let (finished_tx, finished) = tokio::sync::watch::channel(None);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let owner = WorkspaceBindWorker {
            stop: stop.clone(),
            worker: Some(std::thread::spawn(move || {
                release_rx.recv().unwrap();
                Ok(())
            })),
            finished,
        };
        let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let observer = owner.monitor(cancel_rx);
        finished_tx
            .send(Some((Some(libc::ESTALE), "claim changed".into())))
            .unwrap();
        assert_eq!(
            observer.await.unwrap_err().raw_os_error(),
            Some(libc::ESTALE)
        );
        assert!(!owner.worker.as_ref().unwrap().is_finished());
        let mut closure = tokio::task::spawn_blocking(move || owner.shutdown());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut closure)
                .await
                .is_err()
        );
        assert!(stop.load(Ordering::Acquire));
        release_tx.send(()).unwrap();
        closure.await.unwrap().unwrap();
    }

    #[test]
    fn workspace_bind_failed_prepare_retains_worker_for_explicit_closure() {
        use vfs::ownerfs::native_home_tests::{fixture, mkdir_root};
        let (temp, owner, ctx, _disk) = fixture(true);
        mkdir_root(&owner, &ctx, "workspace");
        let (worker, ready) =
            WorkspaceBindWorker::start(Arc::new(owner), temp.path().into(), "workspace".into())
                .unwrap();
        assert!(ready.unwrap_err().to_string().contains("FUSE parent"));
        worker.shutdown().unwrap();
    }
}

#[cfg(feature = "ownerfs")]
fn register_native_workspace_startup(
    services: &mut Services,
    startup: Result<std::io::Result<native_workspace::NativeWorkspace>, tokio::task::JoinError>,
) -> (Option<native_workspace::NativeWorkspace>, Option<BoxError>) {
    match startup {
        Ok(Ok(worker)) => {
            let monitor = worker.monitor(services.stop.subscribe());
            services.spawn(async move { monitor.await.map_err(Into::into) });
            (Some(worker), None)
        }
        Ok(Err(error)) => {
            services.spawn(async {
                Err::<(), BoxError>(
                    std::io::Error::other(
                        "native workspace startup failed after node services started",
                    )
                    .into(),
                )
            });
            (None, Some(error.into()))
        }
        Err(error) => {
            services.spawn(async {
                Err::<(), BoxError>(
                    std::io::Error::other(
                        "native workspace startup task failed after node services started",
                    )
                    .into(),
                )
            });
            (None, Some(error.into()))
        }
    }
}

#[cfg(feature = "ownerfs")]
async fn drain_started_services(
    services: Services,
    on_shutdown: impl FnOnce(),
    startup_error: BoxError,
) -> BoxError {
    let mut shutdown_error = Some(startup_error);
    if let Err(error) = services.run_with_shutdown(on_shutdown).await {
        afs_logging::error!("node.startup_services_failed"; "error" => error.to_string());
        remember_shutdown_error(&mut shutdown_error, error);
    }
    shutdown_error.expect("startup error is preserved")
}

fn heartbeat_error_is_retryable(error: &afs_error::Error) -> bool {
    !matches!(
        error.kind(),
        afs_error::ErrorKind::InvalidArgument
            | afs_error::ErrorKind::PermissionDenied
            | afs_error::ErrorKind::Unauthenticated
    )
}

fn remember_shutdown_error(first: &mut Option<BoxError>, error: BoxError) {
    if first.is_none() {
        *first = Some(error);
    }
}

#[cfg(feature = "dfs")]
fn parse_data_mode(value: &str) -> rpc::peer::DataMode {
    match value {
        "grpc" => rpc::peer::DataMode::Grpc,
        "rdma" => rpc::peer::DataMode::Rdma,
        _ => rpc::peer::DataMode::Auto,
    }
}

#[cfg(feature = "dfs")]
fn dfs_rdma_startup_device(
    mode: rpc::peer::DataMode,
    device: Option<&str>,
) -> afs_error::Result<Option<String>> {
    match mode {
        rpc::peer::DataMode::Grpc => Ok(None),
        rpc::peer::DataMode::Rdma => {
            device
                .map(|value| value.to_owned())
                .map(Some)
                .ok_or_else(|| {
                    afs_error::Error::coded(
                        afs_error::CONFIG_INVALID,
                        "DFS RDMA requires rdma_device",
                    )
                })
        }
        rpc::peer::DataMode::Auto => {
            #[cfg(feature = "rdma")]
            {
                if let Some(value) = device {
                    Ok(Some(value.to_owned()))
                } else {
                    afs_logging::warn!(
                        "dfs.rdma_auto_disabled";
                        "reason" => "rdma_device is not configured"
                    );
                    Ok(None)
                }
            }
            #[cfg(not(feature = "rdma"))]
            {
                let _ = device;
                afs_logging::warn!(
                    "dfs.rdma_auto_disabled";
                    "reason" => "rdma feature is not compiled"
                );
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod shutdown_tests {
    #[cfg(feature = "ownerfs")]
    use std::time::Duration;

    #[test]
    fn heartbeat_retries_transport_failure_but_stops_expired_session() {
        assert!(super::heartbeat_error_is_retryable(
            &afs_error::Error::coded(
                afs_error::CLIENT_CONNECTION_UNAVAILABLE,
                "temporary transport outage",
            )
        ));
        assert!(!super::heartbeat_error_is_retryable(
            &afs_error::Error::coded(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "expired node session cannot be renewed; start a new session_id",
            )
        ));
        assert!(!super::heartbeat_error_is_retryable(
            &afs_error::Error::coded(
                afs_error::IO_PERMISSION_DENIED,
                "registration authority rejected",
            )
        ));
    }

    #[test]
    fn cleanup_keeps_first_failure_after_later_cleanup() {
        let mut first = None;
        super::remember_shutdown_error(&mut first, std::io::Error::other("drain failed").into());
        super::remember_shutdown_error(
            &mut first,
            std::io::Error::other("local shutdown failed").into(),
        );
        assert_eq!(first.unwrap().to_string(), "drain failed");
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_workspace_startup_io_failure_drains_services_and_keeps_original_errno() {
        let mut services = crate::runtime::Services::new();
        let stop = services.stop.subscribe();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (stop_seen_tx, stop_seen_rx) = tokio::sync::oneshot::channel();
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
        services.spawn(async move {
            let _ = started_tx.send(());
            super::cancelled(stop).await;
            let _ = stop_seen_tx.send(());
            let _ = cleanup_tx.send(());
            Err::<(), crate::runtime::BoxError>(
                std::io::Error::other("sibling cleanup failed").into(),
            )
        });
        expect_signal(started_rx).await;

        let startup_error = super::register_native_workspace_startup(
            &mut services,
            Ok(Err(std::io::Error::from_raw_os_error(13))),
        )
        .1
        .expect("startup error");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let final_error = tokio::time::timeout(
            Duration::from_secs(2),
            super::drain_started_services(
                services,
                move || {
                    let _ = shutdown_tx.send(());
                },
                startup_error,
            ),
        )
        .await
        .expect("bounded drain");

        expect_signal(shutdown_rx).await;
        expect_signal(stop_seen_rx).await;
        expect_signal(cleanup_rx).await;
        let io_error = final_error
            .downcast_ref::<std::io::Error>()
            .expect("original io error");
        assert_eq!(io_error.raw_os_error(), Some(13));
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_workspace_startup_join_error_drains_services_and_keeps_original_panic() {
        let mut services = crate::runtime::Services::new();
        let stop = services.stop.subscribe();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (stop_seen_tx, stop_seen_rx) = tokio::sync::oneshot::channel();
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
        services.spawn(async move {
            let _ = started_tx.send(());
            super::cancelled(stop).await;
            let _ = stop_seen_tx.send(());
            let _ = cleanup_tx.send(());
            Ok(())
        });
        expect_signal(started_rx).await;

        let startup = tokio::task::spawn_blocking(
            || -> std::io::Result<super::native_workspace::NativeWorkspace> {
                panic!("native workspace startup panic")
            },
        )
        .await;
        let startup_error = super::register_native_workspace_startup(&mut services, startup)
            .1
            .expect("startup join error");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let final_error = tokio::time::timeout(
            Duration::from_secs(2),
            super::drain_started_services(
                services,
                move || {
                    let _ = shutdown_tx.send(());
                },
                startup_error,
            ),
        )
        .await
        .expect("bounded drain");

        expect_signal(shutdown_rx).await;
        expect_signal(stop_seen_rx).await;
        expect_signal(cleanup_rx).await;
        let join_error = final_error
            .downcast_ref::<tokio::task::JoinError>()
            .expect("original join error");
        assert!(join_error.is_panic());
    }

    #[cfg(feature = "ownerfs")]
    async fn expect_signal<T>(rx: tokio::sync::oneshot::Receiver<T>) -> T {
        tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("bounded signal")
            .expect("sender kept")
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn dfs_auto_rdma_startup_uses_device_only_when_available() {
        assert_eq!(
            super::dfs_rdma_startup_device(crate::node::rpc::peer::DataMode::Grpc, Some("rxe0"))
                .unwrap(),
            None
        );
        assert!(
            super::dfs_rdma_startup_device(crate::node::rpc::peer::DataMode::Rdma, None).is_err()
        );

        #[cfg(feature = "rdma")]
        {
            assert_eq!(
                super::dfs_rdma_startup_device(
                    crate::node::rpc::peer::DataMode::Auto,
                    Some("rxe0")
                )
                .unwrap(),
                Some("rxe0".to_owned())
            );
            assert_eq!(
                super::dfs_rdma_startup_device(crate::node::rpc::peer::DataMode::Auto, None)
                    .unwrap(),
                None
            );
        }

        #[cfg(not(feature = "rdma"))]
        {
            assert_eq!(
                super::dfs_rdma_startup_device(
                    crate::node::rpc::peer::DataMode::Auto,
                    Some("rxe0")
                )
                .unwrap(),
                None
            );
        }
    }
}
