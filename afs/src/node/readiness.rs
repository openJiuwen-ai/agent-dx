//! Node readiness behind `/v1/ready`: Meta registration state, mount
//! identities and the data-device / RDMA resource observations, plus the
//! sampler (`ReadinessSampler`, T1) that keeps those observations fresh.
//!
//! Integration-test entries, compiled in because integration tests link the
//! product library without `cfg(test)`: `note_registration_success` and the
//! `*_for_tests` methods, used only by `tests/node_health_contract.rs`.

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
use super::fuse;
use super::rpc;
use crate::config::DataMode;
use crate::runtime::{Services, cancelled};
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
    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    pub(super) fn from_mounted(mounted: &fuse::MountedFuse, expected_source: &str) -> Self {
        let (path, mount_id) = mounted.identity();
        Self {
            path: path.to_path_buf(),
            mount_id: Some(mount_id.to_string()),
            expected_source: expected_source.to_owned(),
            capture_error: None,
        }
    }

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

impl NodeReadiness {
    #[must_use]
    pub fn new(
        meta_required: bool,
        registered_epoch: Option<u64>,
        registered_at: Option<Instant>,
        ownerfs_mount: Option<PathBuf>,
        dfs_mount: Option<PathBuf>,
        data_mode: DataMode,
        rdma_device: Option<&str>,
    ) -> Self {
        Self::new_with_mount_identities(
            meta_required,
            registered_epoch,
            registered_at,
            ownerfs_mount.map(|path| capture_afs_mount_identity(path, "afs-ownerfs")),
            dfs_mount.map(|path| capture_afs_mount_identity(path, "afs-dfs")),
            data_mode,
            rdma_device,
        )
    }

    pub(super) fn new_with_mount_identities(
        meta_required: bool,
        registered_epoch: Option<u64>,
        registered_at: Option<Instant>,
        ownerfs_mount: Option<AfsMountIdentity>,
        dfs_mount: Option<AfsMountIdentity>,
        data_mode: DataMode,
        rdma_device: Option<&str>,
    ) -> Self {
        let rdma_required = data_mode == DataMode::Rdma;
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
            ownerfs_mount,
            dfs_mount,
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
        let handle = crate::node::bridge::spawn_blocking(move || {
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
        let handle = crate::node::bridge::spawn_blocking(move || {
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

/// T1 `ReadinessSampler`: starts one resource sample (data device, RDMA
/// sysfs) every [`READINESS_SAMPLER_INTERVAL`], ticks delayed when missed.
///
/// Retry class: a sample that cannot start (`start_resource_sample` returns
/// `false`) is ignored and tried on the next tick; the sampler records its own
/// failures in the observations. Stop: on `services.stop` it returns
/// `shutdown_resource_sampler()`, which waits up to
/// [`READINESS_SAMPLER_SHUTDOWN_WAIT`] for a sample in flight; only that
/// shutdown result can end the service with an error. No log events of its own.
pub(super) struct ReadinessSampler {
    readiness: Arc<NodeReadiness>,
    data_dir: PathBuf,
}

impl ReadinessSampler {
    pub(super) fn new(readiness: Arc<NodeReadiness>, data_dir: PathBuf) -> Self {
        Self {
            readiness,
            data_dir,
        }
    }

    /// Start the sampler as one service; it stops with `services.stop`.
    pub(super) fn start(self, services: &mut Services) {
        let stop = services.stop.subscribe();
        let Self {
            readiness,
            data_dir,
        } = self;
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
