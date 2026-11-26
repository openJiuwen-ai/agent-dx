//! Private rollout tasks. Workers never own a Child; the supervisor owns generations.
use self::actor::Owner;
#[cfg(test)]
use self::actor::{CommitStage, Launch};
pub(crate) mod actor;
pub(crate) mod adxlet;
pub(crate) mod artifact;
pub(crate) mod relay;
use crate::config::{Deployment, Role};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_REQUEST_BYTES: u64 = 4096;
const MAX_JOURNAL_BYTES: u64 = 64 * 1024;
const MAX_ERROR_BYTES: usize = 1024;

pub(super) fn journal_error(code: &str, detail: &str) -> String {
    let mut end = detail.len().min(MAX_ERROR_BYTES);
    while !detail.is_char_boundary(end) {
        end -= 1;
    }
    let message = format!("{code}: {}", &detail[..end]);
    eprintln!("rollout {message}");
    message
}

/// Records an exit failure on an in-memory execution without discarding a
/// minted attempt/node_session identity. A failed durable store does not prove
/// the rename failed: identity A may already be visible to the Publisher and
/// approved by the Coordinator, so it must be preserved for an idempotent
/// replay instead of being replaced by a freshly minted B. Only a preparation
/// failure before any identity was minted legitimately keeps an empty attempt.
fn mark_exit_unknown(execution: &mut adxlet::Execution, detail: &str) {
    execution.stage = adxlet::Stage::Exiting;
    execution.error = Some(journal_error("execution_unknown", detail));
}

/// Records a Start failure without losing the replayable attempt. A ready
/// target whose Complete reply was lost keeps `Committing` so the
/// replay resolves it through Complete/Receipt and never respawns or stops it.
/// An in-memory `completed` whose durable store failed is equally only
/// commit-unknown: the actor already cleared its rollout reservation, so only
/// the Receipt path (same pid/attempt) can close it.
fn mark_start_unknown(execution: &mut adxlet::Execution, detail: &str) {
    execution.stage = match execution.stage {
        adxlet::Stage::Committing { candidate_pid } | adxlet::Stage::Selected { candidate_pid } => {
            adxlet::Stage::Committing { candidate_pid }
        }
        stage => adxlet::Stage::Starting {
            candidate_pid: stage.candidate_pid(),
        },
    };
    execution.error = Some(journal_error("execution_unknown", detail));
}

/// Records a failed Exit/Start worker outcome on the in-memory Task before the
/// worker's final store. Shared by the worker so tests exercise the same path.
fn record_step_failure(task: &mut Task, step: Step, detail: &str) {
    // A failure before Exiting is persisted is a Failed local exit; a
    // failure after Exiting is retained as Unknown so the operator can
    // recover the maintenance. The reap never claims success when its mint or
    // store fails.
    let post_intent = task.adxlet_execution.is_some();
    task.phase = if post_intent {
        Phase::Unknown
    } else {
        Phase::Failed
    };
    if let Some(execution) = task.adxlet_execution.as_mut() {
        if step == Step::Start {
            // The same attempt stays replayable: a spawned or possibly
            // selected Child is never stopped here, and a commit-unknown
            // (including an unpersisted `completed`) keeps its Receipt path.
            mark_start_unknown(execution, detail);
        } else if step == Step::Rollback {
            // A saved B is immutable even if its first store was ambiguous.
            // Before B, retain the fixed stop budget and A; a target that may
            // already be committed must keep its observing closure.
            if matches!(
                execution.stage,
                adxlet::Stage::Committing { .. } | adxlet::Stage::Selected { .. }
            ) || execution.leg.source().is_some()
            {
                mark_start_unknown(execution, detail);
            } else {
                execution.error = Some(journal_error("execution_unknown", detail));
            }
        } else {
            // A failed durable store does not undo the minted attempt/
            // node_session identity: the rename may already have exposed A to
            // the Publisher and Coordinator, so a replay re-Observes and
            // re-saves the same A instead of minting a different B. Any exit
            // failure is recorded as an unconfirmed journal entry, never as a
            // half-persisted `exited` success.
            mark_exit_unknown(execution, detail);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "action", deny_unknown_fields)]
pub enum Request {
    #[serde(rename = "rollout_node_info")]
    NodeInfo {},
    #[serde(rename = "rollout_submit")]
    Submit { submission: Box<Submission> },
    #[serde(rename = "rollout_query")]
    Query { task_id: String },
    /// Advance one local adxlet step of an existing prepared Task. The frozen
    /// source/incarnation always comes from the stored Task submission.
    #[serde(rename = "rollout_step")]
    Step {
        task_id: String,
        step: Step,
        attempt: String,
        node_session: String,
    },
}

/// Version of the local step protocol. A publisher must observe this exact
/// value in NodeInfo before any Submit: older adxctl releases run the whole
/// upgrade inside Submit and do not report it.
pub const STEP_PROTOCOL: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    Exit,
    Start,
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Standalone,
    Embedded,
}

#[derive(Clone, Serialize)]
pub struct Component {
    pub service_id: String,
    pub role: Role,
    pub node_id: String,
    pub mode: Mode,
    pub can_upgrade: bool,
    pub runtime: Option<ExpectedRuntime>,
    pub sources: Option<std::collections::BTreeMap<Role, crate::config::ProcessSource>>,
}

#[derive(Clone, Serialize)]
pub struct NodeInfo {
    pub schema_version: u32,
    pub step_protocol: u32,
    pub components: Vec<Component>,
    pub boot_nonce: String,
    pub pod_uid: Option<String>,
}

pub(crate) fn node_info(deployment: &Deployment) -> NodeInfo {
    let components = deployment
        .services
        .iter()
        .filter(|service| matches!(service.role, Role::Adxlet | Role::Relay))
        .filter_map(|service| {
            let mode = deployment.rollout_mode(service);
            let owner = if service.role == Role::Relay {
                // Pair through the same configured control socket, never by list
                // position or service name. Ambiguity deliberately yields no node ID.
                let mut owners = deployment.services.iter().filter(|owner| {
                    owner.role == Role::Adxlet
                        && deployment.rollout_mode(owner) == Mode::Standalone
                        && service
                            .env
                            .get("ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR")
                            .is_some_and(|directory| {
                                owner
                                    .config
                                    .get("proxy_socket")
                                    .and_then(serde_json::Value::as_str)
                                    .is_some_and(|socket| {
                                        std::path::Path::new(directory).join("route.sock")
                                            == std::path::Path::new(socket)
                                    })
                            })
                });
                let first = owners.next();
                if owners.next().is_none() {
                    first
                } else {
                    None
                }
            } else if service.role == Role::Adxlet {
                Some(service)
            } else {
                None
            };
            let node_id = owner
                .and_then(|owner| owner.config.get("node_id"))
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.is_empty())?;
            Some(Component {
                runtime: None,
                sources: None,
                can_upgrade: false,
                service_id: service.id.clone(),
                role: service.role,
                node_id: node_id.to_owned(),
                mode,
            })
        })
        .collect();
    NodeInfo {
        schema_version: 1,
        step_protocol: STEP_PROTOCOL,
        boot_nonce: String::new(),
        pod_uid: None,
        components,
    }
}

/// Local upgrade boundary for one adxlet service: a unique actual node ID,
/// an absolute standalone proxy socket, and exactly one managed Relay whose
/// route socket is that path. This is not a startup check; an external Relay
/// only means the adxlet has no local upgrade capability.
pub(crate) fn adxlet_step_pairing(deployment: &Deployment, service_id: &str) -> bool {
    let Some(owner) = deployment
        .services
        .iter()
        .find(|service| service.id == service_id && service.role == Role::Adxlet)
    else {
        return false;
    };
    let Some(node_id) = owner
        .config
        .get("node_id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.trim().is_empty())
    else {
        return false;
    };
    let same_node = deployment
        .services
        .iter()
        .filter(|service| {
            service.role == Role::Adxlet
                && service
                    .config
                    .get("node_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(node_id)
        })
        .count();
    let Some(socket) = owner
        .config
        .get("proxy_socket")
        .and_then(serde_json::Value::as_str)
        .map(std::path::Path::new)
        .filter(|socket| socket.is_absolute())
    else {
        return false;
    };
    let pairs = deployment
        .services
        .iter()
        .filter(|service| {
            service.role == Role::Relay
                && service
                    .env
                    .get("ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR")
                    .is_some_and(|directory| {
                        std::path::Path::new(directory).join("route.sock") == socket
                    })
        })
        .count();
    same_node == 1 && pairs == 1 && deployment.rollout_mode(owner) == Mode::Standalone
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Submission {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component_config: Option<crate::config::ConfigInput>,
    pub selection: Selection,
    pub task_id: String,
    pub role: Role,
    pub service_id: String,
    pub commit: String,
    /// SHA-256 of the role's fixed bin executable, not of a tarball.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_archive: Option<RemoteArchive>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<ProcessIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Selection {
    Publisher { expected: ExpectedRuntime },
    LocalManual,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteArchive {
    pub url: String,
    pub archive_sha256: String,
}

impl RemoteArchive {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        let url = reqwest::Url::parse(&self.url)?;
        if self.url.chars().any(char::is_whitespace)
            || self.url.contains('\\')
            || !self.url.starts_with("https://")
            || url.scheme() != "https"
            || url.host_str().is_none_or(str::is_empty)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || self.url.split_once("://").is_some_and(|(_, rest)| {
                rest.split(['/', '?', '#'])
                    .next()
                    .is_some_and(|authority| authority.contains('@'))
            })
            || !hex(&self.archive_sha256, 64)
        {
            return Err("invalid remote archive descriptor".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedRuntime {
    pub configuration_sha256: String,
    pub boot_nonce: String,
    pub pod_uid: Option<String>,
    pub pid: u32,
    pub executable_sha256: String,
    pub node_session: String,
    pub relay_session: String,
}
impl ExpectedRuntime {
    fn require_current(&self, actual: &Self) -> crate::Result<()> {
        if self != actual {
            return Err("selected runtime changed".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    pub node_id: String,
    pub identity: ProcessSelection,
}

/// Binary selection is private to the parent, never sent to Node or Coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessSelection {
    pub rollout_id: String,
    pub epoch: u64,
    pub old_node_session: String,
    pub old_relay_session: String,
    pub original_sha256: String,
}

fn safe_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}
pub(crate) fn hex(value: &str, size: usize) -> bool {
    value.len() == size
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl Submission {
    fn validate(&self) -> crate::Result<()> {
        if let Some(input) = &self.component_config {
            input.validate()?;
        }
        if let Some(remote) = &self.remote_archive {
            remote.validate()?;
        }
        if let Selection::Publisher { expected } = &self.selection {
            if expected.boot_nonce.is_empty()
                || expected.boot_nonce.len() > 128
                || expected.pid == 0
                || !hex(&expected.executable_sha256, 64)
                || !hex(&expected.configuration_sha256, 64)
                || expected
                    .pod_uid
                    .as_ref()
                    .is_some_and(|id| id.is_empty() || id.len() > 128)
                || [&expected.node_session, &expected.relay_session]
                    .iter()
                    .any(|s| s.is_empty() || s.len() > 128)
            {
                return Err("exact observed publisher runtime required".into());
            }
            if self.process.as_ref().is_some_and(|p| {
                p.identity.old_node_session != expected.node_session
                    || p.identity.old_relay_session != expected.relay_session
                    || p.identity.original_sha256 != expected.executable_sha256
            }) {
                return Err("frozen process and observed runtime differ".into());
            }
        }
        if !safe_id(&self.task_id)
            || !safe_id(&self.service_id)
            || !matches!(self.role, Role::Adxlet | Role::Relay)
            || !hex(&self.commit, 40)
            || self.sha256.as_ref().is_some_and(|digest| !hex(digest, 64))
            || (self.remote_archive.is_none() && self.sha256.is_none())
        {
            return Err("invalid submission identity".into());
        }
        match (&self.role, &self.process) {
            (Role::Relay, None) => (),
            (Role::Adxlet, Some(process)) => {
                let identity = &process.identity;
                if process.node_id.trim().is_empty()
                    || process.node_id.len() > 128
                    || identity.epoch == 0
                    || !hex(&identity.original_sha256, 64)
                    || [
                        &identity.rollout_id,
                        &identity.old_node_session,
                        &identity.old_relay_session,
                    ]
                    .iter()
                    .any(|id| id.trim().is_empty() || id.len() > 128)
                {
                    return Err("exact frozen process identity required".into());
                }
            }
            _ => return Err("frozen process identity is required only for adxlet".into()),
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Preparing,
    Running,
    Unknown,
    Completed,
    Failed,
    Interrupted,
}

async fn observe_runtime(
    sender: &tokio::sync::mpsc::Sender<actor::Command>,
    service: &str,
    node: &str,
    boot_nonce: &str,
    pod_uid: Option<String>,
) -> crate::Result<ExpectedRuntime> {
    observe_runtime_sources(sender, service, node, boot_nonce, pod_uid)
        .await
        .map(|(identity, _)| identity)
}

async fn observe_runtime_sources(
    sender: &tokio::sync::mpsc::Sender<actor::Command>,
    service: &str,
    node: &str,
    boot_nonce: &str,
    pod_uid: Option<String>,
) -> crate::Result<(
    ExpectedRuntime,
    std::collections::BTreeMap<Role, crate::config::ProcessSource>,
)> {
    use sha2::{Digest, Sha256};
    use std::{
        io::Read,
        time::{Duration, Instant},
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    tokio::time::timeout_at(deadline.into(), async {
        let inspect = || async {
            let (reply, result) = tokio::sync::oneshot::channel();
            tokio::time::timeout(
                Duration::from_secs(2),
                sender.send(actor::Command {
                    service_id: service.into(),
                    task_id: String::new(),
                    operation: actor::Operation::RuntimeInspect,
                    reply,
                    expires: deadline,
                    expected_pid: None,
                }),
            )
            .await??;
            tokio::time::timeout(Duration::from_secs(2), result).await??
        };
        let actor::Reply::Runtime {
            config,
            pid,
            mut executable,
            admin_pid,
            admin_socket,
            relay,
        } = inspect().await?
        else {
            return Err("runtime proof missing".into());
        };
        let mut digest = Sha256::new();
        let mut bytes = [0u8; 65536];
        loop {
            if Instant::now() >= deadline {
                return Err("executable observation timed out".into());
            }
            let count = executable.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            digest.update(&bytes[..count]);
        }
        if !hex(&config.configuration_sha256, 64) {
            return Err("runtime configuration identity unknown".into());
        }
        let channel = relay::channel(&admin_socket, admin_pid).await?;
        let proof =
            adx_protocol::relay::node_admin_service_client::NodeAdminServiceClient::new(channel)
                .inspect(adx_protocol::relay::AdxletRuntimeInspectRequest {})
                .await?
                .into_inner();
        if !proof.ready
            || proof.node_id != node
            || proof.node_session.is_empty()
            || proof.relay_session.is_empty()
        {
            return Err("actual Node identity unknown".into());
        }
        if let Some(socket) = relay {
            let channel = relay::channel(&socket, pid).await?;
            let binding =
                adx_protocol::relay::relay_service_client::RelayServiceClient::new(channel)
                    .get_binding_state(adx_protocol::relay::GetBindingStateRequest {})
                    .await?
                    .into_inner();
            if !binding.ready
                || binding.proxy_session_id != proof.relay_session
                || binding.controller_session_id != proof.route_owner
            {
                return Err("active Relay differs from owned Child".into());
            }
        }
        let actor::Reply::Runtime {
            pid: checked,
            admin_pid: checked_admin,
            config: checked_config,
            ..
        } = inspect().await?
        else {
            return Err("runtime recheck missing".into());
        };
        if checked != pid
            || checked_admin != admin_pid
            || checked_config.configuration_sha256 != config.configuration_sha256
            || checked_config.sources != config.sources
            || Instant::now() >= deadline
        {
            return Err("runtime changed during inspection".into());
        }
        Ok((
            ExpectedRuntime {
                configuration_sha256: config.configuration_sha256,
                boot_nonce: boot_nonce.into(),
                pod_uid,
                pid,
                executable_sha256: format!("{:x}", digest.finalize()),
                node_session: proof.node_session,
                relay_session: proof.relay_session,
            },
            config.sources,
        ))
    })
    .await?
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Task {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_finished"
    )]
    pub finished: Option<Finished>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared: Option<Prepared>,
    pub submission: Submission,
    pub phase: Phase,
    pub deadline_unix_ms: u64,
    pub execution: Option<relay::Execution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adxlet_execution: Option<adxlet::Execution>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Finished {
    pub unix_ms: u64,
    pub boot_nonce: String,
}

fn deserialize_finished<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Finished>, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(serde_json::from_value::<Finished>(value).ok())
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Prepared {
    pub configuration_sha256: String,
    pub binary_sha256: String,
}

impl Task {
    fn finish(&mut self, boot_nonce: &str) {
        if self.finished.is_none() && matches!(self.phase, Phase::Completed | Phase::Failed) {
            self.finished = adx_core::observation::now_millis()
                .ok()
                .map(|unix_ms| Finished {
                    unix_ms,
                    boot_nonce: boot_nonce.to_owned(),
                });
        }
    }
    fn prepared_sha256(&self) -> crate::Result<&str> {
        let prepared = self.prepared.as_ref().ok_or("prepared identity missing")?;
        if !hex(&prepared.configuration_sha256, 64) {
            return Err("prepared configuration identity invalid".into());
        }
        let digest = &prepared.binary_sha256;
        if !hex(digest, 64)
            || self
                .submission
                .sha256
                .as_ref()
                .is_some_and(|expected| expected != digest)
        {
            return Err("prepared binary identity invalid".into());
        }
        Ok(digest)
    }
}

struct Reservation {
    task_id: String,
    worker_finished: bool,
}

fn complete_worker(active: &std::sync::Mutex<Option<Reservation>>, task: &Task, stored: bool) {
    // No writes to this task are permitted after publishing finished.
    if let Ok(mut active) = active.lock() {
        if let Some(reservation) = active
            .as_mut()
            .filter(|reservation| reservation.task_id == task.submission.task_id)
        {
            reservation.worker_finished = true;
            if stored && matches!(task.phase, Phase::Failed | Phase::Completed) {
                *active = None;
            }
        }
    }
}

pub(crate) struct Tasks {
    deployment: Deployment,
    directory: std::path::PathBuf,
    releases: Option<std::path::PathBuf>,
    info: NodeInfo,
    active: std::sync::Arc<std::sync::Mutex<Option<Reservation>>>,
    stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    executor: Option<tokio::sync::mpsc::Sender<actor::Command>>,
    runtime: Option<tokio::runtime::Handle>,
}

fn check_socket_path(path: &std::path::Path) -> crate::Result<()> {
    if !path.is_absolute() {
        return Err("absolute upgrade socket path required".into());
    }
    let length = path.as_os_str().len();
    if length > 100 {
        return Err(format!("upgrade socket path is {length} bytes; limit is 100").into());
    }
    Ok(())
}

fn private_directory(path: &std::path::Path) -> crate::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    match std::fs::DirBuilder::new().mode(0o700).create(path) {
        Ok(()) => {
            std::fs::File::open(path.parent().ok_or("missing directory parent")?)?.sync_all()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(error) => return Err(error.into()),
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("private directory required".into());
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn store(directory: &std::path::Path, task: &Task) -> crate::Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    if task.prepared.is_some() || matches!(task.phase, Phase::Running | Phase::Completed) {
        task.prepared_sha256()?;
    }
    // Serialize and validate before touching either the temporary or prior state.
    let bytes = serde_json::to_vec(task)?;
    if bytes.len() > MAX_JOURNAL_BYTES as usize {
        return Err("task journal exceeds fixed limit".into());
    }
    let temporary = directory.join("state.partial");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(temporary, directory.join("state.json"))?;
    std::fs::File::open(directory)?.sync_all()?;
    Ok(())
}

fn load(directory: &std::path::Path) -> crate::Result<Task> {
    use std::{io::Read, os::unix::fs::OpenOptionsExt};
    if std::fs::symlink_metadata(directory)?
        .file_type()
        .is_symlink()
    {
        return Err("invalid task directory".into());
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(directory.join("state.json"))?;
    if !file.metadata()?.is_file() {
        return Err("invalid state file".into());
    }
    let mut bytes = Vec::new();
    file.take(MAX_JOURNAL_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_JOURNAL_BYTES as usize {
        return Err("invalid task state".into());
    }
    let task: Task = serde_json::from_slice(&bytes)?;
    task.submission.validate()?;
    if task.prepared.is_some()
        || task.execution.is_some()
        || task.adxlet_execution.is_some()
        || matches!(task.phase, Phase::Running | Phase::Completed)
    {
        task.prepared_sha256()?;
    }
    Ok(task)
}

fn random_id() -> crate::Result<String> {
    let mut nonce = [0u8; 32];
    std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom")?, &mut nonce)?;
    Ok(nonce.iter().map(|byte| format!("{byte:02x}")).collect())
}

impl Tasks {
    pub(crate) fn boot_nonce(&self) -> &str {
        &self.info.boot_nonce
    }
    pub(crate) fn new(deployment: &Deployment) -> crate::Result<Self> {
        let directory = deployment.state_dir.join("rollout");
        let package = &deployment.package_dir;
        let releases = package
            .parent()
            .filter(|parent| parent.file_name().is_some_and(|name| name == "releases"))
            .filter(|_| {
                package
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| hex(name, 40))
            })
            .map(std::path::Path::to_path_buf);
        let mut info = node_info(deployment);
        info.boot_nonce = random_id()?;
        info.pod_uid = std::env::var("ADX_POD_UID")
            .ok()
            .filter(|id| !id.is_empty());
        Ok(Self {
            deployment: deployment.clone(),
            directory,
            releases,
            info,
            active: Default::default(),
            stopped: Default::default(),
            executor: None,
            runtime: tokio::runtime::Handle::try_current().ok(),
        })
    }

    pub(crate) fn set_executor(&mut self, executor: tokio::sync::mpsc::Sender<actor::Command>) {
        self.executor = Some(executor);
    }
    pub(crate) fn cancel(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn stop_flag(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.stopped.clone()
    }

    pub(crate) fn response(&self, line: &str) -> serde_json::Value {
        use serde_json::json;
        let request = match serde_json::from_str::<Request>(line) {
            Ok(request) => request,
            Err(_) => return json!({"ok":false,"error":{"code":"invalid_request"}}),
        };
        if let Request::NodeInfo {} = request {
            let mut info = self.info.clone();
            for component in &mut info.components {
                if component.role == Role::Adxlet || component.mode == Mode::Standalone {
                    if let Ok((identity, sources)) =
                        self.observe_sources(&component.service_id, &component.node_id)
                    {
                        component.runtime = Some(identity);
                        component.sources = Some(sources);
                    }
                    component.can_upgrade = component.mode == Mode::Standalone
                        && component.runtime.is_some()
                        && self
                            .executor
                            .as_ref()
                            .is_some_and(|executor| !executor.is_closed())
                        && self.runtime.is_some()
                        && (component.role != Role::Adxlet
                            || adxlet_step_pairing(&self.deployment, &component.service_id));
                }
            }
            return json!({"ok":true,"node_info":info});
        }
        match self.handle(request) {
            Ok(task) => {
                // The preparation budget bounds only the prepare worker; a
                // prepared task waiting for its first step is not expired.
                let expired = task.phase == Phase::Preparing
                    && task.prepared.is_none()
                    && adx_core::observation::now_millis()
                        .is_ok_and(|now| now >= task.deadline_unix_ms);
                json!({"ok":true,"task":task,"preparation_budget_expired":expired})
            }
            Err(code) => json!({"ok":false,"error":{"code":code}}),
        }
    }

    fn observe(&self, service: &str, node: &str) -> crate::Result<ExpectedRuntime> {
        self.observe_sources(service, node)
            .map(|(identity, _)| identity)
    }

    fn observe_sources(
        &self,
        service: &str,
        node: &str,
    ) -> crate::Result<(
        ExpectedRuntime,
        std::collections::BTreeMap<Role, crate::config::ProcessSource>,
    )> {
        let sender = self.executor.as_ref().ok_or("actor unavailable")?;
        let runtime = self.runtime.as_ref().ok_or("runtime unavailable")?;
        runtime.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(6),
                observe_runtime_sources(
                    sender,
                    service,
                    node,
                    &self.info.boot_nonce,
                    self.info.pod_uid.clone(),
                ),
            )
            .await?
        })
    }

    /// Validates one step against the stored Task. The frozen source and
    /// incarnation come from `Task.submission`; the caller supplies only the
    /// attempt/session the step applies to. Never mutates the Task here.
    fn step(
        &self,
        task_id: &str,
        step: Step,
        attempt: &str,
        node_session: &str,
    ) -> std::result::Result<Task, &'static str> {
        if [attempt, node_session]
            .iter()
            .any(|value| value.len() > 128)
        {
            return Err("invalid_request");
        }
        let directory = self.directory.join(task_id);
        if !directory.try_exists().map_err(|_| "state_unavailable")? {
            return Err("not_found");
        }
        let task = load(&directory).map_err(|_| "state_unavailable")?;
        if task.submission.task_id != task_id {
            return Err("conflict");
        }
        if task.submission.role != Role::Adxlet || task.submission.process.is_none() {
            return Err("unsupported_component");
        }
        // Exit always uses the original empty attempt/session identity; it never
        // names the minted attempt/session recorded by an earlier reaped Exit.
        // Later steps echo the exact non-empty attempt/session from the Task.
        match (&task.adxlet_execution, step) {
            (_, Step::Exit) if attempt.is_empty() && node_session.is_empty() => (),
            (Some(execution), Step::Rollback)
                if execution.leg.source().is_some_and(|source| {
                    source.attempt == attempt && source.node_session == node_session
                }) => {}
            (Some(execution), Step::Start)
                if execution.leg == adxlet::ProcessLeg::Target
                    && !attempt.is_empty()
                    && execution.attempt == attempt
                    && execution.node_session == node_session => {}
            (Some(execution), Step::Rollback)
                if execution.leg == adxlet::ProcessLeg::Target
                    && !attempt.is_empty()
                    && execution.attempt == attempt
                    && execution.node_session == node_session => {}
            _ => return Err("conflict"),
        }
        if let Selection::Publisher { expected } = &task.submission.selection {
            if expected.boot_nonce != self.info.boot_nonce || expected.pod_uid != self.info.pod_uid
            {
                return Err("incarnation_conflict");
            }
        }
        if !adxlet_step_pairing(&self.deployment, &task.submission.service_id) {
            return Err("process_pairing_unavailable");
        }
        task.prepared_sha256().map_err(|_| "state_unavailable")?;
        Ok(task)
    }

    /// First Exit confirms the frozen original still owns the adxlet in its
    /// worker, then fences and observes asynchronously. A replay that already
    /// recorded a durable `exited` reap, or a still-running worker, returns the
    /// stored Task without any new side effect; an `Exiting` replay still
    /// carries the same minted attempt and resumes with it. Start runs only
    /// from a reaped Exit of the same attempt; its replays (lost reply, unknown
    /// spawn or commit) rerun the same attempt so the actor returns the one
    /// owned Child or the commit Receipt. Only an idle same-task Reservation
    /// may start work.
    fn begin_step(
        &self,
        mut task: Task,
        directory: &std::path::Path,
        step: Step,
        active: &mut Option<Reservation>,
    ) -> std::result::Result<Task, &'static str> {
        if matches!(task.phase, Phase::Completed | Phase::Failed) {
            return Ok(task);
        }
        let task_id = task.submission.task_id.clone();
        match step {
            // A durable `exited` reap already recorded a minted attempt and
            // closed the local Exit; a replay is read-only. An `Exiting`
            // replay may still carry the same minted A in memory because its
            // durable store failed; that replay continues with the same A.
            Step::Exit
                if task
                    .adxlet_execution
                    .as_ref()
                    .is_some_and(|execution| execution.stage != adxlet::Stage::Exiting) =>
            {
                return Ok(task);
            }
            Step::Start
                if task
                    .adxlet_execution
                    .as_ref()
                    .is_some_and(adxlet::Execution::startable) => {}
            Step::Start => return Err("conflict"),
            Step::Rollback
                if task
                    .adxlet_execution
                    .as_ref()
                    .is_some_and(|execution| execution.leg.source().is_some()) =>
            {
                // Re-persist the same B after an ambiguous mint/store, but
                // never race a live B worker with a stale loaded snapshot.
                if active.as_ref().is_some_and(|reservation| {
                    reservation.task_id == task_id && !reservation.worker_finished
                }) {
                    return Ok(task);
                }
                store(directory, &task).map_err(|_| "state_unavailable")?;
            }
            Step::Rollback
                if task.adxlet_execution.as_ref().is_some_and(|execution| {
                    !execution.attempt.is_empty()
                        && (execution.startable()
                            || matches!(execution.stage, adxlet::Stage::StoppingForRollback { .. }))
                }) => {}
            Step::Rollback => return Err("conflict"),
            _ => {}
        }
        let idle = active.as_ref().is_some_and(|reservation| {
            reservation.task_id == task_id && reservation.worker_finished
        });
        if active.as_ref().is_some_and(|reservation| {
            reservation.task_id == task_id && !reservation.worker_finished
        }) {
            return Ok(task);
        }
        if !idle {
            return Err("conflict");
        }
        let resume = task.adxlet_execution.is_some();
        let sender = self.executor.clone().ok_or("executor_unavailable")?;
        let stopped = self.stopped.clone();
        let runtime = self.runtime.clone().ok_or("runtime_unavailable")?;
        let boot_nonce = self.info.boot_nonce.clone();
        let pod_uid = self.info.pod_uid.clone();
        let worker_active = self.active.clone();
        let worker_directory = directory.to_path_buf();
        let mut worker_task = Task {
            finished: None,
            prepared: task.prepared.clone(),
            submission: task.submission.clone(),
            phase: task.phase,
            deadline_unix_ms: task.deadline_unix_ms,
            execution: None,
            adxlet_execution: task.adxlet_execution.clone(),
        };
        if std::thread::Builder::new()
            .name(format!(
                "rollout-{}",
                match step {
                    Step::Start => "start",
                    Step::Rollback => "rollback",
                    Step::Exit => "exit",
                }
            ))
            .spawn(move || {
                let result = runtime.block_on(async {
                    match step {
                        Step::Start => {
                            adxlet::start(&mut worker_task, &worker_directory, &sender, &stopped)
                                .await
                        }
                        Step::Rollback => {
                            adxlet::rollback(&mut worker_task, &worker_directory, &sender, &stopped)
                                .await
                        }
                        Step::Exit => {
                            adxlet::exit(
                                &mut worker_task,
                                &worker_directory,
                                &sender,
                                &stopped,
                                resume,
                                &boot_nonce,
                                pod_uid.clone(),
                            )
                            .await
                        }
                    }
                });
                if let Err(error) = result {
                    record_step_failure(&mut worker_task, step, &error.to_string());
                }
                worker_task.finish(&boot_nonce);
                // Mirror the Submit worker: only a durable store of a terminal
                // phase releases the reservation; an unwritten outcome stays
                // queryable and re-runnable instead of being closed as failed.
                let stored = store(&worker_directory, &worker_task).is_ok();
                if !stored {
                    // A store failure is a local observability event, not a
                    // recoverable journal field: log it directly instead of
                    // discarding a journal_error return value meant for
                    // `execution.error`.
                    eprintln!("rollout state_unavailable: exit outcome not durable");
                }
                complete_worker(&worker_active, &worker_task, stored);
            })
            .is_err()
        {
            if matches!(step, Step::Start | Step::Rollback) {
                // No worker ran, so no Start was sent: the stored reaped Exit or
                // unknown Start stays exactly replayable with the same attempt.
                return Err("state_unavailable");
            }
            let post_intent = task.adxlet_execution.is_some();
            if let Some(execution) = task.adxlet_execution.as_mut() {
                mark_exit_unknown(execution, "exit worker unavailable");
            }
            task.phase = if post_intent {
                Phase::Unknown
            } else {
                Phase::Failed
            };
            // Only a durable terminal store releases the reservation; an
            // unwritten Failed outcome stays recoverable, mirroring the worker
            // and Submit release guards.
            let stored = store(directory, &task).is_ok();
            if stored && matches!(task.phase, Phase::Failed | Phase::Completed) {
                *active = None;
            }
            return Err("state_unavailable");
        }
        if let Some(reservation) = active
            .as_mut()
            .filter(|reservation| reservation.task_id == task_id)
        {
            reservation.worker_finished = false;
        }
        // First Exit persists Exiting from inside its worker; a replay
        // already carries it in the loaded Task.
        Ok(task)
    }

    fn handle(&self, request: Request) -> std::result::Result<Task, &'static str> {
        use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
        let mut active = self.active.lock().map_err(|_| "state_unavailable")?;
        let (id, submission) = match request {
            Request::Submit { submission } => {
                submission.validate().map_err(|_| "invalid_request")?;
                if let Selection::Publisher { expected } = &submission.selection {
                    if expected.boot_nonce != self.info.boot_nonce
                        || expected.pod_uid != self.info.pod_uid
                    {
                        return Err("incarnation_conflict");
                    }
                }
                (submission.task_id.clone(), Some(*submission))
            }
            Request::Query { task_id } if safe_id(&task_id) => (task_id, None),
            Request::Step {
                task_id,
                step,
                attempt,
                node_session,
            } if safe_id(&task_id) => {
                let directory = self.directory.join(&task_id);
                let task = self.step(&task_id, step, &attempt, &node_session)?;
                return self.begin_step(task, &directory, step, &mut active);
            }
            _ => return Err("invalid_request"),
        };
        let directory = self.directory.join(&id);
        private_directory(&self.directory).map_err(|_| "state_unavailable")?;
        if directory.try_exists().map_err(|_| "state_unavailable")? {
            let mut task = load(&directory).map_err(|_| "state_unavailable")?;
            if task.submission.task_id != id
                || submission.as_ref().is_some_and(|s| s != &task.submission)
            {
                return Err("conflict");
            }
            let running = active
                .as_ref()
                .filter(|reservation| reservation.task_id == id);
            if running.is_some_and(|reservation| reservation.worker_finished)
                && task.phase == Phase::Unknown
                && task.submission.role == Role::Adxlet
            {
                if let (Some(execution), Some(identity), Some(sender), Some(runtime)) = (
                    task.adxlet_execution.as_ref(),
                    task.submission.process.as_ref(),
                    self.executor.as_ref(),
                    self.runtime.as_ref(),
                ) {
                    if let adxlet::Stage::Committing { candidate_pid } = execution.stage {
                        let phase = execution.leg.selected_phase();
                        // handle runs on rollout-state, never on the Child actor.
                        // The reservation lock excludes all other journal writers.
                        let observed = runtime.block_on(async {
                            tokio::time::timeout(
                                Duration::from_secs(2),
                                actor::command(
                                    sender,
                                    &task,
                                    actor::Operation::Receipt {
                                        owner: Owner::Adxlet(identity.clone()),
                                        old_pid: execution.old_pid,
                                        attempt: execution.attempt.clone(),
                                        pid: Some(candidate_pid),
                                    },
                                    Instant::now() + Duration::from_secs(2),
                                ),
                            )
                            .await
                        });
                        if matches!(observed, Ok(Ok(actor::Reply::Committed))) {
                            task.prepared_sha256().map_err(|_| "state_unavailable")?;
                            task.phase = phase;
                            task.finish(&self.info.boot_nonce);
                            if let Some(execution) = task.adxlet_execution.as_mut() {
                                execution.stage = adxlet::Stage::Selected { candidate_pid };
                                execution.error = None;
                            }
                            store(&directory, &task).map_err(|_| "state_unavailable")?;
                            *active = None;
                        }
                    }
                }
                return Ok(task);
            }
            if running.is_some_and(|reservation| reservation.worker_finished)
                && matches!(task.phase, Phase::Completed | Phase::Failed)
            {
                // The worker durably wrote a terminal phase but its final
                // finish/store failed, so it kept the reservation. Re-store the
                // finished record; only a durable store releases it.
                task.finish(&self.info.boot_nonce);
                store(&directory, &task).map_err(|_| "state_unavailable")?;
                *active = None;
                return Ok(task);
            }
            if matches!(task.phase, Phase::Running | Phase::Unknown) && running.is_none() {
                task.phase = Phase::Unknown;
                store(&directory, &task).map_err(|_| "state_unavailable")?;
            }
            if task.phase == Phase::Preparing && running.is_none() {
                task.phase = Phase::Interrupted;
                store(&directory, &task).map_err(|_| "state_unavailable")?;
            }
            return Ok(task);
        }
        let submission = submission.ok_or("not_found")?;
        if !self.info.components.iter().any(|c| {
            c.service_id == submission.service_id
                && c.role == submission.role
                && c.mode == Mode::Standalone
        }) {
            return Err("unsupported_component");
        }
        if let Some(process) = &submission.process {
            if !self.info.components.iter().any(|component| {
                component.service_id == submission.service_id
                    && component.role == Role::Adxlet
                    && component.node_id == process.node_id
            }) {
                return Err("conflict");
            }
        }
        let releases = self.releases.clone();
        if submission.remote_archive.is_none() && releases.is_none() {
            return Err("unsupported_installation");
        }
        let executor = self
            .executor
            .as_ref()
            .filter(|executor| !executor.is_closed())
            .cloned()
            .ok_or("executor_unavailable")?;
        let runtime = self.runtime.clone().ok_or("runtime_unavailable")?;
        if submission.role == Role::Adxlet
            && !adxlet_step_pairing(&self.deployment, &submission.service_id)
        {
            return Err("process_pairing_unavailable");
        }
        if let Selection::Publisher { expected } = &submission.selection {
            let component = self
                .info
                .components
                .iter()
                .find(|c| c.service_id == submission.service_id)
                .ok_or("unsupported_component")?;
            let actual = self
                .observe(&submission.service_id, &component.node_id)
                .map_err(|_| "runtime_unknown")?;
            expected
                .require_current(&actual)
                .map_err(|_| "runtime_conflict")?;
        }
        if active.is_some() {
            return Err("busy");
        }
        for entry in std::fs::read_dir(&self.directory).map_err(|_| "state_unavailable")? {
            let entry = entry.map_err(|_| "state_unavailable")?;
            if entry.file_type().map_err(|_| "state_unavailable")?.is_dir() {
                let previous = load(&entry.path()).map_err(|_| "state_unavailable")?;
                if matches!(previous.phase, Phase::Running | Phase::Unknown) {
                    return Err("busy");
                }
            }
        }
        // Start before the first preparation write or queued blocking work.
        let deadline = Instant::now() + Duration::from_secs(900);
        let deadline_unix_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| "clock_error")?
                .as_millis(),
        )
        .map_err(|_| "clock_error")?
        .checked_add(900_000)
        .ok_or("clock_error")?;
        private_directory(&directory).map_err(|_| "state_unavailable")?;
        let task = Task {
            finished: None,
            prepared: None,
            submission,
            phase: Phase::Preparing,
            deadline_unix_ms,
            execution: None,
            adxlet_execution: None,
        };
        if store(&directory, &task).is_err() {
            let _ = std::fs::remove_file(directory.join("state.partial"));
            let _ = std::fs::remove_file(directory.join("state.json"));
            let _ = std::fs::remove_dir(&directory);
            return Err("state_unavailable");
        }
        *active = Some(Reservation {
            task_id: id,
            worker_finished: false,
        });
        let worker_active = self.active.clone();
        let worker_task = Task {
            finished: None,
            prepared: None,
            submission: task.submission.clone(),
            phase: task.phase,
            deadline_unix_ms,
            execution: None,
            adxlet_execution: None,
        };
        let stopped = self.stopped.clone();
        let boot_nonce = self.info.boot_nonce.clone();
        let pod_uid = self.info.pod_uid.clone();
        let node_id = self
            .info
            .components
            .iter()
            .find(|c| c.service_id == task.submission.service_id)
            .map(|c| c.node_id.clone())
            .unwrap_or_default();
        let failed_directory = directory.clone();
        let deployment = self.deployment.clone();
        if std::thread::Builder::new()
            .name("rollout-worker".into())
            .spawn(move || {
                let mut task = worker_task;
                let mut observation_unknown = false;
                let mut target_process = None;
                let result = (|| -> crate::Result<artifact::PreparedBinary> {
                    if task.submission.remote_archive.is_some() {
                        artifact::remote::prepare(
                            &directory,
                            &task.submission,
                            deadline,
                            &stopped,
                            &runtime,
                        )
                    } else {
                        artifact::prepare_cancellable(
                            releases
                                .as_deref()
                                .ok_or("installed releases unavailable")?,
                            &task.submission,
                            &directory.join("executable"),
                            deadline,
                            &stopped,
                        )
                    }
                })()
                .and_then(|binary| {
                    let current = runtime.block_on(actor::command(
                        &executor,
                        &task,
                        actor::Operation::RuntimeInspect,
                        deadline,
                    ))?;
                    let actor::Reply::Runtime { config, .. } = current else {
                        return Err("current process snapshot missing".into());
                    };
                    let mut process = crate::config::prepare_component(
                        &deployment,
                        &config,
                        task.submission.component_config.as_ref(),
                        &directory.join("configuration"),
                        deadline,
                        &stopped,
                    )?;
                    process.binary = directory.join("executable");
                    let source = process.sources.entry(task.submission.role).or_default();
                    source.version = Some(binary.commit.clone());
                    source.archive_sha256 = task
                        .submission
                        .remote_archive
                        .as_ref()
                        .map(|archive| archive.archive_sha256.clone());
                    let prepared = Prepared {
                        binary_sha256: binary.sha256,
                        configuration_sha256: process.configuration_sha256.clone(),
                    };
                    if task.submission.role == Role::Adxlet {
                        // Target and original are both owned before prepared is published.
                        runtime.block_on(adxlet::prepare(
                            &task,
                            &directory,
                            &executor,
                            &stopped,
                            deadline,
                            &prepared.binary_sha256,
                            &process,
                        ))?;
                    }
                    task.prepared = Some(prepared);
                    target_process = Some(process);
                    task.prepared_sha256()?;
                    store(&directory, &task)?;
                    if let Selection::Publisher { expected } = &task.submission.selection {
                        observation_unknown = true;
                        let actual = runtime.block_on(async {
                            tokio::time::timeout(
                                std::time::Duration::from_secs(6),
                                observe_runtime(
                                    &executor,
                                    &task.submission.service_id,
                                    &node_id,
                                    &boot_nonce,
                                    pod_uid.clone(),
                                ),
                            )
                            .await?
                        })?;
                        expected.require_current(&actual)?;
                        observation_unknown = false;
                    }
                    Ok(())
                });
                let result = result.and_then(|()| {
                    if Instant::now() >= deadline {
                        return Err("preparation deadline exceeded".into());
                    }
                    match task.submission.role {
                        // Prepare-only: the Task stays Preparing with prepared
                        // set and keeps the Reservation for the local steps.
                        Role::Adxlet => Ok(()),
                        Role::Relay => runtime.block_on(relay::execute(
                            target_process.as_ref().ok_or("target process missing")?,
                            &mut task,
                            &directory,
                            &executor,
                            &stopped,
                            deadline,
                        )),
                        _ => Err("unsupported rollout role".into()),
                    }
                });
                if let Err(error) = result {
                    // An adxlet Submit never fences or stops: every failure is Failed.
                    task.phase = if task.submission.role != Role::Adxlet
                        && (observation_unknown
                            || matches!(task.phase, Phase::Running | Phase::Unknown))
                    {
                        Phase::Unknown
                    } else {
                        Phase::Failed
                    };
                    if let Some(execution) = task.adxlet_execution.as_mut() {
                        execution.error =
                            Some(journal_error("execution_unknown", &error.to_string()));
                    }
                    if task
                        .execution
                        .as_ref()
                        .is_some_and(|execution| execution.phase.terminal_phase().is_none())
                    {
                        task.phase = Phase::Unknown;
                    }
                }
                if let Some(phase) = task
                    .execution
                    .as_ref()
                    .and_then(|execution| execution.phase.terminal_phase())
                {
                    task.phase = phase;
                }
                task.finish(&boot_nonce);
                let stored = store(&directory, &task).is_ok();
                complete_worker(&worker_active, &task, stored);
            })
            .is_err()
        {
            let mut failed = task;
            failed.phase = Phase::Failed;
            failed.finish(&self.info.boot_nonce);
            let stored = store(&failed_directory, &failed);
            *active = None;
            stored.map_err(|_| "state_unavailable")?;
            return Err("worker_unavailable");
        }
        Ok(task)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn observed_image_sources_are_explicitly_empty_but_failed_observation_is_null() {
        let mut component = node_info(&deployment()).components.remove(0);
        assert!(serde_json::to_value(&component).unwrap()["sources"].is_null());
        component.sources = Some(std::collections::BTreeMap::new());
        assert_eq!(
            serde_json::to_value(&component).unwrap()["sources"],
            serde_json::json!({})
        );
    }

    #[test]
    fn prepared_identity_is_required_and_submission_is_not_backfilled() {
        let submission: super::Submission = serde_json::from_value(serde_json::json!({
            "selection":{"mode":"local_manual"},"task_id":"derived","role":"relay","service_id":"relay",
            "commit":"a".repeat(40),"remote_archive":{"url":"https://example.invalid/package.tar.gz","archive_sha256":"c".repeat(64)}
        })).unwrap();
        submission.validate().unwrap();
        let original = serde_json::to_value(&submission).unwrap();
        let mut task = super::Task {
            finished: None,
            submission,
            prepared: None,
            phase: super::Phase::Preparing,
            deadline_unix_ms: 0,
            execution: None,
            adxlet_execution: None,
        };
        assert!(task.prepared_sha256().is_err());
        task.prepared = Some(super::Prepared {
            configuration_sha256: "e".repeat(64),
            binary_sha256: "b".repeat(64),
        });
        assert_eq!(task.prepared_sha256().unwrap(), "b".repeat(64));
        assert_eq!(serde_json::to_value(&task.submission).unwrap(), original);
        task.submission.sha256 = Some("d".repeat(64));
        assert!(task.prepared_sha256().is_err());
        task.submission.sha256 = None;
        task.submission.remote_archive = None;
        assert!(task.submission.validate().is_err());
        task.phase = Phase::Completed;
        task.finish("boot");
        let first = serde_json::to_value(&task.finished).unwrap();
        task.finish("other");
        assert_eq!(serde_json::to_value(&task.finished).unwrap(), first);
    }
    use super::*;

    /// Prepared, unfinished journal entry with no execution receipts.
    fn prepared_task(submission: Submission, phase: Phase, deadline_unix_ms: u64) -> Task {
        Task {
            finished: None,
            prepared: Some(Prepared {
                configuration_sha256: "e".repeat(64),
                binary_sha256: "b".repeat(64),
            }),
            submission,
            phase,
            deadline_unix_ms,
            execution: None,
            adxlet_execution: None,
        }
    }

    #[test]
    fn finished_worker_receipt_converges_both_legs_only_after_durable_store() {
        let runtime = test_runtime(1);
        for original in [false, true] {
            let phase = if original {
                Phase::Failed
            } else {
                Phase::Completed
            };
            let final_stage = adxlet::Stage::Selected { candidate_pid: 43 };
            let root = tempfile::tempdir().unwrap();
            let mut deployment = deployment();
            deployment.state_dir = root.path().into();
            let mut tasks = Tasks::new(&deployment).unwrap();
            tasks.runtime = Some(runtime.handle().clone());
            let (sender, mut receiver) = tokio::sync::mpsc::channel::<actor::Command>(4);
            tasks.set_executor(sender);
            private_directory(&tasks.directory).unwrap();
            let directory = tasks.directory.join("receipt");
            private_directory(&directory).unwrap();
            let identity = ProcessIdentity {
                node_id: "node".into(),
                identity: ProcessSelection {
                    rollout_id: "rollout".into(),
                    epoch: 1,
                    old_node_session: "old".into(),
                    old_relay_session: "relay".into(),
                    original_sha256: "a".repeat(64),
                },
            };
            let submission = Submission {
                component_config: None,
                selection: Selection::LocalManual,
                task_id: "receipt".into(),
                role: Role::Adxlet,
                service_id: "node".into(),
                commit: "c".repeat(40),
                sha256: Some("b".repeat(64)),
                remote_archive: None,
                process: Some(identity.clone()),
            };
            let mut task = prepared_task(submission.clone(), Phase::Unknown, 1);
            task.adxlet_execution = Some(adxlet::Execution {
                old_pid: 42,
                error: Some("lost ACK".into()),
                leg: if original {
                    adxlet::ProcessLeg::Rollback {
                        from: adxlet::AttemptIdentity {
                            attempt: "A".into(),
                            node_session: "session-A".into(),
                        },
                    }
                } else {
                    adxlet::ProcessLeg::Target
                },
                ..target_execution(adxlet::Stage::Committing { candidate_pid: 43 }, 1)
            });
            store(&directory, &task).unwrap();
            *tasks.active.lock().unwrap() = Some(Reservation {
                task_id: "receipt".into(),
                worker_finished: false,
            });
            let before = std::fs::read(directory.join("state.json")).unwrap();
            assert_eq!(
                tasks
                    .handle(Request::Query {
                        task_id: "receipt".into()
                    })
                    .unwrap()
                    .phase,
                Phase::Unknown
            );
            assert!(receiver.try_recv().is_err());
            assert_eq!(std::fs::read(directory.join("state.json")).unwrap(), before);
            tasks
                .active
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .worker_finished = true;
            let actor = runtime.spawn(async move {
                for index in 0..3 {
                    let command = receiver.recv().await.unwrap();
                    assert_eq!(command.task_id, "receipt");
                    match command.operation {
                        actor::Operation::Receipt {
                            owner: Owner::Adxlet(actual),
                            old_pid,
                            attempt,
                            pid,
                        } => {
                            assert_eq!(actual, identity);
                            assert_eq!((old_pid, pid, attempt.as_str()), (42, Some(43), "attempt"));
                        }
                        _ => panic!("query must only observe Receipt"),
                    }
                    let reply = if index == 0 {
                        Err("receipt unknown".into())
                    } else {
                        Ok(actor::Reply::Committed)
                    };
                    command.reply.send(reply).ok();
                }
            });
            assert_eq!(
                tasks
                    .handle(Request::Query {
                        task_id: "receipt".into()
                    })
                    .unwrap()
                    .phase,
                Phase::Unknown
            );
            assert!(tasks.active.lock().unwrap().is_some());
            std::fs::create_dir(directory.join("state.partial")).unwrap();
            assert_eq!(
                tasks
                    .handle(Request::Query {
                        task_id: "receipt".into()
                    })
                    .unwrap_err(),
                "state_unavailable"
            );
            assert!(tasks.active.lock().unwrap().is_some());
            assert_eq!(std::fs::read(directory.join("state.json")).unwrap(), before);
            std::fs::remove_dir(directory.join("state.partial")).unwrap();
            let finished = tasks
                .handle(Request::Submit {
                    submission: Box::new(submission),
                })
                .unwrap();
            assert_eq!(finished.phase, phase);
            assert_eq!(finished.adxlet_execution.unwrap().stage, final_stage);
            assert_eq!(load(&directory).unwrap().phase, phase);
            assert!(tasks.active.lock().unwrap().is_none());
            runtime.block_on(actor).unwrap();
        }
    }

    #[test]
    fn parent_journal_roundtrips_and_oversize_preserves_it() {
        let root = tempfile::tempdir().unwrap();
        let identity = ProcessIdentity {
            node_id: "n".repeat(128),
            identity: ProcessSelection {
                rollout_id: "r".repeat(128),
                epoch: u64::MAX,
                old_node_session: "o".repeat(128),
                old_relay_session: "p".repeat(128),
                original_sha256: "a".repeat(64),
            },
        };
        let submission = Submission {
            component_config: None,
            task_id: "t".repeat(64),
            role: Role::Adxlet,
            service_id: "v".repeat(64),
            commit: "c".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: Some(identity.clone()),
            selection: Selection::Publisher {
                expected: ExpectedRuntime {
                    configuration_sha256: "e".repeat(64),
                    boot_nonce: "x".repeat(128),
                    pod_uid: Some("u".repeat(128)),
                    pid: u32::MAX,
                    executable_sha256: identity.identity.original_sha256.clone(),
                    node_session: identity.identity.old_node_session.clone(),
                    relay_session: identity.identity.old_relay_session.clone(),
                },
            },
        };
        let mut task = prepared_task(submission, Phase::Failed, u64::MAX);
        task.adxlet_execution = Some(adxlet::Execution {
            old_pid: u32::MAX,
            error: Some(journal_error(
                "candidate_unknown",
                &"diagnostic".repeat(1000),
            )),
            attempt: "attempt".repeat(128),
            node_session: "session".repeat(128),
            ..target_execution(
                adxlet::Stage::Selected {
                    candidate_pid: u32::MAX - 2,
                },
                u64::MAX,
            )
        });
        let encoded = serde_json::to_vec(&task).unwrap();
        assert!(encoded.len() <= MAX_JOURNAL_BYTES as usize);
        store(root.path(), &task).unwrap();
        let loaded = load(root.path()).unwrap();
        assert_eq!(
            serde_json::to_value(loaded).unwrap(),
            serde_json::to_value(&task).unwrap()
        );
        let previous = std::fs::read(root.path().join("state.json")).unwrap();
        task.adxlet_execution.as_mut().unwrap().error =
            Some("x".repeat(MAX_JOURNAL_BYTES as usize));
        assert!(store(root.path(), &task).is_err());
        assert_eq!(
            std::fs::read(root.path().join("state.json")).unwrap(),
            previous
        );
        assert!(load(root.path()).is_ok());
    }

    #[test]
    fn journal_error_truncation_is_character_safe_and_bounded_in_encoded_state() {
        let text = "\u{1f600}\n\"".repeat(10000);
        let bounded = journal_error("runtime_unknown", &text);
        assert!(bounded.starts_with("runtime_unknown: "));
        assert!(bounded.len() <= MAX_ERROR_BYTES + "runtime_unknown: ".len());
        assert!(serde_json::to_vec(&bounded).unwrap().len() < MAX_JOURNAL_BYTES as usize);
    }

    #[test]
    fn journal_load_rejects_limit_plus_one_independently_of_wire_limit() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("state.json"),
            vec![b' '; MAX_JOURNAL_BYTES as usize + 1],
        )
        .unwrap();
        assert!(load(root.path()).is_err());
        assert_eq!(
            MAX_REQUEST_BYTES, 4096,
            "larger journals must not widen accepted requests"
        );
    }

    #[test]
    fn publisher_freezes_incarnation_and_every_runtime_field() {
        let expected = ExpectedRuntime {
            configuration_sha256: "e".repeat(64),
            boot_nonce: "boot".into(),
            pod_uid: Some("pod".into()),
            pid: 42,
            executable_sha256: "a".repeat(64),
            node_session: "node-session".into(),
            relay_session: "proxy-session".into(),
        };
        for field in 0..6 {
            let mut changed = expected.clone();
            match field {
                0 => changed.boot_nonce.push('x'),
                1 => changed.pod_uid = Some("replacement".into()),
                2 => changed.pid += 1,
                3 => changed.executable_sha256 = "b".repeat(64),
                4 => changed.node_session.push('x'),
                _ => changed.relay_session.push('x'),
            }
            assert_ne!(expected, changed);
            assert!(expected.require_current(&changed).is_err());
        }
        assert!(expected.require_current(&expected).is_ok());
        let wrapper =
            serde_json::json!({"ok":true,"node_info":{"components":[{"runtime":expected}]}});
        let observed: ExpectedRuntime =
            serde_json::from_value(wrapper["node_info"]["components"][0]["runtime"].clone())
                .unwrap();
        assert_eq!(
            observed, expected,
            "NodeInfo runtime is the exact Submit selection shape"
        );
        let mut submission = Submission {
            component_config: None,
            task_id: "local".into(),
            role: Role::Relay,
            service_id: "relay".into(),
            commit: "a".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: None,
            selection: Selection::Publisher {
                expected: expected.clone(),
            },
        };
        assert!(submission.validate().is_ok());
        let frozen = submission.clone();
        submission.selection = Selection::Publisher {
            expected: ExpectedRuntime {
                configuration_sha256: "e".repeat(64),
                pid: 43,
                ..expected
            },
        };
        assert_ne!(
            submission, frozen,
            "same task payload replay includes selected runtime"
        );
        let encoded = serde_json::to_vec(&frozen).unwrap();
        assert_eq!(
            serde_json::from_slice::<Submission>(&encoded).unwrap(),
            frozen
        );
        let root = tempfile::tempdir().unwrap();
        let mut deployment = deployment();
        deployment.state_dir = root.path().into();
        let tasks = Tasks::new(&deployment).unwrap();
        assert_eq!(
            tasks
                .handle(Request::Submit {
                    submission: Box::new(frozen)
                })
                .unwrap_err(),
            "incarnation_conflict"
        );
    }

    #[test]
    fn journal_replay_rejects_changed_selected_runtime_before_observing_again() {
        let root = tempfile::tempdir().unwrap();
        let mut deployment = deployment();
        deployment.state_dir = root.path().into();
        let tasks = Tasks::new(&deployment).unwrap();
        let expected = ExpectedRuntime {
            configuration_sha256: "e".repeat(64),
            boot_nonce: tasks.info.boot_nonce.clone(),
            pod_uid: tasks.info.pod_uid.clone(),
            pid: 42,
            executable_sha256: "a".repeat(64),
            node_session: "node".into(),
            relay_session: "proxy".into(),
        };
        let submission = Submission {
            component_config: None,
            task_id: "replay".into(),
            role: Role::Relay,
            service_id: "relay".into(),
            commit: "a".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: None,
            selection: Selection::Publisher { expected },
        };
        private_directory(&tasks.directory).unwrap();
        let directory = tasks.directory.join("replay");
        private_directory(&directory).unwrap();
        store(
            &directory,
            &prepared_task(submission.clone(), Phase::Completed, 1),
        )
        .unwrap();
        assert_eq!(
            tasks
                .handle(Request::Submit {
                    submission: Box::new(submission.clone())
                })
                .unwrap()
                .phase,
            Phase::Completed
        );
        let mut changed_source = submission.clone();
        changed_source.remote_archive = Some(RemoteArchive {
            url: "https://packages.example/archive.tar.gz".into(),
            archive_sha256: "c".repeat(64),
        });
        assert_eq!(
            tasks
                .handle(Request::Submit {
                    submission: Box::new(changed_source)
                })
                .unwrap_err(),
            "conflict"
        );
        let mut remote_submission = submission.clone();
        remote_submission.remote_archive = Some(RemoteArchive {
            url: "https://packages.example/archive.tar.gz".into(),
            archive_sha256: "c".repeat(64),
        });
        store(
            &directory,
            &prepared_task(remote_submission.clone(), Phase::Completed, 1),
        )
        .unwrap();
        assert_eq!(
            tasks
                .handle(Request::Submit {
                    submission: Box::new(remote_submission.clone()),
                })
                .unwrap()
                .phase,
            Phase::Completed
        );
        remote_submission
            .remote_archive
            .as_mut()
            .unwrap()
            .url
            .push_str("?changed");
        assert_eq!(
            tasks
                .handle(Request::Submit {
                    submission: Box::new(remote_submission),
                })
                .unwrap_err(),
            "conflict"
        );
        let mut changed = submission;
        let Selection::Publisher { expected } = &mut changed.selection else {
            unreachable!()
        };
        expected.pid += 1;
        assert_eq!(
            tasks
                .handle(Request::Submit {
                    submission: Box::new(changed)
                })
                .unwrap_err(),
            "conflict"
        );
    }

    #[test]
    fn replaced_pod_is_rejected_before_creating_a_task_even_with_same_boot_and_child() {
        let root = tempfile::tempdir().unwrap();
        let mut deployment = deployment();
        deployment.state_dir = root.path().into();
        let mut tasks = Tasks::new(&deployment).unwrap();
        tasks.info.pod_uid = Some("actual-pod".into());
        let expected = ExpectedRuntime {
            configuration_sha256: "e".repeat(64),
            boot_nonce: tasks.info.boot_nonce.clone(),
            pod_uid: Some("old-pod".into()),
            pid: 42,
            executable_sha256: "a".repeat(64),
            node_session: "node".into(),
            relay_session: "proxy".into(),
        };
        let submission = Submission {
            component_config: None,
            task_id: "new-task".into(),
            role: Role::Relay,
            service_id: "relay".into(),
            commit: "a".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: None,
            selection: Selection::Publisher { expected },
        };
        assert_eq!(
            tasks
                .handle(Request::Submit {
                    submission: Box::new(submission)
                })
                .unwrap_err(),
            "incarnation_conflict"
        );
        assert!(!tasks.directory.join("new-task").exists());
        assert!(tasks.active.lock().unwrap().is_none());
    }

    #[test]
    fn node_info_without_actor_never_reports_config_as_runtime_evidence() {
        let root = tempfile::tempdir().unwrap();
        let mut deployment = deployment();
        deployment.state_dir = root.path().into();
        let tasks = Tasks::new(&deployment).unwrap();
        let response = tasks.response(r#"{"action":"rollout_node_info"}"#);
        assert_eq!(response["node_info"]["schema_version"], 1);
        assert!(!response["node_info"]["boot_nonce"]
            .as_str()
            .unwrap()
            .is_empty());
        for component in response["node_info"]["components"].as_array().unwrap() {
            assert!(component["runtime"].is_null());
            assert!(component["sources"].is_null());
            assert_eq!(component["can_upgrade"], false);
        }
    }

    #[test]
    fn relay_terminal_phase_is_not_prepared_or_cli_success() {
        use relay::RelayStage;
        for (stage, wire, phase) in [
            (RelayStage::Upgrading, "upgrading", None),
            (RelayStage::RollingBack, "rolling_back", None),
            (RelayStage::Completed, "completed", Some(Phase::Completed)),
            (RelayStage::RolledBack, "rolled_back", Some(Phase::Failed)),
            (
                RelayStage::FailedRetained,
                "failed_retained",
                Some(Phase::Failed),
            ),
            (
                RelayStage::CancelledBeforeBegin,
                "cancelled_before_begin",
                Some(Phase::Failed),
            ),
        ] {
            assert_eq!(stage.terminal_phase(), phase);
            assert_eq!(serde_json::to_value(stage).unwrap(), wire);
            assert_eq!(
                serde_json::from_value::<RelayStage>(serde_json::json!(wire)).unwrap(),
                stage
            );
        }
    }

    #[test]
    fn adxlet_submission_requires_exact_frozen_maintenance_identity() {
        let mut submission = Submission {
            component_config: None,
            task_id: "local".into(),
            service_id: "node".into(),
            role: Role::Adxlet,
            commit: "a".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: None,
            selection: Selection::LocalManual,
        };
        assert!(submission.validate().is_err());
        submission.process = Some(ProcessIdentity {
            node_id: "actual-node".into(),
            identity: ProcessSelection {
                rollout_id: "batch".into(),
                epoch: 1,
                old_node_session: "old".into(),
                old_relay_session: "relay".into(),
                original_sha256: "c".repeat(64),
            },
        });
        assert!(submission.validate().is_ok());
        submission.role = Role::Relay;
        assert!(submission.validate().is_err());
    }
    use crate::config::{Deployment, Profile};

    fn deployment() -> Deployment {
        serde_saphyr::from_str(Profile::Node.template()).unwrap()
    }

    #[test]
    fn live_expired_query_is_read_only_and_keeps_node_reservation() {
        let root = tempfile::tempdir().unwrap();
        let mut deployment = deployment();
        deployment.state_dir = root.path().into();
        let tasks = Tasks::new(&deployment).unwrap();
        private_directory(&tasks.directory).unwrap();
        let directory = tasks.directory.join("live");
        private_directory(&directory).unwrap();
        let submission = Submission {
            component_config: None,
            task_id: "live".into(),
            service_id: "relay".into(),
            role: Role::Relay,
            commit: "a".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: None,
            selection: Selection::LocalManual,
        };
        store(&directory, &prepared_task(submission, Phase::Preparing, 1)).unwrap();
        *tasks.active.lock().unwrap() = Some(Reservation {
            task_id: "live".into(),
            worker_finished: false,
        });
        let observed = tasks
            .handle(Request::Query {
                task_id: "live".into(),
            })
            .unwrap();
        assert_eq!(observed.phase, Phase::Preparing);
        assert_eq!(load(&directory).unwrap().phase, Phase::Preparing);
        assert!(tasks.active.lock().unwrap().is_some());
    }

    #[test]
    fn prepared_adxlet_task_keeps_reservation_and_queries_as_preparing() {
        let root = tempfile::tempdir().unwrap();
        let mut deployment = deployment();
        deployment.state_dir = root.path().into();
        let tasks = Tasks::new(&deployment).unwrap();
        private_directory(&tasks.directory).unwrap();
        let directory = tasks.directory.join("prepared");
        private_directory(&directory).unwrap();
        let submission = Submission {
            component_config: None,
            task_id: "prepared".into(),
            service_id: "adxlet".into(),
            selection: Selection::LocalManual,
            role: Role::Adxlet,
            commit: "a".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: Some(ProcessIdentity {
                node_id: "node".into(),
                identity: ProcessSelection {
                    rollout_id: "rollout".into(),
                    epoch: 1,
                    old_node_session: "old".into(),
                    old_relay_session: "relay".into(),
                    original_sha256: "c".repeat(64),
                },
            }),
        };
        let task = prepared_task(submission, Phase::Preparing, 1);
        assert!(task.prepared.is_some());
        store(&directory, &task).unwrap();
        // State left by a successful prepare-only worker.
        *tasks.active.lock().unwrap() = Some(Reservation {
            task_id: "prepared".into(),
            worker_finished: true,
        });
        for _ in 0..2 {
            let observed = tasks
                .handle(Request::Query {
                    task_id: "prepared".into(),
                })
                .unwrap();
            assert_eq!(observed.phase, Phase::Preparing);
            assert!(observed.prepared.is_some());
            assert!(observed.adxlet_execution.is_none());
        }
        assert_eq!(load(&directory).unwrap().phase, Phase::Preparing);
        assert!(tasks.active.lock().unwrap().is_some());
        // deadline_unix_ms=1 is long past: waiting after preparation is not expiry.
        let response = tasks.response(r#"{"action":"rollout_query","task_id":"prepared"}"#);
        assert_eq!(response["ok"], true);
        assert_eq!(response["preparation_budget_expired"], false);
    }

    #[test]
    fn unavailable_rollout_directory_does_not_prevent_supervision() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("rollout"), b"occupied").unwrap();
        let mut deployment = deployment();
        deployment.state_dir = root.path().into();
        let tasks = Tasks::new(&deployment).unwrap();
        assert_eq!(
            tasks.response(r#"{"action":"rollout_node_info"}"#)["ok"],
            true
        );
        assert_eq!(
            tasks.response(r#"{"action":"rollout_query","task_id":"x"}"#)["ok"],
            false
        );
    }

    #[test]
    fn interrupted_preparation_is_not_restarted_and_embedded_submission_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let mut deployment = deployment();
        deployment.state_dir = root.path().into();
        let service = deployment
            .services
            .iter_mut()
            .find(|s| s.role == Role::Adxlet)
            .unwrap();
        service.config["node_id"] = serde_json::json!("node");
        service.config["proxy_mode"] = serde_json::json!("embedded");
        let submission = Submission {
            component_config: None,
            task_id: "fixed".into(),
            service_id: service.id.clone(),
            selection: Selection::LocalManual,
            role: Role::Adxlet,
            commit: "a".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: Some(ProcessIdentity {
                node_id: "node".into(),
                identity: ProcessSelection {
                    rollout_id: "batch".into(),
                    epoch: 1,
                    old_node_session: "old".into(),
                    old_relay_session: "relay".into(),
                    original_sha256: "c".repeat(64),
                },
            }),
        };
        let tasks = Tasks::new(&deployment).unwrap();
        assert_eq!(
            tasks
                .handle(Request::Submit {
                    submission: Box::new(submission.clone())
                })
                .unwrap_err(),
            "unsupported_component"
        );
        private_directory(&tasks.directory).unwrap();
        let directory = tasks.directory.join("fixed");
        private_directory(&directory).unwrap();
        store(
            &directory,
            &prepared_task(submission.clone(), Phase::Preparing, 1),
        )
        .unwrap();
        let task = tasks
            .handle(Request::Submit {
                submission: Box::new(submission),
            })
            .unwrap();
        assert_eq!(task.phase, Phase::Interrupted);
        assert_eq!(task.deadline_unix_ms, 1);
        assert!(!directory.join("executable").exists());
    }

    #[test]
    fn node_identity_comes_from_paired_adxlet_not_service_name() {
        let mut deployment = deployment();
        let adxlet = deployment
            .services
            .iter_mut()
            .find(|s| s.role == Role::Adxlet)
            .unwrap();
        adxlet.config["node_id"] = serde_json::json!("actual-node");
        adxlet.config["proxy_mode"] = serde_json::json!("standalone");
        adxlet.config["proxy_socket"] = serde_json::json!("/run/adx/relay/route.sock");
        deployment.services.push(crate::config::Service {
            id: "relay".into(),
            role: Role::Relay,
            config: serde_json::Value::Null,
            env: [(
                "ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR".into(),
                "/run/adx/relay".into(),
            )]
            .into(),
        });
        let info = node_info(&deployment);
        let relay = info
            .components
            .iter()
            .find(|c| c.role == Role::Relay)
            .unwrap();
        assert_eq!(relay.node_id, "actual-node");
        assert_eq!(relay.mode, Mode::Standalone);
        assert!(!relay.can_upgrade);
        let serialized = serde_json::to_string(&info).unwrap();
        assert!(!serialized.contains("redis_url"));
        assert!(!serialized.contains("ADX_DATA_PLANE"));
    }

    #[test]
    fn embedded_components_are_not_independent_process_targets() {
        let mut deployment = deployment();
        deployment.services.retain(|s| s.role != Role::Relay);
        let adxlet = deployment
            .services
            .iter_mut()
            .find(|s| s.role == Role::Adxlet)
            .unwrap();
        adxlet.config["proxy_mode"] = serde_json::json!("embedded");
        adxlet.config["node_id"] = serde_json::json!("node");
        let info = node_info(&deployment);
        let adxlet = info
            .components
            .iter()
            .find(|c| c.role == Role::Adxlet)
            .unwrap();
        assert_eq!(adxlet.mode, Mode::Embedded);
        assert!(!adxlet.can_upgrade);
    }

    #[test]
    fn mismatched_relay_pair_does_not_invent_node_identity() {
        let mut deployment = deployment();
        deployment.services.push(crate::config::Service {
            id: "unpaired-relay".into(),
            role: Role::Relay,
            config: serde_json::Value::Null,
            env: [(
                "ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR".into(),
                "/unpaired".into(),
            )]
            .into(),
        });
        let info = node_info(&deployment);
        assert!(!info
            .components
            .iter()
            .any(|c| c.service_id == "unpaired-relay"));
    }

    #[test]
    fn wire_contract_rejects_unknown_actions_and_fields() {
        assert_eq!(
            serde_json::from_str::<Request>(r#"{"action":"rollout_node_info"}"#).unwrap(),
            Request::NodeInfo {}
        );
        for json in [
            r#"{"action":"rollout_node_info","unexpected":true}"#,
            r#"{"action":"rollout_node_info","declaration_path":"/x"}"#,
        ] {
            assert!(serde_json::from_str::<Request>(json).is_err());
        }
        assert!(serde_json::from_str::<Request>(r#"{"action":"rollout_submit"}"#).is_err());
    }

    #[test]
    fn rollback_wire_uses_rollback_without_legacy_alias() {
        let json = r#"{"action":"rollout_step","task_id":"task-1","step":"rollback","attempt":"A","node_session":"session-A"}"#;
        let request = serde_json::from_str::<Request>(json)
            .expect("protocol 2 must accept the single local rollback step");
        assert_eq!(serde_json::to_string(&request).unwrap(), json);
        assert!(serde_json::from_str::<Request>(
            r#"{"action":"rollout_step","task_id":"task-1","step":"rollback_stop","attempt":"A","node_session":"session-A"}"#,
        )
        .is_err());
    }

    #[test]
    fn rollback_node_info_advertises_protocol_two() {
        let info = node_info(&deployment());
        assert_eq!(serde_json::to_value(info).unwrap()["step_protocol"], 2);
    }

    #[test]
    fn step_request_wire_contract_is_exact() {
        let json = r#"{"action":"rollout_step","task_id":"task-1","step":"rollback","attempt":"a1","node_session":"s1"}"#;
        let request = serde_json::from_str::<Request>(json).unwrap();
        assert_eq!(
            request,
            Request::Step {
                task_id: "task-1".into(),
                step: Step::Rollback,
                attempt: "a1".into(),
                node_session: "s1".into(),
            }
        );
        assert_eq!(serde_json::to_string(&request).unwrap(), json);
        for step in ["exit", "start", "rollback"] {
            let json = format!(
                r#"{{"action":"rollout_step","task_id":"t","step":"{step}","attempt":"","node_session":""}}"#
            );
            assert!(serde_json::from_str::<Request>(&json).is_ok(), "{step}");
        }
        for invalid in [
            r#"{"action":"rollout_step","task_id":"t","step":"replace_session","attempt":"","node_session":""}"#,
            r#"{"action":"rollout_step","task_id":"t","step":"verify","attempt":"","node_session":""}"#,
            r#"{"action":"rollout_step","task_id":"t","step":"exit","attempt":""}"#,
            r#"{"action":"rollout_step","task_id":"t","step":"exit","attempt":"","node_session":"","expected":{}}"#,
        ] {
            assert!(
                serde_json::from_str::<Request>(invalid).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn step_rejects_unknown_task_stale_identity_and_relay_tasks() {
        let root = tempfile::tempdir().unwrap();
        let mut deployment = deployment();
        deployment.state_dir = root.path().into();
        let tasks = Tasks::new(&deployment).unwrap();
        let step = |task_id: &str, attempt: &str, node_session: &str| Request::Step {
            task_id: task_id.into(),
            step: Step::Start,
            attempt: attempt.into(),
            node_session: node_session.into(),
        };
        assert_eq!(
            tasks.handle(step("../bad", "", "")).unwrap_err(),
            "invalid_request"
        );
        private_directory(&tasks.directory).unwrap();
        assert_eq!(
            tasks.handle(step("missing", "", "")).unwrap_err(),
            "not_found"
        );
        let identity = ProcessIdentity {
            node_id: "node".into(),
            identity: ProcessSelection {
                rollout_id: "rollout".into(),
                epoch: 1,
                old_node_session: "old".into(),
                old_relay_session: "relay".into(),
                original_sha256: "a".repeat(64),
            },
        };
        let submission = Submission {
            component_config: None,
            selection: Selection::LocalManual,
            task_id: "steps".into(),
            role: Role::Adxlet,
            service_id: "node".into(),
            commit: "c".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: Some(identity),
        };
        let directory = tasks.directory.join("steps");
        private_directory(&directory).unwrap();
        let mut task = prepared_task(submission.clone(), Phase::Unknown, u64::MAX);
        task.adxlet_execution = Some(adxlet::Execution {
            old_pid: 42,
            ..target_execution(adxlet::Stage::Exited, u64::MAX)
        });
        store(&directory, &task).unwrap();
        let before = std::fs::read(directory.join("state.json")).unwrap();
        for (attempt, session) in [("stale", "next"), ("attempt", "stale"), ("", "")] {
            assert_eq!(
                tasks.handle(step("steps", attempt, session)).unwrap_err(),
                "conflict"
            );
        }
        // Neither a stale nor a not-yet-wired step mutates the stored Task.
        assert!(tasks.handle(step("steps", "attempt", "next")).is_err());
        assert_eq!(std::fs::read(directory.join("state.json")).unwrap(), before);
        let relay = tasks.directory.join("relay-task");
        private_directory(&relay).unwrap();
        let mut relay_submission = submission;
        relay_submission.task_id = "relay-task".into();
        relay_submission.role = Role::Relay;
        relay_submission.service_id = "relay".into();
        relay_submission.process = None;
        store(
            &relay,
            &prepared_task(relay_submission, Phase::Running, u64::MAX),
        )
        .unwrap();
        assert_eq!(
            tasks.handle(step("relay-task", "", "")).unwrap_err(),
            "unsupported_component"
        );
    }

    #[test]
    fn node_info_reports_step_protocol_and_local_pairing_gate() {
        let mut deployment = deployment();
        let info = node_info(&deployment);
        assert_eq!(info.step_protocol, STEP_PROTOCOL);
        let serialized = serde_json::to_value(&info).unwrap();
        assert_eq!(serialized["step_protocol"], STEP_PROTOCOL);
        let adxlet = deployment
            .services
            .iter_mut()
            .find(|s| s.role == Role::Adxlet)
            .unwrap();
        adxlet.config["node_id"] = serde_json::json!("actual-node");
        adxlet.config["proxy_mode"] = serde_json::json!("standalone");
        adxlet.config["proxy_socket"] = serde_json::json!("/run/adx/relay/route.sock");
        let adxlet_id = adxlet.id.clone();
        deployment.services.retain(|s| s.role != Role::Relay);
        // A standalone adxlet whose Relay is external has no local pair; the
        // deployment still loads, only the local upgrade boundary refuses.
        assert!(!adxlet_step_pairing(&deployment, &adxlet_id));
        let relay = |id: &str| crate::config::Service {
            id: id.into(),
            role: Role::Relay,
            config: serde_json::Value::Null,
            env: [(
                "ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR".into(),
                "/run/adx/relay".into(),
            )]
            .into(),
        };
        deployment.services.push(relay("relay"));
        assert!(adxlet_step_pairing(&deployment, &adxlet_id));
        deployment.services.push(relay("relay-2"));
        assert!(!adxlet_step_pairing(&deployment, &adxlet_id));
        deployment.services.pop();
        deployment
            .services
            .iter_mut()
            .find(|s| s.id == adxlet_id)
            .unwrap()
            .config["proxy_socket"] = serde_json::json!("relative/route.sock");
        assert!(!adxlet_step_pairing(&deployment, &adxlet_id));
    }

    #[test]
    fn submission_identity_is_strict_and_complete() {
        let mut request = Submission {
            component_config: None,
            task_id: "fixed-task".into(),
            role: Role::Relay,
            service_id: "relay".into(),
            commit: "a".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: None,
            selection: Selection::LocalManual,
        };
        assert!(request.validate().is_ok());
        for url in [
            "http://host/a",
            "https:///",
            "https://user@host/a",
            "https://:pass@host/a",
            "https://host/a#fragment",
            "https://@host/a",
        ] {
            assert!(RemoteArchive {
                url: url.into(),
                archive_sha256: "a".repeat(64)
            }
            .validate()
            .is_err());
        }
        for digest in ["A".repeat(64), "a".repeat(63), "g".repeat(64)] {
            assert!(RemoteArchive {
                url: "https://host/a".into(),
                archive_sha256: digest
            }
            .validate()
            .is_err());
        }
        request.remote_archive = Some(RemoteArchive {
            url: "https://host/archive.tar.gz?version=1".into(),
            archive_sha256: "a".repeat(64),
        });
        assert!(request.validate().is_ok());
        let remote = request.clone();
        request.remote_archive.as_mut().unwrap().url.push('2');
        assert_ne!(request, remote);
        request = remote;
        let frozen = request.clone();
        request.sha256 = Some("c".repeat(64));
        assert_ne!(request, frozen);
        request.task_id = "../escape".into();
        assert!(request.validate().is_err());
        request = frozen;
        request.role = Role::ApiServer;
        assert!(request.validate().is_err());
    }

    fn paired_adxlet() -> (Deployment, String) {
        let mut deployment = deployment();
        let service_id = deployment
            .services
            .iter_mut()
            .find(|service| service.role == Role::Adxlet)
            .map(|service| {
                service.config["node_id"] = serde_json::json!("node");
                service.config["proxy_mode"] = serde_json::json!("standalone");
                service.config["proxy_socket"] = serde_json::json!("/run/adx/relay/route.sock");
                service.id.clone()
            })
            .unwrap();
        deployment
            .services
            .retain(|service| service.role != Role::Relay);
        deployment.services.push(crate::config::Service {
            id: "relay".into(),
            role: Role::Relay,
            config: serde_json::Value::Null,
            env: [(
                "ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR".into(),
                "/run/adx/relay".into(),
            )]
            .into(),
        });
        (deployment, service_id)
    }

    fn adxlet_submission(task_id: &str, service_id: &str) -> Submission {
        Submission {
            component_config: None,
            selection: Selection::LocalManual,
            task_id: task_id.into(),
            role: Role::Adxlet,
            service_id: service_id.into(),
            commit: "c".repeat(40),
            sha256: Some("b".repeat(64)),
            remote_archive: None,
            process: Some(ProcessIdentity {
                node_id: "node".into(),
                identity: ProcessSelection {
                    rollout_id: "rollout".into(),
                    epoch: 1,
                    old_node_session: "old".into(),
                    old_relay_session: "relay".into(),
                    original_sha256: "a".repeat(64),
                },
            }),
        }
    }

    fn sha256_hex(data: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    fn test_process() -> crate::config::Process {
        crate::config::Process {
            sources: std::collections::BTreeMap::new(),
            configuration_sha256: "e".repeat(64),
            id: "adxlet".into(),
            role: Role::Adxlet,
            binary: std::path::PathBuf::from("/unused/executable"),
            args: vec!["--config".into(), "/etc/adxlet.json".into()],
            env: [("ADX_NODE".to_owned(), "node".to_owned())].into(),
            admin_socket: Some(std::path::PathBuf::from("/run/adxlet/admin.sock")),
            health_url: None,
        }
    }

    /// Writes an actual config JSON at `directory/name` and returns a Process
    /// whose `--config` names it and whose digest is the real effective one,
    /// as `adxlet::prepare` pins it.
    fn pinned_process(
        directory: &std::path::Path,
        name: &str,
        admin_socket: &std::path::Path,
    ) -> crate::config::Process {
        let config = serde_json::json!({"node_id": "node", "file": name});
        let path = directory.join(name);
        std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let mut process = test_process();
        process.args = vec!["--config".into(), path.to_string_lossy().into_owned()];
        process.admin_socket = Some(admin_socket.to_path_buf());
        process.configuration_sha256 =
            crate::config::configuration_sha256(&config, &process.env).unwrap();
        process
    }

    /// Pins both sides into the task directory and returns (original, target).
    fn pin_snapshots(
        directory: &std::path::Path,
        admin_socket: &std::path::Path,
    ) -> (crate::config::Process, crate::config::Process) {
        let original = pinned_process(directory, adxlet::ORIGINAL_CONFIG, admin_socket);
        let mut target = pinned_process(directory, adxlet::TARGET_CONFIG, admin_socket);
        target.binary = directory.join("executable");
        for (name, process) in [
            (adxlet::ORIGINAL_PROCESS, &original),
            (adxlet::TARGET_PROCESS, &target),
        ] {
            std::fs::write(directory.join(name), serde_json::to_vec(process).unwrap()).unwrap();
        }
        (original, target)
    }

    fn target_execution(stage: adxlet::Stage, deadline_unix_ms: u64) -> adxlet::Execution {
        adxlet::Execution {
            old_pid: 4242,
            stage,
            error: None,
            attempt: "attempt".into(),
            node_session: "next".into(),
            deadline_unix_ms,
            leg: adxlet::ProcessLeg::Target,
        }
    }

    fn step_request(task_id: &str, step: Step, attempt: &str, node_session: &str) -> Request {
        Request::Step {
            task_id: task_id.into(),
            step,
            attempt: attempt.into(),
            node_session: node_session.into(),
        }
    }

    struct TestHarness {
        root: tempfile::TempDir,
        service_id: String,
        task_id: String,
        directory: std::path::PathBuf,
        tasks: Tasks,
    }

    impl TestHarness {
        fn new(task_id: &str) -> Self {
            let root = tempfile::tempdir().unwrap();
            let (mut deployment, service_id) = paired_adxlet();
            deployment.state_dir = root.path().into();
            let tasks = Tasks::new(&deployment).unwrap();
            private_directory(&tasks.directory).unwrap();
            let directory = tasks.directory.join(task_id);
            private_directory(&directory).unwrap();
            Self {
                root,
                service_id,
                task_id: task_id.into(),
                directory,
                tasks,
            }
        }

        fn executor(
            &mut self,
            runtime: &tokio::runtime::Runtime,
            capacity: usize,
        ) -> tokio::sync::mpsc::Receiver<actor::Command> {
            self.tasks.runtime = Some(runtime.handle().clone());
            let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
            self.tasks.set_executor(sender);
            receiver
        }

        fn task(&self, phase: Phase, deadline_unix_ms: u64) -> Task {
            prepared_task(
                adxlet_submission(&self.task_id, &self.service_id),
                phase,
                deadline_unix_ms,
            )
        }

        fn install(&self, task: &Task) {
            store(&self.directory, task).unwrap();
            *self.tasks.active.lock().unwrap() = Some(Reservation {
                task_id: self.task_id.clone(),
                worker_finished: true,
            });
        }
    }

    fn test_runtime(workers: usize) -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn step_exit_replays_minted_identity_without_reminting_or_refresh() {
        let h = TestHarness::new("exit-replay");
        let mut task = h.task(Phase::Unknown, u64::MAX);
        task.deadline_unix_ms = 900_000;
        task.adxlet_execution = Some(adxlet::Execution {
            old_pid: 42,
            ..target_execution(adxlet::Stage::Exited, 900_000)
        });
        h.install(&task);
        let before = std::fs::read(h.directory.join("state.json")).unwrap();
        let observed = h
            .tasks
            .handle(step_request("exit-replay", Step::Exit, "", ""))
            .unwrap();
        let execution = observed.adxlet_execution.unwrap();
        assert_eq!(execution.attempt, "attempt");
        assert_eq!(execution.node_session, "next");
        assert_eq!(execution.deadline_unix_ms, 900_000);
        assert_eq!(execution.stage, adxlet::Stage::Exited);
        assert_eq!(
            std::fs::read(h.directory.join("state.json")).unwrap(),
            before
        );
        assert!(
            h.tasks
                .active
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .worker_finished
        );
    }

    #[test]
    fn step_exit_terminal_or_without_reservation_has_no_side_effect() {
        let h = TestHarness::new("exit-terminal");
        h.install(&h.task(Phase::Completed, u64::MAX));
        let terminal_dir = &h.directory;
        let tasks = &h.tasks;
        let service_id = &h.service_id;
        let before = std::fs::read(terminal_dir.join("state.json")).unwrap();
        let observed = tasks
            .handle(step_request("exit-terminal", Step::Exit, "", ""))
            .unwrap();
        assert_eq!(observed.phase, Phase::Completed);
        assert!(observed.adxlet_execution.is_none());
        assert_eq!(
            std::fs::read(terminal_dir.join("state.json")).unwrap(),
            before
        );

        let orphan_dir = tasks.directory.join("exit-orphan");
        private_directory(&orphan_dir).unwrap();
        store(
            &orphan_dir,
            &prepared_task(
                adxlet_submission("exit-orphan", service_id),
                Phase::Preparing,
                u64::MAX,
            ),
        )
        .unwrap();
        *tasks.active.lock().unwrap() = None;
        let before = std::fs::read(orphan_dir.join("state.json")).unwrap();
        assert_eq!(
            tasks
                .handle(step_request("exit-orphan", Step::Exit, "", ""))
                .unwrap_err(),
            "conflict"
        );
        assert_eq!(
            std::fs::read(orphan_dir.join("state.json")).unwrap(),
            before
        );
    }

    #[test]
    fn step_exit_starts_past_preparation_deadline_and_waits_for_real_observe() {
        let mut h = TestHarness::new("exit-first");
        let runtime = test_runtime(2);
        let mut receiver = h.executor(&runtime, 4);

        let target_bytes = b"owned prepared target executable bytes";
        let target_sha256 = sha256_hex(target_bytes);
        let original_bytes = b"owned original binary bytes";
        let original_sha256 = sha256_hex(original_bytes);
        // The three first-exit file proofs: the prepared target executable, the
        // owned original copy, and the live original bytes served by the actor
        // as the RuntimeInspect executable.
        std::fs::write(h.directory.join("executable"), target_bytes).unwrap();
        std::fs::write(h.directory.join("original"), original_bytes).unwrap();
        let executable_path = h.root.path().join("live-exe");
        std::fs::write(&executable_path, original_bytes).unwrap();

        let mut submission = adxlet_submission("exit-first", &h.service_id);
        submission.sha256 = Some(target_sha256.clone());
        submission
            .process
            .as_mut()
            .unwrap()
            .identity
            .original_sha256 = original_sha256.clone();
        let mut stored = prepared_task(submission, Phase::Preparing, 1);
        stored.prepared.as_mut().unwrap().binary_sha256 = target_sha256;
        // deadline_unix_ms = 1 is long past the preparation budget; the first
        // Exit still starts because preparation expiry only bounds the prepare worker.
        h.install(&stored);

        let pid = 4242u32;
        let admin_socket = h.root.path().join("admin.sock");
        // Pinned private config copies must still carry their effective digest
        // before any Fence; the live RuntimeInspect matches the original copy.
        let (original_process, _) = pin_snapshots(&h.directory, &admin_socket);
        let actor_executable = executable_path.clone();
        let actor = runtime.spawn(async move {
            let mut fences = 0;
            let mut observes = 0;
            for _ in 0..3 {
                let command = receiver.recv().await.unwrap();
                match command.operation {
                    actor::Operation::RuntimeInspect => {
                        command
                            .reply
                            .send(Ok(actor::Reply::Runtime {
                                config: Box::new(original_process.clone()),
                                pid,
                                executable: std::fs::File::open(&actor_executable).unwrap(),
                                admin_pid: pid,
                                admin_socket: admin_socket.clone(),
                                relay: None,
                            }))
                            .ok();
                    }
                    actor::Operation::Fence { .. } => {
                        fences += 1;
                        command
                            .reply
                            .send(Ok(actor::Reply::State {
                                exited: false,
                                success: false,
                            }))
                            .ok();
                    }
                    actor::Operation::Observe { .. } => {
                        observes += 1;
                        command
                            .reply
                            .send(Ok(actor::Reply::State {
                                exited: true,
                                success: true,
                            }))
                            .ok();
                    }
                    _ => panic!("unexpected exit worker command"),
                }
            }
            (fences, observes)
        });

        let started = h
            .tasks
            .handle(step_request("exit-first", Step::Exit, "", ""))
            .unwrap();
        assert_eq!(started.phase, Phase::Preparing);
        assert!(started.adxlet_execution.is_none());

        let (fences, observes) = runtime.block_on(actor).unwrap();
        assert_eq!(
            (fences, observes),
            (1, 1),
            "exit relies on Fence then a real Observe receipt"
        );
        wait_worker(&h.tasks);
        assert!(
            h.tasks
                .active
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .worker_finished
        );
        let loaded = load(&h.directory).unwrap();
        let execution = loaded.adxlet_execution.unwrap();
        assert_eq!(execution.stage, adxlet::Stage::Exited);
        assert_eq!(execution.old_pid, pid);
        assert!(!execution.attempt.is_empty());
        assert!(!execution.node_session.is_empty());
        assert_eq!(loaded.phase, Phase::Unknown);
    }

    #[test]
    fn step_exit_resume_with_expired_deadline_observes_only_once_converges_unknown() {
        let mut h = TestHarness::new("exit-resume");
        let runtime = test_runtime(2);
        let mut receiver = h.executor(&runtime, 4);

        // The resume path reloads the original admin socket before any Observe
        // or Fence, even when the saved phase deadline is already past.
        std::fs::write(
            h.directory.join("original-process.json"),
            serde_json::to_vec(&test_process()).unwrap(),
        )
        .unwrap();

        let expired_deadline = 1u64;
        let mut task = h.task(Phase::Unknown, expired_deadline);
        task.adxlet_execution = Some(adxlet::Execution {
            attempt: String::new(),
            node_session: "old".into(),
            ..target_execution(adxlet::Stage::Exiting, expired_deadline)
        });
        h.install(&task);

        // An expired replay is permitted exactly one fresh, bounded Observe to
        // confirm whether the old Child was already reaped. It never fences,
        // begins or starts, and never refreshes the saved phase deadline.
        let actor = runtime.spawn(async move {
            let command = receiver.recv().await.unwrap();
            match command.operation {
                actor::Operation::Observe { .. } => command
                    .reply
                    .send(Ok(actor::Reply::State {
                        exited: false,
                        success: false,
                    }))
                    .ok(),
                _ => panic!("expired resume must Observe-only, never Fence/Begin/Start"),
            }
        });

        let started = h
            .tasks
            .handle(step_request("exit-resume", Step::Exit, "", ""))
            .unwrap();
        assert_eq!(started.phase, Phase::Unknown);
        runtime.block_on(actor).unwrap();
        wait_worker(&h.tasks);
        let loaded = load(&h.directory).unwrap();
        let execution = loaded.adxlet_execution.unwrap();
        assert_eq!(execution.stage, adxlet::Stage::Exiting);
        assert!(execution.attempt.is_empty());
        assert_eq!(execution.node_session, "old");
        assert!(execution.error.is_some());
        assert_eq!(
            execution.deadline_unix_ms, expired_deadline,
            "an expired replay must not refresh the saved phase deadline"
        );
        assert_eq!(loaded.phase, Phase::Unknown);
        // A durable Unknown outcome keeps the reservation so the maintenance
        // stays recoverable and re-runnable rather than being closed as failed.
        let active = h.tasks.active.lock().unwrap();
        assert!(active.as_ref().unwrap().worker_finished);
    }

    #[test]
    fn step_exit_failed_outcome_without_durable_store_keeps_reservation() {
        let mut h = TestHarness::new("exit-store-fail");
        let runtime = test_runtime(2);
        let mut receiver = h.executor(&runtime, 4);

        // No prepared executable: the first proof fails before Exiting is
        // persisted, so the worker converges on a terminal Failed phase without
        // issuing any relay command.
        let submission = adxlet_submission("exit-store-fail", &h.service_id);
        let task = prepared_task(submission, Phase::Preparing, 1);
        h.install(&task);
        let before = std::fs::read(h.directory.join("state.json")).unwrap();
        // A pre-existing directory at the partial path makes the worker's
        // durable store fail, mirroring the Submit worker's release guard.
        std::fs::create_dir(h.directory.join("state.partial")).unwrap();

        let started = h
            .tasks
            .handle(step_request("exit-store-fail", Step::Exit, "", ""))
            .unwrap();
        assert_eq!(started.phase, Phase::Preparing);
        assert!(receiver.try_recv().is_err());
        wait_worker(&h.tasks);
        // A store failure never closes the task or releases the reservation:
        // the durable journal is unchanged and the outcome stays re-runnable.
        assert_eq!(
            std::fs::read(h.directory.join("state.json")).unwrap(),
            before
        );
        let active = h.tasks.active.lock().unwrap();
        let reservation = active.as_ref().unwrap();
        assert!(reservation.worker_finished);
        assert_eq!(reservation.task_id, "exit-store-fail");
    }

    #[test]
    fn mark_exit_unknown_preserves_minted_identity() {
        // The worker's failure transition must not discard a minted identity:
        // a durable store returning Err does not prove the rename failed, so
        // the generated A stays A for an idempotent replay.
        let mut execution = adxlet::Execution {
            old_pid: 42,
            ..target_execution(adxlet::Stage::Exited, 900_000)
        };
        mark_exit_unknown(&mut execution, "exit outcome not durable");
        assert_eq!(execution.attempt, "attempt");
        assert_eq!(execution.node_session, "next");
        assert_eq!(execution.stage, adxlet::Stage::Exiting);
        assert!(execution.error.is_some());
    }

    #[test]
    fn step_exit_replays_exit_unknown_with_same_minted_identity() {
        let mut h = TestHarness::new("exit-unknown-replay");
        let runtime = test_runtime(2);
        let mut receiver = h.executor(&runtime, 4);

        // A durable `Exiting` still carries the minted A whose store had
        // failed. A replay must re-Observe and re-save the same A, never mint B.
        std::fs::write(
            h.directory.join("original-process.json"),
            serde_json::to_vec(&test_process()).unwrap(),
        )
        .unwrap();
        let deadline_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let mut task = h.task(Phase::Unknown, deadline_unix_ms);
        task.adxlet_execution = Some(adxlet::Execution {
            error: Some(journal_error(
                "execution_unknown",
                "exit outcome not durable",
            )),
            ..target_execution(adxlet::Stage::Exiting, deadline_unix_ms)
        });
        h.install(&task);

        let actor = runtime.spawn(async move {
            let command = receiver.recv().await.unwrap();
            match command.operation {
                actor::Operation::Observe { .. } => command
                    .reply
                    .send(Ok(actor::Reply::State {
                        exited: true,
                        success: true,
                    }))
                    .ok(),
                _ => panic!("replayed exit_unknown must Observe-first only"),
            }
        });

        let started = h
            .tasks
            .handle(step_request("exit-unknown-replay", Step::Exit, "", ""))
            .unwrap();
        assert_eq!(started.phase, Phase::Unknown);
        runtime.block_on(actor).unwrap();
        wait_worker(&h.tasks);
        let execution = load(&h.directory).unwrap().adxlet_execution.unwrap();
        assert_eq!(execution.stage, adxlet::Stage::Exited);
        assert_eq!(execution.attempt, "attempt");
        assert_eq!(execution.node_session, "next");
        assert!(execution.error.is_none());
    }

    /// Reports `ready` with the saved session only after the bootstrap payload
    /// was actually delivered to this (owned) process.
    struct ReadyAfterBootstrapStub {
        delivered: std::sync::Arc<std::sync::atomic::AtomicBool>,
        queries: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[tonic::async_trait]
    impl adx_protocol::relay::adxlet_process_service_server::AdxletProcessService
        for ReadyAfterBootstrapStub
    {
        async fn begin(
            &self,
            _request: tonic::Request<adx_protocol::relay::AdxletProcessRequest>,
        ) -> std::result::Result<
            tonic::Response<adx_protocol::relay::AdxletProcessState>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("begin is not used by Start"))
        }

        async fn query(
            &self,
            _request: tonic::Request<adx_protocol::relay::AdxletProcessRequest>,
        ) -> std::result::Result<
            tonic::Response<adx_protocol::relay::AdxletProcessState>,
            tonic::Status,
        > {
            let observed = self
                .queries
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let ready = self.delivered.load(std::sync::atomic::Ordering::SeqCst) && observed >= 2;
            Ok(tonic::Response::new(
                adx_protocol::relay::AdxletProcessState {
                    identity: None,
                    phase: if ready { "ready" } else { "starting" }.into(),
                    node_session: "next".into(),
                },
            ))
        }
    }

    fn wait_worker(tasks: &Tasks) {
        for _ in 0..1000 {
            if tasks
                .active
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(|reservation| reservation.worker_finished)
            {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("step worker did not finish");
    }

    #[test]
    fn both_legs_replay_lost_start_replies_with_pinned_files_and_fixed_deadlines() {
        for original in [false, true] {
            start_leg_replays_same_attempt_after_lost_replies(original);
        }
    }

    fn start_leg_replays_same_attempt_after_lost_replies(original: bool) {
        use adx_protocol::relay::adxlet_process_service_server::AdxletProcessServiceServer;
        use tonic::transport::Server;
        const ATTEMPT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let task_id = "a".repeat(64);
        let mut h = TestHarness::new(&task_id);
        let runtime = test_runtime(2);
        let mut receiver = h.executor(&runtime, 8);
        let target_bytes = b"owned prepared target executable bytes";
        let target_sha256 = sha256_hex(target_bytes);
        std::fs::write(h.directory.join("executable"), target_bytes).unwrap();
        let admin_socket = h.root.path().join("admin.sock");
        let original_bytes = b"different owned original executable bytes";
        std::fs::write(h.directory.join("original"), original_bytes).unwrap();
        let (original_process, target_process) = pin_snapshots(&h.directory, &admin_socket);
        let target = if original {
            original_process
        } else {
            target_process
        };
        let executable = h
            .directory
            .join(if original { "original" } else { "executable" });

        // The owned Child is this test process so peer-credential checks on
        // the bootstrap and admin sockets see the actual candidate PID.
        let pid = std::process::id();
        let delivered = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let queries = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let listener = runtime
            .block_on(async { tokio::net::UnixListener::bind(&admin_socket) })
            .unwrap();
        let stub = ReadyAfterBootstrapStub {
            delivered: delivered.clone(),
            queries: queries.clone(),
        };
        runtime.spawn(async move {
            Server::builder()
                .add_service(AdxletProcessServiceServer::new(stub))
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
                .await
                .unwrap();
        });
        let bootstrap = h.root.path().join(format!("{}-start.sock", target.id));
        let reader_path = bootstrap.clone();
        let reader_delivered = delivered.clone();
        let reader = runtime.spawn(async move {
            use tokio::io::AsyncReadExt;
            loop {
                if let Ok(mut socket) = tokio::net::UnixStream::connect(&reader_path).await {
                    let mut payload = Vec::new();
                    socket.read_to_end(&mut payload).await.unwrap();
                    let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
                    reader_delivered.store(true, std::sync::atomic::Ordering::SeqCst);
                    return value;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        });

        let mut submission = adxlet_submission(&task_id, &h.service_id);
        submission.sha256 = Some(target_sha256.clone());
        submission
            .process
            .as_mut()
            .unwrap()
            .identity
            .original_sha256 = sha256_hex(original_bytes);
        let deadline_unix_ms = adx_core::observation::now_millis().unwrap() + 60_000;
        let mut task = prepared_task(submission, Phase::Unknown, deadline_unix_ms);
        task.prepared.as_mut().unwrap().binary_sha256 = target_sha256;
        task.adxlet_execution = Some(adxlet::Execution {
            attempt: ATTEMPT.into(),
            leg: if original {
                adxlet::ProcessLeg::Rollback {
                    from: adxlet::AttemptIdentity {
                        attempt: "target-attempt".into(),
                        node_session: "target-session".into(),
                    },
                }
            } else {
                adxlet::ProcessLeg::Target
            },
            ..target_execution(adxlet::Stage::Exited, deadline_unix_ms)
        });
        h.install(&task);

        let target_config = target.clone();
        let actor = runtime.spawn(async move {
            let (mut starts, mut completes, mut spawn_receipts, mut inspections) =
                (Vec::new(), 0, 0, 0);
            loop {
                let command = receiver.recv().await.unwrap();
                match command.operation {
                    actor::Operation::Start {
                        attempt,
                        pid: old,
                        config,
                        binary,
                        launch: Launch::Adxlet { bootstrap, .. },
                        ..
                    } => {
                        assert_eq!(old, 4242);
                        assert_eq!(binary, executable);
                        assert_eq!(
                            serde_json::to_value(config).unwrap(),
                            serde_json::to_value(&target_config).unwrap()
                        );
                        starts.push((attempt, bootstrap));
                        // The Child is created, but the Start reply is lost.
                        drop(command.reply);
                    }
                    actor::Operation::Receipt {
                        pid: observed,
                        attempt,
                        old_pid,
                        ..
                    } => {
                        assert_eq!((attempt.as_str(), old_pid), (ATTEMPT, 4242));
                        if observed.is_none() {
                            // The lost Start is resolved by observing first.
                            assert_eq!(starts.len(), 1, "Receipt precedes any re-Start");
                            spawn_receipts += 1;
                        }
                        command
                            .reply
                            .send(Ok(actor::Reply::Candidate { pid, exited: false }))
                            .ok();
                    }
                    actor::Operation::RuntimeInspect => {
                        inspections += 1;
                        assert_eq!(command.expected_pid, Some(pid));
                        command
                            .reply
                            .send(Ok(actor::Reply::Runtime {
                                config: Box::new(target_config.clone()),
                                pid,
                                executable: std::fs::File::open(&executable).unwrap(),
                                admin_pid: pid,
                                admin_socket: target_config.admin_socket.clone().unwrap(),
                                relay: None,
                            }))
                            .ok();
                    }
                    actor::Operation::Commit {
                        pid: selected,
                        old_pid,
                        attempt,
                        stage: CommitStage::Select,
                        ..
                    } => {
                        assert_eq!((selected, old_pid, attempt.as_str()), (pid, 4242, ATTEMPT));
                        completes += 1;
                        // The first Complete reply is lost after the commit.
                        if completes > 1 {
                            command
                                .reply
                                .send(Ok(actor::Reply::Candidate { pid, exited: false }))
                                .ok();
                            return (starts, completes, spawn_receipts, inspections);
                        }
                    }
                    _ => panic!("Start must not Fence, Observe or StopFailed"),
                }
            }
        });

        let started = h
            .tasks
            .handle(step_request(
                &task_id,
                if original {
                    Step::Rollback
                } else {
                    Step::Start
                },
                if original { "target-attempt" } else { ATTEMPT },
                if original { "target-session" } else { "next" },
            ))
            .unwrap();
        assert_eq!(
            started.adxlet_execution.as_ref().unwrap().stage,
            adxlet::Stage::Exited
        );
        wait_worker(&h.tasks);
        assert_eq!(
            load(&h.directory).unwrap().phase,
            if original {
                Phase::Failed
            } else {
                Phase::Completed
            }
        );
        let (starts, completes, spawn_receipts, inspections) = runtime.block_on(actor).unwrap();
        assert!(queries.load(std::sync::atomic::Ordering::SeqCst) >= 3);
        assert_eq!(
            inspections, 1,
            "verify the owned executable when ready, not every pending poll"
        );
        assert_eq!(
            starts.len(),
            1,
            "a lost Start reply is observed, never re-created"
        );
        assert_eq!(spawn_receipts, 1);
        assert!(starts
            .iter()
            .all(|(attempt, path)| attempt == ATTEMPT && path == &bootstrap));
        assert_eq!(completes, 2);
        let payload = runtime.block_on(reader).unwrap();
        assert_eq!(payload["node_session"], "next");
        assert_eq!(payload["owner"]["local_task_id"], task_id);
        assert_eq!(payload["deadline_unix_millis"], deadline_unix_ms);
        wait_worker(&h.tasks);
        let loaded = load(&h.directory).unwrap();
        assert_eq!(
            loaded.phase,
            if original {
                Phase::Failed
            } else {
                Phase::Completed
            }
        );
        let execution = loaded.adxlet_execution.unwrap();
        assert_eq!(
            execution.stage,
            adxlet::Stage::Selected { candidate_pid: pid }
        );
        assert_eq!(execution.stage.candidate_pid(), Some(pid));
        assert_eq!(
            execution.deadline_unix_ms, deadline_unix_ms,
            "900s budget never refreshed"
        );
        assert!(h.tasks.active.lock().unwrap().is_none());
        assert!(!bootstrap.exists());
    }

    #[test]
    fn step_start_expired_commit_unknown_closes_by_receipt_only() {
        let mut h = TestHarness::new("start-commit");
        let runtime = test_runtime(2);
        let mut receiver = h.executor(&runtime, 4);
        let mut task = h.task(Phase::Unknown, 1);
        task.adxlet_execution = Some(target_execution(adxlet::Stage::Exiting, 1));
        h.install(&task);
        let start = || step_request("start-commit", Step::Start, "attempt", "next");
        // Start never runs before a reaped Exit recorded the attempt.
        let before = std::fs::read(h.directory.join("state.json")).unwrap();
        assert_eq!(h.tasks.handle(start()).unwrap_err(), "conflict");
        assert_eq!(
            std::fs::read(h.directory.join("state.json")).unwrap(),
            before
        );

        task.adxlet_execution.as_mut().unwrap().stage =
            adxlet::Stage::Committing { candidate_pid: 77 };
        store(&h.directory, &task).unwrap();
        let actor = runtime.spawn(async move {
            let command = receiver.recv().await.unwrap();
            match command.operation {
                actor::Operation::Receipt { pid, attempt, .. } => {
                    assert_eq!((pid, attempt.as_str()), (Some(77), "attempt"));
                    command.reply.send(Ok(actor::Reply::Committed)).ok();
                }
                _ => panic!("expired commit_unknown may only observe a Receipt"),
            }
        });
        h.tasks.handle(start()).unwrap();
        runtime.block_on(actor).unwrap();
        wait_worker(&h.tasks);
        let loaded = load(&h.directory).unwrap();
        assert_eq!(loaded.phase, Phase::Completed);
        let execution = loaded.adxlet_execution.unwrap();
        assert!(matches!(execution.stage, adxlet::Stage::Selected { .. }));
        assert_eq!(execution.deadline_unix_ms, 1);
    }

    #[test]
    fn both_legs_close_selection_store_failure_by_receipt_without_respawn() {
        for original in [false, true] {
            unpersisted_selection_falls_back_to_receipt(original);
        }
    }

    fn unpersisted_selection_falls_back_to_receipt(original: bool) {
        let commit_stage = adxlet::Stage::Committing { candidate_pid: 77 };
        let final_stage = adxlet::Stage::Selected { candidate_pid: 77 };
        let final_phase = if original {
            Phase::Failed
        } else {
            Phase::Completed
        };
        let mut h = TestHarness::new("start-unstored");
        let runtime = test_runtime(2);
        let mut receiver = h.executor(&runtime, 4);
        let sender = h.tasks.executor.as_ref().unwrap().clone();
        let deadline_unix_ms = adx_core::observation::now_millis().unwrap() + 60_000;
        let mut task = h.task(Phase::Unknown, deadline_unix_ms);
        task.adxlet_execution = Some(adxlet::Execution {
            leg: if original {
                adxlet::ProcessLeg::Rollback {
                    from: adxlet::AttemptIdentity {
                        attempt: "target-attempt".into(),
                        node_session: "target-session".into(),
                    },
                }
            } else {
                adxlet::ProcessLeg::Target
            },
            ..target_execution(
                adxlet::Stage::Committing { candidate_pid: 77 },
                deadline_unix_ms,
            )
        });
        h.install(&task);

        // The actor commits (and clears its rollout reservation); afterwards
        // it only answers an observing Receipt for the same pid/attempt and
        // rejects any Start, Fence, StopFailed or second Complete.
        let actor = runtime.spawn(async move {
            let mut operations = Vec::new();
            while let Some(command) = receiver.recv().await {
                let reply = match command.operation {
                    actor::Operation::Commit {
                        pid,
                        attempt,
                        stage: CommitStage::Select,
                        ..
                    } if operations.is_empty() => {
                        assert_eq!((pid, attempt.as_str()), (77, "attempt"));
                        actor::Reply::Candidate { pid, exited: false }
                    }
                    actor::Operation::Receipt {
                        pid,
                        old_pid,
                        attempt,
                        ..
                    } => {
                        assert_eq!(
                            (pid, old_pid, attempt.as_str()),
                            (Some(77), 4242, "attempt")
                        );
                        actor::Reply::Committed
                    }
                    _ => panic!("selected target must never be re-started or stopped"),
                };
                let receipt = matches!(reply, actor::Reply::Committed);
                operations.push(receipt);
                command.reply.send(Ok(reply)).ok();
                if receipt {
                    return operations;
                }
            }
            panic!("executor closed before Receipt");
        });

        // The `completed` store fails (the rename may or may not be durable).
        let unwritable = h.root.path().join("missing").join("start-unstored");
        let stopped = std::sync::atomic::AtomicBool::new(false);
        let mut worker_task = load(&h.directory).unwrap();
        let result = runtime.block_on(adxlet::start(
            &mut worker_task,
            &unwritable,
            &sender,
            &stopped,
        ));
        assert!(result.is_err());
        record_step_failure(&mut worker_task, Step::Start, "state store failed");
        assert_eq!(worker_task.phase, Phase::Unknown);
        let execution = worker_task.adxlet_execution.as_ref().unwrap();
        assert_eq!(execution.stage, commit_stage);
        assert_eq!(
            (
                execution.stage.candidate_pid(),
                execution.old_pid,
                execution.attempt.as_str()
            ),
            (Some(77), 4242, "attempt")
        );
        // Even an in-memory `completed` that never reached disk is demoted to
        // the Receipt closure, never to a re-startable `Starting`.
        let mut completed: Task =
            serde_json::from_value(serde_json::to_value(&worker_task).unwrap()).unwrap();
        completed.phase = final_phase;
        completed.adxlet_execution.as_mut().unwrap().stage = final_stage;
        record_step_failure(&mut completed, Step::Start, "state store failed");
        assert_eq!(
            serde_json::to_value(&completed).unwrap(),
            serde_json::to_value(&worker_task).unwrap()
        );

        // The worker's final store then succeeds; Query closes it by Receipt.
        store(&h.directory, &worker_task).unwrap();
        let queried = h
            .tasks
            .handle(Request::Query {
                task_id: "start-unstored".into(),
            })
            .unwrap();
        assert_eq!(runtime.block_on(actor).unwrap(), vec![false, true]);
        assert_eq!(queried.phase, final_phase);
        let loaded = load(&h.directory).unwrap();
        assert_eq!(loaded.phase, final_phase);
        let execution = loaded.adxlet_execution.unwrap();
        assert_eq!(execution.stage, final_stage);
        assert_eq!(execution.stage.candidate_pid(), Some(77));
        assert!(h.tasks.active.lock().unwrap().is_none());
    }

    #[test]
    fn query_restores_finished_terminal_and_releases_only_after_durable_store() {
        let h = TestHarness::new("terminal-held");
        // The worker durably wrote Completed, then its final finish/store
        // failed and it kept the finished reservation.
        let mut task = h.task(Phase::Completed, 1);
        task.finished = None;
        h.install(&task);
        let query = || Request::Query {
            task_id: "terminal-held".into(),
        };

        // While the store keeps failing the reservation stays held.
        std::fs::create_dir(h.directory.join("state.partial")).unwrap();
        assert_eq!(h.tasks.handle(query()).unwrap_err(), "state_unavailable");
        assert!(h.tasks.active.lock().unwrap().is_some());
        assert!(load(&h.directory).unwrap().finished.is_none());

        std::fs::remove_dir(h.directory.join("state.partial")).unwrap();
        let queried = h.tasks.handle(query()).unwrap();
        assert_eq!(queried.phase, Phase::Completed);
        assert!(queried.finished.is_some());
        let loaded = load(&h.directory).unwrap();
        assert_eq!(loaded.phase, Phase::Completed);
        assert!(loaded.finished.is_some());
        assert!(h.tasks.active.lock().unwrap().is_none());
    }

    struct BeginPendingStub {
        received: std::sync::Arc<std::sync::atomic::AtomicBool>,
        request:
            std::sync::Arc<std::sync::Mutex<Option<adx_protocol::relay::AdxletProcessRequest>>>,
    }

    #[tonic::async_trait]
    impl adx_protocol::relay::adxlet_process_service_server::AdxletProcessService for BeginPendingStub {
        async fn begin(
            &self,
            request: tonic::Request<adx_protocol::relay::AdxletProcessRequest>,
        ) -> std::result::Result<
            tonic::Response<adx_protocol::relay::AdxletProcessState>,
            tonic::Status,
        > {
            self.received
                .store(true, std::sync::atomic::Ordering::SeqCst);
            *self.request.lock().unwrap() = Some(request.into_inner());
            // Never complete: only the worker's saved phase deadline may end the
            // attempt. The stub must not return early and extend that deadline.
            std::future::pending::<()>().await;
            unreachable!("Begin stub is only cancelled by the worker deadline")
        }

        async fn query(
            &self,
            _request: tonic::Request<adx_protocol::relay::AdxletProcessRequest>,
        ) -> std::result::Result<
            tonic::Response<adx_protocol::relay::AdxletProcessState>,
            tonic::Status,
        > {
            Err(tonic::Status::unimplemented("query is not used by Exit"))
        }
    }

    #[test]
    fn step_exit_begin_received_before_deadline_converges_unknown_without_reap_or_refresh() {
        use adx_protocol::relay::adxlet_process_service_server::AdxletProcessServiceServer;
        use tonic::transport::Server;

        let mut h = TestHarness::new("exit-begin-deadline");
        let runtime = test_runtime(2);
        let mut receiver = h.executor(&runtime, 4);

        // The resume path reloads the original admin socket, which must point at
        // the local UDS stub served by this same process so peer-PID matching
        // passes against `old_pid == std::process::id()`.
        let admin_socket = h.root.path().join("admin.sock");
        let mut process = test_process();
        process.admin_socket = Some(admin_socket.clone());
        std::fs::write(
            h.directory.join("original-process.json"),
            serde_json::to_vec(&process).unwrap(),
        )
        .unwrap();

        let received = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let received_request = std::sync::Arc::new(std::sync::Mutex::new(None));
        let stub = BeginPendingStub {
            received: received.clone(),
            request: received_request.clone(),
        };
        // Binding a UnixListener requires a reactor; bind inside the runtime
        // instead of the sync test body, then hand the bound listener to the
        // spawned Tonic server task.
        let listener = runtime
            .block_on(async { tokio::net::UnixListener::bind(&admin_socket) })
            .unwrap();
        let server = runtime.spawn(async move {
            Server::builder()
                .add_service(AdxletProcessServiceServer::new(stub))
                .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(listener))
                .await
                .unwrap();
        });

        // A short, fixed phase deadline proves the same deadline closes the
        // Begin attempt without any real 900s wait. One second tolerates local
        // fsync jitter while still asserting the worker reached Begin and did
        // not refresh the deadline.
        let deadline_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 1000;
        let mut task = h.task(Phase::Unknown, deadline_unix_ms);
        task.adxlet_execution = Some(adxlet::Execution {
            old_pid: std::process::id(),
            attempt: String::new(),
            node_session: "old".into(),
            ..target_execution(adxlet::Stage::Exiting, deadline_unix_ms)
        });
        h.install(&task);

        let actor = runtime.spawn(async move {
            loop {
                let command = receiver.recv().await.unwrap();
                match command.operation {
                    actor::Operation::Observe { .. } | actor::Operation::Fence { .. } => command
                        .reply
                        .send(Ok(actor::Reply::State {
                            exited: false,
                            success: false,
                        }))
                        .ok(),
                    _ => panic!("unexpected exit worker command"),
                };
            }
        });

        let started = h
            .tasks
            .handle(step_request("exit-begin-deadline", Step::Exit, "", ""))
            .unwrap();
        assert_eq!(started.phase, Phase::Unknown);
        wait_worker(&h.tasks);

        // The worker had to actually connect and reach the Begin RPC before the
        // saved phase deadline ended the attempt.
        assert!(
            received.load(std::sync::atomic::Ordering::SeqCst),
            "Exit must reach the Begin RPC before the phase deadline"
        );

        let loaded = load(&h.directory).unwrap();
        let execution = loaded.adxlet_execution.unwrap();
        assert_eq!(execution.stage, adxlet::Stage::Exiting);
        assert!(
            execution.attempt.is_empty(),
            "no reap before the deadline, so no identity is minted"
        );
        assert_eq!(execution.node_session, "old");
        assert!(execution.error.is_some());
        assert_eq!(
            execution.deadline_unix_ms, deadline_unix_ms,
            "the phase deadline must not be refreshed"
        );
        assert_eq!(loaded.phase, Phase::Unknown);
        assert!(
            h.tasks
                .active
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .worker_finished,
            "a durable Unknown outcome stays recoverable"
        );

        // The Begin request carried the original saved deadline and identity.
        let request = received_request.lock().unwrap().clone().unwrap();
        assert_eq!(request.deadline_unix_millis, deadline_unix_ms);
        assert_eq!(request.old_node_session, "old");

        actor.abort();
        server.abort();
    }

    fn rollback_fixture(
        task_id: &str,
        runtime: &tokio::runtime::Runtime,
    ) -> (
        tempfile::TempDir,
        Tasks,
        tokio::sync::mpsc::Receiver<actor::Command>,
    ) {
        let mut h = TestHarness::new(task_id);
        let receiver = h.executor(runtime, 8);
        let original = b"fixed original executable";
        std::fs::write(h.directory.join("original"), original).unwrap();
        pin_snapshots(&h.directory, &h.root.path().join("original.sock"));
        let mut submission = adxlet_submission(task_id, &h.service_id);
        submission
            .process
            .as_mut()
            .unwrap()
            .identity
            .original_sha256 = sha256_hex(original);
        let deadline_unix_ms = adx_core::observation::now_millis().unwrap() + 900_000;
        let mut task = prepared_task(submission, Phase::Unknown, deadline_unix_ms);
        task.adxlet_execution = Some(adxlet::Execution {
            error: Some("target failed".into()),
            attempt: "A".into(),
            node_session: "session-A".into(),
            ..target_execution(
                adxlet::Stage::Starting {
                    candidate_pid: Some(77),
                },
                deadline_unix_ms,
            )
        });
        h.install(&task);
        (h.root, h.tasks, receiver)
    }

    #[test]
    fn rollback_external_start_rejects_original_b_without_effects() {
        let runtime = test_runtime(2);
        let (_root, tasks, mut receiver) = rollback_fixture("rollback-private-b", &runtime);
        let directory = tasks.directory.join("rollback-private-b");
        let mut task = load(&directory).unwrap();
        let execution = task.adxlet_execution.as_mut().unwrap();
        execution.stage = adxlet::Stage::Exited;
        execution.attempt = "B".into();
        execution.node_session = "session-B".into();
        execution.leg = adxlet::ProcessLeg::Rollback {
            from: adxlet::AttemptIdentity {
                attempt: "A".into(),
                node_session: "session-A".into(),
            },
        };
        store(&directory, &task).unwrap();
        let before = std::fs::read(directory.join("state.json")).unwrap();
        let result = tasks.step("rollback-private-b", Step::Start, "B", "session-B");
        assert!(result.is_err(), "only rollback(A) may start or resume B");
        assert_eq!(std::fs::read(directory.join("state.json")).unwrap(), before);
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn rollback_waits_for_reap_then_starts_original_and_replays_same_b() {
        let runtime = test_runtime(2);
        let (_root, tasks, mut receiver) = rollback_fixture("rollback-replay", &runtime);
        let directory = tasks.directory.join("rollback-replay");
        let actor_directory = directory.clone();
        let before = adx_core::observation::now_millis().unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let actor = runtime.spawn(async move {
            let (mut stops, mut observations, mut starts, mut b_receipts) = (0, 0, 0, 0);
            let mut reaped = false;
            let mut saved_deadline = None;
            let mut saved_b = None;
            loop {
                let command = receiver.recv().await.unwrap();
                let execution = load(&actor_directory).unwrap().adxlet_execution.unwrap();
                let reply = match command.operation {
                    actor::Operation::StopFailed {
                        pid,
                        old_pid,
                        attempt,
                        ..
                    } => {
                        assert_eq!((pid, old_pid, attempt.as_str()), (Some(77), 4242, "A"));
                        stops += 1;
                        assert!(matches!(
                            execution.stage,
                            adxlet::Stage::StoppingForRollback { .. }
                        ));
                        assert!(execution.deadline_unix_ms >= before + 599_000);
                        assert!(execution.deadline_unix_ms <= before + 601_000);
                        saved_deadline = Some(execution.deadline_unix_ms);
                        actor::Reply::Candidate {
                            pid: 77,
                            exited: false,
                        }
                    }
                    actor::Operation::Receipt { attempt, pid, .. } if attempt == "A" => {
                        assert_eq!(pid, Some(77));
                        observations += 1;
                        assert_eq!(execution.attempt, "A", "B requires a confirmed reap");
                        assert!(execution.leg.source().is_none());
                        if let Some(deadline) = saved_deadline {
                            assert_eq!(execution.deadline_unix_ms, deadline);
                        }
                        reaped = stops == 1 && observations >= 3;
                        actor::Reply::Candidate {
                            pid: 77,
                            exited: reaped,
                        }
                    }
                    actor::Operation::Start {
                        binary,
                        config,
                        attempt,
                        pid,
                        launch: Launch::Adxlet { mut executable, .. },
                        ..
                    } => {
                        use std::io::{Read, Seek};
                        assert!(reaped, "Start(B) must wait for real A reap");
                        starts += 1;
                        assert_eq!(starts, 1, "replay must not create another Child");
                        assert_eq!(pid, 4242);
                        assert_eq!(binary, actor_directory.join("original"));
                        let mut bytes = Vec::new();
                        executable.rewind().unwrap();
                        executable.read_to_end(&mut bytes).unwrap();
                        assert_eq!(bytes, b"fixed original executable");
                        assert_eq!(config.role, Role::Adxlet);
                        assert_eq!(attempt, execution.attempt);
                        assert_ne!(attempt, "A");
                        assert_eq!(execution.leg.source().unwrap().attempt, "A");
                        assert_eq!(execution.deadline_unix_ms, saved_deadline.unwrap());
                        saved_b = Some(attempt);
                        started_tx.send(()).unwrap();
                        // Keep the worker live while the publisher replays A.
                        release_rx
                            .recv_timeout(std::time::Duration::from_secs(5))
                            .unwrap();
                        actor::Reply::Candidate {
                            pid: 88,
                            exited: false,
                        }
                    }
                    actor::Operation::Receipt { attempt, pid, .. } => {
                        assert_eq!(Some(&attempt), saved_b.as_ref());
                        assert_eq!(execution.attempt, attempt);
                        assert_eq!(execution.deadline_unix_ms, saved_deadline.unwrap());
                        b_receipts += 1;
                        assert_eq!(pid, if b_receipts == 2 { None } else { Some(88) });
                        // The second worker observes the same owned Child; no Start.
                        actor::Reply::Candidate {
                            pid: 88,
                            exited: true,
                        }
                    }
                    _ => panic!("rollback must never stop B or start A"),
                };
                command.reply.send(Ok(reply)).ok();
                if b_receipts == 3 {
                    assert!(receiver.try_recv().is_err());
                    return (stops, starts, saved_deadline.unwrap(), saved_b.unwrap());
                }
            }
        });
        let request = || step_request("rollback-replay", Step::Rollback, "A", "session-A");
        tasks.handle(request()).unwrap();
        started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let live = std::fs::read(directory.join("state.json")).unwrap();
        let replay = tasks.handle(request()).unwrap();
        assert_eq!(replay.adxlet_execution.unwrap().stage.candidate_pid(), None);
        assert_eq!(std::fs::read(directory.join("state.json")).unwrap(), live);
        assert!(
            !tasks
                .active
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .worker_finished
        );
        release_tx.send(()).unwrap();
        wait_worker(&tasks);
        let first = load(&directory).unwrap();
        let execution = first.adxlet_execution.as_ref().unwrap();
        assert!(matches!(execution.stage, adxlet::Stage::Starting { .. }));
        assert_eq!(execution.stage.candidate_pid(), Some(88));
        assert_eq!(execution.leg.source().unwrap().node_session, "session-A");
        assert_eq!(
            tasks
                .handle(step_request(
                    "rollback-replay",
                    Step::Start,
                    "A",
                    "session-A"
                ))
                .unwrap_err(),
            "conflict"
        );
        tasks.handle(request()).unwrap();
        let (stops, starts, deadline, b) = runtime.block_on(actor).unwrap();
        wait_worker(&tasks);
        let restored = load(&directory).unwrap().adxlet_execution.unwrap();
        assert_eq!((stops, starts), (1, 1));
        assert_eq!(restored.attempt, b);
        assert_eq!(restored.node_session, execution.node_session);
        assert_eq!(restored.deadline_unix_ms, deadline);
        assert_eq!(restored.leg, execution.leg);
        assert!(matches!(restored.stage, adxlet::Stage::Starting { .. }));
    }

    #[test]
    fn rollback_committed_target_closes_success_without_stopping_or_minting() {
        let runtime = test_runtime(1);
        let (_root, tasks, mut receiver) = rollback_fixture("rollback-committed", &runtime);
        let directory = tasks.directory.join("rollback-committed");
        let mut task = load(&directory).unwrap();
        task.adxlet_execution.as_mut().unwrap().stage =
            adxlet::Stage::Committing { candidate_pid: 77 };
        task.adxlet_execution.as_mut().unwrap().deadline_unix_ms = 1;
        let deadline = task.adxlet_execution.as_ref().unwrap().deadline_unix_ms;
        store(&directory, &task).unwrap();
        let actor = runtime.spawn(async move {
            let command = receiver.recv().await.unwrap();
            assert!(matches!(
                command.operation,
                actor::Operation::Receipt { pid: Some(77), .. }
            ));
            command.reply.send(Ok(actor::Reply::Committed)).ok();
        });
        tasks
            .handle(step_request(
                "rollback-committed",
                Step::Rollback,
                "A",
                "session-A",
            ))
            .unwrap();
        runtime.block_on(actor).unwrap();
        wait_worker(&tasks);
        let task = load(&directory).unwrap();
        assert_eq!(task.phase, Phase::Completed);
        let execution = task.adxlet_execution.unwrap();
        assert!(matches!(execution.stage, adxlet::Stage::Selected { .. }));
        assert_eq!(execution.attempt, "A");
        assert!(execution.leg.source().is_none());
        assert_eq!(execution.deadline_unix_ms, deadline);
    }

    #[test]
    fn rollback_unknown_receipt_keeps_candidate_and_never_assumes_not_created() {
        let runtime = test_runtime(1);
        for committing in [false, true] {
            let (_root, tasks, mut receiver) = rollback_fixture("rollback-unknown", &runtime);
            if committing {
                let directory = tasks.directory.join("rollback-unknown");
                let mut task = load(&directory).unwrap();
                let execution = task.adxlet_execution.as_mut().unwrap();
                execution.stage = adxlet::Stage::Committing { candidate_pid: 77 };
                execution.deadline_unix_ms = 1;
                store(&directory, &task).unwrap();
            }
            let actor = runtime.spawn(async move {
                let command = receiver.recv().await.unwrap();
                assert!(matches!(
                    command.operation,
                    actor::Operation::Receipt { pid: Some(77), .. }
                ));
                command.reply.send(Err("receipt unavailable".into())).ok();
            });
            tasks
                .handle(step_request(
                    "rollback-unknown",
                    Step::Rollback,
                    "A",
                    "session-A",
                ))
                .unwrap();
            runtime.block_on(actor).unwrap();
            wait_worker(&tasks);
            let task = load(&tasks.directory.join("rollback-unknown")).unwrap();
            assert_eq!(task.phase, Phase::Unknown);
            let execution = task.adxlet_execution.unwrap();
            assert_eq!(execution.attempt, "A");
            assert_eq!(execution.stage.candidate_pid(), Some(77));
            assert!(execution.leg.source().is_none());
            assert!(tasks.active.lock().unwrap().is_some());
            if committing {
                assert_eq!(
                    execution.stage,
                    adxlet::Stage::Committing { candidate_pid: 77 }
                );
                assert_eq!(execution.deadline_unix_ms, 1);
            }
        }
    }

    #[test]
    fn rollback_freezes_budget_before_unavailable_receipt_and_never_refreshes_it() {
        let runtime = test_runtime(1);
        let (_root, tasks, mut receiver) = rollback_fixture("rollback-budget", &runtime);
        let directory = tasks.directory.join("rollback-budget");
        let actor = runtime.spawn(async move {
            for index in 0..3 {
                let Some(command) = receiver.recv().await else {
                    return;
                };
                assert!(matches!(
                    command.operation,
                    actor::Operation::Receipt { pid: Some(77), .. }
                ));
                let reply = if index < 2 {
                    Err("receipt unavailable".into())
                } else {
                    Ok(actor::Reply::Candidate {
                        pid: 77,
                        exited: false,
                    })
                };
                command.reply.send(reply).ok();
            }
        });
        let advance = || {
            tasks
                .handle(step_request(
                    "rollback-budget",
                    Step::Rollback,
                    "A",
                    "session-A",
                ))
                .unwrap();
            wait_worker(&tasks);
            load(&directory).unwrap().adxlet_execution.unwrap()
        };
        let before = adx_core::observation::now_millis().unwrap();
        let first = advance();
        assert_eq!(
            first.stage,
            adxlet::Stage::StoppingForRollback {
                candidate_pid: Some(77)
            }
        );
        assert!((before + 599_000..=before + 601_000).contains(&first.deadline_unix_ms));
        assert_eq!(advance().deadline_unix_ms, first.deadline_unix_ms);
        let mut expired = load(&directory).unwrap();
        expired.adxlet_execution.as_mut().unwrap().deadline_unix_ms = 1;
        store(&directory, &expired).unwrap();
        let expired = advance();
        runtime.block_on(actor).unwrap();
        assert_eq!(expired.deadline_unix_ms, 1);
        assert_eq!(
            expired.stage,
            adxlet::Stage::StoppingForRollback {
                candidate_pid: Some(77)
            }
        );
        assert_eq!(expired.attempt, "A");
        assert_eq!(expired.leg, adxlet::ProcessLeg::Target);
        assert!(tasks.active.lock().unwrap().is_some());
    }

    #[test]
    fn rollback_mint_store_failure_preserves_and_resaves_same_b() {
        let runtime = test_runtime(1);
        let (_root, tasks, mut receiver) = rollback_fixture("rollback-store", &runtime);
        let directory = tasks.directory.join("rollback-store");
        let actor_directory = directory.clone();
        let actor = runtime.spawn(async move {
            let receipt = receiver.recv().await.unwrap();
            assert!(matches!(
                receipt.operation,
                actor::Operation::Receipt { .. }
            ));
            receipt
                .reply
                .send(Ok(actor::Reply::Candidate {
                    pid: 77,
                    exited: true,
                }))
                .ok();
            let stop = receiver.recv().await.unwrap();
            assert!(matches!(
                stop.operation,
                actor::Operation::StopFailed { pid: Some(77), .. }
            ));
            // Make the B store fail after closure/reap, retaining its in-memory
            // identity just as an ambiguous rename/fsync failure must.
            std::fs::create_dir(actor_directory.join("state.partial")).unwrap();
            stop.reply
                .send(Ok(actor::Reply::Candidate {
                    pid: 77,
                    exited: true,
                }))
                .ok();
        });
        let mut task = load(&directory).unwrap();
        let sender = tasks.executor.as_ref().unwrap();
        let stopped = std::sync::atomic::AtomicBool::new(false);
        assert!(runtime
            .block_on(adxlet::rollback(&mut task, &directory, sender, &stopped))
            .is_err());
        runtime.block_on(actor).unwrap();
        let minted = task.adxlet_execution.as_ref().unwrap().attempt.clone();
        assert_ne!(minted, "A");
        assert_eq!(
            task.adxlet_execution
                .as_ref()
                .unwrap()
                .leg
                .source()
                .unwrap()
                .attempt,
            "A"
        );
        record_step_failure(&mut task, Step::Rollback, "B store unavailable");
        assert_eq!(
            task.adxlet_execution.as_ref().unwrap().stage,
            adxlet::Stage::Starting {
                candidate_pid: None
            }
        );
        std::fs::remove_dir(directory.join("state.partial")).unwrap();
        stopped.store(true, std::sync::atomic::Ordering::Release);
        // A stopping supervisor resaves B but cannot start it; no fresh budget.
        let deadline = task.adxlet_execution.as_ref().unwrap().deadline_unix_ms;
        assert!(runtime
            .block_on(adxlet::rollback(&mut task, &directory, sender, &stopped,))
            .is_err());
        assert_eq!(
            task.adxlet_execution.as_ref().unwrap().deadline_unix_ms,
            deadline
        );
        assert_eq!(
            load(&directory).unwrap().adxlet_execution.unwrap().attempt,
            minted
        );
        tasks
            .stopped
            .store(true, std::sync::atomic::Ordering::Release);
        let replay = tasks
            .handle(step_request(
                "rollback-store",
                Step::Rollback,
                "A",
                "session-A",
            ))
            .unwrap();
        assert_eq!(replay.adxlet_execution.unwrap().attempt, minted);
        wait_worker(&tasks);
        let saved = load(&directory).unwrap().adxlet_execution.unwrap();
        assert_eq!(saved.attempt, minted);
        assert_eq!(saved.deadline_unix_ms, deadline);
    }

    #[test]
    fn rollback_replay_keeps_saved_deadline_before_stop_and_after_mint() {
        let runtime = test_runtime(1);
        let (_root, tasks, mut receiver) = rollback_fixture("rollback-deadline", &runtime);
        let directory = tasks.directory.join("rollback-deadline");
        let mut task = load(&directory).unwrap();
        let deadline = adx_core::observation::now_millis().unwrap() + 30_000;
        let execution = task.adxlet_execution.as_mut().unwrap();
        execution.stage = adxlet::Stage::StoppingForRollback {
            candidate_pid: execution.stage.candidate_pid(),
        };
        execution.deadline_unix_ms = deadline;
        store(&directory, &task).unwrap();
        let actor_directory = directory.clone();
        let actor = runtime.spawn(async move {
            let receipt = receiver.recv().await.unwrap();
            receipt
                .reply
                .send(Ok(actor::Reply::Candidate {
                    pid: 77,
                    exited: true,
                }))
                .ok();
            let stop = receiver.recv().await.unwrap();
            assert!(matches!(
                stop.operation,
                actor::Operation::StopFailed { .. }
            ));
            assert_eq!(
                load(&actor_directory)
                    .unwrap()
                    .adxlet_execution
                    .unwrap()
                    .deadline_unix_ms,
                deadline
            );
            assert!(
                stop.expires
                    .saturating_duration_since(std::time::Instant::now())
                    <= std::time::Duration::from_secs(30)
            );
            stop.reply
                .send(Ok(actor::Reply::Candidate {
                    pid: 77,
                    exited: true,
                }))
                .ok();
            let start = receiver.recv().await.unwrap();
            assert!(matches!(start.operation, actor::Operation::Start { .. }));
            assert_eq!(
                load(&actor_directory)
                    .unwrap()
                    .adxlet_execution
                    .unwrap()
                    .deadline_unix_ms,
                deadline
            );
            start.reply.send(Ok(actor::Reply::NeverCreated)).ok();
        });
        tasks
            .handle(step_request(
                "rollback-deadline",
                Step::Rollback,
                "A",
                "session-A",
            ))
            .unwrap();
        runtime.block_on(actor).unwrap();
        wait_worker(&tasks);
        let execution = load(&directory).unwrap().adxlet_execution.unwrap();
        assert!(matches!(execution.stage, adxlet::Stage::Starting { .. }));
        assert_eq!(execution.deadline_unix_ms, deadline);
    }
}
