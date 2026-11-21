use crate::Result;
use adx_protocol::relay as pb;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Execution {
    pub phase: String,
    pub deadline_unix_ms: u64,
    pub old_pid: u32,
    pub old_socket: String,
    pub old_session: String,
    pub controller: String,
    pub epoch: u64,
    pub candidate_socket: String,
    pub candidate_session: String,
    pub admin_pid: u32,
    pub admin_socket: String,
    pub begin_accepted: bool,
    pub released: bool,
    pub old_exited: bool,
    pub loss_observed: bool,
    pub rollback_deadline_unix_ms: Option<u64>,
    pub failed_candidate_pid: Option<u32>,
    pub failed_candidate_session: String,
    pub failed_candidate_socket: String,
    pub failure_reason: String,
    pub failed_observed_session: String,
    pub failed_observed_epoch: Option<u64>,
}

pub(crate) enum Operation {
    RuntimeInspect,
    Adxlet(super::adxlet::Operation),
    Fence {
        pid: u32,
    },
    Start {
        config: crate::config::Process,
        pid: u32,
        binary: PathBuf,
        directory: PathBuf,
    },
    Observe {
        pid: u32,
    },
    Select {
        pid: u32,
    },
    Confirm {
        pid: u32,
    },
    Release,
    Candidate,
    StopFailed {
        pid: u32,
    },
    RestartFailed {
        pid: u32,
        binary: PathBuf,
        directory: PathBuf,
    },
}
pub(crate) enum Reply {
    NeverCreated,
    Committed,
    Runtime {
        config: Box<crate::config::Process>,
        pid: u32,
        executable: std::fs::File,
        admin_pid: u32,
        admin_socket: PathBuf,
        relay: Option<PathBuf>,
    },
    State {
        exited: bool,
        success: bool,
        candidate_pid: Option<u32>,
    },
    Candidate {
        pid: u32,
        exited: bool,
    },
}
pub(crate) struct Command {
    pub expected_pid: Option<u32>,
    pub service_id: String,
    pub task_id: String,
    pub operation: Operation,
    pub reply: oneshot::Sender<Result<Reply>>,
    pub expires: Instant,
}
pub(super) async fn command(
    sender: &mpsc::Sender<Command>,
    task: &super::Task,
    operation: Operation,
    deadline: Instant,
) -> Result<Reply> {
    command_with_pid(sender, task, operation, deadline, None).await
}

pub(super) async fn inspect_candidate(
    sender: &mpsc::Sender<Command>,
    task: &super::Task,
    pid: u32,
    deadline: Instant,
) -> Result<Reply> {
    command_with_pid(sender, task, Operation::RuntimeInspect, deadline, Some(pid)).await
}

async fn command_with_pid(
    sender: &mpsc::Sender<Command>,
    task: &super::Task,
    operation: Operation,
    deadline: Instant,
    candidate_pid: Option<u32>,
) -> Result<Reply> {
    check_budget(deadline)?;
    let (reply, result) = oneshot::channel();
    tokio::time::timeout(
        Duration::from_secs(2),
        sender.send(Command {
            expected_pid: if matches!(&operation, Operation::RuntimeInspect) {
                candidate_pid.or(match &task.submission.selection {
                    super::Selection::Publisher { expected } => Some(expected.pid),
                    _ => None,
                })
            } else {
                None
            },
            service_id: task.submission.service_id.clone(),
            task_id: task.submission.task_id.clone(),
            operation,
            reply,
            expires: deadline,
        }),
    )
    .await?
    .map_err(|_| "supervisor unavailable")?;
    tokio::time::timeout(Duration::from_secs(2), result).await??
}
fn check_budget(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        return Err("stage deadline exceeded; outcome retained as unknown".into());
    }
    Ok(())
}
fn stage_request<T>(value: T, deadline: Instant) -> Result<tonic::Request<T>> {
    check_budget(deadline)?;
    let mut request = tonic::Request::new(value);
    request.set_timeout(
        deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(2)),
    );
    Ok(request)
}
pub(crate) async fn channel(path: &Path, pid: u32) -> Result<Channel> {
    let path = path.to_path_buf();
    Ok(tokio::time::timeout(
        Duration::from_secs(2),
        tonic::transport::Endpoint::from_static("http://managed-local")
            .timeout(Duration::from_secs(2))
            .connect_timeout(Duration::from_secs(2))
            .connect_with_connector(tower::service_fn(move |_| {
                let path = path.clone();
                async move {
                    let socket = tokio::net::UnixStream::connect(path).await?;
                    if socket
                        .peer_cred()?
                        .pid()
                        .and_then(|pid| u32::try_from(pid).ok())
                        != Some(pid)
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "owned Child peer PID mismatch",
                        ));
                    }
                    Ok(hyper_util::rt::TokioIo::new(socket))
                }
            })),
    )
    .await??)
}
fn released(identity: &pb::RelayHandoffRequest, state: &pb::RelayUpgradeState) -> bool {
    state.task_id == identity.task_id
        && state.proxy_session_id == identity.old_proxy_session_id
        && state.listeners_released
        && state.drain_deadline_unix_ms > 0
        && matches!(
            state.phase.as_str(),
            "draining" | "completed" | "forced" | "interrupted" | "failed"
        )
}

fn unsupported_protocol(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unimplemented
}

// Called only on replies from a channel authenticated to the actual owned PID.
// A closed gate alone is recoverable; a foreign session/owner/epoch is not.
fn binding_failure(
    identity: &pb::RelayHandoffRequest,
    state: &pb::BindingState,
) -> Option<&'static str> {
    if state.proxy_session_id.is_empty() {
        return Some("empty_candidate_session");
    }
    if !identity.candidate_proxy_session_id.is_empty()
        && state.proxy_session_id != identity.candidate_proxy_session_id
    {
        return Some("candidate_session_conflict");
    }
    if state.sync_epoch == 0 {
        if state.ready || !state.controller_session_id.is_empty() {
            return Some("invalid_fresh_candidate");
        }
    } else if state.sync_epoch != 1
        || state.controller_session_id != identity.old_controller_session_id
    {
        return Some("candidate_owner_epoch_conflict");
    }
    None
}

pub(super) async fn execute(
    target_process: &crate::config::Process,
    task: &mut super::Task,
    directory: &Path,
    sender: &mpsc::Sender<Command>,
    stopped: &Arc<AtomicBool>,
    preparation_deadline: Instant,
) -> Result<()> {
    let mut begin_sent = false;
    let result = execute_inner(
        target_process,
        task,
        directory,
        sender,
        stopped,
        preparation_deadline,
        &mut begin_sent,
    )
    .await;
    if result.is_err() && !begin_sent {
        let mut cancelled = task.execution.is_none();
        if let Some(execution) = task.execution.as_ref() {
            let identity = identity(task, execution);
            while Instant::now() < preparation_deadline {
                if let Ok(channel) =
                    channel(Path::new(&execution.admin_socket), execution.admin_pid).await
                {
                    let mut admin =
                        pb::relay_handoff_service_client::RelayHandoffServiceClient::new(channel);
                    match admin
                        .cancel(stage_request(identity.clone(), preparation_deadline)?)
                        .await
                    {
                        Ok(_) => {
                            cancelled = true;
                            break;
                        }
                        Err(status) if status.code() == tonic::Code::NotFound => {
                            cancelled = true;
                            break;
                        }
                        Err(_) => {}
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        if cancelled
            && command(sender, task, Operation::Release, preparation_deadline)
                .await
                .is_ok()
        {
            task.phase = super::Phase::Failed;
            if let Some(execution) = task.execution.as_mut() {
                execution.phase = "cancelled_before_begin".into();
            }
            super::store(directory, task)?;
        }
    }
    result
}

fn identity(task: &super::Task, execution: &Execution) -> pb::RelayHandoffRequest {
    pb::RelayHandoffRequest {
        task_id: task.submission.task_id.clone(),
        old_socket: execution.old_socket.clone(),
        old_proxy_session_id: execution.old_session.clone(),
        old_controller_session_id: execution.controller.clone(),
        old_sync_epoch: execution.epoch,
        candidate_socket: execution.candidate_socket.clone(),
        candidate_proxy_session_id: execution.candidate_session.clone(),
    }
}

fn generation_sockets(root: &Path, task_id: &str) -> Result<(PathBuf, PathBuf)> {
    use sha2::{Digest, Sha256};
    let digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&["upgrade-uds-v1", "relay", task_id])?)
    );
    let key = &digest[..24];
    let directory = root.join("s");
    let candidate = directory.join(format!("r-{key}")).join("route.sock");
    let original = directory.join(format!("o-{key}")).join("route.sock");
    for path in [&candidate, &original] {
        super::check_socket_path(path)?;
    }
    Ok((candidate, original))
}

async fn execute_inner(
    target_process: &crate::config::Process,
    task: &mut super::Task,
    directory: &Path,
    sender: &mpsc::Sender<Command>,
    stopped: &Arc<AtomicBool>,
    preparation_deadline: Instant,
    begin_sent: &mut bool,
) -> Result<()> {
    if stopped.load(Ordering::Acquire) {
        return Err("supervisor stopping".into());
    }
    let expected = task.prepared_sha256()?;
    if target_process.configuration_sha256
        != task
            .prepared
            .as_ref()
            .ok_or("prepared identity missing")?
            .configuration_sha256
    {
        return Err("target configuration identity mismatch".into());
    }
    let mut target = std::fs::File::open(directory.join("executable"))?;
    super::artifact::verify_file(&mut target, expected, preparation_deadline, stopped)?;
    let Reply::Runtime {
        config: original_process,
        pid,
        executable,
        admin_pid,
        admin_socket,
        relay,
    } = command(
        sender,
        task,
        Operation::RuntimeInspect,
        preparation_deadline,
    )
    .await?
    else {
        return Err("missing owned Child".into());
    };
    let old_socket = relay.ok_or("Relay socket missing")?;
    let (candidate_socket, rollback_socket) = generation_sockets(
        admin_socket.parent().ok_or("missing state directory")?,
        &task.submission.task_id,
    )?;
    let old_channel = channel(&old_socket, pid).await?;
    let mut bindings = pb::relay_service_client::RelayServiceClient::new(old_channel.clone());
    let state = bindings
        .get_binding_state(stage_request(
            pb::GetBindingStateRequest {},
            preparation_deadline,
        )?)
        .await?
        .into_inner();
    if !state.ready || state.controller_session_id.is_empty() {
        return Err("old bindings not ready".into());
    }
    if let super::Selection::Publisher { expected } = &task.submission.selection {
        if pid != expected.pid || state.proxy_session_id != expected.relay_session {
            return Err("selected Relay generation changed".into());
        }
    }
    // Pin actual running bytes, not the replaceable installation pathname.
    let rollback = directory.join("rollback");
    let mut source = executable;
    super::artifact::copy_owned(&mut source, &rollback, stopped, preparation_deadline)?;
    if let super::Selection::Publisher { expected } = &task.submission.selection {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        let mut file = std::fs::File::open(&rollback)?;
        let mut digest = Sha256::new();
        let mut bytes = [0u8; 65536];
        loop {
            check_budget(preparation_deadline)?;
            let count = file.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            digest.update(&bytes[..count]);
        }
        if format!("{:x}", digest.finalize()) != expected.executable_sha256 {
            return Err("selected Relay executable changed".into());
        }
    }
    let generation_directory = candidate_socket
        .parent()
        .ok_or("missing generation directory")?;
    super::private_directory(
        generation_directory
            .parent()
            .ok_or("missing socket directory")?,
    )?;
    super::private_directory(generation_directory)?;
    let proof_deadline = preparation_deadline.min(Instant::now() + Duration::from_secs(5));
    let mut admin = pb::relay_handoff_service_client::RelayHandoffServiceClient::new(
        tokio::time::timeout_at(proof_deadline.into(), channel(&admin_socket, admin_pid)).await??,
    );
    if let super::Selection::Publisher { expected } = &task.submission.selection {
        let actual = tokio::time::timeout_at(proof_deadline.into(), async {
            let mut node = pb::node_admin_service_client::NodeAdminServiceClient::new(
                channel(&admin_socket, admin_pid).await?,
            );
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                node.inspect(stage_request(
                    pb::AdxletRuntimeInspectRequest {},
                    proof_deadline,
                )?)
                .await?
                .into_inner(),
            )
        })
        .await??;
        if !actual.ready
            || actual.node_session != expected.node_session
            || actual.relay_session != expected.relay_session
        {
            return Err("selected actual Node/Relay session changed".into());
        }
    }
    let Reply::Runtime {
        pid: checked_pid,
        admin_pid: checked_admin,
        relay: checked_socket,
        ..
    } = command(sender, task, Operation::RuntimeInspect, proof_deadline).await?
    else {
        return Err("runtime recheck missing".into());
    };
    if checked_pid != pid
        || checked_admin != admin_pid
        || checked_socket.as_ref() != Some(&old_socket)
    {
        return Err("Relay changed during preparation".into());
    }
    let mut identity = pb::RelayHandoffRequest {
        task_id: task.submission.task_id.clone(),
        old_socket: old_socket.to_string_lossy().into_owned(),
        old_proxy_session_id: state.proxy_session_id,
        old_controller_session_id: state.controller_session_id,
        old_sync_epoch: state.sync_epoch,
        candidate_socket: candidate_socket.to_string_lossy().into_owned(),
        candidate_proxy_session_id: String::new(),
    };
    // Readiness reservation must precede any irreversible old-generation command.
    task.execution = Some(Execution {
        phase: "reserving".into(),
        deadline_unix_ms: task.deadline_unix_ms,
        old_pid: pid,
        old_socket: identity.old_socket.clone(),
        old_session: identity.old_proxy_session_id.clone(),
        controller: identity.old_controller_session_id.clone(),
        epoch: identity.old_sync_epoch,
        candidate_socket: identity.candidate_socket.clone(),
        candidate_session: String::new(),
        admin_pid,
        admin_socket: admin_socket.to_string_lossy().into_owned(),
        begin_accepted: false,
        released: false,
        old_exited: false,
        loss_observed: false,
        rollback_deadline_unix_ms: None,
        failed_candidate_pid: None,
        failed_candidate_session: String::new(),
        failed_candidate_socket: String::new(),
        failure_reason: String::new(),
        failed_observed_session: String::new(),
        failed_observed_epoch: None,
    });
    super::store(directory, task)?;
    loop {
        if stopped.load(Ordering::Acquire)
            || Instant::now() + Duration::from_secs(5) >= preparation_deadline
        {
            return Err("reservation budget exhausted".into());
        }
        match admin
            .reserve(stage_request(identity.clone(), preparation_deadline)?)
            .await
        {
            Ok(_) => break,
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::FailedPrecondition | tonic::Code::InvalidArgument
                ) =>
            {
                return Err(status.into())
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let mut deadline = Instant::now() + Duration::from_secs(900);
    let deadline_unix_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?
    .checked_add(900_000)
    .ok_or("clock overflow")?;
    task.phase = super::Phase::Running;
    let execution = task.execution.as_mut().ok_or("missing execution")?;
    execution.phase = "reserved".into();
    execution.deadline_unix_ms = deadline_unix_ms;
    super::store(directory, task)?;
    if stopped.load(Ordering::Acquire) || Instant::now() >= deadline {
        return Err("execution budget exhausted before Begin".into());
    }
    command(sender, task, Operation::Fence { pid }, deadline).await?;
    task.execution.as_mut().ok_or("missing execution")?.phase = "begin_unknown".into();
    super::store(directory, task)?;
    let mut upgrade = pb::relay_upgrade_service_client::RelayUpgradeServiceClient::new(old_channel);
    let upgrade_request = pb::RelayUpgradeRequest {
        task_id: identity.task_id.clone(),
        proxy_session_id: identity.old_proxy_session_id.clone(),
    };
    let mut started = false;
    let mut rolling_back = false;
    let mut replacement_request = None;
    let mut confirmed_failure: Option<(u32, String)> = None;
    let mut failed_observation: Option<(String, u64)> = None;
    loop {
        if stopped.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err("execution stopped or deadline exceeded; generations retained".into());
        }
        let Ok(observation) = command(sender, task, Operation::Observe { pid }, deadline).await
        else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let (exited, success, candidate_pid) = match observation {
            Reply::State {
                exited,
                success,
                candidate_pid,
            } => (exited, success, candidate_pid),
            _ => return Err("invalid Child observation".into()),
        };
        let before = serde_json::to_vec(&task.execution)?;
        let execution = task.execution.as_mut().ok_or("missing execution")?;
        if exited && (execution.begin_accepted || execution.released) {
            execution.old_exited = true;
            execution.loss_observed |= !success;
        }
        if !exited {
            let result = if execution.begin_accepted {
                upgrade
                    .query(stage_request(upgrade_request.clone(), deadline)?)
                    .await
            } else {
                *begin_sent = true;
                upgrade
                    .begin(stage_request(upgrade_request.clone(), deadline)?)
                    .await
            };
            if let Ok(response) = result {
                let state = response.into_inner();
                if state.task_id != identity.task_id
                    || state.proxy_session_id != identity.old_proxy_session_id
                {
                    return Err("old generation identity mismatch".into());
                }
                if matches!(state.phase.as_str(), "forced" | "interrupted" | "failed") {
                    execution.loss_observed = true;
                }
                if !matches!(
                    state.phase.as_str(),
                    "accepted" | "draining" | "completed" | "forced" | "interrupted" | "failed"
                ) {
                    return Err("unknown old generation phase".into());
                }
                execution.begin_accepted = true;
                execution.released |= released(&identity, &state);
            }
        }
        if serde_json::to_vec(&task.execution)? != before {
            super::store(directory, task)?;
        }
        if started && !rolling_back {
            if let Ok(Reply::Candidate { pid, exited }) =
                command(sender, task, Operation::Candidate, deadline).await
            {
                if exited
                    || confirmed_failure
                        .as_ref()
                        .is_some_and(|(failed, _)| *failed == pid)
                {
                    check_budget(deadline)?;
                    if stopped.load(Ordering::Acquire) {
                        return Err("supervisor stopping".into());
                    }
                    // Either exact reaping or a PID-authenticated protocol conflict;
                    // transport/health failure alone never authorizes stopping.
                    let rollback_deadline = Instant::now() + Duration::from_secs(600);
                    let rollback_unix = u64::try_from(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)?
                            .as_millis(),
                    )?
                    .checked_add(600_000)
                    .ok_or("clock overflow")?;
                    let execution = task.execution.as_mut().ok_or("missing execution")?;
                    execution.phase = if exited {
                        "rollback_fencing"
                    } else {
                        "rollback_stopping"
                    }
                    .into();
                    execution.rollback_deadline_unix_ms = Some(rollback_unix);
                    execution.failed_candidate_pid = Some(pid);
                    execution.failed_candidate_session =
                        identity.candidate_proxy_session_id.clone();
                    execution.failed_candidate_socket = identity.candidate_socket.clone();
                    execution.loss_observed = true;
                    execution.failure_reason = confirmed_failure
                        .as_ref()
                        .map(|(_, reason)| reason.clone())
                        .unwrap_or_else(|| "candidate_reaped".into());
                    if let Some((session, epoch)) = failed_observation.as_ref() {
                        execution.failed_observed_session = session.clone();
                        execution.failed_observed_epoch = Some(*epoch);
                    }
                    super::store(directory, task)?;
                    deadline = rollback_deadline;
                    check_budget(deadline)?;
                    let replacement_socket = &rollback_socket;
                    let replacement_directory = replacement_socket
                        .parent()
                        .ok_or("missing generation parent")?;
                    super::private_directory(replacement_directory)?;
                    check_budget(deadline)?;
                    replacement_request = Some(pb::RelayReplacementRequest {
                        identity: Some(identity.clone()),
                        failed_pid: pid,
                        failed_exited: true,
                        replacement_socket: replacement_socket.to_string_lossy().into_owned(),
                    });
                    rolling_back = true;
                }
            }
        }
        if let Some(replacement) = replacement_request.as_ref() {
            // Once durably fenced, the failed generation's exit proof is fixed.
            // Restart may already have applied despite a lost actor reply.
            if task
                .execution
                .as_ref()
                .is_some_and(|execution| execution.phase == "rollback_stopping")
            {
                match command(sender, task, Operation::Candidate, deadline).await {
                    Ok(Reply::Candidate { pid, exited }) if pid == replacement.failed_pid => {
                        if !exited {
                            let _ = command(sender, task, Operation::StopFailed { pid }, deadline)
                                .await;
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                    _ => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
                task.execution.as_mut().ok_or("missing execution")?.phase =
                    "rollback_fencing".into();
                super::store(directory, task)?;
            }
            let Ok(response) = admin
                .replace_failed_candidate(stage_request(replacement.clone(), deadline)?)
                .await
            else {
                continue;
            };
            let replacement_identity = response
                .into_inner()
                .identity
                .ok_or("replacement identity missing")?;
            if replacement_identity.candidate_socket != replacement.replacement_socket
                || replacement_identity.task_id != identity.task_id
            {
                return Err("replacement identity mismatch".into());
            }
            if command(
                sender,
                task,
                Operation::RestartFailed {
                    pid: replacement.failed_pid,
                    binary: directory.join("rollback"),
                    directory: Path::new(&replacement.replacement_socket)
                        .parent()
                        .ok_or("missing rollback parent")?
                        .to_path_buf(),
                },
                deadline,
            )
            .await
            .is_err()
            {
                continue;
            }
            identity = replacement_identity;
            let execution = task.execution.as_mut().ok_or("missing execution")?;
            execution.phase = "rollback_starting".into();
            execution.candidate_socket = identity.candidate_socket.clone();
            execution.candidate_session.clear();
            super::store(directory, task)?;
            replacement_request = None;
            continue;
        }
        if rolling_back {
            if let Ok(Reply::Candidate { exited: true, .. }) =
                command(sender, task, Operation::Candidate, deadline).await
            {
                return Err(
                    "rollback generation exited; further replacement requires operator recovery"
                        .into(),
                );
            }
        }
        let execution = task.execution.as_ref().ok_or("missing execution")?;
        if execution.released || execution.old_exited {
            let proof = pb::RelayHandoffProof {
                identity: Some(identity.clone()),
                old_pid: pid,
                begin_accepted: execution.begin_accepted,
                listeners_released: execution.released,
                old_exited: execution.old_exited,
            };
            if admin
                .prove(stage_request(proof.clone(), deadline)?)
                .await
                .is_err()
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            if !started {
                if command(
                    sender,
                    task,
                    Operation::Start {
                        config: target_process.clone(),
                        pid,
                        binary: directory.join("executable"),
                        directory: candidate_socket
                            .parent()
                            .ok_or("missing generation directory")?
                            .to_path_buf(),
                    },
                    deadline,
                )
                .await
                .is_err()
                {
                    continue;
                }
                started = true;
            }
            if let Some(pid) = candidate_pid {
                let selected = if rolling_back {
                    &original_process
                } else {
                    target_process
                };
                let observed = inspect_candidate(sender, task, pid, deadline).await;
                let Ok(Reply::Runtime {
                    pid: actual_pid,
                    config: actual_config,
                    ..
                }) = observed
                else {
                    continue;
                };
                if actual_pid != pid
                    || actual_config.configuration_sha256 != selected.configuration_sha256
                {
                    continue;
                }
                if !rolling_back {
                    let expected_digest = task.prepared_sha256()?;
                    let mut executable = std::fs::File::open(format!("/proc/{pid}/exe"))?;
                    super::artifact::verify_file(
                        &mut executable,
                        expected_digest,
                        deadline,
                        stopped,
                    )?;
                }
                // The candidate's private endpoint must belong to our actual Child.
                if let Ok(candidate_channel) =
                    channel(Path::new(&identity.candidate_socket), pid).await
                {
                    let mut candidate =
                        pb::relay_service_client::RelayServiceClient::new(candidate_channel);
                    let response = match candidate
                        .get_binding_state(stage_request(pb::GetBindingStateRequest {}, deadline)?)
                        .await
                    {
                        Ok(response) => response,
                        Err(status) => {
                            if !rolling_back && unsupported_protocol(&status) {
                                confirmed_failure =
                                    Some((pid, "binding_protocol_unsupported".into()));
                            }
                            continue;
                        }
                    };
                    let state = response.into_inner();
                    if let Some(reason) = binding_failure(&identity, &state) {
                        if rolling_back {
                            return Err(reason.into());
                        }
                        confirmed_failure = Some((pid, reason.into()));
                        failed_observation = Some((
                            state.proxy_session_id.chars().take(128).collect(),
                            state.sync_epoch,
                        ));
                        continue;
                    }
                    if identity.candidate_proxy_session_id.is_empty() {
                        if state.sync_epoch != 0 {
                            if rolling_back {
                                return Err("rollback candidate not fresh".into());
                            }
                            confirmed_failure = Some((pid, "candidate_not_fresh".into()));
                            failed_observation = Some((
                                state.proxy_session_id.chars().take(128).collect(),
                                state.sync_epoch,
                            ));
                            continue;
                        }
                        identity.candidate_proxy_session_id = state.proxy_session_id.clone();
                        task.execution
                            .as_mut()
                            .ok_or("missing execution")?
                            .candidate_session = state.proxy_session_id;
                        super::store(directory, task)?;
                    }
                    if let Ok(response) = admin
                        .switch(stage_request(identity.clone(), deadline)?)
                        .await
                    {
                        let switched = response.into_inner();
                        if !matches!(switched.phase.as_str(), "active_fenced" | "completed")
                            || !proof.old_exited
                        {
                            continue;
                        }
                        // Never use a previous Switch boolean as current liveness.
                        let Ok(current_channel) =
                            channel(Path::new(&identity.candidate_socket), pid).await
                        else {
                            continue;
                        };
                        let mut candidate =
                            pb::relay_service_client::RelayServiceClient::new(current_channel);
                        let Ok(current_response) = candidate
                            .get_binding_state(stage_request(
                                pb::GetBindingStateRequest {},
                                deadline,
                            )?)
                            .await
                        else {
                            continue;
                        };
                        let current = current_response.into_inner();
                        if let Some(reason) = binding_failure(&identity, &current) {
                            if rolling_back {
                                return Err(reason.into());
                            }
                            confirmed_failure = Some((pid, reason.into()));
                            failed_observation = Some((
                                current.proxy_session_id.chars().take(128).collect(),
                                current.sync_epoch,
                            ));
                            continue;
                        }
                        if !current.ready || current.sync_epoch != switched.candidate_sync_epoch {
                            return Err("candidate changed before Finish".into());
                        }
                        if command(sender, task, Operation::Confirm { pid }, deadline)
                            .await
                            .is_err()
                        {
                            continue;
                        }
                        let mut final_proof = proof.clone();
                        final_proof.identity = Some(identity.clone());
                        let Ok(receipt) = admin.finish(stage_request(final_proof, deadline)?).await
                        else {
                            continue;
                        };
                        if receipt.into_inner().phase != "completed" {
                            continue;
                        }
                        // Reconnect checks SO_PEERCRED again after Finish as well.
                        let Ok(final_channel) =
                            channel(Path::new(&identity.candidate_socket), pid).await
                        else {
                            continue;
                        };
                        let mut candidate =
                            pb::relay_service_client::RelayServiceClient::new(final_channel);
                        let Ok(final_response) = candidate
                            .get_binding_state(stage_request(
                                pb::GetBindingStateRequest {},
                                deadline,
                            )?)
                            .await
                        else {
                            continue;
                        };
                        let final_state = final_response.into_inner();
                        if let Some(reason) = binding_failure(&identity, &final_state) {
                            if rolling_back {
                                return Err(reason.into());
                            }
                            confirmed_failure = Some((pid, reason.into()));
                            failed_observation = Some((
                                final_state.proxy_session_id.chars().take(128).collect(),
                                final_state.sync_epoch,
                            ));
                            continue;
                        }
                        if final_state != current {
                            continue;
                        }
                        if command(sender, task, Operation::Select { pid }, deadline)
                            .await
                            .is_err()
                        {
                            continue;
                        }
                        let execution = task.execution.as_mut().ok_or("missing execution")?;
                        execution.phase = if rolling_back {
                            "rolled_back"
                        } else if execution.loss_observed {
                            "failed_retained"
                        } else {
                            "completed"
                        }
                        .into();
                        task.phase = super::relay_phase(&execution.phase);
                        super::store(directory, task)?;
                        return Ok(());
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn generation_paths_accept_full_publisher_identity_and_bind_both_legs() {
        let root = tempfile::tempdir().unwrap();
        let task_id = "a".repeat(64);
        let paths = generation_sockets(root.path(), &task_id).unwrap();
        assert_eq!(paths, generation_sockets(root.path(), &task_id).unwrap());
        assert_ne!(paths.0, paths.1);
        let other = format!("{}b", "a".repeat(63));
        assert_ne!(paths.0, generation_sockets(root.path(), &other).unwrap().0);
        for path in [paths.0, paths.1] {
            assert!(path.as_os_str().len() <= 100);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            let _pair = tokio::try_join!(tokio::net::UnixStream::connect(&path), listener.accept())
                .unwrap();
        }
    }

    #[test]
    fn generation_paths_accept_pod_uid_state_directory() {
        let root = Path::new("/work/supervisor-00000000-0000-0000-0000-000000000000");
        generation_sockets(root, &"a".repeat(64))
            .expect("both Relay generations fit the socket path limit");
    }

    #[test]
    fn generation_paths_check_byte_limit_before_either_leg() {
        let task_id = "a".repeat(64);
        let suffix = generation_sockets(Path::new("/"), &task_id)
            .unwrap()
            .0
            .as_os_str()
            .len();
        let root = format!("/{}", "x".repeat(99 - suffix));
        let (candidate, original) = generation_sockets(Path::new(&root), &task_id).unwrap();
        assert_eq!(candidate.as_os_str().len(), 100);
        assert_eq!(original.as_os_str().len(), 100);
        assert!(generation_sockets(Path::new(&format!("{root}x")), &task_id).is_err());
        let unicode = format!("/{}界", "x".repeat(97 - suffix));
        assert!(generation_sockets(Path::new(&unicode), &task_id).is_err());
        assert!(generation_sockets(Path::new("relative"), &task_id).is_err());
    }

    #[test]
    fn only_irrecoverable_binding_evidence_triggers_live_failure() {
        let identity = pb::RelayHandoffRequest {
            candidate_proxy_session_id: "candidate".into(),
            old_controller_session_id: "owner".into(),
            ..Default::default()
        };
        let mut state = pb::BindingState {
            proxy_session_id: "candidate".into(),
            controller_session_id: "owner".into(),
            sync_epoch: 1,
            ready: false,
        };
        assert!(binding_failure(&identity, &state).is_none());
        state.ready = true;
        assert!(binding_failure(&identity, &state).is_none());
        state.controller_session_id = "other".into();
        assert!(binding_failure(&identity, &state).is_some());
        state.controller_session_id = "owner".into();
        state.sync_epoch = 2;
        assert!(binding_failure(&identity, &state).is_some());
        state.sync_epoch = 1;
        state.proxy_session_id = "different".into();
        assert!(binding_failure(&identity, &state).is_some());
        for code in [
            tonic::Code::Unavailable,
            tonic::Code::DeadlineExceeded,
            tonic::Code::Unknown,
            tonic::Code::FailedPrecondition,
        ] {
            assert!(!unsupported_protocol(&tonic::Status::new(
                code,
                "temporary or ambiguous"
            )));
        }
        assert!(unsupported_protocol(&tonic::Status::unimplemented(
            "unknown service"
        )));
    }

    #[tokio::test]
    async fn expired_stage_cannot_enqueue_actor_mutation() {
        let (sender, mut receiver) = mpsc::channel(1);
        let task = super::super::Task {
            finished: None,
            prepared: None,
            submission: super::super::Submission {
                component_config: None,
                selection: super::super::Selection::LocalManual,
                task_id: "deadline".into(),
                service_id: "relay".into(),
                role: crate::config::Role::Relay,
                commit: "a".repeat(40),
                sha256: Some("b".repeat(64)),
                remote_archive: None,
                process: None,
            },
            phase: super::super::Phase::Preparing,
            deadline_unix_ms: 1,
            execution: None,
            adxlet_execution: None,
        };
        assert!(command(
            &sender,
            &task,
            Operation::Select { pid: 42 },
            Instant::now()
        )
        .await
        .is_err());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn release_is_exact_and_transport_failure_is_not_exit() {
        let identity = adx_protocol::relay::RelayHandoffRequest {
            task_id: "task".into(),
            old_proxy_session_id: "old".into(),
            ..Default::default()
        };
        let mut state = adx_protocol::relay::RelayUpgradeState {
            task_id: "task".into(),
            proxy_session_id: "old".into(),
            phase: "draining".into(),
            listeners_released: true,
            drain_deadline_unix_ms: 1,
        };
        assert!(released(&identity, &state));
        state.task_id = "other".into();
        assert!(!released(&identity, &state));
        state.task_id = "task".into();
        state.phase = "forced".into();
        assert!(released(&identity, &state));
    }

    #[test]
    fn late_rpc_reply_cannot_authorize_next_stage_mutation() {
        let expired = Instant::now();
        assert!(stage_request(pb::GetBindingStateRequest {}, expired).is_err());
        assert!(check_budget(expired).is_err());
    }

    #[tokio::test]
    async fn control_channel_rejects_a_socket_not_owned_by_the_child() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });
        assert!(channel(&path, u32::MAX).await.is_err());
        drop(accept.await.unwrap());
    }
}
