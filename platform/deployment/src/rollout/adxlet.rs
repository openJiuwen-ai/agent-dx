//! Parent-owned executable selection and a single candidate startup path.
use super::actor::{command, Command, CommitStage, Launch, Operation, Owner, Reply};
use crate::Result;
use adx_protocol::relay as pb;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Execution {
    pub old_pid: u32,
    #[serde(flatten)]
    pub stage: Stage,
    pub error: Option<String>,
    pub attempt: String,
    pub node_session: String,
    pub deadline_unix_ms: u64,
    pub leg: ProcessLeg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum Stage {
    Exiting,
    Exited,
    Starting { candidate_pid: Option<u32> },
    Committing { candidate_pid: u32 },
    StoppingForRollback { candidate_pid: Option<u32> },
    Selected { candidate_pid: u32 },
}

impl Stage {
    pub(super) fn candidate_pid(self) -> Option<u32> {
        match self {
            Self::Starting { candidate_pid } | Self::StoppingForRollback { candidate_pid } => {
                candidate_pid
            }
            Self::Committing { candidate_pid } | Self::Selected { candidate_pid } => {
                Some(candidate_pid)
            }
            Self::Exiting | Self::Exited => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProcessLeg {
    Target,
    Rollback { from: AttemptIdentity },
}

impl ProcessLeg {
    pub(super) fn source(&self) -> Option<&AttemptIdentity> {
        match self {
            Self::Target => None,
            Self::Rollback { from } => Some(from),
        }
    }

    pub(super) fn selected_phase(&self) -> super::Phase {
        match self {
            Self::Target => super::Phase::Completed,
            Self::Rollback { .. } => super::Phase::Failed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct AttemptIdentity {
    pub attempt: String,
    pub node_session: String,
}

impl Execution {
    pub(super) fn startable(&self) -> bool {
        !self.attempt.is_empty()
            && matches!(
                self.stage,
                Stage::Exited | Stage::Starting { .. } | Stage::Committing { .. }
            )
    }
}

/// Rendered target Process the later `start` step launches.
pub(super) const TARGET_PROCESS: &str = "target-process.json";
/// Rendered original Process captured with the owned `original` copy.
pub(super) const ORIGINAL_PROCESS: &str = "original-process.json";
const MAX_SNAPSHOT_BYTES: usize = 1024 * 1024;

/// Prepare-only adxlet Submit. Verifies the prepared target executable,
/// takes one RuntimeInspect snapshot of the owned original, verifies and
/// copies its executable, then records both rendered Process snapshots in the
/// private task directory. It never fences, stops or contacts the Coordinator;
/// the caller publishes `Task.prepared` only after this returns Ok.
pub(super) async fn prepare(
    task: &super::Task,
    directory: &Path,
    sender: &tokio::sync::mpsc::Sender<Command>,
    stopped: &AtomicBool,
    deadline: Instant,
    binary_sha256: &str,
    target: &crate::config::Process,
) -> Result<()> {
    let identity = task
        .submission
        .process
        .as_ref()
        .ok_or("missing parent selection")?;
    if target.binary != directory.join("executable") || target.configuration_sha256.is_empty() {
        return Err("target process snapshot invalid".into());
    }
    bootstrap_socket(target)?;
    let mut prepared = std::fs::File::open(directory.join("executable"))?;
    super::artifact::verify_file(&mut prepared, binary_sha256, deadline, stopped)?;
    let Reply::Runtime {
        pid,
        mut executable,
        config,
        ..
    } = super::actor::command(
        sender,
        task,
        super::actor::Operation::RuntimeInspect,
        deadline,
    )
    .await?
    else {
        return Err("owned adxlet absent".into());
    };
    bootstrap_socket(&config)?;
    if let super::Selection::Publisher { expected } = &task.submission.selection {
        if expected.pid != pid || expected.configuration_sha256 != config.configuration_sha256 {
            return Err("selected runtime changed".into());
        }
    }
    let original_sha256 = &identity.identity.original_sha256;
    super::artifact::verify_file(&mut executable, original_sha256, deadline, stopped)?;
    super::artifact::copy_owned(
        &mut executable,
        &directory.join("original"),
        stopped,
        deadline,
    )?;
    // The live file may change between verification and copy; trust only the copy.
    let mut original = std::fs::File::open(directory.join("original"))?;
    super::artifact::verify_file(&mut original, original_sha256, deadline, stopped)?;
    // A `--config` path in the Process JSON does not fix the file contents:
    // pin the actual JSON of both sides into private task copies whose
    // effective digest matches the recorded Process, input=None included.
    let pinned_original = pin_config(directory, ORIGINAL_CONFIG, &config)?;
    let pinned_target = pin_config(directory, TARGET_CONFIG, target)?;
    write_snapshot(directory, ORIGINAL_PROCESS, &pinned_original)?;
    write_snapshot(directory, TARGET_PROCESS, &pinned_target)?;
    Ok(())
}

/// Private copy of the target Process configuration JSON.
pub(super) const TARGET_CONFIG: &str = "target-config.json";
/// Private copy of the original Process configuration JSON.
pub(super) const ORIGINAL_CONFIG: &str = "original-config.json";

/// Index of the single `--config` value in a rendered adxlet Process.
fn config_index(process: &crate::config::Process) -> Result<usize> {
    if process.args.iter().filter(|arg| *arg == "--config").count() != 1 {
        return Err("adxlet requires exactly one --config argument".into());
    }
    process
        .args
        .iter()
        .position(|arg| arg == "--config")
        .and_then(|index| index.checked_add(1))
        .filter(|index| *index < process.args.len())
        .ok_or_else(|| "adxlet --config value missing".into())
}

/// Bounded read of a configuration file. The live source may be a managed
/// symlink; a pinned private copy is never followed through a symlink.
fn read_config(path: &Path, follow: bool) -> Result<Vec<u8>> {
    use std::{io::Read, os::unix::fs::OpenOptionsExt};
    let flags = if follow {
        libc::O_NONBLOCK
    } else {
        libc::O_NOFOLLOW | libc::O_NONBLOCK
    };
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(flags)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err("configuration file required".into());
    }
    let mut bytes = Vec::new();
    file.take(MAX_SNAPSHOT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err("configuration exceeds fixed limit".into());
    }
    Ok(bytes)
}

/// The actual JSON plus the Process environment must produce the recorded
/// effective configuration digest.
fn verify_config(bytes: &[u8], process: &crate::config::Process) -> Result<()> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    if crate::config::configuration_sha256(&value, &process.env)? != process.configuration_sha256 {
        return Err("configuration file digest mismatch".into());
    }
    Ok(())
}

/// Reads the actual `--config` file, verifies its effective digest, stores a
/// private copy as `name` and returns the Process pointing at that copy.
fn pin_config(
    directory: &Path,
    name: &str,
    process: &crate::config::Process,
) -> Result<crate::config::Process> {
    let index = config_index(process)?;
    let source = process
        .args
        .get(index)
        .ok_or("adxlet --config value missing")?;
    let bytes = read_config(Path::new(source), true)?;
    verify_config(&bytes, process)?;
    write_private(directory, name, &bytes)?;
    let mut pinned = process.clone();
    let value = pinned
        .args
        .get_mut(index)
        .ok_or("adxlet --config value missing")?;
    *value = directory.join(name).to_string_lossy().into_owned();
    Ok(pinned)
}

/// A pinned Process must still name exactly its private copy, and that copy
/// must still produce the recorded effective digest.
fn validate_pinned(directory: &Path, name: &str, process: &crate::config::Process) -> Result<()> {
    let index = config_index(process)?;
    let path = directory.join(name);
    if process.args.get(index).map(Path::new) != Some(path.as_path()) {
        return Err("pinned configuration path changed".into());
    }
    verify_config(&read_config(&path, false)?, process)
}

/// First Exit confirms the frozen original still owns the adxlet with a
/// complete runtime observation (PID, configuration digest and Node/Relay
/// session), then re-verifies the prepared target executable, the owned
/// original copy and the live original bytes before any side effect. It
/// persists a fixed 900s `Exiting`, then fences and observes until the
/// Child is actually reaped. `resume` replays an already-persisted intent under
/// the same fixed deadline and Observes first rather than forcing the old PID
/// to still be live. On the real reap the next attempt/node_session is minted
/// exactly once and recorded as `exited`. Every failure is returned to the thin
/// Step worker, which records the outcome; a reap whose mint or store fails is
/// never reported as success.
pub(super) async fn exit(
    task: &mut super::Task,
    directory: &Path,
    sender: &tokio::sync::mpsc::Sender<Command>,
    stopped: &AtomicBool,
    resume: bool,
    boot_nonce: &str,
    pod_uid: Option<String>,
) -> Result<()> {
    let identity = task
        .submission
        .process
        .clone()
        .ok_or("missing parent selection")?;

    let (old_pid, admin_socket, deadline, deadline_unix_ms) = if resume {
        let execution = task
            .adxlet_execution
            .as_ref()
            .ok_or("missing exit intent")?;
        let admin_socket =
            load_original_admin_socket(directory).ok_or("original admin socket missing")?;
        let remaining = execution.deadline_unix_ms.saturating_sub(
            adx_core::observation::now_millis().unwrap_or(execution.deadline_unix_ms),
        );
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(remaining))
            .unwrap_or_else(Instant::now);
        (
            execution.old_pid,
            admin_socket,
            deadline,
            execution.deadline_unix_ms,
        )
    } else {
        let deadline = Instant::now() + Duration::from_secs(900);
        let deadline_unix_ms = adx_core::observation::now_millis()
            .ok()
            .and_then(|now| now.checked_add(900_000))
            .ok_or("deadline overflow")?;
        // Complete runtime identity, including the live Node/Relay session; a
        // publisher selection must match every frozen field, not only PID and
        // configuration digest.
        if let super::Selection::Publisher { expected } = &task.submission.selection {
            let actual = super::observe_runtime(
                sender,
                &task.submission.service_id,
                &identity.node_id,
                boot_nonce,
                pod_uid,
            )
            .await?;
            expected.require_current(&actual)?;
        }
        // Re-verify the three file-level proofs before the first side effect:
        // the prepared target executable, the owned original copy and the live
        // original bytes. Any change rejects the exit before the old process is
        // stopped.
        let target_digest = task.prepared_sha256()?.to_owned();
        let mut prepared = std::fs::File::open(directory.join("executable"))?;
        super::artifact::verify_file(&mut prepared, &target_digest, deadline, stopped)?;
        let original_sha256 = identity.identity.original_sha256.clone();
        let mut original = std::fs::File::open(directory.join("original"))?;
        super::artifact::verify_file(&mut original, &original_sha256, deadline, stopped)?;
        let (old_pid, mut executable, admin_socket, config) = match super::actor::command(
            sender,
            task,
            super::actor::Operation::RuntimeInspect,
            deadline,
        )
        .await?
        {
            Reply::Runtime {
                pid,
                executable,
                admin_socket,
                config,
                ..
            } => (pid, executable, admin_socket, config),
            _ => return Err("owned adxlet absent".into()),
        };
        // Every selection, including LocalManual, must re-confirm the frozen
        // original against the live RuntimeInspect before any Exiting or
        // Fence: the current configuration digest and admin socket must still
        // match the ORIGINAL_PROCESS snapshot captured during prepare. Any
        // drift rejects the exit before the old process is stopped.
        let original_snapshot =
            load_original_process(directory).ok_or("original process snapshot missing")?;
        if original_snapshot.configuration_sha256 != config.configuration_sha256
            || original_snapshot.admin_socket.as_ref() != Some(&admin_socket)
        {
            return Err("original process snapshot changed".into());
        }
        // The pinned configuration files, not only the Process metadata, must
        // still carry their recorded effective digests before any Fence.
        validate_pinned(directory, ORIGINAL_CONFIG, &original_snapshot)?;
        validate_pinned(directory, TARGET_CONFIG, &load_target_process(directory)?)?;
        // Re-fetch the frozen old PID and configuration digest after the file
        // proof; a publisher must still match them even if an automatic repull
        // advanced the generation between the initial observation and this
        // re-check. The three local file proofs above are retained and this
        // guard is an additional check, not a replacement for them.
        if let super::Selection::Publisher { expected } = &task.submission.selection {
            if expected.pid != old_pid
                || expected.configuration_sha256 != config.configuration_sha256
            {
                return Err("selected runtime changed".into());
            }
        }
        super::artifact::verify_file(&mut executable, &original_sha256, deadline, stopped)?;
        task.adxlet_execution = Some(Execution {
            old_pid,
            stage: Stage::Exiting,
            error: None,
            attempt: String::new(),
            node_session: identity.identity.old_node_session.clone(),
            deadline_unix_ms,
            leg: ProcessLeg::Target,
        });
        task.phase = super::Phase::Unknown;
        super::store(directory, task)?;
        (old_pid, admin_socket, deadline, deadline_unix_ms)
    };

    let begin = pb::AdxletProcessRequest {
        rollout_id: identity.identity.rollout_id.clone(),
        local_task_id: task.submission.task_id.clone(),
        epoch: identity.identity.epoch,
        old_node_session: identity.identity.old_node_session.clone(),
        old_relay_session: identity.identity.old_relay_session.clone(),
        deadline_unix_millis: deadline_unix_ms,
    };

    if resume {
        // A replayed intent Observes first rather than forcing the old PID to
        // still be live. Even when the saved phase deadline is exhausted, a
        // fresh bounded Observe-only confirmation is permitted: it can prove an
        // already-reaped Child without ever issuing Fence/Begin/Start or
        // refreshing the persisted phase deadline. A non-reaped result still
        // converges to Unknown.
        let observe_deadline = if Instant::now() >= deadline {
            Instant::now() + Duration::from_secs(2)
        } else {
            deadline
        };
        match command(
            sender,
            task,
            Operation::Observe {
                pid: old_pid,
                owner: Owner::Adxlet(identity.clone()),
            },
            observe_deadline,
        )
        .await
        {
            Ok(Reply::State { exited: true, .. }) => {
                record_reap(task, directory)?;
                return Ok(());
            }
            _ if Instant::now() >= deadline => {
                return Err("old process outcome unknown; maintenance retained".into());
            }
            _ => {}
        }
    }
    command(
        sender,
        task,
        Operation::Fence {
            pid: old_pid,
            owner: Owner::Adxlet(identity.clone()),
        },
        deadline,
    )
    .await?;
    loop {
        if stopped.load(Ordering::Acquire) {
            return Err("supervisor stopping".into());
        }
        if Instant::now() >= deadline {
            return Err("old process outcome unknown; maintenance retained".into());
        }
        // Transient Observe failures are retried, never treated as a reap.
        if let Ok(Reply::State { exited: true, .. }) = command(
            sender,
            task,
            Operation::Observe {
                pid: old_pid,
                owner: Owner::Adxlet(identity.clone()),
            },
            deadline,
        )
        .await
        {
            record_reap(task, directory)?;
            return Ok(());
        }
        // Connect + Begin share the same saved absolute phase deadline so a
        // near-expiry attempt cannot overrun it by the sum of the two 2s
        // transport timeouts. A stalled attempt is dropped; the loop re-checks
        // the deadline and keeps Observing rather than authorizing a stop.
        let _ = tokio::time::timeout_at(deadline.into(), async {
            if let Ok(channel) = super::relay::channel(&admin_socket, old_pid).await {
                let mut admin =
                    pb::adxlet_process_service_client::AdxletProcessServiceClient::new(channel);
                let _ = admin.begin(tonic::Request::new(begin.clone())).await;
            }
        })
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Reap confirmed by the actor: mint the next attempt/node_session exactly once
/// and record `exited`, then durably store it. A replay that already carries a
/// minted attempt is not re-minted; an un-mintable identity or a failed store
/// is an error, never a silent success or an `exited` stage with an empty
/// attempt.
fn record_reap(task: &mut super::Task, directory: &Path) -> Result<()> {
    let execution = task
        .adxlet_execution
        .as_mut()
        .ok_or("missing exit intent")?;
    if execution.attempt.is_empty() {
        let attempt = super::random_id()?;
        let node_session = super::random_id()?;
        execution.attempt = attempt;
        execution.node_session = node_session;
    }
    execution.stage = Stage::Exited;
    execution.error = None;
    super::store(directory, task)
}

/// Closes target A in the actor before minting B. Receipt ambiguity never
/// proves non-execution. StopFailed(None) can atomically seal a not-yet-run A
/// and return NeverCreated; a created Child must be actually reaped. The one
/// saved 600s budget covers stop/reap and the internal original Start B.
pub(super) async fn rollback(
    task: &mut super::Task,
    directory: &Path,
    sender: &tokio::sync::mpsc::Sender<Command>,
    stopped: &AtomicBool,
) -> Result<()> {
    let identity = task
        .submission
        .process
        .clone()
        .ok_or("missing parent selection")?;
    let execution = task
        .adxlet_execution
        .clone()
        .ok_or("missing target attempt")?;
    if execution.leg.source().is_some() || matches!(execution.stage, Stage::Committing { .. }) {
        // A failed rename/fsync may already have exposed B. Re-save the same
        // identity before resuming its existing Start/Receipt path.
        super::store(directory, task)?;
        return start(task, directory, sender, stopped).await;
    }
    let rollback_started = matches!(execution.stage, Stage::StoppingForRollback { .. });
    let deadline_unix_ms = if rollback_started {
        execution.deadline_unix_ms
    } else {
        adx_core::observation::now_millis()?
            .checked_add(600_000)
            .ok_or("deadline overflow")?
    };
    let remaining = deadline_unix_ms.saturating_sub(adx_core::observation::now_millis()?);
    let deadline = Instant::now()
        .checked_add(Duration::from_millis(remaining))
        .unwrap_or_else(Instant::now);
    let current = task.adxlet_execution.as_mut().ok_or("execution absent")?;
    current.deadline_unix_ms = deadline_unix_ms;
    current.stage = Stage::StoppingForRollback {
        candidate_pid: execution.stage.candidate_pid(),
    };
    task.phase = super::Phase::Unknown;
    // Freeze the recovery budget even when the first observing Receipt is unavailable.
    super::store(directory, task)?;
    let observing_deadline = Instant::now() + Duration::from_secs(2);
    let observed = command(
        sender,
        task,
        Operation::Receipt {
            owner: Owner::Adxlet(identity.clone()),
            old_pid: execution.old_pid,
            attempt: execution.attempt.clone(),
            pid: execution.stage.candidate_pid(),
        },
        observing_deadline,
    )
    .await;
    let candidate_pid = match observed {
        Ok(Reply::Committed) => return record_commit(task, directory),
        Ok(Reply::Candidate { pid, .. })
            if execution
                .stage
                .candidate_pid()
                .is_none_or(|saved| saved == pid) =>
        {
            Some(pid)
        }
        Ok(Reply::NeverCreated) if execution.stage.candidate_pid().is_none() => None,
        // None is only a request for the actor to prove and seal non-creation,
        // never a local inference from an unavailable Receipt.
        Err(_) if execution.stage.candidate_pid().is_none() => None,
        _ => return Err("candidate outcome unknown; maintenance retained".into()),
    };
    let current = task.adxlet_execution.as_mut().ok_or("execution absent")?;
    if current.stage.candidate_pid() != candidate_pid {
        current.stage = Stage::StoppingForRollback { candidate_pid };
        super::store(directory, task)?;
    }
    let reply = command(
        sender,
        task,
        Operation::StopFailed {
            owner: Owner::Adxlet(identity),
            old_pid: execution.old_pid,
            pid: candidate_pid,
            attempt: execution.attempt.clone(),
        },
        deadline,
    )
    .await?;
    let mut observed = reply;
    loop {
        if stopped.load(Ordering::Acquire) {
            return Err("supervisor stopping".into());
        }
        match observed {
            Reply::Committed => return record_commit(task, directory),
            Reply::NeverCreated if candidate_pid.is_none() => break,
            Reply::Candidate { pid, exited: true } if Some(pid) == candidate_pid => break,
            Reply::Candidate { pid, exited: false } if Some(pid) == candidate_pid => {}
            _ => return Err("candidate reap unknown; maintenance retained".into()),
        }
        if Instant::now() >= deadline {
            return Err("candidate reap deadline exceeded; maintenance retained".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        observed = command(
            sender,
            task,
            Operation::Receipt {
                owner: Owner::Adxlet(
                    task.submission
                        .process
                        .clone()
                        .ok_or("missing parent selection")?,
                ),
                old_pid: execution.old_pid,
                attempt: execution.attempt.clone(),
                pid: candidate_pid,
            },
            deadline,
        )
        .await?;
    }
    // Verify both the original executable copy and its actual pinned config
    // after the actor has proved A closed/reaped, before exposing identity B.
    original_process(task, directory, deadline, stopped)?;
    let attempt = super::random_id()?;
    let node_session = super::random_id()?;
    let current = task.adxlet_execution.as_mut().ok_or("execution absent")?;
    current.leg = ProcessLeg::Rollback {
        from: AttemptIdentity {
            attempt: execution.attempt,
            node_session: execution.node_session,
        },
    };
    current.attempt = attempt;
    current.node_session = node_session;
    current.stage = Stage::Exited;
    current.error = None;
    super::store(directory, task)?;
    start(task, directory, sender, stopped).await
}

fn original_process(
    task: &super::Task,
    directory: &Path,
    deadline: Instant,
    stopped: &AtomicBool,
) -> Result<crate::config::Process> {
    let original = load_original_process(directory).ok_or("original process snapshot missing")?;
    let target = load_target_process(directory)?;
    if original.role != crate::config::Role::Adxlet || original.id != target.id {
        return Err("original process ownership changed".into());
    }
    validate_pinned(directory, ORIGINAL_CONFIG, &original)?;
    let identity = task
        .submission
        .process
        .as_ref()
        .ok_or("missing parent selection")?;
    let mut executable = std::fs::File::open(directory.join("original"))?;
    super::artifact::verify_file(
        &mut executable,
        &identity.identity.original_sha256,
        deadline,
        stopped,
    )?;
    Ok(original)
}

/// Start the target or rollback process with its saved attempt/node_session
/// and fixed phase deadline (900s upgrade, 600s rollback). Persist the leg's
/// start intent before actor Start; a replay returns the same Child. Deliver
/// identity to the owned PID, verify readiness, then record commit-unknown
/// before Complete. Resolve a lost Complete through replay or Receipt without
/// stopping a possibly selected Child. This worker never contacts Coordinator.
pub(super) async fn start(
    task: &mut super::Task,
    directory: &Path,
    sender: &tokio::sync::mpsc::Sender<Command>,
    stopped: &AtomicBool,
) -> Result<()> {
    let identity = task
        .submission
        .process
        .clone()
        .ok_or("missing parent selection")?;
    let execution = task
        .adxlet_execution
        .clone()
        .ok_or("missing exit receipt")?;
    if !execution.startable() {
        return Err("start requires a reaped exit".into());
    }
    let original = execution.leg.source().is_some();
    let remaining = execution
        .deadline_unix_ms
        .saturating_sub(adx_core::observation::now_millis().unwrap_or(execution.deadline_unix_ms));
    let leg = Leg {
        sender,
        identity: &identity,
        old_pid: execution.old_pid,
        attempt: &execution.attempt,
        deadline: Instant::now()
            .checked_add(Duration::from_millis(remaining))
            .unwrap_or_else(Instant::now),
    };
    let pid = match execution.stage {
        Stage::Committing { candidate_pid } => candidate_pid,
        // Expiry permits observation only: no new spawn, no refreshed budget.
        stage if Instant::now() >= leg.deadline => {
            return commit(task, directory, &leg, stage.candidate_pid()).await;
        }
        _ => {
            let target = if original {
                original_process(task, directory, leg.deadline, stopped)?
            } else {
                load_target_process(directory)?
            };
            validate_pinned(
                directory,
                if original {
                    ORIGINAL_CONFIG
                } else {
                    TARGET_CONFIG
                },
                &target,
            )?;
            if !original && target.binary != directory.join("executable") {
                return Err("target process snapshot invalid".into());
            }
            let digest = if original {
                identity.identity.original_sha256.clone()
            } else {
                task.prepared_sha256()?.to_owned()
            };
            let binary = directory.join(if original { "original" } else { "executable" });
            let mut executable = std::fs::File::open(&binary)?;
            super::artifact::verify_file(&mut executable, &digest, leg.deadline, stopped)?;
            let admin_socket = target
                .admin_socket
                .clone()
                .ok_or("target admin socket missing")?;
            // Task reservation serializes starts; complete identities stay in the payload.
            let path = bootstrap_socket(&target)?;
            let listener = bind_bootstrap(&path).await?;
            let payload = serde_json::to_vec(&adx_protocol::process_bootstrap::ProcessBootstrap {
                owner: adx_protocol::control::MaintenanceOwner {
                    node_id: identity.node_id.clone(),
                    rollout_id: identity.identity.rollout_id.clone(),
                    local_task_id: task.submission.task_id.clone(),
                    epoch: identity.identity.epoch,
                },
                node_session: execution.node_session.clone(),
                deadline_unix_millis: execution.deadline_unix_ms,
            })?;
            let stage = task.adxlet_execution.as_mut().ok_or("execution absent")?;
            stage.stage = Stage::Starting {
                candidate_pid: execution.stage.candidate_pid(),
            };
            stage.error = None;
            super::store(directory, task)?;
            // A lost or failed Start reply is never answered by blindly
            // re-sending Start: the same attempt is first observed through a
            // Receipt. A recorded Child continues to bootstrap/ready with its
            // pid, NeverCreated is a fact, and only a still-unknown outcome
            // re-requests the idempotent Start for the same attempt.
            let mut observe_first = execution.stage != Stage::Exited;
            let pid = loop {
                if stopped.load(Ordering::Acquire) {
                    return Err("supervisor stopping".into());
                }
                if Instant::now() >= leg.deadline {
                    return Err("spawn outcome unknown; maintenance retained".into());
                }
                if observe_first {
                    match command(sender, task, receipt(&leg, None), leg.deadline).await {
                        Ok(Reply::Candidate { pid, .. }) => break pid,
                        Ok(Reply::NeverCreated) => {
                            let _ = std::fs::remove_file(&path);
                            return Err("target Child never created; maintenance retained".into());
                        }
                        _ => tokio::time::sleep(Duration::from_millis(100)).await,
                    }
                    if Instant::now() >= leg.deadline {
                        return Err("spawn outcome unknown; maintenance retained".into());
                    }
                }
                match command(
                    sender,
                    task,
                    Operation::Start {
                        config: Box::new(target.clone()),
                        pid: leg.old_pid,
                        binary: binary.clone(),
                        attempt: execution.attempt.clone(),
                        owner: Owner::Adxlet(identity.clone()),
                        launch: Launch::Adxlet {
                            executable: executable.try_clone()?,
                            bootstrap: path.clone(),
                        },
                    },
                    leg.deadline,
                )
                .await
                {
                    Ok(Reply::Candidate { pid, .. }) => break pid,
                    Ok(Reply::NeverCreated) => {
                        let _ = std::fs::remove_file(&path);
                        return Err("target Child never created; maintenance retained".into());
                    }
                    _ => observe_first = true,
                }
            };
            task.adxlet_execution
                .as_mut()
                .ok_or("execution absent")?
                .stage = Stage::Starting {
                candidate_pid: Some(pid),
            };
            super::store(directory, task)?;
            let evidence = Ready {
                target: &target,
                digest: &digest,
                admin_socket: &admin_socket,
                session: &execution.node_session,
                payload: &payload,
            };
            let ready = await_ready(task, &leg, stopped, evidence, listener, pid).await;
            let _ = std::fs::remove_file(&path);
            if !ready {
                return Err("candidate readiness unknown; maintenance retained".into());
            }
            let stage = task.adxlet_execution.as_mut().ok_or("execution absent")?;
            stage.stage = Stage::Committing { candidate_pid: pid };
            super::store(directory, task)?;
            pid
        }
    };
    commit(task, directory, &leg, Some(pid)).await
}

/// Fixed identity of one Start leg, shared by readiness and Complete.
struct Leg<'a> {
    sender: &'a tokio::sync::mpsc::Sender<Command>,
    identity: &'a super::ProcessIdentity,
    old_pid: u32,
    attempt: &'a str,
    deadline: Instant,
}

/// Evidence the owned Child must present before Complete.
struct Ready<'a> {
    target: &'a crate::config::Process,
    digest: &'a str,
    admin_socket: &'a Path,
    session: &'a str,
    payload: &'a [u8],
}

fn receipt(leg: &Leg<'_>, pid: Option<u32>) -> Operation {
    Operation::Receipt {
        owner: Owner::Adxlet(leg.identity.clone()),
        old_pid: leg.old_pid,
        attempt: leg.attempt.to_owned(),
        pid,
    }
}

fn bootstrap_socket(process: &crate::config::Process) -> Result<PathBuf> {
    let path = process
        .admin_socket
        .as_ref()
        .ok_or("adxlet admin socket missing")?
        .with_file_name(format!("{}-start.sock", process.id));
    super::check_socket_path(&path)?;
    Ok(path)
}

async fn bind_bootstrap(path: &Path) -> Result<tokio::net::UnixListener> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
    super::check_socket_path(path)?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_socket() {
                return Err("bootstrap socket path occupied".into());
            }
            match tokio::time::timeout(
                Duration::from_secs(2),
                tokio::net::UnixStream::connect(path),
            )
            .await
            {
                Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                    let current = std::fs::symlink_metadata(path)?;
                    if (metadata.dev(), metadata.ino()) != (current.dev(), current.ino()) {
                        return Err("bootstrap socket path changed".into());
                    }
                    std::fs::remove_file(path)?;
                }
                Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => (),
                _ => return Err("bootstrap socket occupied or ownership unknown".into()),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    let listener = tokio::net::UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Delivers the bootstrap only to the actual owned PID and waits until that
/// Child serves the target executable, the target effective configuration and
/// reports `ready` with the saved node_session. A failed delivery is not proof
/// of non-execution; only an observed exit or the fixed deadline ends the wait.
async fn await_ready(
    task: &super::Task,
    leg: &Leg<'_>,
    stopped: &AtomicBool,
    evidence: Ready<'_>,
    listener: tokio::net::UnixListener,
    pid: u32,
) -> bool {
    let bootstrap = async {
        use tokio::io::AsyncWriteExt;
        loop {
            let (mut socket, _) = listener.accept().await?;
            if socket
                .peer_cred()?
                .pid()
                .and_then(|value| u32::try_from(value).ok())
                != Some(pid)
            {
                continue;
            }
            socket.write_all(evidence.payload).await?;
            socket.shutdown().await?;
            return Ok::<(), std::io::Error>(());
        }
    };
    tokio::pin!(bootstrap);
    tokio::time::timeout_at(leg.deadline.into(), async {
        let mut bootstrap_pending = true;
        loop {
            tokio::select! {
                result = &mut bootstrap, if bootstrap_pending => {
                    bootstrap_pending = false;
                    if let Err(error) = result {
                        eprintln!("adxlet bootstrap delivery to PID {pid} failed: {error}");
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
            if stopped.load(Ordering::Acquire) {
                return false;
            }
            if matches!(
                command(leg.sender, task, receipt(leg, Some(pid)), leg.deadline).await,
                Ok(Reply::Candidate { exited: true, .. })
            ) {
                return false;
            }
            let Ok(channel) = super::relay::channel(evidence.admin_socket, pid).await else {
                continue;
            };
            let mut candidate =
                pb::adxlet_process_service_client::AdxletProcessServiceClient::new(channel);
            let Ok(response) = candidate
                .query(pb::AdxletProcessRequest {
                    rollout_id: leg.identity.identity.rollout_id.clone(),
                    local_task_id: task.submission.task_id.clone(),
                    epoch: leg.identity.identity.epoch,
                    ..Default::default()
                })
                .await
            else {
                continue;
            };
            let state = response.into_inner();
            if state.phase != "ready" || state.node_session != evidence.session {
                continue;
            }
            // Check the actual owned executable once readiness is reported,
            // within this same fixed deadline; pending polls do not hash it.
            let Ok(Reply::Runtime {
                pid: actual,
                config,
                mut executable,
                ..
            }) = super::actor::inspect_candidate(leg.sender, task, pid, leg.deadline).await
            else {
                continue;
            };
            if actual != pid
                || config.configuration_sha256 != evidence.target.configuration_sha256
                || super::artifact::verify_file(
                    &mut executable,
                    evidence.digest,
                    leg.deadline,
                    stopped,
                )
                .is_err()
            {
                continue;
            }
            return true;
        }
    })
    .await
    .unwrap_or(false)
}

/// Selects the ready target with Complete (idempotent in the actor) while the
/// saved deadline holds; after expiry only an observing Receipt may prove the
/// commit. Success records Completed/`Selected`; otherwise the stage stays
/// `Committing` and the Child is never stopped here.
async fn commit(
    task: &mut super::Task,
    directory: &Path,
    leg: &Leg<'_>,
    pid: Option<u32>,
) -> Result<()> {
    loop {
        let committed = match pid {
            Some(pid) if Instant::now() < leg.deadline => matches!(
                command(
                    leg.sender,
                    task,
                    Operation::Commit { pid, old_pid: leg.old_pid, attempt: leg.attempt.to_owned(), owner: Owner::Adxlet( leg.identity.clone()), stage: CommitStage::Select },
                    leg.deadline,
                )
                .await,
                Ok(Reply::Candidate { pid: selected, exited: false }) if selected == pid
            ),
            _ => matches!(
                command(
                    leg.sender,
                    task,
                    receipt(leg, pid),
                    Instant::now() + Duration::from_secs(2),
                )
                .await,
                Ok(Reply::Committed)
            ),
        };
        if committed {
            return record_commit(task, directory);
        }
        if Instant::now() >= leg.deadline {
            return Err("commit_unknown; selected candidate must not be stopped".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn record_commit(task: &mut super::Task, directory: &Path) -> Result<()> {
    let execution = task.adxlet_execution.as_mut().ok_or("execution absent")?;
    let candidate_pid = execution
        .stage
        .candidate_pid()
        .ok_or("committed candidate PID absent")?;
    task.phase = execution.leg.selected_phase();
    execution.stage = Stage::Selected { candidate_pid };
    execution.error = None;
    let stored = super::store(directory, task);
    if stored.is_err() {
        // The actor has already selected this Child. Only its Receipt may
        // close an ambiguous local write; never re-Start or stop it.
        task.phase = super::Phase::Unknown;
        if let Some(execution) = task.adxlet_execution.as_mut() {
            execution.stage = Stage::Committing { candidate_pid };
        }
    }
    stored
}

fn load_original_process(directory: &Path) -> Option<crate::config::Process> {
    serde_json::from_slice(&read_config(&directory.join(ORIGINAL_PROCESS), false).ok()?).ok()
}

fn load_original_admin_socket(directory: &Path) -> Option<PathBuf> {
    load_original_process(directory)?.admin_socket
}

fn write_snapshot(directory: &Path, name: &str, process: &crate::config::Process) -> Result<()> {
    write_private(directory, name, &serde_json::to_vec(process)?)
}

fn load_target_process(directory: &Path) -> Result<crate::config::Process> {
    Ok(serde_json::from_slice(&read_config(
        &directory.join(TARGET_PROCESS),
        false,
    )?)?)
}

fn write_private(directory: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    if bytes.len() > MAX_SNAPSHOT_BYTES {
        return Err("process snapshot exceeds fixed limit".into());
    }
    let output = directory.join(name);
    let temporary = output.with_extension("partial");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary, &output)?;
    std::fs::File::open(directory)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_execution_json_keeps_leg_and_requires_commit_pid() {
        let mut execution = Execution {
            old_pid: 42,
            stage: Stage::Committing { candidate_pid: 77 },
            error: None,
            attempt: "B".into(),
            node_session: "session-B".into(),
            deadline_unix_ms: 600,
            leg: ProcessLeg::Rollback {
                from: AttemptIdentity {
                    attempt: "A".into(),
                    node_session: "session-A".into(),
                },
            },
        };
        let mut value = serde_json::to_value(&execution).unwrap();
        assert_eq!(value["stage"], "committing");
        assert_eq!(value["candidate_pid"], 77);
        assert_eq!(
            value["leg"],
            serde_json::json!({"kind":"rollback","from":{"attempt":"A","node_session":"session-A"}})
        );
        value.as_object_mut().unwrap().remove("candidate_pid");
        assert!(serde_json::from_value::<Execution>(value.clone()).is_err());
        value["stage"] = "selected".into();
        assert!(serde_json::from_value::<Execution>(value).is_err());
        execution.stage = Stage::Exited;
        let value = serde_json::to_value(&execution).unwrap();
        assert!(value.get("candidate_pid").is_none());
        assert_eq!(
            serde_json::from_value::<Execution>(value).unwrap().stage,
            Stage::Exited
        );
        assert_eq!(
            serde_json::to_value(ProcessLeg::Target).unwrap(),
            serde_json::json!({"kind":"target"})
        );
    }

    #[tokio::test]
    async fn bootstrap_bind_preserves_occupied_files_and_listeners() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("adxlet-admin.sock");
        std::fs::write(&path, b"not a socket").unwrap();
        assert!(bind_bootstrap(&path).await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"not a socket");
        std::fs::remove_file(&path).unwrap();
        let admin = tokio::net::UnixListener::bind(&path).unwrap();
        assert!(bind_bootstrap(&path).await.is_err());
        let _pair =
            tokio::try_join!(tokio::net::UnixStream::connect(&path), admin.accept()).unwrap();
        drop(admin);
        let stale = bind_bootstrap(&path).await.unwrap();
        drop(stale);
        std::fs::remove_file(&path).unwrap();
        let target = root.path().join("keep");
        std::fs::write(&target, b"keep").unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(bind_bootstrap(&path).await.is_err());
        assert!(std::fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read(&target).unwrap(), b"keep");
    }

    #[test]
    fn bootstrap_path_is_fixed_and_checked_before_start() {
        let root = tempfile::tempdir().unwrap();
        let mut target = process(root.path());
        target.admin_socket = Some(root.path().join("adxlet-admin.sock"));
        let expected = root.path().join("adxlet-start.sock");
        assert_eq!(bootstrap_socket(&target).unwrap(), expected);
        target.binary = root.path().join("another-version");
        assert_eq!(bootstrap_socket(&target).unwrap(), expected);
        target.admin_socket = Some(PathBuf::from(format!("/{}/admin.sock", "x".repeat(100))));
        assert!(bootstrap_socket(&target).is_err());
    }

    fn process(directory: &Path) -> crate::config::Process {
        crate::config::Process {
            sources: std::collections::BTreeMap::new(),
            configuration_sha256: "e".repeat(64),
            id: "adxlet".into(),
            role: crate::config::Role::Adxlet,
            binary: directory.join("executable"),
            args: vec!["--config".into(), "/etc/adxlet.json".into()],
            env: [("ADX_NODE".to_owned(), "node".to_owned())].into(),
            admin_socket: Some("/run/adxlet/admin.sock".into()),
            health_url: None,
        }
    }

    #[test]
    fn process_snapshot_is_private_exact_and_not_left_partial() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let target = process(root.path());
        write_snapshot(root.path(), TARGET_PROCESS, &target).unwrap();
        let path = root.path().join(TARGET_PROCESS);
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(!path.with_extension("partial").exists());
        let loaded: crate::config::Process =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(&loaded).unwrap(),
            serde_json::to_value(&target).unwrap()
        );
        // A leftover partial from an interrupted write is never followed or reused.
        std::fs::write(root.path().join("original-process.partial"), b"x").unwrap();
        assert!(write_snapshot(root.path(), ORIGINAL_PROCESS, &target).is_err());
        assert!(!root.path().join(ORIGINAL_PROCESS).exists());
    }
}
