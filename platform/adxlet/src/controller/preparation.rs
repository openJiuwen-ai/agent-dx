//! One source preparation in the existing per-Environment owner queue.
use super::*;
use adx_core::snapshot_preparation::{
    PreparationLog, PreparationPlan, PreparationStep, SourcePreparationResult,
    SourcePreparationStatus as Phase,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::Mutex,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct Slot {
    result: Mutex<Option<SourcePreparationResult>>,
    pub(super) cancel: CancellationToken,
}
impl Slot {
    pub(super) fn busy(&self) -> bool {
        self.result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|v| v.status == Phase::Running)
    }
    fn log(&self, step: Option<u32>, message: String) {
        let mut guard = self.result.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(value) = guard.as_mut() {
            if let Some(step) = step {
                value.step_index = step;
            }
            // Retain a contiguous prefix, including one explicit truncation entry.
            if value.logs_truncated {
                return;
            }
            let mut message: String = message
                .chars()
                .filter(|c| !c.is_control() || *c == '\n' || *c == '\t')
                .collect();
            if message.len() > 1024 {
                let mut end = 1024;
                while !message.is_char_boundary(end) {
                    end -= 1;
                }
                message.truncate(end);
            }
            let retained_bytes: usize = value
                .logs
                .iter()
                .map(|entry| serde_json::to_vec(entry).map_or(0, |v| v.len()))
                .sum();
            if value.logs.len() == 255 || retained_bytes + message.len() * 2 + 256 >= 128 * 1024 {
                message = "Further preparation logs truncated".into();
                value.logs_truncated = true;
            }
            value.logs.push(PreparationLog {
                offset: value.next_offset,
                timestamp_unix_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
                level: "info".into(),
                message,
            });
            value.next_offset += 1;
        }
    }
    fn finish(&self, phase: Phase, error: String) {
        if let Some(value) = self
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_mut()
        {
            value.status = phase;
            value.error = error;
        }
    }
}

pub(crate) struct Request {
    pub operation_id: String,
    pub runtime_id: String,
    pub expected_revision: u64,
    pub plan: PreparationPlan,
}
impl EnvironmentHandle {
    pub(crate) async fn prepare_source(&self, request: Request) -> Result<SourcePreparationResult> {
        let digest = request.plan.fingerprint()?;
        crate::checkpoint::validate_operation(&request.operation_id, request.expected_revision)?;
        {
            if self.view().0.runtime.id != request.runtime_id {
                return Err(Error::Conflict);
            }
            let mut guard = self
                .preparation
                .result
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(existing) = guard.as_ref() {
                if existing.operation_id != request.operation_id || existing.plan_sha256 != digest {
                    return Err(Error::Conflict);
                }
                return Ok(existing.clone());
            }
            let record = self.view().0;
            if record.state != EnvironmentState::Running
                || record.runtime.id != request.runtime_id
                || record.revision != request.expected_revision
            {
                return Err(Error::Conflict);
            }
            *guard = Some(SourcePreparationResult {
                operation_id: request.operation_id.clone(),
                plan_sha256: digest,
                status: Phase::Running,
                step_index: 0,
                error: String::new(),
                logs: vec![],
                next_offset: 0,
                logs_truncated: false,
            });
        }
        let (unused, _) = oneshot::channel();
        if self
            .enqueue(Command::Prepare(Box::new(request)), unused)
            .await
            .is_err()
        {
            self.preparation
                .finish(Phase::Unknown, "source controller stopped".into());
        }
        self.source_preparation()
    }
    pub(crate) fn source_preparation(&self) -> Result<SourcePreparationResult> {
        self.preparation
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or(Error::NotFound)
    }
}
impl Controller {
    pub(super) async fn prepare_source(&mut self, request: Request) {
        let result=async {
            if self.retired || self.authority_fenced || self.record.state != EnvironmentState::Running || self.record.runtime.id != request.runtime_id || self.preparation.cancel.is_cancelled() { return Err(Error::Conflict); }
            if self.replay_operation(&request.operation_id,request.expected_revision,LifecycleKind::Prepare)? {return Err(Error::Conflict);}
            let control=self.services.runtime_control.clone().ok_or_else(||Error::Invalid("preparation requires Execd".into()))?;
            let mut executor=Executor { control,record:self.record.clone(),files:self.services.preparation_files.clone(),slot:self.preparation.clone(),user:"root".into(),cwd:"/".into(),env:BTreeMap::new(),active:None,start:None,sequence:0,operation_id:request.operation_id.clone() };
            let result=tokio::select! {
                _=self.preparation.cancel.cancelled()=>Err(Error::Conflict),
                result=tokio::time::timeout(Duration::from_secs(request.plan.timeout_seconds),executor.run(&request.plan))=>result.unwrap_or_else(|_|Err(Error::DeadlineExceeded("source preparation timed out".into())))
            };
            if result.is_err() { executor.stop().await; }
            result?;
            self.record.revision=self.record.revision.checked_add(1).ok_or_else(||Error::Invalid("revision exhausted".into()))?;
            self.record.last_operation=Some(adx_core::CompletedOperation {id:request.operation_id,expected_revision:request.expected_revision,kind:LifecycleKind::Prepare});
            self.sync().await?;
            if self.durability != Some(Durability::Published) { return Err(Error::Unavailable("preparation completion not published".into())); }
            Ok(())
        }.await;
        match result {
            Ok(()) => self.preparation.finish(Phase::Succeeded, String::new()),
            Err(error) => {
                self.preparation
                    .log(None, "Source preparation failed".into());
                let phase = if matches!(error, Error::Unavailable(_) | Error::ResponseTimeout(_)) {
                    Phase::Unknown
                } else {
                    Phase::Failed
                };
                self.preparation.finish(phase, error.to_string());
            }
        }
    }
}

struct Executor {
    control: crate::runtime_control::RuntimeControlClient,
    record: EnvironmentRecord,
    files: Option<adx_preparation_files::FileStore>,
    slot: Arc<Slot>,
    user: String,
    cwd: String,
    env: BTreeMap<String, String>,
    active: Option<String>,
    start: Option<String>,
    sequence: u32,
    operation_id: String,
}
fn invalid(message: &str) -> Error {
    Error::Invalid(format!("source preparation: {message}"))
}
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
fn absolute(base: &str, path: &str) -> Result<String> {
    let joined = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        Path::new(base).join(path)
    };
    let mut out = PathBuf::from("/");
    for part in joined.components() {
        match part {
            Component::Normal(p) => out.push(p),
            Component::ParentDir => {
                out.pop();
            }
            Component::RootDir | Component::CurDir => {}
            _ => return Err(invalid("invalid working directory")),
        }
    }
    out.to_str()
        .map(str::to_owned)
        .ok_or_else(|| invalid("non-UTF8 working directory"))
}

fn workdir_command(path: &str, user: &str) -> String {
    let owner = if user.contains(':') {
        user.to_owned()
    } else {
        format!("{user}:")
    };
    format!(
        r#"set -e
target={}
owner={}
if [ ! -d "$target" ]; then
    first_new=$target
    check=$(dirname -- "$target")
    while [ ! -d "$check" ]; do
        first_new=$check
        check=$(dirname -- "$check")
    done
    mkdir -p -- "$target"
    chown -R -- "$owner" "$first_new"
fi
cd -- "$target"
pwd -P"#,
        quote(path),
        quote(&owner)
    )
}
impl Executor {
    async fn invoke(&self, action: &str, args: Value) -> Result<Value> {
        self.control
            .preparation_invoke(&self.record, action, args)
            .await
    }
    async fn stop(&self) {
        for id in [&self.active, &self.start].into_iter().flatten() {
            let _ = self.invoke("cmd_kill", json!({"command_id":id})).await;
        }
    }
    async fn command(
        &mut self,
        command: String,
        user: String,
        background: bool,
    ) -> Result<Option<String>> {
        let id = format!("{}-{}", self.operation_id, self.sequence);
        self.sequence += 1;
        self.active = Some(id.clone());
        let args = json!({"command_id":id,"command":command,"cwd":self.cwd,"user":user,"envs":self.env,"timeout":0});
        // Start may have committed before a transport failure. Query, never submit a new ID.
        let submitted = self.invoke("cmd_start", args).await;
        if submitted.is_err() {
            self.invoke("cmd_get", json!({"command_id":id})).await?;
        }
        loop {
            let value = self.invoke("cmd_get", json!({"command_id":id})).await?;
            match value["status"].as_str() {
                Some("PENDING") => {}
                Some("RUNNING") if background => {
                    self.start = Some(id);
                    self.active = None;
                    return Ok(Some(String::new()));
                }
                Some("RUNNING") => {}
                Some("SUCCEEDED") => {
                    if let Some(output) = value["stdout"].as_str().filter(|s| !s.is_empty()) {
                        self.slot.log(None, output.to_owned());
                    }
                    self.active = None;
                    if background {
                        self.start = Some(id);
                    }
                    return Ok(Some(
                        value["stdout"].as_str().unwrap_or_default().to_owned(),
                    ));
                }
                Some("FAILED" | "KILLED" | "TIMED_OUT") => {
                    if let Some(output) = value["stderr"].as_str().filter(|s| !s.is_empty()) {
                        self.slot.log(None, output.to_owned());
                    }
                    self.active = None;
                    return Ok(None);
                }
                _ => return Err(Error::Unavailable("command result is unknown".into())),
            }
            self.check_start().await?;
            if self.slot.cancel.is_cancelled() {
                return Err(Error::Conflict);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn required_command(
        &mut self,
        command: String,
        user: String,
        background: bool,
    ) -> Result<()> {
        if self.command(command, user, background).await?.is_some() {
            Ok(())
        } else {
            Err(invalid("preparation command exited unsuccessfully"))
        }
    }
    async fn check_start(&self) -> Result<()> {
        if let Some(id) = &self.start {
            let value = self.invoke("cmd_get", json!({"command_id":id})).await?;
            if !matches!(value["status"].as_str(), Some("RUNNING" | "SUCCEEDED")) {
                return Err(invalid("start command failed before readiness"));
            }
        }
        Ok(())
    }
    async fn wait_ready(&mut self, command: &str) -> Result<()> {
        loop {
            if self
                .command(command.to_owned(), self.user.clone(), false)
                .await?
                .is_some()
            {
                return Ok(());
            }
            self.check_start().await?;
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn run(&mut self, plan: &PreparationPlan) -> Result<()> {
        let capabilities = self.invoke("cmd_capabilities", json!({})).await?;
        if !capabilities["capabilities"]
            .as_array()
            .is_some_and(|v| v.iter().any(|v| v == "per-command-user"))
        {
            return Err(invalid("Execd does not support per-command users"));
        }
        for (index, step) in plan.steps.iter().enumerate() {
            self.slot.log(
                Some(u32::try_from(index).map_err(|_| invalid("too many steps"))?),
                format!("Executing preparation step {}", index + 1),
            );
            match step {
                PreparationStep::Run { command, user } => {
                    self.required_command(
                        command.clone(),
                        user.clone().unwrap_or_else(|| self.user.clone()),
                        false,
                    )
                    .await?
                }
                PreparationStep::Environment { values } => self.env.extend(values.clone()),
                PreparationStep::User { user } => self.user = user.clone(),
                PreparationStep::Workdir { path } => {
                    let path = absolute(&self.cwd, path)?;
                    // The previous cwd may have been deleted by RUN. Only newly
                    // created directories acquire the current user's ownership.
                    self.cwd = "/".into();
                    let output = self
                        .command(workdir_command(&path, &self.user), "root".into(), false)
                        .await?
                        .ok_or_else(|| invalid("working directory preparation failed"))?;
                    let resolved = output.strip_suffix('\n').unwrap_or(&output);
                    if !resolved.starts_with('/') || resolved.contains(['\n', '\0']) {
                        return Err(invalid("working directory resolution failed"));
                    }
                    self.cwd = resolved.to_owned();
                }
                PreparationStep::Copy {
                    file,
                    source,
                    destination,
                    user,
                    mode,
                } => {
                    let store = self
                        .files
                        .clone()
                        .ok_or_else(|| invalid("file storage is not configured on owner node"))?;
                    let temp = tempfile::tempdir()
                        .map_err(|_| invalid("cannot create file staging directory"))?;
                    let archive = temp.path().join("context.tar");
                    store
                        .download(&self.record.spec.tenant_id, file, &archive)
                        .await?;
                    let validation = archive.clone();
                    tokio::task::spawn_blocking(move || validate_archive(&validation))
                        .await
                        .map_err(|_| invalid("archive validation stopped"))??;
                    let staging = format!("/tmp/adx-prepare-{}", uuid::Uuid::new_v4());
                    self.control
                        .preparation_upload(&self.record, &format!("{staging}.tar"), &archive)
                        .await?;
                    let trailing = destination.ends_with('/');
                    let destination = absolute(&self.cwd, destination)?;
                    let script = copy_command(
                        &staging,
                        source,
                        &destination,
                        user.as_deref().unwrap_or(&self.user),
                        *mode,
                        trailing,
                    );
                    self.required_command(script, "root".into(), false).await?;
                }
            }
        }
        if let Some(command) = &plan.start_command {
            self.required_command(command.clone(), self.user.clone(), true)
                .await?;
        }
        let ready = plan
            .ready_command
            .clone()
            .or_else(|| plan.start_command.as_ref().map(|_| "sleep 20".into()));
        if let Some(command) = ready {
            tokio::time::timeout(Duration::from_secs(600), self.wait_ready(&command))
                .await
                .map_err(|_| Error::DeadlineExceeded("readiness command timed out".into()))??;
        }
        self.check_start().await?;
        self.slot.log(
            Some(u32::try_from(plan.steps.len()).map_err(|_| invalid("too many steps"))?),
            "Source preparation complete".into(),
        );
        Ok(())
    }
}

/// Inspect without unpacking untrusted entries onto the node filesystem.
fn validate_archive(path: &Path) -> Result<()> {
    use std::io::Read;
    let file = std::fs::File::open(path).map_err(|_| invalid("cannot read archive"))?;
    let mut magic = [0; 2];
    let mut probe = &file;
    probe
        .read_exact(&mut magic)
        .map_err(|_| invalid("invalid archive"))?;
    let file = std::fs::File::open(path).map_err(|_| invalid("cannot read archive"))?;
    let reader: Box<dyn Read> = if magic == [0x1f, 0x8b] {
        Box::new(flate2::read::GzDecoder::new(file))
    } else {
        Box::new(file)
    };
    let mut archive = tar::Archive::new(reader.take(2 * 1024 * 1024 * 1024));
    let mut total = 0u64;
    for (count, entry) in archive
        .entries()
        .map_err(|_| invalid("invalid archive"))?
        .enumerate()
    {
        if count >= 100_000 {
            return Err(invalid("too many archive entries"));
        }
        let entry = entry.map_err(|_| invalid("invalid archive entry"))?;
        let path = entry.path().map_err(|_| invalid("invalid archive path"))?;
        if path.is_absolute()
            || path
                .components()
                .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
        {
            return Err(invalid("archive path escapes context"));
        }
        // SDK resolves ordinary source links. Explicit link/device entries are rejected.
        if !entry.header().entry_type().is_file() && !entry.header().entry_type().is_dir() {
            return Err(invalid("archive links and special files are not supported"));
        }
        total = total
            .checked_add(entry.size())
            .ok_or_else(|| invalid("archive too large"))?;
        if total > 2 * 1024 * 1024 * 1024 {
            return Err(invalid("expanded archive exceeds limit"));
        }
    }
    if archive.into_inner().limit() == 0 {
        return Err(invalid("expanded archive exceeds limit"));
    }
    Ok(())
}

fn copy_command(
    staging: &str,
    source: &str,
    target: &str,
    owner: &str,
    mode: Option<u32>,
    trailing: bool,
) -> String {
    let script = include_str!("preparation_copy.sh");
    format!(
        "/bin/bash -c {} prepare-copy {} {} {} {} {} {}",
        quote(script),
        quote(staging),
        quote(source),
        quote(target),
        quote(owner),
        quote(&mode.map(|v| format!("{v:o}")).unwrap_or_default()),
        if trailing { "yes" } else { "no" }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn workdir_owns_only_new_directories_and_resolves_existing_links() {
        use std::os::unix::fs::MetadataExt;
        let root = tempfile::tempdir().unwrap();
        let metadata = root.path().metadata().unwrap();
        let owner = format!("{}:{}", metadata.uid(), metadata.gid());
        let target = root.path().join("new/child");
        let run = |path: &Path, owner: &str| {
            let output = std::process::Command::new("bash")
                .args(["-c", &workdir_command(path.to_str().unwrap(), owner)])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        };
        run(&target, &owner);
        for path in [root.path().join("new"), target.clone()] {
            let actual = path.metadata().unwrap();
            assert_eq!(
                (actual.uid(), actual.gid()),
                (metadata.uid(), metadata.gid())
            );
        }
        // No ownership lookup or modification is needed for an existing directory.
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert_eq!(
            run(&link, "no-such-preparation-user"),
            format!("{}\n", target.canonicalize().unwrap().display())
        );
    }

    #[tokio::test]
    async fn duplicate_prepare_attaches_and_busy_source_rejects_pause_without_blocking_monitors() {
        let record:EnvironmentRecord=serde_json::from_value(json!({
            "spec":{"id":"source","tenant_id":"tenant","image":"image","runtime_class":"runsc","resources":{"cpu_millis":1000,"memory_bytes":1048576,"disk_bytes":0},"priority":0},
            "assignment":{"environment_id":"source","node_id":"node","shard_id":0,"generation":1},
            "state":"Running","revision":1,"runtime":{"id":"source-1","ip":"127.0.0.1"},"resources_held":true
        })).unwrap();
        let (tx, mut rx) = mpsc::channel(4);
        let (delete_tx, _delete_rx) = mpsc::channel(1);
        let (_view, current) = tokio::sync::watch::channel((record, Some(Durability::Published)));
        let handle = EnvironmentHandle {
            tx,
            delete_tx,
            submissions: Arc::new(Submissions::new()),
            demand: Arc::default(),
            current,
            cleanup_pending: Arc::default(),
            metrics_registration: crate::metrics::Metrics::default().register("source"),
            preparation: Arc::default(),
        };
        let plan = PreparationPlan {
            steps: vec![PreparationStep::Run {
                command: "echo once".into(),
                user: None,
            }],
            start_command: None,
            ready_command: None,
            timeout_seconds: 60,
        };
        let request = || Request {
            operation_id: "prepare-test".into(),
            runtime_id: "source-1".into(),
            expected_revision: 1,
            plan: plan.clone(),
        };
        let first = handle.prepare_source(request()).await.unwrap();
        assert_eq!(first, handle.prepare_source(request()).await.unwrap());
        assert!(matches!(
            rx.try_recv().unwrap().command,
            Command::Prepare(_)
        ));
        assert!(rx.try_recv().is_err());
        assert!(handle
            .pause(crate::checkpoint::PauseRequest {
                operation_id: "pause".into(),
                expected_revision: 1,
                retention: Default::default(),
                timeout_seconds: 60
            })
            .await
            .is_err());
        let resume = crate::checkpoint::ResumeRequest {
            operation_id: "resume".into(),
            expected_revision: 1,
            run_timeout_seconds: None,
        };
        for result in [
            tokio::time::timeout(Duration::from_millis(100), handle.resume(resume.clone())).await,
            tokio::time::timeout(
                Duration::from_millis(100),
                handle.ensure_running("ensure".into()),
            )
            .await,
            tokio::time::timeout(Duration::from_millis(100), handle.recover(resume)).await,
        ] {
            assert!(matches!(result, Ok(Err(Error::Conflict))));
        }
        assert!(handle.tick().await.unwrap().is_none());
        assert!(matches!(
            handle
                .check_authority(AuthorityCheck {
                    record: Some(handle.view().0),
                    references: None
                })
                .await
                .unwrap(),
            AuthorityResult::Current
        ));
        assert!(handle.run_control().await.is_err());
        handle.preparation.finish(Phase::Succeeded, String::new());
        assert_eq!(
            handle.prepare_source(request()).await.unwrap().status,
            Phase::Succeeded
        );
        assert!(rx.try_recv().is_err());
        let mut changed = request();
        changed.plan.steps.clear();
        assert!(handle.prepare_source(changed).await.is_err());
    }

    #[test]
    fn archive_validation_accepts_files_and_rejects_links() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("context.tar");
        {
            let mut tar = tar::Builder::new(std::fs::File::create(&path).unwrap());
            let mut header = tar::Header::new_gnu();
            header.set_size(2);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append_data(&mut header, "a.txt", &b"ok"[..]).unwrap();
            tar.finish().unwrap();
        }
        validate_archive(&path).unwrap();
        {
            let mut tar = tar::Builder::new(std::fs::File::create(&path).unwrap());
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_size(0);
            header.set_mode(0o777);
            header.set_cksum();
            tar.append_link(&mut header, "escape", "/etc/passwd")
                .unwrap();
            tar.finish().unwrap();
        }
        assert!(validate_archive(&path).is_err());
    }

    #[test]
    fn paths_and_shell_arguments_remain_literal() {
        assert_eq!(absolute("/app", "../out").unwrap(), "/out");
        assert_eq!(quote("a'b"), "'a'\\''b'");
    }
    #[test]
    fn logs_retain_contiguous_offsets_when_truncated() {
        let slot = Slot::default();
        *slot.result.lock().unwrap() = Some(SourcePreparationResult {
            operation_id: "test".into(),
            plan_sha256: "a".repeat(64),
            status: Phase::Running,
            step_index: 0,
            error: String::new(),
            logs: vec![],
            next_offset: 0,
            logs_truncated: false,
        });
        for _ in 0..300 {
            slot.log(Some(1), "entry".into());
        }
        slot.log(None, "stdout does not reset the failing step".into());
        let guard = slot.result.lock().unwrap();
        let value = guard.as_ref().unwrap();
        assert_eq!(value.step_index, 1);
        assert_eq!(value.logs.len(), 256);
        assert_eq!(value.next_offset, 256);
        assert!(value.logs_truncated);
        assert!(value
            .logs
            .iter()
            .enumerate()
            .all(|(i, v)| v.offset == i as u64));
    }
}
