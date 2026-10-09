//! Explicit administrator-only experiment for one managed Owner workspace container.
//! The dedicated mount thread owns runc and final-view cleanup. This is not the
//! production Agent READY/revocation/restart-reconciliation protocol.

use crate::{
    config::NativeWorkspaceConfig,
    node::vfs::ownerfs::{
        HomeExportAuthority, OwnerFs,
        bind_mount::{
            DirectoryIdentity, WorkspaceBindMount, detach_secondary_clone, inspect_secondary_clone,
        },
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    ffi::{CString, OsStr},
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    os::unix::{
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

type WorkerFailure = Option<(Option<i32>, String)>;

const MAX_REQUEST: u64 = 8192;
const MAX_OPERATIONS: usize = 64;
const MAX_COMMAND_ARGS: usize = 32;
const MAX_ARG_BYTES: usize = 4096;
const COMMAND_SUFFIXES: [&str; 4] = [".stdout", ".stderr", ".command.json", ".exit.json"];

pub(super) struct NativeWorkspace {
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<io::Result<()>>>,
    finished: tokio::sync::watch::Receiver<WorkerFailure>,
}

impl NativeWorkspace {
    pub(super) fn start(
        owner: Arc<OwnerFs>,
        mount: PathBuf,
        cfg: NativeWorkspaceConfig,
    ) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (finished_tx, finished) = tokio::sync::watch::channel(None::<(Option<i32>, String)>);
        let worker = thread::Builder::new()
            .name("afs-native-workspace".into())
            .spawn(move || {
                let driver = match Driver::new(owner, mount, cfg, worker_stop) {
                    Ok(driver) => {
                        let _ = ready_tx.send(Ok(()));
                        driver
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(io::Error::other(error.to_string())));
                        return Err(error);
                    }
                };
                driver.run(finished_tx)
            })?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                stop,
                worker: Some(worker),
                finished,
            }),
            Ok(Err(error)) => {
                stop.store(true, Ordering::Release);
                let _ = worker.join();
                Err(error)
            }
            Err(error) => {
                stop.store(true, Ordering::Release);
                let _ = worker.join();
                Err(io::Error::other(error))
            }
        }
    }

    // The monitor owns no JoinHandle or mount resources. Node retains the
    // worker until its explicit closure gate before FUSE teardown.
    pub(super) fn monitor(
        &self,
        stop: tokio::sync::watch::Receiver<bool>,
    ) -> impl std::future::Future<Output = io::Result<()>> + Send + 'static {
        let worker_stop = self.stop.clone();
        let mut finished = self.finished.clone();
        async move {
            tokio::select! {
                biased;
                _ = crate::runtime::cancelled(stop) => {
                    worker_stop.store(true, Ordering::Release);
                    Ok(())
                }
                _ = finished.changed() => {
                    Err(match finished.borrow().as_ref() {
                        Some((Some(errno), _)) => io::Error::from_raw_os_error(*errno),
                        Some((None, message)) => io::Error::other(message.clone()),
                        None => io::Error::other("native workspace worker exited unexpectedly"),
                    })
                }
            }
        }
    }

    pub(super) fn shutdown(mut self) -> io::Result<()> {
        self.stop.store(true, Ordering::Release);
        self.worker
            .take()
            .expect("owned worker")
            .join()
            .map_err(|_| io::Error::other("native workspace thread panicked"))?
    }
}

impl Drop for NativeWorkspace {
    fn drop(&mut self) {
        // An aborted supervisor still cancels its owned thread. Only the
        // explicit shutdown join can report cleanup success.
        self.stop.store(true, Ordering::Release);
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
enum Request {
    Start { id: String, workspace: String },
    Exec { id: String, argv: Vec<String> },
    Stop { id: String },
    Status { id: String },
}
impl Request {
    fn id(&self) -> &str {
        match self {
            Self::Start { id, .. }
            | Self::Exec { id, .. }
            | Self::Stop { id }
            | Self::Status { id } => id,
        }
    }
}

struct Active {
    permit: HomeExportAuthority,
    export: WorkspaceBindMount,
    container: String,
    // Holding the final namespace/root allows normal clone unmount after all
    // managed processes have stopped. These are dropped only after that proof.
    final_namespace: Option<File>,
    final_root: Option<File>,
    final_unique: Option<u64>,
    runtime_rootfs: Option<PathBuf>,
    pid: Option<ProcessIdentity>,
    verified: bool,
    container_attempted: bool,
    final_clone_detached: bool,
    container_deleted: bool,
}
struct ProcessIdentity {
    pid: u32,
    start: String,
}
struct Driver {
    owner: Arc<OwnerFs>,
    mount: PathBuf,
    cfg: NativeWorkspaceConfig,
    stop: Arc<AtomicBool>,
    listener: UnixListener,
    lock: File,
    active: Option<Active>,
    responses: HashMap<String, (Value, Value)>,
    sequence: u64,
}

impl Driver {
    fn new(
        owner: Arc<OwnerFs>,
        mount: PathBuf,
        cfg: NativeWorkspaceConfig,
        stop: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        require_root()?;
        trusted_path(&cfg.control_dir, true)?;
        if fs::metadata(&cfg.control_dir)?.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::other(
                "native control directory must be administrator-owned mode0700",
            ));
        }
        trusted_path(&cfg.runtime, false)?;
        trusted_tree(&cfg.rootfs)?;
        check_adapter_command(&cfg.idle_command)?;
        check_adapter_command(&cfg.identity_command)?;
        let idle_program = rootfs_command_path(&cfg.rootfs, &cfg.idle_command[0])?;
        let identity_program = rootfs_command_path(&cfg.rootfs, &cfg.identity_command[0])?;
        trusted_path(&idle_program, false)?;
        trusted_path(&identity_program, false)?;
        if !cfg.rootfs.join("workspace").is_dir() || !cfg.rootfs.join("proc").is_dir() {
            return Err(io::Error::other(
                "native rootfs needs empty workspace and proc directories",
            ));
        }
        for directory in [cfg.rootfs.join("workspace"), cfg.rootfs.join("proc")] {
            if fs::read_dir(directory)?.next().is_some() {
                return Err(io::Error::other(
                    "native rootfs mount targets must be empty",
                ));
            }
        }
        for executable in [&cfg.runtime, &idle_program, &identity_program] {
            if fs::metadata(executable)?.permissions().mode() & 0o111 == 0 {
                return Err(io::Error::other(
                    "native runtime and configured commands must be executable",
                ));
            }
        }
        let state = cfg.control_dir.join("runtime-state");
        if state.exists() {
            trusted_path(&state, true)?;
            if fs::read_dir(&state)?.next().is_some() {
                return Err(io::Error::other(
                    "native runtime state is nonempty; reconciliation is required",
                ));
            }
        } else {
            fs::create_dir(&state)?;
            fs::set_permissions(&state, fs::Permissions::from_mode(0o700))?;
        }
        isolate_mount_namespace()?;
        let lock_path = cfg.control_dir.join("controller.lock");
        let socket_path = cfg.control_dir.join("control.sock");
        let lock = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&lock_path)?;
        let listener = match UnixListener::bind(&socket_path) {
            Ok(listener) => listener,
            Err(error) => {
                let _ = fs::remove_file(&lock_path);
                return Err(error);
            }
        };
        if let Err(error) = listener
            .set_nonblocking(true)
            .and_then(|_| fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)))
        {
            let _ = fs::remove_file(&socket_path);
            let _ = fs::remove_file(&lock_path);
            return Err(error);
        }
        let sequence = match next_command_sequence(&cfg.control_dir) {
            Ok(sequence) => sequence,
            Err(error) => {
                let _ = fs::remove_file(&socket_path);
                let _ = fs::remove_file(&lock_path);
                return Err(error);
            }
        };
        Ok(Self {
            owner,
            mount,
            cfg,
            stop,
            listener,
            lock,
            active: None,
            responses: HashMap::new(),
            sequence,
        })
    }

    fn run(mut self, finished: tokio::sync::watch::Sender<WorkerFailure>) -> io::Result<()> {
        let result = self.serve(&finished);
        let failure = result
            .as_ref()
            .err()
            .map(|e| (e.raw_os_error(), e.to_string()));
        let _ = finished.send(failure);
        if let Err(error) = &result {
            afs_logging::error!("ownerfs.workspace_shutdown_unresolved"; "error" => error.to_string());
            // Terminal identity/runtime errors are not retryable. Retain the
            // Driver and its namespace/authority/export on this mount thread.
            // The monitor starts Node shutdown; the existing process watchdog
            // fails124 if closure cannot be proved. Never proceed to FUSE.
            while self.active.is_some() {
                thread::park();
            }
        }
        result
    }

    fn cleanup_for_shutdown(&mut self) -> io::Result<()> {
        loop {
            match self.cleanup() {
                Err(error) if error.raw_os_error() == Some(libc::EBUSY) => {
                    thread::sleep(Duration::from_millis(50));
                }
                result => return result,
            }
        }
    }

    fn cleanup_listener_failure(
        &mut self,
        error: io::Error,
        finished: &tokio::sync::watch::Sender<WorkerFailure>,
    ) -> io::Result<()> {
        // Publish the failure before an EBUSY drain can wait: Services must
        // arm the existing process budget even without an external signal.
        let _ = finished.send(Some((error.raw_os_error(), error.to_string())));
        self.cleanup_for_shutdown().and(Err(error))
    }

    fn serve(&mut self, finished: &tokio::sync::watch::Sender<WorkerFailure>) -> io::Result<()> {
        while !self.stop.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
                    let response = self.receive(&stream).and_then(|request| self.handle(request))
                        .unwrap_or_else(|error| json!({"status":"ERROR","error":error.to_string(),"production_ready":false}));
                    let _ = writeln!(stream, "{response}");
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(50))
                }
                Err(error) => {
                    return self.cleanup_listener_failure(error, finished);
                }
            }
        }
        self.cleanup_for_shutdown()?;
        fs::remove_file(self.cfg.control_dir.join("control.sock"))?;
        self.lock.sync_all()?;
        fs::remove_file(self.cfg.control_dir.join("controller.lock"))?;
        Ok(())
    }

    fn receive(&self, stream: &UnixStream) -> io::Result<Request> {
        use std::io::Read;
        let mut line = String::new();
        BufReader::new(stream.take(MAX_REQUEST + 1)).read_line(&mut line)?;
        if line.len() as u64 > MAX_REQUEST || !line.ends_with('\n') {
            return Err(io::Error::other(
                "native command must be one bounded JSON line",
            ));
        }
        serde_json::from_str(&line).map_err(io::Error::other)
    }

    fn handle(&mut self, request: Request) -> io::Result<Value> {
        check_id(request.id())?;
        let id = request.id().to_owned();
        let fingerprint = serde_json::to_value(&request).map_err(io::Error::other)?;
        if let Some((original, old)) = self.responses.get(&id) {
            if original != &fingerprint {
                return Err(io::Error::other(
                    "operation id was reused with different input",
                ));
            }
            return Ok(old.clone());
        }
        // Read-only status never consumes the bounded operation ledger. Keep
        // cleanup available even after workload operation admission is full.
        if matches!(request, Request::Status { .. }) {
            return Ok(self.status());
        }
        if self.responses.len() >= MAX_OPERATIONS && !matches!(request, Request::Stop { .. }) {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        let result = match request {
            Request::Start { id, workspace } => self.start(&id, &workspace),
            Request::Exec { argv, .. } => match self.exec(&argv) {
                Ok(value) => Ok(value),
                Err(error) => match self.cleanup() {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(io::Error::other(format!(
                        "{error}; cleanup unresolved: {cleanup}"
                    ))),
                },
            },
            Request::Stop { .. } => self
                .cleanup()
                .map(|_| json!({"status":"Stopped","production_ready":false})),
            Request::Status { .. } => Ok(self.status()),
        };
        let response = result.unwrap_or_else(|error| json!({"status":"ERROR","error":error.to_string(),"active":self.status(),"production_ready":false}));
        if self.responses.len() <= MAX_OPERATIONS {
            self.responses.insert(id, (fingerprint, response.clone()));
        }
        Ok(response)
    }

    fn status(&self) -> Value {
        match &self.active {
            Some(active) => {
                let grant = active.permit.grant();
                json!({"state":if active.verified {"FinalVerified"} else {"Unknown"},
                    "container":active.container,"root":grant.id.0,"epoch":grant.epoch,
                    "home_node":grant.home_node_id,"home_session":grant.home_session_id,
                    "access_generation":grant.access_generation,"production_ready":false,
                    "scope":"single managed experimental container; not READY or revocation ACK"})
            }
            None => json!({"state":"Idle","production_ready":false}),
        }
    }

    fn start(&mut self, id: &str, workspace: &str) -> io::Result<Value> {
        if self.active.is_some() {
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        check_component(workspace)?;
        let permit = self
            .owner
            .native_home_export_for_current_namespace(OsStr::new(workspace))
            .map_err(io::Error::other)?;
        permit
            .verify_current(&self.owner)
            .map_err(io::Error::other)?;
        let source: crate::node::vfs::ownerfs::NativeHomeDirectoryIdentity =
            permit.source_identity();
        let namespace: crate::node::vfs::ownerfs::NativeHomeNamespaceIdentity = permit.namespace();
        let actual_ns = File::open("/proc/thread-self/ns/mnt")?.metadata()?;
        if (namespace.dev, namespace.ino) != (actual_ns.dev(), actual_ns.ino()) {
            return Err(io::Error::from_raw_os_error(libc::ESTALE));
        }
        let export = WorkspaceBindMount::prepare(
            permit.source_descriptor().map_err(io::Error::other)?,
            File::open(&self.mount)?,
            permit.name(),
        )?;
        self.active = Some(Active {
            permit,
            export,
            container: format!("afs-native-{id}"),
            final_namespace: None,
            final_root: None,
            final_unique: None,
            runtime_rootfs: None,
            pid: None,
            verified: false,
            container_attempted: false,
            final_clone_detached: false,
            container_deleted: false,
        });
        let result = (|| {
            let active = self.active.as_mut().expect("retained permit and export");
            active.export.activate()?;
            let claim = active.export.mount_identity()?;
            if (
                active.export.source_identity().device,
                active.export.source_identity().inode,
            ) != (source.dev, source.ino)
                || (claim.namespace.device, claim.namespace.inode) != (namespace.dev, namespace.ino)
            {
                return Err(io::Error::from_raw_os_error(libc::ESTALE));
            }
            active
                .permit
                .verify_current(&self.owner)
                .map_err(io::Error::other)?;
            self.start_container(workspace, source.dev, source.ino)
        })();
        if let Err(error) = result {
            let detail = error.to_string();
            return match self.cleanup() {
                Ok(()) => Err(io::Error::other(detail)),
                Err(cleanup) => Err(io::Error::other(format!(
                    "{detail}; cleanup unresolved: {cleanup}"
                ))),
            };
        }
        Ok(self.status())
    }

    fn start_container(&mut self, workspace: &str, dev: u64, ino: u64) -> io::Result<()> {
        let container = self
            .active
            .as_ref()
            .expect("active export")
            .container
            .clone();
        let bundle = self.cfg.control_dir.join(format!("bundle-{container}"));
        fs::create_dir(&bundle)?;
        let runtime_rootfs = bundle.join("rootfs");
        prepare_runtime_rootfs(&self.cfg.rootfs, &runtime_rootfs)?;
        let source = self.mount.join(workspace);
        let spec = container_spec(&self.cfg, &runtime_rootfs, &source);
        fs::write(
            bundle.join("config.json"),
            serde_json::to_vec_pretty(&spec)?,
        )?;
        {
            let active = self.active.as_mut().expect("retained export");
            active.runtime_rootfs = Some(runtime_rootfs);
            active.container_attempted = true;
        }
        self.run_runtime(
            &["create", "--bundle", path_str(&bundle)?, &container],
            Duration::from_secs(5),
            true,
        )?;
        self.run_runtime(&["start", &container], Duration::from_secs(5), true)?;
        let state = self.runtime_state(&container)?;
        let pid = state["pid"]
            .as_u64()
            .filter(|pid| *pid > 0 && *pid <= u32::MAX as u64)
            .ok_or_else(|| io::Error::other("runc did not report a live container pid"))?
            as u32;
        if state["status"] != "running" || state["id"] != container {
            return Err(io::Error::from_raw_os_error(libc::ESTALE));
        }
        let process = process_identity(pid)?;
        let final_ns = File::open(format!("/proc/{pid}/ns/mnt"))?;
        let final_root = File::open(format!("/proc/{pid}/root"))?;
        let ns_metadata = final_ns.metadata()?;
        let current_ns = File::open("/proc/thread-self/ns/mnt")?.metadata()?;
        if (ns_metadata.dev(), ns_metadata.ino()) == (current_ns.dev(), current_ns.ino()) {
            return Err(io::Error::other(
                "container did not create a distinct mount namespace",
            ));
        }
        let active = self.active.as_mut().expect("active export");
        active.final_namespace = Some(final_ns);
        active.final_root = Some(final_root);
        active.pid = Some(process);
        let observed = self.identity_observation(&container)?;
        verify_final(
            &observed,
            (dev, ino),
            (ns_metadata.dev(), ns_metadata.ino()),
        )?;
        if !same_process(
            self.active
                .as_ref()
                .expect("active export")
                .pid
                .as_ref()
                .expect("registered process"),
        )? {
            return Err(io::Error::from_raw_os_error(libc::ESTALE));
        }
        let active = self.active.as_mut().expect("active export");
        active
            .permit
            .verify_current(&self.owner)
            .map_err(io::Error::other)?;
        active.final_unique = observed["unique_mount_id"].as_u64();
        active.verified = true;
        Ok(())
    }

    fn exec(&mut self, argv: &[String]) -> io::Result<Value> {
        check_exec(argv)?;
        let active = self
            .active
            .as_ref()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
        if !active.verified {
            return Err(io::Error::other("container final view is not verified"));
        }
        active
            .permit
            .verify_current(&self.owner)
            .map_err(io::Error::other)?;
        if !same_process(active.pid.as_ref().expect("verified process"))? {
            return Err(io::Error::from_raw_os_error(libc::ESTALE));
        }
        let container = active.container.clone();
        let source = active.permit.source_identity();
        let namespace = active
            .final_namespace
            .as_ref()
            .expect("verified namespace")
            .metadata()?;
        let unique = active.final_unique.expect("verified unique mount");
        let observed = self.identity_observation(&container)?;
        verify_final(
            &observed,
            (source.dev, source.ino),
            (namespace.dev(), namespace.ino()),
        )?;
        if observed["unique_mount_id"].as_u64() != Some(unique) {
            return Err(io::Error::from_raw_os_error(libc::ESTALE));
        }
        let mut command = vec!["exec".to_owned(), container];
        command.extend_from_slice(argv);
        let args: Vec<_> = command.iter().map(String::as_str).collect();
        let output = self.run_runtime(&args, Duration::from_secs(120), true)?;
        // Avoid reflecting arbitrary workload output through the bounded control
        // socket. Full output and exit records are retained in the control dir.
        Ok(json!({"status":"Executed","stdout_bytes":output.len(),"production_ready":false}))
    }

    fn runtime_state(&mut self, container: &str) -> io::Result<Value> {
        serde_json::from_slice(&self.run_runtime(
            &["state", container],
            Duration::from_secs(1),
            false,
        )?)
        .map_err(io::Error::other)
    }

    fn identity_observation(&mut self, container: &str) -> io::Result<Value> {
        let command = identity_runtime_args(container, &self.cfg.identity_command);
        let args: Vec<_> = command.iter().map(String::as_str).collect();
        serde_json::from_slice(&self.run_runtime(&args, Duration::from_secs(5), true)?)
            .map_err(io::Error::other)
    }

    fn cleanup(&mut self) -> io::Result<()> {
        let Some(active) = self.active.as_mut() else {
            return Ok(());
        };
        active.verified = false;
        if !active.container_attempted {
            active.export.detach()?;
            self.active = None;
            return Ok(());
        }
        if !active.container_deleted {
            self.cleanup_container()?;
        }
        // A confirmed delete is monotonic: a busy export retry must not query
        // an already deleted container or reacquire its authority/mount.
        self.active
            .as_mut()
            .expect("retained export")
            .export
            .detach()?;
        self.active = None;
        Ok(())
    }

    fn cleanup_container(&mut self) -> io::Result<()> {
        let container = self
            .active
            .as_ref()
            .expect("retained container")
            .container
            .clone();
        // Recover an exact physical claim if the in-container observer failed.
        // Captured handles remain owned until normal clone detach is proved.
        if let Some(active) = self.active.as_ref()
            && !active.final_clone_detached
            && active.final_namespace.is_some()
            && active.final_unique.is_none()
        {
            let namespace = active.final_namespace.as_ref().expect("captured namespace");
            let root = active.final_root.as_ref().expect("captured root");
            let (observed_ns, observed_source, unique) =
                inspect_secondary_clone(namespace, root, OsStr::new("workspace"))?;
            let expected_ns = namespace.metadata()?;
            let expected_source = active.permit.source_identity();
            if (observed_ns.device, observed_ns.inode) != (expected_ns.dev(), expected_ns.ino())
                || (observed_source.device, observed_source.inode)
                    != (expected_source.dev, expected_source.ino)
                || unique == 0
            {
                return Err(io::Error::from_raw_os_error(libc::ESTALE));
            }
            self.active
                .as_mut()
                .expect("owned active claim")
                .final_unique = Some(unique);
        }
        // A missing state after a failed create is distinct from an observed
        // running container. Do not turn an unrecognized failure into drain.
        let state = self.runtime_state(&container);
        match state {
            Ok(state) => {
                if state["id"] != container {
                    return Err(io::Error::from_raw_os_error(libc::ESTALE));
                }
                if state["status"] == "running" || state["status"] == "created" {
                    self.run_runtime(
                        &["kill", "--all", &container, "KILL"],
                        Duration::from_secs(1),
                        false,
                    )?;
                }
                let until = Instant::now() + Duration::from_secs(2);
                loop {
                    let state = self.runtime_state(&container)?;
                    if state["status"] == "stopped" {
                        break;
                    }
                    if Instant::now() >= until {
                        return Err(io::Error::from_raw_os_error(libc::EBUSY));
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
            Err(error) => {
                return Err(io::Error::other(format!(
                    "container state unknown: {error}"
                )));
            }
        }
        if let Some(process) = &self.active.as_ref().expect("active").pid
            && same_process(process)?
        {
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        let active = self.active.as_ref().expect("active");
        if active.final_namespace.is_some() && active.final_unique.is_none() {
            return Err(io::Error::other(
                "final clone identity remains unverified; retain claim for reconciliation",
            ));
        }
        if !active.final_clone_detached
            && let (Some(namespace), Some(root), Some(unique)) = (
                &active.final_namespace,
                &active.final_root,
                active.final_unique,
            )
        {
            let source = active.permit.source_identity();
            let runtime_rootfs = active
                .runtime_rootfs
                .as_ref()
                .ok_or_else(|| io::Error::other("missing owned runtime rootfs"))?;
            let covered = fs::metadata(runtime_rootfs.join("workspace"))?;
            detach_secondary_clone(
                namespace,
                root,
                OsStr::new("workspace"),
                DirectoryIdentity {
                    device: source.dev,
                    inode: source.ino,
                },
                unique,
                DirectoryIdentity {
                    device: covered.dev(),
                    inode: covered.ino(),
                },
            )?;
            self.active
                .as_mut()
                .expect("retained final claim")
                .final_clone_detached = true;
        }
        self.run_runtime(&["delete", &container], Duration::from_secs(1), false)?;
        let active = self.active.as_mut().expect("active");
        active.container_deleted = true;
        active.final_root = None;
        active.final_namespace = None;
        active.runtime_rootfs = None;
        Ok(())
    }

    fn run_runtime(
        &mut self,
        args: &[&str],
        timeout: Duration,
        cancel: bool,
    ) -> io::Result<Vec<u8>> {
        let runtime = self.cfg.runtime.clone();
        let root = self.cfg.control_dir.join("runtime-state");
        let mut all = vec!["--root", path_str(&root)?];
        all.extend_from_slice(args);
        self.run_external(&runtime, &all, timeout, cancel)
    }
    fn run_external(
        &mut self,
        binary: &Path,
        args: &[&str],
        timeout: Duration,
        cancel: bool,
    ) -> io::Result<Vec<u8>> {
        let next = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EOVERFLOW))?;
        let stem = self.cfg.control_dir.join(format!("command-{next:04}"));
        self.sequence = next;
        let mut command = Command::new(binary);
        command.args(args);
        run_recorded(
            &mut command,
            &stem,
            timeout,
            if cancel { Some(&self.stop) } else { None },
        )
    }
}

fn next_command_sequence(control_dir: &Path) -> io::Result<u64> {
    let mut max = 0u64;
    for entry in fs::read_dir(control_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(rest) = name.strip_prefix("command-") else {
            continue;
        };
        let metadata = fs::symlink_metadata(entry.path())?;
        if !metadata.is_file() {
            return Err(io::Error::from_raw_os_error(libc::EPERM));
        }
        let Some(suffix) = COMMAND_SUFFIXES
            .iter()
            .find(|suffix| rest.ends_with(**suffix))
        else {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        };
        let digits = &rest[..rest.len() - suffix.len()];
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        let sequence = digits
            .parse::<u64>()
            .map_err(|_| io::Error::from_raw_os_error(libc::EOVERFLOW))?;
        max = max.max(sequence);
    }
    Ok(max)
}

fn container_spec(cfg: &NativeWorkspaceConfig, rootfs: &Path, source: &Path) -> Value {
    json!({"ociVersion":"1.0.2","root":{"path":rootfs,"readonly":true},
        "hostname":"afs-native-workspace","process":{"terminal":false,"cwd":"/",
            "args":cfg.idle_command.clone(),"user":{"uid":cfg.workload_uid,"gid":cfg.workload_gid},
            "env":["PATH=/bin:/usr/bin"],"noNewPrivileges":true,
            "capabilities":{"bounding":[],"effective":[],"inheritable":[],"permitted":[],"ambient":[]},
            "rlimits":[{"type":"RLIMIT_NOFILE","hard":256,"soft":256}]},
        "mounts":[{"destination":"/proc","type":"proc","source":"proc","options":["nosuid","nodev","noexec"]},
            {"destination":"/workspace","type":"bind","source":source,"options":["bind","rw","nosuid","nodev"]}],
        "linux":{"namespaces":[{"type":"mount"},{"type":"pid"},{"type":"network"},{"type":"ipc"},{"type":"uts"},{"type":"cgroup"}]}})
}

fn identity_runtime_args(container: &str, identity_command: &[String]) -> Vec<String> {
    let mut command = vec!["exec".to_owned(), container.to_owned()];
    command.extend_from_slice(identity_command);
    command
}

fn prepare_runtime_rootfs(template: &Path, destination: &Path) -> io::Result<()> {
    trusted_tree(template)?;
    if destination.exists() {
        return Err(io::Error::from_raw_os_error(libc::EEXIST));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;
    trusted_path(parent, true)?;
    let root_metadata = fs::symlink_metadata(template)?;
    fs::create_dir(destination)?;
    fs::set_permissions(
        destination,
        fs::Permissions::from_mode(root_metadata.permissions().mode() & 0o777),
    )?;
    let mut stack = vec![template.to_owned()];
    let mut count = 0;
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory)? {
            count += 1;
            if count > 256 {
                return Err(io::Error::from_raw_os_error(libc::E2BIG));
            }
            let source_path = entry?.path();
            let metadata = fs::symlink_metadata(&source_path)?;
            if metadata.file_type().is_symlink()
                || metadata.uid() != 0
                || metadata.permissions().mode() & 0o022 != 0
                || (!metadata.is_dir() && !metadata.is_file())
            {
                return Err(io::Error::from_raw_os_error(libc::EPERM));
            }
            let relative = source_path
                .strip_prefix(template)
                .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
            let target = destination.join(relative);
            if metadata.is_dir() {
                fs::create_dir(&target)?;
                fs::set_permissions(
                    &target,
                    fs::Permissions::from_mode(metadata.permissions().mode() & 0o777),
                )?;
                stack.push(source_path);
            } else {
                let copied = fs::copy(&source_path, &target)?;
                if copied != metadata.len() {
                    return Err(io::Error::from_raw_os_error(libc::EIO));
                }
                fs::set_permissions(
                    &target,
                    fs::Permissions::from_mode(metadata.permissions().mode() & 0o777),
                )?;
            }
        }
    }
    trusted_tree(destination)
}

fn verify_final(value: &Value, source: (u64, u64), namespace: (u64, u64)) -> io::Result<()> {
    if value["source"]["dev"].as_u64() != Some(source.0)
        || value["source"]["ino"].as_u64() != Some(source.1)
        || value["namespace"]["dev"].as_u64() != Some(namespace.0)
        || value["namespace"]["ino"].as_u64() != Some(namespace.1)
        || value["unique_mount_id"].as_u64().is_none_or(|id| id == 0)
    {
        return Err(io::Error::from_raw_os_error(libc::ESTALE));
    }
    let flags = value["flags"]
        .as_array()
        .ok_or_else(|| io::Error::from_raw_os_error(libc::EPERM))?;
    if !flags.iter().any(|f| f == "nosuid") || !flags.iter().any(|f| f == "nodev") {
        return Err(io::Error::from_raw_os_error(libc::EPERM));
    }
    Ok(())
}

fn check_id(id: &str) -> io::Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    Ok(())
}
fn check_component(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name.len() > 255
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\0')
    {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    Ok(())
}
fn check_exec(argv: &[String]) -> io::Result<()> {
    if argv.is_empty()
        || argv.len() > 64
        || argv
            .iter()
            .any(|s| s.contains('\0') || s.len() > MAX_ARG_BYTES)
        || !Path::new(&argv[0]).is_absolute()
    {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    Ok(())
}
fn check_adapter_command(argv: &[String]) -> io::Result<()> {
    check_exec(argv)?;
    if argv.len() > MAX_COMMAND_ARGS
        || argv.iter().any(String::is_empty)
        || !valid_container_program(&argv[0])
    {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    Ok(())
}
fn valid_container_program(program: &str) -> bool {
    let path = Path::new(program);
    path.is_absolute()
        && path.file_name().is_some()
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}
fn rootfs_command_path(rootfs: &Path, program: &str) -> io::Result<PathBuf> {
    if !valid_container_program(program) {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    let mut path = rootfs.to_owned();
    for component in Path::new(program).components() {
        match component {
            Component::RootDir => {}
            Component::Normal(part) => path.push(part),
            _ => return Err(io::Error::from_raw_os_error(libc::EINVAL)),
        }
    }
    Ok(path)
}
fn path_str(path: &Path) -> io::Result<&str> {
    path.to_str()
        .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))
}
fn trusted_path(path: &Path, directory: bool) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        if matches!(component, Component::ParentDir | Component::CurDir) {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        current.push(component);
        let meta = fs::symlink_metadata(&current)?;
        if meta.file_type().is_symlink()
            || meta.uid() != 0
            || meta.permissions().mode() & 0o022 != 0
        {
            return Err(io::Error::from_raw_os_error(libc::EPERM));
        }
    }
    let metadata = fs::metadata(path)?;
    if metadata.is_dir() != directory || (!directory && !metadata.is_file()) {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    Ok(())
}
fn trusted_tree(root: &Path) -> io::Result<()> {
    trusted_path(root, true)?;
    let mut stack = vec![root.to_owned()];
    let mut count = 0;
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(directory)? {
            count += 1;
            if count > 256 {
                return Err(io::Error::from_raw_os_error(libc::E2BIG));
            }
            let path = entry?.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink()
                || metadata.uid() != 0
                || metadata.permissions().mode() & 0o022 != 0
                || (!metadata.is_dir() && !metadata.is_file())
            {
                return Err(io::Error::from_raw_os_error(libc::EPERM));
            }
            if metadata.is_dir() {
                stack.push(path);
            }
        }
    }
    Ok(())
}
fn process_identity(pid: u32) -> io::Result<ProcessIdentity> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let fields = stat
        .rsplit_once(") ")
        .ok_or_else(|| io::Error::other("invalid process stat"))?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    Ok(ProcessIdentity {
        pid,
        start: fields
            .get(19)
            .ok_or_else(|| io::Error::other("missing process start"))?
            .to_string(),
    })
}
fn same_process(expected: &ProcessIdentity) -> io::Result<bool> {
    match process_identity(expected.pid) {
        Ok(actual) => Ok(actual.start == expected.start),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
fn run_recorded(
    command: &mut Command,
    stem: &Path,
    timeout: Duration,
    stop: Option<&AtomicBool>,
) -> io::Result<Vec<u8>> {
    let stdout = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(stem.with_extension("stdout"))?;
    let stderr = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(stem.with_extension("stderr"))?;
    fs::write(
        stem.with_extension("command.json"),
        serde_json::to_vec(
            &json!({"program":command.get_program().to_string_lossy(),"argv":command.get_args().map(|a|a.to_string_lossy()).collect::<Vec<_>>(),"timeout_ms":timeout.as_millis()}),
        )?,
    )?;
    let mut child = command
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()?;
    let deadline = Instant::now() + timeout;
    let (exit, reason) = loop {
        if let Some(status) = child.try_wait()? {
            break (status, None);
        }
        let output_limit = fs::metadata(stem.with_extension("stdout"))?.len()
            + fs::metadata(stem.with_extension("stderr"))?.len()
            > 2 * 1024 * 1024;
        if Instant::now() >= deadline
            || stop.is_some_and(|stop| stop.load(Ordering::Acquire))
            || output_limit
        {
            child.kill()?;
            let status = child.wait()?;
            break (
                status,
                Some(if output_limit {
                    "output limit"
                } else {
                    "deadline or controller shutdown"
                }),
            );
        }
        thread::sleep(Duration::from_millis(10));
    };
    fs::write(
        stem.with_extension("exit.json"),
        serde_json::to_vec(&json!({"code":exit.code(),"success":exit.success(),"reason":reason}))?,
    )?;
    if !exit.success() || reason.is_some() {
        return Err(io::Error::other(format!(
            "command failed; see {}",
            stem.display()
        )));
    }
    let metadata = fs::metadata(stem.with_extension("stdout"))?;
    if metadata.len() > 1024 * 1024 {
        return Err(io::Error::from_raw_os_error(libc::EFBIG));
    }
    fs::read(stem.with_extension("stdout"))
}
#[allow(unsafe_code)]
fn require_root() -> io::Result<()> {
    // SAFETY: geteuid has no arguments and no mutable process effects.
    if unsafe { libc::geteuid() } != 0 {
        return Err(io::Error::from_raw_os_error(libc::EPERM));
    }
    Ok(())
}
#[allow(unsafe_code)]
fn isolate_mount_namespace() -> io::Result<()> {
    // SAFETY: called once on the owned dedicated thread before accepting any
    // request. It changes this thread's namespace, never an async task's scope.
    if unsafe { libc::unshare(libc::CLONE_NEWNS) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let root = CString::new("/").expect("fixed root");
    // SAFETY: fixed live NUL-terminated root, null pointers ignored for PRIVATE.
    if unsafe {
        libc::mount(
            std::ptr::null(),
            root.as_ptr(),
            std::ptr::null(),
            libc::MS_REC | libc::MS_PRIVATE,
            std::ptr::null(),
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::symlink;

    #[cfg(target_os = "linux")]
    fn cleanup_retry_fixture() -> (tempfile::TempDir, tempfile::TempDir, Driver) {
        require_root().unwrap();
        isolate_mount_namespace().unwrap();
        let (source_temp, owner, ctx, _) =
            crate::node::vfs::ownerfs::native_home_tests::fixture(true);
        crate::node::vfs::ownerfs::native_home_tests::mkdir_root(&owner, &ctx, "workspace");
        let owner = Arc::new(owner);
        let permit = owner
            .native_home_export_for_current_namespace(OsStr::new("workspace"))
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let mount = temp.path().join("mount");
        fs::create_dir_all(mount.join("workspace")).unwrap();
        let mut export = WorkspaceBindMount::prepare(
            permit.source_descriptor().unwrap(),
            File::open(&mount).unwrap(),
            permit.name(),
        )
        .unwrap();
        export.activate().unwrap();
        let runtime = temp.path().join("runtime");
        fs::write(
            &runtime,
            format!(
                "#!/bin/sh\nset -eu\ncd '{}'\nprintf '%s\\n' \"$3\" >> calls\n[ ! -e deleted ] || exit 64\ncase \"$3\" in\nstate) printf '%s\\n' '{{\"id\":\"afs-native-retry\",\"status\":\"stopped\"}}';;\ndelete) if [ -e fail-delete-once ]; then rm fail-delete-once; exit 1; fi; touch deleted;;\n*) exit 65;;\nesac\n",
                temp.path().display()
            ),
        )
        .unwrap();
        fs::set_permissions(&runtime, fs::Permissions::from_mode(0o755)).unwrap();
        let driver = Driver {
            owner,
            mount,
            cfg: NativeWorkspaceConfig {
                control_dir: temp.path().into(),
                runtime,
                rootfs: temp.path().join("rootfs"),
                idle_command: vec!["/bin/view-observer".into(), "idle".into()],
                identity_command: vec!["/bin/view-observer".into(), "identity".into()],
                workload_uid: 501,
                workload_gid: 501,
            },
            stop: Arc::new(AtomicBool::new(false)),
            listener: UnixListener::bind(temp.path().join("control.sock")).unwrap(),
            lock: File::create(temp.path().join("controller.lock")).unwrap(),
            active: Some(Active {
                permit,
                export,
                container: "afs-native-retry".into(),
                final_namespace: None,
                final_root: None,
                final_unique: None,
                runtime_rootfs: None,
                pid: None,
                verified: true,
                container_attempted: true,
                final_clone_detached: false,
                container_deleted: false,
            }),
            responses: HashMap::new(),
            sequence: 0,
        };
        (temp, source_temp, driver)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_workspace_owner_survives_services_deadline_before_explicit_join() {
        let stop = Arc::new(AtomicBool::new(false));
        let (finished_tx, finished) = tokio::sync::watch::channel(None::<(Option<i32>, String)>);
        let (release_tx, release_rx) = mpsc::channel();
        let worker = NativeWorkspace {
            stop: stop.clone(),
            worker: Some(thread::spawn(move || {
                release_rx.recv().unwrap();
                let _ = finished_tx.send(None);
                Ok(())
            })),
            finished,
        };
        let mut services = crate::runtime::Services::new();
        let (owner, error) =
            super::super::register_native_workspace_startup(&mut services, Ok(Ok(worker)));
        assert!(error.is_none());
        services.spawn(async { std::future::pending::<crate::runtime::ServiceResult>().await });
        services.spawn(async { Err(io::Error::other("test sibling failure").into()) });
        let began = Instant::now();
        let error = tokio::time::timeout(Duration::from_secs(15), services.run())
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("tasks aborted"));
        assert!(began.elapsed() >= Duration::from_secs(10));
        let owner = owner.expect("Node still owns its original worker");
        assert!(stop.load(Ordering::Acquire));
        assert!(!owner.worker.as_ref().unwrap().is_finished());
        let mut closure = tokio::task::spawn_blocking(move || owner.shutdown());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut closure)
                .await
                .is_err()
        );
        // This is the Node-owned closure gate: teardown may continue only
        // after the same worker completes, not after Services aborts observers.
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), closure)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[cfg(target_os = "linux")]
    fn shutdown_terminal_claim_child() {
        let record = std::env::var_os("AFS_TEST_WORKSPACE_TERMINAL_RECORD")
            .expect("run only from terminal retention parent");
        let (temp, _source, driver) = cleanup_retry_fixture();
        fs::write(&driver.cfg.runtime, format!(
            "#!/bin/sh\nset -eu\ncd '{}'\nprintf '%s\n' \"$3\" >> calls\nprintf '%s\n' '{{\"id\":\"wrong-container\",\"status\":\"stopped\"}}'\n",
            temp.path().display())).unwrap();
        driver.stop.store(true, Ordering::Release);
        let target = driver.mount.join("workspace");
        let expected = driver.active.as_ref().unwrap().permit.source_identity();
        let (finished_tx, finished) = tokio::sync::watch::channel(None::<(Option<i32>, String)>);
        let deadline = crate::runtime::ShutdownDeadline::new(Duration::from_millis(500)).unwrap();
        deadline.trigger().arm();
        thread::spawn(move || {
            let until = Instant::now() + Duration::from_millis(300);
            while finished.borrow().is_none() {
                assert!(Instant::now() < until, "terminal error must be observable");
                thread::sleep(Duration::from_millis(5));
            }
            let failure = finished.borrow().clone().unwrap();
            assert_eq!(failure.0, Some(libc::ESTALE));
            thread::sleep(Duration::from_millis(100));
            let observed = fs::metadata(target).unwrap();
            let calls = fs::read_to_string(temp.path().join("calls")).unwrap();
            assert_eq!(calls, "state\n");
            assert_eq!(
                (observed.dev(), observed.ino()),
                (expected.dev, expected.ino)
            );
            assert!(temp.path().join("control.sock").exists());
            assert!(temp.path().join("controller.lock").exists());
            fs::write(
                record,
                serde_json::to_vec(&json!({
                    "errno":failure.0,"calls":calls,"same_physical_mount_retained":true,
                    "control_claim_retained":true,"normal_closure":false
                }))
                .unwrap(),
            )
            .unwrap();
        });
        let _ = driver.run(finished_tx);
        panic!("terminal claim must stay owned until failure watchdog exits");
    }

    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires root Linux private mount namespace; run explicitly"]
    fn shutdown_terminal_retains_claim_until_process_watchdog() {
        if std::env::var_os("AFS_TEST_WORKSPACE_TERMINAL_RECORD").is_some() {
            shutdown_terminal_claim_child();
            return;
        }
        require_root().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let record = temp.path().join("record.json");
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "node::native_workspace::tests::shutdown_terminal_retains_claim_until_process_watchdog",
                "--ignored",
                "--exact",
                "--test-threads=1",
                "--nocapture",
            ])
            .env("AFS_TEST_WORKSPACE_TERMINAL_RECORD", &record)
            .env("TMPDIR", temp.path())
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(124),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let proof: Value = serde_json::from_slice(&fs::read(record).unwrap()).unwrap();
        assert_eq!(proof["errno"], libc::ESTALE);
        assert_eq!(proof["calls"], "state\n");
        assert_eq!(proof["same_physical_mount_retained"], true);
        assert_eq!(proof["control_claim_retained"], true);
        println!("terminal child: exit124, {proof}");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires root Linux private mount namespace; run explicitly"]
    async fn listener_failure_arms_shutdown_before_busy_export_drains() {
        require_root().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let (finished_tx, finished) = tokio::sync::watch::channel(None::<(Option<i32>, String)>);
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let (temp, _source, mut driver) = cleanup_retry_fixture();
            driver.stop = worker_stop;
            let target = driver.mount.join("workspace");
            let mut holder = Command::new("sleep")
                .arg("30")
                .current_dir(&target)
                .spawn()
                .unwrap();
            let release = thread::spawn(move || {
                // A backup release keeps the regression bounded if notification
                // ordering breaks; the parent must observe failure much sooner.
                let _ = release_rx.recv_timeout(Duration::from_secs(1));
                holder.kill().unwrap();
                holder.wait().unwrap();
            });
            ready_tx
                .send((
                    File::open(target).unwrap(),
                    driver.active.as_ref().unwrap().permit.source_identity(),
                ))
                .unwrap();
            let result = driver
                .cleanup_listener_failure(io::Error::from_raw_os_error(libc::EMFILE), &finished_tx);
            release.join().unwrap();
            assert!(driver.active.is_none());
            assert_eq!(
                fs::read_to_string(temp.path().join("calls")).unwrap(),
                "state\nstate\ndelete\n"
            );
            result
        });
        let (target, expected) = ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let worker = NativeWorkspace {
            stop,
            worker: Some(worker),
            finished,
        };
        let mut services = crate::runtime::Services::new();
        let (owner, error) =
            super::super::register_native_workspace_startup(&mut services, Ok(Ok(worker)));
        assert!(error.is_none());
        let deadline = crate::runtime::ShutdownDeadline::new(Duration::from_secs(2)).unwrap();
        let trigger = deadline.trigger();
        let armed = Arc::new(AtomicBool::new(false));
        let armed_callback = armed.clone();
        let began = Instant::now();
        let error = services
            .run_with_shutdown(move || {
                trigger.arm();
                armed_callback.store(true, Ordering::Release);
            })
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().raw_os_error(),
            Some(libc::EMFILE)
        );
        assert!(began.elapsed() < Duration::from_millis(500));
        assert!(armed.load(Ordering::Acquire));
        let owner = owner.unwrap();
        assert!(!owner.worker.as_ref().unwrap().is_finished());
        let observed = target.metadata().unwrap();
        assert_eq!(
            (observed.dev(), observed.ino()),
            (expected.dev, expected.ino)
        );
        drop(target);
        release_tx.send(()).unwrap();
        let error = tokio::task::spawn_blocking(move || owner.shutdown())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
        deadline.complete();
    }

    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires root Linux private mount namespace; run explicitly"]
    fn shutdown_waits_for_busy_export_before_controller_closure() {
        let (temp, _source, mut driver) = cleanup_retry_fixture();
        let mut holder = Command::new("sleep")
            .arg("30")
            .current_dir(driver.mount.join("workspace"))
            .spawn()
            .unwrap();
        driver.stop.store(true, Ordering::Release);
        let release = thread::spawn(move || {
            thread::sleep(Duration::from_millis(150));
            holder.kill().unwrap();
            holder.wait().unwrap();
        });
        let began = Instant::now();
        let (finished_tx, _) = tokio::sync::watch::channel(None::<(Option<i32>, String)>);
        let result = driver.serve(&finished_tx);
        release.join().unwrap();
        result.expect("shutdown retains the busy export until normal detach can finish");
        assert!(began.elapsed() >= Duration::from_millis(150));
        assert!(driver.active.is_none());
        assert!(!temp.path().join("control.sock").exists());
        assert!(!temp.path().join("controller.lock").exists());
        assert_eq!(
            fs::read_to_string(temp.path().join("calls")).unwrap(),
            "state\nstate\ndelete\n"
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires root Linux private mount namespace; run explicitly"]
    fn cleanup_retry_after_deleted_container_retains_busy_export() {
        let (temp, _source, mut driver) = cleanup_retry_fixture();
        let claim = driver
            .active
            .as_ref()
            .unwrap()
            .export
            .mount_identity()
            .unwrap();
        let mut holder = Command::new("sleep")
            .arg("30")
            .current_dir(driver.mount.join("workspace"))
            .spawn()
            .unwrap();
        let first = driver.cleanup();
        let held = driver
            .active
            .as_ref()
            .map(|a| a.export.mount_identity().unwrap());
        holder.kill().unwrap();
        holder.wait().unwrap();
        let second = driver.cleanup();
        assert_eq!(first.unwrap_err().raw_os_error(), Some(libc::EBUSY));
        assert_eq!(held, Some(claim));
        second.expect("retry must only detach the same export after confirmed delete");
        assert_eq!(driver.status()["state"], "Idle");
        assert_eq!(
            fs::read_to_string(temp.path().join("calls")).unwrap(),
            "state\nstate\ndelete\n"
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires root Linux private mount namespace; run explicitly"]
    fn cleanup_retry_after_clone_detach_does_not_unmount_twice() {
        let (temp, _source, mut driver) = cleanup_retry_fixture();
        let rootfs = temp.path().join("runtime-rootfs");
        fs::create_dir_all(rootfs.join("workspace")).unwrap();
        let active = driver.active.as_mut().unwrap();
        // A real second namespace keeps the controller's rootfs path covered,
        // just as the adapter sees the private OCI root separately from its view.
        let mut child = Command::new("unshare")
            .args(["--mount", "--propagation", "private", "/bin/sh", "-ec",
                "mount --bind \"$1\" \"$2/workspace\"; mount -o remount,bind,nosuid,nodev \"$2/workspace\"; echo ready; read release",
                "clone-fixture"])
            .arg(driver.mount.join("workspace"))
            .arg(&rootfs)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut ready = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut ready)
            .unwrap();
        assert_eq!(ready, "ready\n");
        let namespace = File::open(format!("/proc/{}/ns/mnt", child.id())).unwrap();
        let root = File::open(format!("/proc/{}/root{}", child.id(), rootfs.display())).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        let (_, _, unique) =
            inspect_secondary_clone(&namespace, &root, OsStr::new("workspace")).unwrap();
        active.final_namespace = Some(namespace);
        active.final_root = Some(root);
        active.final_unique = Some(unique);
        active.runtime_rootfs = Some(rootfs);
        fs::write(temp.path().join("fail-delete-once"), b"").unwrap();
        let first = driver.cleanup();
        assert!(
            first.is_err(),
            "injected runtime deletion failure must propagate"
        );
        assert_eq!(driver.status()["state"], "Unknown");
        assert!(
            driver.active.is_some(),
            "failed cleanup retains its authority and claim"
        );
        driver
            .cleanup()
            .expect("retry must skip the confirmed final clone detach");
        assert_eq!(driver.status()["state"], "Idle");
        assert_eq!(
            fs::read_to_string(temp.path().join("calls")).unwrap(),
            "state\nstate\ndelete\nstate\nstate\ndelete\n"
        );
    }

    fn rootfs_template(root: &Path) {
        fs::create_dir(root).unwrap();
        fs::set_permissions(root, fs::Permissions::from_mode(0o755)).unwrap();
        for directory in ["dev", "proc", "workspace", "bin"] {
            fs::create_dir(root.join(directory)).unwrap();
            fs::set_permissions(root.join(directory), fs::Permissions::from_mode(0o755)).unwrap();
        }
        fs::write(root.join("bin/view-observer"), b"probe").unwrap();
        fs::set_permissions(
            root.join("bin/view-observer"),
            fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        fs::write(root.join("bin/sh"), b"shell").unwrap();
        fs::set_permissions(root.join("bin/sh"), fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn tree_fingerprint(root: &Path) -> Vec<(PathBuf, u32, u64, bool)> {
        let mut entries = Vec::new();
        let mut stack = vec![root.to_owned()];
        while let Some(directory) = stack.pop() {
            for entry in fs::read_dir(&directory).unwrap() {
                let path = entry.unwrap().path();
                let metadata = fs::symlink_metadata(&path).unwrap();
                entries.push((
                    path.strip_prefix(root).unwrap().to_owned(),
                    metadata.permissions().mode() & 0o777,
                    metadata.len(),
                    metadata.is_dir(),
                ));
                if metadata.is_dir() {
                    stack.push(path);
                }
            }
        }
        entries.sort();
        entries
    }

    #[cfg(target_os = "linux")]
    fn root_owned_tempdir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("afs-native-rootfs-")
            .tempdir_in("/root")
            .unwrap()
    }

    #[cfg(target_os = "linux")]
    fn create_char_device(path: &Path) {
        let status = Command::new("/usr/bin/mknod")
            .args([path_str(path).unwrap(), "c", "1", "3"])
            .status()
            .unwrap();
        assert!(status.success(), "mknod failed: {status}");
    }

    #[test]
    fn native_control_replay_preserves_failure_and_capacity_never_blocks_stop() {
        let temp = tempfile::tempdir().unwrap();
        let cfg = NativeWorkspaceConfig {
            control_dir: temp.path().into(),
            runtime: "/not-admitted".into(),
            rootfs: temp.path().join("rootfs"),
            idle_command: vec!["/bin/view-observer".into(), "idle".into()],
            identity_command: vec!["/bin/view-observer".into(), "identity".into()],
            workload_uid: 501,
            workload_gid: 501,
        };
        // Controller admission is deliberately not bypassed in product code;
        // this in-process fixture exercises only request ledger behavior.
        let mut driver = Driver {
            owner: Arc::new(OwnerFs::new()),
            mount: temp.path().into(),
            cfg,
            stop: Arc::new(AtomicBool::new(false)),
            listener: UnixListener::bind(temp.path().join("control.sock")).unwrap(),
            lock: File::create(temp.path().join("controller.lock")).unwrap(),
            active: None,
            responses: HashMap::new(),
            sequence: 0,
        };
        let first = driver
            .handle(Request::Start {
                id: "replay".into(),
                workspace: "root".into(),
            })
            .unwrap();
        assert_eq!(first["status"], "ERROR");
        assert_eq!(
            driver
                .handle(Request::Start {
                    id: "replay".into(),
                    workspace: "root".into()
                })
                .unwrap(),
            first
        );
        assert!(
            driver
                .handle(Request::Start {
                    id: "replay".into(),
                    workspace: "other".into()
                })
                .is_err()
        );
        for i in 1..MAX_OPERATIONS {
            assert_eq!(
                driver
                    .handle(Request::Exec {
                        id: format!("exec-{i}"),
                        argv: vec!["/bin/true".into()]
                    })
                    .unwrap()["status"],
                "ERROR"
            );
        }
        assert_eq!(driver.responses.len(), MAX_OPERATIONS);
        assert!(
            driver
                .handle(Request::Start {
                    id: "full".into(),
                    workspace: "root".into()
                })
                .is_err()
        );
        for i in 0..100 {
            assert_eq!(
                driver
                    .handle(Request::Status {
                        id: format!("status-{i}")
                    })
                    .unwrap()["state"],
                "Idle"
            );
            assert_eq!(
                driver
                    .handle(Request::Stop {
                        id: format!("stop-{i}")
                    })
                    .unwrap()["status"],
                "Stopped"
            );
        }
        assert_eq!(driver.responses.len(), MAX_OPERATIONS + 1);
    }
    #[test]
    fn native_command_sequence_continues_after_retained_partial_records() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("command-0007.stdout"), b"old stdout").unwrap();
        fs::write(
            temp.path().join("command-0007.command.json"),
            b"old command",
        )
        .unwrap();
        fs::write(temp.path().join("command-0003.exit.json"), b"older exit").unwrap();
        let old_stdout = fs::read(temp.path().join("command-0007.stdout")).unwrap();
        let old_command = fs::read(temp.path().join("command-0007.command.json")).unwrap();
        let cfg = NativeWorkspaceConfig {
            control_dir: temp.path().into(),
            runtime: "/not-admitted".into(),
            rootfs: temp.path().join("rootfs"),
            idle_command: vec!["/bin/view-observer".into(), "idle".into()],
            identity_command: vec!["/bin/view-observer".into(), "identity".into()],
            workload_uid: 501,
            workload_gid: 501,
        };
        let mut driver = Driver {
            owner: Arc::new(OwnerFs::new()),
            mount: temp.path().into(),
            cfg,
            stop: Arc::new(AtomicBool::new(false)),
            listener: UnixListener::bind(temp.path().join("control.sock")).unwrap(),
            lock: File::create(temp.path().join("controller.lock")).unwrap(),
            active: None,
            responses: HashMap::new(),
            sequence: next_command_sequence(temp.path()).unwrap(),
        };
        driver
            .run_external(Path::new("/bin/true"), &[], Duration::from_secs(1), false)
            .unwrap();
        assert_eq!(
            fs::read(temp.path().join("command-0007.stdout")).unwrap(),
            old_stdout
        );
        assert_eq!(
            fs::read(temp.path().join("command-0007.command.json")).unwrap(),
            old_command
        );
        assert!(temp.path().join("command-0008.stdout").exists());
        assert!(temp.path().join("command-0008.stderr").exists());
        assert!(temp.path().join("command-0008.command.json").exists());
        assert!(temp.path().join("command-0008.exit.json").exists());
    }
    #[test]
    fn native_command_sequence_rejects_malformed_records_and_overflow() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("command-0001.output"), b"ambiguous").unwrap();
        assert!(matches!(
            next_command_sequence(temp.path()),
            Err(error) if error.raw_os_error() == Some(libc::EINVAL)
        ));
        fs::remove_file(temp.path().join("command-0001.output")).unwrap();
        fs::write(
            temp.path().join(format!("command-{}.stdout", u64::MAX)),
            b"max",
        )
        .unwrap();
        let cfg = NativeWorkspaceConfig {
            control_dir: temp.path().into(),
            runtime: "/not-admitted".into(),
            rootfs: temp.path().join("rootfs"),
            idle_command: vec!["/bin/view-observer".into(), "idle".into()],
            identity_command: vec!["/bin/view-observer".into(), "identity".into()],
            workload_uid: 501,
            workload_gid: 501,
        };
        let mut driver = Driver {
            owner: Arc::new(OwnerFs::new()),
            mount: temp.path().into(),
            cfg,
            stop: Arc::new(AtomicBool::new(false)),
            listener: UnixListener::bind(temp.path().join("control.sock")).unwrap(),
            lock: File::create(temp.path().join("controller.lock")).unwrap(),
            active: None,
            responses: HashMap::new(),
            sequence: next_command_sequence(temp.path()).unwrap(),
        };
        assert!(matches!(
            driver.run_external(Path::new("/bin/true"), &[], Duration::from_secs(1), false),
            Err(error) if error.raw_os_error() == Some(libc::EOVERFLOW)
        ));
    }
    #[test]
    #[cfg(target_os = "linux")]
    fn native_command_sequence_rejects_symlinked_record() {
        let temp = tempfile::tempdir().unwrap();
        symlink("/etc/passwd", temp.path().join("command-0001.stdout")).unwrap();
        assert!(matches!(
            next_command_sequence(temp.path()),
            Err(error) if error.raw_os_error() == Some(libc::EPERM)
        ));
    }
    #[test]
    fn native_control_rejects_source_injection_and_bad_identifiers() {
        assert!(
            serde_json::from_str::<Request>(
                r#"{"operation":"start","id":"a","workspace":"root","source":"/etc"}"#
            )
            .is_err()
        );
        for name in ["", ".", "..", "a/b", "a\0b"] {
            assert!(check_component(name).is_err());
        }
        for id in ["", "../x", "a b", "a\nb"] {
            assert!(check_id(id).is_err());
        }
        assert!(check_exec(&[]).is_err());
        assert!(check_exec(&["relative".into()]).is_err());
        assert!(check_exec(&["/bin/view-observer".into(), "x\0x".into()]).is_err());
        assert!(check_exec(&["/bin/view-observer".into(), "x".repeat(MAX_ARG_BYTES + 1)]).is_err());
        let workload_64_args = (0..64)
            .map(|index| {
                if index == 0 {
                    "/bin/../legacy-workload".to_owned()
                } else if index == 1 {
                    String::new()
                } else {
                    format!("arg-{index}")
                }
            })
            .collect::<Vec<_>>();
        check_exec(&workload_64_args).unwrap();
        assert!(check_adapter_command(&["/bin/../view-observer".into()]).is_err());
        assert!(check_adapter_command(&["/bin/view-observer".into(), String::new()]).is_err());
        assert!(check_adapter_command(&["/bin/view\0observer".into()]).is_err());
        assert!(
            check_adapter_command(&["/bin/view-observer".into(), "x".repeat(MAX_ARG_BYTES + 1)])
                .is_err()
        );
        assert!(
            check_adapter_command(
                &(0..=MAX_COMMAND_ARGS)
                    .map(|index| {
                        if index == 0 {
                            "/bin/view-observer".to_owned()
                        } else {
                            format!("arg-{index}")
                        }
                    })
                    .collect::<Vec<_>>()
            )
            .is_err()
        );
        assert_eq!(
            rootfs_command_path(Path::new("/rootfs"), "/bin/view-observer").unwrap(),
            Path::new("/rootfs/bin/view-observer")
        );
    }
    #[test]
    fn native_final_view_rejects_wrong_source_namespace_mount_and_policy() {
        let valid = json!({"source":{"dev":1,"ino":2},"namespace":{"dev":3,"ino":4},"unique_mount_id":5,"flags":["rw","nosuid","nodev"]});
        verify_final(&valid, (1, 2), (3, 4)).unwrap();
        assert!(verify_final(&valid, (1, 8), (3, 4)).is_err());
        assert!(verify_final(&valid, (1, 2), (3, 9)).is_err());
        let mut wrong = valid.clone();
        wrong["unique_mount_id"] = json!(0);
        assert!(verify_final(&wrong, (1, 2), (3, 4)).is_err());
        let mut wrong = valid;
        wrong["flags"] = json!(["rw", "nosuid"]);
        assert!(verify_final(&wrong, (1, 2), (3, 4)).is_err());
    }
    #[test]
    fn native_container_has_only_workspace_and_drops_capabilities() {
        let cfg = NativeWorkspaceConfig {
            control_dir: "/var/native".into(),
            runtime: "/usr/bin/runc".into(),
            rootfs: "/var/rootfs".into(),
            idle_command: vec!["/bin/custom-idle".into(), "--sleep".into()],
            identity_command: vec!["/bin/custom-observer".into(), "--json".into()],
            workload_uid: 501,
            workload_gid: 501,
        };
        let spec = container_spec(
            &cfg,
            Path::new("/var/native/bundle/rootfs"),
            Path::new("/afs/root"),
        );
        assert_eq!(spec["root"]["readonly"], true);
        assert_eq!(spec["root"]["path"], "/var/native/bundle/rootfs");
        assert_ne!(spec["root"]["path"], cfg.rootfs.to_string_lossy().as_ref());
        assert_eq!(
            spec["process"]["args"],
            json!(["/bin/custom-idle", "--sleep"])
        );
        assert_eq!(
            identity_runtime_args("container-a", &cfg.identity_command),
            vec![
                "exec".to_owned(),
                "container-a".to_owned(),
                "/bin/custom-observer".to_owned(),
                "--json".to_owned()
            ]
        );
        assert_eq!(spec["process"]["noNewPrivileges"], true);
        assert_eq!(spec["process"]["capabilities"]["permitted"], json!([]));
        let mounts = spec["mounts"].as_array().unwrap();
        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[1]["destination"], "/workspace");
        assert_eq!(mounts[1]["source"], "/afs/root");
        assert_eq!(spec["linux"]["namespaces"].as_array().unwrap().len(), 6);
    }

    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires root Linux trusted rootfs fixture; run physical-native gate"]
    fn runtime_rootfs_copy_keeps_template_reusable_after_mutable_dev_artifacts() {
        require_root().unwrap();
        let temp = root_owned_tempdir();
        let template = temp.path().join("template");
        rootfs_template(&template);
        let before = tree_fingerprint(&template);
        let first = temp.path().join("bundle-a/rootfs");
        fs::create_dir(temp.path().join("bundle-a")).unwrap();
        fs::set_permissions(
            temp.path().join("bundle-a"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        prepare_runtime_rootfs(&template, &first).unwrap();
        let template_probe = fs::metadata(template.join("bin/view-observer")).unwrap();
        let first_probe = fs::metadata(first.join("bin/view-observer")).unwrap();
        assert_ne!(
            (template_probe.dev(), template_probe.ino()),
            (first_probe.dev(), first_probe.ino())
        );
        symlink("pts/ptmx", first.join("dev/ptmx")).unwrap();
        create_char_device(&first.join("dev/null"));
        assert_eq!(tree_fingerprint(&template), before);

        let second = temp.path().join("bundle-b/rootfs");
        fs::create_dir(temp.path().join("bundle-b")).unwrap();
        fs::set_permissions(
            temp.path().join("bundle-b"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        prepare_runtime_rootfs(&template, &second).unwrap();
        assert!(!second.join("dev/ptmx").exists());
        assert!(!second.join("dev/null").exists());
        trusted_tree(&template).unwrap();
        trusted_tree(&second).unwrap();
    }

    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires root Linux trusted rootfs fixture; run physical-native gate"]
    fn runtime_rootfs_copy_rejects_untrusted_source_and_destination_collision() {
        require_root().unwrap();
        let temp = root_owned_tempdir();
        let template = temp.path().join("template");
        rootfs_template(&template);
        symlink("/etc/passwd", template.join("bad-link")).unwrap();
        fs::create_dir(temp.path().join("bundle-a")).unwrap();
        fs::set_permissions(
            temp.path().join("bundle-a"),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        assert!(prepare_runtime_rootfs(&template, &temp.path().join("bundle-a/rootfs")).is_err());

        fs::remove_file(template.join("bad-link")).unwrap();
        let destination = temp.path().join("bundle-a/rootfs");
        fs::create_dir(&destination).unwrap();
        assert!(matches!(
            prepare_runtime_rootfs(&template, &destination),
            Err(error) if error.raw_os_error() == Some(libc::EEXIST)
        ));
    }

    #[test]
    #[cfg(target_os = "linux")]
    #[ignore = "requires root Linux trusted rootfs fixture; run physical-native gate"]
    fn runtime_rootfs_workspace_is_the_oci_root_covered_directory() {
        require_root().unwrap();
        let temp = root_owned_tempdir();
        let template = temp.path().join("template");
        rootfs_template(&template);
        let bundle = temp.path().join("bundle");
        fs::create_dir(&bundle).unwrap();
        fs::set_permissions(&bundle, fs::Permissions::from_mode(0o700)).unwrap();
        let runtime_rootfs = bundle.join("rootfs");
        prepare_runtime_rootfs(&template, &runtime_rootfs).unwrap();
        let cfg = NativeWorkspaceConfig {
            control_dir: temp.path().into(),
            runtime: "/usr/bin/runc".into(),
            rootfs: template,
            idle_command: vec!["/bin/view-observer".into(), "idle".into()],
            identity_command: vec!["/bin/view-observer".into(), "identity".into()],
            workload_uid: 501,
            workload_gid: 501,
        };
        let spec = container_spec(&cfg, &runtime_rootfs, Path::new("/afs/root"));
        assert_eq!(
            Path::new(spec["root"]["path"].as_str().unwrap()),
            runtime_rootfs
        );
        let covered = fs::metadata(runtime_rootfs.join("workspace")).unwrap();
        let template_covered = fs::metadata(cfg.rootfs.join("workspace")).unwrap();
        assert_ne!(
            (covered.dev(), covered.ino()),
            (template_covered.dev(), template_covered.ino())
        );
    }
    #[test]
    fn native_command_cancellation_retains_failure_receipt_and_reaps_child() {
        let temp = tempfile::tempdir().unwrap();
        let stem = temp.path().join("cancel");
        let stop = AtomicBool::new(true);
        let mut command = Command::new("/bin/sleep");
        command.arg("5");
        assert!(run_recorded(&mut command, &stem, Duration::from_secs(1), Some(&stop)).is_err());
        let receipt: Value =
            serde_json::from_slice(&fs::read(stem.with_extension("exit.json")).unwrap()).unwrap();
        assert_eq!(receipt["success"], false);
        assert!(receipt["reason"].is_string());
    }
}
