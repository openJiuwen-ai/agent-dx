use super::child_env;
use super::codec::map_value;
use rmpv::Value;
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) const CONFIG_ENV: &str = "ADX_IMAGE_PROCESS_CONFIG";
const CONFIG_VERSION: u32 = 1;
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;
const STDERR_TAIL_BYTES: usize = 64 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessSpec {
    version: u32,
    args: Vec<String>,
    cwd: String,
    user: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ExitInfo {
    #[serde(rename = "type")]
    kind: &'static str,
    pid: u32,
    status_kind: &'static str,
    exit_code: Option<i32>,
    signal: Option<i32>,
    signal_name: Option<&'static str>,
    shell_exit_code: i32,
    core_dumped: bool,
    started_at: u64,
    exited_at: u64,
    runtime_ms: u64,
    stderr_tail: String,
    message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub(crate) enum FailureCode {
    // Keep the existing HTTP entrypoint error values without protobuf types.
    InvalidConfiguration = 1001,
    ProcessFailed = 2002,
}

#[derive(Clone, Debug)]
pub(crate) struct Failure {
    pub code: FailureCode,
    pub message: String,
}

enum State {
    Disabled,
    Failed(Failure),
    Running {
        pid: u32,
        started_at: u64,
        started: Instant,
        child: Arc<Mutex<std::process::Child>>,
        stderr_tail: Arc<Mutex<Vec<u8>>>,
    },
    Exited(ExitInfo),
}

struct Inner {
    state: State,
    create_completed: bool,
}

struct Manager {
    inner: Mutex<Inner>,
    changed: Condvar,
}

static MANAGER: OnceLock<Arc<Manager>> = OnceLock::new();
static INITIALIZED: OnceLock<()> = OnceLock::new();

fn global() -> &'static Arc<Manager> {
    MANAGER.get_or_init(|| Arc::new(Manager::disabled()))
}

impl Manager {
    fn disabled() -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: State::Disabled,
                create_completed: false,
            }),
            changed: Condvar::new(),
        }
    }

    fn fail(&self, code: FailureCode, message: impl Into<String>) {
        self.inner.lock().expect("entrypoint state poisoned").state = State::Failed(Failure {
            code,
            message: message.into(),
        });
        self.changed.notify_all();
    }

    fn complete_create(&self) -> Result<(), Failure> {
        let mut inner = self.inner.lock().expect("entrypoint state poisoned");
        if inner.create_completed {
            return Ok(());
        }
        match &inner.state {
            State::Disabled => {
                inner.create_completed = true;
                Ok(())
            }
            State::Running {
                pid,
                started_at,
                started,
                child,
                stderr_tail,
            } => {
                let status = child
                    .lock()
                    .expect("entrypoint child poisoned")
                    .try_wait()
                    .map_err(|error| Failure {
                        code: FailureCode::ProcessFailed,
                        message: format!("failed to inspect image entrypoint: {error}"),
                    })?;
                let Some(status) = status else {
                    inner.create_completed = true;
                    return Ok(());
                };
                let exited_at = unix_millis();
                let stderr_tail = String::from_utf8_lossy(
                    &stderr_tail.lock().expect("entrypoint stderr poisoned"),
                )
                .into_owned();
                let info = exit_info(
                    *pid,
                    status,
                    *started_at,
                    exited_at,
                    started.elapsed().as_millis() as u64,
                    stderr_tail,
                );
                Err(Failure {
                    code: FailureCode::ProcessFailed,
                    message: exit_json(&info),
                })
            }
            State::Failed(failure) => Err(failure.clone()),
            State::Exited(info) => Err(Failure {
                code: FailureCode::ProcessFailed,
                message: exit_json(info),
            }),
        }
    }

    fn poll(&self, timeout: Duration) -> Value {
        let inner = self.inner.lock().expect("entrypoint state poisoned");
        let (inner, _) = self
            .changed
            .wait_timeout_while(inner, timeout, |inner| {
                matches!(&inner.state, State::Running { .. })
            })
            .expect("entrypoint state poisoned");
        match &inner.state {
            State::Running {
                pid, started_at, ..
            } => map_value(vec![
                ("status", Value::from("running")),
                ("pid", Value::from(*pid as i64)),
                ("started_at", Value::from(*started_at as i64)),
            ]),
            State::Exited(info) => exit_value(info),
            State::Failed(failure) => map_value(vec![
                ("status", Value::from("error")),
                ("code", Value::from(failure.code as i64)),
                ("message", Value::from(failure.message.clone())),
            ]),
            State::Disabled => map_value(vec![
                ("status", Value::from("error")),
                (
                    "message",
                    Value::from("inherit_entrypoint is not enabled for this sandbox"),
                ),
            ]),
        }
    }
}

pub(crate) fn initialize() {
    if INITIALIZED.set(()).is_err() {
        return;
    }
    let manager = global().clone();
    start_from_environment(manager.clone());
}

fn start_from_environment(manager: Arc<Manager>) {
    let Some(raw_path) = std::env::var_os(CONFIG_ENV).filter(|value| !value.is_empty()) else {
        return;
    };
    std::env::remove_var(CONFIG_ENV);
    let path = PathBuf::from(raw_path);
    let spec = match read_spec(&path) {
        Ok(spec) => spec,
        Err(message) => {
            manager.fail(FailureCode::InvalidConfiguration, message);
            return;
        }
    };

    let identity = match resolve_identity(&spec.user) {
        Ok(identity) => identity,
        Err(message) => {
            manager.fail(FailureCode::InvalidConfiguration, message);
            return;
        }
    };
    let stderr_tail = Arc::new(Mutex::new(Vec::new()));
    let mut command = Command::new(&spec.args[0]);
    command
        .args(&spec.args[1..])
        .current_dir(&spec.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    child_env::apply(&mut command);
    if let Some(identity) = identity {
        // SAFETY: the closure runs in the child immediately before exec, captures owned identity
        // data whose pointers stay valid for the call, performs no access to parent-only state,
        // and converts every syscall failure into an aborted exec.
        unsafe {
            command.pre_exec(move || {
                let group_result = if let Some(user_name) = &identity.user_name {
                    libc::initgroups(user_name.as_ptr(), identity.gid as _)
                } else {
                    libc::setgroups(0, std::ptr::null())
                };
                if group_result != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::setgid(identity.gid) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::setuid(identity.uid) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            manager.fail(
                FailureCode::ProcessFailed,
                format!("failed to spawn image entrypoint: {error}"),
            );
            return;
        }
    };
    let pid = child.id();
    let started_at = unix_millis();
    let started = Instant::now();
    spawn_tee_reader(child.stdout.take(), false, None);
    spawn_tee_reader(child.stderr.take(), true, Some(stderr_tail.clone()));
    let child = Arc::new(Mutex::new(child));
    manager
        .inner
        .lock()
        .expect("entrypoint state poisoned")
        .state = State::Running {
        pid,
        started_at,
        started,
        child: child.clone(),
        stderr_tail: stderr_tail.clone(),
    };
    std::thread::spawn(move || {
        let status = loop {
            match child.lock().expect("entrypoint child poisoned").try_wait() {
                Ok(Some(status)) => break Ok(status),
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(error) => break Err(error),
            }
        };
        let exited_at = unix_millis();
        let (started_at, runtime_ms) = {
            let inner = manager.inner.lock().expect("entrypoint state poisoned");
            match &inner.state {
                State::Running {
                    started_at,
                    started,
                    ..
                } => (*started_at, started.elapsed().as_millis() as u64),
                _ => (exited_at, 0),
            }
        };
        let stderr_tail =
            String::from_utf8_lossy(&stderr_tail.lock().expect("entrypoint stderr poisoned"))
                .into_owned();
        let info = match status {
            Ok(status) => exit_info(pid, status, started_at, exited_at, runtime_ms, stderr_tail),
            Err(error) => ExitInfo {
                kind: "entrypoint_exited",
                pid,
                status_kind: "wait_failed",
                exit_code: None,
                signal: None,
                signal_name: None,
                shell_exit_code: -1,
                core_dumped: false,
                started_at,
                exited_at,
                runtime_ms,
                stderr_tail,
                message: format!("failed to wait for image entrypoint: {error}"),
            },
        };
        manager
            .inner
            .lock()
            .expect("entrypoint state poisoned")
            .state = State::Exited(info);
        manager.changed.notify_all();
    });
}

fn read_spec(path: &Path) -> Result<ProcessSpec, String> {
    validate_canonical_absolute_path(path, "image process config")?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| {
            format!(
                "failed to open image process config {}: {error}",
                path.display()
            )
        })?;
    let metadata = file.metadata().map_err(|error| {
        format!(
            "failed to stat image process config {}: {error}",
            path.display()
        )
    })?;
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES {
        return Err(format!(
            "image process config {} must be a regular file no larger than {} bytes",
            path.display(),
            MAX_CONFIG_BYTES
        ));
    }
    let mut content = String::new();
    file.read_to_string(&mut content).map_err(|error| {
        format!(
            "failed to read image process config {}: {error}",
            path.display()
        )
    })?;
    let spec: ProcessSpec = serde_json::from_str(&content)
        .map_err(|error| format!("invalid image process config {}: {error}", path.display()))?;
    if spec.version != CONFIG_VERSION {
        return Err(format!(
            "unsupported image process config version {}",
            spec.version
        ));
    }
    if spec.args.is_empty() {
        return Err("image has no startup command".to_string());
    }
    if spec.args[0].is_empty() {
        return Err("image process argv[0] is empty".to_string());
    }
    if spec.args.iter().any(|arg| arg.contains('\0')) {
        return Err("image process argument contains NUL".to_string());
    }
    validate_canonical_absolute_path(Path::new(&spec.cwd), "image working directory")?;
    if spec.user.contains('\0') {
        return Err("image user contains NUL".to_string());
    }
    Ok(spec)
}

fn validate_canonical_absolute_path(path: &Path, label: &str) -> Result<(), String> {
    if !path.is_absolute()
        || (label == "image process config" && path == Path::new("/"))
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(format!("{label} must be an absolute canonical path"));
    }
    Ok(())
}

struct Identity {
    uid: libc::uid_t,
    gid: libc::gid_t,
    user_name: Option<CString>,
}

fn resolve_identity(user: &str) -> Result<Option<Identity>, String> {
    if user.is_empty() {
        return Ok(None);
    }
    let (user_part, group_part) = user.split_once(':').unwrap_or((user, ""));
    if user_part.is_empty() {
        return Err("image user must not have an empty user component".to_string());
    }
    let (uid, default_gid, user_name) = resolve_user(user_part)?;
    let gid = if group_part.is_empty() {
        default_gid
    } else {
        resolve_group(group_part)?
    };
    Ok(Some(Identity {
        uid,
        gid,
        user_name,
    }))
}

fn resolve_user(user: &str) -> Result<(libc::uid_t, libc::gid_t, Option<CString>), String> {
    if let Ok(uid) = user.parse::<libc::uid_t>() {
        return Ok((uid, lookup_uid_gid(uid).unwrap_or(0), None));
    }
    let name = CString::new(user).map_err(|_| "image user contains NUL".to_string())?;
    // SAFETY: libc::passwd is a C output structure for which an all-zero value is a valid
    // initialized placeholder before getpwnam_r fills its fields.
    let mut pwd = unsafe { std::mem::zeroed::<libc::passwd>() };
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; 16 * 1024];
    // SAFETY: name is NUL terminated, pwd and result are writable, and buffer remains allocated
    // for the call. No returned pointer is used after buffer is dropped.
    let rc = unsafe {
        libc::getpwnam_r(
            name.as_ptr(),
            &mut pwd,
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if rc != 0 || result.is_null() {
        return Err(format!("image user {user:?} was not found"));
    }
    Ok((pwd.pw_uid, pwd.pw_gid, Some(name)))
}

fn lookup_uid_gid(uid: libc::uid_t) -> Option<libc::gid_t> {
    // SAFETY: libc::passwd is a C output structure for which an all-zero value is a valid
    // initialized placeholder before getpwuid_r fills its fields.
    let mut pwd = unsafe { std::mem::zeroed::<libc::passwd>() };
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; 16 * 1024];
    // SAFETY: pwd and result are writable, and buffer remains allocated for the call. No returned
    // pointer is used after buffer is dropped.
    let rc = unsafe {
        libc::getpwuid_r(
            uid,
            &mut pwd,
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    (rc == 0 && !result.is_null()).then_some(pwd.pw_gid)
}

fn resolve_group(group: &str) -> Result<libc::gid_t, String> {
    if let Ok(gid) = group.parse::<libc::gid_t>() {
        return Ok(gid);
    }
    let name = CString::new(group).map_err(|_| "image group contains NUL".to_string())?;
    // SAFETY: libc::group is a C output structure for which an all-zero value is a valid
    // initialized placeholder before getgrnam_r fills its fields.
    let mut grp = unsafe { std::mem::zeroed::<libc::group>() };
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; 16 * 1024];
    // SAFETY: name is NUL terminated, grp and result are writable, and buffer remains allocated
    // for the call. No returned pointer is used after buffer is dropped.
    let rc = unsafe {
        libc::getgrnam_r(
            name.as_ptr(),
            &mut grp,
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if rc != 0 || result.is_null() {
        return Err(format!("image group {group:?} was not found"));
    }
    Ok(grp.gr_gid)
}

fn spawn_tee_reader<R>(stream: Option<R>, stderr: bool, tail: Option<Arc<Mutex<Vec<u8>>>>)
where
    R: Read + Send + 'static,
{
    let Some(mut stream) = stream else { return };
    std::thread::spawn(move || {
        let mut buffer = [0u8; 4096];
        while let Ok(count) = stream.read(&mut buffer) {
            if count == 0 {
                break;
            }
            if stderr {
                let _ = std::io::stderr().write_all(&buffer[..count]);
            } else {
                let _ = std::io::stdout().write_all(&buffer[..count]);
            }
            if let Some(tail) = &tail {
                let mut tail = tail.lock().expect("entrypoint stderr poisoned");
                tail.extend_from_slice(&buffer[..count]);
                if tail.len() > STDERR_TAIL_BYTES {
                    let drain = tail.len() - STDERR_TAIL_BYTES;
                    tail.drain(..drain);
                }
            }
        }
    });
}

fn exit_info(
    pid: u32,
    status: std::process::ExitStatus,
    started_at: u64,
    exited_at: u64,
    runtime_ms: u64,
    stderr_tail: String,
) -> ExitInfo {
    let exit_code = status.code();
    let signal = status.signal();
    let shell_exit_code =
        exit_code.unwrap_or_else(|| signal.map(|signal| 128 + signal).unwrap_or(-1));
    let status_kind = if exit_code.is_some() {
        "exited"
    } else {
        "signaled"
    };
    let signal_name = signal.and_then(signal_name);
    let message = match (exit_code, signal, signal_name) {
        (Some(code), _, _) => format!("entrypoint exited with code {code}"),
        (_, Some(signal), Some(name)) => {
            format!("entrypoint exited after signal {name} ({signal})")
        }
        (_, Some(signal), None) => format!("entrypoint exited after signal {signal}"),
        _ => "entrypoint exited without a terminal status".to_string(),
    };
    ExitInfo {
        kind: "entrypoint_exited",
        pid,
        status_kind,
        exit_code,
        signal,
        signal_name,
        shell_exit_code,
        core_dumped: status.core_dumped(),
        started_at,
        exited_at,
        runtime_ms,
        stderr_tail,
        message,
    }
}

fn signal_name(signal: i32) -> Option<&'static str> {
    match signal {
        libc::SIGHUP => Some("SIGHUP"),
        libc::SIGINT => Some("SIGINT"),
        libc::SIGQUIT => Some("SIGQUIT"),
        libc::SIGILL => Some("SIGILL"),
        libc::SIGABRT => Some("SIGABRT"),
        libc::SIGFPE => Some("SIGFPE"),
        libc::SIGKILL => Some("SIGKILL"),
        libc::SIGSEGV => Some("SIGSEGV"),
        libc::SIGPIPE => Some("SIGPIPE"),
        libc::SIGALRM => Some("SIGALRM"),
        libc::SIGTERM => Some("SIGTERM"),
        _ => None,
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn exit_json(info: &ExitInfo) -> String {
    serde_json::to_string(info).unwrap_or_else(|_| info.message.clone())
}

fn exit_value(info: &ExitInfo) -> Value {
    map_value(vec![
        ("status", Value::from("exited")),
        ("type", Value::from(info.kind)),
        ("pid", Value::from(info.pid as i64)),
        ("status_kind", Value::from(info.status_kind)),
        (
            "exit_code",
            info.exit_code.map(Value::from).unwrap_or(Value::Nil),
        ),
        ("signal", info.signal.map(Value::from).unwrap_or(Value::Nil)),
        (
            "signal_name",
            info.signal_name.map(Value::from).unwrap_or(Value::Nil),
        ),
        ("shell_exit_code", Value::from(info.shell_exit_code)),
        ("core_dumped", Value::from(info.core_dumped)),
        ("started_at", Value::from(info.started_at as i64)),
        ("exited_at", Value::from(info.exited_at as i64)),
        ("runtime_ms", Value::from(info.runtime_ms as i64)),
        ("stderr_tail", Value::from(info.stderr_tail.clone())),
        ("message", Value::from(info.message.clone())),
    ])
}

pub(crate) fn complete_create() -> Result<(), Failure> {
    global().complete_create()
}

pub(crate) fn poll(wait_timeout: f64) -> Value {
    let wait_timeout = if wait_timeout.is_finite() {
        wait_timeout.clamp(0.0, 30.0)
    } else {
        10.0
    };
    global().poll(Duration::from_secs_f64(wait_timeout))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_effective_argv_matches_docker_create_failure() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("image-process.json");
        std::fs::write(&path, r#"{"version":1,"args":[],"cwd":"/","user":""}"#)
            .expect("write spec");
        assert_eq!(
            read_spec(&path).unwrap_err(),
            "image has no startup command"
        );
    }

    #[test]
    fn parses_pr44_process_spec_contract() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("image-process.json");
        std::fs::write(
            &path,
            r#"{"version":1,"args":["/bin/echo","ok"],"cwd":"/","user":""}"#,
        )
        .expect("write spec");
        let spec = read_spec(&path).expect("valid PR 44 process spec");
        assert_eq!(spec.args, ["/bin/echo", "ok"]);
        assert_eq!(spec.cwd, "/");
    }

    #[test]
    fn preserves_empty_nonzero_argv_elements() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("image-process.json");
        std::fs::write(
            &path,
            r#"{"version":1,"args":["/bin/echo",""],"cwd":"/","user":""}"#,
        )
        .expect("write spec");
        let spec = read_spec(&path).expect("empty argument is valid after argv[0]");
        assert_eq!(spec.args[1], "");
    }

    #[test]
    fn resolves_numeric_and_named_image_users() {
        let numeric = resolve_identity("1234:5678")
            .expect("numeric identity")
            .expect("configured identity");
        assert_eq!(numeric.uid, 1234);
        assert_eq!(numeric.gid, 5678);
        assert!(numeric.user_name.is_none());

        let named = resolve_identity("root")
            .expect("root identity")
            .expect("configured identity");
        assert_eq!(named.uid, 0);
        assert!(named.user_name.is_some());
    }

    #[test]
    fn complete_create_fails_if_entrypoint_already_exited() {
        let manager = Manager::disabled();
        manager.inner.lock().unwrap().state = State::Exited(ExitInfo {
            kind: "entrypoint_exited",
            pid: 7,
            status_kind: "exited",
            exit_code: Some(0),
            signal: None,
            signal_name: None,
            shell_exit_code: 0,
            core_dumped: false,
            started_at: 1,
            exited_at: 2,
            runtime_ms: 1,
            stderr_tail: String::new(),
            message: "entrypoint exited with code 0".to_string(),
        });
        let error = manager.complete_create().expect_err("create must fail");
        assert_eq!(error.code, FailureCode::ProcessFailed);
    }

    #[test]
    fn completed_create_remains_successful_after_entrypoint_exit() {
        let manager = Manager::disabled();
        {
            let mut inner = manager.inner.lock().unwrap();
            inner.create_completed = true;
            inner.state = State::Exited(ExitInfo {
                kind: "entrypoint_exited",
                pid: 7,
                status_kind: "exited",
                exit_code: Some(1),
                signal: None,
                signal_name: None,
                shell_exit_code: 1,
                core_dumped: false,
                started_at: 1,
                exited_at: 2,
                runtime_ms: 1,
                stderr_tail: String::new(),
                message: "entrypoint exited with code 1".to_string(),
            });
        }
        assert!(manager.complete_create().is_ok());
    }
}
