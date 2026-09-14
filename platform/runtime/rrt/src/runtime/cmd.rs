//! Durable-for-the-sandbox command registry.
//!
//! `command_id` is generated before submission and is the authoritative
//! identity. Legacy callers may omit it and continue using the OS pid.

use super::codec::{kw_str, map_value};
use rmpv::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_OUTPUT_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_RESULT_TTL_SECS: u64 = 3600;
const DEFAULT_REGISTRY_MAX_RECORDS: usize = 4096;
const DEFAULT_REGISTRY_MAX_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_REGISTRY_HIGH_WATERMARK_BYTES: usize = 192 * 1024 * 1024;
static RETAINED_OUTPUT_BYTES: AtomicUsize = AtomicUsize::new(0);
static COMMAND_COMPLETED: AtomicU64 = AtomicU64::new(0);
static COMMAND_RESULT_TRUNCATED: AtomicU64 = AtomicU64::new(0);
static COMMAND_RECORD_EXPIRED: AtomicU64 = AtomicU64::new(0);
static COMMAND_ID_CONFLICT: AtomicU64 = AtomicU64::new(0);

struct ExitState {
    // Serializes child observation/reaping with signals so a completed PID can
    // never be signalled after the OS has made it reusable.
    signal_guard: Mutex<()>,
    code: Mutex<Option<i64>>,
    finished_at_ms: Mutex<Option<u64>>,
    killed: AtomicBool,
    timed_out: AtomicBool,
    spawn_error: Mutex<Option<String>>,
    state_version: AtomicU64,
    cond: Condvar,
}

struct BoundedBuffer {
    data: Vec<u8>,
    total_bytes: u64,
    truncated: bool,
    limit: usize,
}

impl BoundedBuffer {
    fn append(&mut self, bytes: &[u8]) {
        let stream_take = self.limit.saturating_sub(self.data.len()).min(bytes.len());
        let global_limit = env_usize("RRT_COMMAND_REGISTRY_MAX_BYTES", DEFAULT_REGISTRY_MAX_BYTES);
        let mut take = 0;
        if stream_take > 0 {
            let _ =
                RETAINED_OUTPUT_BYTES.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    take = stream_take.min(global_limit.saturating_sub(used));
                    (take > 0).then_some(used + take)
                });
        }
        self.data.extend_from_slice(&bytes[..take]);
        self.total_bytes += bytes.len() as u64;
        if take != bytes.len() && !self.truncated {
            COMMAND_RESULT_TRUNCATED.fetch_add(1, Ordering::Relaxed);
        }
        self.truncated |= take != bytes.len();
    }
}

impl Drop for BoundedBuffer {
    fn drop(&mut self) {
        RETAINED_OUTPUT_BYTES.fetch_sub(self.data.len(), Ordering::AcqRel);
    }
}

#[derive(Clone)]
struct Reader {
    buf: Arc<Mutex<BoundedBuffer>>,
    finished: Arc<AtomicBool>,
}

struct CommandRecord {
    command_id: String,
    request_fingerprint: String,
    pid: i64,
    cmd: String,
    created_at_ms: u64,
    started_at_ms: Option<u64>,
    stdin_enabled: bool,
    stdin_closed: bool,
    stdin: Option<std::process::ChildStdin>,
    stdout: Reader,
    stderr: Reader,
    exit: Arc<ExitState>,
}

struct CommandSnapshot {
    command_id: String,
    pid: i64,
    cmd: String,
    created_at_ms: u64,
    started_at_ms: Option<u64>,
    stdout: Reader,
    stderr: Reader,
    exit: Arc<ExitState>,
}

impl From<&CommandRecord> for CommandSnapshot {
    fn from(record: &CommandRecord) -> Self {
        Self {
            command_id: record.command_id.clone(),
            pid: record.pid,
            cmd: record.cmd.clone(),
            created_at_ms: record.created_at_ms,
            started_at_ms: record.started_at_ms,
            stdout: record.stdout.clone(),
            stderr: record.stderr.clone(),
            exit: Arc::clone(&record.exit),
        }
    }
}

fn commands() -> &'static Mutex<HashMap<String, CommandRecord>> {
    static COMMANDS: OnceLock<Mutex<HashMap<String, CommandRecord>>> = OnceLock::new();
    COMMANDS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn cleanup_expired(records: &mut HashMap<String, CommandRecord>) {
    let now = now_ms();
    let ttl_ms = env_u64("RRT_COMMAND_RESULT_TTL_SECS", DEFAULT_RESULT_TTL_SECS) * 1000;
    let before = records.len();
    records.retain(|_, record| {
        record
            .exit
            .finished_at_ms
            .lock()
            .unwrap()
            .map(|finished| now.saturating_sub(finished) < ttl_ms)
            .unwrap_or(true)
    });
    COMMAND_RECORD_EXPIRED.fetch_add((before - records.len()) as u64, Ordering::Relaxed);
}

fn output_limit(name: &str) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| env_usize("RRT_COMMAND_OUTPUT_LIMIT_BYTES", DEFAULT_OUTPUT_LIMIT_BYTES))
}

fn spawn_reader(stream: Option<impl Read + Send + 'static>, limit_name: &str) -> Reader {
    let limit = output_limit(limit_name);
    let buf = Arc::new(Mutex::new(BoundedBuffer {
        data: Vec::new(),
        total_bytes: 0,
        truncated: false,
        limit,
    }));
    let finished = Arc::new(AtomicBool::new(false));
    if let Some(mut stream) = stream {
        let output = Arc::clone(&buf);
        let done = Arc::clone(&finished);
        std::thread::spawn(move || {
            let mut chunk = [0u8; 8192];
            loop {
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(size) => output.lock().unwrap().append(&chunk[..size]),
                }
            }
            done.store(true, Ordering::Release);
        });
    } else {
        finished.store(true, Ordering::Release);
    }
    Reader { buf, finished }
}

fn empty_reader(limit_name: &str) -> Reader {
    let limit = output_limit(limit_name);
    Reader {
        buf: Arc::new(Mutex::new(BoundedBuffer {
            data: Vec::new(),
            total_bytes: 0,
            truncated: false,
            limit,
        })),
        finished: Arc::new(AtomicBool::new(true)),
    }
}

fn make_exit_state() -> Arc<ExitState> {
    Arc::new(ExitState {
        signal_guard: Mutex::new(()),
        code: Mutex::new(None),
        finished_at_ms: Mutex::new(None),
        killed: AtomicBool::new(false),
        timed_out: AtomicBool::new(false),
        spawn_error: Mutex::new(None),
        state_version: AtomicU64::new(1),
        cond: Condvar::new(),
    })
}

fn make_room(records: &mut HashMap<String, CommandRecord>) -> bool {
    cleanup_expired(records);
    let max = env_usize(
        "RRT_COMMAND_REGISTRY_MAX_RECORDS",
        DEFAULT_REGISTRY_MAX_RECORDS,
    );
    let max_bytes = env_usize("RRT_COMMAND_REGISTRY_MAX_BYTES", DEFAULT_REGISTRY_MAX_BYTES);
    let high_watermark = env_usize(
        "RRT_COMMAND_REGISTRY_MEMORY_HIGH_WATERMARK_BYTES",
        DEFAULT_REGISTRY_HIGH_WATERMARK_BYTES,
    )
    .min(max_bytes);
    while records.len() >= max || registry_retained_bytes(records) >= high_watermark {
        let oldest_terminal = records
            .iter()
            .filter_map(|(id, record)| {
                record
                    .exit
                    .finished_at_ms
                    .lock()
                    .unwrap()
                    .map(|finished| (id.clone(), finished))
            })
            .min_by_key(|(_, finished)| *finished)
            .map(|(id, _)| id);
        match oldest_terminal {
            Some(id) => {
                records.remove(&id);
            }
            None => return false,
        }
    }
    true
}

fn registry_retained_bytes(records: &HashMap<String, CommandRecord>) -> usize {
    records
        .values()
        .map(|record| {
            record.command_id.len()
                + record.request_fingerprint.len()
                + record.cmd.len()
                + record.stdout.buf.lock().unwrap().data.len()
                + record.stderr.buf.lock().unwrap().data.len()
                + std::mem::size_of::<CommandRecord>()
        })
        .sum()
}

fn kw_i64(kw: &BTreeMap<String, Value>, key: &str) -> Option<i64> {
    kw.get(key).and_then(Value::as_i64)
}
fn kw_bool(kw: &BTreeMap<String, Value>, key: &str) -> Option<bool> {
    kw.get(key).and_then(Value::as_bool)
}
fn kw_f64(kw: &BTreeMap<String, Value>, key: &str) -> Option<f64> {
    kw.get(key)
        .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
}
fn nil() -> Value {
    Value::Nil
}

fn command_id(kw: &BTreeMap<String, Value>) -> Option<String> {
    kw_str(kw, "command_id").filter(|value| !value.is_empty())
}

fn valid_command_id(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b':' | b'-'))
}

fn request_fingerprint(kw: &BTreeMap<String, Value>) -> String {
    let cmd = kw_str(kw, "command")
        .or_else(|| kw_str(kw, "cmd"))
        .unwrap_or_default();
    let cwd = kw_str(kw, "cwd").unwrap_or_default();
    let want_stdin = kw_bool(kw, "want_stdin").unwrap_or(false);
    let envs = match kw.get("envs") {
        Some(Value::Map(items)) => {
            let mut pairs: Vec<(String, String)> = items
                .iter()
                .filter_map(|(k, v)| Some((k.as_str()?.to_owned(), v.as_str()?.to_owned())))
                .collect();
            pairs.sort();
            pairs
        }
        _ => Vec::new(),
    };
    let timeout_ms = kw_f64(kw, "timeout").map(|seconds| (seconds * 1000.0).round() as i64);
    let canonical = serde_json::json!({
        "command": cmd,
        "cwd": cwd,
        "envs": envs,
        "timeout_ms": timeout_ms,
        "want_stdin": want_stdin,
    });
    let mut hasher = Sha256::new();
    hasher.update(b"adx-command-request-v1\0");
    hasher.update(serde_json::to_vec(&canonical).expect("canonical command JSON"));
    format!("v1:{:x}", hasher.finalize())
}

fn resolve_key(
    records: &HashMap<String, CommandRecord>,
    kw: &BTreeMap<String, Value>,
) -> Option<String> {
    if let Some(id) = command_id(kw) {
        return records.contains_key(&id).then_some(id);
    }
    let pid = kw_i64(kw, "pid")?;
    records
        .iter()
        .find_map(|(id, record)| (record.pid == pid).then(|| id.clone()))
}

fn wait_exit(exit: &ExitState, timeout: Option<Duration>) -> Option<i64> {
    let mut code = exit.code.lock().unwrap();
    match timeout {
        None => {
            while code.is_none() && exit.spawn_error.lock().unwrap().is_none() {
                code = exit.cond.wait(code).unwrap();
            }
            Some(code.unwrap_or(-1))
        }
        Some(timeout) => {
            let deadline = Instant::now() + timeout;
            while code.is_none() && exit.spawn_error.lock().unwrap().is_none() {
                let now = Instant::now();
                if now >= deadline {
                    return None;
                }
                (code, _) = exit.cond.wait_timeout(code, deadline - now).unwrap();
            }
            Some(code.unwrap_or(-1))
        }
    }
}

fn collect_reader(reader: &Reader) -> (String, bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !reader.finished.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = reader.buf.lock().unwrap();
    (
        String::from_utf8_lossy(&output.data).into_owned(),
        output.truncated,
    )
}

fn command_status(pid: i64, exit: &ExitState) -> &'static str {
    if exit.spawn_error.lock().unwrap().is_some() {
        "FAILED"
    } else if pid < 0 {
        "PENDING"
    } else if exit.code.lock().unwrap().is_none() {
        "RUNNING"
    } else if exit.timed_out.load(Ordering::Acquire) {
        "TIMED_OUT"
    } else if exit.killed.load(Ordering::Acquire) {
        "KILLED"
    } else if *exit.code.lock().unwrap() == Some(0) {
        "SUCCEEDED"
    } else {
        "FAILED"
    }
}

fn status(record: &CommandRecord) -> &'static str {
    command_status(record.pid, &record.exit)
}

fn snapshot_status(record: &CommandSnapshot) -> &'static str {
    command_status(record.pid, &record.exit)
}

fn record_value(record: &CommandSnapshot, include_output: bool) -> Value {
    let spawn_error = record.exit.spawn_error.lock().unwrap().clone();
    let raw_code = *record.exit.code.lock().unwrap();
    let code = if spawn_error.is_some()
        || record.exit.killed.load(Ordering::Acquire)
        || record.exit.timed_out.load(Ordering::Acquire)
    {
        None
    } else {
        raw_code
    };
    let finished_at = *record.exit.finished_at_ms.lock().unwrap();
    let terminal = !matches!(snapshot_status(record), "PENDING" | "RUNNING");
    let (stdout, stdout_truncated, stderr, stderr_truncated) = if include_output && terminal {
        let (stdout, stdout_truncated) = collect_reader(&record.stdout);
        let (stderr, stderr_truncated) = collect_reader(&record.stderr);
        (stdout, stdout_truncated, stderr, stderr_truncated)
    } else {
        (String::new(), false, String::new(), false)
    };
    let stdout_buf = record.stdout.buf.lock().unwrap();
    let stdout_total_bytes = stdout_buf.total_bytes;
    let stdout_retained_bytes = stdout_buf.data.len() as u64;
    drop(stdout_buf);
    let stderr_buf = record.stderr.buf.lock().unwrap();
    let stderr_total_bytes = stderr_buf.total_bytes;
    let stderr_retained_bytes = stderr_buf.data.len() as u64;
    drop(stderr_buf);
    map_value(vec![
        ("command_id", Value::from(record.command_id.clone())),
        ("pid", Value::from(record.pid)),
        ("cmd", Value::from(record.cmd.clone())),
        ("status", Value::from(snapshot_status(record))),
        ("running", Value::from(!terminal)),
        (
            "state_version",
            Value::from(record.exit.state_version.load(Ordering::Acquire)),
        ),
        ("stdout", Value::from(stdout)),
        ("stderr", Value::from(stderr)),
        ("exit_code", code.map(Value::from).unwrap_or(Value::Nil)),
        ("created_at_ms", Value::from(record.created_at_ms)),
        (
            "started_at_ms",
            record.started_at_ms.map(Value::from).unwrap_or(Value::Nil),
        ),
        (
            "finished_at_ms",
            finished_at.map(Value::from).unwrap_or(Value::Nil),
        ),
        ("stdout_truncated", Value::from(stdout_truncated)),
        ("stderr_truncated", Value::from(stderr_truncated)),
        (
            "truncated",
            Value::from(stdout_truncated || stderr_truncated),
        ),
        ("stdout_total_bytes", Value::from(stdout_total_bytes)),
        ("stdout_retained_bytes", Value::from(stdout_retained_bytes)),
        ("stderr_total_bytes", Value::from(stderr_total_bytes)),
        ("stderr_retained_bytes", Value::from(stderr_retained_bytes)),
        (
            "error_code",
            spawn_error
                .as_ref()
                .map(|_| Value::from("SPAWN_FAILED"))
                .unwrap_or(Value::Nil),
        ),
        (
            "error_message",
            spawn_error.map(Value::from).unwrap_or(Value::Nil),
        ),
    ])
}

/// PID-based clients predate stable command identities and recognize only the
/// running/done/error states. Keep their result and signal-exit-code contract.
fn legacy_record_value(record: &CommandSnapshot, include_output: bool) -> Value {
    let mut value = record_value(record, include_output);
    let Value::Map(items) = &mut value else {
        unreachable!("command result is a map");
    };
    for (key, value) in items.iter_mut() {
        match key.as_str() {
            Some("status") => {
                *value = Value::from(match value.as_str() {
                    Some("PENDING" | "RUNNING") => "running",
                    _ if record.exit.spawn_error.lock().unwrap().is_some() => "error",
                    _ => "done",
                });
            }
            Some("exit_code") => {
                *value = record
                    .exit
                    .code
                    .lock()
                    .unwrap()
                    .map(Value::from)
                    .unwrap_or(nil());
            }
            _ => {}
        }
    }
    items.push((
        Value::from("error"),
        record
            .exit
            .spawn_error
            .lock()
            .unwrap()
            .clone()
            .map(Value::from)
            .unwrap_or(nil()),
    ));
    value
}

fn legacy_command_id(records: &HashMap<String, CommandRecord>) -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    loop {
        let id = format!(
            "legacy-{}-{}",
            now_ms(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        // User-supplied IDs share the registry; never deduplicate a legacy start.
        if !records.contains_key(&id) {
            return id;
        }
    }
}

fn not_found(kw: &BTreeMap<String, Value>) -> String {
    command_id(kw)
        .map(|id| format!("No command with id {id}"))
        .unwrap_or_else(|| format!("No process with pid {}", kw_i64(kw, "pid").unwrap_or(-1)))
}

/// Start a command; a supplied `command_id` makes submission idempotent.
pub fn cmd_start(kw: &BTreeMap<String, Value>) -> Value {
    let supplied_id = command_id(kw);
    if kw.contains_key("command_id") && !supplied_id.as_deref().is_some_and(valid_command_id) {
        return map_value(vec![
            ("command_id", Value::from(supplied_id.unwrap_or_default())),
            ("pid", Value::from(-1i64)),
            ("error_code", Value::from("INVALID_ARGUMENT")),
            ("error", Value::from("invalid command_id")),
        ]);
    }
    let legacy = supplied_id.is_none();
    let fingerprint = request_fingerprint(kw);
    let cmd = kw_str(kw, "command")
        .or_else(|| kw_str(kw, "cmd"))
        .unwrap_or_default();
    let cwd = kw_str(kw, "cwd");
    let want_stdin = kw_bool(kw, "want_stdin").unwrap_or(false);
    let mut records = commands().lock().unwrap();
    cleanup_expired(&mut records);
    let command_id = supplied_id.unwrap_or_else(|| legacy_command_id(&records));
    if let Some(existing) = records.get(&command_id) {
        if existing.request_fingerprint != fingerprint {
            COMMAND_ID_CONFLICT.fetch_add(1, Ordering::Relaxed);
            return map_value(vec![
                ("command_id", Value::from(command_id)),
                ("pid", Value::from(existing.pid)),
                ("error_code", Value::from("COMMAND_CONFLICT")),
                (
                    "error",
                    Value::from("command_id already exists with a different request"),
                ),
            ]);
        }
        return map_value(vec![
            ("command_id", Value::from(existing.command_id.clone())),
            ("pid", Value::from(existing.pid)),
            ("status", Value::from(status(existing))),
            ("error_code", nil()),
            ("error", nil()),
        ]);
    }
    if !make_room(&mut records) {
        return map_value(vec![
            ("command_id", Value::from(command_id)),
            ("pid", Value::from(-1i64)),
            ("error_code", Value::from("RESOURCE_EXHAUSTED")),
            ("error", Value::from("command registry is full")),
        ]);
    }
    let command_activity = super::activity::enter_command();

    // Reserve the stable identity before spawn. Holding the registry lock
    // serializes the PENDING -> RUNNING/FAILED transition with duplicate starts.
    let created_at_ms = now_ms();
    let exit = make_exit_state();
    records.insert(
        command_id.clone(),
        CommandRecord {
            command_id: command_id.clone(),
            request_fingerprint: fingerprint,
            pid: -1,
            cmd: cmd.clone(),
            created_at_ms,
            started_at_ms: None,
            stdin_enabled: want_stdin,
            stdin_closed: false,
            stdin: None,
            stdout: empty_reader("RRT_COMMAND_STDOUT_LIMIT_BYTES"),
            stderr: empty_reader("RRT_COMMAND_STDERR_LIMIT_BYTES"),
            exit: Arc::clone(&exit),
        },
    );

    let mut process = Command::new("/bin/sh");
    super::child_env::apply(&mut process);
    // One command owns one process group. Timeout/kill therefore terminates
    // the shell and descendants instead of leaving detached workloads alive.
    process.process_group(0);
    process
        .arg("-c")
        .arg(&cmd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    process.stdin(if want_stdin {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    if let Some(cwd) = &cwd {
        if !cwd.is_empty() {
            process.current_dir(cwd);
        }
    }
    if let Some(Value::Map(envs)) = kw.get("envs") {
        for (key, value) in envs {
            if let (Some(key), Some(value)) = (key.as_str(), value.as_str()) {
                if let Err(error) = super::child_env::validate_override(key) {
                    return map_value(vec![
                        ("command_id", Value::from(command_id.clone())),
                        ("pid", Value::from(-1i64)),
                        ("error_code", Value::from("INVALID_ARGUMENT")),
                        ("error", Value::from(error)),
                    ]);
                }
                process.env(key, value);
            }
        }
    }
    let mut child = match process.spawn() {
        Ok(child) => child,
        Err(error) => {
            *exit.spawn_error.lock().unwrap() = Some(error.to_string());
            *exit.finished_at_ms.lock().unwrap() = Some(now_ms());
            exit.state_version.fetch_add(1, Ordering::AcqRel);
            exit.cond.notify_all();
            COMMAND_COMPLETED.fetch_add(1, Ordering::Relaxed);
            drop(command_activity);
            let record = records.get(&command_id).expect("pending command exists");
            let snapshot = CommandSnapshot::from(record);
            drop(records);
            return if legacy {
                legacy_record_value(&snapshot, true)
            } else {
                record_value(&snapshot, true)
            };
        }
    };
    let pid = child.id() as i64;
    let stdin = child.stdin.take();
    let stdout = spawn_reader(child.stdout.take(), "RRT_COMMAND_STDOUT_LIMIT_BYTES");
    let stderr = spawn_reader(child.stderr.take(), "RRT_COMMAND_STDERR_LIMIT_BYTES");
    let waiter_exit = Arc::clone(&exit);
    let command_timeout = kw_f64(kw, "timeout")
        .filter(|value| *value > 0.0)
        .map(Duration::from_secs_f64);
    std::thread::spawn(move || {
        let _command_activity = command_activity;
        let deadline = command_timeout.map(|timeout| Instant::now() + timeout);
        loop {
            let signal_guard = waiter_exit.signal_guard.lock().unwrap();
            match child.try_wait() {
                Ok(Some(result)) => {
                    *waiter_exit.code.lock().unwrap() = Some(result.code().unwrap_or(-1) as i64);
                    *waiter_exit.finished_at_ms.lock().unwrap() = Some(now_ms());
                    waiter_exit.state_version.fetch_add(1, Ordering::AcqRel);
                    COMMAND_COMPLETED.fetch_add(1, Ordering::Relaxed);
                    waiter_exit.cond.notify_all();
                    break;
                }
                Ok(None) if deadline.is_some_and(|deadline| Instant::now() >= deadline) => {
                    waiter_exit.timed_out.store(true, Ordering::Release);
                    unsafe {
                        libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
                    }
                    let code = child
                        .wait()
                        .ok()
                        .and_then(|result| result.code())
                        .unwrap_or(-1) as i64;
                    *waiter_exit.code.lock().unwrap() = Some(code);
                    *waiter_exit.finished_at_ms.lock().unwrap() = Some(now_ms());
                    waiter_exit.state_version.fetch_add(1, Ordering::AcqRel);
                    COMMAND_COMPLETED.fetch_add(1, Ordering::Relaxed);
                    waiter_exit.cond.notify_all();
                    break;
                }
                Ok(None) => {
                    drop(signal_guard);
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => {
                    *waiter_exit.spawn_error.lock().unwrap() = Some(error.to_string());
                    *waiter_exit.code.lock().unwrap() = Some(-1);
                    *waiter_exit.finished_at_ms.lock().unwrap() = Some(now_ms());
                    waiter_exit.state_version.fetch_add(1, Ordering::AcqRel);
                    COMMAND_COMPLETED.fetch_add(1, Ordering::Relaxed);
                    waiter_exit.cond.notify_all();
                    break;
                }
            }
        }
    });
    let record = records
        .get_mut(&command_id)
        .expect("pending command exists");
    record.pid = pid;
    record.started_at_ms = Some(now_ms());
    record.stdin = stdin;
    record.stdout = stdout;
    record.stderr = stderr;
    exit.state_version.fetch_add(1, Ordering::AcqRel);

    map_value(vec![
        ("command_id", Value::from(command_id)),
        ("pid", Value::from(pid)),
        ("status", Value::from("RUNNING")),
        ("error_code", nil()),
        ("error", nil()),
    ])
}

pub fn cmd_get(kw: &BTreeMap<String, Value>) -> Value {
    let mut records = commands().lock().unwrap();
    cleanup_expired(&mut records);
    let snapshot = resolve_key(&records, kw)
        .and_then(|key| records.get(&key))
        .map(CommandSnapshot::from);
    drop(records);
    match snapshot {
        Some(record) if !kw.contains_key("command_id") => legacy_record_value(&record, true),
        Some(record) => record_value(&record, true),
        None => map_value(vec![
            (
                "status",
                Value::from(if kw.contains_key("command_id") {
                    "not_found"
                } else {
                    "error"
                }),
            ),
            ("error_code", Value::from("COMMAND_NOT_FOUND")),
            ("error", Value::from(not_found(kw))),
        ]),
    }
}

pub fn cmd_wait(kw: &BTreeMap<String, Value>) -> Value {
    let timeout = kw_f64(kw, "timeout").map(Duration::from_secs_f64);
    let exit = {
        let records = commands().lock().unwrap();
        match resolve_key(&records, kw)
            .and_then(|key| records.get(&key).map(|record| Arc::clone(&record.exit)))
        {
            Some(exit) => exit,
            None => {
                return map_value(vec![
                    (
                        "status",
                        Value::from(if kw.contains_key("command_id") {
                            "not_found"
                        } else {
                            "error"
                        }),
                    ),
                    ("error_code", Value::from("COMMAND_NOT_FOUND")),
                    ("error", Value::from(not_found(kw))),
                ])
            }
        }
    };
    if wait_exit(&exit, timeout).is_none() {
        return map_value(vec![
            ("status", Value::from("running")),
            ("error_code", Value::from("WAIT_TIMEOUT")),
            ("error", Value::from("command wait timed out")),
        ]);
    }
    cmd_get(kw)
}

pub fn cmd_poll(kw: &BTreeMap<String, Value>) -> Value {
    let wait_timeout = kw_f64(kw, "wait_timeout").unwrap_or(0.0).max(0.0);
    let exit = {
        let records = commands().lock().unwrap();
        match resolve_key(&records, kw)
            .and_then(|key| records.get(&key).map(|record| Arc::clone(&record.exit)))
        {
            Some(exit) => exit,
            None => {
                return map_value(vec![
                    (
                        "status",
                        Value::from(if kw.contains_key("command_id") {
                            "not_found"
                        } else {
                            "error"
                        }),
                    ),
                    ("error_code", Value::from("COMMAND_NOT_FOUND")),
                    ("error", Value::from(not_found(kw))),
                ])
            }
        }
    };
    let _ = wait_exit(&exit, Some(Duration::from_secs_f64(wait_timeout)));
    cmd_get(kw)
}

pub fn cmd_list(_kw: &BTreeMap<String, Value>) -> Value {
    let mut records = commands().lock().unwrap();
    cleanup_expired(&mut records);
    let snapshots = records
        .values()
        .map(CommandSnapshot::from)
        .collect::<Vec<_>>();
    drop(records);
    map_value(vec![(
        "processes",
        Value::Array(
            snapshots
                .iter()
                .map(|record| record_value(record, false))
                .collect(),
        ),
    )])
}

pub fn cmd_capabilities(_kw: &BTreeMap<String, Value>) -> Value {
    map_value(vec![
        ("protocol_version", Value::from(1u64)),
        (
            "capabilities",
            Value::Array(
                [
                    "stable-command-id",
                    "recoverable-command-result",
                    "multiplexed-command-watch",
                    "bounded-command-output",
                    "command-activity-lease",
                ]
                .into_iter()
                .map(Value::from)
                .collect(),
            ),
        ),
    ])
}

pub struct CommandMetricSnapshot {
    pub records: usize,
    pub running: usize,
    pub completed: u64,
    pub result_bytes: usize,
    pub result_truncated_total: u64,
    pub record_expired_total: u64,
    pub id_conflict_total: u64,
}

pub fn command_metrics() -> CommandMetricSnapshot {
    let mut records = commands().lock().unwrap();
    cleanup_expired(&mut records);
    CommandMetricSnapshot {
        records: records.len(),
        running: records
            .values()
            .filter(|record| matches!(status(record), "PENDING" | "RUNNING"))
            .count(),
        completed: COMMAND_COMPLETED.load(Ordering::Relaxed),
        result_bytes: RETAINED_OUTPUT_BYTES.load(Ordering::Relaxed),
        result_truncated_total: COMMAND_RESULT_TRUNCATED.load(Ordering::Relaxed),
        record_expired_total: COMMAND_RECORD_EXPIRED.load(Ordering::Relaxed),
        id_conflict_total: COMMAND_ID_CONFLICT.load(Ordering::Relaxed),
    }
}

pub fn watch_snapshot(command_ids: &[String]) -> Vec<Value> {
    let mut records = commands().lock().unwrap();
    cleanup_expired(&mut records);
    command_ids
        .iter()
        .map(|command_id| {
            records
                .get(command_id)
                .map(|record| {
                    map_value(vec![
                        ("command_id", Value::from(command_id.clone())),
                        ("status", Value::from(status(record))),
                        (
                            "state_version",
                            Value::from(record.exit.state_version.load(Ordering::Acquire)),
                        ),
                    ])
                })
                .unwrap_or_else(|| {
                    map_value(vec![
                        ("command_id", Value::from(command_id.clone())),
                        ("status", Value::from("NOT_FOUND")),
                        ("state_version", Value::from(0u64)),
                    ])
                })
        })
        .collect()
}

pub fn cmd_kill(kw: &BTreeMap<String, Value>) -> Value {
    let (pid, exit) = {
        let records = commands().lock().unwrap();
        match resolve_key(&records, kw).and_then(|key| {
            records
                .get(&key)
                .map(|record| (record.pid, Arc::clone(&record.exit)))
        }) {
            Some(value) => value,
            None => {
                return map_value(vec![
                    ("killed", Value::from(false)),
                    ("error_code", Value::from("COMMAND_NOT_FOUND")),
                    ("error", Value::from(not_found(kw))),
                ])
            }
        }
    };
    let _signal_guard = exit.signal_guard.lock().unwrap();
    if pid <= 0
        || exit.code.lock().unwrap().is_some()
        || exit.spawn_error.lock().unwrap().is_some()
        || exit.killed.load(Ordering::Acquire)
    {
        return map_value(vec![
            ("killed", Value::from(false)),
            ("error_code", Value::from("COMMAND_NOT_RUNNING")),
            ("error", Value::from("CommandNotRunning")),
        ]);
    }
    let result = unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
    if result == 0 {
        exit.killed.store(true, Ordering::Release);
        map_value(vec![
            ("killed", Value::from(true)),
            ("error_code", nil()),
            ("error", nil()),
        ])
    } else {
        map_value(vec![
            ("killed", Value::from(false)),
            ("error_code", Value::from("SIGNAL_FAILED")),
            (
                "error",
                Value::from(std::io::Error::last_os_error().to_string()),
            ),
        ])
    }
}

pub fn cmd_send_stdin(kw: &BTreeMap<String, Value>) -> Value {
    let data = kw_str(kw, "data").unwrap_or_default();
    let eof = kw_bool(kw, "eof").unwrap_or(false);
    let mut records = commands().lock().unwrap();
    let Some(key) = resolve_key(&records, kw) else {
        return map_value(vec![
            ("error_code", Value::from("COMMAND_NOT_FOUND")),
            ("error", Value::from(not_found(kw))),
        ]);
    };
    let record = records.get_mut(&key).expect("resolved command exists");
    if status(record) != "RUNNING" {
        return map_value(vec![
            ("error_code", Value::from("COMMAND_NOT_RUNNING")),
            ("error", Value::from("CommandNotRunning")),
        ]);
    }
    if !record.stdin_enabled {
        return map_value(vec![
            ("error_code", Value::from("STDIN_UNAVAILABLE")),
            ("error", Value::from("StdinUnavailable")),
        ]);
    }
    if eof && record.stdin_closed {
        return map_value(vec![("error_code", nil()), ("error", nil())]);
    }
    match record.stdin.as_mut() {
        None => map_value(vec![
            ("error_code", Value::from("STDIN_UNAVAILABLE")),
            ("error", Value::from("StdinUnavailable")),
        ]),
        Some(stdin) => {
            if !data.is_empty() {
                if let Err(error) = stdin.write_all(data.as_bytes()).and_then(|_| stdin.flush()) {
                    return map_value(vec![
                        ("error_code", Value::from("STDIN_WRITE_FAILED")),
                        ("error", Value::from(error.to_string())),
                    ]);
                }
            }
            if eof {
                record.stdin = None;
                record.stdin_closed = true;
            }
            map_value(vec![("error_code", nil()), ("error", nil())])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(command_id: &str, cmd: &str) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("command_id".to_string(), Value::from(command_id)),
            ("cmd".to_string(), Value::from(cmd)),
        ])
    }
    fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
        let Value::Map(items) = value else {
            panic!("expected map")
        };
        items
            .iter()
            .find_map(|(key, value)| (key.as_str() == Some(name)).then_some(value))
            .unwrap()
    }

    #[test]
    fn legacy_starts_are_independent_and_poll_preserves_output_and_exit_code() {
        let request = BTreeMap::from([(
            "cmd".to_string(),
            Value::from("printf legacy; printf err >&2; exit 17"),
        )]);
        let first = cmd_start(&request);
        let second = cmd_start(&request);
        assert!(field(&first, "error").is_nil());
        assert_ne!(field(&first, "command_id"), field(&second, "command_id"));
        for started in [&first, &second] {
            let lookup = BTreeMap::from([
                ("pid".to_string(), field(started, "pid").clone()),
                ("wait_timeout".to_string(), Value::from(5)),
            ]);
            let result = cmd_poll(&lookup);
            assert_eq!(field(&result, "status").as_str(), Some("done"));
            assert_eq!(field(&result, "stdout").as_str(), Some("legacy"));
            assert_eq!(field(&result, "stderr").as_str(), Some("err"));
            assert_eq!(field(&result, "exit_code").as_i64(), Some(17));
            let stable = cmd_get(&BTreeMap::from([(
                "command_id".to_string(),
                field(started, "command_id").clone(),
            )]));
            assert_eq!(field(&stable, "status").as_str(), Some("FAILED"));
        }
    }

    #[test]
    fn legacy_stdin_list_and_wait_work_by_pid() {
        let started = cmd_start(&BTreeMap::from([
            ("cmd".to_string(), Value::from("cat")),
            ("want_stdin".to_string(), Value::from(true)),
        ]));
        let mut lookup = BTreeMap::from([("pid".to_string(), field(&started, "pid").clone())]);
        let running = cmd_poll(&lookup);
        assert_eq!(field(&running, "status").as_str(), Some("running"));
        let listed = cmd_list(&BTreeMap::new());
        let process = field(&listed, "processes")
            .as_array()
            .unwrap()
            .iter()
            .find(|item| field(item, "pid") == field(&started, "pid"))
            .unwrap();
        assert_eq!(field(process, "running").as_bool(), Some(true));
        lookup.insert("data".to_string(), Value::from("legacy stdin\n"));
        lookup.insert("eof".to_string(), Value::from(true));
        assert!(field(&cmd_send_stdin(&lookup), "error").is_nil());
        lookup.insert("timeout".to_string(), Value::from(5));
        let result = cmd_wait(&lookup);
        assert_eq!(field(&result, "status").as_str(), Some("done"));
        assert_eq!(field(&result, "stdout").as_str(), Some("legacy stdin\n"));
        assert_eq!(field(&result, "exit_code").as_i64(), Some(0));
        assert_eq!(field(&result, "running").as_bool(), Some(false));
    }

    #[test]
    fn legacy_kill_and_spawn_failure_terminate_polling() {
        let started = cmd_start(&BTreeMap::from([(
            "cmd".to_string(),
            Value::from("sleep 30"),
        )]));
        let lookup = BTreeMap::from([
            ("pid".to_string(), field(&started, "pid").clone()),
            ("wait_timeout".to_string(), Value::from(5)),
        ]);
        assert_eq!(field(&cmd_kill(&lookup), "killed").as_bool(), Some(true));
        let result = cmd_poll(&lookup);
        assert_eq!(field(&result, "status").as_str(), Some("done"));
        assert_eq!(field(&result, "exit_code").as_i64(), Some(-1));
        let failed = cmd_start(&BTreeMap::from([
            ("cmd".to_string(), Value::from("true")),
            (
                "cwd".to_string(),
                Value::from("/definitely-not-a-real-adx-command-directory"),
            ),
        ]));
        assert_eq!(field(&failed, "status").as_str(), Some("error"));
        assert!(field(&failed, "error").as_str().is_some());
        let missing = cmd_poll(&BTreeMap::from([(
            "pid".to_string(),
            Value::from(i64::MAX),
        )]));
        assert_eq!(field(&missing, "status").as_str(), Some("error"));
    }

    #[test]
    fn explicit_invalid_ids_are_not_treated_as_legacy_requests() {
        for id in [
            Value::Nil,
            Value::from(""),
            Value::from(42),
            Value::from("invalid/id"),
        ] {
            let result = cmd_start(&BTreeMap::from([
                ("command_id".to_string(), id),
                ("cmd".to_string(), Value::from("true")),
            ]));
            assert_eq!(
                field(&result, "error_code").as_str(),
                Some("INVALID_ARGUMENT")
            );
        }
    }

    #[test]
    fn stable_id_is_idempotent_and_conflicts_on_different_request() {
        let id = format!("test-idempotent-{}", now_ms());
        let first = cmd_start(&args(&id, "sleep 0.1"));
        let second = cmd_start(&args(&id, "sleep 0.1"));
        assert_eq!(field(&first, "pid"), field(&second, "pid"));
        let conflict = cmd_start(&args(&id, "true"));
        assert_eq!(
            field(&conflict, "error_code").as_str(),
            Some("COMMAND_CONFLICT")
        );
        assert!(field(&conflict, "error")
            .as_str()
            .unwrap()
            .contains("different request"));
    }

    #[test]
    fn command_can_be_recovered_and_waited_by_id() {
        let id = format!("test-recover-{}", now_ms());
        cmd_start(&args(&id, "printf recovered"));
        let lookup = BTreeMap::from([("command_id".to_string(), Value::from(id.clone()))]);
        let result = cmd_wait(&lookup);
        assert_eq!(field(&result, "command_id").as_str(), Some(id.as_str()));
        assert_eq!(field(&result, "status").as_str(), Some("SUCCEEDED"));
        assert_eq!(field(&result, "stdout").as_str(), Some("recovered"));
        assert_eq!(field(&result, "exit_code").as_i64(), Some(0));
    }

    #[test]
    fn bounded_output_keeps_the_first_bytes_and_marks_truncation() {
        let mut output = BoundedBuffer {
            data: Vec::new(),
            total_bytes: 0,
            truncated: false,
            limit: 4,
        };
        output.append(b"abcdef");
        output.append(b"gh");
        assert_eq!(output.data, b"abcd");
        assert_eq!(output.total_bytes, 8);
        assert!(output.truncated);
    }

    #[test]
    fn rejects_command_ids_outside_the_wire_contract() {
        assert!(valid_command_id("cmd-abc_1.2:x"));
        assert!(!valid_command_id(""));
        assert!(!valid_command_id("contains space"));
        assert!(!valid_command_id("path/name"));
    }

    #[test]
    fn kill_rejects_a_terminal_spawn_failure_without_signalling_negative_pid() {
        let id = format!("test-spawn-failed-kill-{}", now_ms());
        let mut request = args(&id, "true");
        request.insert(
            "cwd".to_string(),
            Value::from("/definitely-not-a-real-adx-command-directory"),
        );
        let started = cmd_start(&request);
        assert_eq!(field(&started, "status").as_str(), Some("FAILED"));

        let lookup = BTreeMap::from([("command_id".to_string(), Value::from(id))]);
        let killed = cmd_kill(&lookup);
        assert_eq!(field(&killed, "killed").as_bool(), Some(false));
        assert_eq!(field(&killed, "error").as_str(), Some("CommandNotRunning"));
    }
}
