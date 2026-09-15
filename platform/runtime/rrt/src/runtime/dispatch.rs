//! HTTP sandbox action dispatch.

use super::codec;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

const REQUEST_DEDUP_TTL: Duration = Duration::from_secs(30 * 60);
const REQUEST_DEDUP_MAX_ENTRIES: usize = 65_536;
const REQUEST_DEDUP_EVICTION_BATCH: usize = 64;

struct DedupSlot {
    created: Instant,
    response: Mutex<Option<Result<rmpv::Value, String>>>,
    ready: Condvar,
}

#[derive(Default)]
struct DedupCache {
    slots: HashMap<String, Arc<DedupSlot>>,
    order: VecDeque<String>,
}

impl DedupCache {
    fn evict_incrementally(&mut self, now: Instant, max_entries: usize) {
        let scan_count = if self.slots.len() >= max_entries {
            self.order.len().min(REQUEST_DEDUP_EVICTION_BATCH)
        } else {
            self.order.len().min(1)
        };
        for _ in 0..scan_count {
            let request_id = self.order.pop_front().expect("dedup order is non-empty");
            let Some(slot) = self.slots.get(&request_id) else {
                continue;
            };
            let expired = now.duration_since(slot.created) > REQUEST_DEDUP_TTL;
            let completed_over_limit =
                self.slots.len() >= max_entries && slot.response.lock().unwrap().is_some();
            if expired || completed_over_limit {
                self.slots.remove(&request_id);
                if self.slots.len() < max_entries {
                    break;
                }
            } else {
                // Rotate live entries so one long-running request cannot block
                // capacity eviction of completed requests behind it.
                self.order.push_back(request_id);
            }
        }
    }

    fn reserve(
        &mut self,
        request_id: &str,
        now: Instant,
        max_entries: usize,
    ) -> (Arc<DedupSlot>, bool) {
        let max_entries = max_entries.max(1);
        if let Some(slot) = self.slots.get(request_id) {
            if now.duration_since(slot.created) <= REQUEST_DEDUP_TTL {
                return (slot.clone(), false);
            }
            // Remove the expired key before general cleanup. Its queue entry
            // is harmless and will be discarded when the cursor reaches it.
            self.slots.remove(request_id);
        }

        self.evict_incrementally(now, max_entries);
        let slot = Arc::new(DedupSlot {
            created: now,
            response: Mutex::new(None),
            ready: Condvar::new(),
        });
        self.slots.insert(request_id.to_string(), slot.clone());
        self.order.push_back(request_id.to_string());
        (slot, true)
    }
}

fn dedup_cache() -> &'static Mutex<DedupCache> {
    static CACHE: OnceLock<Mutex<DedupCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(DedupCache::default()))
}

fn reserve_request_id(request_id: &str) -> (Arc<DedupSlot>, bool) {
    let now = Instant::now();
    let mut cache = dedup_cache().lock().unwrap();
    cache.reserve(request_id, now, REQUEST_DEDUP_MAX_ENTRIES)
}

fn wait_dedup_response(slot: Arc<DedupSlot>) -> Result<rmpv::Value, String> {
    let mut guard = slot.response.lock().unwrap();
    loop {
        if let Some(response) = guard.clone() {
            return response;
        }
        guard = slot.ready.wait(guard).unwrap();
    }
}

fn complete_dedup_response(slot: &Arc<DedupSlot>, response: Result<rmpv::Value, String>) {
    *slot.response.lock().unwrap() = Some(response);
    slot.ready.notify_all();
}

fn sanitize_log_field(value: &str) -> String {
    let mut out = value.replace('\r', "\\r").replace('\n', "\\n");
    const MAX_LOG_FIELD_LEN: usize = 512;
    if out.len() > MAX_LOG_FIELD_LEN {
        let mut truncate_at = MAX_LOG_FIELD_LEN;
        while !out.is_char_boundary(truncate_at) {
            truncate_at -= 1;
        }
        out.truncate(truncate_at);
        out.push_str("...");
    }
    out
}

pub(crate) fn access_command_summary(
    method: &str,
    kw: &std::collections::BTreeMap<String, rmpv::Value>,
) -> String {
    let cmd = match method {
        "cmd_run" | "cmd_start" => {
            codec::kw_str(kw, "cmd").or_else(|| codec::kw_str(kw, "command"))
        }
        "bash_submit" => codec::kw_str(kw, "command").or_else(|| codec::kw_str(kw, "cmd")),
        "cmd_get" | "cmd_poll" | "cmd_wait" | "cmd_kill" | "cmd_send_stdin" => kw
            .get("command_id")
            .and_then(|v| v.as_str())
            .map(|id| format!("command_id={}", sanitize_log_field(id)))
            .or_else(|| {
                kw.get("pid")
                    .and_then(|v| v.as_i64())
                    .map(|pid| format!("pid={pid}"))
            }),
        _ => None,
    };
    match cmd {
        Some(cmd) if !cmd.is_empty() => format!("{method} {}", sanitize_log_field(&cmd)),
        Some(cmd) => format!("{method} {}", sanitize_log_field(&cmd)),
        None => method.to_string(),
    }
}

pub(crate) fn log_access(trace_id: &str, command: &str, started: Instant) {
    rrt_info!(
        "[rrt-access] traceid={} command={} duration_ms={}",
        sanitize_log_field(trace_id),
        sanitize_log_field(command),
        started.elapsed().as_millis()
    );
}

fn command_result(stdout: String, stderr: String, exit_code: i64) -> rmpv::Value {
    codec::map_value(vec![
        ("stdout", rmpv::Value::from(stdout)),
        ("stderr", rmpv::Value::from(stderr)),
        ("exit_code", rmpv::Value::from(exit_code)),
    ])
}

fn command_timeout(kw: &std::collections::BTreeMap<String, rmpv::Value>) -> Option<f64> {
    let timeout = kw.get("timeout").and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_i64().map(|v| v as f64))
            .or_else(|| value.as_u64().map(|v| v as f64))
    })?;
    (timeout.is_finite() && timeout >= 0.0).then_some(timeout)
}

fn read_command_output(file: &mut std::fs::File) -> String {
    use std::io::{Read, Seek, SeekFrom};

    if file.seek(SeekFrom::Start(0)).is_err() {
        return String::new();
    }
    let mut output = Vec::new();
    if file.read_to_end(&mut output).is_err() {
        return String::new();
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn reap_timed_out_child(
    mut child: std::process::Child,
    trace_id: &str,
    pid: libc::pid_t,
) -> std::io::Result<Option<std::process::ExitStatus>> {
    const REAP_GRACE: Duration = Duration::from_secs(1);
    let deadline = Instant::now() + REAP_GRACE;
    loop {
        match child.try_wait()? {
            Some(status) => return Ok(Some(status)),
            None if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            None => {
                let trace_id = trace_id.to_string();
                rrt_warn!(
                    "[rrt-command] phase=reap_deferred traceid={} pid={} grace_ms={}",
                    sanitize_log_field(&trace_id),
                    pid,
                    REAP_GRACE.as_millis()
                );
                std::thread::spawn(move || {
                    match child.wait() {
                    Ok(status) => rrt_info!(
                        "[rrt-command] phase=wait_done traceid={} pid={} exit_code={} timed_out=true deferred=true",
                        sanitize_log_field(&trace_id),
                        pid,
                        status.code().unwrap_or(-1)
                    ),
                    Err(e) => rrt_error!(
                        "[rrt-command] phase=wait_failed traceid={} pid={} deferred=true error={}",
                        sanitize_log_field(&trace_id),
                        pid,
                        sanitize_log_field(&e.to_string())
                    ),
                }
                });
                return Ok(None);
            }
        }
    }
}

/// Run one shell command and return `{stdout, stderr, exit_code}`, matching akernel cmd_run.
///
/// Timed commands run in their own process group so expiry can terminate the
/// shell and every descendant. Output is redirected to anonymous temporary
/// files instead of pipes: a descendant that escapes the process group cannot
/// keep a pipe open and block this function after the shell has been reaped.
fn run_command(
    cmd: &str,
    cwd: Option<&str>,
    envs: Option<&rmpv::Value>,
    timeout_seconds: Option<f64>,
    trace_id: &str,
) -> rmpv::Value {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};

    let mut stdout = match tempfile::tempfile() {
        Ok(file) => file,
        Err(e) => return command_result(String::new(), e.to_string(), -1),
    };
    let mut stderr = match tempfile::tempfile() {
        Ok(file) => file,
        Err(e) => return command_result(String::new(), e.to_string(), -1),
    };
    let stdout_child = match stdout.try_clone() {
        Ok(file) => file,
        Err(e) => return command_result(String::new(), e.to_string(), -1),
    };
    let stderr_child = match stderr.try_clone() {
        Ok(file) => file,
        Err(e) => return command_result(String::new(), e.to_string(), -1),
    };

    let mut c = Command::new("/bin/sh");
    c.arg("-c")
        .arg(cmd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_child))
        .stderr(Stdio::from(stderr_child))
        .process_group(0);
    super::child_env::apply(&mut c);
    if let Some(d) = cwd {
        if !d.is_empty() {
            c.current_dir(d);
        }
    }
    if let Some(rmpv::Value::Map(kvs)) = envs {
        for (k, v) in kvs {
            if let (Some(k), Some(v)) = (k.as_str(), v.as_str()) {
                if let Err(error) = super::child_env::validate_override(k) {
                    return command_result(String::new(), error, -1);
                }
                c.env(k, v);
            }
        }
    }

    let started = Instant::now();
    let timeout_label = timeout_seconds
        .map(|timeout| timeout.to_string())
        .unwrap_or_else(|| "none".to_string());
    rrt_info!(
        "[rrt-command] phase=spawn_start traceid={} timeout_seconds={}",
        sanitize_log_field(trace_id),
        timeout_label
    );
    let mut child = match c.spawn() {
        Ok(child) => child,
        Err(e) => {
            rrt_error!(
                "[rrt-command] phase=spawn_failed traceid={} error={}",
                sanitize_log_field(trace_id),
                sanitize_log_field(&e.to_string())
            );
            return command_result(String::new(), e.to_string(), -1);
        }
    };
    let pid = child.id() as libc::pid_t;
    rrt_info!(
        "[rrt-command] phase=spawned_pid traceid={} pid={}",
        sanitize_log_field(trace_id),
        pid
    );

    let deadline = timeout_seconds.map(|timeout| started + Duration::from_secs_f64(timeout));
    let mut timed_out = false;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(Some(status)),
            Ok(None) => {}
            Err(e) => break Err(e),
        }

        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            timed_out = true;
            rrt_warn!(
                "[rrt-command] phase=timeout_kill traceid={} pid={} timeout_seconds={}",
                sanitize_log_field(trace_id),
                pid,
                timeout_label
            );
            let kill_result = unsafe { libc::kill(-pid, libc::SIGKILL) };
            if kill_result != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    rrt_error!(
                        "[rrt-command] phase=timeout_kill_failed traceid={} pid={} error={}",
                        sanitize_log_field(trace_id),
                        pid,
                        sanitize_log_field(&error.to_string())
                    );
                    let _ = child.kill();
                }
            }
            break reap_timed_out_child(child, trace_id, pid);
        }

        let sleep_for = deadline
            .map(|deadline| {
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10))
            })
            .unwrap_or(Duration::from_millis(10));
        std::thread::sleep(sleep_for);
    };

    let stdout = read_command_output(&mut stdout);
    let captured_stderr = read_command_output(&mut stderr);
    match status {
        Ok(Some(status)) => {
            let exit_code = status.code().unwrap_or(-1) as i64;
            rrt_info!(
                "[rrt-command] phase=wait_done traceid={} pid={} exit_code={} timed_out={} duration_ms={}",
                sanitize_log_field(trace_id),
                pid,
                exit_code,
                timed_out,
                started.elapsed().as_millis()
            );
            if timed_out {
                command_result(
                    stdout,
                    format!("Command timed out after {timeout_label} seconds"),
                    -1,
                )
            } else {
                command_result(stdout, captured_stderr, exit_code)
            }
        }
        Ok(None) => command_result(
            stdout,
            format!("Command timed out after {timeout_label} seconds"),
            -1,
        ),
        Err(e) => {
            rrt_error!(
                "[rrt-command] phase=wait_failed traceid={} pid={} error={}",
                sanitize_log_field(trace_id),
                pid,
                sanitize_log_field(&e.to_string())
            );
            command_result(stdout, e.to_string(), -1)
        }
    }
}

pub(crate) fn normalize_sandbox_action(action: &str) -> Option<&'static str> {
    match action {
        "ping" | "noop" => Some("ping"),
        "cmd_run" | "exec" | "process.exec" | "process.run" | "cmd.run" => Some("cmd_run"),
        "cmd_start" | "process.start" | "cmd.start" => Some("cmd_start"),
        "cmd_get" | "process.get" | "cmd.get" => Some("cmd_get"),
        "cmd_poll" | "process.poll" | "cmd.poll" => Some("cmd_poll"),
        "cmd_wait" | "process.wait" | "cmd.wait" => Some("cmd_wait"),
        "cmd_kill" | "process.kill" | "cmd.kill" => Some("cmd_kill"),
        "cmd_list" | "process.list" | "cmd.list" => Some("cmd_list"),
        "cmd_capabilities" | "process.capabilities" | "cmd.capabilities" => {
            Some("cmd_capabilities")
        }
        "cmd_send_stdin" | "process.stdin" | "process.send_stdin" | "cmd.send_stdin" => {
            Some("cmd_send_stdin")
        }
        "entrypoint.poll" => Some("entrypoint_poll"),
        "fs_read" | "file.read" | "fs.read" => Some("fs_read"),
        "fs_write" | "file.write" | "fs.write" => Some("fs_write"),
        "fs_write_chunk" | "file.write_chunk" | "file.upload.chunk" | "fs.write_chunk" => {
            Some("fs_write_chunk")
        }
        "fs_read_chunk" | "file.read_chunk" | "file.download.chunk" | "fs.read_chunk" => {
            Some("fs_read_chunk")
        }
        "fs_list" | "file.list" | "fs.list" => Some("fs_list"),
        "fs_exists" | "file.exists" | "fs.exists" => Some("fs_exists"),
        "fs_remove" | "file.remove" | "fs.remove" => Some("fs_remove"),
        "fs_rename" | "file.rename" | "fs.rename" => Some("fs_rename"),
        "fs_make_dir" | "file.mkdir" | "file.make_dir" | "fs.mkdir" | "fs.make_dir" => {
            Some("fs_make_dir")
        }
        "fs_get_info" | "file.stat" | "file.info" | "fs.stat" | "fs.get_info" => {
            Some("fs_get_info")
        }
        "bash_init" | "shell.create" | "shell.init" => Some("bash_init"),
        "bash_submit" | "shell.run" | "shell.submit" => Some("bash_submit"),
        "bash_poll" | "shell.poll" => Some("bash_poll"),
        "bash_destroy" | "shell.delete" | "shell.destroy" | "shell.close" => Some("bash_destroy"),
        _ => None,
    }
}

pub(crate) fn dispatch_runtime_action_with_trace(
    method: &str,
    kw: &std::collections::BTreeMap<String, rmpv::Value>,
    trace_id: &str,
) -> Option<rmpv::Value> {
    match method {
        "ping" => Some(codec::map_value(vec![("status", rmpv::Value::from("ok"))])),
        "cmd_run" => {
            let cmd = codec::kw_str(kw, "cmd")
                .or_else(|| codec::kw_str(kw, "command"))
                .unwrap_or_default();
            let cwd = codec::kw_str(kw, "cwd").or_else(|| codec::kw_str(kw, "working_dir"));
            let envs = kw.get("envs").or_else(|| kw.get("env"));
            Some(run_command(
                &cmd,
                cwd.as_deref(),
                envs,
                command_timeout(kw),
                trace_id,
            ))
        }
        "fs_read" | "fs_write" | "fs_write_chunk" | "fs_read_chunk" | "fs_list" | "fs_exists"
        | "fs_remove" | "fs_rename" | "fs_make_dir" | "fs_get_info" => Some(match method {
            "fs_read" => super::fs::fs_read(kw),
            "fs_write" => super::fs::fs_write(kw),
            "fs_write_chunk" => super::fs::fs_write_chunk(kw),
            "fs_read_chunk" => super::fs::fs_read_chunk(kw),
            "fs_list" => super::fs::fs_list(kw),
            "fs_exists" => super::fs::fs_exists(kw),
            "fs_remove" => super::fs::fs_remove(kw),
            "fs_rename" => super::fs::fs_rename(kw),
            "fs_make_dir" => super::fs::fs_make_dir(kw),
            _ => super::fs::fs_get_info(kw),
        }),
        "cmd_start" | "cmd_get" | "cmd_poll" | "cmd_wait" | "cmd_kill" | "cmd_list"
        | "cmd_capabilities" | "cmd_send_stdin" => Some(match method {
            "cmd_start" => super::cmd::cmd_start(kw),
            "cmd_get" => super::cmd::cmd_get(kw),
            "cmd_poll" => super::cmd::cmd_poll(kw),
            "cmd_wait" => super::cmd::cmd_wait(kw),
            "cmd_kill" => super::cmd::cmd_kill(kw),
            "cmd_list" => super::cmd::cmd_list(kw),
            "cmd_capabilities" => super::cmd::cmd_capabilities(kw),
            _ => super::cmd::cmd_send_stdin(kw),
        }),
        "bash_init" | "bash_submit" | "bash_poll" | "bash_destroy" => Some(match method {
            "bash_init" => super::bash::bash_init(kw),
            "bash_submit" => super::bash::bash_submit(kw),
            "bash_poll" => super::bash::bash_poll(kw),
            _ => super::bash::bash_destroy(kw),
        }),
        "entrypoint_poll" => {
            let wait_timeout = kw
                .get("wait_timeout")
                .and_then(|value| {
                    value
                        .as_f64()
                        .or_else(|| value.as_i64().map(|value| value as f64))
                })
                .unwrap_or(10.0);
            Some(super::entrypoint::poll(wait_timeout))
        }
        _ => None,
    }
}

/// Execute one public sandbox action through the same normalized RRT primitive
/// used by the HTTP endpoint.
pub(crate) fn execute_sandbox_action(
    action: &str,
    kw: &std::collections::BTreeMap<String, rmpv::Value>,
    trace_id: &str,
) -> Result<rmpv::Value, String> {
    let started = Instant::now();
    let method = normalize_sandbox_action(action)
        .ok_or_else(|| format!("unsupported sandbox action: {action}"))?;
    let command = access_command_summary(method, kw);
    let result = dispatch_runtime_action_with_trace(method, kw, trace_id)
        .ok_or_else(|| format!("unsupported sandbox action: {action}"));
    log_access(trace_id, &command, started);
    result
}

/// Execute a sandbox action at most once for a non-empty request ID. Both
/// HTTP requests use this cache, so concurrent retries
/// observe the same result.
pub(crate) fn execute_sandbox_action_once(
    request_id: Option<&str>,
    action: &str,
    kw: &std::collections::BTreeMap<String, rmpv::Value>,
    trace_id: &str,
) -> Result<rmpv::Value, String> {
    let Some(request_id) = request_id.filter(|id| !id.is_empty()) else {
        return execute_sandbox_action(action, kw, trace_id);
    };
    let (slot, owner) = reserve_request_id(request_id);
    if !owner {
        return wait_dedup_response(slot);
    }
    let response = execute_sandbox_action(action, kw, trace_id);
    complete_dedup_response(&slot, response.clone());
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    #[test]
    fn normalizes_public_sandbox_actions_to_rrt_methods() {
        assert_eq!(normalize_sandbox_action("ping"), Some("ping"));
        assert_eq!(normalize_sandbox_action("noop"), Some("ping"));
        assert_eq!(normalize_sandbox_action("process.exec"), Some("cmd_run"));
        assert_eq!(normalize_sandbox_action("file.read"), Some("fs_read"));
        assert_eq!(normalize_sandbox_action("shell.run"), Some("bash_submit"));
        assert_eq!(normalize_sandbox_action("unknown"), None);
    }

    #[test]
    fn dispatches_public_ping_and_noop_actions() {
        let kw = BTreeMap::new();
        for action in ["ping", "noop"] {
            let result = execute_sandbox_action(action, &kw, "trace-test")
                .expect("lightweight action should dispatch through the shared path");
            assert_eq!(result_field(&result, "status").as_str(), Some("ok"));
        }
    }

    #[test]
    fn dedup_cache_reuses_live_slots_and_expires_old_slots() {
        let now = Instant::now();
        let mut cache = DedupCache::default();
        let (first, owner) = cache.reserve("request-a", now, 4);
        assert!(owner);
        complete_dedup_response(&first, Ok(rmpv::Value::from("first")));

        let (duplicate, owner) = cache.reserve("request-a", now + Duration::from_secs(1), 4);
        assert!(!owner);
        assert!(Arc::ptr_eq(&first, &duplicate));

        cache.reserve(
            "request-b",
            now + REQUEST_DEDUP_TTL + Duration::from_secs(1),
            4,
        );
        assert!(!cache.slots.contains_key("request-a"));
        let (_, owner) = cache.reserve(
            "request-a",
            now + REQUEST_DEDUP_TTL + Duration::from_secs(2),
            4,
        );
        assert!(owner);
    }

    #[test]
    fn dedup_cache_evicts_oldest_completed_slot_at_capacity() {
        let now = Instant::now();
        let mut cache = DedupCache::default();
        let (first, _) = cache.reserve("request-a", now, 2);
        complete_dedup_response(&first, Ok(rmpv::Value::from("first")));
        let (second, _) = cache.reserve("request-b", now + Duration::from_secs(1), 2);
        complete_dedup_response(&second, Ok(rmpv::Value::from("second")));

        cache.reserve("request-c", now + Duration::from_secs(2), 2);
        assert_eq!(cache.slots.len(), 2);
        assert!(!cache.slots.contains_key("request-a"));
        assert!(cache.slots.contains_key("request-b"));
        assert!(cache.slots.contains_key("request-c"));
    }

    #[test]
    fn dedup_cache_preserves_a_duplicate_at_capacity() {
        let now = Instant::now();
        let mut cache = DedupCache::default();
        let (first, _) = cache.reserve("request-a", now, 1);
        complete_dedup_response(&first, Ok(rmpv::Value::from("first")));

        let (duplicate, owner) = cache.reserve("request-a", now + Duration::from_secs(1), 1);
        assert!(!owner);
        assert!(Arc::ptr_eq(&first, &duplicate));
        assert_eq!(cache.slots.len(), 1);
    }

    #[test]
    fn dedup_cache_never_evicts_an_in_flight_slot_for_capacity() {
        let now = Instant::now();
        let mut cache = DedupCache::default();
        let (in_flight, _) = cache.reserve("request-a", now, 1);

        cache.reserve("request-b", now + Duration::from_secs(1), 1);
        let (duplicate, owner) = cache.reserve("request-a", now + Duration::from_secs(2), 1);
        assert!(!owner);
        assert!(Arc::ptr_eq(&in_flight, &duplicate));
        assert_eq!(cache.slots.len(), 2);
    }

    #[test]
    fn dedup_cache_evicts_completed_slots_behind_an_in_flight_slot() {
        let now = Instant::now();
        let mut cache = DedupCache::default();
        cache.reserve("request-a", now, 2);
        let (completed, _) = cache.reserve("request-b", now + Duration::from_secs(1), 2);
        complete_dedup_response(&completed, Ok(rmpv::Value::from("done")));

        cache.reserve("request-c", now + Duration::from_secs(2), 2);
        assert_eq!(cache.slots.len(), 2);
        assert!(cache.slots.contains_key("request-a"));
        assert!(!cache.slots.contains_key("request-b"));
        assert!(cache.slots.contains_key("request-c"));
    }

    #[test]
    fn access_command_summary_includes_command_and_sanitizes_newlines() {
        let mut kw = BTreeMap::new();
        kw.insert(
            "cmd".to_string(),
            rmpv::Value::from("printf 'hello\nworld'"),
        );
        assert_eq!(
            access_command_summary("cmd_run", &kw),
            "cmd_run printf 'hello\\nworld'"
        );
    }

    #[test]
    fn access_command_summary_truncates_at_utf8_boundary() {
        let command = format!("{}中{}", "a".repeat(511), "b".repeat(280));
        assert_eq!(command.len(), 794);

        let mut kw = BTreeMap::new();
        kw.insert("cmd".to_string(), rmpv::Value::from(command));

        assert_eq!(
            access_command_summary("cmd_run", &kw),
            format!("cmd_run {}...", "a".repeat(511))
        );
    }

    #[test]
    fn dispatches_process_exec_action_args() {
        let mut kw = BTreeMap::new();
        kw.insert(
            "cmd".to_string(),
            rmpv::Value::from("printf sandbox-invoke"),
        );
        let result = execute_sandbox_action("process.exec", &kw, "trace-test")
            .expect("sandbox action should dispatch through the shared path");
        if let rmpv::Value::Map(fields) = result {
            let stdout = fields
                .iter()
                .find_map(|(k, v)| (k.as_str() == Some("stdout")).then_some(v.as_str()))
                .flatten()
                .unwrap_or_default();
            let exit_code = fields
                .iter()
                .find_map(|(k, v)| (k.as_str() == Some("exit_code")).then_some(v.as_i64()))
                .flatten()
                .unwrap_or_default();
            assert_eq!(stdout, "sandbox-invoke");
            assert_eq!(exit_code, 0);
        } else {
            panic!("cmd_run should return a map");
        }
    }

    fn result_field<'a>(result: &'a rmpv::Value, name: &str) -> &'a rmpv::Value {
        let rmpv::Value::Map(fields) = result else {
            panic!("command result should be a map");
        };
        fields
            .iter()
            .find_map(|(key, value)| (key.as_str() == Some(name)).then_some(value))
            .unwrap_or_else(|| panic!("command result should contain {name}"))
    }

    #[test]
    fn cmd_run_honors_timeout_and_reaps_the_shell_process() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let shell_pid = temp.path().join("shell.pid");
        let descendant_marker = temp.path().join("descendant-finished");
        let command = format!(
            "echo $$ > {}; (sleep 0.5; touch {}) & wait",
            shell_pid.display(),
            descendant_marker.display()
        );
        let mut kw = BTreeMap::new();
        kw.insert("cmd".to_string(), rmpv::Value::from(command));
        kw.insert("timeout".to_string(), rmpv::Value::F64(0.1));

        let started = Instant::now();
        let result = dispatch_runtime_action_with_trace("cmd_run", &kw, "")
            .expect("cmd_run should dispatch");

        assert!(
            started.elapsed() < Duration::from_millis(400),
            "timed command should return near its deadline"
        );
        assert_eq!(result_field(&result, "exit_code").as_i64(), Some(-1));
        assert!(result_field(&result, "stderr")
            .as_str()
            .unwrap_or_default()
            .contains("Command timed out after 0.1 seconds"));

        let pid = std::fs::read_to_string(&shell_pid)
            .expect("shell should write its pid before timeout")
            .trim()
            .parse::<libc::pid_t>()
            .expect("shell pid should be numeric");
        let alive = unsafe { libc::kill(pid, 0) };
        assert_eq!(
            alive, -1,
            "the direct child should already be reaped when cmd_run returns"
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );

        std::thread::sleep(Duration::from_millis(550));
        assert!(
            !descendant_marker.exists(),
            "timeout should kill the whole process group, not only /bin/sh"
        );
    }

    #[test]
    fn deduplicates_sandbox_actions_by_request_id() {
        let first_args = BTreeMap::from([("cmd".to_string(), rmpv::Value::from("printf first"))]);
        let second_args = BTreeMap::from([("cmd".to_string(), rmpv::Value::from("printf second"))]);

        let first = execute_sandbox_action_once(
            Some("rrt-dedup-test-request"),
            "process.exec",
            &first_args,
            "trace-first",
        )
        .expect("first action should run");
        let duplicate = execute_sandbox_action_once(
            Some("rrt-dedup-test-request"),
            "process.exec",
            &second_args,
            "trace-second",
        )
        .expect("duplicate action should reuse the first result");

        assert_eq!(duplicate, first);
    }
}
