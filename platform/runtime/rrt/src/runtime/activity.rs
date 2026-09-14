// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
// See the LICENSE file in this repository for the complete license text.

//! Local busy/idle tracking for the HTTP atomic-operation server, tunnel WS,
//! RuntimeRPC call handling, and processes launched through `process.start`.
//! The active counter reports via `KillRequest(signal=23)`: busy is reasserted
//! when work restarts and on direct/tunnel lease renewals, while idle is
//! debounced after the final `1 -> 0` transition. Function-proxy reuses IdleMgr
//! to start or stop the idle timer.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};

use tokio::sync::mpsc;

use crate::posix::runtime_rpc::StreamingMessage;

static ACTIVE: AtomicI64 = AtomicI64::new(0);
static ACTIVE_COMMANDS: AtomicI64 = AtomicI64::new(0);
static IDLE_EPOCH: AtomicU64 = AtomicU64::new(0);
static REPORTER: OnceLock<ActivityReporter> = OnceLock::new();
const IDLE_REPORT_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(800);

struct ActivityReporter {
    instance_id: RwLock<String>,
    tx: mpsc::Sender<StreamingMessage>,
}

impl ActivityReporter {
    fn new(instance_id: String, tx: mpsc::Sender<StreamingMessage>) -> Self {
        Self {
            instance_id: RwLock::new(instance_id),
            tx,
        }
    }

    fn rebind_instance_id(&self, instance_id: &str) {
        *self
            .instance_id
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = instance_id.to_string();
    }

    fn instance_id(&self) -> String {
        self.instance_id
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

/// Initialize the activity counter. This is a reserved init hook; the global counter naturally maintains the initial value.
pub fn init() {}

/// Initialize the activity reporter before starting HTTP/tunnel servers so the first direct request can report busy.
pub fn init_reporter(instance_id: String, tx: mpsc::Sender<StreamingMessage>) {
    let _ = REPORTER.set(ActivityReporter::new(instance_id, tx));
}

/// Adopt the target logical identity after the restore environment has been
/// validated. Direct HTTP/tunnel activity survives checkpoint/restore, so its
/// reports must follow the same identity as the reconnected RuntimeRPC stream.
pub fn rebind_reporter_instance_id(instance_id: &str) {
    if let Some(reporter) = REPORTER.get() {
        reporter.rebind_instance_id(instance_id);
    }
}

/// RAII guard: increments activity on creation and decrements on drop, including connection/call end and panic unwinding.
#[must_use]
pub struct ActiveGuard {
    source: ActivitySource,
}

#[must_use]
pub struct CommandActivityGuard;

/// Identifies which runtime surface produced an activity report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActivitySource {
    Checkpoint,
    DirectHttp,
    RuntimeRpc,
    Tunnel,
}

impl ActivitySource {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::DirectHttp => "direct-http",
            Self::RuntimeRpc => "runtime-rpc",
            Self::Tunnel => "tunnel",
        }
    }

    const fn reasserts_busy(self) -> bool {
        matches!(self, Self::DirectHttp | Self::Tunnel)
    }
}

/// Mark a connection/call active and return a guard; dropping the guard ends the activity.
pub(crate) fn enter(source: ActivitySource) -> ActiveGuard {
    let previous = ACTIVE.fetch_add(1, Ordering::SeqCst);
    let crossed_from_idle = state_transition(previous, true).is_some();
    if crossed_from_idle {
        // A new activity invalidates any pending debounced idle report. Always
        // reassert busy after crossing from zero: the proxy may have armed its
        // timer through another traffic source since the previous report.
        IDLE_EPOCH.fetch_add(1, Ordering::SeqCst);
    }

    // Direct requests are lease renewals, and a tunnel connection is a
    // long-lived activity owner. Reassert both even when another request is
    // active so polling and overlapping init/tunnel startup cannot leave the
    // proxy with an armed idle timer.
    if crossed_from_idle || source.reasserts_busy() {
        report_state_transition(true, source);
    }
    ActiveGuard { source }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let previous = ACTIVE.fetch_sub(1, Ordering::SeqCst);
        if state_transition(previous, false).is_some() {
            schedule_idle_report(self.source);
        }
    }
}

pub fn enter_command() -> CommandActivityGuard {
    let count = ACTIVE_COMMANDS.fetch_add(1, Ordering::SeqCst) + 1;
    report_command_count(count);
    CommandActivityGuard
}

impl Drop for CommandActivityGuard {
    fn drop(&mut self) {
        let count = ACTIVE_COMMANDS.fetch_sub(1, Ordering::SeqCst) - 1;
        report_command_count(count.max(0));
    }
}

fn report_command_count(count: i64) {
    let Some(reporter) = REPORTER.get() else {
        return;
    };
    let instance_id = reporter.instance_id();
    let payload = format!("command:{count}").into_bytes();
    let msg = super::activity_report_msg(&instance_id, payload);
    if let Err(error) = reporter.tx.try_send(msg) {
        rrt_error!(
            "[rrt-runtime] command activity report failed instance={} active_commands={} error={}",
            instance_id,
            count,
            error
        );
    }
}

pub fn active_command_count() -> i64 {
    ACTIVE_COMMANDS.load(Ordering::SeqCst)
}

/// Re-send the complete command count as a lease heartbeat.
///
/// This is intentionally a best-effort snapshot: losing the reporting channel
/// must never stop command execution. FunctionSystem expires the independent
/// command-activity lease and pauses idle reclamation until a later snapshot
/// arrives.
pub fn report_command_snapshot() {
    report_command_count(active_command_count());
}

fn schedule_idle_report(source: ActivitySource) {
    let epoch = IDLE_EPOCH.fetch_add(1, Ordering::SeqCst) + 1;
    let idle_task = async move {
        tokio::time::sleep(IDLE_REPORT_DEBOUNCE).await;
        let current_epoch = IDLE_EPOCH.load(Ordering::SeqCst);
        let active_count = ACTIVE.load(Ordering::SeqCst);
        if current_epoch != epoch || active_count != 0 {
            rrt_info!(
                "[rrt-runtime] activity idle report cancelled source={} scheduled_epoch={} current_epoch={} active_count={}",
                source.as_str(),
                epoch,
                current_epoch,
                active_count
            );
            return;
        }
        report_state_transition(false, source);
    };
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(idle_task);
    }
}

fn report_state_transition(busy: bool, source: ActivitySource) {
    let Some(reporter) = REPORTER.get() else {
        return;
    };
    let instance_id = reporter.instance_id();
    let state = if busy { "busy" } else { "idle" };
    let msg = super::activity_report_msg(&instance_id, state.as_bytes().to_vec());
    match reporter.tx.try_send(msg) {
        Ok(()) => {
            rrt_info!(
                "[rrt-runtime] activity state={} source={} report_signal={} instance={} active_count={}",
                state,
                source.as_str(),
                super::IDLE_REPORT_SIGNAL,
                instance_id,
                ACTIVE.load(Ordering::SeqCst)
            );
        }
        Err(e) => {
            rrt_error!(
                "[rrt-runtime] activity report failed state={} source={} instance={} error={}",
                state,
                source.as_str(),
                instance_id,
                e
            );
        }
    }
}

/// Current activity state text. Used to resynchronize state with function-proxy after MessageStream reconnects.
pub fn current_state() -> &'static str {
    if ACTIVE.load(Ordering::SeqCst) > 0 {
        "busy"
    } else {
        "idle"
    }
}

/// Current number of active connections, calls, and launched processes.
pub fn active_count() -> i64 {
    ACTIVE.load(Ordering::SeqCst)
}

/// Wait until all in-flight RuntimeRPC/HTTP/tunnel requests and launched
/// processes finish.
pub async fn wait_until_idle(timeout: std::time::Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if active_count() <= 0 {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Report only when the global active counter crosses the zero boundary.
fn state_transition(previous: i64, entering: bool) -> Option<&'static str> {
    match (previous, entering) {
        (0, true) => Some("busy"),
        (1, false) => Some("idle"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tokio::sync::mpsc::error::TryRecvError;

    fn run_in_isolated_process(env_name: &str, test_name: &str) -> bool {
        if std::env::var_os(env_name).is_some() {
            return true;
        }
        let status = Command::new(std::env::current_exe().expect("current test executable"))
            .arg(test_name)
            .arg("--exact")
            .arg("--test-threads=1")
            .env(env_name, "1")
            .status()
            .expect("run isolated activity test");
        assert!(status.success(), "isolated activity test failed");
        false
    }

    async fn recv_report(rx: &mut mpsc::Receiver<StreamingMessage>, expected_state: &str) {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("activity report timed out")
            .expect("activity reporter closed");
        let Some(crate::posix::runtime_rpc::streaming_message::Body::KillReq(kill)) = msg.body
        else {
            panic!("expected activity KillReq");
        };
        assert_eq!(kill.payload, expected_state.as_bytes());
    }

    #[test]
    fn state_transition_only_on_zero_boundary() {
        assert_eq!(state_transition(0, true), Some("busy"));
        assert_eq!(state_transition(1, true), None);
        assert_eq!(state_transition(2, false), None);
        assert_eq!(state_transition(1, false), Some("idle"));
        assert_eq!(state_transition(0, false), None);
    }

    #[test]
    fn guard_increments_then_decrements() {
        let base = active_count();
        {
            let _g = enter(ActivitySource::DirectHttp);
            assert_eq!(active_count(), base + 1);
        }
        assert_eq!(active_count(), base);
    }

    #[test]
    fn reporter_uses_rebound_target_logical_identity() {
        let (tx, _rx) = mpsc::channel(1);
        let reporter = ActivityReporter::new("source-sandbox".to_string(), tx);

        reporter.rebind_instance_id("clone-sandbox");

        assert_eq!(reporter.instance_id(), "clone-sandbox");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tunnel_within_idle_debounce_reasserts_busy_then_reports_idle_on_disconnect() {
        const ENV: &str = "ADX_RRT_ACTIVITY_TUNNEL_WITHIN_DEBOUNCE_ISOLATED";
        const TEST: &str = "runtime::activity::tests::tunnel_within_idle_debounce_reasserts_busy_then_reports_idle_on_disconnect";
        if !run_in_isolated_process(ENV, TEST) {
            return;
        }

        let (tx, mut rx) = mpsc::channel(8);
        init_reporter("sandbox-under-test".to_string(), tx);

        let init_call = enter(ActivitySource::RuntimeRpc);
        recv_report(&mut rx, "busy").await;
        drop(init_call);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let tunnel = enter(ActivitySource::Tunnel);
        recv_report(&mut rx, "busy").await;

        tokio::time::sleep(IDLE_REPORT_DEBOUNCE + std::time::Duration::from_millis(100)).await;
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        drop(tunnel);
        recv_report(&mut rx, "idle").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tunnel_after_idle_debounce_reports_idle_then_busy() {
        const ENV: &str = "ADX_RRT_ACTIVITY_TUNNEL_AFTER_DEBOUNCE_ISOLATED";
        const TEST: &str =
            "runtime::activity::tests::tunnel_after_idle_debounce_reports_idle_then_busy";
        if !run_in_isolated_process(ENV, TEST) {
            return;
        }

        let (tx, mut rx) = mpsc::channel(8);
        init_reporter("sandbox-under-test".to_string(), tx);

        let init_call = enter(ActivitySource::RuntimeRpc);
        recv_report(&mut rx, "busy").await;
        drop(init_call);
        recv_report(&mut rx, "idle").await;

        let tunnel = enter(ActivitySource::Tunnel);
        recv_report(&mut rx, "busy").await;
        drop(tunnel);
        recv_report(&mut rx, "idle").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overlapping_init_call_and_tunnel_each_report_busy() {
        const ENV: &str = "ADX_RRT_ACTIVITY_OVERLAPPING_TUNNEL_ISOLATED";
        const TEST: &str =
            "runtime::activity::tests::overlapping_init_call_and_tunnel_each_report_busy";
        if !run_in_isolated_process(ENV, TEST) {
            return;
        }

        let (tx, mut rx) = mpsc::channel(8);
        init_reporter("sandbox-under-test".to_string(), tx);

        let init_call = enter(ActivitySource::RuntimeRpc);
        recv_report(&mut rx, "busy").await;
        let tunnel = enter(ActivitySource::Tunnel);
        recv_report(&mut rx, "busy").await;

        let poll = enter(ActivitySource::DirectHttp);
        recv_report(&mut rx, "busy").await;
        drop(poll);

        drop(init_call);
        tokio::time::sleep(IDLE_REPORT_DEBOUNCE + std::time::Duration::from_millis(100)).await;
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        drop(tunnel);
        recv_report(&mut rx, "idle").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_direct_polling_reasserts_busy_before_debounced_idle() {
        const ENV: &str = "ADX_RRT_ACTIVITY_REPEATED_POLL_ISOLATED";
        const TEST: &str =
            "runtime::activity::tests::repeated_direct_polling_reasserts_busy_before_debounced_idle";
        if !run_in_isolated_process(ENV, TEST) {
            return;
        }

        let (tx, mut rx) = mpsc::channel(8);
        init_reporter("sandbox-under-test".to_string(), tx);

        for _ in 0..3 {
            let poll = enter(ActivitySource::DirectHttp);
            recv_report(&mut rx, "busy").await;
            drop(poll);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        recv_report(&mut rx, "idle").await;
    }
}
