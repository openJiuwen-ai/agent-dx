//! Process-owned observability and bounded service lifecycle. No business authority here.
//!
//! 三种公共设施分工：logging 输出事件；metrics 聚合次数；tracing 关联一次跨服务请求。
//! 每个进程只初始化一次全局日志/Trace，SDK 复用调用进程的上下文，不自行安装 subscriber。
//! Services 管 Tokio 长运行任务；FUSE BackgroundSession 与本机 UDS 的资源归 Node 管。
//! 不创建全局 actor 队列：不同 RPC 可并发，只有具体业务需要的状态才局部加锁。

use crate::config::Config;
use afs_metrics::{IntCounterVec, Opts, Registry};
use std::{future::Future, time::Duration};
use tokio::{sync::watch, task::JoinSet};
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ServiceResult = Result<(), BoxError>;

/// Covers service drain, FUSE cleanup, runtime destruction and observability
/// teardown. It does not cancel a blocking syscall or an uncertain commit.
/// The process is terminated with a failure status if graceful stop cannot
/// complete; crash recovery then uses only acknowledged durability barriers.
pub struct ShutdownDeadline {
    trigger: ShutdownTrigger,
    worker: std::thread::JoinHandle<()>,
    signal_observer: Option<ProcessSignalObserver>,
}

/// Only process entry opts in; ordinary embedded guards do not install signals.
struct ProcessSignalObserver {
    stop: tokio::sync::oneshot::Sender<()>,
    worker: std::thread::JoinHandle<()>,
}

#[derive(Clone)]
pub struct ShutdownTrigger {
    sender: std::sync::mpsc::Sender<ShutdownMessage>,
    armed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    budget: Duration,
}

enum ShutdownMessage {
    Arm(std::time::Instant),
    Complete,
}

impl ShutdownDeadline {
    pub fn new(budget: Duration) -> std::io::Result<Self> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("afs-shutdown-deadline".into())
            .spawn(move || {
                let deadline = match receiver.recv() {
                    Ok(ShutdownMessage::Arm(deadline)) => deadline,
                    Ok(ShutdownMessage::Complete) | Err(_) => return,
                };
                loop {
                    match receiver
                        .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                    {
                        Ok(ShutdownMessage::Complete) => return,
                        // Repeated signals cannot extend the original deadline.
                        Ok(ShutdownMessage::Arm(_)) => {}
                        Err(_) => {
                            // No logging/disk flush on this thread: either could
                            // block at the boundary this guard must enforce.
                            // Exit 124 is deliberately not a clean-stop status.
                            std::process::exit(124);
                        }
                    }
                }
            })?;
        Ok(Self {
            trigger: ShutdownTrigger {
                sender,
                armed: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                budget,
            },
            worker,
            signal_observer: None,
        })
    }

    /// Arm on process signals independently of business executor scheduling.
    /// Registration is complete before returning to the process entry. Tokio
    /// broadcasts each signal to all listeners; Services still drains normally.
    pub fn for_process(budget: Duration) -> std::io::Result<Self> {
        let mut deadline = Self::new(budget)?;
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()?;
        let trigger = deadline.trigger();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
        let worker = std::thread::Builder::new()
            .name("afs-shutdown-signals".into())
            .spawn(move || {
                executor.block_on(async move {
                    use tokio::signal::unix::{SignalKind, signal};
                    let registered = signal(SignalKind::terminate()).and_then(|terminate| {
                        signal(SignalKind::interrupt()).map(|interrupt| (terminate, interrupt))
                    });
                    let (mut terminate, mut interrupt) = match registered {
                        Ok(signals) => signals,
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                            return;
                        }
                    };
                    if ready_tx.send(Ok(())).is_err() {
                        return;
                    }
                    loop {
                        tokio::select! {
                            _ = &mut stop_rx => return,
                            _ = terminate.recv() => trigger.arm(),
                            _ = interrupt.recv() => trigger.arm(),
                        }
                    }
                });
            })?;
        deadline.signal_observer = Some(ProcessSignalObserver {
            stop: stop_tx,
            worker,
        });
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(deadline),
            ready => {
                deadline.complete();
                Err(match ready {
                    Ok(Err(error)) => error,
                    _ => {
                        std::io::Error::other("process signal observer exited before registration")
                    }
                })
            }
        }
    }

    pub fn trigger(&self) -> ShutdownTrigger {
        self.trigger.clone()
    }

    /// Call only after all process-owned resources, including the Tokio
    /// runtime and logging guards, have completed their teardown.
    pub fn complete(self) {
        if let Some(observer) = self.signal_observer {
            let _ = observer.stop.send(());
            let _ = observer.worker.join();
        }
        let _ = self.trigger.sender.send(ShutdownMessage::Complete);
        let _ = self.worker.join();
    }
}

impl ShutdownTrigger {
    pub fn arm(&self) {
        if !self.armed.swap(true, std::sync::atomic::Ordering::AcqRel) {
            let _ = self.sender.send(ShutdownMessage::Arm(
                std::time::Instant::now() + self.budget,
            ));
        }
    }
}
#[derive(Clone)]
/// 每进程独立的指标注册表和基础请求计数；不把文件路径等高基数字段当指标标签。
pub struct Observability {
    pub registry: Registry,
    requests: IntCounterVec,
}
impl Observability {
    pub fn new() -> Result<Self, BoxError> {
        let registry = Registry::new();
        let requests = IntCounterVec::new(
            Opts::new("afs_requests_total", "Completed foundation operations"),
            &["component", "operation", "result"],
        )?;
        registry.register(Box::new(requests.clone()))?;
        Ok(Self { registry, requests })
    }
    /// All callers use bounded static labels; never put paths or request IDs in labels.
    pub fn record(&self, component: &'static str, operation: &'static str, ok: bool) {
        self.requests
            .with_label_values(&[component, operation, if ok { "ok" } else { "error" }])
            .inc();
    }
}
/// 进程退出前保留的日志/Trace 运行句柄；Drop 负责对应观测设施的收尾。
pub struct ProcessGuards {
    _logging: afs_logging::LoggingGuard,
    _tracing: afs_tracing::TracingGuard,
}
pub fn initialize(
    cfg: &Config,
    service: &str,
    obs: &Observability,
) -> Result<ProcessGuards, BoxError> {
    let _logging = afs_logging::init_process_logging(
        &afs_logging::LoggingConfig {
            level: afs_logging::parse_level(&cfg.log_level)?,
            ..Default::default()
        },
        afs_logging::ProcessIdentity::new(service, &cfg.id),
    )?;
    let _tracing = afs_tracing::init_process_tracing(
        &afs_tracing::TracingConfig {
            enabled: cfg.trace_enabled,
            otlp_endpoint: cfg.trace_endpoint.clone(),
            sample_ratio: cfg.trace_sample_ratio,
            ..Default::default()
        },
        afs_tracing::ProcessIdentity::new(service, &cfg.id),
        Some(afs_metrics::TraceRuntimeMetrics::register(&obs.registry)?),
    )?;
    Ok(ProcessGuards { _logging, _tracing })
}
pub async fn cancelled(mut rx: watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            break;
        }
    }
}
/// 一组同生共死的异步服务。任一服务意外结束应让进程退出，避免只剩 health 正常。
pub struct Services {
    pub stop: watch::Sender<bool>,
    tasks: JoinSet<ServiceResult>,
}
impl Default for Services {
    fn default() -> Self {
        Self::new()
    }
}
impl Services {
    pub fn new() -> Self {
        let (stop, _) = watch::channel(false);
        Self {
            stop,
            tasks: JoinSet::new(),
        }
    }
    pub fn spawn(&mut self, future: impl Future<Output = ServiceResult> + Send + 'static) {
        self.tasks.spawn(future);
    }
    /// 等待 SIGINT/SIGTERM 或服务退出，然后广播停止并限时排空。
    /// 超时会中止异步任务并返回错误；不把强制退出报告成正常关闭。
    pub async fn run(self) -> ServiceResult {
        self.run_with_shutdown(|| {}).await
    }

    pub async fn run_with_shutdown(mut self, on_shutdown: impl FnOnce()) -> ServiceResult {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let result = tokio::select! {
            result=tokio::signal::ctrl_c()=>result.map_err(Into::into),
            _=terminate.recv()=>Ok(()),
            task=self.tasks.join_next()=>match task {
                Some(Ok(Err(e)))=>Err(e),Some(Err(e))=>Err(e.into()),
                _=>Err(std::io::Error::other("service exited unexpectedly").into()),
            }
        };
        on_shutdown();
        let _ = self.stop.send(true);
        let mut failure = None;
        let drained = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(task) = self.tasks.join_next().await {
                match task {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => failure = Some(error),
                    Err(error) => failure = Some(error.into()),
                }
            }
        })
        .await;
        if drained.is_err() {
            self.tasks.abort_all();
            let _ = tokio::time::timeout(Duration::from_secs(2), async {
                while self.tasks.join_next().await.is_some() {}
            })
            .await;
            afs_logging::error!("process.shutdown_forced");
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "service shutdown exceeded deadline; tasks aborted",
            )
            .into());
        }
        if let Some(error) = failure {
            return Err(error);
        }
        afs_logging::info!("services.stopped");
        result
    }
}

#[cfg(test)]
mod shutdown_deadline_tests {
    use super::*;

    #[test]
    fn shutdown_deadline_child() {
        let Ok(mode) = std::env::var("AFS_TEST_SHUTDOWN_CHILD") else {
            return;
        };
        if mode == "signal-graceful-term" || mode == "signal-graceful-int" {
            let deadline = ShutdownDeadline::for_process(Duration::from_millis(500)).unwrap();
            let trigger = deadline.trigger();
            let executor = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let (registered_tx, registered_rx) = std::sync::mpsc::channel();
            let sender = std::thread::spawn(move || {
                registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                let signal = if mode == "signal-graceful-term" {
                    "-TERM"
                } else {
                    "-INT"
                };
                assert!(
                    std::process::Command::new("kill")
                        .args([signal, &std::process::id().to_string()])
                        .status()
                        .unwrap()
                        .success()
                );
            });
            executor.block_on(async {
                let mut services = Services::new();
                let stop = services.stop.subscribe();
                services.spawn(async move {
                    registered_tx.send(()).unwrap();
                    cancelled(stop).await;
                    Ok(())
                });
                services
                    .run_with_shutdown(move || trigger.arm())
                    .await
                    .unwrap();
            });
            sender.join().unwrap();
            drop(executor);
            deadline.complete();
            // A cancelled/joined observer and disarmed watchdog cannot force a
            // healthy process to fail later, despite having received a signal.
            std::thread::sleep(Duration::from_millis(600));
            return;
        }
        if mode == "signal-starved" {
            let deadline = ShutdownDeadline::for_process(Duration::from_millis(150)).unwrap();
            let executor = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let (registered_tx, registered_rx) = std::sync::mpsc::channel();
            let sender = std::thread::spawn(move || {
                registered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                let status = std::process::Command::new("kill")
                    .args(["-TERM", &std::process::id().to_string()])
                    .status()
                    .unwrap();
                assert!(status.success());
            });
            executor.block_on(async {
                // The business reactor deliberately cannot poll the signal.
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .unwrap();
                registered_tx.send(()).unwrap();
                std::thread::sleep(Duration::from_secs(5));
                terminate.recv().await;
                deadline.trigger().arm();
            });
            sender.join().unwrap();
            drop(executor);
            deadline.complete();
            return;
        }
        let deadline = ShutdownDeadline::new(Duration::from_millis(150)).unwrap();
        let trigger = deadline.trigger();
        if mode == "complete" {
            trigger.arm();
            deadline.complete();
            std::thread::sleep(Duration::from_millis(250));
            return;
        }
        let executor = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        executor.spawn_blocking(move || {
            started_tx.send(()).unwrap();
            loop {
                std::thread::park();
            }
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        trigger.arm();
        std::thread::sleep(Duration::from_millis(100));
        trigger.arm();
        // This drop waits forever without a native process deadline.
        drop(executor);
        panic!("blocking runtime unexpectedly finished");
    }

    fn child(mode: &str) -> (std::process::ExitStatus, Duration) {
        let start = std::time::Instant::now();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::shutdown_deadline_tests::shutdown_deadline_child",
                "--nocapture",
            ])
            .env("AFS_TEST_SHUTDOWN_CHILD", mode)
            .spawn()
            .unwrap();
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return (status, start.elapsed());
            }
            if start.elapsed() > Duration::from_secs(3) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("shutdown child failed to exit within 3s");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn shutdown_deadline_forces_failure_even_during_runtime_drop() {
        let (status, elapsed) = child("blocked");
        assert_eq!(status.code(), Some(124));
        assert!(elapsed < Duration::from_secs(2));
    }

    #[test]
    fn shutdown_deadline_covers_signal_when_business_reactor_is_blocked() {
        let (status, elapsed) = child("signal-starved");
        assert_eq!(status.code(), Some(124));
        assert!(elapsed < Duration::from_secs(2));
    }

    #[test]
    fn process_signal_observer_does_not_steal_graceful_service_signals() {
        for mode in ["signal-graceful-term", "signal-graceful-int"] {
            let (status, _) = child(mode);
            assert!(status.success(), "{mode}: {status}");
        }
    }

    #[test]
    fn shutdown_deadline_completion_disarms_process_failure() {
        let (status, _) = child("complete");
        assert!(status.success());
        ShutdownDeadline::new(Duration::from_millis(1))
            .unwrap()
            .complete();
    }
}
