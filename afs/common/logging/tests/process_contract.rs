use afs_logging::{LogOutput, LoggingConfig, ProcessIdentity, init_process_logging};
use afs_tracing::{TracingConfig, init_process_tracing, tracing};

// 每个配置使用新进程，避免其它测试曾注册 Subscriber 后掩盖 tracing/log 回退。
#[test]
fn trace_modes_preserve_business_logs_without_span_fallback() {
    for mode in ["off", "0", "1"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process_probe", "--nocapture"])
            .env("AFS_LOGGING_PROCESS_TEST", mode)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn process_probe() {
    let Ok(mode) = std::env::var("AFS_LOGGING_PROCESS_TEST") else {
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("process.log");
    let logging = init_process_logging(
        &LoggingConfig {
            output: LogOutput::File(path.clone()),
            ..LoggingConfig::default()
        },
        ProcessIdentity::new("afs-test", "isolated"),
    )
    .unwrap();
    let tracing_guard = init_process_tracing(
        &TracingConfig {
            enabled: mode != "off",
            sample_ratio: mode.parse().unwrap_or(0.0),
            ..TracingConfig::default()
        },
        afs_tracing::ProcessIdentity::new("afs-test", "isolated"),
        None,
    )
    .unwrap();
    for _ in 0..32 {
        let span = tracing::info_span!(target: "afs_tracing", "rpc_fallback_sentinel", transport.result = tracing::field::Empty);
        let _entered = span.enter();
        assert_eq!(afs_tracing::current_exemplar().is_some(), mode == "1");
        span.record("transport.result", "ok");
    }
    afs_logging::info!("business_ready_sentinel");
    log::error!("business_error_sentinel");
    drop(tracing_guard);
    drop(logging);
    let records = std::fs::read_to_string(path).unwrap();
    assert!(!records.contains("rpc_fallback_sentinel"), "{records}");
    assert!(!records.contains("transport.result"), "{records}");
    assert!(records.contains("business_ready_sentinel"));
    assert!(records.contains("business_error_sentinel"));
}

#[test]
fn debug_filter_precedes_argument_evaluation() {
    if std::env::var_os("AFS_DEBUG_GATE_TEST").is_some() {
        let logging = init_process_logging(
            &LoggingConfig::default(),
            ProcessIdentity::new("afs-test", "gate"),
        )
        .unwrap();
        let evaluated = std::cell::Cell::new(false);
        afs_logging::debug!("filtered"; "value" => { evaluated.set(true); 42 });
        assert!(!evaluated.get());
        logging.set_level(afs_logging::slog::Level::Debug);
        afs_logging::debug!("enabled"; "value" => { evaluated.set(true); 42 });
        assert!(evaluated.get());
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "debug_filter_precedes_argument_evaluation",
            "--nocapture",
        ])
        .env("AFS_DEBUG_GATE_TEST", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}
