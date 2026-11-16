use std::process::Command;

#[test]
fn service_logs_never_emit_ansi_colors() {
    const CHILD: &str = "ADX_TEST_LOG_OUTPUT_INIT";
    const MESSAGE: &str = "plain service log output";
    if let Ok(initializer) = std::env::var(CHILD) {
        let guard = match initializer.as_str() {
            "service" => Some(adx_observability::logging::init("test-service", false).unwrap()),
            "basic" => {
                adx_observability::init().unwrap();
                None
            }
            _ => panic!("unknown logging initializer"),
        };
        adx_observability::info!(answer = 42, "{MESSAGE}");
        if let Some(guard) = guard {
            guard.shutdown().unwrap();
        }
        return;
    }

    for (initializer, format, file) in [
        ("service", "text", false),
        ("service", "json", false),
        ("service", "text", true),
        ("basic", "text", false),
        ("basic", "json", false),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("ADX_") {
                command.env_remove(key);
            }
        }
        command
            .args([
                "--exact",
                "service_logs_never_emit_ansi_colors",
                "--nocapture",
            ])
            .env_remove("NO_COLOR")
            .env(CHILD, initializer)
            .env("ADX_TRACE_ENABLED", "false")
            .env("ADX_LOG_FORMAT", format)
            .env("RUST_LOG", "info");
        if file {
            command.env("ADX_DATA_PLANE_LOG_DIR", directory.path());
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{initializer}/{format}: {output:?}"
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains(MESSAGE), "missing log record: {stdout:?}");
        assert!(
            !stdout.contains('\u{1b}'),
            "{initializer}/{format} emitted ANSI escapes: {stdout:?}"
        );
        if file {
            let text = std::fs::read_to_string(directory.path().join("test-service.log")).unwrap();
            assert!(text.contains(MESSAGE), "missing file log record: {text:?}");
            assert!(
                !text.contains('\u{1b}'),
                "file emitted ANSI escapes: {text:?}"
            );
        }
    }
}
