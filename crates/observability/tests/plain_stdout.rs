#[test]
fn plain_stdout_contains_no_ansi_colors() {
    const CHILD: &str = "ADX_TEST_PLAIN_STDOUT";
    if std::env::var_os(CHILD).is_some() {
        let guard = adx_observability::logging::init("plain-test", false).unwrap();
        tracing::info!("plain-stdout-marker");
        guard.shutdown().unwrap();
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "plain_stdout_contains_no_ansi_colors",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("ADX_DATA_PLANE_LOG_STDOUT", "1")
        .env("ADX_LOG_FORMAT", "text")
        .env("RUST_LOG", "info")
        .env_remove("ADX_DATA_PLANE_LOG_DIR")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("plain-stdout-marker"), "{stdout}");
    assert!(!stdout.contains('\u{1b}'), "{stdout:?}");
}
