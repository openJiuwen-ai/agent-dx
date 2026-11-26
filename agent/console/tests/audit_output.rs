use std::process::Command;

#[test]
fn independent_binary_initializes_structured_audit_before_loading_config() {
    let output=Command::new(env!("CARGO_BIN_EXE_adx-agent-console"))
        .env_remove("ADX_CONSOLE_CONFIG").output().unwrap();
    assert!(!output.status.success());
    let logs=String::from_utf8_lossy(&output.stdout);
    assert!(logs.contains("console.starting"),"missing startup audit: {logs}");
    assert!(logs.lines().any(|line|serde_json::from_str::<serde_json::Value>(line).is_ok()));
}
