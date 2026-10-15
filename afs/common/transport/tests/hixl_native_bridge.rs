//! Runs the real bridge with fake CANN entrypoints and official SDK headers.
#![cfg(feature = "hixl")]

use std::{path::PathBuf, process::Command};

#[test]
fn bridge_enforces_device_roce_direction_and_uncertain_submission_retention() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let sdk = PathBuf::from(env!("AFS_HIXL_SDK_PATH"));
    let scratch = tempfile::tempdir().unwrap();
    let executable = scratch.path().join("hixl-bridge-test");
    let compiler = std::env::var_os("CXX").unwrap_or_else(|| "c++".into());
    let mut build = Command::new(compiler);
    build
        .arg("-std=c++17")
        .arg(format!("-I{}", sdk.join("include").display()))
        .arg(root.join("tests/native/hixl_bridge_test.cc"))
        .arg(root.join("native/hixl.cc"));
    for directory in [
        sdk.join("lib64"),
        sdk.join("devlib/linux").join(std::env::consts::ARCH),
        sdk.join("devlib/device"),
    ] {
        if directory.is_dir() {
            build
                .arg(format!("-L{}", directory.display()))
                .arg(format!("-Wl,-rpath-link,{}", directory.display()));
        }
    }
    let output = build
        .args(["-lgraph", "-lmetadef"])
        .arg("-o")
        .arg(&executable)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "native fake-boundary compile failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    for scenario in [
        "roce",
        "roce-listen-port",
        "roce-max-listen-port",
        "roce-invalid-listen-port",
        "unknown-submit",
        "completed",
        "failed-poll",
        "pre-submit-invalid",
        "startup-cleanup-failed",
        "shared-acl-device-leases",
        "shared-acl-same-device-leases",
        "external-acl-init",
        "acl-reset-retry",
        "acl-finalize-retry",
        "hccs-completed",
        "hccs-listen-port",
        "hccs-invalid-listen-port",
        "hccs-timeout",
        "hccs-failed",
        "hccs-exception",
        "hccs-pre-submit-invalid",
        "hccs-invalid-pool",
        "hccs-unsupported-soc",
        "hccs-null-soc",
        "hccs-roce-conflict",
        "hccs-inter-roce-conflict",
        "hccs-unknown-domain",
        "hccs-negative-domain",
        "roce-batch-100mib",
        "hccs-batch-100mib",
    ] {
        let output = Command::new(&executable).arg(scenario).output().unwrap();
        assert!(
            output.status.success(),
            "{scenario}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
