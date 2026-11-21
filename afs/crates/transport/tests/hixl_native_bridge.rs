//! Runs the real bridge with fake CANN entrypoints and official SDK headers.
#![cfg(feature = "hixl")]

use std::{fs, path::PathBuf, process::Command};

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
        .arg("-DAFS_HIXL_TEST_FAULT_INJECTION")
        .arg(format!("-I{}", sdk.join("include").display()))
        .arg(root.join("tests/native/hixl_bridge_test.cc"))
        .arg(root.join("native/hixl.cc"))
        .arg(root.join("native/hixl_numa.cc"));
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
        .args(["-lgraph", "-lmetadef", "-ldl"])
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
        "roce-pre-dispatch-bad-alloc",
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
        "hccs-pre-dispatch-bad-alloc",
        "hccs-invalid-pool",
        "hccs-unsupported-soc",
        "hccs-null-soc",
        "hccs-roce-conflict",
        "hccs-inter-roce-conflict",
        "hccs-unknown-domain",
        "hccs-negative-domain",
        "reserve-export",
        "hccs-pinned-read",
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

    const PCI_KNOWN: &[(&str, &str)] = &[("0001:3a:0b.2", "3\n")];
    const PCI_UNKNOWN: &[(&str, &str)] = &[("0001:3a:0b.2", "-1\n")];
    const PCI_AMBIGUOUS: &[(&str, &str)] = &[("0000:3a:0b.2", "3\n"), ("0001:3a:0b.2", "4\n")];
    let mut failures = Vec::new();
    for (scenario, pci_devices, source, node, policy) in [
        ("numa-worker", PCI_UNKNOWN, "worker_cpu", 4, "preferred"),
        ("numa-roce", PCI_KNOWN, "pci", 3, "preferred"),
        ("numa-relay", PCI_KNOWN, "pci", 3, "preferred"),
        ("numa-ambiguous", PCI_AMBIGUOUS, "unknown", -1, "inherited"),
        ("numa-unknown", PCI_UNKNOWN, "unknown", -1, "inherited"),
        ("numa-worker-multi", PCI_UNKNOWN, "unknown", -1, "inherited"),
        (
            "numa-worker-partial",
            PCI_UNKNOWN,
            "unknown",
            -1,
            "inherited",
        ),
        (
            "numa-worker-ambiguous",
            PCI_UNKNOWN,
            "unknown",
            -1,
            "inherited",
        ),
        ("numa-worker-empty", PCI_UNKNOWN, "unknown", -1, "inherited"),
        (
            "numa-worker-failed",
            PCI_UNKNOWN,
            "unknown",
            -1,
            "inherited",
        ),
        (
            "numa-worker-non-default",
            PCI_UNKNOWN,
            "worker_cpu",
            4,
            "inherited",
        ),
        ("numa-non-default", PCI_KNOWN, "pci", 3, "inherited"),
        ("numa-set-rejected", PCI_KNOWN, "pci", 3, "inherited"),
        ("numa-alloc-failure", PCI_KNOWN, "pci", 3, "preferred"),
    ] {
        let topology = scratch.path().join(scenario);
        let pci_root = topology.join("pci");
        let cpu_root = topology.join("cpu");
        fs::create_dir_all(&cpu_root).unwrap();
        for (bdf, node) in pci_devices {
            let device = pci_root.join(bdf);
            fs::create_dir_all(&device).unwrap();
            fs::write(device.join("numa_node"), node).unwrap();
        }

        let mut run = Command::new(&executable);
        run.arg(scenario).arg(pci_root).arg(&cpu_root);
        if matches!(
            scenario,
            "numa-ambiguous" | "numa-unknown" | "numa-worker-failed"
        ) {
            run.arg("failure");
        } else {
            run.arg("mask");
            if scenario != "numa-worker-empty" {
                // Reproduce the worker's CPU 320–359 affinity, with node 4
                // even when a known PCI device resolves to node 3.
                let linked_node = topology.join("node4");
                fs::create_dir(&linked_node).unwrap();
                for cpu in 320..=359 {
                    run.arg(cpu.to_string());
                    let cpu_dir = cpu_root.join(format!("cpu{cpu}"));
                    fs::create_dir(&cpu_dir).unwrap();
                    match (scenario, cpu) {
                        ("numa-worker-partial", 359) => {}
                        ("numa-worker-multi", 359) => {
                            fs::create_dir(cpu_dir.join("node5")).unwrap()
                        }
                        ("numa-worker-ambiguous", 359) => {
                            fs::create_dir(cpu_dir.join("node4")).unwrap();
                            fs::create_dir(cpu_dir.join("node5")).unwrap();
                        }
                        (_, 320) => {
                            std::os::unix::fs::symlink(&linked_node, cpu_dir.join("node4")).unwrap()
                        }
                        _ => fs::create_dir(cpu_dir.join("node4")).unwrap(),
                    }
                }
            }
        }

        let output = run.output().unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !output.status.success() {
            failures.push(format!("{scenario}: {}\n{stderr}", output.status));
            continue;
        }
        let expected = [
            "device=2".to_owned(),
            format!("source={source}"),
            format!("node={node}"),
            format!("policy={policy}"),
        ];
        if !stderr.lines().any(|line| {
            line.starts_with("AFS_HIXL_NUMA ")
                && expected
                    .iter()
                    .all(|field| line.split_ascii_whitespace().any(|actual| actual == field))
        }) {
            failures.push(format!(
                "{scenario}: expected startup log with {expected:?}\n{stderr}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
