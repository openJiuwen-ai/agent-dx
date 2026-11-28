//! Runs the real bridge with fake CANN and Fabric provider entrypoints and official SDK headers.
#![cfg(feature = "hixl")]

use std::{fs, path::PathBuf, process::Command};

#[test]
#[ignore = "requires the CANN SDK headers and C++ compiler"]
fn bridge_enforces_device_roce_direction_and_uncertain_submission_retention() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let configured = PathBuf::from(std::env::var_os("ADX_CANN_PATH").expect("set ADX_CANN_PATH"));
    let target = configured.join(format!("{}-linux", std::env::consts::ARCH));
    let sdk = if target.join("include").is_dir() {
        target
    } else {
        configured
    };
    let scratch = tempfile::tempdir().unwrap();
    let executable = scratch.path().join("hixl-bridge-test");
    let compiler = std::env::var_os("CXX").unwrap_or_else(|| "c++".into());
    let mut build = Command::new(compiler);
    build
        .arg("-std=c++17")
        .arg("-DAFS_HIXL_TEST_FAULT_INJECTION")
        .arg(format!("-I{}", sdk.join("include").display()))
        // The fixture supplies Provider member definitions for this executable.
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
    let run = |scenario: &str| {
        let output = Command::new(&executable).arg(scenario).output().unwrap();
        assert!(
            output.status.success(),
            "{scenario}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    for case in [
        "listen-port",
        "invalid-listen-port",
        "completed",
        "timeout",
        "failed",
        "exception",
        "pre-submit-invalid",
        "pre-dispatch-bad-alloc",
        "pre-dispatch-deadline",
        "pinned-read",
        "batch-100mib",
        "write-success",
        "write-failed",
        "write-exception",
        "write-timeout",
    ] {
        for transport in ["roce", "hccs"] {
            run(&format!("{transport}-{case}"));
        }
    }
    for scenario in [
        "roce",
        "roce-max-listen-port",
        "roce-pool-completed",
        "roce-pool-listen-port",
        "roce-pool-timeout",
        "roce-pool-write-success",
        "roce-pool-write-timeout",
        "roce-pool-write-exception",
        "roce-pool-missing-intra",
        "roce-pool-missing-inter",
        "roce-pool-pcie-conflict",
        "roce-pool-hccs-conflict",
        "roce-pool-partial",
        "hccs-direct",
        "hccs-partial-pool",
        "startup-cleanup-failed",
        "shared-acl-device-leases",
        "shared-acl-same-device-leases",
        "external-acl-init",
        "acl-reset-retry",
        "acl-finalize-retry",
        "hccs-invalid-pool",
        "hccs-unsupported-soc",
        "hccs-null-soc",
        "hccs-roce-conflict",
        "hccs-inter-roce-conflict",
        "hccs-unknown-domain",
        "hccs-negative-domain",
        "reserve-export",
        "roce-default-slot-count",
        "roce-registration-ceiling",
        "fabric-completed",
        "fabric-capacity",
        "fabric-invalid-pool",
        "fabric-listen-port",
        "fabric-sdk-tuning",
        "fabric-sdk-partial",
        "fabric-sdk-invalid",
        "roce-rejects-fabric-sdk-options",
        "fabric-unsupported-soc",
        "fabric-unknown-domain",
        "fabric-provider-unavailable",
        "fabric-partial-allocate",
        "fabric-partial-register",
        "fabric-start-quarantine",
        "fabric-allocate-exception",
        "fabric-register-exception",
        "fabric-register-timeout",
        "fabric-initialize-timeout",
        "fabric-connect-timeout",
        "fabric-connect-failure",
        "fabric-connect-exception",
        "fabric-pinned-drain",
        "fabric-deregister-retry",
        "fabric-disconnect-timeout",
        "fabric-deregister-timeout",
        "fabric-free-retry",
        "fabric-free-exception",
        "fabric-free-unknown",
        "fabric-write-timeout",
    ] {
        run(scenario);
    }

    const PCI_KNOWN: &[(&str, &str)] = &[("0001:3a:0b.2", "3\n")];
    const PCI_UNKNOWN: &[(&str, &str)] = &[("0001:3a:0b.2", "-1\n")];
    const PCI_AMBIGUOUS: &[(&str, &str)] = &[("0000:3a:0b.2", "3\n"), ("0001:3a:0b.2", "4\n")];
    let mut failures = Vec::new();
    let worker = ("worker_cpu", 4, "preferred");
    let pci = ("pci", 3, "preferred");
    let unknown = ("unknown", -1, "inherited");
    for (scenario, pci_devices, (source, node, policy)) in [
        ("numa-worker", PCI_UNKNOWN, worker),
        ("numa-roce", PCI_KNOWN, pci),
        ("numa-relay", PCI_KNOWN, pci),
        ("numa-ambiguous", PCI_AMBIGUOUS, unknown),
        ("numa-unknown", PCI_UNKNOWN, unknown),
        ("numa-worker-multi", PCI_UNKNOWN, unknown),
        ("numa-worker-partial", PCI_UNKNOWN, unknown),
        ("numa-worker-ambiguous", PCI_UNKNOWN, unknown),
        ("numa-worker-empty", PCI_UNKNOWN, unknown),
        ("numa-worker-failed", PCI_UNKNOWN, unknown),
        (
            "numa-worker-non-default",
            PCI_UNKNOWN,
            ("worker_cpu", 4, "inherited"),
        ),
        ("numa-non-default", PCI_KNOWN, ("pci", 3, "inherited")),
        ("numa-set-rejected", PCI_KNOWN, ("pci", 3, "inherited")),
        ("numa-alloc-failure", PCI_KNOWN, pci),
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
