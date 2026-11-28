//! Exercises provider selection and ownership against fake ACL/ADXL calls on Linux.
#![cfg(all(feature = "hixl", target_os = "linux"))]

use std::{fs, path::PathBuf, process::Command};

fn checked(command: &mut Command) {
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{command:?}: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "requires CANN headers and a Linux C++ compiler"]
fn native_fabric_allocator_selects_provider_and_tracks_cleanup() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let native = root.join("native");
    let fixtures = root.join("tests/native");
    let configured = PathBuf::from(std::env::var_os("ADX_CANN_PATH").expect("set ADX_CANN_PATH"));
    let target = configured.join(format!("{}-linux", std::env::consts::ARCH));
    let sdk = if target.join("include").is_dir() {
        target
    } else {
        configured
    };
    let compiler = std::env::var_os("CXX").unwrap_or_else(|| "c++".into());
    let scratch = tempfile::tempdir().unwrap();
    let path = scratch.path();
    let compile = || {
        let mut command = Command::new(&compiler);
        command.args(["-std=c++17", "-Wall", "-Wextra", "-Werror"]);
        command
    };
    let mut adapter = compile();
    adapter
        .args(["-fPIC", "-shared", "-Wl,-z,defs"])
        .arg(format!(
            "-Wl,--version-script,{}",
            native.join("hixl-fabric-module.map").display()
        ))
        .arg(native.join("hixl_fabric_adxl.cc"))
        .args(["-ldl", "-o"])
        .arg(path.join("libafs_hixl_fabric.so"));
    checked(&mut adapter);

    let probe = Command::new(&compiler)
        .args(["-std=c++17", "-isystem"])
        .arg(sdk.join("include"))
        .arg("-fsyntax-only")
        .arg(native.join("hixl_fabric_vmm_probe.cc"))
        .output()
        .unwrap();
    let has_vmm = probe.status.success();
    if !has_vmm {
        eprintln!(
            "CANN SDK lacks ACL external FabricMem VMM declarations; running ADXL-only cases"
        );
    }
    let executables = [
        ("allocator", &["-DAFS_HIXL_HAVE_ACL_FABRIC_VMM"][..]),
        (
            "allocator-missing-retain",
            &[
                "-DAFS_HIXL_HAVE_ACL_FABRIC_VMM",
                "-DAFS_FABRIC_TEST_WITHOUT_RETAIN",
            ],
        ),
        ("allocator-no-vmm", &[]),
    ];
    for (executable, flags) in executables {
        if !has_vmm && !flags.is_empty() {
            continue;
        }
        checked(
            compile()
                .args(flags)
                .arg("-isystem")
                .arg(sdk.join("include"))
                .arg("-rdynamic")
                .arg(fixtures.join("hixl_fabric_allocator_test.cc"))
                .arg(native.join("hixl_fabric.cc"))
                .args(["-ldl", "-o"])
                .arg(path.join(executable)),
        );
    }

    let library = path.join("libllm_datadist.so");
    let stub = |flags: &[&str]| {
        checked(
            compile()
                .args(["-fPIC", "-shared", "-Wl,-z,defs"])
                .args(flags)
                .arg(fixtures.join("hixl_fabric_allocator_adxl_stub.cc"))
                .arg("-o")
                .arg(&library),
        );
    };
    let run = |executable: &str, scenario: &str| {
        checked(
            Command::new(path.join(executable))
                .arg(scenario)
                .env("LD_LIBRARY_PATH", path)
                .env_remove("LD_PRELOAD")
                .env("AFS_FABRIC_ALLOCATOR_SCENARIO", scenario),
        );
    };

    stub(&[]);
    for scenario in [
        "adxl-success",
        "adxl-alloc-failure",
        "adxl-alloc-exception",
        "adxl-free-failure",
    ] {
        run("allocator-no-vmm", scenario);
        if has_vmm {
            run("allocator", scenario);
        }
    }
    if has_vmm {
        run("allocator-missing-retain", "adxl-success");
    }

    stub(&["-DAFS_FABRIC_TEST_PARTIAL_ADXL"]);
    run("allocator-no-vmm", "no-vmm-headers");
    if has_vmm {
        run("allocator-missing-retain", "missing-retain");
        for scenario in [
            "numa-success",
            "fallback",
            "physical-denied",
            "reserve-failure",
            "map-failure",
            "access-failure",
            "short-range",
            "range-unsupported",
            "range-rt-unsupported",
            "range-denied",
            "range-rollback-stuck",
            "retain-failure",
            "drop-retain-failure",
            "rollback-stuck",
            "release-retry",
        ] {
            run("allocator", scenario);
        }
    }

    fs::write(&library, b"not an ELF library").unwrap();
    run("allocator-no-vmm", "broken-adxl-library");
    if has_vmm {
        run("allocator", "broken-adxl-library");
    }
}
