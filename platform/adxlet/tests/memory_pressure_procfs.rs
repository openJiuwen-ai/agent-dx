#![cfg(target_os = "linux")]

use adxlet::resources::{
    detect, pressure, Pressure, PressureGate, PressurePolicy, PressureResources,
};
use std::{path::Path, process::Command};

const MEMORY_ONLY: PressureResources = PressureResources {
    memory_enabled: true,
    disk_enabled: false,
};

// Re-exec each test in a private mount namespace so real sampler paths can be
// replaced without changing the test runner's or host's procfs mounts.
fn isolated(test: &str, check: impl FnOnce()) {
    if std::env::var("ADX_TEST_PRESSURE_PROCFS").as_deref() == Ok(test) {
        check();
        return;
    }
    let output = Command::new("unshare")
        .args(["--mount", "--propagation", "private", "--"])
        .arg(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", test, "--nocapture"])
        .env("ADX_TEST_PRESSURE_PROCFS", test)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "isolated test failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

struct Procfs {
    directory: tempfile::TempDir,
}

impl Procfs {
    fn new() -> Self {
        let fixture = Self {
            directory: tempfile::tempdir().unwrap(),
        };
        for name in ["meminfo", "cgroup", "mountinfo"] {
            let source = fixture.directory.path().join(name);
            std::fs::write(&source, "").unwrap();
            let target = if name == "meminfo" {
                "/proc/meminfo".to_owned()
            } else {
                format!("/proc/{}/{name}", std::process::id())
            };
            let output = Command::new("mount")
                .arg("--bind")
                .arg(source)
                .arg(target)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "bind procfs fixture: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        fixture.memory(30);
        fixture
    }

    fn write(&self, name: &str, value: &str) {
        std::fs::write(self.directory.path().join(name), value).unwrap();
    }

    fn memory(&self, used_percent: u8) {
        self.write(
            "meminfo",
            &format!(
                "MemTotal:       100000 kB\nMemFree:             0 kB\nMemAvailable:   {} kB\n",
                (100 - u64::from(used_percent)) * 1000
            ),
        );
    }

    fn cgroup(&self, v2: bool) -> (String, String) {
        let root = self.directory.path().join("cg");
        let service = root.join("system.slice/adx.service");
        std::fs::create_dir_all(&service).unwrap();
        let (limit, usage) = if v2 {
            ("memory.max", "memory.current")
        } else {
            ("memory.limit_in_bytes", "memory.usage_in_bytes")
        };
        for path in [&root, &root.join("system.slice"), &service] {
            std::fs::write(path.join(limit), "1000000\n").unwrap();
            std::fs::write(path.join(usage), "100000\n").unwrap();
        }
        self.write(
            "cgroup",
            if v2 {
                "0::/system.slice/adx.service\n"
            } else {
                "5:memory:/system.slice/adx.service\n"
            },
        );
        self.write(
            "mountinfo",
            &format!(
                "36 29 0:32 / {} rw - {} cgroup rw,memory\n",
                root.display(),
                if v2 { "cgroup2" } else { "cgroup" }
            ),
        );
        (limit.to_owned(), usage.to_owned())
    }
}

#[test]
#[ignore = "requires Linux CAP_SYS_ADMIN and util-linux unshare/mount"]
fn host_memory_pressure_ignores_cgroup_usage_and_read_errors() {
    isolated(
        "host_memory_pressure_ignores_cgroup_usage_and_read_errors",
        || {
            let fixture = Procfs::new();
            for v2 in [false, true] {
                let (limit, usage) = fixture.cgroup(v2);
                // Capacity detection still honors the cgroup allocation limit.
                assert_eq!(
                    detect(Path::new("/tmp")).unwrap().capacity.memory_bytes,
                    1000000
                );
                for group in ["cg/system.slice", "cg/system.slice/adx.service"] {
                    let usage = format!("{group}/{usage}");
                    for value in ["950000\n", "invalid\n"] {
                        fixture.write(&usage, value);
                        let sample = pressure(MEMORY_ONLY, None).unwrap();
                        assert_eq!(sample.memory_percent, 30, "v2={v2}, {usage}={value}");
                        assert_eq!(sample.disk_percent, 0);
                    }
                    std::fs::remove_file(fixture.directory.path().join(&usage)).unwrap();
                    assert_eq!(pressure(MEMORY_ONLY, None).unwrap().memory_percent, 30);
                    fixture.write(&format!("{group}/{limit}"), "invalid\n");
                    assert_eq!(pressure(MEMORY_ONLY, None).unwrap().memory_percent, 30);
                }
            }
        },
    );
}

#[test]
#[ignore = "requires Linux CAP_SYS_ADMIN and util-linux unshare/mount"]
fn host_memory_pressure_ignores_reclaimable_cgroup_file_cache() {
    isolated(
        "host_memory_pressure_ignores_reclaimable_cgroup_file_cache",
        || {
            let fixture = Procfs::new();
            fixture.cgroup(true);
            let mib = 1024_u64 * 1024;
            // A 2 GiB service slice at 92% usage, mostly inactive file cache.
            for group in ["cg", "cg/system.slice/adx.service"] {
                fixture.write(&format!("{group}/memory.max"), "max\n");
            }
            fixture.write("cg/memory.current", &(1890 * mib).to_string());
            fixture.write(
                "cg/system.slice/adx.service/memory.current",
                &(100 * mib).to_string(),
            );
            fixture.write("cg/system.slice/memory.max", &(2048 * mib).to_string());
            fixture.write("cg/system.slice/memory.current", &(1890 * mib).to_string());
            fixture.write(
                "cg/system.slice/memory.stat",
                &format!(
                    "anon {}\nfile {}\ninactive_file {}\n",
                    100 * mib,
                    1680 * mib,
                    1640 * mib
                ),
            );
            for disk_percent in [9, 27] {
                // Configurations that omit resource flags enable both resources.
                // Start from a latched state before the node observation.
                let mut gate = PressureGate::new(PressurePolicy::default()).unwrap();
                for memory_percent in [90, 89] {
                    assert!(
                        !gate
                            .update(Pressure {
                                memory_percent,
                                disk_percent
                            })
                            .accepting
                    );
                }
                for node_percent in [3, 4, 5] {
                    fixture.write(
                        "meminfo",
                        &format!(
                            "MemTotal: 100000000 kB\nMemFree: 0 kB\nMemAvailable: {} kB\n",
                            (100 - u64::from(node_percent)) * 1000000
                        ),
                    );
                    let sample = pressure(MEMORY_ONLY, None).unwrap();
                    assert_eq!(sample.memory_percent, node_percent);
                    let update = gate.update(Pressure {
                        disk_percent,
                        ..sample
                    });
                    assert!(update.accepting);
                }
            }
        },
    );
}

#[test]
#[ignore = "requires Linux CAP_SYS_ADMIN and util-linux unshare/mount"]
fn host_memory_pressure_does_not_require_cgroup_metadata() {
    isolated(
        "host_memory_pressure_does_not_require_cgroup_metadata",
        || {
            let fixture = Procfs::new();
            fixture.cgroup(true);
            fixture.write("cgroup", "0::/outside-the-mount\n");
            let root = fixture.directory.path().join("cg");
            fixture.write(
                "mountinfo",
                &format!(
                    "36 29 0:32 /different-root {} rw - cgroup2 cgroup rw\n",
                    root.display()
                ),
            );
            // The old cgroup resolver rejects this mismatch before returning a sample.
            assert!(detect(Path::new("/tmp")).is_err());
            assert_eq!(pressure(MEMORY_ONLY, None).unwrap().memory_percent, 30);
        },
    );
}

#[test]
#[ignore = "requires Linux CAP_SYS_ADMIN and util-linux unshare/mount"]
fn host_memory_pressure_preserves_hysteresis_and_failed_sample_admission() {
    isolated(
        "host_memory_pressure_preserves_hysteresis_and_failed_sample_admission",
        || {
            let fixture = Procfs::new();
            fixture.cgroup(true);
            let mut gate =
                PressureGate::with_resources(PressurePolicy::default(), MEMORY_ONLY).unwrap();
            for (percent, accepting) in [
                (89, true),
                (90, false),
                (85, false),
                (81, false),
                (80, true),
                (85, true),
                (95, false),
            ] {
                fixture.memory(percent);
                let sample = pressure(MEMORY_ONLY, None).unwrap();
                assert_eq!(sample.memory_percent, percent);
                assert_eq!(gate.update(sample).accepting, accepting, "host={percent}");
            }
            for meminfo in [
                "MemTotal: 100000 kB\n",
                "MemTotal: 100000 kB\nMemAvailable: invalid kB\n",
                "MemAvailable: 15000 kB\n",
            ] {
                fixture.write("meminfo", meminfo);
                assert!(pressure(MEMORY_ONLY, None).is_err());
                assert!(gate.fail().accepting);
            }
            fixture.memory(85);
            let recovered = gate.update(pressure(MEMORY_ONLY, None).unwrap());
            assert!(recovered.accepting);
            assert_eq!(recovered.state.consecutive_failures, 0);
            fixture.memory(90);
            assert!(!gate.update(pressure(MEMORY_ONLY, None).unwrap()).accepting);
            fixture.memory(80);
            assert!(gate.update(pressure(MEMORY_ONLY, None).unwrap()).accepting);
        },
    );
}
