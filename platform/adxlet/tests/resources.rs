use adx_core::scheduling::DeviceKind;
use adxlet::resources::{
    cpu_quota, cpu_set_count, parse_external, NodePressureState, Pressure, PressureGate,
    PressureLevel, PressurePolicy, ResourcePressure,
};

#[test]
fn independent_resources_ignore_disabled_pressure_during_isolation_and_recovery() {
    use adxlet::resources::PressureResources;
    for memory_enabled in [true, false] {
        let resources = PressureResources {
            memory_enabled,
            disk_enabled: !memory_enabled,
        };
        let mut gate = PressureGate::with_resources(PressurePolicy::default(), resources).unwrap();
        let sample = |enabled_percent| Pressure {
            memory_percent: if memory_enabled { enabled_percent } else { 100 },
            disk_percent: if memory_enabled { 100 } else { enabled_percent },
        };
        assert!(!gate.update(sample(95)).accepting);
        assert!(!gate.update(sample(85)).accepting);
        let recovered = gate.update(sample(70));
        assert!(recovered.accepting);
        assert!(!recovered.state.has_high_pressure());
        let report = adxlet::resources::report_from(&recovered.state).unwrap();
        assert_eq!(report.memory.is_some(), memory_enabled);
        assert_eq!(report.disk.is_some(), !memory_enabled);
        assert!(gate.fail().accepting);
        assert!(gate.update(sample(85)).accepting);
        assert!(!gate.update(sample(95)).accepting);
    }
}

#[test]
fn memory_only_pressure_uses_exact_default_boundaries() {
    use adxlet::resources::PressureResources;
    let mut gate = PressureGate::with_resources(
        PressurePolicy::default(),
        PressureResources {
            memory_enabled: true,
            disk_enabled: false,
        },
    )
    .unwrap();
    for (memory_percent, accepting) in [
        (89, true),
        (90, false),
        (85, false),
        (81, false),
        (80, true),
        (85, true),
    ] {
        let update = gate.update(Pressure {
            memory_percent,
            disk_percent: 100,
        });
        assert_eq!(update.accepting, accepting, "memory={memory_percent}");
    }
}

#[test]
fn disabled_resource_thresholds_are_not_validated() {
    use adxlet::resources::PressureResources;
    let invalid_disk = PressurePolicy {
        disk_low_percent: 100,
        disk_high_percent: 0,
        ..Default::default()
    };
    assert!(PressureGate::new(invalid_disk.clone()).is_err());
    assert!(PressureGate::with_resources(
        invalid_disk,
        PressureResources {
            memory_enabled: true,
            disk_enabled: false,
        }
    )
    .is_ok());
    let invalid_memory = PressurePolicy {
        memory_low_percent: 100,
        memory_high_percent: 0,
        ..Default::default()
    };
    assert!(PressureGate::with_resources(
        invalid_memory,
        PressureResources {
            memory_enabled: false,
            disk_enabled: true,
        }
    )
    .is_ok());
}

#[test]
fn disk_only_sampling_does_not_require_proc_memory_files() {
    use adxlet::resources::{pressure, PressureResources};
    let directory = tempfile::tempdir().unwrap();
    let sample = pressure(
        PressureResources {
            memory_enabled: false,
            disk_enabled: true,
        },
        Some(directory.path()),
    )
    .unwrap();
    assert!(sample.disk_percent <= 100);
    assert_eq!(sample.memory_percent, 0);
}

#[test]
fn disabled_pressure_resources_need_no_probes_or_disk_path() {
    use adxlet::resources::{pressure, PressureResources};
    let resources = PressureResources {
        memory_enabled: false,
        disk_enabled: false,
    };
    let sample = pressure(resources, None).unwrap();
    let mut gate = PressureGate::with_resources(PressurePolicy::default(), resources).unwrap();
    let update = gate.update(sample);
    assert!(update.accepting);
    assert!(!update.state.has_high_pressure());
    assert!(adxlet::resources::report_from(&update.state).is_none());
    assert!(pressure(PressureResources::default(), None).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn memory_only_sampling_skips_missing_disk_paths() {
    use adxlet::resources::{pressure, PressureResources};
    let resources = PressureResources {
        memory_enabled: true,
        disk_enabled: false,
    };
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing");
    for path in [None, Some(missing.as_path())] {
        let sample = pressure(resources, path).unwrap();
        assert!(sample.memory_percent <= 100);
        assert_eq!(sample.disk_percent, 0);
    }
}

#[test]
fn sandboxd_capacity_uses_cores_bytes_and_whole_card_ids() {
    let sample = parse_external(br#"{"cpu":4,"mem":8192,"storage":32768,"features":["storage-quota-v1"],"xpu":[{"type":"npu","product_model":"Ascend","device_ids":[2,5]}]}"#).unwrap();
    assert_eq!(sample.capacity.cpu_millis, 4000);
    assert_eq!(sample.capacity.memory_bytes, 8192);
    assert_eq!(sample.capacity.disk_bytes, 32768);
    assert_eq!(sample.devices[1].id, 5);
    assert_eq!(sample.devices[0].kind, DeviceKind::Npu);
    assert!(parse_external(br#"{"cpu":-1,"mem":10,"xpu":[]}"#).is_err());
}

#[test]
fn cgroup_fractional_quota_and_cpuset_are_not_rounded_up() {
    assert_eq!(cpu_quota("150000 100000").unwrap(), Some(1500));
    assert_eq!(cpu_quota("max 100000").unwrap(), None);
    assert!(cpu_quota("1000 0").is_err());
    assert_eq!(cpu_set_count("0-2,5,7-8").unwrap(), 6);
    assert!(cpu_set_count("2-1").is_err());
    assert!(cpu_set_count("0-2,2").is_err());
}

#[test]
fn pressure_uses_hysteresis_and_does_not_open_on_partial_recovery() {
    let mut gate = PressureGate::new(PressurePolicy::default()).unwrap();
    let pressured = gate.update(Pressure {
        memory_percent: 95,
        disk_percent: 10,
    });
    assert!(!pressured.accepting);
    let memory_band = gate.update(Pressure {
        memory_percent: 85,
        disk_percent: 10,
    });
    assert!(!memory_band.accepting);
    let disk_high = gate.update(Pressure {
        memory_percent: 70,
        disk_percent: 95,
    });
    assert!(!disk_high.accepting);
    let relieved = gate.update(Pressure {
        memory_percent: 70,
        disk_percent: 70,
    });
    assert!(relieved.accepting);
    // Divergence case: memory fully recovers while disk sits inside the
    // hysteresis band. Admission must stay closed because the historical
    // formula requires both resources to fall back below the low threshold.
    let again = gate.update(Pressure {
        memory_percent: 95,
        disk_percent: 10,
    });
    assert!(!again.accepting);
    let partial = gate.update(Pressure {
        memory_percent: 70,
        disk_percent: 85,
    });
    assert!(!partial.accepting);
}

#[test]
fn per_resource_levels_advance_with_independent_hysteresis() {
    let mut gate = PressureGate::new(PressurePolicy::default()).unwrap();
    // Memory crosses the high threshold while disk stays low.
    let update = gate.update(Pressure {
        memory_percent: 95,
        disk_percent: 10,
    });
    assert_eq!(update.state.memory.level, PressureLevel::High);
    assert_eq!(update.state.memory.percent, 95);
    assert_eq!(update.state.disk.level, PressureLevel::Normal);
    assert!(update.state.memory.last_transition_unix_seconds > 0);
    assert_eq!(update.state.disk.last_transition_unix_seconds, 0);
    // Memory falls into the band: the level keeps its previous value.
    let update = gate.update(Pressure {
        memory_percent: 85,
        disk_percent: 10,
    });
    assert_eq!(update.state.memory.level, PressureLevel::High);
    // Memory fully recovers: the level returns to Normal and the transition
    // timestamp moves forward.
    let previous = gate.update(Pressure {
        memory_percent: 70,
        disk_percent: 10,
    });
    assert_eq!(previous.state.memory.level, PressureLevel::Normal);
    // Disk independently crosses high while memory stays Normal.
    let update = gate.update(Pressure {
        memory_percent: 10,
        disk_percent: 95,
    });
    assert_eq!(update.state.memory.level, PressureLevel::Normal);
    assert_eq!(update.state.disk.level, PressureLevel::High);
    // Disk falls into the band: the level keeps High.
    let update = gate.update(Pressure {
        memory_percent: 10,
        disk_percent: 85,
    });
    assert_eq!(update.state.disk.level, PressureLevel::High);
    // The node-level view stays derived, never stored: both fields are
    // observable independently and the combined answer is the caller's.
    assert!(update.state.memory.level != PressureLevel::High);
    assert_eq!(update.state.disk.level, PressureLevel::High);
}

#[test]
fn failed_samples_freeze_levels_and_count_consecutive_failures() {
    let mut gate = PressureGate::new(PressurePolicy::default()).unwrap();
    let observed = gate
        .update(Pressure {
            memory_percent: 95,
            disk_percent: 10,
        })
        .state
        .observed_at_unix_seconds;
    assert!(observed > 0);
    let frozen = gate.fail().state;
    assert_eq!(frozen.consecutive_failures, 1);
    assert_eq!(frozen.observed_at_unix_seconds, observed);
    assert_eq!(frozen.memory.level, PressureLevel::High);
    assert_eq!(frozen.memory.percent, 95);
    let frozen_again = gate.fail().state;
    assert_eq!(frozen_again.consecutive_failures, 2);
    assert_eq!(frozen_again.observed_at_unix_seconds, observed);
    // Failed samples reopen admission while retaining the last observation.
    assert!(gate.fail().accepting);
    let recovered = gate.update(Pressure {
        memory_percent: 70,
        disk_percent: 70,
    });
    assert_eq!(recovered.state.consecutive_failures, 0);
    assert!(recovered.state.observed_at_unix_seconds >= observed);
    assert!(recovered.accepting);
}

#[test]
fn default_state_marks_a_never_observed_node() {
    let state = NodePressureState::default();
    assert_eq!(state.observed_at_unix_seconds, 0);
    assert_eq!(state.memory.level, PressureLevel::Normal);
    assert_eq!(state.memory.percent, 0);
    assert_eq!(state.memory.last_transition_unix_seconds, 0);
    assert_eq!(state.disk.level, PressureLevel::Normal);
    assert_eq!(state.consecutive_failures, 0);
    // A gate with no successful sample keeps the never-observed stamp.
    let mut gate = PressureGate::new(PressurePolicy::default()).unwrap();
    let failed = gate.fail();
    assert_eq!(failed.state.observed_at_unix_seconds, 0);
    assert_eq!(failed.state.consecutive_failures, 1);
    assert_eq!(failed.state.memory.level, PressureLevel::Normal);
}

#[test]
fn pressure_state_round_trips_through_serde() {
    let state = NodePressureState {
        memory: ResourcePressure {
            enabled: true,
            level: PressureLevel::High,
            percent: 95,
            last_transition_unix_seconds: 1_700_000_000,
        },
        disk: ResourcePressure {
            enabled: true,
            level: PressureLevel::Normal,
            percent: 40,
            last_transition_unix_seconds: 0,
        },
        observed_at_unix_seconds: 1_700_000_123,
        consecutive_failures: 2,
    };
    let encoded = serde_json::to_string(&state).unwrap();
    let decoded: NodePressureState = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, state);
}

#[test]
fn pressure_report_round_trips_through_proto_with_all_fields() {
    use prost::Message;
    let state = NodePressureState {
        memory: ResourcePressure {
            enabled: true,
            level: PressureLevel::High,
            percent: 100,
            last_transition_unix_seconds: 1_700_000_000,
        },
        disk: ResourcePressure {
            enabled: true,
            level: PressureLevel::Normal,
            percent: 0,
            last_transition_unix_seconds: 0,
        },
        observed_at_unix_seconds: 1_700_000_123,
        consecutive_failures: 2,
    };
    let report = adxlet::resources::report_from(&state).unwrap();
    let decoded =
        adx_protocol::control::NodePressureReport::decode(report.encode_to_vec().as_slice())
            .unwrap();
    let memory = decoded.memory.unwrap();
    assert_eq!(
        adx_protocol::control::NodePressureLevel::try_from(memory.level).unwrap(),
        adx_protocol::control::NodePressureLevel::High
    );
    assert_eq!(memory.percent, 100);
    assert_eq!(memory.last_transition_unix_seconds, 1_700_000_000);
    let disk = decoded.disk.unwrap();
    assert_eq!(
        adx_protocol::control::NodePressureLevel::try_from(disk.level).unwrap(),
        adx_protocol::control::NodePressureLevel::Normal
    );
    assert_eq!(disk.percent, 0);
    assert_eq!(disk.last_transition_unix_seconds, 0);
    assert_eq!(decoded.observed_at_unix_seconds, 1_700_000_123);
    assert_eq!(decoded.consecutive_failures, 2);
}

#[test]
fn pressure_report_omits_never_observed_states() {
    // Never observed: the wire field stays absent.
    assert!(adxlet::resources::report_from(&NodePressureState::default()).is_none());
    // A gate with only failed samples keeps the never-observed stamp.
    let mut gate = PressureGate::new(PressurePolicy::default()).unwrap();
    let failed = gate.fail();
    assert!(adxlet::resources::report_from(&failed.state).is_none());
    // After a successful sample the report is present and faithful.
    let observed = gate
        .update(Pressure {
            memory_percent: 95,
            disk_percent: 10,
        })
        .state;
    let report = adxlet::resources::report_from(&observed).unwrap();
    assert_eq!(
        report.observed_at_unix_seconds,
        observed.observed_at_unix_seconds
    );
    assert_eq!(report.consecutive_failures, 0);
    let memory = report.memory.unwrap();
    assert_eq!(
        adx_protocol::control::NodePressureLevel::try_from(memory.level).unwrap(),
        adx_protocol::control::NodePressureLevel::High
    );
}

#[test]
fn v1_heartbeat_bytes_never_carry_the_disposition_field() {
    use prost::Message;
    // Field 15 (disposition) with wire type 0 encodes its tag as 0x78. The
    // v1 construction path never sets the field: the encoded heartbeat must
    // not contain that tag byte. Constructed values below avoid the byte so
    // the scan stays unambiguous.
    let request = adx_protocol::control::RegisterNodeRequest {
        pressure: Some(
            adxlet::resources::report_from(&NodePressureState {
                memory: ResourcePressure {
                    enabled: true,
                    level: PressureLevel::High,
                    percent: 95,
                    last_transition_unix_seconds: 1_700_000_000,
                },
                disk: ResourcePressure::default(),
                observed_at_unix_seconds: 1_700_000_123,
                consecutive_failures: 0,
            })
            .unwrap(),
        ),
        checkpoint_scopes: vec![],
        storage_mounts: Default::default(),
        node_id: "n".into(),
        node_address: "localhost:1234".into(),
        proxy_address: "localhost:1235".into(),
        session_id: "session-a".into(),
        heartbeat_sequence: 7,
        reconciling: false,
        runtime_classes: vec!["runsc".into()],
        accepting_allocations: true,
        capacity: Some(adx_protocol::control::Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        }),
        labels: Default::default(),
        devices: vec![],
        pressure_disposition: Default::default(),
    };
    let bytes = request.encode_to_vec();
    assert!(
        !bytes.contains(&0x78),
        "disposition leaked onto the heartbeat wire: {bytes:?}"
    );
    // The proto3 default is the zero value without wire presence.
    assert_eq!(request.pressure_disposition, 0);
}

#[test]
fn disposition_enum_keeps_none_as_the_zero_default() {
    use adx_protocol::control::NodePressureDisposition;
    assert_eq!(
        NodePressureDisposition::try_from(0).unwrap(),
        NodePressureDisposition::None
    );
    assert_eq!(i32::from(NodePressureDisposition::Local), 1);
    assert_eq!(i32::from(NodePressureDisposition::Relocation), 2);
}

#[test]
fn pressure_sampling_failure_before_first_observation_allows_admission() {
    let mut gate = PressureGate::new(PressurePolicy::default()).unwrap();
    assert!(gate.fail().accepting);
    assert!(gate.fail().accepting);
}

#[test]
fn pressure_sampling_failure_reopens_admission_and_resets_hysteresis() {
    for high in [
        Pressure {
            memory_percent: 95,
            disk_percent: 10,
        },
        Pressure {
            memory_percent: 10,
            disk_percent: 95,
        },
        Pressure {
            memory_percent: 95,
            disk_percent: 95,
        },
    ] {
        let mut gate = PressureGate::new(PressurePolicy::default()).unwrap();
        assert!(!gate.update(high).accepting);
        assert!(gate.fail().accepting);
        assert!(gate.fail().accepting);
        let recovered = gate.update(Pressure {
            memory_percent: 85,
            disk_percent: 85,
        });
        assert!(recovered.accepting);
        assert_eq!(recovered.state.consecutive_failures, 0);
        assert_eq!(recovered.state.memory.level, PressureLevel::Normal);
        assert_eq!(recovered.state.disk.level, PressureLevel::Normal);
        assert_eq!(recovered.state.memory.percent, 85);
        assert_eq!(recovered.state.disk.percent, 85);
        let pressured = gate.update(high);
        assert!(!pressured.accepting);
        assert_eq!(
            pressured.state.memory.level == PressureLevel::High,
            high.memory_percent >= 90
        );
        assert_eq!(
            pressured.state.disk.level == PressureLevel::High,
            high.disk_percent >= 90
        );
        assert!(
            gate.update(Pressure {
                memory_percent: 70,
                disk_percent: 70,
            })
            .accepting
        );
    }
}

#[tokio::test]
async fn external_http_source_accepts_zero_capacity_and_reports_disconnect() {
    use adxlet::resources::ResourceSource;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let tmp = tempfile::Builder::new()
        .prefix("adx-resource-")
        .tempdir_in("/tmp")
        .unwrap();
    let path = tmp.path().join("resource.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        for body in [
            r#"{"cpu":0,"mem":4096,"xpu":[]}"#,
            r#"{"cpu":2,"mem":0,"xpu":[]}"#,
            r#"{"cpu":0,"mem":0,"xpu":[]}"#,
            r#"{"cpu":2,"mem":4096,"xpu":[]}"#,
        ] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut headers = vec![];
            loop {
                let mut byte = [0];
                stream.read_exact(&mut byte).await.unwrap();
                headers.push(byte[0]);
                if headers.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let has_host = String::from_utf8(headers)
                .unwrap()
                .to_ascii_lowercase()
                .contains("\r\nhost: localhost\r\n");
            let status = if has_host {
                "200 OK"
            } else {
                "400 Bad Request"
            };
            stream
                .write_all(
                    format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let source = ResourceSource::Sandboxd {
        socket: path,
        valid_for_seconds: 3,
    };
    for (cpu_millis, memory_bytes) in [(0, 4096), (2000, 0), (0, 0), (2000, 4096)] {
        let (sample, valid) = source
            .sample(std::time::Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(sample.capacity.cpu_millis, cpu_millis);
        assert_eq!(sample.capacity.memory_bytes, memory_bytes);
        assert_eq!(valid, std::time::Duration::from_secs(3));
    }
    server.await.unwrap();
    assert!(source
        .sample(std::time::Duration::from_secs(2))
        .await
        .is_err());
}

#[tokio::test]
async fn file_source_accepts_zero_capacity_but_rejects_expired_observations() {
    use adxlet::resources::ResourceSource;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("capacity.json");
    let source = ResourceSource::File { path: path.clone() };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let observation = serde_json::json!({
        "capacity": {"cpu_millis": 0, "memory_bytes": 0, "disk_bytes": 0},
        "valid_until_unix_seconds": now + 60,
    });
    std::fs::write(&path, observation.to_string()).unwrap();
    let (sample, valid) = source.sample(Duration::from_secs(1)).await.unwrap();
    assert_eq!(sample.capacity, adx_core::Resources::default());
    assert!(!valid.is_zero());
    let mut expired = observation;
    expired["valid_until_unix_seconds"] = now.into();
    std::fs::write(path, expired.to_string()).unwrap();
    assert!(source.sample(Duration::from_secs(1)).await.is_err());
}
