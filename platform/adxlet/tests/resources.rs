use adx_core::scheduling::DeviceKind;
use adxlet::resources::{
    cpu_quota, cpu_set_count, parse_external, Pressure, PressureGate, PressurePolicy,
};

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
    assert!(!gate.update(Pressure {
        memory_percent: 95,
        disk_percent: 10
    }));
    assert!(!gate.update(Pressure {
        memory_percent: 85,
        disk_percent: 10
    }));
    assert!(!gate.update(Pressure {
        memory_percent: 70,
        disk_percent: 95
    }));
    assert!(gate.update(Pressure {
        memory_percent: 70,
        disk_percent: 70
    }));
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
