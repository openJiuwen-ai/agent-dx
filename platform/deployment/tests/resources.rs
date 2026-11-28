use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    net::TcpListener,
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

fn response() -> Value {
    json!({"items": [
        {"id": "small-node", "status": 0,
         "capacity": {"CPU": 500, "Memory": 1024, "Disk": 4096, "GPU/A100": 2},
         "allocated": {"CPU": 250, "Memory": 512, "Disk": 1024, "GPU/A100": 1},
         "allocatable": {"CPU": 250, "Memory": 512, "Disk": 3072, "GPU/A100": 1}},
        {"id": "paused-node", "status": 1,
         "capacity": {"CPU": 2000, "Memory": 2048, "Disk": 4096},
         "allocated": {"CPU": 3000, "Memory": 4096, "Disk": 4096, "NPU/910B": 1},
         "allocatable": {"CPU": 0, "Memory": 0, "Disk": 0}}
    ]})
}

fn request(status: &str, body: Value, format: &str, configured: bool) -> Output {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let status = status.to_owned();
    let server = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "CLI never queried resources");
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("accept: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut data = Vec::new();
        while !data.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            data.push(byte[0]);
            assert!(data.len() < 8192);
        }
        let request = String::from_utf8(data).unwrap();
        assert!(request.starts_with("GET /api/environment/v1/resources?view=accounting HTTP/1.1"));
        assert!(request
            .to_ascii_lowercase()
            .contains("authorization: bearer test-key\r\n"));
        let body = body.to_string();
        write!(stream, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let key = directory.path().join("key");
    std::fs::write(&key, "test-key\n").unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_adxctl"));
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("ADX_") {
            command.env_remove(name);
        }
    }
    if configured {
        let config = directory.path().join("deployment.yaml");
        std::fs::write(&config, format!(
            "schema_version: 1\npackage_dir: /opt/adx/current\nstate_dir: /tmp/adx-resource-test\nredis_url: redis://127.0.0.1/\nnamespace: test\nrestart_delay_ms: 1000\nstop_timeout_seconds: 5\nservices:\n  - id: coordinator\n    role: coordinator\n    config:\n      bootstrap_credentials:\n        - key_file: {}\n          tenant_id: admin\n          administrator: true\n  - id: apiserver\n    role: apiserver\n    config:\n      listen: {}\n      loopback_http: true\n", key.display(), address
        )).unwrap();
        command.arg("-c").arg(config);
    } else {
        command.args(["-c", "/missing/deployment.yaml"]);
    }
    command.args(["resources", "--output", format]);
    if !configured {
        command.args(["--endpoint", &format!("http://{address}"), "--token-file"]);
        command.arg(key);
    }
    let output = command.output().unwrap();
    server.join().unwrap();
    output
}

#[test]
fn resources_json_preserves_real_allocations_and_sums_every_node() {
    let output = request("200 OK", response(), "json", false);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["summary"]["node_count"], 2);
    assert_eq!(data["summary"]["accepting_count"], 1);
    assert_eq!(data["summary"]["capacity"]["CPU"], 2500);
    assert_eq!(data["summary"]["allocated"]["CPU"], 3250);
    assert_eq!(data["summary"]["remaining"]["CPU"], 250);
    assert_eq!(data["summary"]["allocated"]["NPU/910B"], 1);
    assert_eq!(data["summary"]["capacity"]["GPU/A100"], 2);
    assert_eq!(data["nodes"][0]["id"], "paused-node");
    assert_eq!(data["nodes"][0]["allocated"]["CPU"], 3000);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("test-key"));
}

#[test]
fn resources_table_includes_small_and_paused_nodes_and_resource_units() {
    let output = request("200 OK", response(), "table", false);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    for expected in [
        "Cluster",
        "CAPACITY",
        "ALLOCATED",
        "REMAINING",
        "small-node",
        "paused-node",
        "GPU/A100",
        "NPU/910B",
        "GiB",
        "cores",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    assert!(!text.contains('\u{1b}'));
}

#[test]
fn resources_uses_local_api_and_bootstrap_key_from_deployment() {
    let output = request("200 OK", response(), "json", true);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn resources_empty_cluster_has_an_explicit_zero_summary() {
    let output = request("200 OK", json!({"items": []}), "json", false);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let data: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(data["summary"]["node_count"], 0);
    assert_eq!(data["summary"]["allocated"]["CPU"], 0);
    assert_eq!(data["nodes"], json!([]));
}

#[test]
fn resources_errors_preserve_backend_detail_and_leave_stdout_empty() {
    let output = request(
        "503 Service Unavailable",
        json!({"message": "node directory unavailable"}),
        "json",
        false,
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(
        error.contains("503") && error.contains("node directory unavailable"),
        "{error}"
    );
}

#[test]
fn resources_rejects_missing_allocation_accounting_instead_of_fabricating_usage() {
    let output = request(
        "200 OK",
        json!({"items": [{"id": "old-node", "status": 0, "capacity": {}, "allocatable": {}}]}),
        "json",
        false,
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("allocated"));
}
