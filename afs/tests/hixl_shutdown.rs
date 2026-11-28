//! Local process tests of the native HIXL shutdown path, with the SDK replaced
//! by an idle-node LD_PRELOAD fixture. Uses the shipped binaries and deadline.

#![cfg(all(target_os = "linux", feature = "hixl", feature = "ownerfs"))]

use std::{
    fs,
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

const POLL: Duration = Duration::from_millis(20);
const PROCESSCTL_STOP_WAIT: Duration = Duration::from_secs(20);

#[test]
#[ignore = "requires the CANN SDK headers, c++, and openssl to build and run the local mock"]
fn idle_node_waits_for_native_shutdown() {
    run("normal", Some(0));
}

#[test]
#[ignore = "requires the CANN SDK headers, c++, and openssl to build and run the local mock"]
fn blocked_native_shutdown_exits_124() {
    run("native-blocked", Some(124));
}

#[test]
#[ignore = "requires the CANN SDK headers, c++, and openssl to build and run the local mock"]
fn blocked_c_exit_handler_exits_124_after_native_shutdown() {
    run("exit-blocked", Some(124));
}

fn run(mode: &str, expected: Option<i32>) {
    let tmp = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let root = tmp.path();
    prepare_mock(root);
    let [meta_grpc, meta_rest, node_grpc, node_rest] = {
        let listeners: [TcpListener; 4] =
            std::array::from_fn(|_| TcpListener::bind("127.0.0.1:0").unwrap());
        listeners
            .each_ref()
            .map(|listener| listener.local_addr().unwrap())
    };
    let mut meta = spawn(
        root,
        "meta",
        env!("CARGO_BIN_EXE_afs-meta"),
        &format!(
            "grpc_listen = '{meta_grpc}'\nrest_listen = '{meta_rest}'\n\
             meta_store = 'local-file'\n"
        ),
        None,
    );
    wait_for_log(root, "meta", "meta.ready", &mut meta.0);
    let mut node = spawn(
        root,
        "node-a",
        env!("CARGO_BIN_EXE_afs-node"),
        &format!(
            "grpc_listen = '{node_grpc}'\nrest_listen = '{node_rest}'\n\
             meta_endpoint = 'https://{meta_grpc}'\nuds_path = '{}/node.sock'\n\
             data_mode = 'hixl'\n",
            root.display()
        ),
        Some(mode),
    );
    wait_for_log(root, "node-a", "node.ready", &mut node.0);
    if mode == "exit-blocked" {
        fs::write(root.join("native-unblock"), []).unwrap();
    }
    let started = Instant::now();
    assert!(
        Command::new("kill")
            .args(["-TERM", &node.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    if mode == "normal" {
        wait_for_marker(root, &mut node.0, "native-entered");
        let held_until = Instant::now() + Duration::from_millis(250);
        while Instant::now() < held_until {
            assert!(
                node.0.try_wait().unwrap().is_none(),
                "Node exited before native cleanup was released"
            );
            thread::sleep(POLL);
        }
        fs::write(root.join("native-unblock"), []).unwrap();
    }
    let (status, elapsed) = wait_for_exit(root, &mut node.0, started);
    eprintln!("{mode}: {status} after {elapsed:.3?} from SIGTERM");
    let logs = log(root, "node-a");
    assert_eq!(status.code(), expected, "{mode}: {logs}");
    if mode == "normal" {
        let home_drained = logs
            .find("ownerfs.hixl_home_drained")
            .expect("idle Home was drained");
        let services_stopped = logs.find("services.stopped").expect("services stopped");
        assert!(home_drained < services_stopped, "{logs}");
    }
    assert!(root.join("native-entered").exists());
    assert_eq!(
        root.join("native-finished").exists(),
        mode != "native-blocked"
    );
    assert_eq!(root.join("c-exit-entered").exists(), mode == "exit-blocked");
    if expected == Some(124) {
        assert!(elapsed >= Duration::from_secs(15));
    }
}

fn prepare_mock(root: &Path) {
    let afs = Path::new(env!("CARGO_MANIFEST_DIR"));
    let sdk = std::path::PathBuf::from(
        std::env::var_os("ADX_CANN_PATH")
            .expect("set ADX_CANN_PATH to the SDK used for this build"),
    );
    let arch_include = sdk.join(format!("{}-linux/include", std::env::consts::ARCH));
    let include = if arch_include.is_dir() {
        arch_include
    } else {
        sdk.join("include")
    };
    let compile = Command::new("c++")
        .args(["-shared", "-fPIC", "-std=c++17", "-I"])
        .arg(include)
        // Export only the SDK calls replaced by the fixture. Vendor C++ header
        // singletons must stay local to this preloaded library.
        .arg("-Wl,--version-script")
        .arg("-Xlinker")
        .arg(afs.join("tests/fixtures/hixl_shutdown_sdk.map"))
        .arg(afs.join("tests/fixtures/hixl_shutdown_sdk.cc"))
        .arg("-o")
        .arg(root.join("mock.so"))
        .status()
        .unwrap();
    assert!(compile.success());
    let tls = Command::new("bash")
        .arg(afs.join("../build/config/examples/afs/prepare-local-tls.sh"))
        .arg(root.join("tls"))
        .status()
        .unwrap();
    assert!(tls.success());
}

fn spawn(root: &Path, role: &str, bin: &str, extra: &str, mode: Option<&str>) -> Reaped {
    let config = root.join(format!("{role}.toml"));
    fs::write(
        &config,
        format!(
            "id = '{role}'\nfs = 'ownerfs'\ndata_dir = '{0}/{role}'\n\
             tls_ca_certificate = '{0}/tls/ca.pem'\n\
             tls_identity_certificate = '{0}/tls/{role}.pem'\n\
             tls_identity_private_key = '{0}/tls/{role}-key.pem'\n\
             tls_server_name = 'afs-cluster'\n{extra}\n\
             [trusted_node_certs]\nnode-a = '{0}/tls/node-a.pem'\n{1}",
            root.display(),
            if mode.is_some() {
                // This test exercises shutdown ordering, not deployment-sized pools.
                "[hixl]\nengine_id = '127.0.0.1:31000'\ntrusted_peers = ['node-a']\n\
                 receive_slots = 8\nexport_slots_per_session = 8\nownerfs_sessions_per_home = 1\n"
            } else {
                ""
            },
        ),
    )
    .unwrap();
    let log = fs::File::create(root.join(format!("{role}.log"))).unwrap();
    let mut command = Command::new(bin);
    command
        // Exercise cleanup ordering independently of the host CPU count.
        .env("TOKIO_WORKER_THREADS", "4")
        .current_dir(root)
        .arg("--config")
        .arg(config)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log));
    if let Some(mode) = mode {
        command
            .env("LD_PRELOAD", root.join("mock.so"))
            .env("AFS_TEST_HIXL_SHUTDOWN", mode);
    }
    Reaped(command.spawn().unwrap())
}

fn log(root: &Path, role: &str) -> String {
    fs::read_to_string(root.join(format!("{role}.log"))).unwrap_or_default()
}

fn wait_for_log(root: &Path, role: &str, event: &str, child: &mut Child) {
    let started = Instant::now();
    while !log(root, role).contains(event) {
        assert!(
            child.try_wait().unwrap().is_none() && started.elapsed() < Duration::from_secs(30),
            "{event}: {}",
            log(root, role)
        );
        thread::sleep(POLL);
    }
}

fn wait_for_marker(root: &Path, child: &mut Child, name: &str) {
    let started = Instant::now();
    while !root.join(name).exists() {
        assert!(
            child.try_wait().unwrap().is_none() && started.elapsed() < Duration::from_secs(10),
            "{name}: {}",
            log(root, "node-a")
        );
        thread::sleep(POLL);
    }
}

fn wait_for_exit(root: &Path, child: &mut Child, started: Instant) -> (ExitStatus, Duration) {
    loop {
        let status = child.try_wait().unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed <= PROCESSCTL_STOP_WAIT,
            "Node did not exit within {PROCESSCTL_STOP_WAIT:?} after SIGTERM: {}",
            log(root, "node-a")
        );
        if let Some(status) = status {
            return (status, elapsed);
        }
        thread::sleep(POLL);
    }
}

struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
