//! Characterization of the Node shutdown sequence against the real `node::run`.
//!
//! A real Meta and a real Node run as child processes of the test binary, the
//! Node receives SIGTERM, and the stages it reports (`record_shutdown_stage`,
//! compiled only for tests) must match the documented order exactly. The test
//! knows nothing about where the stages live, so it stays valid when `run_node`
//! is split up, as long as each record call moves together with its stage.
//!
//! `/dev/fuse`, `fusermount3` and root add the mounts and a client write to the
//! scenario; without them the FUSE stages are skipped with a printed notice.

use std::{
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use crate::{
    config::{Cli, Config, Role},
    runtime::Observability,
};

const TRACE_ENV: &str = "AFS_TEST_SHUTDOWN_TRACE";
const ROLE_ENV: &str = "AFS_TEST_SHUTDOWN_ROLE";
const CONFIG_ENV: &str = "AFS_TEST_SHUTDOWN_CONFIG";
const CHILD_TEST: &str = "node::shutdown_order_tests::shutdown_order_child_process";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);

/// The sequence the test pins:
/// 1. admission closes before Services stops;
/// 2. the OwnerFs mount manager stops, then FUSE sessions are joined, so no new
///    kernel request can dirty a file;
/// 3. DFS drains while its stores are still open;
/// 4. ChunkStore background workers exit, then fetcher, Peer cache, external
///    verifier and write buffers; the local API goes last.
fn expected_stages(ownerfs: bool, fuse: bool) -> Vec<&'static str> {
    let mut stages = vec!["serving", "admission_closed", "services_stopped"];
    if ownerfs {
        stages.push("ownerfs_mounts_stopped");
    }
    if fuse {
        stages.push("dfs_fuse_joined");
    }
    if ownerfs && fuse {
        stages.push("ownerfs_fuse_joined");
    }
    stages.extend([
        "dfs_drained",
        "chunk_store_exited",
        "fetcher_stopping",
        "peer_cache_exited",
        "write_buffers_stopped",
        "local_api_stopped",
    ]);
    stages
}

/// Entry of the Meta and Node child processes; a no-op in the normal test run.
#[test]
fn shutdown_order_child_process() {
    let Ok(role) = std::env::var(ROLE_ENV) else {
        return;
    };
    let config = PathBuf::from(std::env::var(CONFIG_ENV).unwrap());
    let cli = Cli {
        config: Some(config),
        ..Cli::default()
    };
    let executor = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    executor.block_on(async {
        // Keeps SIGTERM non-fatal until `Services` subscribes; the parent resends it.
        let _armed =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        let obs = Observability::new().unwrap();
        match role.as_str() {
            "meta" => crate::meta::run(Config::resolve(Role::Meta, cli).unwrap(), obs).await,
            _ => crate::node::run(Config::resolve(Role::Node, cli).unwrap(), obs).await,
        }
        .unwrap();
    });
}

#[test]
fn node_shutdown_stops_admission_then_fuse_then_drains_before_chunk_store_exit() {
    if std::env::var_os(ROLE_ENV).is_some() {
        return;
    }
    run_scenario("dfs");
}

#[cfg(feature = "ownerfs")]
#[test]
fn node_shutdown_order_holds_with_both_backends() {
    if std::env::var_os(ROLE_ENV).is_some() {
        return;
    }
    run_scenario("all");
}

fn run_scenario(fs: &str) {
    let ownerfs = fs == "all";
    let fuse = fuse_usable();
    if !fuse {
        println!("shutdown order: FUSE stages skipped (needs root, /dev/fuse, fusermount3)");
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    generate_certificates(root);

    let [meta_grpc, meta_rest, node_grpc, node_rest] = free_addresses();
    let meta_config = root.join("meta.toml");
    let tls = tls_section(root);
    std::fs::write(
        &meta_config,
        format!(
            "id = \"meta\"\nfs = \"{fs}\"\ngrpc_listen = \"{meta_grpc}\"\n\
             rest_listen = \"{meta_rest}\"\nmeta_store = \"local-file\"\n\
             data_dir = \"{}\"\n{}",
            root.join("meta").display(),
            tls("meta"),
        ),
    )
    .unwrap();
    let dfs_mount = root.join("dfs-mount");
    let ownerfs_mount = root.join("ownerfs-mount");
    let mut mounts = String::new();
    if fuse {
        std::fs::create_dir(&dfs_mount).unwrap();
        mounts += &format!("dfs_mount = \"{}\"\n", dfs_mount.display());
        if ownerfs {
            std::fs::create_dir(&ownerfs_mount).unwrap();
            mounts += &format!("ownerfs_mount = \"{}\"\n", ownerfs_mount.display());
        }
    }
    let node_config = root.join("node.toml");
    let peer_cache = root.join("peer-cache");
    std::fs::create_dir(&peer_cache).unwrap();
    std::fs::set_permissions(
        &peer_cache,
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .unwrap();
    std::fs::write(
        &node_config,
        format!(
            "id = \"node-a\"\nfs = \"{fs}\"\ngrpc_listen = \"{node_grpc}\"\n\
             rest_listen = \"{node_rest}\"\nmeta_endpoint = \"https://{meta_grpc}\"\n\
             peer_endpoint = \"https://{node_grpc}\"\ndata_mode = \"grpc\"\n\
             data_dir = \"{}\"\nuds_path = \"{}\"\n{mounts}{}\n\
             [dfs_peer_cache]\ndirectory = \"{}\"\ncapacity_bytes = 268435456\n",
            root.join("node").display(),
            root.join("n.sock").display(),
            tls("node-a"),
            peer_cache.display(),
        ),
    )
    .unwrap();

    let trace = root.join("trace");
    let mut meta = Reaped(spawn_child(root, "meta", &meta_config, None));
    let mut node = Reaped(spawn_child(root, "node", &node_config, Some(&trace)));

    let started = Instant::now();
    while !read_trace(&trace).iter().any(|stage| stage == "serving") {
        for (name, child) in [("meta", &mut meta.0), ("node", &mut node.0)] {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("{name} exited during startup: {status}\n{}", logs(root));
            }
        }
        assert!(
            started.elapsed() < STARTUP_TIMEOUT,
            "Node not serving\n{}",
            logs(root)
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    if fuse {
        // Real client traffic before the stop, so shutdown does not run on an idle Node.
        std::fs::write(dfs_mount.join("written"), vec![7_u8; 64 * 1024]).unwrap();
    }

    let pid = node.0.id().to_string();
    let started = Instant::now();
    let status = loop {
        if let Some(status) = node.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            started.elapsed() < SHUTDOWN_TIMEOUT,
            "Node did not exit; stages {:?}\n{}",
            read_trace(&trace),
            logs(root)
        );
        let _ = Command::new("kill").args(["-TERM", &pid]).status();
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(status.success(), "Node shutdown failed\n{}", logs(root));
    assert_eq!(read_trace(&trace), expected_stages(ownerfs, fuse));
}

/// Kills a leftover child when the test unwinds, so no process outlives a failure.
struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn fuse_usable() -> bool {
    // SAFETY: geteuid has no preconditions and only reads process credentials.
    let root = unsafe { libc::geteuid() } == 0;
    root && Path::new("/dev/fuse").exists()
        && std::env::var_os("PATH").is_some_and(|path| {
            std::env::split_paths(&path).any(|d| d.join("fusermount3").exists())
        })
}

fn spawn_child(root: &Path, role: &str, config: &Path, trace: Option<&Path>) -> Child {
    let log = |suffix: &str| {
        Stdio::from(std::fs::File::create(root.join(format!("{role}.{suffix}"))).unwrap())
    };
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env(ROLE_ENV, role)
        .env(CONFIG_ENV, config)
        .env_remove(TRACE_ENV)
        .stdout(log("out"))
        .stderr(log("err"));
    if let Some(trace) = trace {
        command.env(TRACE_ENV, trace);
    }
    command.spawn().unwrap()
}

fn read_trace(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn logs(root: &Path) -> String {
    ["meta.out", "meta.err", "node.out", "node.err"]
        .iter()
        .map(|name| {
            let text = std::fs::read_to_string(root.join(name)).unwrap_or_default();
            let tail: Vec<&str> = text.lines().rev().take(40).collect();
            let tail: Vec<&str> = tail.into_iter().rev().collect();
            format!("--- {name}\n{}\n", tail.join("\n"))
        })
        .collect()
}

fn free_addresses() -> [SocketAddr; 4] {
    // Held together so the four ports differ; released right before the children bind.
    let listeners: [TcpListener; 4] =
        std::array::from_fn(|_| TcpListener::bind("127.0.0.1:0").unwrap());
    listeners.map(|listener| listener.local_addr().unwrap())
}

fn tls_section(root: &Path) -> impl Fn(&str) -> String + '_ {
    move |name| {
        format!(
            "tls_encryption_enabled = false\ntls_ca_certificate = \"{0}/ca.pem\"\n\
             tls_identity_certificate = \"{0}/{name}.pem\"\n\
             tls_identity_private_key = \"{0}/{name}.key\"\n\
             tls_server_name = \"localhost\"\n\
             [trusted_node_certs]\nnode-a = \"{0}/node-a.pem\"\n",
            root.display()
        )
    }
}

fn generate_certificates(dir: &Path) {
    let openssl = |args: &[&str]| {
        let output = Command::new("openssl")
            .current_dir(dir)
            .args(args)
            .output()
            .expect("the shutdown order test requires openssl");
        assert!(
            output.status.success(),
            "openssl {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    std::fs::write(
        dir.join("ca.cnf"),
        "[req]\ndistinguished_name=dn\nx509_extensions=ca\nprompt=no\n[dn]\nCN=AFS shutdown CA\n\
         [ca]\nbasicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("leaf.ext"),
        "basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n\
         extendedKeyUsage=serverAuth,clientAuth\n",
    )
    .unwrap();
    openssl(&[
        "req", "-config", "ca.cnf", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
        "-keyout", "ca.key", "-out", "ca.pem",
    ]);
    for (serial, name) in ["meta", "node-a"].into_iter().enumerate() {
        let (key, csr, pem) = (
            format!("{name}.key"),
            format!("{name}.csr"),
            format!("{name}.pem"),
        );
        let subject = format!("/CN={name}");
        openssl(&[
            "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", &subject, "-keyout", &key,
            "-out", &csr,
        ]);
        let serial = (serial + 1).to_string();
        openssl(&[
            "x509",
            "-req",
            "-in",
            &csr,
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-set_serial",
            &serial,
            "-days",
            "2",
            "-extfile",
            "leaf.ext",
            "-out",
            &pem,
        ]);
    }
}
