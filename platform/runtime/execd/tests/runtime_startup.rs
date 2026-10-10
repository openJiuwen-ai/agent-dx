use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn unused_local_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local address").port()
}

fn can_connect(port: u16) -> bool {
    TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}").parse().expect("socket address"),
        Duration::from_millis(50),
    )
    .is_ok()
}

#[test]
fn runtime_waits_for_seed_then_refreshes_environment() {
    let temp = tempfile::tempdir().expect("create temp directory");
    let seed_file = temp.path().join("runtime.seed");
    let seed_path = CString::new(seed_file.to_string_lossy().as_bytes()).expect("seed path");
    // SAFETY: seed_path is NUL terminated and remains valid for the call; mkfifo does not retain
    // the pointer.
    let result = unsafe { libc::mkfifo(seed_path.as_ptr(), 0o600) };
    assert_eq!(
        result,
        0,
        "create seed fifo: {}",
        std::io::Error::last_os_error()
    );

    let refreshed_port = unused_local_port();
    let env_file = temp.path().join("runtime.env");
    std::fs::write(&env_file, format!("EXECD_HTTP_PORT={refreshed_port}\n"))
        .expect("write environment file");

    let child = Command::new(env!("CARGO_BIN_EXE_adx-execd"))
        .env("EXECD_HTTP_ONLY", "1")
        .env("EXECD_HTTP_PORT", "0")
        .env("ADX_SEED_FILE", &seed_file)
        .env("ADX_ENV_FILE", &env_file)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start adx-execd");
    let _child = ChildGuard(child);

    thread::sleep(Duration::from_millis(200));
    assert!(
        !can_connect(refreshed_port),
        "runtime must not start serving before the seed file releases it"
    );

    let mut seed = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&seed_file)
        .expect("open seed fifo");

    thread::sleep(Duration::from_millis(200));
    assert!(
        !can_connect(refreshed_port),
        "runtime must remain blocked in the seed read while the FIFO has no data"
    );

    seed.write_all(b"resume").expect("write seed fifo");

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && !can_connect(refreshed_port) {
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        can_connect(refreshed_port),
        "runtime did not refresh EXECD_HTTP_PORT from ADX_ENV_FILE after the seed read"
    );
    drop(seed);
}
