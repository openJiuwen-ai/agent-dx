use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

struct Execd {
    child: Child,
    port: u16,
    directory: tempfile::TempDir,
}

impl Execd {
    fn start(spool_limit: usize) -> Self {
        Self::with_limits(spool_limit, 4096)
    }

    fn with_limits(spool_limit: usize, registry_limit: usize) -> Self {
        Self::with_spool(Some(spool_limit), registry_limit)
    }

    fn with_spool(spool_limit: Option<usize>, registry_limit: usize) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let socket = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let mut command = Command::new(env!("CARGO_BIN_EXE_adx-execd"));
        if let Some(limit) = spool_limit {
            command
                .env("EXECD_COMMAND_SPOOL_DIR", directory.path())
                .env("EXECD_COMMAND_SPOOL_MAX_BYTES", limit.to_string());
        } else {
            command.env_remove("EXECD_COMMAND_SPOOL_DIR");
        }
        let child = command
            .env("EXECD_HTTP_ONLY", "1")
            .env("EXECD_HTTP_PORT", port.to_string())
            .env("EXECD_HTTP_TOKEN", "test-command-output")
            .env("EXECD_COMMAND_STDOUT_LIMIT_BYTES", "32")
            .env("EXECD_COMMAND_STDERR_LIMIT_BYTES", "32")
            .env(
                "EXECD_COMMAND_REGISTRY_MAX_BYTES",
                registry_limit.to_string(),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self {
            child,
            port,
            directory,
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(Instant::now() < deadline, "Execd did not start");
            thread::sleep(Duration::from_millis(10));
        }
        server
    }

    fn request(&self, method: &str, path: &str, body: &str) -> BufReader<TcpStream> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        write!(stream, "{method} {path} HTTP/1.1\r\nHost: localhost\r\nX-Auth: test-command-output\r\nContent-Length: {}\r\n\r\n{body}", body.len()).unwrap();
        BufReader::new(stream)
    }

    fn invoke(&self, action: &str, args: Value, request_id: &str) -> (u16, bool, Value) {
        let body = json!({"action": action, "args": args, "requestId": request_id});
        let (status, streamed, body) = response(self.request("POST", "/invoke", &body.to_string()));
        (status, streamed, serde_json::from_slice(&body).unwrap())
    }

    fn metric(&self, name: &str) -> u64 {
        let (_, _, body) = response(self.request("GET", "/metrics", ""));
        String::from_utf8(body)
            .unwrap()
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(' ')?;
                (key == name).then(|| value.parse().unwrap())
            })
            .unwrap_or_else(|| panic!("missing metric {name}"))
    }
}

impl Drop for Execd {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn head(stream: &mut impl BufRead) -> (u16, bool, usize) {
    let mut line = String::new();
    stream.read_line(&mut line).unwrap();
    let status = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut chunked = false;
    let mut length = 0;
    loop {
        line.clear();
        assert!(stream.read_line(&mut line).unwrap() > 0);
        if line == "\r\n" {
            return (status, chunked, length);
        }
        chunked |= line.eq_ignore_ascii_case("Transfer-Encoding: chunked\r\n");
        if let Some(value) = line.strip_prefix("Content-Length: ") {
            length = value.trim().parse().unwrap();
        }
    }
}

fn response(mut stream: impl BufRead) -> (u16, bool, Vec<u8>) {
    let (status, chunked, length) = head(&mut stream);
    let mut body = Vec::new();
    if chunked {
        loop {
            let mut line = String::new();
            stream.read_line(&mut line).unwrap();
            let len = usize::from_str_radix(line.trim(), 16).unwrap();
            if len == 0 {
                break;
            }
            let offset = body.len();
            body.resize(offset + len, 0);
            stream.read_exact(&mut body[offset..]).unwrap();
            let mut separator = [0; 2];
            stream.read_exact(&mut separator).unwrap();
            assert_eq!(&separator, b"\r\n");
        }
    } else {
        body.resize(length, 0);
        stream.read_exact(&mut body).unwrap();
    }
    (status, chunked, body)
}

fn run(server: &Execd, command: &str, background: bool, id: &str) -> (u16, bool, Value) {
    if background {
        let (status, _, _) = server.invoke(
            "process.start",
            json!({"command_id": id, "command": command}),
            id,
        );
        assert_eq!(status, 200);
        server.invoke(
            "process.wait",
            json!({"command_id": id, "timeout": 5}),
            &format!("{id}-wait"),
        )
    } else {
        server.invoke("process.exec", json!({"cmd": command, "timeout": 5}), id)
    }
}

// The shell exits with a writer still holding both pipes. Release that writer
// only after fetching the terminal result so it cannot close the pipes early.
const INHERITED_PIPE_COMMAND: &str = r#"
(i=0; while [ ! -f release ] && [ "$i" -lt 140 ]; do
    i=$((i + 1)); sleep 0.05
done; printf late && printf late >&2 && touch writer-finished) &
head -c 4096 /dev/zero | tr '\0' x
printf error >&2
exit 7
"#;

fn release_inherited_writer(server: &Execd) {
    std::fs::write(server.directory.path().join("release"), b"").unwrap();
    let done = server.directory.path().join("writer-finished");
    let deadline = Instant::now() + Duration::from_secs(3);
    while !done.exists() {
        assert!(
            Instant::now() < deadline,
            "the inherited writer did not finish"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn synchronous_spooled_result_does_not_wait_for_an_inherited_pipe_to_close() {
    let server = Execd::start(16384);
    let started = Instant::now();
    let (status, streamed, body) = server.invoke(
        "process.exec",
        json!({"cmd": INHERITED_PIPE_COMMAND, "cwd": server.directory.path(), "timeout": 3}),
        "sync-inherited-pipe",
    );
    assert_eq!((status, streamed), (200, true));
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "waited for the inherited pipe"
    );
    assert_eq!(body["exit_code"], 7);
    assert_eq!(body["stdout"], "x".repeat(4096));
    assert_eq!(body["stderr"], "error");
    release_inherited_writer(&server);
}

#[test]
fn background_terminal_result_and_kill_agree_while_a_descendant_holds_pipes() {
    for spool_limit in [Some(16384), None] {
        let server = Execd::with_spool(spool_limit, 4096);
        let command_id = "background-inherited-pipe";
        let (status, _, _) = server.invoke(
            "process.start",
            json!({"command_id": command_id, "command": INHERITED_PIPE_COMMAND, "cwd": server.directory.path()}),
            "background-inherited-start",
        );
        assert_eq!(status, 200);
        let (status, streamed, first) = server.invoke(
            "process.wait",
            json!({"command_id": command_id, "timeout": 1}),
            "background-inherited-wait",
        );
        assert_eq!(status, 200);
        assert_eq!(first["status"], "FAILED", "{first}");
        assert_eq!(first["exit_code"], 7);
        assert!(first["finished_at_ms"].is_number());
        assert_eq!(streamed, spool_limit.is_some());
        assert_eq!(
            first["stdout"],
            "x".repeat(if streamed { 4096 } else { 32 })
        );
        assert_eq!(first["stderr"], "error");
        assert_eq!(first["truncated"], !streamed);

        let (status, _, kill) = server.invoke(
            "process.kill",
            json!({"command_id": command_id}),
            "background-inherited-kill",
        );
        assert_eq!(status, 200);
        assert_eq!(kill["status"], "ALREADY_EXITED");
        assert_eq!(kill["killed"], false);
        release_inherited_writer(&server);
        let (status, _, replay) = server.invoke(
            "process.get",
            json!({"command_id": command_id}),
            "background-inherited-get",
        );
        assert_eq!(status, 200);
        assert_eq!(replay, first);
    }
}

#[test]
fn small_output_is_inline_and_large_output_spills_for_both_command_paths() {
    let server = Execd::start(256 * 1024);
    for background in [false, true] {
        let before = server.metric("command_spool_streams_total");
        let (status, streamed, body) = run(
            &server,
            "printf out; printf err >&2",
            background,
            &format!("small-{background}"),
        );
        assert_eq!((status, streamed), (200, false));
        assert_eq!(
            (body["stdout"].as_str(), body["stderr"].as_str()),
            (Some("out"), Some("err"))
        );
        assert_eq!(server.metric("command_spool_streams_total"), before);

        let command = "printf small; head -c 16384 /dev/zero | tr '\\0' x >&2";
        let (status, streamed, body) =
            run(&server, command, background, &format!("large-{background}"));
        assert_eq!((status, streamed), (200, true));
        assert_eq!(body["stdout"], "small");
        assert_eq!(body["stderr"], "x".repeat(16384));
        assert_eq!(server.metric("command_spool_streams_total"), before + 1);
    }
}

#[test]
fn spool_quota_failure_reports_real_terminal_exit_for_both_command_paths() {
    let server = Execd::start(256);
    for background in [false, true] {
        let (status, _, body) = run(
            &server,
            "head -c 65536 /dev/zero; exit 0",
            background,
            &format!("full-{background}"),
        );
        assert_eq!(status, 507);
        assert_eq!(body["error"]["code"], "COMMAND_OUTPUT_INCOMPLETE");
        assert_eq!(body["error"]["retry"], "never");
        assert_eq!(body["exit_code"], 0);
        assert_eq!(server.metric("command_spool_bytes"), 0);
    }
}

#[test]
fn shared_budget_spills_a_stream_even_at_its_individual_limit() {
    for background in [false, true] {
        let server = Execd::with_limits(16384, 48);
        let (status, streamed, body) = run(
            &server,
            "printf '%032d' 1; printf '%032d' 2 >&2",
            background,
            "shared-budget",
        );
        assert_eq!((status, streamed), (200, true));
        assert_eq!(body["stdout"], format!("{:032}", 1));
        assert_eq!(body["stderr"], format!("{:032}", 2));
        assert_eq!(server.metric("command_spool_streams_total"), 1);
    }
}

#[test]
fn streamed_json_preserves_utf8_boundaries_and_json_escapes() {
    let server = Execd::start(262144);
    let (status, streamed, body) = run(
        &server,
        r#"head -c 65535 /dev/zero | tr '\0' a; printf '\342\202\254\042\134\012\377\342\202'"#,
        false,
        "encoding",
    );
    assert_eq!((status, streamed), (200, true));
    assert_eq!(body["stdout"], format!("{}€\"\\\n��", "a".repeat(65535)));
}

#[test]
fn partial_response_can_be_replayed_without_reexecuting_the_command() {
    let server = Execd::start(16 * 1024 * 1024);
    for background in [false, true] {
        let marker = server.directory.path().join(format!("marker-{background}"));
        let command = format!(
            "printf x >> {}; head -c 4194304 /dev/zero | tr '\\0' y",
            marker.display()
        );
        let request_id = format!("replay-{background}");
        let (action, args) = if background {
            let (status, _, _) = server.invoke(
                "process.start",
                json!({"command_id": request_id, "command": command}),
                &format!("{request_id}-start"),
            );
            assert_eq!(status, 200);
            (
                "process.wait",
                json!({"command_id": request_id, "timeout": 5}),
            )
        } else {
            ("process.exec", json!({"cmd": command, "timeout": 5}))
        };
        let body = json!({"action": action, "args": args, "requestId": request_id});
        let mut cut = server.request("POST", "/invoke", &body.to_string());
        assert_eq!(head(&mut cut), (200, true, 0));
        cut.read_exact(&mut [0; 128]).unwrap();
        drop(cut);

        let (status, streamed, body) = server.invoke(action, args, &request_id);
        assert_eq!((status, streamed), (200, true));
        assert_eq!(body["stdout"], "y".repeat(4194304));
        assert_eq!(std::fs::read(marker).unwrap(), b"x");
    }
}
