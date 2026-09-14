/// Cross-language round-trip tests for pyval codec.
///
/// These tests invoke real Python to produce/consume pickle bytes, verifying
/// that the Rust codec interops with actual Python pickle output — not a
/// Rust-internal round-trip only.
use rrt_daemon::pyval;
use std::io::Write;
use std::process::{Command, Stdio};

/// Find a working python3 binary in the environment.
fn python_bin() -> &'static str {
    // Try python3 first, fall back to python3.11
    for candidate in &["python3", "python3.11"] {
        if Command::new(candidate)
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            // Safety: candidate is a static string literal
            return if *candidate == "python3" {
                "python3"
            } else {
                "python3.11"
            };
        }
    }
    panic!("neither python3 nor python3.11 found in PATH");
}

/// Ask Python to pickle a value and return the raw bytes.
fn python_dumps(expr: &str) -> Vec<u8> {
    let py = python_bin();
    let code = format!(
        "import pickle, sys; sys.stdout.buffer.write(pickle.dumps({}))",
        expr
    );
    let out = Command::new(py)
        .args(["-c", &code])
        .output()
        .expect("failed to run python");
    assert!(
        out.status.success(),
        "python pickle failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

// ── decode tests ────────────────────────────────────────────────────────────

#[test]
fn decode_list_of_strings() {
    let bytes = python_dumps("['ls', '-la']");
    let got: Vec<String> = pyval::decode(&bytes).expect("decode failed");
    assert_eq!(got, vec!["ls", "-la"]);
}

#[test]
fn decode_string() {
    let bytes = python_dumps("'ls -la'");
    let got: String = pyval::decode(&bytes).expect("decode failed");
    assert_eq!(got, "ls -la");
}

#[test]
fn decode_integer() {
    let bytes = python_dumps("42");
    let got: i64 = pyval::decode(&bytes).expect("decode failed");
    assert_eq!(got, 42);
}

// ── encode test ─────────────────────────────────────────────────────────────

#[test]
fn encode_exec_result_python_readable() {
    let pickled =
        pyval::encode_exec_result(0, "hello".to_string(), "".to_string()).expect("encode failed");

    let py = python_bin();
    let verify_code = concat!(
        "import pickle, sys\n",
        "d = pickle.loads(sys.stdin.buffer.read())\n",
        "assert d['returncode'] == 0, f\"returncode={d['returncode']}\"\n",
        "assert d['stdout'] == 'hello', f\"stdout={d['stdout']!r}\"\n",
        "assert d['stderr'] == '', f\"stderr={d['stderr']!r}\"\n",
        "print('ok')\n"
    );
    let mut child = Command::new(py)
        .args(["-c", verify_code])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn python");

    child
        .stdin
        .take()
        .unwrap()
        .write_all(&pickled)
        .expect("write to stdin failed");

    let out = child.wait_with_output().expect("wait failed");
    assert!(
        out.status.success(),
        "python verification failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(stdout.trim(), "ok", "expected 'ok', got: {stdout}");
}
