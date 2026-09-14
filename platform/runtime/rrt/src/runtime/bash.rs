//! Rust implementation of akernel persistent `bash_*` PTY sessions using portable-pty, a global session table, and sentinels.
//! Semantics match `instance.py`: persistent bash attaches to a PTY, submit writes `cmd; echo SENTINEL$?`, and poll completes after reading the sentinel.

use super::codec::{kw_str, map_value};
use rmpv::Value;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex, OnceLock};

const RRT_PROMPT: &str = "__RRT_PROMPT__ ";

struct Session {
    writer: Box<dyn Write + Send>,
    output: Arc<Mutex<String>>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
}

fn sessions() -> &'static Mutex<HashMap<String, Session>> {
    static S: OnceLock<Mutex<HashMap<String, Session>>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(HashMap::new()))
}

fn nil() -> Value {
    Value::Nil
}

/// bash_init(session_id, shell) → {error}。
pub fn bash_init(kw: &BTreeMap<String, Value>) -> Value {
    let sid = kw_str(kw, "session_id").unwrap_or_default();
    let shell = kw_str(kw, "shell").unwrap_or_else(|| "/bin/bash".into());
    {
        if sessions().lock().unwrap().contains_key(&sid) {
            return map_value(vec![(
                "error",
                Value::from(format!("session {sid} already exists")),
            )]);
        }
    }
    let pty = portable_pty::native_pty_system();
    let pair = match pty.openpty(portable_pty::PtySize::default()) {
        Ok(p) => p,
        Err(e) => return map_value(vec![("error", Value::from(format!("openpty failed: {e}")))]),
    };
    let mut cmd = portable_pty::CommandBuilder::new(&shell);
    super::child_env::apply_pty(&mut cmd);
    // A deterministic reserved prompt lets clients remove terminal echo
    // without accidentally deleting command output that did not end in a
    // newline (for example `printf value`).
    cmd.env("PS1", RRT_PROMPT);
    cmd.env("PS2", RRT_PROMPT);
    // An interactive bash normally reads ~/.bashrc after importing the
    // environment.  A user or image-level bashrc can therefore replace PS1
    // and make the PTY framing ambiguous.  RRT sessions are an API transport,
    // not a login shell: keep startup deterministic and let callers apply
    // their desired cwd/env through shell.run.
    if shell.rsplit('/').next() == Some("bash") {
        cmd.arg("--noprofile");
        cmd.arg("--norc");
        cmd.arg("-i");
    }
    let child = match pair.slave.spawn_command(cmd) {
        Ok(c) => c,
        Err(e) => return map_value(vec![("error", Value::from(format!("spawn failed: {e}")))]),
    };
    let reader = match pair.master.try_clone_reader() {
        Ok(r) => r,
        Err(e) => return map_value(vec![("error", Value::from(format!("reader failed: {e}")))]),
    };
    let writer = match pair.master.take_writer() {
        Ok(w) => w,
        Err(e) => return map_value(vec![("error", Value::from(format!("writer failed: {e}")))]),
    };
    let output = Arc::new(Mutex::new(String::new()));
    let out2 = output.clone();
    std::thread::spawn(move || {
        let mut reader = reader;
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => out2
                    .lock()
                    .unwrap()
                    .push_str(&String::from_utf8_lossy(&buf[..n])),
            }
        }
    });
    sessions().lock().unwrap().insert(
        sid,
        Session {
            writer,
            output,
            child,
        },
    );
    map_value(vec![("error", nil())])
}

/// bash_submit(session_id, command, timeout) → {error}。
pub fn bash_submit(kw: &BTreeMap<String, Value>) -> Value {
    let sid = kw_str(kw, "session_id").unwrap_or_default();
    let command = kw_str(kw, "command").unwrap_or_default();
    let mut s = sessions().lock().unwrap();
    let session = match s.get_mut(&sid) {
        Some(x) => x,
        None => {
            return map_value(vec![(
                "error",
                Value::from(format!("session {sid} not found")),
            )])
        }
    };
    session.output.lock().unwrap().clear();
    // Command plus sentinel carrying the exit code. __RRT_DONE_<n>__ distinguishes echoed `$?` from the actual numeric code.
    let line = format!("{command}\necho __RRT_DONE_$?__\n");
    match session
        .writer
        .write_all(line.as_bytes())
        .and_then(|_| session.writer.flush())
    {
        Ok(_) => map_value(vec![("error", nil())]),
        Err(e) => map_value(vec![("error", Value::from(e.to_string()))]),
    }
}

/// bash_poll(session_id, wait_timeout) → {status, stdout?, stderr?, exit_code?}。
pub fn bash_poll(kw: &BTreeMap<String, Value>) -> Value {
    let sid = kw_str(kw, "session_id").unwrap_or_default();
    let s = sessions().lock().unwrap();
    let session = match s.get(&sid) {
        Some(x) => x,
        None => {
            return map_value(vec![
                ("status", Value::from("error")),
                ("error", Value::from(format!("session {sid} not found"))),
            ])
        }
    };
    let out = session.output.lock().unwrap().clone();
    // Find __RRT_DONE_<digits>__: skip echoed `echo __RRT_DONE_$?__` entries where `$?` is non-numeric.
    let marker = "__RRT_DONE_";
    let mut from = 0;
    while let Some(rel) = out[from..].find(marker) {
        let pos = from + rel;
        let rest = &out[pos + marker.len()..];
        if let Some(end) = rest.find("__") {
            if let Ok(code) = rest[..end].parse::<i64>() {
                let stdout = out[..pos].to_string();
                return map_value(vec![
                    ("status", Value::from("done")),
                    ("stdout", Value::from(stdout)),
                    ("stderr", Value::from("")),
                    ("exit_code", Value::from(code)),
                ]);
            }
        }
        from = pos + marker.len();
    }
    map_value(vec![("status", Value::from("running"))])
}

/// bash_destroy(session_id) → {error}。
pub fn bash_destroy(kw: &BTreeMap<String, Value>) -> Value {
    let sid = kw_str(kw, "session_id").unwrap_or_default();
    if let Some(mut session) = sessions().lock().unwrap().remove(&sid) {
        let _ = session.child.kill();
    }
    map_value(vec![("error", nil())])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn args(values: &[(&str, Value)]) -> BTreeMap<String, Value> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
        let Value::Map(entries) = value else {
            return None;
        };
        entries
            .iter()
            .find_map(|(name, value)| (name.as_str() == Some(key)).then_some(value))
    }

    fn poll_until_done(session_id: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let result = bash_poll(&args(&[
                ("session_id", Value::from(session_id)),
                ("wait_timeout", Value::from(1)),
            ]));
            if field(&result, "status").and_then(Value::as_str) == Some("done") {
                return result;
            }
            assert!(Instant::now() < deadline, "shell command did not complete");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn deterministic_prompt_preserves_output_without_trailing_newline() {
        let session_id = format!("prompt-test-{}", std::process::id());
        let init = bash_init(&args(&[
            ("session_id", Value::from(session_id.as_str())),
            ("shell", Value::from("/bin/bash")),
        ]));
        assert_eq!(field(&init, "error"), Some(&Value::Nil));

        let export = bash_submit(&args(&[
            ("session_id", Value::from(session_id.as_str())),
            ("command", Value::from("export SDK_E2E=stateful")),
        ]));
        assert_eq!(field(&export, "error"), Some(&Value::Nil));
        let _ = poll_until_done(&session_id);

        let submit = bash_submit(&args(&[
            ("session_id", Value::from(session_id.as_str())),
            ("command", Value::from("printf $SDK_E2E")),
        ]));
        assert_eq!(field(&submit, "error"), Some(&Value::Nil));
        let result = poll_until_done(&session_id);
        let stdout = field(&result, "stdout")
            .and_then(Value::as_str)
            .expect("stdout");
        assert!(
            stdout.contains("stateful__RRT_PROMPT__ echo __RRT_DONE_$?__"),
            "unexpected shell framing: {stdout:?}"
        );

        let _ = bash_destroy(&args(&[("session_id", Value::from(session_id.as_str()))]));
    }
}
