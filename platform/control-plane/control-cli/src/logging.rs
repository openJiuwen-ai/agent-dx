//! Supervisor-owned file logs; only closed archives are compressed or pruned.
use flate2::{write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::OwnedFd,
        unix::{fs::OpenOptionsExt, net::UnixStream},
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, SystemTime},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub enabled: bool,
    pub line_records: bool,
    pub max_record_bytes: usize,
    pub compress_after_seconds: u64,
    pub max_file_bytes: u64,
    pub rotate_seconds: Option<u64>,
    pub compress: bool,
    pub max_files: usize,
    pub max_age_seconds: Option<u64>,
    /// Budget for closed archives; the active file has its own size limit.
    pub max_total_bytes: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            enabled: false,
            line_records: false,
            max_record_bytes: 65536,
            compress_after_seconds: 0,
            max_file_bytes: 100 * 1024 * 1024,
            rotate_seconds: Some(86400),
            compress: true,
            max_files: 5,
            max_age_seconds: Some(7 * 86400),
            max_total_bytes: 1024 * 1024 * 1024,
        }
    }
}
impl Policy {
    pub fn validate(&self) -> crate::Result<()> {
        if self.max_record_bytes == 0
            || self.max_record_bytes > 16 * 1024 * 1024
            || self.max_file_bytes == 0
            || self.max_files == 0
            || self.max_total_bytes == 0
            || self.rotate_seconds == Some(0)
            || self.max_age_seconds == Some(0)
        {
            return Err("logging size, time and retention limits must be positive".into());
        }
        Ok(())
    }
}
#[derive(Default)]
struct Health {
    error: Mutex<Option<String>>,
    failed_bytes: AtomicU64,
}
#[derive(Clone, Serialize)]
pub struct Status {
    pub error: Option<String>,
    pub failed_bytes: u64,
}
impl Health {
    fn report(&self, error: io::Error) {
        let mut last = self.error.lock().expect("shared state lock poisoned");
        let text = error.to_string();
        if last.as_ref() != Some(&text) {
            eprintln!("component log I/O failed: {text}");
        }
        *last = Some(text);
    }
}
struct Archive {
    sequence: u64,
    path: PathBuf,
    bytes: u64,
    modified: SystemTime,
}
fn archives(root: &Path, id: &str) -> io::Result<Vec<Archive>> {
    let prefix = format!("{id}.log.");
    let mut files = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(suffix) = name.strip_prefix(&prefix) else {
            continue;
        };
        let digits = suffix.strip_suffix(".gz").unwrap_or(suffix);
        if digits.len() != 20 || !digits.bytes().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let meta = fs::symlink_metadata(entry.path())?;
        if !meta.is_file() {
            continue;
        }
        files.push(Archive {
            sequence: digits.parse().map_err(io::Error::other)?,
            path: entry.path(),
            bytes: meta.len(),
            modified: meta.modified()?,
        });
    }
    files.sort_by_key(|a| a.sequence);
    Ok(files)
}
fn open(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}
fn maintain(root: &Path, id: &str, policy: &Policy) -> io::Result<()> {
    if policy.compress {
        for archive in archives(root, id)? {
            if archive.modified.elapsed().unwrap_or_default()
                < Duration::from_secs(policy.compress_after_seconds)
            {
                continue;
            }
            if archive.path.extension().is_some_and(|s| s == "gz") {
                continue;
            }
            let gz = PathBuf::from(format!("{}.gz", archive.path.display()));
            let temp = PathBuf::from(format!("{}.gz.tmp", archive.path.display()));
            // A previous interrupted compression still has its canonical source.
            match fs::remove_file(&temp) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => return Err(e),
            }
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temp)?;
            let mut encoder = GzEncoder::new(file, Compression::fast());
            let mut source = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&archive.path)?;
            io::copy(&mut source, &mut encoder)?;
            let compressed = encoder.finish()?;
            compressed.set_times(fs::FileTimes::new().set_modified(archive.modified))?;
            compressed.sync_all()?;
            fs::rename(&temp, &gz)?;
            // If interrupted after rename, retrying from the source safely
            // replaces this gzip, rather than retaining two completed copies.
            fs::remove_file(&archive.path)?;
        }
    }
    let files = archives(root, id)?;
    let mut count = files.len();
    let mut bytes = files.iter().map(|f| u128::from(f.bytes)).sum::<u128>();
    for file in files {
        if file.modified.elapsed().unwrap_or_default()
            < Duration::from_secs(policy.compress_after_seconds)
        {
            continue;
        }
        let expired = policy.max_age_seconds.is_some_and(|age| {
            file.modified
                .elapsed()
                .is_ok_and(|elapsed| elapsed >= Duration::from_secs(age))
        });
        if count > policy.max_files || bytes > u128::from(policy.max_total_bytes) || expired {
            fs::remove_file(file.path)?;
            count -= 1;
            bytes -= u128::from(file.bytes);
        }
    }
    Ok(())
}
struct Writer {
    root: PathBuf,
    id: String,
    policy: Policy,
    file: Option<File>,
    bytes: u64,
    sequence: u64,
    since: SystemTime,
    wake: mpsc::SyncSender<()>,
}
impl Writer {
    fn ensure(&mut self) -> io::Result<()> {
        if self.file.is_none() {
            let f = open(&self.root.join(format!("{}.log", self.id)))?;
            self.bytes = f.metadata()?.len();
            self.since = f.metadata()?.modified()?;
            self.file = Some(f)
        }
        Ok(())
    }
    fn rotate(&mut self) -> io::Result<()> {
        if self.bytes == 0 {
            return Ok(());
        }
        let next = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("log sequence exhausted"))?;
        if let Some(file) = &self.file {
            file.sync_all()?
        }
        drop(self.file.take());
        fs::rename(
            self.root.join(format!("{}.log", self.id)),
            self.root.join(format!("{}.log.{next:020}", self.id)),
        )?;
        self.sequence = next;
        self.bytes = 0;
        self.ensure()?;
        self.since = SystemTime::now();
        let _ = self.wake.try_send(());
        Ok(())
    }
    fn tick(&mut self) -> io::Result<()> {
        self.ensure()?;
        if self.bytes > 0
            && (self.bytes >= self.policy.max_file_bytes
                || self.policy.rotate_seconds.is_some_and(|secs| {
                    self.since
                        .elapsed()
                        .is_ok_and(|age| age >= Duration::from_secs(secs))
                }))
        {
            self.rotate()?
        }
        Ok(())
    }
    fn record(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.tick()?;
        if self.bytes > 0
            && self.bytes.saturating_add(bytes.len() as u64) > self.policy.max_file_bytes
        {
            self.rotate()?;
        }
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::other("active log file missing"))?
            .write_all(bytes)?;
        self.bytes += bytes.len() as u64;
        Ok(())
    }
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            self.tick()?;
            let n = bytes
                .len()
                .min((self.policy.max_file_bytes - self.bytes).min(usize::MAX as u64) as usize);
            self.file
                .as_mut()
                .ok_or_else(|| io::Error::other("active log file missing"))?
                .write_all(&bytes[..n])?;
            self.bytes += n as u64;
            bytes = &bytes[n..];
        }
        Ok(())
    }
}
#[derive(Default)]
struct Records {
    pending: Vec<u8>,
    discarding: bool,
}
impl Records {
    fn push(&mut self, bytes: &[u8], writer: &mut Writer, health: &Health) {
        for part in bytes.split_inclusive(|b| *b == b'\n') {
            let end = part.ends_with(b"\n");
            if self.discarding {
                health
                    .failed_bytes
                    .fetch_add(part.len() as u64, Ordering::Relaxed);
                if end {
                    self.discarding = false;
                }
                continue;
            }
            if self.pending.len() + part.len() > writer.policy.max_record_bytes {
                health
                    .failed_bytes
                    .fetch_add((self.pending.len() + part.len()) as u64, Ordering::Relaxed);
                health.report(io::Error::other(
                    "component log record exceeds max_record_bytes",
                ));
                self.pending.clear();
                self.discarding = !end;
                continue;
            }
            self.pending.extend_from_slice(part);
            if end {
                if let Err(error) = writer.record(&self.pending) {
                    health
                        .failed_bytes
                        .fetch_add(self.pending.len() as u64, Ordering::Relaxed);
                    health.report(error);
                    writer.file = None;
                }
                self.pending.clear();
            }
        }
    }
}
pub struct Capture {
    stop: Arc<AtomicBool>,
    health: Arc<Health>,
    thread: Option<JoinHandle<()>>,
}
impl Capture {
    pub fn attach(
        command: &mut Command,
        root: &Path,
        id: &str,
        policy: Policy,
    ) -> io::Result<Self> {
        let (mut read, write) = UnixStream::pair()?;
        read.set_read_timeout(Some(Duration::from_millis(100)))?;
        let sequence = archives(root, id)?
            .iter()
            .map(|a| a.sequence)
            .max()
            .unwrap_or(0);
        let (wake, events) = mpsc::sync_channel(1);
        let mut writer = Writer {
            root: root.into(),
            id: id.into(),
            policy: policy.clone(),
            file: None,
            bytes: 0,
            sequence,
            since: SystemTime::now(),
            wake,
        };
        writer.ensure()?;
        let stdout: OwnedFd = write.try_clone()?.into();
        let stderr: OwnedFd = write.into();
        command
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        let health = Arc::new(Health::default());
        let stop = Arc::new(AtomicBool::new(false));
        let done = stop.clone();
        let state = health.clone();
        let thread = thread::spawn(move || {
            let (root, id, policy) = (
                writer.root.clone(),
                writer.id.clone(),
                writer.policy.clone(),
            );
            let h = state.clone();
            let maintenance = thread::spawn(move || loop {
                if let Err(error) = maintain(&root, &id, &policy) {
                    h.report(error)
                }
                if matches!(
                    events.recv_timeout(Duration::from_secs(1)),
                    Err(mpsc::RecvTimeoutError::Disconnected)
                ) {
                    break;
                }
            });
            let mut buffer = [0; 16384];
            let mut records = Records::default();
            loop {
                match read.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => {
                        if writer.policy.line_records {
                            records.push(&buffer[..n], &mut writer, &state);
                            continue;
                        }
                        if let Err(error) = writer.write(&buffer[..n]) {
                            state.failed_bytes.fetch_add(n as u64, Ordering::Relaxed);
                            state.report(error);
                            writer.file = None;
                        }
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) =>
                    {
                        if done.load(Ordering::Acquire) {
                            break;
                        }
                        if let Err(error) = writer.tick() {
                            state.report(error)
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => {
                        state.report(error);
                        break;
                    }
                }
            }
            if writer.policy.line_records && !records.pending.is_empty() {
                records.push(b"\n", &mut writer, &state);
            }
            if let Some(file) = &writer.file {
                if let Err(error) = file.sync_all() {
                    state.report(error)
                }
            }
            // The final wake is consumed before channel disconnection, so the
            // maintenance thread completes one last closed-file sweep.
            let _ = writer.wake.send(());
            drop(writer);
            if maintenance.join().is_err() {
                state.report(io::Error::other("log maintenance thread panicked"));
            }
        });
        Ok(Self {
            stop,
            health,
            thread: Some(thread),
        })
    }
    pub fn status(&self) -> Status {
        Status {
            error: self
                .health
                .error
                .lock()
                .expect("shared state lock poisoned")
                .clone(),
            failed_bytes: self.health.failed_bytes.load(Ordering::Relaxed),
        }
    }
    pub fn finish(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                self.health
                    .report(io::Error::other("log capture thread panicked"));
            }
        }
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        self.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    fn policy() -> Policy {
        Policy {
            enabled: true,
            max_file_bytes: 64,
            rotate_seconds: None,
            compress: true,
            max_files: 100,
            max_age_seconds: None,
            max_total_bytes: 100000,
            ..Default::default()
        }
    }
    fn read_all(root: &Path) -> Vec<u8> {
        let mut bytes = Vec::new();
        for file in archives(root, "test").unwrap() {
            if file.path.extension().is_some_and(|s| s == "gz") {
                GzDecoder::new(File::open(file.path).unwrap())
                    .read_to_end(&mut bytes)
                    .unwrap();
            } else {
                File::open(file.path)
                    .unwrap()
                    .read_to_end(&mut bytes)
                    .unwrap();
            }
        }
        if let Ok(mut active) = File::open(root.join("test.log")) {
            active.read_to_end(&mut bytes).unwrap();
        }
        bytes
    }
    #[test]
    fn line_records_survive_rotation_as_complete_json() {
        let root = tempfile::tempdir().unwrap();
        let p: Policy = serde_json::from_value(serde_json::json!({
            "enabled":true,"line_records":true,"max_record_bytes":1024,
            "max_file_bytes":50,"compress_after_seconds":60,"max_files":100
        }))
        .unwrap();
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("i=1; while [ $i -le 20 ]; do printf '{\"sequence\":%s,\"message\":\"record across read and file boundaries\"}\\n' \"$i\"; i=$((i+1)); done");
        let mut capture = Capture::attach(&mut command, root.path(), "test", p).unwrap();
        assert!(command.status().unwrap().success());
        drop(command);
        capture.finish();
        assert!(capture.status().error.is_none());
        let files = archives(root.path(), "test").unwrap();
        assert!(files.len() > 1);
        let mut sequences = Vec::new();
        for path in files
            .into_iter()
            .map(|f| f.path)
            .chain([root.path().join("test.log")])
        {
            assert_ne!(path.extension().unwrap(), "gz");
            for line in fs::read_to_string(path).unwrap().lines() {
                let row: serde_json::Value = serde_json::from_str(line).unwrap();
                sequences.push(row["sequence"].as_u64().unwrap());
            }
        }
        assert_eq!(sequences, (1..=20).collect::<Vec<_>>());
    }

    #[test]
    fn record_buffer_bounds_and_recovers_after_oversize_line() {
        let root = tempfile::tempdir().unwrap();
        let (wake, _events) = mpsc::sync_channel(1);
        let mut p = policy();
        p.max_record_bytes = 8;
        let mut writer = Writer {
            root: root.path().into(),
            id: "test".into(),
            policy: p,
            file: None,
            bytes: 0,
            sequence: 0,
            since: SystemTime::now(),
            wake,
        };
        let health = Health::default();
        let mut records = Records::default();
        records.push(b"ab", &mut writer, &health);
        records.push(b"c\n123456", &mut writer, &health);
        records.push(b"789", &mut writer, &health);
        assert!(records.pending.is_empty());
        records.push(b"0\nok\n", &mut writer, &health);
        assert_eq!(read_all(root.path()), b"abc\nok\n");
        assert_eq!(health.failed_bytes.load(Ordering::Relaxed), 11);
        assert!(health
            .error
            .lock()
            .expect("shared state lock poisoned")
            .is_some());
    }
    #[test]
    fn collection_grace_defers_compression_and_retention() {
        let root = tempfile::tempdir().unwrap();
        let mut p = policy();
        p.compress_after_seconds = 60;
        p.max_files = 1;
        for n in 1..=3 {
            fs::write(root.path().join(format!("test.log.{n:020}")), b"line\n").unwrap();
        }
        maintain(root.path(), "test", &p).unwrap();
        let files = archives(root.path(), "test").unwrap();
        assert_eq!(files.len(), 3);
        for file in files {
            assert_ne!(file.path.extension().unwrap(), "gz");
            File::options()
                .write(true)
                .open(file.path)
                .unwrap()
                .set_times(fs::FileTimes::new().set_modified(SystemTime::UNIX_EPOCH))
                .unwrap();
        }
        maintain(root.path(), "test", &p).unwrap();
        let files = archives(root.path(), "test").unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].sequence, 3);
        assert_eq!(files[0].path.extension().unwrap(), "gz");
        assert_eq!(read_all(root.path()), b"line\n");
    }

    #[test]
    fn capture_preserves_stdout_stderr_across_size_rotation_and_restarts() {
        let root = tempfile::tempdir().unwrap();
        let mut expected = Vec::new();
        for run in 0..2 {
            let text=format!("for i in $(seq 1 80); do printf 'out-{run}-%s\\n' \"$i\"; printf 'err-{run}-%s\\n' \"$i\" >&2; done");
            let mut command = Command::new("/bin/sh");
            command.arg("-c").arg(text);
            let mut capture = Capture::attach(&mut command, root.path(), "test", policy()).unwrap();
            let mut child = command.spawn().unwrap();
            drop(command);
            assert!(child.wait().unwrap().success());
            capture.finish();
            assert!(capture.status().error.is_none());
            assert_eq!(capture.status().failed_bytes, 0);
            for i in 1..=80 {
                expected.extend(format!("out-{run}-{i}\nerr-{run}-{i}\n").bytes());
            }
            assert_eq!(read_all(root.path()), expected);
        }
        let files = archives(root.path(), "test").unwrap();
        assert!(files.len() > 2);
        assert!(files.iter().all(|f| f.path.extension().unwrap() == "gz"));
    }
    #[test]
    fn interrupted_compression_and_publish_failure_preserve_source() {
        let root = tempfile::tempdir().unwrap();
        let raw = root.path().join("test.log.00000000000000000001");
        fs::write(&raw, b"unique bytes").unwrap();
        let gz = root.path().join("test.log.00000000000000000001.gz");
        fs::create_dir(&gz).unwrap();
        assert!(maintain(root.path(), "test", &policy()).is_err());
        assert_eq!(fs::read(&raw).unwrap(), b"unique bytes");
        fs::remove_dir(&gz).unwrap();
        maintain(root.path(), "test", &policy()).unwrap();
        assert_eq!(read_all(root.path()), b"unique bytes");
        assert!(!raw.exists());
        assert!(!root
            .path()
            .join("test.log.00000000000000000001.gz.tmp")
            .exists());
    }
    #[test]
    fn retention_limits_only_owned_closed_files() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("test.log"), b"active").unwrap();
        fs::write(root.path().join("other.log.00000000000000000001"), b"other").unwrap();
        for n in 1..=5 {
            fs::write(root.path().join(format!("test.log.{n:020}")), [b'a'; 10]).unwrap();
        }
        let mut p = policy();
        p.compress = false;
        p.max_files = 3;
        p.max_total_bytes = 20;
        maintain(root.path(), "test", &p).unwrap();
        let files = archives(root.path(), "test").unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].sequence, 4);
        for file in files {
            File::options()
                .write(true)
                .open(file.path)
                .unwrap()
                .set_times(fs::FileTimes::new().set_modified(SystemTime::UNIX_EPOCH))
                .unwrap();
        }
        p.max_age_seconds = Some(1);
        maintain(root.path(), "test", &p).unwrap();
        assert!(archives(root.path(), "test").unwrap().is_empty());
        assert_eq!(fs::read(root.path().join("test.log")).unwrap(), b"active");
        assert!(root.path().join("other.log.00000000000000000001").exists());
    }
    #[test]
    fn idle_tick_rolls_existing_file_after_time_limit() {
        let root = tempfile::tempdir().unwrap();
        let (wake, _events) = mpsc::sync_channel(1);
        let mut p = policy();
        p.rotate_seconds = Some(1);
        let mut writer = Writer {
            root: root.path().into(),
            id: "test".into(),
            policy: p,
            file: None,
            bytes: 0,
            sequence: 0,
            since: SystemTime::now(),
            wake,
        };
        writer.write(b"before time limit").unwrap();
        writer.since = SystemTime::UNIX_EPOCH;
        writer.tick().unwrap();
        writer.write(b"after time limit").unwrap();
        assert_eq!(
            fs::read(root.path().join("test.log.00000000000000000001")).unwrap(),
            b"before time limit"
        );
        assert_eq!(
            fs::read(root.path().join("test.log")).unwrap(),
            b"after time limit"
        );
    }
}

#[cfg(all(test, target_os = "linux"))]
mod linux_faults {
    use super::*;
    #[test]
    fn full_device_returns_write_error() {
        let root = tempfile::tempdir().unwrap();
        let (wake, _events) = mpsc::sync_channel(1);
        let mut writer = Writer {
            root: root.path().into(),
            id: "full".into(),
            policy: Policy::default(),
            file: Some(OpenOptions::new().write(true).open("/dev/full").unwrap()),
            bytes: 0,
            sequence: 0,
            since: SystemTime::now(),
            wake,
        };
        assert_eq!(
            writer
                .write(b"must not disappear silently")
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ENOSPC)
        );
    }
}
