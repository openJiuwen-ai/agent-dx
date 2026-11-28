//! Command output retention and replay for the Execd HTTP data plane.
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use rmpv::Value;

pub(super) const DEFAULT_REGISTRY_MAX_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_OUTPUT_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const CAPTURE_CHUNK_BYTES: usize = 8192;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
static MEMORY_BYTES: AtomicUsize = AtomicUsize::new(0);
static SPOOL_BYTES: AtomicUsize = AtomicUsize::new(0);
static SPOOL_FILES: AtomicUsize = AtomicUsize::new(0);
static SPOOL_STREAMS: AtomicU64 = AtomicU64::new(0);
static INCOMPLETE_STREAMS: AtomicU64 = AtomicU64::new(0);
static TRUNCATED_STREAMS: AtomicU64 = AtomicU64::new(0);

pub(super) fn metrics() -> (usize, u64, usize, usize, u64, u64) {
    (
        MEMORY_BYTES.load(Ordering::Relaxed),
        TRUNCATED_STREAMS.load(Ordering::Relaxed),
        SPOOL_BYTES.load(Ordering::Relaxed),
        SPOOL_FILES.load(Ordering::Relaxed),
        SPOOL_STREAMS.load(Ordering::Relaxed),
        INCOMPLETE_STREAMS.load(Ordering::Relaxed),
    )
}

fn limit(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn stream_limit(name: &str) -> usize {
    limit(
        name,
        limit(
            "EXECD_COMMAND_OUTPUT_LIMIT_BYTES",
            DEFAULT_OUTPUT_LIMIT_BYTES,
        ),
    )
}

/// One budget shared by the two streams of a command.
pub(super) struct Spool {
    directory: PathBuf,
    max_total_bytes: usize,
    max_command_bytes: usize,
    max_files: usize,
    command_bytes: AtomicUsize,
}

impl Spool {
    pub(super) fn enabled() -> bool {
        std::env::var_os("EXECD_COMMAND_SPOOL_DIR").is_some_and(|path| !path.is_empty())
    }

    pub(super) fn configured() -> Option<Arc<Self>> {
        let path = std::env::var_os("EXECD_COMMAND_SPOOL_DIR").filter(|path| !path.is_empty())?;
        let max_total_bytes = limit("EXECD_COMMAND_SPOOL_MAX_BYTES", 1024 * 1024 * 1024);
        Some(Arc::new(Self {
            directory: PathBuf::from(path),
            max_total_bytes,
            max_command_bytes: limit("EXECD_COMMAND_SPOOL_MAX_COMMAND_BYTES", 512 * 1024 * 1024)
                .min(max_total_bytes),
            max_files: limit("EXECD_COMMAND_SPOOL_MAX_FILES", 256),
            command_bytes: AtomicUsize::new(0),
        }))
    }
}

fn reserve(counter: &AtomicUsize, amount: usize, limit: usize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(amount).filter(|total| *total <= limit)
        })
        .is_ok()
}

#[derive(Default)]
struct Memory(Vec<u8>);

impl Drop for Memory {
    fn drop(&mut self) {
        MEMORY_BYTES.fetch_sub(self.0.len(), Ordering::AcqRel);
    }
}

struct DiskReservation {
    spool: Arc<Spool>,
    bytes: usize,
}

impl DiskReservation {
    fn grow(&mut self, amount: usize) -> Result<(), OutputFailure> {
        if !reserve(
            &self.spool.command_bytes,
            amount,
            self.spool.max_command_bytes,
        ) {
            return Err(OutputFailure::SpoolQuota);
        }
        if !reserve(&SPOOL_BYTES, amount, self.spool.max_total_bytes) {
            self.spool.command_bytes.fetch_sub(amount, Ordering::AcqRel);
            return Err(OutputFailure::SpoolQuota);
        }
        self.bytes += amount;
        Ok(())
    }
}

impl Drop for DiskReservation {
    fn drop(&mut self) {
        self.spool
            .command_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
        SPOOL_BYTES.fetch_sub(self.bytes, Ordering::AcqRel);
        SPOOL_FILES.fetch_sub(1, Ordering::AcqRel);
    }
}

struct SpoolFile {
    // Close the anonymous file before releasing its quota.
    file: File,
    reservation: DiskReservation,
    read_failed: AtomicBool,
}

impl SpoolFile {
    fn create(spool: &Arc<Spool>, bytes: usize) -> Result<Self, OutputFailure> {
        if !reserve(&SPOOL_FILES, 1, spool.max_files) {
            return Err(OutputFailure::SpoolQuota);
        }
        let mut reservation = DiskReservation {
            spool: Arc::clone(spool),
            bytes: 0,
        };
        reservation.grow(bytes)?;
        let file = tempfile::tempfile_in(&spool.directory).map_err(OutputFailure::storage)?;
        Ok(Self {
            file,
            reservation,
            read_failed: AtomicBool::new(false),
        })
    }

    fn append(&mut self, data: &[u8]) -> Result<(), OutputFailure> {
        self.reservation.grow(data.len())?;
        self.file.write_all(data).map_err(OutputFailure::storage)
    }

    fn record_read_failure(&self) {
        if !self.read_failed.swap(true, Ordering::AcqRel) {
            INCOMPLETE_STREAMS.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn read_at(&self, offset: usize, data: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.file.read_at(data, offset as u64) {
                Ok(0) if offset < self.reservation.bytes => {
                    self.record_read_failure();
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "command output ended early",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    self.record_read_failure();
                    return Err(error);
                }
                Ok(count) => return Ok(count),
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputFailure {
    SpoolQuota,
    NoSpace,
    SpoolIo,
    PipeRead,
    DrainTimeout,
}

impl OutputFailure {
    fn storage(error: io::Error) -> Self {
        if matches!(
            error.raw_os_error(),
            Some(libc::ENOSPC) | Some(libc::EDQUOT)
        ) {
            Self::NoSpace
        } else {
            Self::SpoolIo
        }
    }

    fn reason(self) -> &'static str {
        match self {
            Self::SpoolQuota => "spool_quota",
            Self::NoSpace => "no_space",
            Self::SpoolIo => "spool_io",
            Self::PipeRead => "pipe_read",
            Self::DrainTimeout => "drain_timeout",
        }
    }
}

enum Storage {
    Memory(Memory),
    File(SpoolFile),
    Failed(OutputFailure),
}

struct Capture {
    storage: Storage,
    limit: usize,
    registry_limit: usize,
    spool: Option<Arc<Spool>>,
    truncated: bool,
}

impl Capture {
    fn new(limit: usize, spool: Option<Arc<Spool>>) -> Self {
        Self {
            storage: Storage::Memory(Memory::default()),
            limit,
            registry_limit: limit_from_env(),
            spool,
            truncated: false,
        }
    }

    fn append(&mut self, bytes: &[u8]) {
        if let Err(failure) = self.try_append(bytes) {
            self.storage = Storage::Failed(failure);
            INCOMPLETE_STREAMS.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn try_append(&mut self, bytes: &[u8]) -> Result<(), OutputFailure> {
        let data = match &mut self.storage {
            Storage::File(file) => return file.append(bytes),
            Storage::Memory(data) => data,
            Storage::Failed(_) => return Ok(()),
        };
        let room = self.limit.saturating_sub(data.0.len());
        if let Some(spool) = &self.spool {
            if bytes.len() <= room && reserve(&MEMORY_BYTES, bytes.len(), self.registry_limit) {
                data.0.extend_from_slice(bytes);
            } else {
                let mut file = SpoolFile::create(spool, data.0.len() + bytes.len())?;
                file.file
                    .write_all(&data.0)
                    .map_err(OutputFailure::storage)?;
                file.file.write_all(bytes).map_err(OutputFailure::storage)?;
                self.storage = Storage::File(file);
                SPOOL_STREAMS.fetch_add(1, Ordering::Relaxed);
            }
        } else if !self.truncated {
            let mut take = 0;
            let _ = MEMORY_BYTES.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                take = room
                    .min(bytes.len())
                    .min(self.registry_limit.saturating_sub(used));
                (take > 0).then_some(used + take)
            });
            data.0.extend_from_slice(&bytes[..take]);
            if take < bytes.len() {
                self.truncated = true;
                TRUNCATED_STREAMS.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn fail_pipe(&mut self, reason: OutputFailure) {
        if !matches!(self.storage, Storage::Failed(_)) && self.spool.is_some() {
            self.storage = Storage::Failed(reason);
            INCOMPLETE_STREAMS.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn memory_bytes(&self) -> usize {
        match &self.storage {
            Storage::Memory(data) => data.0.len(),
            _ => 0,
        }
    }

    fn seal(self) -> Output {
        Output(Arc::new(SealedOutput {
            storage: self.storage,
            truncated: self.truncated,
        }))
    }
}

fn limit_from_env() -> usize {
    limit(
        "EXECD_COMMAND_REGISTRY_MAX_BYTES",
        DEFAULT_REGISTRY_MAX_BYTES,
    )
}

struct SealedOutput {
    storage: Storage,
    truncated: bool,
}

#[derive(Clone)]
pub(super) struct Output(Arc<SealedOutput>);

impl Output {
    pub(super) fn text(text: String) -> Self {
        let data = text.into_bytes();
        MEMORY_BYTES.fetch_add(data.len(), Ordering::AcqRel);
        Self(Arc::new(SealedOutput {
            storage: Storage::Memory(Memory(data)),
            truncated: false,
        }))
    }

    pub(super) fn is_file(&self) -> bool {
        matches!(self.0.storage, Storage::File(_))
    }

    pub(super) fn inline_text(&self) -> String {
        match &self.0.storage {
            Storage::Memory(data) => String::from_utf8_lossy(&data.0).into_owned(),
            _ => String::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        match &self.0.storage {
            Storage::Memory(data) => data.0.len(),
            Storage::File(file) => file.reservation.bytes,
            Storage::Failed(_) => 0,
        }
    }

    pub(super) fn truncated(&self) -> bool {
        self.0.truncated || self.failure().is_some()
    }

    pub(super) fn failure(&self) -> Option<&'static str> {
        match self.0.storage {
            Storage::Failed(reason) => Some(reason.reason()),
            _ => None,
        }
    }

    pub(super) fn check_file(&self) -> io::Result<()> {
        if let Storage::File(file) = &self.0.storage {
            if file.read_failed.load(Ordering::Acquire) {
                return Err(io::Error::other("previous command output read failed"));
            }
            let actual = file.file.metadata()?.len();
            if actual != file.reservation.bytes as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "command output length changed",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn read_at(&self, offset: usize, data: &mut [u8]) -> io::Result<usize> {
        match &self.0.storage {
            Storage::Memory(memory) => (&memory.0[offset..]).read(data),
            Storage::File(file) => file.read_at(offset, data),
            Storage::Failed(_) => Err(io::Error::other("command output is incomplete")),
        }
    }
}

#[derive(Default)]
struct ReaderState {
    deadline: Option<Instant>,
    output: Option<Output>,
}

/// A reader only shares accounting and its sealed result with the registry.
/// The worker owns its pipe and storage while it performs blocking file I/O.
#[derive(Clone, Default)]
pub(super) struct Reader(Arc<ReaderInner>);

#[derive(Default)]
struct ReaderInner {
    state: Mutex<ReaderState>,
    completed: Condvar,
    total: AtomicU64,
    memory: AtomicUsize,
}

impl Reader {
    pub(super) fn empty() -> Self {
        let reader = Self::default();
        reader.publish(Capture::new(0, None).seal());
        reader
    }

    pub(super) fn spawn(
        stream: Option<impl Read + AsRawFd + Send + 'static>,
        limit_name: &str,
        spool: Option<Arc<Spool>>,
    ) -> Self {
        let Some(stream) = stream else {
            return Self::empty();
        };
        let reader = Self::default();
        let worker = reader.clone();
        let limit = stream_limit(limit_name);
        let worker_spool = spool.clone();
        let started = std::thread::Builder::new().spawn(move || {
            worker.drain(stream, Capture::new(limit, worker_spool));
        });
        if started.is_err() {
            let mut capture = Capture::new(limit, spool);
            capture.fail_pipe(OutputFailure::PipeRead);
            reader.publish(capture.seal());
        }
        reader
    }

    fn drain(&self, mut stream: impl Read + AsRawFd, mut capture: Capture) {
        let mut buffer = [0; CAPTURE_CHUNK_BYTES];
        loop {
            let deadline = self.0.state.lock().ok().and_then(|state| state.deadline);
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                capture.fail_pipe(OutputFailure::DrainTimeout);
                break;
            }
            match poll_pipe(&stream) {
                Ok(false) => continue,
                Ok(true) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    capture.fail_pipe(OutputFailure::PipeRead);
                    break;
                }
            }
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    self.0.total.fetch_add(count as u64, Ordering::Relaxed);
                    capture.append(&buffer[..count]);
                    self.0
                        .memory
                        .store(capture.memory_bytes(), Ordering::Relaxed);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    capture.fail_pipe(OutputFailure::PipeRead);
                    break;
                }
            }
        }
        self.0
            .memory
            .store(capture.memory_bytes(), Ordering::Relaxed);
        self.publish(capture.seal());
    }

    fn publish(&self, output: Output) {
        let Ok(mut state) = self.0.state.lock() else {
            return;
        };
        if state.output.is_none() {
            state.output = Some(output);
            self.0.completed.notify_all();
        }
    }

    pub(super) fn total_bytes(&self) -> u64 {
        self.0.total.load(Ordering::Relaxed)
    }

    pub(super) fn memory_bytes(&self) -> usize {
        self.0.memory.load(Ordering::Relaxed)
    }

    pub(super) fn retained_bytes(&self) -> usize {
        self.output()
            .map_or_else(|| self.memory_bytes(), |output| output.len())
    }

    pub(super) fn output(&self) -> Option<Output> {
        self.0
            .state
            .lock()
            .ok()
            .and_then(|state| state.output.clone())
    }

    fn set_deadline(&self, deadline: Instant) {
        if let Ok(mut state) = self.0.state.lock() {
            state.deadline = Some(deadline);
        }
    }

    fn wait(&self, deadline: Instant) {
        if let Ok(state) = self.0.state.lock() {
            drop(self.0.completed.wait_timeout_while(
                state,
                deadline.saturating_duration_since(Instant::now()),
                |state| state.output.is_none(),
            ));
        }
    }

    pub(super) fn finish(stdout: &Self, stderr: &Self) {
        let deadline = Instant::now() + DRAIN_TIMEOUT;
        stdout.set_deadline(deadline);
        stderr.set_deadline(deadline);
        // Allow the worker to finish its final poll and publish the sealed state.
        let publish_deadline = deadline + Duration::from_millis(100);
        stdout.wait(publish_deadline);
        stderr.wait(publish_deadline);
        for reader in [stdout, stderr] {
            let Ok(mut state) = reader.0.state.lock() else {
                continue;
            };
            if state.output.is_none() {
                state.output = Some(Output(Arc::new(SealedOutput {
                    storage: Storage::Failed(OutputFailure::DrainTimeout),
                    truncated: false,
                })));
                INCOMPLETE_STREAMS.fetch_add(1, Ordering::Relaxed);
                reader.0.completed.notify_all();
            }
        }
    }
}

fn poll_pipe(pipe: &impl AsRawFd) -> io::Result<bool> {
    let mut fd = libc::pollfd {
        fd: pipe.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: fd lives for the whole call and the borrowed pipe owns its descriptor.
    let result = unsafe { libc::poll(&mut fd, 1, 50) };
    match result {
        -1 => Err(io::Error::last_os_error()),
        0 => Ok(false),
        _ => Ok(true),
    }
}

#[derive(Clone)]
pub(super) struct CommandOutput {
    pub stdout: Output,
    pub stderr: Output,
}

impl CommandOutput {
    pub(super) fn from_readers(stdout: &Reader, stderr: &Reader) -> Self {
        Self {
            stdout: stdout
                .output()
                .unwrap_or_else(|| Output::text(String::new())),
            stderr: stderr
                .output()
                .unwrap_or_else(|| Output::text(String::new())),
        }
    }

    pub(super) fn has_files(&self) -> bool {
        self.stdout.is_file() || self.stderr.is_file()
    }
}

#[derive(Clone)]
pub(super) struct ActionResult {
    pub fields: Value,
    pub output: Option<CommandOutput>,
}

impl From<Value> for ActionResult {
    fn from(fields: Value) -> Self {
        Self {
            fields,
            output: None,
        }
    }
}

impl ActionResult {
    pub(super) fn with_output(fields: Value, output: CommandOutput) -> Self {
        Self {
            fields,
            output: Some(output),
        }
    }

    pub(super) fn into_value(mut self) -> Value {
        if let (Value::Map(fields), Some(output)) = (&mut self.fields, self.output) {
            for (field, value) in fields {
                match field.as_str() {
                    Some("stdout") => *value = output.stdout.inline_text().into(),
                    Some("stderr") => *value = output.stderr.inline_text().into(),
                    _ => {}
                }
            }
        }
        self.fields
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_spool_retains_only_the_prefix() {
        let mut capture = Capture::new(4, None);
        capture.append(b"abcdef");
        capture.append(b"gh");
        let output = capture.seal();
        assert_eq!(output.inline_text(), "abcd");
        assert!(output.truncated());
    }
}
