use flate2::write::GzEncoder;
use flate2::Compression;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::prelude::*;

const DEFAULT_MAX_SIZE_MB: u64 = 40;
const DEFAULT_MAX_FILES: usize = 10;
const DEFAULT_QUEUE_CAPACITY: usize = 32_768;
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 200;
const DEFAULT_COMPRESSION: &str = "gzip";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[must_use = "keep the logging guard alive until the service has drained"]
pub struct LoggingGuard {
    workers: Vec<WorkerGuard>,
    _trace: adx_observability::trace::TraceGuard,
}

impl LoggingGuard {
    pub fn shutdown(mut self) -> io::Result<()> {
        let mut first_error = None;
        for worker in &mut self.workers {
            if let Err(error) = worker.shutdown() {
                first_error.get_or_insert(error);
            }
        }
        self.workers.clear();
        first_error.map_or(Ok(()), Err)
    }
}

pub fn init(
    component: &str,
    edge_component: bool,
) -> Result<LoggingGuard, Box<dyn std::error::Error>> {
    let config = LoggingConfig::from_env()?;
    let trace = adx_observability::trace::init(component)
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;
    if adx_observability::json_enabled().map_err(|e| -> Box<dyn std::error::Error> { e })? {
        if config.directory.is_some() {
            return Err("JSON logging uses stdout; leave ADX_DATA_PLANE_LOG_DIR unset".into());
        }
        let access = config.edge_access_log;
        let audit = config.edge_audit_log;
        let stdout = config.stdout;
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_ansi(false)
                    .with_target(true)
                    .with_filter(filter_fn(move |metadata| {
                        stdout
                            && include_general_log_target(
                                edge_component,
                                true,
                                access,
                                audit,
                                metadata.target(),
                            )
                    }))
                    .with_filter(env_filter()),
            )
            .try_init()?;
        return Ok(LoggingGuard {
            workers: Vec::new(),
            _trace: trace,
        });
    }
    let mut workers = Vec::new();
    let edge_access_enabled = edge_component && config.edge_access_log;
    let edge_audit_enabled = edge_component && config.edge_audit_log;
    let file_writer = if let Some(directory) = config.directory.as_ref() {
        let (writer, worker) = AsyncRollingWriter::new(
            directory.join(format!("{component}.log")),
            config.max_size_bytes,
            config.max_files,
            config.queue_capacity,
            config.flush_interval,
            config.compression,
        )?;
        workers.push(worker);
        Some(writer)
    } else {
        None
    };
    let access_writer = if edge_access_enabled || edge_audit_enabled {
        if let Some(directory) = config.directory.as_ref() {
            let (writer, worker) = AsyncRollingWriter::new(
                directory.join("edge-frontend-access.log"),
                config.max_size_bytes,
                config.max_files,
                config.queue_capacity,
                config.flush_interval,
                config.compression,
            )?;
            workers.push(worker);
            Some(writer)
        } else {
            None
        }
    } else {
        None
    };
    let access_in_general_log =
        (edge_access_enabled || edge_audit_enabled) && access_writer.is_none();
    let stdout_layer = config.stdout.then(|| {
        tracing_subscriber::fmt::layer()
            .with_target(true)
            .with_filter(filter_fn(move |metadata| {
                include_general_log_target(
                    edge_component,
                    access_in_general_log,
                    edge_access_enabled,
                    edge_audit_enabled,
                    metadata.target(),
                )
            }))
            .with_filter(env_filter())
    });
    let file_layer = file_writer.map(|writer| {
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_target(true)
            .with_writer(writer)
            .with_filter(filter_fn(move |metadata| {
                include_general_log_target(
                    edge_component,
                    access_in_general_log,
                    edge_access_enabled,
                    edge_audit_enabled,
                    metadata.target(),
                )
            }))
            .with_filter(env_filter())
    });
    let access_layer = access_writer.map(|writer| {
        tracing_subscriber::fmt::layer()
            .compact()
            .with_ansi(false)
            .with_target(true)
            .with_writer(writer)
            .with_filter(filter_fn(move |metadata| {
                (matches!(metadata.target(), "adx_access") && edge_access_enabled)
                    || (matches!(metadata.target(), "adx_audit") && edge_audit_enabled)
            }))
    });
    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(file_layer)
        .with(access_layer)
        .try_init()?;
    Ok(LoggingGuard {
        workers,
        _trace: trace,
    })
}

fn include_general_log_target(
    edge_component: bool,
    access_in_general_log: bool,
    edge_access_enabled: bool,
    edge_audit_enabled: bool,
    target: &str,
) -> bool {
    if !edge_component {
        return true;
    }
    match target {
        "adx_access" => edge_access_enabled && access_in_general_log,
        "adx_audit" => edge_audit_enabled && access_in_general_log,
        _ => true,
    }
}

fn env_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
}

struct LoggingConfig {
    directory: Option<PathBuf>,
    max_size_bytes: u64,
    max_files: usize,
    stdout: bool,
    edge_access_log: bool,
    edge_audit_log: bool,
    queue_capacity: usize,
    flush_interval: Duration,
    compression: LogCompression,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogCompression {
    None,
    Gzip,
}

impl LoggingConfig {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let directory = env::var("ADX_DATA_PLANE_LOG_DIR")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        if let Some(directory) = &directory {
            fs::create_dir_all(directory)?;
            let probe = directory.join(format!(".write-probe-{}", std::process::id()));
            File::create(&probe)?;
            fs::remove_file(probe)?;
        }
        let max_size_mb = parse_positive("ADX_DATA_PLANE_LOG_MAX_SIZE_MB", DEFAULT_MAX_SIZE_MB)?;
        let max_files = parse_positive("ADX_DATA_PLANE_LOG_MAX_FILES", DEFAULT_MAX_FILES as u64)?;
        let queue_capacity = parse_positive(
            "ADX_DATA_PLANE_LOG_QUEUE_CAPACITY",
            DEFAULT_QUEUE_CAPACITY as u64,
        )?;
        let flush_interval_ms = parse_positive(
            "ADX_DATA_PLANE_LOG_FLUSH_INTERVAL_MS",
            DEFAULT_FLUSH_INTERVAL_MS,
        )?;
        Ok(Self {
            directory,
            max_size_bytes: max_size_mb
                .checked_mul(1024 * 1024)
                .ok_or("ADX_DATA_PLANE_LOG_MAX_SIZE_MB is too large")?,
            max_files: usize::try_from(max_files)?,
            stdout: parse_bool("ADX_DATA_PLANE_LOG_STDOUT", true)?,
            edge_access_log: parse_bool("ADX_DATA_PLANE_EDGE_FRONTEND_ACCESS_LOG_ENABLED", true)?,
            edge_audit_log: parse_bool("ADX_DATA_PLANE_EDGE_FRONTEND_AUDIT_LOG_ENABLED", true)?,
            queue_capacity: usize::try_from(queue_capacity)?,
            flush_interval: Duration::from_millis(flush_interval_ms),
            compression: parse_compression()?,
        })
    }
}

fn parse_compression() -> Result<LogCompression, Box<dyn std::error::Error>> {
    match env::var("ADX_DATA_PLANE_LOG_COMPRESSION")
        .unwrap_or_else(|_| DEFAULT_COMPRESSION.to_owned())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "none" => Ok(LogCompression::None),
        "gzip" => Ok(LogCompression::Gzip),
        value => Err(format!(
            "ADX_DATA_PLANE_LOG_COMPRESSION must be 'none' or 'gzip', got '{value}'"
        )
        .into()),
    }
}

fn parse_positive(name: &str, default: u64) -> Result<u64, Box<dyn std::error::Error>> {
    let value = env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse::<u64>()?;
    if value == 0 {
        return Err(format!("{name} must be greater than zero").into());
    }
    Ok(value)
}

fn parse_bool(name: &str, default: bool) -> Result<bool, Box<dyn std::error::Error>> {
    match env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("{name} must be a boolean").into()),
    }
}

enum WorkerMessage {
    Record(Vec<u8>),
    Shutdown(mpsc::Sender<io::Result<()>>),
}

#[derive(Clone)]
struct AsyncRollingWriter {
    sender: SyncSender<WorkerMessage>,
    dropped: Arc<AtomicU64>,
}

struct EventWriter {
    sender: SyncSender<WorkerMessage>,
    dropped: Arc<AtomicU64>,
    buffer: Vec<u8>,
}

struct WorkerGuard {
    sender: SyncSender<WorkerMessage>,
    thread: Option<JoinHandle<()>>,
    compressor: Option<CompressionGuard>,
}

struct RollingState {
    path: PathBuf,
    writer: BufWriter<File>,
    size: u64,
    max_size: u64,
    max_files: usize,
    compression: Option<Sender<CompressionMessage>>,
    rotation_sequence: u64,
}

enum CompressionMessage {
    Compress(PathBuf),
    Shutdown(Sender<io::Result<()>>),
}

struct CompressionGuard {
    sender: Sender<CompressionMessage>,
    thread: Option<JoinHandle<()>>,
}

impl AsyncRollingWriter {
    fn new(
        path: PathBuf,
        max_size: u64,
        max_files: usize,
        queue_capacity: usize,
        flush_interval: Duration,
        compression: LogCompression,
    ) -> io::Result<(Self, WorkerGuard)> {
        let (compression_sender, compressor) = match compression {
            LogCompression::None => (None, None),
            LogCompression::Gzip => {
                let (sender, guard) = CompressionGuard::new(path.clone(), max_files)?;
                (Some(sender), Some(guard))
            }
        };
        let state = RollingState::new(path.clone(), max_size, max_files, compression_sender)?;
        let (sender, receiver) = mpsc::sync_channel(queue_capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let worker_dropped = dropped.clone();
        let thread_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("data-plane")
            .to_owned();
        let worker = thread::Builder::new()
            .name(format!("log-writer-{thread_name}"))
            .spawn(move || run_worker(state, receiver, worker_dropped, flush_interval))?;
        Ok((
            Self {
                sender: sender.clone(),
                dropped,
            },
            WorkerGuard {
                sender,
                thread: Some(worker),
                compressor,
            },
        ))
    }
}

impl<'a> MakeWriter<'a> for AsyncRollingWriter {
    type Writer = EventWriter;

    fn make_writer(&'a self) -> Self::Writer {
        EventWriter {
            sender: self.sender.clone(),
            dropped: self.dropped.clone(),
            buffer: Vec::with_capacity(512),
        }
    }
}

impl Write for EventWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Flushing is deliberately owned by the background worker. A tracing
        // formatter may call this on the request thread; blocking here would
        // reintroduce the latency this writer is designed to avoid.
        Ok(())
    }
}

impl Drop for EventWriter {
    fn drop(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let record = std::mem::take(&mut self.buffer);
        match self.sender.try_send(WorkerMessage::Record(record)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl WorkerGuard {
    fn shutdown(&mut self) -> io::Result<()> {
        let mut first_error = None;
        if let Some(worker) = self.thread.take() {
            let (result_sender, result_receiver) = mpsc::channel();
            if self
                .sender
                .send(WorkerMessage::Shutdown(result_sender))
                .is_err()
            {
                if worker.join().is_err() {
                    first_error.get_or_insert_with(|| io::Error::other("logging worker panicked"));
                }
            } else {
                match result_receiver.recv_timeout(SHUTDOWN_TIMEOUT) {
                    Ok(result) => {
                        if worker.join().is_err() {
                            first_error
                                .get_or_insert_with(|| io::Error::other("logging worker panicked"));
                        }
                        if let Err(error) = result {
                            first_error.get_or_insert(error);
                        }
                    }
                    Err(_) => {
                        // Dropping a JoinHandle detaches the stuck worker. Shutdown
                        // must not hang forever if the log filesystem is unavailable.
                        drop(worker);
                        first_error.get_or_insert_with(|| {
                            io::Error::new(
                                io::ErrorKind::TimedOut,
                                "logging worker did not drain within five seconds",
                            )
                        });
                    }
                }
            }
        }
        if let Some(compressor) = self.compressor.as_mut() {
            if let Err(error) = compressor.shutdown() {
                first_error.get_or_insert(error);
            }
        }
        self.compressor = None;
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            eprintln!("data-plane logging shutdown failed: {error}");
        }
    }
}

fn run_worker(
    mut state: RollingState,
    receiver: Receiver<WorkerMessage>,
    dropped: Arc<AtomicU64>,
    flush_interval: Duration,
) {
    let mut next_flush = Instant::now() + flush_interval;
    let mut dirty = false;
    let mut pending_error: Option<String> = None;
    loop {
        let timeout = next_flush.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(timeout) {
            Ok(WorkerMessage::Record(record)) => {
                if let Err(error) = state.write_record(&record) {
                    pending_error.get_or_insert_with(|| error.to_string());
                } else {
                    dirty = true;
                }
            }
            Ok(WorkerMessage::Shutdown(result_sender)) => {
                while let Ok(message) = receiver.try_recv() {
                    if let WorkerMessage::Record(record) = message {
                        if let Err(error) = state.write_record(&record) {
                            pending_error.get_or_insert_with(|| error.to_string());
                        }
                    }
                }
                let result = state.flush();
                report_worker_health(&state.path, &dropped, &mut pending_error);
                let _ = result_sender.send(result);
                break;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                let _ = state.flush();
                report_worker_health(&state.path, &dropped, &mut pending_error);
                break;
            }
        }

        if Instant::now() >= next_flush {
            if dirty {
                if let Err(error) = state.flush() {
                    pending_error.get_or_insert_with(|| error.to_string());
                }
                dirty = false;
            }
            report_worker_health(&state.path, &dropped, &mut pending_error);
            next_flush = Instant::now() + flush_interval;
        }
    }
}

fn report_worker_health(path: &Path, dropped: &AtomicU64, pending_error: &mut Option<String>) {
    let dropped = dropped.swap(0, Ordering::Relaxed);
    if dropped != 0 {
        eprintln!(
            "data-plane logging queue for {} dropped {dropped} records",
            path.display()
        );
    }
    if let Some(error) = pending_error.take() {
        eprintln!(
            "data-plane logging write failed for {}: {error}",
            path.display()
        );
    }
}

impl RollingState {
    fn new(
        path: PathBuf,
        max_size: u64,
        max_files: usize,
        compression: Option<Sender<CompressionMessage>>,
    ) -> io::Result<Self> {
        let (writer, size) = open_append(&path)?;
        Ok(Self {
            path,
            writer,
            size,
            max_size,
            max_files,
            compression,
            rotation_sequence: 0,
        })
    }

    fn write_record(&mut self, buffer: &[u8]) -> io::Result<()> {
        if self.size != 0 && self.size.saturating_add(buffer.len() as u64) > self.max_size {
            self.rotate()?;
        }
        self.writer.write_all(buffer)?;
        self.size = self.size.saturating_add(buffer.len() as u64);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    fn rotate(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        if let Some(compression) = self.compression.clone() {
            let staging = self.next_staging_path();
            fs::rename(&self.path, &staging)?;
            let (writer, size) = open_append(&self.path)?;
            self.writer = writer;
            self.size = size;
            compression
                .send(CompressionMessage::Compress(staging))
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "log compression worker stopped")
                })?;
            return Ok(());
        } else {
            let discard = rotated_path(&self.path, self.max_files);
            remove_if_present(&discard)?;
            for index in (1..self.max_files).rev() {
                let source = rotated_path(&self.path, index);
                let destination = rotated_path(&self.path, index + 1);
                rename_if_present(&source, &destination)?;
            }
            rename_if_present(&self.path, &rotated_path(&self.path, 1))?;
        }
        let (writer, size) = open_append(&self.path)?;
        self.writer = writer;
        self.size = size;
        Ok(())
    }

    fn next_staging_path(&mut self) -> PathBuf {
        loop {
            self.rotation_sequence = self.rotation_sequence.wrapping_add(1);
            let candidate = PathBuf::from(format!(
                "{}.rotate.{}.{}",
                self.path.display(),
                std::process::id(),
                self.rotation_sequence
            ));
            if !candidate.exists() {
                return candidate;
            }
        }
    }
}

impl CompressionGuard {
    fn new(path: PathBuf, max_files: usize) -> io::Result<(Sender<CompressionMessage>, Self)> {
        let (sender, receiver) = mpsc::channel();
        let worker_sender = sender.clone();
        let recovery = recovery_rotation_paths(&path)?;
        let thread_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("data-plane")
            .to_owned();
        let thread = thread::Builder::new()
            .name(format!("log-compress-{thread_name}"))
            .spawn(move || run_compressor(path, max_files, receiver))?;
        for staging in recovery {
            sender
                .send(CompressionMessage::Compress(staging))
                .map_err(|_| io::Error::other("log compression worker stopped during recovery"))?;
        }
        Ok((
            sender.clone(),
            Self {
                sender: worker_sender,
                thread: Some(thread),
            },
        ))
    }

    fn shutdown(&mut self) -> io::Result<()> {
        let Some(worker) = self.thread.take() else {
            return Ok(());
        };
        let (result_sender, result_receiver) = mpsc::channel();
        if self
            .sender
            .send(CompressionMessage::Shutdown(result_sender))
            .is_err()
        {
            return worker
                .join()
                .map_err(|_| io::Error::other("log compression worker panicked"));
        }
        match result_receiver.recv_timeout(SHUTDOWN_TIMEOUT) {
            Ok(result) => {
                worker
                    .join()
                    .map_err(|_| io::Error::other("log compression worker panicked"))?;
                result
            }
            Err(_) => {
                drop(worker);
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "log compression worker did not drain within five seconds",
                ))
            }
        }
    }
}

fn recovery_rotation_paths(path: &Path) -> io::Result<Vec<PathBuf>> {
    let Some(directory) = path.parent() else {
        return Ok(Vec::new());
    };
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(Vec::new());
    };
    let prefix = format!("{file_name}.");
    let mut plain_rotations = Vec::new();
    let mut staging_rotations = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(suffix) = name.strip_prefix(&prefix) else {
            continue;
        };
        if let Ok(index) = suffix.parse::<usize>() {
            plain_rotations.push((index, entry.path()));
        } else if suffix.starts_with("rotate.") {
            let modified = entry.metadata()?.modified().ok();
            staging_rotations.push((modified, entry.path()));
        }
    }
    // Legacy .N files use larger indexes for older data. Feed those first so
    // the compressor's .1.gz insertion preserves newest-first ordering.
    plain_rotations.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    staging_rotations.sort_by_key(|entry| entry.0);
    Ok(plain_rotations
        .into_iter()
        .map(|(_, path)| path)
        .chain(staging_rotations.into_iter().map(|(_, path)| path))
        .collect())
}

impl Drop for CompressionGuard {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            eprintln!("data-plane log compression shutdown failed: {error}");
        }
    }
}

fn run_compressor(path: PathBuf, max_files: usize, receiver: Receiver<CompressionMessage>) {
    let mut pending_error: Option<String> = None;
    let mut sequence = 0_u64;
    while let Ok(message) = receiver.recv() {
        match message {
            CompressionMessage::Compress(staging) => {
                sequence = sequence.wrapping_add(1);
                if let Err(error) = compress_rotation(&path, &staging, max_files, sequence) {
                    eprintln!(
                        "data-plane log compression failed for {}: {error}; retaining {}",
                        path.display(),
                        staging.display()
                    );
                    pending_error.get_or_insert_with(|| error.to_string());
                }
            }
            CompressionMessage::Shutdown(result_sender) => {
                while let Ok(CompressionMessage::Compress(staging)) = receiver.try_recv() {
                    sequence = sequence.wrapping_add(1);
                    if let Err(error) = compress_rotation(&path, &staging, max_files, sequence) {
                        eprintln!(
                            "data-plane log compression failed for {}: {error}; retaining {}",
                            path.display(),
                            staging.display()
                        );
                        pending_error.get_or_insert_with(|| error.to_string());
                    }
                }
                let result = pending_error.take().map_or(Ok(()), |error| {
                    Err(io::Error::other(format!("log compression failed: {error}")))
                });
                let _ = result_sender.send(result);
                break;
            }
        }
    }
}

fn compress_rotation(
    path: &Path,
    staging: &Path,
    max_files: usize,
    sequence: u64,
) -> io::Result<()> {
    let input = File::open(staging)?;
    let temporary = PathBuf::from(format!(
        "{}.1.gz.tmp.{}.{}",
        path.display(),
        std::process::id(),
        sequence
    ));
    remove_if_present(&temporary)?;
    let result = (|| {
        let output = open_new_file(&temporary)?;
        let mut encoder = GzEncoder::new(BufWriter::new(output), Compression::fast());
        io::copy(&mut BufReader::new(input), &mut encoder)?;
        let mut output = encoder.finish()?;
        output.flush()?;

        remove_if_present(&compressed_rotated_path(path, max_files))?;
        for index in (1..max_files).rev() {
            rename_if_present(
                &compressed_rotated_path(path, index),
                &compressed_rotated_path(path, index + 1),
            )?;
        }
        fs::rename(&temporary, compressed_rotated_path(path, 1))?;
        fs::remove_file(staging)
    })();
    if result.is_err() {
        let _ = remove_if_present(&temporary);
    }
    result
}

fn open_append(path: &Path) -> io::Result<(BufWriter<File>, u64)> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o640);
    }
    let file = options.open(path)?;
    let size = file.metadata()?.len();
    Ok((BufWriter::new(file), size))
}

fn open_new_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o640);
    }
    options.open(path)
}

fn rotated_path(path: &Path, index: usize) -> PathBuf {
    PathBuf::from(format!("{}.{}", path.display(), index))
}

fn compressed_rotated_path(path: &Path, index: usize) -> PathBuf {
    PathBuf::from(format!("{}.{}.gz", path.display(), index))
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn rename_if_present(source: &Path, destination: &Path) -> io::Result<()> {
    match fs::rename(source, destination) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;

    #[test]
    fn size_rotation_keeps_a_bounded_number_of_files() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node-proxy.log");
        let (writer, mut guard) = AsyncRollingWriter::new(
            path.clone(),
            32,
            2,
            32,
            Duration::from_secs(60),
            LogCompression::None,
        )
        .unwrap();
        for value in *b"abcd" {
            let mut event = writer.make_writer();
            event.write_all(&[value; 24]).unwrap();
        }
        guard.shutdown().unwrap();
        assert!(path.exists());
        assert!(rotated_path(&path, 1).exists());
        assert!(rotated_path(&path, 2).exists());
        assert!(!rotated_path(&path, 3).exists());
        assert_eq!(fs::read(&path).unwrap(), vec![b'd'; 24]);
        assert_eq!(fs::read(rotated_path(&path, 1)).unwrap(), vec![b'c'; 24]);
        assert_eq!(fs::read(rotated_path(&path, 2)).unwrap(), vec![b'b'; 24]);
    }

    #[test]
    fn gzip_rotation_compresses_in_background_and_keeps_a_bounded_history() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("edge-frontend-access.log");
        let (writer, mut guard) = AsyncRollingWriter::new(
            path.clone(),
            32,
            2,
            32,
            Duration::from_secs(60),
            LogCompression::Gzip,
        )
        .unwrap();
        for value in *b"abcd" {
            let mut event = writer.make_writer();
            event.write_all(&[value; 24]).unwrap();
        }
        guard.shutdown().unwrap();

        assert_eq!(fs::read(&path).unwrap(), vec![b'd'; 24]);
        assert_eq!(read_gzip(compressed_rotated_path(&path, 1)), vec![b'c'; 24]);
        assert_eq!(read_gzip(compressed_rotated_path(&path, 2)), vec![b'b'; 24]);
        assert!(!compressed_rotated_path(&path, 3).exists());
        assert!(fs::read_dir(directory.path()).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".rotate.")));
    }

    #[test]
    fn gzip_startup_migrates_legacy_plain_rotations() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("edge-frontend.log");
        fs::write(rotated_path(&path, 1), b"newer").unwrap();
        fs::write(rotated_path(&path, 2), b"older").unwrap();

        let (_, mut guard) = AsyncRollingWriter::new(
            path.clone(),
            1024,
            2,
            32,
            Duration::from_secs(60),
            LogCompression::Gzip,
        )
        .unwrap();
        guard.shutdown().unwrap();

        assert_eq!(read_gzip(compressed_rotated_path(&path, 1)), b"newer");
        assert_eq!(read_gzip(compressed_rotated_path(&path, 2)), b"older");
        assert!(!rotated_path(&path, 1).exists());
        assert!(!rotated_path(&path, 2).exists());
    }

    #[test]
    fn gzip_failure_retains_the_uncompressed_staging_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node-proxy.log");
        let staging = directory.path().join("node-proxy.log.rotate.test");
        fs::write(&staging, b"must-not-be-lost").unwrap();
        fs::create_dir(compressed_rotated_path(&path, 2)).unwrap();

        assert!(compress_rotation(&path, &staging, 2, 1).is_err());
        assert_eq!(fs::read(&staging).unwrap(), b"must-not-be-lost");
        assert!(!PathBuf::from(format!(
            "{}.1.gz.tmp.{}.1",
            path.display(),
            std::process::id()
        ))
        .exists());
    }

    #[test]
    fn shutdown_drains_queued_records() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("edge-frontend-access.log");
        let (writer, mut guard) = AsyncRollingWriter::new(
            path.clone(),
            1024 * 1024,
            2,
            128,
            Duration::from_secs(60),
            LogCompression::None,
        )
        .unwrap();
        for index in 0..100 {
            let mut event = writer.make_writer();
            writeln!(event, "request-{index}").unwrap();
        }
        guard.shutdown().unwrap();
        let contents = fs::read_to_string(path).unwrap();
        assert!(contents.contains("request-0\n"));
        assert!(contents.contains("request-99\n"));
        assert_eq!(contents.lines().count(), 100);
    }

    #[test]
    fn worker_flushes_records_on_the_configured_interval() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("edge-frontend-access.log");
        let (writer, mut guard) = AsyncRollingWriter::new(
            path.clone(),
            1024 * 1024,
            2,
            128,
            Duration::from_millis(20),
            LogCompression::None,
        )
        .unwrap();
        let mut event = writer.make_writer();
        event.write_all(b"periodic-flush\n").unwrap();
        drop(event);

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if fs::read(&path).unwrap() == b"periodic-flush\n" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "record was not flushed by the background worker"
            );
            thread::sleep(Duration::from_millis(10));
        }
        guard.shutdown().unwrap();
    }

    #[test]
    fn full_queue_drops_records_without_blocking() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let dropped = Arc::new(AtomicU64::new(0));
        sender
            .try_send(WorkerMessage::Record(b"occupied".to_vec()))
            .unwrap();
        let mut event = EventWriter {
            sender,
            dropped: dropped.clone(),
            buffer: Vec::new(),
        };
        event.write_all(b"new-record").unwrap();
        drop(event);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        drop(receiver);
    }

    #[test]
    fn access_and_audit_log_filters_are_independent() {
        assert!(!include_general_log_target(
            true,
            false,
            false,
            true,
            "adx_access"
        ));
        assert!(!include_general_log_target(
            true,
            false,
            false,
            true,
            "adx_audit"
        ));
        assert!(include_general_log_target(
            true,
            true,
            false,
            true,
            "adx_audit"
        ));
        assert!(!include_general_log_target(
            true,
            true,
            false,
            true,
            "adx_access"
        ));
        assert!(include_general_log_target(
            true,
            false,
            false,
            true,
            "adx_edge_frontend"
        ));
        assert!(include_general_log_target(
            false,
            false,
            false,
            false,
            "adx_access"
        ));
    }

    fn read_gzip(path: PathBuf) -> Vec<u8> {
        let mut decoded = Vec::new();
        GzDecoder::new(File::open(path).unwrap())
            .read_to_end(&mut decoded)
            .unwrap();
        decoded
    }
}
