//! Assembly of formatter, dynamic filter, async queue and global logger.

use std::{
    io::{self, BufWriter, Write},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use slog::{Drain, Never, OwnedKVList, Record};

use crate::{
    LogFormat, LogOutput, LoggingConfig, LoggingError, OverflowPolicy, ProcessIdentity,
    fallback::FallbackDrain,
    file_writer::RotatingFileWriter,
    retention::{RetentionPolicy, RetentionWorker},
};

enum OutputWriter {
    Stderr(io::Stderr),
    File(RotatingFileWriter),
}

impl Write for OutputWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Stderr(stderr) => stderr.write(bytes),
            Self::File(file) => file.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Stderr(stderr) => stderr.flush(),
            Self::File(file) => file.flush(),
        }
    }
}

/// Cheap, cloneable control handle used by the future online-config endpoint.
#[derive(Clone, Debug)]
pub struct LevelController {
    level: Arc<AtomicU8>,
}

impl LevelController {
    /// Create the cheap handle shared by the config owner and logging filter.
    #[must_use]
    pub fn new(level: slog::Level) -> Self {
        Self {
            level: Arc::new(AtomicU8::new(level_to_u8(level))),
        }
    }

    pub fn set(&self, level: slog::Level) {
        self.level.store(level_to_u8(level), Ordering::Relaxed);
        log::set_max_level(to_log_level_filter(level));
    }

    #[must_use]
    pub fn get(&self) -> slog::Level {
        u8_to_level(self.level.load(Ordering::Relaxed))
    }
}

struct DynamicLevelFilter<D> {
    inner: D,
    controller: LevelController,
}

impl<D> Drain for DynamicLevelFilter<D>
where
    D: Drain<Ok = (), Err = Never>,
{
    type Ok = ();
    type Err = Never;

    fn log(&self, record: &Record<'_>, values: &OwnedKVList) -> Result<(), Never> {
        if record.level().is_at_least(self.controller.get()) {
            self.inner.log(record, values)
        } else {
            Ok(())
        }
    }
}

/// Owns the asynchronous logger and retention worker until process shutdown.
pub struct LoggingGuard {
    async_guard: Option<slog_async::AsyncGuard>,
    retention: Option<RetentionWorker>,
    level: LevelController,
}

impl LoggingGuard {
    #[must_use]
    pub fn level_controller(&self) -> LevelController {
        self.level.clone()
    }

    pub fn set_level(&self, level: slog::Level) {
        self.level.set(level);
    }
}

impl Drop for LoggingGuard {
    fn drop(&mut self) {
        // Remove the global sender before waiting for slog-async to drain.
        slog_global::clear_global();
        drop(self.async_guard.take());
        if let Some(mut retention) = self.retention.take() {
            retention.shutdown();
        }
    }
}

/// Initialize the one process-global logger for a service process.
pub fn init_process_logging(
    config: &LoggingConfig,
    identity: ProcessIdentity,
) -> Result<LoggingGuard, LoggingError> {
    config.validate()?;
    let (writer, mut retention) = output_writer(config)?;
    let writer = BufWriter::new(writer);

    let formatted: Box<dyn Drain<Ok = (), Err = Never> + Send> = match config.format {
        LogFormat::Json => Box::new(FallbackDrain::new(
            // `Json::new` intentionally starts with no implicit fields.  Add
            // slog-json's stable default keys so every record carries the
            // timestamp, level and human-readable message expected by Alloy,
            // Loki and operators.
            slog_json::Json::new(writer)
                .add_default_keys()
                .set_flush(true)
                .build(),
        )),
        LogFormat::Text => {
            let decorator = slog_term::PlainDecorator::new(writer);
            Box::new(FallbackDrain::new(
                slog_term::FullFormat::new(decorator).build(),
            ))
        }
    };
    let (async_drain, async_guard) = slog_async::Async::new(formatted)
        .chan_size(config.async_queue_capacity)
        .overflow_strategy(match config.overflow {
            OverflowPolicy::DropAndReport => slog_async::OverflowStrategy::DropAndReport,
            OverflowPolicy::Block => slog_async::OverflowStrategy::Block,
        })
        .thread_name("afs-log-writer".to_string())
        .build_with_guard();
    let level = LevelController::new(config.level);
    let filtered = DynamicLevelFilter {
        // With DropAndReport/Block and a downstream FallbackDrain, the async
        // drain has no recoverable error left for the business caller. Fuse
        // narrows its type to the infallible root-drain contract.
        inner: async_drain.fuse(),
        controller: level.clone(),
    };
    let logger = slog::Logger::root(
        filtered,
        slog::o!(
            "service_name" => identity.service_name,
            "instance" => identity.instance,
        ),
    );
    slog_global::set_global(logger);
    if slog_global::redirect_std_log(Some(config.level)).is_err() {
        // `set_global` happened first so records emitted while installing the
        // `log` facade have a valid sink. If the facade is already owned by the
        // embedding process, unwind every resource created by this initializer
        // instead of leaving a global sender pointing at a stopped async drain.
        slog_global::clear_global();
        drop(async_guard);
        if let Some(worker) = retention.as_mut() {
            worker.shutdown();
        }
        return Err(LoggingError::Redirect);
    }

    Ok(LoggingGuard {
        async_guard: Some(async_guard),
        retention,
        level,
    })
}

fn output_writer(
    config: &LoggingConfig,
) -> Result<(OutputWriter, Option<RetentionWorker>), LoggingError> {
    match &config.output {
        LogOutput::Stderr => Ok((OutputWriter::Stderr(io::stderr()), None)),
        LogOutput::File(path) => {
            let policy = RetentionPolicy {
                max_backups: config.max_backups,
                max_age: config.max_age,
            };
            let (worker, handle) = RetentionWorker::spawn(path.clone(), policy)?;
            let writer = RotatingFileWriter::open(path.clone(), config.max_file_size, handle)?;
            Ok((OutputWriter::File(writer), Some(worker)))
        }
    }
}

const fn level_to_u8(level: slog::Level) -> u8 {
    match level {
        slog::Level::Critical => 1,
        slog::Level::Error => 2,
        slog::Level::Warning => 3,
        slog::Level::Info => 4,
        slog::Level::Debug => 5,
        slog::Level::Trace => 6,
    }
}

const fn u8_to_level(level: u8) -> slog::Level {
    match level {
        1 => slog::Level::Critical,
        2 => slog::Level::Error,
        3 => slog::Level::Warning,
        4 => slog::Level::Info,
        5 => slog::Level::Debug,
        _ => slog::Level::Trace,
    }
}

const fn to_log_level_filter(level: slog::Level) -> log::LevelFilter {
    match level {
        slog::Level::Critical | slog::Level::Error => log::LevelFilter::Error,
        slog::Level::Warning => log::LevelFilter::Warn,
        slog::Level::Info => log::LevelFilter::Info,
        slog::Level::Debug => log::LevelFilter::Debug,
        slog::Level::Trace => log::LevelFilter::Trace,
    }
}

/// 先读标准 facade 的原子级别门禁，避免禁用日志捕获上下文或求值字段。
#[doc(hidden)]
pub fn level_enabled(level: slog::Level) -> bool {
    to_log_level_filter(level) <= log::max_level()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_controller_changes_without_rebuilding_writer() {
        let controller = LevelController::new(slog::Level::Info);
        assert_eq!(controller.get(), slog::Level::Info);
        controller.set(slog::Level::Debug);
        assert_eq!(controller.get(), slog::Level::Debug);
    }

    #[test]
    fn process_logger_writes_machine_readable_identity_and_event() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("afs.log");
        let guard = init_process_logging(
            &LoggingConfig {
                output: LogOutput::File(path.clone()),
                max_file_size: 1024 * 1024,
                ..LoggingConfig::default()
            },
            ProcessIdentity::new("afs-test", "node-a"),
        )
        .expect("logger");

        crate::info!(
            "ready";
            "event" => "test.ready",
            "answer" => 42,
        );
        drop(guard);

        let record = std::fs::read_to_string(path).expect("read log");
        assert!(record.contains("\"service_name\":\"afs-test\""));
        assert!(record.contains("\"instance\":\"node-a\""));
        assert!(record.contains("\"event\":\"test.ready\""));
        assert!(record.contains("\"answer\":42"));
        assert!(record.contains("\"level\":\"INFO\""));
        assert!(record.contains("\"msg\":\"ready\""));
        assert!(record.contains("\"ts\":"));
    }
}
