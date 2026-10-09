//! Process-level logging infrastructure for long-running AFS binaries.
//!
//! Worker and Master processes initialize this crate once. The small macros below
//! only inject the process-global logger into upstream `slog`; formatting,
//! queuing and output remain owned by the standard crates.
//!
//! Libraries embedded in user applications must not install this global logger.

#![forbid(unsafe_code)]

mod config;
mod fallback;
mod file_writer;
mod init;
mod retention;

pub use config::{
    LogFormat, LogOutput, LoggingConfig, LoggingError, OverflowPolicy, ProcessIdentity, parse_level,
};
pub use file_writer::RotatingFileWriter;
#[doc(hidden)]
pub use init::level_enabled;
pub use init::{LevelController, LoggingGuard, init_process_logging};

#[doc(hidden)]
pub use slog;
#[doc(hidden)]
pub use slog_global;

/// Clones the process logger and, when a sampled Span is active, attaches its
/// stable correlation keys to this one log record.
///
/// The logger clone is a cheap shared handle. No sampled Span means no added
/// fields, so disabled tracing does not change business behavior or log shape.
#[doc(hidden)]
pub fn correlated_logger() -> slog::Logger {
    let logger = (**slog_global::borrow_global()).clone();
    match afs_tracing::current_correlation() {
        Some(correlation) => logger.new(slog::o!(
            "trace_id" => correlation.trace_id,
            "span_id" => correlation.span_id,
        )),
        None => logger,
    }
}

/// Log a debug process event through the initialized global logger.
///
/// Debug records are useful for per-request correlation during a focused
/// investigation. Production can keep the configured level at `info` without
/// paying the formatting and I/O cost for these records.
#[macro_export]
macro_rules! debug {
    ($($argument:tt)+) => {{
        if $crate::level_enabled($crate::slog::Level::Debug) {
            let logger = $crate::correlated_logger();
            $crate::slog::debug!(logger, $($argument)+)
        }
    }};
}

/// Log an informational process event through the initialized global logger.
#[macro_export]
macro_rules! info {
    ($($argument:tt)+) => {{
        if $crate::level_enabled($crate::slog::Level::Info) {
            let logger = $crate::correlated_logger();
            $crate::slog::info!(logger, $($argument)+)
        }
    }};
}

/// Log a warning process event through the initialized global logger.
#[macro_export]
macro_rules! warn {
    ($($argument:tt)+) => {{
        if $crate::level_enabled($crate::slog::Level::Warning) {
            let logger = $crate::correlated_logger();
            $crate::slog::warn!(logger, $($argument)+)
        }
    }};
}

/// Log an error process event through the initialized global logger.
#[macro_export]
macro_rules! error {
    ($($argument:tt)+) => {{
        if $crate::level_enabled($crate::slog::Level::Error) {
            let logger = $crate::correlated_logger();
            $crate::slog::error!(logger, $($argument)+)
        }
    }};
}
