//! Non-recursive fallback for failures inside the logging pipeline itself.

use std::fmt;

use slog::{Drain, Never, OwnedKVList, Record};

/// Converts formatter/writer failures into a direct stderr diagnostic.
///
/// A root slog drain cannot return errors. Using `Drain::fuse()` would panic;
/// using `ignore_res()` would silently lose the reason. This adapter reports
/// the failure without trying to log through the broken pipeline again.
pub(crate) struct FallbackDrain<D> {
    inner: D,
}

impl<D> FallbackDrain<D> {
    pub(crate) const fn new(inner: D) -> Self {
        Self { inner }
    }
}

impl<D> Drain for FallbackDrain<D>
where
    D: Drain<Ok = ()>,
    D::Err: fmt::Display,
{
    type Ok = ();
    type Err = Never;

    fn log(&self, record: &Record<'_>, values: &OwnedKVList) -> Result<(), Never> {
        if let Err(error) = self.inner.log(record, values) {
            fallback(&format!("log writer failure: {error}"));
        }
        Ok(())
    }
}

pub(crate) fn fallback(message: &str) {
    eprintln!("afs-logging fallback: {message}");
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingDrain;

    impl Drain for FailingDrain {
        type Ok = ();
        type Err = &'static str;

        fn log(&self, _record: &Record<'_>, _values: &OwnedKVList) -> Result<(), Self::Err> {
            Err("injected writer failure")
        }
    }

    #[test]
    fn downstream_failure_is_reported_without_panicking_or_recursing() {
        // The root Logger requires an infallible Drain. FallbackDrain turns a
        // formatter/writer failure into a direct stderr diagnostic and returns
        // success only to the logging framework; it does not change the result
        // of the business operation that emitted this record.
        let logger = slog::Logger::root(FallbackDrain::new(FailingDrain), slog::o!());
        slog::warn!(logger, "exercise fallback"; "event" => "logging.fallback.test");
    }
}
