//! Record-boundary size rotation for process log files.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::retention::RetentionHandle;

/// A `Write` implementation whose `flush()` marks the end of one log record.
///
/// Formatters may call `write()` several times for one record, so rotation is
/// intentionally checked only after the formatter flushes the complete line.
pub struct RotatingFileWriter {
    path: PathBuf,
    file: File,
    written_bytes: u64,
    max_file_size: u64,
    rotation_sequence: u64,
    retention: RetentionHandle,
}

impl RotatingFileWriter {
    pub(crate) fn open(
        path: PathBuf,
        max_file_size: u64,
        retention: RetentionHandle,
    ) -> io::Result<Self> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        let file = open_append(&path)?;
        let written_bytes = file.metadata()?.len();
        Ok(Self {
            path,
            file,
            written_bytes,
            max_file_size,
            rotation_sequence: 0,
            retention,
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        let archive = archive_path(&self.path, self.rotation_sequence)?;
        self.rotation_sequence = self.rotation_sequence.saturating_add(1);
        fs::rename(&self.path, &archive)?;
        match open_append(&self.path) {
            Ok(file) => self.file = file,
            Err(open_error) => {
                // Keep the canonical path recoverable when creating the next
                // active file fails. The already-open descriptor still points
                // at `archive`, so a failed rollback remains writable and the
                // FallbackDrain reports the operational error to stderr.
                if let Err(rollback_error) = fs::rename(&archive, &self.path) {
                    return Err(io::Error::other(format!(
                        "failed to open new log file: {open_error}; rollback failed: {rollback_error}"
                    )));
                }
                return Err(open_error);
            }
        }
        self.written_bytes = 0;
        self.retention.request_cleanup();
        Ok(())
    }
}

impl Write for RotatingFileWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.written_bytes = self.written_bytes.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        if self.written_bytes >= self.max_file_size {
            self.rotate()?;
        }
        Ok(())
    }
}

fn open_append(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn archive_path(path: &Path, sequence: u64) -> io::Result<PathBuf> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "log path has no file name"))?;
    Ok(path.with_file_name(format!(
        "{name}.{}.{:09}.{sequence}",
        elapsed.as_secs(),
        elapsed.subsec_nanos()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retention::{RetentionPolicy, RetentionWorker};
    use std::time::Duration;

    #[test]
    fn rotation_happens_only_on_flush() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("afs.log");
        let (mut worker, handle) = RetentionWorker::spawn(
            path.clone(),
            RetentionPolicy {
                max_backups: 10,
                max_age: Duration::from_secs(60),
            },
        )
        .expect("retention");
        let mut writer = RotatingFileWriter::open(path.clone(), 5, handle).expect("writer");

        writer.write_all(b"abc").expect("part one");
        writer.write_all(b"def\n").expect("part two");
        assert_eq!(fs::read(&path).expect("current before flush"), b"abcdef\n");
        writer.flush().expect("flush and rotate");
        assert!(fs::read(&path).expect("new current").is_empty());
        let archives = fs::read_dir(directory.path())
            .expect("read dir")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("afs.log."))
            .collect::<Vec<_>>();
        assert_eq!(archives.len(), 1);
        assert_eq!(fs::read(archives[0].path()).expect("archive"), b"abcdef\n");
        drop(writer);
        worker.shutdown();
    }

    #[test]
    fn existing_file_size_is_resumed_after_restart() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("afs.log");
        fs::write(&path, b"1234").expect("seed");
        let (mut worker, handle) = RetentionWorker::spawn(
            path.clone(),
            RetentionPolicy {
                max_backups: 10,
                max_age: Duration::from_secs(60),
            },
        )
        .expect("retention");
        let mut writer = RotatingFileWriter::open(path.clone(), 5, handle).expect("writer");
        writer.write_all(b"5\n").expect("append");
        writer.flush().expect("rotate");
        assert!(fs::read(path).expect("current").is_empty());
        drop(writer);
        worker.shutdown();
    }
}
