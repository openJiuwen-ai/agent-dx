//! Asynchronous cleanup of rotated log files.

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::mpsc::{self, SyncSender, TrySendError},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime},
};

use crate::fallback::fallback;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RetentionPolicy {
    pub(crate) max_backups: usize,
    pub(crate) max_age: Duration,
}

enum Command {
    Cleanup,
    Stop,
}

#[derive(Clone)]
pub(crate) struct RetentionHandle {
    sender: SyncSender<Command>,
}

impl RetentionHandle {
    pub(crate) fn request_cleanup(&self) {
        match self.sender.try_send(Command::Cleanup) {
            Ok(()) | Err(TrySendError::Full(Command::Cleanup)) => {}
            Err(TrySendError::Disconnected(Command::Cleanup)) => {
                fallback("retention worker is unavailable");
            }
            Err(TrySendError::Full(Command::Stop) | TrySendError::Disconnected(Command::Stop)) => {
                unreachable!("request_cleanup only sends Cleanup")
            }
        }
    }
}

pub(crate) struct RetentionWorker {
    sender: SyncSender<Command>,
    join: Option<JoinHandle<()>>,
}

impl RetentionWorker {
    pub(crate) fn spawn(
        path: PathBuf,
        policy: RetentionPolicy,
    ) -> io::Result<(Self, RetentionHandle)> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let worker_sender = sender.clone();
        let join = thread::Builder::new()
            .name("afs-log-retention".to_string())
            .spawn(move || {
                while let Ok(command) = receiver.recv() {
                    match command {
                        Command::Cleanup => {
                            if let Err(error) = cleanup(&path, policy, SystemTime::now()) {
                                fallback(&format!("retention cleanup failed: {error}"));
                            }
                        }
                        Command::Stop => break,
                    }
                }
            })?;
        let worker = Self {
            sender: worker_sender,
            join: Some(join),
        };
        let handle = RetentionHandle { sender };
        handle.request_cleanup();
        Ok((worker, handle))
    }

    pub(crate) fn shutdown(&mut self) {
        let _ = self.sender.send(Command::Stop);
        if let Some(join) = self.join.take()
            && join.join().is_err()
        {
            fallback("retention worker panicked during shutdown");
        }
    }
}

impl Drop for RetentionWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn cleanup(path: &Path, policy: RetentionPolicy, now: SystemTime) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "log path has no file name"))?;
    let prefix = format!("{file_name}.");
    let mut history = fs::read_dir(parent)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&prefix))
        })
        .filter_map(|entry| {
            let modified = entry.metadata().ok()?.modified().ok()?;
            Some((entry.path(), modified))
        })
        .collect::<Vec<_>>();
    history.sort_by_key(|entry| std::cmp::Reverse(entry.1));

    for (index, (candidate, modified)) in history.into_iter().enumerate() {
        let over_count = index >= policy.max_backups;
        let over_age = now
            .duration_since(modified)
            .is_ok_and(|age| age >= policy.max_age);
        if over_count || over_age {
            fs::remove_file(candidate)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::FileTimes, io::Write};

    #[test]
    fn cleanup_applies_count_limit() {
        let directory = tempfile::tempdir().expect("tempdir");
        let current = directory.path().join("afs.log");
        fs::write(&current, b"current").expect("current");
        for index in 0..4 {
            let path = directory.path().join(format!("afs.log.{index}"));
            let mut file = fs::File::create(path).expect("history");
            writeln!(file, "{index}").expect("write");
            std::thread::sleep(Duration::from_millis(2));
        }

        cleanup(
            &current,
            RetentionPolicy {
                max_backups: 2,
                max_age: Duration::from_secs(60),
            },
            SystemTime::now(),
        )
        .expect("cleanup");

        let remaining = fs::read_dir(directory.path())
            .expect("read dir")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("afs.log."))
            .count();
        assert_eq!(remaining, 2);
    }

    #[test]
    fn cleanup_applies_age_limit_independently_of_count() {
        let directory = tempfile::tempdir().expect("tempdir");
        let current = directory.path().join("afs.log");
        let archive = directory.path().join("afs.log.old");
        fs::write(&current, b"current").expect("current");
        let file = fs::File::create(&archive).expect("archive");
        let old = SystemTime::now() - Duration::from_secs(120);
        file.set_times(FileTimes::new().set_modified(old))
            .expect("set old mtime");

        cleanup(
            &current,
            RetentionPolicy {
                max_backups: 10,
                max_age: Duration::from_secs(60),
            },
            SystemTime::now(),
        )
        .expect("cleanup");

        assert!(!archive.exists());
        assert!(current.exists());
    }
}
