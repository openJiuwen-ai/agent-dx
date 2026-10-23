//! Open a new handoff without relying on a restored remote-task notification.
use super::control::{CheckpointHooks, Handoff};
use std::{io, os::unix::net::UnixStream, sync::mpsc, sync::Arc};
use tokio::io::AsyncReadExt;

/// The completion socket is fresh for each prepare and closes before Prepared.
/// Do not use it for the handoff read, whose waiter must survive checkpoint.
pub(super) async fn open(
    hooks: Arc<dyn CheckpointHooks>,
    operation: String,
) -> io::Result<Handoff> {
    let (notify, completion) = UnixStream::pair()?;
    completion.set_nonblocking(true)?;
    let mut completion = tokio::net::UnixStream::from_std(completion)?;
    let (send, receive) = mpsc::sync_channel(1);
    tokio::task::spawn_blocking(move || {
        execd_info!(
            "[checkpoint-control] handoff_open_started operation={}",
            operation
        );
        let opened = hooks.open();
        execd_info!(
            "[checkpoint-control] handoff_open_completed operation={} success={}",
            operation,
            opened.is_ok()
        );
        // EOF notifies the I/O waiter after the result is available. Both ends
        // close before Prepared, including when the caller has disconnected.
        let _ = send.send(opened);
        drop(notify);
    });
    let mut byte = [0];
    if completion.read(&mut byte).await? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected checkpoint open notification",
        ));
    }
    receive.try_recv().map_err(|error| {
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("checkpoint open result unavailable: {error}"),
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::super::control::HandoffOutcome;
    use super::*;
    use adx_core::runtime::RuntimeIdentity;
    use std::{sync::Mutex, time::Duration};

    struct Hooks<F>(Mutex<Option<F>>);
    impl<F: FnOnce() -> io::Result<Handoff> + Send + 'static> CheckpointHooks for Hooks<F> {
        fn open(&self) -> io::Result<Handoff> {
            self.0.lock().unwrap().take().unwrap()()
        }
        fn restore(&self, _: &RuntimeIdentity) -> io::Result<RuntimeIdentity> {
            unreachable!()
        }
    }
    fn hooks<F: FnOnce() -> io::Result<Handoff> + Send + 'static>(
        work: F,
    ) -> Arc<dyn CheckpointHooks> {
        Arc::new(Hooks(Mutex::new(Some(work))))
    }

    #[tokio::test]
    async fn blocked_open_keeps_control_responsive_and_delivers_errors() {
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (release, released) = mpsc::channel();
        let task = tokio::spawn(open(
            hooks(move || {
                entered.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(2)).unwrap();
                Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied"))
            }),
            "blocked".into(),
        ));
        entering.await.unwrap();
        assert!(!task.is_finished());
        tokio::time::timeout(Duration::from_secs(1), tokio::task::yield_now())
            .await
            .unwrap();
        release.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::PermissionDenied));
    }

    #[tokio::test]
    async fn disconnected_waiter_does_not_cancel_accepted_open() {
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (release, released) = mpsc::channel();
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let finished = completed.clone();
        let task = tokio::spawn(open(
            hooks(move || {
                entered.send(()).unwrap();
                released.recv_timeout(Duration::from_secs(2)).unwrap();
                finished.store(true, std::sync::atomic::Ordering::Release);
                Ok(Box::pin(std::future::pending()))
            }),
            "disconnected".into(),
        ));
        entering.await.unwrap();
        task.abort();
        let _ = task.await;
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !completed.load(std::sync::atomic::Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn repeated_opens_deliver_the_handoff_without_unrelated_timers() {
        tokio::time::timeout(Duration::from_secs(1), async {
            for _ in 0..100 {
                let handoff = open(
                    hooks(|| Ok(Box::pin(async { Ok(HandoffOutcome::Resume) }))),
                    "repeat".into(),
                )
                .await
                .unwrap();
                assert!(matches!(handoff.await.unwrap(), HandoffOutcome::Resume));
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn worker_exit_without_result_fails_instead_of_waiting_forever() {
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            open(
                hooks(|| panic!("checkpoint worker failed")),
                "failed".into(),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::UnexpectedEof));
    }
}
