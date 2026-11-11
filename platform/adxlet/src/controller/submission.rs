//! Admission is independent of the actor queue and the lifetime of RPC waiters.
use super::{Command, Envelope, OperationResult, ResumeRequest};
use adx_core::{Error, Result};
use std::sync::{Mutex, MutexGuard};
use tokio::sync::{mpsc, oneshot, watch};

const ADMISSION_CAPACITY: usize = 32;

#[derive(PartialEq, Eq)]
pub(super) enum RestoreKey {
    // Ensure is an Environment-level demand signal. The first admitted request
    // supplies execution metadata; concurrent demand waits for that same result.
    Ensure,
    Resume(ResumeRequest),
    // Coordinator progress revisions may advance while this operation is active.
    Recover(String),
}

type Completion = watch::Receiver<Option<Result<OperationResult>>>;
// All commands for one Environment share this FIFO admission path. Registry
// updates and admission sends happen under the same synchronous lock, so the
// worker observes the same order as callers. Delete retains a separate priority
// path so it can cancel a queued restore and bypass a full actor queue.
enum Admission {
    Restore {
        tx: mpsc::Sender<Envelope>,
        envelope: Envelope,
        result: oneshot::Receiver<Result<OperationResult>>,
        completed: watch::Sender<Option<Result<OperationResult>>>,
        deletion: watch::Receiver<bool>,
        processing: oneshot::Sender<()>,
    },
    Command {
        tx: mpsc::Sender<Envelope>,
        envelope: Envelope,
        admitted: oneshot::Sender<Result<()>>,
    },
}
struct Flight {
    key: RestoreKey,
    barrier_epoch: u64,
    completion: Completion,
}
#[derive(Default)]
struct Pending {
    restore: Option<Flight>,
    restore_barrier_epoch: u64,
    delete: Option<Completion>,
    // Reconciliation may reject a provisionally latched delete before execution.
    // Once actor dispatch accepts it, cleanup failures must keep admission closed.
    delete_accepted: bool,
}

pub(super) struct Submissions {
    pending: Mutex<Pending>,
    admission: mpsc::Sender<Admission>,
    deleting: watch::Sender<bool>,
}

impl Submissions {
    pub(super) fn new() -> Self {
        let (admission, requests) = mpsc::channel(ADMISSION_CAPACITY);
        tokio::spawn(admit_in_order(requests));
        Self {
            pending: Mutex::new(Pending::default()),
            admission,
            deleting: watch::channel(false).0,
        }
    }

    pub(super) fn deleting(&self) -> bool {
        *self.deleting.borrow()
    }

    pub(super) async fn cancelled(&self) {
        let mut changed = self.deleting.subscribe();
        let _ = changed.wait_for(|deleting| *deleting).await;
    }

    pub(super) async fn enqueue(
        &self,
        tx: &mpsc::Sender<Envelope>,
        envelope: Envelope,
        restore_barrier: bool,
    ) -> Result<()> {
        let admitted = {
            let mut pending = self.lock_pending()?;
            let barrier_epoch = if restore_barrier {
                pending
                    .restore_barrier_epoch
                    .checked_add(1)
                    .ok_or(Error::Conflict)?
            } else {
                pending.restore_barrier_epoch
            };
            let (admitted, result) = oneshot::channel();
            self.try_admit(Admission::Command {
                tx: tx.clone(),
                envelope,
                admitted,
            })?;
            pending.restore_barrier_epoch = barrier_epoch;
            result
        };
        admitted.await.map_err(|_| stopped())?
    }

    pub(super) async fn restore(
        &self,
        tx: &mpsc::Sender<Envelope>,
        key: RestoreKey,
        command: Command,
    ) -> Result<OperationResult> {
        let completion = {
            let mut pending = self.lock_pending()?;
            if self.deleting() {
                return Err(Error::Conflict);
            }
            let barrier_epoch = pending.restore_barrier_epoch;
            if let Some(flight) = pending.restore.as_ref().filter(|flight| {
                flight.barrier_epoch == barrier_epoch && flight.completion.borrow().is_none()
            }) {
                if key != RestoreKey::Ensure && flight.key != key {
                    return Err(Error::Conflict);
                }
                flight.completion.clone()
            } else {
                let (reply, result) = oneshot::channel();
                let envelope = Envelope::new(command, reply);
                let (completed, completion) = watch::channel(None);
                let deletion = self.deleting.subscribe();
                let (processing, queued) = oneshot::channel();
                self.try_admit(Admission::Restore {
                    tx: tx.clone(),
                    envelope,
                    result,
                    completed: completed.clone(),
                    deletion: deletion.clone(),
                    processing,
                })?;
                cancel_queued_restore(deletion, queued, completed);
                pending.restore = Some(Flight {
                    key,
                    barrier_epoch,
                    completion: completion.clone(),
                });
                completion
            }
        };
        wait(completion).await
    }

    pub(super) async fn delete(&self, tx: &mpsc::Sender<Envelope>) -> Result<OperationResult> {
        let completion = {
            let mut pending = self.lock_pending()?;
            if let Some(completion) = pending
                .delete
                .as_ref()
                .filter(|completion| completion.borrow().is_none())
            {
                completion.clone()
            } else {
                // Rejection before acceptance reopens this provisional latch.
                self.deleting.send_replace(true);
                let completion = submit_delete(tx.clone());
                pending.delete = Some(completion.clone());
                completion
            }
        };
        wait(completion).await
    }

    pub(super) fn accept_delete(&self) -> Result<()> {
        let mut pending = self.lock_pending()?;
        pending.delete_accepted = true;
        Ok(())
    }

    pub(super) fn reject_delete(&self) {
        if let Ok(pending) = self.pending.lock() {
            if !pending.delete_accepted {
                self.deleting.send_replace(false);
            }
        }
    }

    fn try_admit(&self, request: Admission) -> Result<()> {
        self.admission
            .try_send(request)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    Error::Unavailable("environment command admission full".into())
                }
                mpsc::error::TrySendError::Closed(_) => stopped(),
            })
    }

    fn lock_pending(&self) -> Result<MutexGuard<'_, Pending>> {
        self.pending
            .lock()
            .map_err(|_| Error::Unavailable("environment submission registry unavailable".into()))
    }
}

async fn admit_in_order(mut requests: mpsc::Receiver<Admission>) {
    while let Some(request) = requests.recv().await {
        match request {
            Admission::Restore {
                tx,
                envelope,
                result,
                completed,
                mut deletion,
                processing,
            } => {
                if processing.send(()).is_err() {
                    // Cancellation already published Conflict to the waiters.
                    continue;
                }
                // Cancellation before actor admission proves the command never
                // entered the queue. Accepted work outlives its RPC waiter.
                let admitted = tokio::select! {
                    biased;
                    _ = deletion.wait_for(|deleting| *deleting) => Err(Error::Conflict),
                    result = tx.send(envelope) => result.map_err(|_| stopped()),
                };
                match admitted {
                    Ok(()) => complete(result, completed),
                    Err(error) => {
                        completed.send_replace(Some(Err(error)));
                    }
                }
            }
            Admission::Command {
                tx,
                envelope,
                admitted,
            } => {
                let result = tx.send(envelope).await.map_err(|_| stopped());
                let _ = admitted.send(result);
            }
        }
    }
}

fn cancel_queued_restore(
    mut deletion: watch::Receiver<bool>,
    processing: oneshot::Receiver<()>,
    completed: watch::Sender<Option<Result<OperationResult>>>,
) {
    tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = processing => {}
            deleted = deletion.wait_for(|deleting| *deleting) => {
                if deleted.is_ok() {
                    completed.send_replace(Some(Err(Error::Conflict)));
                }
            }
        }
    });
}

fn complete(
    result: oneshot::Receiver<Result<OperationResult>>,
    completed: watch::Sender<Option<Result<OperationResult>>>,
) {
    tokio::spawn(async move {
        let result = result.await.unwrap_or_else(|_| Err(stopped()));
        completed.send_replace(Some(result));
    });
}

fn submit_delete(tx: mpsc::Sender<Envelope>) -> Completion {
    let (reply, result) = oneshot::channel();
    let envelope = Envelope::new(Command::Delete, reply);
    let (completed, completion) = watch::channel(None);
    tokio::spawn(async move {
        let admitted = tx.send(envelope).await.map_err(|_| stopped());
        let result = match admitted {
            Ok(()) => result.await.unwrap_or_else(|_| Err(stopped())),
            Err(error) => Err(error),
        };
        completed.send_replace(Some(result));
    });
    completion
}

async fn wait(mut completion: Completion) -> Result<OperationResult> {
    loop {
        if let Some(result) = completion.borrow_and_update().clone() {
            return result;
        }
        completion.changed().await.map_err(|_| stopped())?;
    }
}

fn stopped() -> Error {
    Error::Unavailable("environment controller stopped".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{PauseRequest, SnapshotRequest};
    use std::{future::Future, sync::Arc, task::Poll};

    async fn assert_pending<F: Future>(future: &mut std::pin::Pin<Box<F>>) {
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    fn envelope(command: Command) -> Envelope {
        let (reply, _) = oneshot::channel();
        Envelope::new(command, reply)
    }

    fn pause_envelope(operation_id: &str) -> Envelope {
        envelope(Command::Pause(PauseRequest {
            operation_id: operation_id.into(),
            expected_revision: 1,
            retention: adx_core::lifecycle::RetentionPolicy::default(),
            timeout_seconds: 60,
        }))
    }

    #[tokio::test]
    async fn admitted_restore_wins_over_delete_cancellation() {
        let (deleting, deletion) = watch::channel(false);
        let (processing, queued) = oneshot::channel();
        let (completed, completion) = watch::channel(None);
        processing.send(()).unwrap();
        deleting.send_replace(true);
        cancel_queued_restore(deletion, queued, completed);
        tokio::task::yield_now().await;
        assert!(completion.borrow().is_none());
    }

    #[tokio::test]
    async fn delete_cancels_restore_waiting_behind_barrier() {
        let submissions = Arc::new(Submissions::new());
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(envelope(Command::Sync)).await.unwrap();

        let mut pausing = Box::pin(submissions.enqueue(&tx, pause_envelope("pause"), true));
        assert_pending(&mut pausing).await;
        let mut restoring = Box::pin(submissions.restore(
            &tx,
            RestoreKey::Ensure,
            Command::EnsureRunning("wake".into(), None),
        ));
        assert_pending(&mut restoring).await;
        let (delete_tx, mut delete_rx) = mpsc::channel(1);
        let mut deleting = Box::pin(submissions.delete(&delete_tx));
        assert_pending(&mut deleting).await;
        let delete_envelope = delete_rx.recv().await.unwrap();

        assert_eq!(restoring.await, Err(Error::Conflict));
        submissions.reject_delete();
        delete_envelope.reply.send(Err(Error::Conflict)).unwrap();
        assert_eq!(deleting.await, Err(Error::Conflict));

        assert!(matches!(rx.recv().await.unwrap().command, Command::Sync));
        pausing.await.unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().command,
            Command::Pause(_)
        ));
        tokio::task::yield_now().await;
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn rejected_delete_does_not_relatch_while_result_is_pending() {
        let submissions = Submissions::new();
        let (tx, mut rx) = mpsc::channel(1);
        let mut first = Box::pin(submissions.delete(&tx));
        assert_pending(&mut first).await;
        let envelope = rx.recv().await.unwrap();

        submissions.reject_delete();
        let mut shared = Box::pin(submissions.delete(&tx));
        assert_pending(&mut shared).await;
        assert!(!submissions.deleting());

        envelope.reply.send(Err(Error::Conflict)).unwrap();
        assert_eq!(first.await, Err(Error::Conflict));
        assert_eq!(shared.await, Err(Error::Conflict));
    }

    #[tokio::test]
    async fn restore_does_not_cross_barrier_waiting_for_queue_admission() {
        assert!(!Command::Reconcile.is_restore_barrier());
        let submissions = Arc::new(Submissions::new());
        let (tx, mut rx) = mpsc::channel(1);

        let mut first = Box::pin(submissions.restore(
            &tx,
            RestoreKey::Ensure,
            Command::EnsureRunning("first".into(), None),
        ));
        assert_pending(&mut first).await;
        let first_envelope = rx.recv().await.unwrap();

        tx.send(envelope(Command::Sync)).await.unwrap();
        let mut pausing = Box::pin(submissions.enqueue(&tx, pause_envelope("pause"), true));
        assert_pending(&mut pausing).await;

        let (snapshot_reply, _) = oneshot::channel();
        let snapshot = Command::Snapshot(
            SnapshotRequest {
                kind: adx_core::snapshots::SnapshotKind::Regular,
                operation_id: "snapshot".into(),
                expected_revision: 1,
                names: Vec::new(),
                timeout_seconds: 60,
            },
            snapshot_reply,
        );
        let mut snapshotting = Box::pin(submissions.enqueue(&tx, envelope(snapshot), false));
        assert_pending(&mut snapshotting).await;

        let mut later = Box::pin(submissions.restore(
            &tx,
            RestoreKey::Ensure,
            Command::EnsureRunning("later".into(), None),
        ));
        assert_pending(&mut later).await;
        let mut shared = Box::pin(submissions.restore(
            &tx,
            RestoreKey::Ensure,
            Command::EnsureRunning("shared".into(), Some(30)),
        ));
        assert_pending(&mut shared).await;

        assert!(matches!(rx.recv().await.unwrap().command, Command::Sync));
        pausing.await.unwrap();
        let barrier = rx.recv().await.unwrap();
        assert!(
            matches!(barrier.command, Command::Pause(_)),
            "later restore crossed a barrier waiting for queue admission"
        );
        snapshotting.await.unwrap();
        assert!(matches!(
            rx.recv().await.unwrap().command,
            Command::Snapshot(..)
        ));
        let later_envelope = rx.recv().await.unwrap();
        assert!(matches!(later_envelope.command, Command::EnsureRunning(..)));
        tokio::task::yield_now().await;
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        let _ = first_envelope.reply.send(Err(Error::Conflict));
        let _ = later_envelope.reply.send(Err(Error::Conflict));
        assert_eq!(first.await, Err(Error::Conflict));
        assert_eq!(later.await, Err(Error::Conflict));
        assert_eq!(shared.await, Err(Error::Conflict));
    }
}
