//! Commands for the supervisor's single owner of process generations.
use crate::{config::Process, Result};
use std::{
    fs::File,
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Owner {
    Relay,
    Adxlet(super::ProcessIdentity),
}

pub(crate) enum Launch {
    Relay {
        directory: PathBuf,
    },
    Adxlet {
        bootstrap: PathBuf,
        executable: File,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitStage {
    Confirm,
    Select,
}

pub(crate) enum Operation {
    RuntimeInspect,
    Fence {
        pid: u32,
        owner: Owner,
    },
    Observe {
        pid: u32,
        owner: Owner,
    },
    Start {
        config: Box<Process>,
        pid: u32,
        binary: PathBuf,
        launch: Launch,
        attempt: String,
        owner: Owner,
    },
    Receipt {
        owner: Owner,
        old_pid: u32,
        attempt: String,
        pid: Option<u32>,
    },
    StopFailed {
        owner: Owner,
        old_pid: u32,
        attempt: String,
        pid: Option<u32>,
    },
    Commit {
        owner: Owner,
        old_pid: u32,
        attempt: String,
        pid: u32,
        stage: CommitStage,
    },
    Release,
}

pub(crate) enum Reply {
    NeverCreated,
    Committed,
    Runtime {
        config: Box<Process>,
        pid: u32,
        executable: File,
        admin_pid: u32,
        admin_socket: PathBuf,
        relay: Option<PathBuf>,
    },
    State {
        exited: bool,
        success: bool,
    },
    Candidate {
        pid: u32,
        exited: bool,
    },
}

pub(crate) struct Command {
    pub expected_pid: Option<u32>,
    pub service_id: String,
    pub task_id: String,
    pub operation: Operation,
    pub reply: oneshot::Sender<Result<Reply>>,
    pub expires: Instant,
}

pub(super) async fn command(
    sender: &mpsc::Sender<Command>,
    task: &super::Task,
    operation: Operation,
    deadline: Instant,
) -> Result<Reply> {
    command_with_pid(sender, task, operation, deadline, None).await
}

pub(super) async fn inspect_candidate(
    sender: &mpsc::Sender<Command>,
    task: &super::Task,
    pid: u32,
    deadline: Instant,
) -> Result<Reply> {
    command_with_pid(sender, task, Operation::RuntimeInspect, deadline, Some(pid)).await
}

async fn command_with_pid(
    sender: &mpsc::Sender<Command>,
    task: &super::Task,
    operation: Operation,
    deadline: Instant,
    candidate_pid: Option<u32>,
) -> Result<Reply> {
    if Instant::now() >= deadline {
        return Err("actor operation deadline exceeded".into());
    }
    let (reply, result) = oneshot::channel();
    tokio::time::timeout(
        Duration::from_secs(2),
        sender.send(Command {
            expected_pid: if matches!(&operation, Operation::RuntimeInspect) {
                candidate_pid.or(match &task.submission.selection {
                    super::Selection::Publisher { expected } => Some(expected.pid),
                    _ => None,
                })
            } else {
                None
            },
            service_id: task.submission.service_id.clone(),
            task_id: task.submission.task_id.clone(),
            operation,
            reply,
            expires: deadline,
        }),
    )
    .await?
    .map_err(|_| "supervisor unavailable")?;
    tokio::time::timeout(Duration::from_secs(2), result).await??
}
