//! T7 `LocalApiWatch`.

use crate::runtime::{Services, cancelled};

/// T7 `LocalApiWatch`: the local UDS API task is owned by `LocalApiServer`;
/// this service polls its abort handle every 100 ms so a dead UDS cannot hide
/// behind a healthy TCP health check.
///
/// Fatal class: once the local API task has finished, it returns `Err`
/// ("local SDK service exited unexpectedly"), which shuts the Node down. No
/// log events of its own. Stop: `services.stop` ends it with `Ok`.
pub(crate) struct LocalApiWatch {
    local_task: tokio::task::AbortHandle,
}

impl LocalApiWatch {
    pub(crate) fn new(local_task: tokio::task::AbortHandle) -> Self {
        Self { local_task }
    }

    /// Start the watch as one service; it stops with `services.stop`.
    pub(crate) fn start(self, services: &mut Services) {
        let local_task = self.local_task;
        let stop = services.stop.subscribe();
        services.spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(100));
            tokio::pin! {let shutdown=cancelled(stop);}
            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    _ = tick.tick() => if local_task.is_finished() {
                        return Err(std::io::Error::other("local SDK service exited unexpectedly").into());
                    }
                }
            }
        });
    }
}
