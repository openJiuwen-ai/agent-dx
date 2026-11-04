//! The one place where synchronous and asynchronous node code meet.
//!
//! The write/commit core, `Backend` and the sync port methods stay synchronous
//! for now; tonic and the read core are asynchronous. Every crossing goes
//! through this module, so the rules live in one place:
//!
//! - Sync to async: [`block_on`] / [`SyncRuntime::block_on`]. Allowed on a plain
//!   thread, on a `spawn_blocking` thread and inside `block_in_place`. Called
//!   while a runtime is driving the thread, they return a structured error
//!   instead of letting Tokio panic.
//! - Async to sync: [`spawn_blocking`] for work that may wait on the network or
//!   a contended lock, [`offload`] for short local work that may avoid the pool,
//!   [`local_io`] for finite local disk work inside an async read, and
//!   [`run_sync_backend`] for genuinely synchronous provider implementations.
use std::{future::Future, panic::Location, sync::Arc};

use afs_error::{Error, Result};
use tokio::runtime::{Handle, Runtime, RuntimeFlavor};

/// Output of a bridged future: fallible, so a refused entry can be reported in
/// the future's own error type.
pub(crate) trait BridgeOutput {
    fn from_bridge_error(error: Error) -> Self;
}

impl<T, E: From<Error>> BridgeOutput for std::result::Result<T, E> {
    fn from_bridge_error(error: Error) -> Self {
        Err(error.into())
    }
}

/// Fails with `CLIENT_WORKER_FAILED` when blocking here would panic, because a
/// runtime is driving this thread (async worker or `block_on` future).
///
/// A runtime handle alone does not decide this: `spawn_blocking` threads and
/// `block_in_place` regions have one and may block. Tokio exposes that state
/// only through the panic in its own entry check, which fires before the runtime
/// is entered, so the check asks exactly that question with a ready future. On
/// misuse the panic hook still prints Tokio's message. A thread without a handle
/// (the FUSE workers) cannot be inside a runtime and skips the probe.
#[track_caller]
pub(crate) fn check_sync_entry() -> Result<()> {
    let Ok(current) = Handle::try_current() else {
        return Ok(());
    };
    let entered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        current.block_on(std::future::ready(()));
    }))
    .is_err();
    if entered {
        return Err(Error::coded(
            afs_error::CLIENT_WORKER_FAILED,
            format!(
                "synchronous bridge entered at {} while a Tokio runtime drives this thread; \
                 use spawn_blocking or block_in_place, or await the async method",
                Location::caller()
            ),
        ));
    }
    Ok(())
}

/// Blocks this thread on `future`, or returns the [`check_sync_entry`] error
/// without polling it.
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
#[track_caller]
pub(crate) fn block_on<F>(runtime: &Handle, future: F) -> F::Output
where
    F: Future,
    F::Output: BridgeOutput,
{
    match check_sync_entry() {
        Ok(()) => runtime.block_on(future),
        Err(error) => F::Output::from_bridge_error(error),
    }
}

/// Runtime of a synchronous client: the ambient one, or a private one kept alive
/// with the client when it is built outside any runtime.
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
#[derive(Clone)]
pub(crate) enum SyncRuntime {
    Existing(Handle),
    Owned(Arc<Runtime>),
}

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
impl SyncRuntime {
    /// Reuses the ambient runtime, or builds a private current-thread one.
    /// `client` names the client in the build error.
    pub(crate) fn current_or_new(client: &str) -> Result<Self> {
        if let Ok(handle) = Handle::try_current() {
            return Ok(Self::Existing(handle));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                Error::coded(
                    afs_error::CLIENT_WORKER_FAILED,
                    format!("failed to build {client} runtime: {error}"),
                )
            })?;
        Ok(Self::Owned(Arc::new(runtime)))
    }

    /// Reuses the ambient runtime, or builds a private one whose single worker
    /// keeps running tasks spawned on it between `block_on` calls.
    #[cfg(feature = "dfs")]
    pub(crate) fn current_or_new_with_worker() -> Result<Self> {
        if let Ok(handle) = Handle::try_current() {
            return Ok(Self::Existing(handle));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(Error::from)?;
        Ok(Self::Owned(Arc::new(runtime)))
    }

    /// See [`block_on`].
    #[track_caller]
    pub(crate) fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
        F::Output: BridgeOutput,
    {
        if let Err(error) = check_sync_entry() {
            return F::Output::from_bridge_error(error);
        }
        match self {
            Self::Existing(handle) => handle.block_on(future),
            Self::Owned(runtime) => runtime.block_on(future),
        }
    }
}

/// Drives `future` on a private current-thread runtime on a scoped thread, for
/// synchronous adapters that have no runtime of their own.
#[cfg(feature = "dfs")]
pub(crate) fn block_on_scoped_runtime<T: Send>(
    future: impl Future<Output = Result<T>> + Send,
) -> Result<T> {
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| Error::coded(afs_error::CLIENT_WORKER_FAILED, e.to_string()))?;
                runtime.block_on(future)
            })
            .join()
            .map_err(|_| {
                Error::coded(
                    afs_error::CLIENT_WORKER_FAILED,
                    "sync read adapter panicked",
                )
            })?
    })
}

/// Runs `work` on the blocking pool of the ambient runtime.
#[track_caller]
pub(crate) fn spawn_blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    tokio::task::spawn_blocking(work)
}

/// Runs `work` on the blocking pool of `runtime`.
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
#[track_caller]
pub(crate) fn spawn_blocking_on<T: Send + 'static>(
    runtime: &Handle,
    work: impl FnOnce() -> T + Send + 'static,
) -> tokio::task::JoinHandle<T> {
    runtime.spawn_blocking(work)
}

/// Runs a request handler in its own task, so a dropped RPC future cannot stop
/// it between steps that must finish together (admission and its cleanup).
#[cfg(feature = "dfs")]
pub(crate) async fn run_detached<T: Send + 'static>(
    work: impl Future<Output = T> + Send + 'static,
) -> std::result::Result<T, tokio::task::JoinError> {
    tokio::spawn(work).await
}

/// Short blocking work from async code. A multi-thread runtime keeps it on this
/// worker and hands the worker's other tasks off, which saves a pool hop per
/// call; a current-thread runtime cannot, so it uses the pool.
#[cfg(feature = "ownerfs")]
pub(crate) async fn offload<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> std::result::Result<T, tokio::task::JoinError> {
    if on_multi_thread_runtime() {
        Ok(tokio::task::block_in_place(work))
    } else {
        tokio::task::spawn_blocking(work).await
    }
}

/// Only genuine synchronous backend adapters use this boundary. Native gRPC
/// overrides the async methods and never sends network waiting through it.
/// Multi-thread runtimes support a finite blocking region; current-thread
/// compatibility drivers use a scoped thread outside all runtime contexts.
#[cfg(feature = "dfs")]
pub(crate) fn run_sync_backend<T: Send>(work: impl FnOnce() -> Result<T> + Send) -> Result<T> {
    if Handle::try_current().is_err() {
        return work();
    }
    if on_multi_thread_runtime() {
        return tokio::task::block_in_place(work);
    }
    std::thread::scope(|scope| scope.spawn(work).join()).map_err(|_| {
        Error::coded(
            afs_error::CLIENT_WORKER_FAILED,
            "synchronous read backend panicked",
        )
    })?
}

/// Genuine local file/codec work may borrow the private response, never a
/// network wait. Production multi-thread runtimes replace this worker while
/// it is in the blocking region. Synchronous/current-thread test callers run
/// the same finite local operation inline.
#[cfg(feature = "dfs")]
pub(crate) fn local_io<T>(work: impl FnOnce() -> T) -> T {
    if on_multi_thread_runtime() {
        tokio::task::block_in_place(work)
    } else {
        work()
    }
}

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
fn on_multi_thread_runtime() -> bool {
    Handle::try_current().is_ok_and(|handle| handle.runtime_flavor() == RuntimeFlavor::MultiThread)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn multi_thread() -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    fn ready_after_yield(handle: &Handle) -> Result<u32> {
        block_on(handle, async {
            tokio::task::yield_now().await;
            Ok(7)
        })
    }

    fn assert_misuse(result: Result<u32>) {
        let error = result.unwrap_err();
        assert_eq!(error.code(), afs_error::CLIENT_WORKER_FAILED);
        let message = error.to_string();
        assert!(message.contains("Tokio runtime"), "{message}");
        assert!(message.contains("bridge.rs"), "names the caller: {message}");
    }

    #[test]
    fn sync_entry_on_a_multi_thread_worker_returns_an_error() {
        let runtime = multi_thread();
        let handle = runtime.handle().clone();
        let result = runtime
            .block_on(runtime.spawn(async move { ready_after_yield(&handle) }))
            .unwrap();
        assert_misuse(result);
    }

    #[test]
    fn sync_entry_in_a_multi_thread_block_on_future_returns_an_error() {
        let runtime = multi_thread();
        let handle = runtime.handle().clone();
        assert_misuse(runtime.block_on(async move { ready_after_yield(&handle) }));
    }

    #[tokio::test]
    async fn sync_entry_on_a_current_thread_runtime_returns_an_error() {
        assert_misuse(ready_after_yield(&Handle::current()));
    }

    #[test]
    fn sync_entry_does_not_poll_the_future_it_refuses() {
        let runtime = multi_thread();
        let handle = runtime.handle().clone();
        runtime.block_on(async move {
            let polled = std::sync::atomic::AtomicBool::new(false);
            let result: Result<()> = block_on(&handle, async {
                polled.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            });
            assert!(result.is_err());
            assert!(!polled.load(std::sync::atomic::Ordering::SeqCst));
        });
    }

    #[test]
    fn sync_entry_from_a_plain_thread_works() {
        let runtime = multi_thread();
        let handle = runtime.handle().clone();
        let result = std::thread::spawn(move || ready_after_yield(&handle))
            .join()
            .unwrap();
        assert_eq!(result.unwrap(), 7);
    }

    #[test]
    fn sync_entry_inside_block_in_place_works() {
        let runtime = multi_thread();
        let handle = runtime.handle().clone();
        let result =
            runtime
                .block_on(runtime.spawn(async move {
                    tokio::task::block_in_place(|| ready_after_yield(&handle))
                }))
                .unwrap();
        assert_eq!(result.unwrap(), 7);
    }

    #[test]
    fn sync_entry_on_a_blocking_pool_thread_works() {
        let runtime = multi_thread();
        let handle = runtime.handle().clone();
        let worker = handle.clone();
        let result = runtime
            .block_on(spawn_blocking_on(&handle, move || {
                ready_after_yield(&worker)
            }))
            .unwrap();
        assert_eq!(result.unwrap(), 7);
    }

    #[tokio::test]
    async fn sync_entry_on_a_current_thread_blocking_pool_thread_works() {
        let handle = Handle::current();
        let result = spawn_blocking(move || ready_after_yield(&handle))
            .await
            .unwrap();
        assert_eq!(result.unwrap(), 7);
    }

    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    #[test]
    fn sync_runtime_outside_a_runtime_owns_one_and_refuses_inside_a_runtime() {
        let owned = SyncRuntime::current_or_new("test client").unwrap();
        assert!(matches!(owned, SyncRuntime::Owned(_)));
        let ok: Result<u32> = owned.block_on(async { Ok(7) });
        assert_eq!(ok.unwrap(), 7);

        let outer = multi_thread();
        outer.block_on(async {
            let refused: Result<u32> = owned.block_on(async { Ok(7) });
            assert_misuse(refused);
            let reused = SyncRuntime::current_or_new("test client").unwrap();
            assert!(matches!(reused, SyncRuntime::Existing(_)));
        });
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn detached_work_finishes_after_its_caller_is_dropped() {
        let runtime = multi_thread();
        runtime.block_on(async {
            let release = Arc::new(tokio::sync::Notify::new());
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
            let waiter = release.clone();
            let caller = tokio::spawn(run_detached(async move {
                let _ = started_tx.send(());
                waiter.notified().await;
                let _ = finished_tx.send(());
            }));
            started_rx.await.unwrap();
            caller.abort();
            let _ = caller.await;
            release.notify_one();
            finished_rx.await.expect("detached work was cancelled");
        });
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn offload_on_a_current_thread_runtime_uses_the_pool() {
        assert_eq!(offload(|| 7).await.unwrap(), 7);
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test(flavor = "multi_thread")]
    async fn offload_on_a_multi_thread_runtime_stays_on_this_worker() {
        let caller = std::thread::current().id();
        let worker = offload(move || std::thread::current().id()).await.unwrap();
        assert_eq!(caller, worker);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn run_sync_backend_and_local_io_work_on_a_current_thread_runtime() {
        assert_eq!(run_sync_backend(|| Ok(7)).unwrap(), 7);
        assert_eq!(local_io(|| 7), 7);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread")]
    async fn run_sync_backend_and_local_io_work_on_a_multi_thread_worker() {
        assert_eq!(run_sync_backend(|| Ok(7)).unwrap(), 7);
        assert_eq!(local_io(|| 7), 7);
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn run_sync_backend_and_local_io_run_inline_outside_a_runtime() {
        let caller = std::thread::current().id();
        let inline = run_sync_backend(|| Ok(std::thread::current().id())).unwrap();
        assert_eq!(inline, caller);
        assert_eq!(local_io(|| std::thread::current().id()), caller);
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn scoped_runtime_drives_a_future_from_any_thread() {
        assert_eq!(block_on_scoped_runtime(async { Ok(7) }).unwrap(), 7);
        multi_thread().block_on(async {
            assert_eq!(block_on_scoped_runtime(async { Ok(7) }).unwrap(), 7);
        });
    }
}
