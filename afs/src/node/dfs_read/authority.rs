//! Node-lifetime authority for native reads.

use super::*;

#[derive(Clone, Copy, Debug)]
pub(in crate::node) struct NativeReadAuthoritySnapshot {
    pub(super) epoch: u64,
    pub(super) expires_at: Option<Instant>,
}

impl NativeReadAuthoritySnapshot {
    pub(in crate::node) fn expires_at(&self) -> Option<Instant> {
        self.expires_at
    }
}

#[derive(Clone, Debug)]
pub(super) struct LocalReadAuthorityState {
    pub(super) epoch: u64,
    pub(super) expires_at: Option<Instant>,
    pub(super) stopped: bool,
}

/// Shared local reader authority state supplied by the Node lifecycle.
///
/// Product wiring should refresh it from registration/heartbeat and stop it
/// before FUSE output can be committed. Unit-only engines use an unbounded
/// unknown-epoch authority so legacy tests do not accidentally depend on Node
/// lifecycle plumbing.
pub(in crate::node) struct LocalReadAuthority {
    pub(super) state: Mutex<LocalReadAuthorityState>,
}

impl LocalReadAuthority {
    pub(in crate::node) fn new(epoch: u64, expires_at: Instant) -> Self {
        Self {
            state: Mutex::new(LocalReadAuthorityState {
                epoch,
                expires_at: Some(expires_at),
                stopped: false,
            }),
        }
    }

    pub(super) fn unbounded_for_tests_and_legacy() -> Self {
        Self {
            state: Mutex::new(LocalReadAuthorityState {
                epoch: 0,
                expires_at: None,
                stopped: false,
            }),
        }
    }

    pub(in crate::node) fn refresh(&self, epoch: u64, expires_at: Instant) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS read authority state is poisoned"))?;
        if state.stopped {
            return Err(external_denied("DFS read authority has stopped"));
        }
        if expires_at <= Instant::now()
            || state
                .expires_at
                .is_some_and(|deadline| deadline <= Instant::now())
        {
            state.stopped = true;
            return Err(external_denied("DFS read authority expired"));
        }
        if state.epoch != epoch {
            state.stopped = true;
            return Err(external_denied("DFS read authority epoch changed"));
        }
        state.expires_at = Some(expires_at);
        Ok(())
    }

    pub(in crate::node) fn stop(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.stopped = true;
        }
    }

    pub(super) fn check_snapshot(&self, snapshot: &NativeReadAuthoritySnapshot) -> Result<()> {
        // Same-epoch heartbeats may extend future admissions, never an already
        // captured operation or opened view. Known stop/epoch changes win.
        let current = self.snapshot()?;
        if current.epoch != snapshot.epoch
            || snapshot
                .expires_at
                .is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(external_denied(
                "DFS read authority changed or expired before publication",
            ));
        }
        Ok(())
    }

    pub(super) fn snapshot(&self) -> Result<NativeReadAuthoritySnapshot> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS read authority state is poisoned"))?;
        if state.stopped {
            return Err(external_denied("DFS read authority has stopped"));
        }
        if state
            .expires_at
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(external_denied("DFS read authority expired"));
        }
        Ok(NativeReadAuthoritySnapshot {
            epoch: state.epoch,
            expires_at: state.expires_at,
        })
    }
}

impl DfsReadEngine {
    pub(in crate::node) fn capture_native_read_authority(
        &self,
    ) -> Result<NativeReadAuthoritySnapshot> {
        self.read_authority.snapshot()
    }

    pub(in crate::node) fn check_native_read_authority(
        &self,
        snapshot: &NativeReadAuthoritySnapshot,
    ) -> Result<()> {
        self.read_authority.check_snapshot(snapshot)
    }

    pub(super) fn invalidate_native_batch_authority(&self, batch: &ReadBatch) {
        // A read/grant denial belongs to this immutable read context, not the
        // Node registration lease. Drop its sources so a later attempt must
        // return to Meta; only Node lifecycle events stop global authority.
        if let Some(version) = &batch.file_version_id
            && let Ok(mut cache) = self.source_cache.lock()
        {
            cache.invalidate_scope(version, &batch.layout_root_id);
            cache.record(&self.local);
        }
    }

    pub(super) fn invalidate_native_batch_on_error(&self, batch: &ReadBatch, error: &Error) {
        if matches!(
            error.kind(),
            afs_error::ErrorKind::PermissionDenied | afs_error::ErrorKind::Unauthenticated
        ) {
            self.invalidate_native_batch_authority(batch);
        }
    }
}
