//! Owner-side replay table for proxied mutating operations (ledger R54).
use super::*;

/// Results an owner handle keeps for proxied write/resize/sync replay (R54).
/// The oldest completed result is evicted to make room; in-flight
/// placeholders are never evicted.
pub(super) const OWNER_OPERATION_RESULTS_PER_HANDLE: usize = 64;

#[derive(Clone)]
pub(super) enum RemoteOperationResult {
    Write {
        fingerprint: [u8; 32],
        reply: afs_protocol::node_data::DfsOwnerWriteReply,
    },
    Resize {
        fingerprint: [u8; 32],
        reply: afs_protocol::node_data::DfsOwnerResizeReply,
    },
    Sync {
        fingerprint: [u8; 32],
        reply: afs_protocol::node_data::DfsOwnerSyncReply,
    },
}

impl RemoteOperationResult {
    fn fingerprint(&self) -> [u8; 32] {
        match self {
            Self::Write { fingerprint, .. }
            | Self::Resize { fingerprint, .. }
            | Self::Sync { fingerprint, .. } => *fingerprint,
        }
    }
}

enum OwnerOperationSlot {
    /// The operation is being applied; `token` identifies the claim that owns it.
    InFlight {
        fingerprint: [u8; 32],
        token: u64,
    },
    Done(RemoteOperationResult),
}

#[derive(Default)]
struct OwnerHandleOperations {
    /// Operation ids in arrival order (oldest first); same set as `entries`.
    order: VecDeque<OperationId>,
    entries: HashMap<OperationId, OwnerOperationSlot>,
}

/// Owner-side idempotency table for proxied mutating operations (ledger R54).
///
/// Invariants:
/// - Keyed by the owner-side `FileHandle`. Handle ids are never reused and the
///   table is only consulted after `validate_owner_handle` bound the caller
///   node/session to the handle, so the key is at least as narrow as the old
///   `(peer, caller_session, operation_id)` key.
/// - Each handle keeps at most `OWNER_OPERATION_RESULTS_PER_HANDLE` slots;
///   admitting a new operation evicts that handle's oldest `Done` slot.
/// - The process-wide slot count never exceeds `MAX_REMOTE_OPERATION_RESULTS`;
///   an operation that would grow past it is rejected *before* it is applied.
/// - A slot is `InFlight` from admission until the operation finishes; a
///   duplicate arriving meanwhile gets a retryable error. A failed operation
///   removes its placeholder, so errors are never cached.
/// - All slots of a handle are dropped when the owner handle is released,
///   its open is cancelled, or it is reaped with its caller session.
#[derive(Default)]
pub(super) struct OwnerOperationTable {
    handles: HashMap<FileHandle, OwnerHandleOperations>,
    total: usize,
    next_token: u64,
}

pub(super) enum OwnerOperationBegin {
    Cached(RemoteOperationResult),
    Apply(u64),
}

impl OwnerOperationTable {
    pub(super) fn begin(
        &mut self,
        file: FileHandle,
        operation_id: &OperationId,
        fingerprint: [u8; 32],
    ) -> Result<OwnerOperationBegin> {
        if let Some(slot) = self
            .handles
            .get(&file)
            .and_then(|ops| ops.entries.get(operation_id))
        {
            let recorded = match slot {
                OwnerOperationSlot::InFlight { fingerprint, .. } => *fingerprint,
                OwnerOperationSlot::Done(result) => result.fingerprint(),
            };
            if recorded != fingerprint {
                return Err(invalid(
                    "DFS remote operation id was reused with a different request body",
                ));
            }
            return match slot {
                OwnerOperationSlot::InFlight { .. } => Err(unavailable(
                    "DFS remote operation is still in progress; retry later",
                )),
                OwnerOperationSlot::Done(result) => Ok(OwnerOperationBegin::Cached(result.clone())),
            };
        }
        let held = self.handles.get(&file).map_or(0, |ops| ops.entries.len());
        if held < OWNER_OPERATION_RESULTS_PER_HANDLE {
            if self.total >= MAX_REMOTE_OPERATION_RESULTS {
                return Err(unavailable(
                    "DFS remote operation table is at capacity; retry later",
                ));
            }
        } else {
            let ops = self.handles.get_mut(&file).expect("held > 0 implies entry");
            let oldest_done = ops
                .order
                .iter()
                .position(|id| matches!(ops.entries.get(id), Some(OwnerOperationSlot::Done(_))));
            let Some(index) = oldest_done else {
                return Err(unavailable(
                    "DFS owner handle has too many operations in progress; retry later",
                ));
            };
            let evicted = ops.order.remove(index).expect("index from position");
            ops.entries.remove(&evicted);
            self.total -= 1;
        }
        self.next_token = self.next_token.wrapping_add(1);
        let token = self.next_token;
        let ops = self.handles.entry(file).or_default();
        ops.order.push_back(operation_id.clone());
        ops.entries.insert(
            operation_id.clone(),
            OwnerOperationSlot::InFlight { fingerprint, token },
        );
        self.total += 1;
        Ok(OwnerOperationBegin::Apply(token))
    }

    fn claimed_slot(
        &mut self,
        file: FileHandle,
        operation_id: &OperationId,
        token: u64,
    ) -> Option<&mut OwnerOperationSlot> {
        self.handles
            .get_mut(&file)?
            .entries
            .get_mut(operation_id)
            .filter(|slot| {
                matches!(slot, OwnerOperationSlot::InFlight { token: owned, .. } if *owned == token)
            })
    }

    fn complete(
        &mut self,
        file: FileHandle,
        operation_id: &OperationId,
        token: u64,
        result: RemoteOperationResult,
    ) {
        // The handle may have been released while the operation ran; its
        // slots are gone then and there is nothing left to replay against.
        if let Some(slot) = self.claimed_slot(file, operation_id, token) {
            *slot = OwnerOperationSlot::Done(result);
        }
    }

    fn abandon(&mut self, file: FileHandle, operation_id: &OperationId, token: u64) {
        if self.claimed_slot(file, operation_id, token).is_none() {
            return;
        }
        let ops = self
            .handles
            .get_mut(&file)
            .expect("claimed slot implies entry");
        ops.entries.remove(operation_id);
        ops.order.retain(|id| id != operation_id);
        self.total -= 1;
        if ops.entries.is_empty() {
            self.handles.remove(&file);
        }
    }

    pub(super) fn clear_handle(&mut self, file: FileHandle) {
        if let Some(ops) = self.handles.remove(&file) {
            self.total -= ops.entries.len();
        }
    }

    pub(super) fn handle_ids(&self) -> Vec<FileHandle> {
        self.handles.keys().copied().collect()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.total
    }
}

/// An admitted operation's in-flight placeholder. Dropping it without
/// `complete` (error or panic while applying) removes the placeholder so the
/// same operation id can be applied again.
pub(super) struct OwnerOperationClaim<'a> {
    table: &'a Mutex<OwnerOperationTable>,
    file: FileHandle,
    operation_id: OperationId,
    token: u64,
    done: bool,
}

impl<'a> OwnerOperationClaim<'a> {
    pub(super) fn new(
        table: &'a Mutex<OwnerOperationTable>,
        file: FileHandle,
        operation_id: OperationId,
        token: u64,
    ) -> Self {
        Self {
            table,
            file,
            operation_id,
            token,
            done: false,
        }
    }

    pub(super) fn complete(mut self, result: RemoteOperationResult) {
        self.done = true;
        // A poisoned table already fails every later admission, so dropping
        // the record here cannot turn into a silent second apply.
        if let Ok(mut table) = self.table.lock() {
            table.complete(self.file, &self.operation_id, self.token, result);
        }
    }
}

impl Drop for OwnerOperationClaim<'_> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Ok(mut table) = self.table.lock() {
            table.abandon(self.file, &self.operation_id, self.token);
        }
    }
}

pub(super) enum OwnerOperationStart<'a> {
    Cached(RemoteOperationResult),
    Apply(OwnerOperationClaim<'a>),
}
