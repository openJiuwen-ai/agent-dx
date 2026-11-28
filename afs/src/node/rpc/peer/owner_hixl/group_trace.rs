//! Trace identity carried with a verified single-file window.

pub(super) use crate::node::data_plane::hixl::group_trace::GroupTrace;
use afs_protocol::node_data::OwnerPrepareReadHixlRequest;

pub(super) struct ReadyGroup {
    trace: GroupTrace,
    read_id: String,
    prepare_len: u64,
}

impl ReadyGroup {
    pub(super) fn new(trace: GroupTrace, prepare: OwnerPrepareReadHixlRequest) -> Self {
        Self {
            trace,
            read_id: prepare.read_id,
            prepare_len: prepare.length,
        }
    }

    pub(super) fn emit(&self, event: &'static str, bytes: usize, outcome: &'static str) {
        self.trace.emit(
            event,
            &self.read_id,
            self.prepare_len,
            bytes as u64,
            outcome,
        );
    }
}
