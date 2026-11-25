//! OwnerFiles client hooks for the optional HIXL body transport.

use super::{
    Backend, DataMode, FileAttributes, OwnerPeerClient, PeerError, PeerResult, RemoteFile,
    RootGrant, WriteOptions, owner_hixl, owner_hixl_direct_unsupported,
    owner_hixl_write_unsupported, select,
};
use std::{ffi::OsStr, sync::Arc};

impl OwnerPeerClient {
    /// Serves eligible buffered opens of `home_node_id` with HIXL writes and read-ahead
    /// windows when the client selects HIXL.
    pub fn with_hixl(
        mut self,
        reader: Arc<owner_hixl::OwnerHixlReader>,
        home_node_id: String,
    ) -> Self {
        self.read_session = Some(owner_hixl::OwnerReadSession::new(reader, home_node_id));
        self
    }

    pub(super) fn hixl_read_session(&self) -> Option<&owner_hixl::OwnerReadSession> {
        self.read_session
            .as_ref()
            .filter(|_| select(self.data_mode, true, false) == Backend::Hixl)
    }

    pub(super) fn warm_body_transport(&self, grant: &RootGrant) {
        if let Some(hixl) = self.hixl_read_session()
            && let Ok(client) = self.cloned_client()
        {
            hixl.warm(&self.runtime, &client, grant);
        }
    }

    pub(super) fn before_body_open(
        &self,
        grant: &RootGrant,
        flags: i32,
    ) -> afs_error::Result<bool> {
        if self.data_mode == DataMode::Hixl && flags & libc::O_DIRECT != 0 {
            return Err(owner_hixl_direct_unsupported());
        }
        if self.data_mode == DataMode::Hixl
            && flags & libc::O_PATH == 0
            && flags & (libc::O_ACCMODE | libc::O_TRUNC) != libc::O_RDONLY
        {
            self.hixl_read_session()
                .ok_or_else(owner_hixl_write_unsupported)?
                .ensure_session_for_open(
                    &self.runtime,
                    &self.cloned_client().map_err(|error| error.0)?,
                    grant,
                )?;
        }
        let hixl_allows_prefetch = self
            .hixl_read_session()
            .is_none_or(owner_hixl::OwnerReadSession::allows_grpc_open_prefetch);
        Ok(hixl_allows_prefetch && self.ordinary_backend() == Backend::Grpc)
    }

    pub(super) fn body_opened(
        &self,
        grant: &RootGrant,
        path: &OsStr,
        handle: &[u8],
        identity: &[u8],
        flags: i32,
        attributes: &FileAttributes,
    ) {
        if let Some(hixl) = self.hixl_read_session() {
            hixl.opened(grant, path, handle, identity, flags, attributes);
        }
    }

    pub(super) fn try_hixl_read(
        &self,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<Option<usize>> {
        if let Some(hixl) = self.hixl_read_session() {
            let client = self.cloned_client().map_err(|error| error.0)?;
            match hixl.read(&self.runtime, &client, grant, file, offset, out)? {
                owner_hixl::WindowRead::Done(read) => return Ok(Some(read)),
                owner_hixl::WindowRead::Fallback { served: 0 } => {}
                owner_hixl::WindowRead::Fallback { served } => {
                    let rest = self.read_uncached(
                        grant,
                        file,
                        offset.saturating_add(served as u64),
                        &mut out[served..],
                    )?;
                    return Ok(Some(served + rest));
                }
            }
        }
        Ok(None)
    }

    pub(super) fn try_hixl_reply(
        &self,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        size: usize,
    ) -> Option<prost::bytes::Bytes> {
        if size != 0
            && let Some(hixl) = self.hixl_read_session()
            && let Some(hit) = hixl.slice_window(
                &self.runtime,
                || self.cloned_client().ok(),
                grant,
                file,
                offset,
                size,
            )
        {
            return Some(hit);
        }
        None
    }

    pub(super) fn try_hixl_write(
        &self,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> PeerResult<Option<usize>> {
        if !data.is_empty()
            && let Some(session) = self.hixl_read_session()
            && session.is_buffered_writer(&file.handle)
        {
            match session.write(
                &self.runtime,
                &self.cloned_client()?,
                grant,
                file,
                offset,
                data,
                options,
            ) {
                Ok(Some(written)) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.record_payload("client", "write", "hixl", written as u64);
                    }
                    return Ok(Some(written));
                }
                Ok(None) => {}
                Err(error) => {
                    if error.code() != afs_error::NODE_OWNER_WRITE_UNKNOWN {
                        self.invalidate_owner_body(&file.identity.0);
                    }
                    return Err(PeerError(error));
                }
            }
        }
        if !data.is_empty() && self.data_mode == DataMode::Hixl {
            return Err(PeerError(owner_hixl_write_unsupported()));
        }
        Ok(None)
    }

    pub(super) fn body_released(&self, handle: &[u8]) {
        if let Some(hixl) = &self.read_session {
            if hixl.is_buffered_writer(handle) {
                hixl.release_write_window(&self.runtime, handle);
            }
            hixl.released(handle);
        }
    }
}
