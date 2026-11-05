//! Streaming sink that verifies and publishes peer bytes.

use super::*;

/// One complete BLAKE3 verification against trusted Meta identity, borrowing
/// the caller's temporary body. Neither proof nor cache retains a body.
#[derive(Debug)]
pub struct VerifiedPeerChunk<'a> {
    pub(super) chunk: &'a ChunkObject,
    pub(super) bytes: &'a [u8],
    pub(super) outboard: Option<SharedOutboard>,
    stream_origin: bool,
}

impl<'a> VerifiedPeerChunk<'a> {
    pub fn verify(
        chunk: &'a ChunkObject,
        bytes: &'a [u8],
        budget: Arc<OutboardBudget>,
    ) -> Result<Self> {
        validate_chunk(chunk)?;
        if bytes.len() as u64 != chunk.length {
            return Err(corrupt(
                "Peer cache fill differs from trusted object length",
            ));
        }
        if chunk.length > MAX_OBJECT_BYTES {
            if blake3::hash(bytes).as_bytes() != &chunk.content_digest.bytes {
                return Err(corrupt(
                    "Peer cache fill differs from trusted object identity",
                ));
            }
            return Ok(Self {
                chunk,
                bytes,
                outboard: None,
                stream_origin: false,
            });
        }
        let built = build_borrowed(bytes, &budget);
        if built.digest != chunk.content_digest {
            return Err(corrupt(
                "Peer cache fill differs from trusted object identity",
            ));
        }
        Ok(Self {
            chunk,
            bytes,
            outboard: built.outboard,
            stream_origin: false,
        })
    }

    pub(in crate::node) fn from_completed_stream(
        chunk: &'a ChunkObject,
        bytes: &'a [u8],
        proof: impl PeerObjectProof<'a>,
    ) -> Result<Self> {
        validate_chunk(chunk)?;
        if bytes.len() as u64 != chunk.length
            || !std::ptr::eq(bytes, proof.bytes())
            || proof.root_digest() != &chunk.content_digest.bytes
        {
            return Err(corrupt(
                "Peer completed stream proof differs from trusted object",
            ));
        }
        let outboard = proof.into_outboard();
        if let Some(proof) = &outboard {
            validate_outboard(chunk, proof)?;
        }
        Ok(Self {
            chunk,
            bytes,
            outboard,
            stream_origin: true,
        })
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
    pub fn chunk(&self) -> &'a ChunkObject {
        self.chunk
    }
    pub fn uses_completed_stream_proof(&self) -> bool {
        self.stream_origin
    }
}

/// Opaque per-fill binding. Only a sink can mint it; decoding carries it from
/// first byte through EOF so another same-object pending file cannot reuse proof.
#[derive(Clone, Debug)]
pub(in crate::node) struct PeerStreamBinding {
    pub(super) identity: Arc<()>,
    pointer: usize,
    length: usize,
    offset: u64,
}

impl PeerStreamBinding {
    pub(in crate::node) fn matches_output(&self, offset: u64, output: &[u8]) -> bool {
        self.offset == offset
            && self.pointer == output.as_ptr() as usize
            && self.length == output.len()
    }
}

/// Complete stream receipt borrowing only the unpublished requested output.
/// The file proof is separately bound to its actual pending descriptor identity.
#[derive(Debug)]
pub struct CompletedPeerDiskRead<'output> {
    pub(super) chunk: ChunkObject,
    requested_offset: u64,
    output: &'output [u8],
    root_digest: [u8; 32],
    pub(super) outboard: Option<SharedOutboard>,
    file_identity: Option<FileIdentity>,
    binding: PeerStreamBinding,
}

impl<'output> CompletedPeerDiskRead<'output> {
    pub fn chunk(&self) -> &ChunkObject {
        &self.chunk
    }
    pub fn requested_offset(&self) -> u64 {
        self.requested_offset
    }
    pub fn requested_length(&self) -> usize {
        self.output.len()
    }
    pub fn root_digest(&self) -> &[u8; 32] {
        &self.root_digest
    }
    #[cfg(test)]
    pub fn private_output(&self) -> &'output [u8] {
        self.output
    }
    pub fn matches_private_output_identity(&self, pointer: usize, length: usize) -> bool {
        self.output.as_ptr() as usize == pointer && self.output.len() == length
    }
}

/// Private staging borrows the unique native fill allowance, not a table lock.
/// Cache failures disable this sink; they never stop the same RPC verification.
pub struct PeerStreamSink<'a> {
    pub(super) cache: &'a DiskPeerCache,
    pub(super) _fill: &'a FillPermit<'a>,
    pub(super) chunk: ChunkObject,
    pub(super) key: String,
    pub(super) received: u64,
    pub(super) header_length: u64,
    pub(super) file: Option<File>,
    pub(super) pending_identity: Option<(u64, u64)>,
    pub(super) pending_generation: u64,
    pub(super) owns_pending: bool,
    pub(super) committed: bool,
    pub(super) prepare_error: Option<Error>,
    pub(super) allow_eviction: bool,
    pub(super) entry_lease: Option<OutboardLease>,
    pub(super) identity: Arc<()>,
    #[cfg(test)]
    pub(super) write_delay: std::time::Duration,
}

impl PeerStreamSink<'_> {
    pub(crate) fn is_caching(&self) -> bool {
        self.file.is_some()
    }
    pub(in crate::node) fn proof_budget(&self) -> Arc<OutboardBudget> {
        self.cache.proof_budget()
    }

    pub(in crate::node) fn bind_output(
        &self,
        offset: u64,
        output: &[u8],
    ) -> Result<PeerStreamBinding> {
        if offset
            .checked_add(output.len() as u64)
            .is_none_or(|end| end > self.chunk.length)
        {
            return Err(invalid("Peer stream output binding exceeds object"));
        }
        Ok(PeerStreamBinding {
            identity: self.identity.clone(),
            pointer: output.as_ptr() as usize,
            length: output.len(),
            offset,
        })
    }

    pub(in crate::node) fn accepts_binding(&self, binding: &PeerStreamBinding) -> bool {
        Arc::ptr_eq(&self.identity, &binding.identity)
    }

    pub(super) fn prepare(&mut self) -> Result<()> {
        self.cache.check_root()?;
        if self.allow_eviction {
            self.cache.cleanup_pending()?;
        }
        self.header_length = MAX_HEADER_BYTES as u64;
        let unit = self
            .cache
            .root
            .metadata()
            .map_err(Error::from)?
            .blksize()
            .max(512);
        let length = self.header_length + self.chunk.length + MAX_OUTBOARD_BYTES as u64;
        let reservation = length
            .checked_add(unit - 1)
            .and_then(|size| size.checked_div(unit))
            .and_then(|blocks| blocks.checked_add(2))
            .and_then(|blocks| blocks.checked_mul(unit))
            .ok_or_else(|| invalid("Peer stream reservation overflow"))?;
        let metadata_charge = entry_metadata_charge(&self.key, &self.chunk);
        self.entry_lease = if self.allow_eviction {
            self.cache.reserve_entry_metadata(metadata_charge)?
        } else {
            self.cache
                .proof_budget
                .try_reserve_resident(metadata_charge)
        };
        if self.entry_lease.is_none() {
            return Ok(());
        }
        if !self.cache.reserve_pending(
            &self.key,
            self.pending_generation,
            reservation,
            metadata_charge,
            &mut self.entry_lease,
            self.allow_eviction,
        )? {
            return Ok(());
        }
        let opened = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.cache.pending_path(self.pending_generation));
        let file = match opened {
            Ok(file) => file,
            Err(error) => {
                // No file was created by this attempt; retain any foreign file.
                let mut state = self.cache.lock_state()?;
                remove_pending_charge(&mut state, self.pending_generation);
                return Err(Error::from(error));
            }
        };
        self.file = Some(file);
        self.owns_pending = true;
        #[cfg(test)]
        if self.cache.lock_state()?.fail_stream_metadata {
            return Err(unavailable("injected pending descriptor metadata failure"));
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| unavailable("Peer stream file was not opened"))?;
        let metadata = file.metadata().map_err(Error::from)?;
        self.pending_identity = Some((metadata.dev(), metadata.ino()));
        if let Some(pending) = self
            .cache
            .lock_state()?
            .pending
            .get_mut(&self.pending_generation)
        {
            pending.identity = self.pending_identity;
        }
        write_header(
            file,
            &Header {
                version: VERSION,
                namespace_id: self.cache.options.namespace_id.clone(),
                chunk: self.chunk.clone(),
                outboard_length: 0,
            },
        )?;
        Ok(())
    }

    fn abort_pending(&mut self) -> Result<()> {
        // Pin the inode until identity checking and unlink finish, so close
        // cannot open an inode-reuse gap while deciding which path is owned.
        let _file = self.file.take();
        if !self.owns_pending {
            return Ok(());
        }
        self.cache.check_root()?;
        match fs::symlink_metadata(self.cache.pending_path(self.pending_generation)) {
            Ok(metadata) if Some((metadata.dev(), metadata.ino())) == self.pending_identity => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => {
                return Err(invalid(
                    "Peer stream cleanup refuses a replaced pending file",
                ));
            }
        }
        self.cache
            .clear_pending(self.pending_generation, self.pending_identity)?;
        self.owns_pending = false;
        Ok(())
    }

    pub(super) fn disable(&mut self, error: &Error) {
        afs_logging::warn!("dfs.peer_stream_cache_disabled"; "error" => error.to_string());
        if let Err(cleanup) = self.abort_pending() {
            afs_logging::warn!("dfs.peer_stream_cache_cleanup_failed"; "error" => cleanup.to_string());
        }
    }

    #[cfg(test)]
    pub(in crate::node) fn delay_writes_for_test(&mut self, delay: std::time::Duration) {
        self.write_delay = delay;
    }

    pub(in crate::node) fn write(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        if offset != self.received
            || offset
                .checked_add(bytes.len() as u64)
                .is_none_or(|end| end > self.chunk.length)
        {
            return Err(invalid("Peer stream sink range differs from the object"));
        }
        self.received += bytes.len() as u64;
        #[cfg(test)]
        std::thread::sleep(self.write_delay);
        #[cfg(test)]
        if self.cache.lock_state()?.fail_stream_write {
            self.disable(&unavailable("injected stream cache write failure"));
        }
        if let Some(file) = &mut self.file
            && let Err(error) = file.write_all(bytes)
        {
            self.disable(&Error::from(error));
        }
        Ok(())
    }

    pub(in crate::node) fn seal_completed<'output>(
        &mut self,
        proof: impl PeerStreamProof,
        requested_offset: u64,
        output: &'output [u8],
    ) -> Result<CompletedPeerDiskRead<'output>> {
        if !self.accepts_binding(proof.binding())
            || !proof.binding().matches_output(requested_offset, output)
            || proof.chunk() != &self.chunk
            || self.received != self.chunk.length
            || proof.root_digest() != &self.chunk.content_digest.bytes
            || requested_offset
                .checked_add(output.len() as u64)
                .is_none_or(|end| end > self.chunk.length)
        {
            return Err(corrupt(
                "Peer stream completion differs from the sink object",
            ));
        }
        let binding = proof.binding().clone();
        let root_digest = *proof.root_digest();
        let outboard = proof
            .into_outboard()
            .filter(|proof| proof.uses_budget(&self.cache.proof_budget));
        if let Some(proof) = &outboard {
            validate_outboard(&self.chunk, proof)?;
        }
        let checked = (|| {
            let Some(file) = &mut self.file else {
                return Ok(None);
            };
            let trailer = outboard.as_ref().map_or(&[][..], |proof| proof.encoded());
            file.write_all(trailer).map_err(Error::from)?;
            file.seek(SeekFrom::Start(0)).map_err(Error::from)?;
            write_header(
                file,
                &Header {
                    version: VERSION,
                    namespace_id: self.cache.options.namespace_id.clone(),
                    chunk: self.chunk.clone(),
                    outboard_length: trailer.len() as u32,
                },
            )?;
            file.flush().map_err(Error::from)?;
            let metadata = file.metadata().map_err(Error::from)?;
            validate_private_file(&metadata)?;
            self.cache.check_root()?;
            let current = fs::symlink_metadata(self.cache.pending_path(self.pending_generation))
                .map_err(Error::from)?;
            if metadata.len()
                != self.header_length
                    + self.chunk.length
                    + outboard
                        .as_ref()
                        .map_or(0, |proof| proof.encoded().len() as u64)
                || Some((metadata.dev(), metadata.ino())) != self.pending_identity
                || file_identity(&metadata) != file_identity(&current)
            {
                return Err(corrupt(
                    "Peer stream pending descriptor changed before sealing",
                ));
            }
            let charge = allocated(&metadata)?;
            if self
                .cache
                .account_pending_growth(self.pending_generation, charge)?
            {
                return Err(unavailable(
                    "Peer stream allocation exceeds reservation before seal",
                ));
            }
            Ok(Some(file_identity(&metadata)))
        })();
        let file_identity = match checked {
            Ok(identity) => identity,
            Err(error) => {
                self.disable(&error);
                None
            }
        };
        Ok(CompletedPeerDiskRead {
            chunk: self.chunk.clone(),
            requested_offset,
            output,
            root_digest,
            outboard,
            file_identity,
            binding,
        })
    }

    pub fn commit(&mut self, completed: CompletedPeerDiskRead<'_>) -> Result<bool> {
        if !self.accepts_binding(&completed.binding)
            || completed.chunk != self.chunk
            || completed.root_digest != self.chunk.content_digest.bytes
        {
            return Err(corrupt("Peer disk completion belongs to another object"));
        }
        let Some(file) = &self.file else {
            return Ok(false);
        };
        self.cache.check_root()?;
        let metadata = file.metadata().map_err(Error::from)?;
        validate_private_file(&metadata)?;
        let current = fs::symlink_metadata(self.cache.pending_path(self.pending_generation))
            .map_err(Error::from)?;
        if completed.file_identity != Some(file_identity(&metadata))
            || file_identity(&metadata) != file_identity(&current)
        {
            return Err(corrupt(
                "Peer disk completion does not bind this pending descriptor",
            ));
        }
        let charge = allocated(&metadata)?;
        if self
            .cache
            .account_pending_growth(self.pending_generation, charge)?
        {
            return Err(unavailable(
                "Peer stream allocation exceeds reserved blocks",
            ));
        }
        match fs::symlink_metadata(self.cache.path(&self.key)) {
            Ok(_) => return Err(invalid("Peer stream publication refuses an untracked file")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
        fs::rename(
            self.cache.pending_path(self.pending_generation),
            self.cache.path(&self.key),
        )
        .map_err(Error::from)?;
        self.committed = true;
        self.owns_pending = false;
        let published = file
            .metadata()
            .ok()
            .map(|metadata| file_identity(&metadata));
        self.cache.publish_entry(
            (self.pending_generation, self.key.clone()),
            self.chunk.clone(),
            charge,
            published.unwrap_or_else(|| file_identity(&metadata)),
            if published.is_some() {
                completed.outboard
            } else {
                None
            },
            self.entry_lease.take(),
        )?;
        Ok(true)
    }
}

impl Drop for PeerStreamSink<'_> {
    fn drop(&mut self) {
        if !self.committed
            && let Err(error) = self.abort_pending()
        {
            afs_logging::warn!("dfs.peer_stream_drop_cleanup_failed"; "error" => error.to_string());
        }
        drop(self.entry_lease.take());
        if let Ok(mut state) = self.cache.state.lock()
            && let Some(pending) = state.pending.get_mut(&self.pending_generation)
        {
            pending.active = false;
        }
        self.cache.report_resources();
    }
}
