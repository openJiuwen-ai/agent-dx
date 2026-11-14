//! A sealed Chunk waiting for durable placement.

use super::*;

/// Shared producer/replica budget per immutable staged chunk, never a file limit.
#[cfg(feature = "dfs")]
pub(crate) const MAX_STAGED_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StagedChunk {
    pub operation_id: OperationId,
    pub chunk: ChunkObject,
    pub(super) bytes: SharedChunkBytes,
    pub(super) outboard: Option<SharedOutboard>,
    pub(super) segment_crc: Option<Arc<super::super::chunk_crc32c::CrcTable>>,
    // This private identity binds the immutable allocation to its initial integrity
    // descriptor. CRC descriptors bind segment checksums, not cryptographic body proofs.
    #[cfg(feature = "dfs")]
    pub(super) sealed_chunk: ChunkObject,
}

impl StagedChunk {
    /// Rebuild a received/repaired body under its original immutable identity.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn from_replica(
        operation_id: OperationId,
        expected: &ChunkObject,
        bytes: Vec<u8>,
    ) -> Result<Self> {
        let staged = if expected.is_segment_crc32c() {
            Self::from_shared_crc32c_id(
                operation_id,
                SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(bytes)),
                expected.id.clone(),
            )?
        } else {
            Self::new(operation_id, bytes)
        };
        if &staged.chunk != expected {
            return Err(corrupt(
                &expected.id,
                "replica body differs from immutable descriptor",
            ));
        }
        Ok(staged)
    }

    pub fn new(operation_id: OperationId, bytes: Vec<u8>) -> Self {
        Self::from_shared(
            operation_id,
            SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(bytes)),
        )
    }

    pub fn from_shared(operation_id: OperationId, bytes: SharedChunkBytes) -> Self {
        tracing::info_span!(
            "node.dfs.chunk.stage",
            operation_id = %operation_id.0,
            bytes = bytes.len() as u64,
            allocation_id = bytes.allocation_id(),
            capacity = bytes.capacity() as u64,
            body_pointer = bytes.as_ptr() as usize,
            chunk_id = tracing::field::Empty,
        )
        .in_scope(|| {
            let bytes_len = bytes.len() as u64;
            let built = bytes
                .buffer
                .pool
                .upgrade()
                .and_then(|pool| pool.outboard_budget.clone())
                .map(|budget| {
                    let built = tracing::info_span!("node.dfs.chunk.hash", bytes = bytes_len,)
                        .in_scope(|| outboard::build_borrowed(&bytes, &budget));
                    if built.outboard.is_none() {
                        budget.record_write_without_proof();
                    }
                    built
                });
            let content_digest = built
                .as_ref()
                .map(|built| built.digest.clone())
                .unwrap_or_else(|| {
                    tracing::info_span!("node.dfs.chunk.hash", bytes = bytes_len,)
                        .in_scope(|| digest(&bytes))
                });
            let outboard = built.and_then(|built| built.outboard);
            let chunk = ChunkObject {
                id: ChunkId::new(format!(
                    "blake3-{}-{}",
                    digest_hex(&content_digest),
                    bytes.len()
                )),
                length: bytes.len() as u64,
                content_digest,
                encoding: ChunkEncoding::Raw,
            };
            tracing::Span::current().record("chunk_id", chunk.id.0.as_str());
            Self {
                operation_id,
                #[cfg(feature = "dfs")]
                sealed_chunk: chunk.clone(),
                chunk,
                bytes,
                outboard,
                segment_crc: None,
            }
        })
    }

    /// S3 payload under its generational BLAKE3 identity (R05).
    pub fn from_shared_generational(operation_id: OperationId, bytes: SharedChunkBytes) -> Self {
        let mut staged = Self::from_shared(operation_id, bytes);
        let root = blake3::Hash::from(staged.chunk.content_digest.bytes);
        staged.chunk.id =
            ChunkObject::generational_blake3_id(&staged.operation_id, &root, staged.chunk.length);
        #[cfg(feature = "dfs")]
        {
            staged.sealed_chunk = staged.chunk.clone();
        }
        staged
    }

    pub fn new_generational(operation_id: OperationId, bytes: Vec<u8>) -> Self {
        Self::from_shared_generational(
            operation_id,
            SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(bytes)),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_crc32c(operation_id: OperationId, bytes: Vec<u8>) -> Result<Self> {
        Self::from_shared_crc32c(
            operation_id,
            SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(bytes)),
        )
    }

    /// Immutable native payload, bound to its persisted CRC descriptor.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn from_shared_crc32c(
        operation_id: OperationId,
        bytes: SharedChunkBytes,
    ) -> Result<Self> {
        let id = ChunkId::new(format!(
            "crc32c-v2-{}-{}",
            ChunkObject::generation_id(&operation_id),
            uuid::Uuid::new_v4().simple()
        ));
        Self::from_shared_crc32c_id(operation_id, bytes, id)
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn from_shared_crc32c_id(
        operation_id: OperationId,
        bytes: SharedChunkBytes,
        id: ChunkId,
    ) -> Result<Self> {
        let table = super::super::chunk_crc32c::CrcTable::build(&bytes)?;
        let chunk = ChunkObject {
            id,
            length: bytes.len() as u64,
            content_digest: table.descriptor_digest(),
            encoding: ChunkEncoding::Raw,
        };
        if !chunk.valid_immutable_identity() {
            return Err(invalid("invalid CRC32C object identity"));
        }
        Ok(Self {
            operation_id,
            #[cfg(feature = "dfs")]
            sealed_chunk: chunk.clone(),
            chunk,
            bytes,
            outboard: None,
            segment_crc: Some(Arc::new(table)),
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The BLAKE3 outboard built while sealing, when the buffer pool carried
    /// an outboard budget and it had room.
    #[cfg(feature = "dfs")]
    pub(in crate::node) fn outboard(&self) -> Option<&SharedOutboard> {
        self.outboard.as_ref()
    }

    /// Content proof only: operation identity, authorization and persistence
    /// still require their own checks. No mutable payload is exposed.
    #[cfg(feature = "dfs")]
    pub(in crate::node) fn has_sealed_identity(&self) -> bool {
        self.chunk == self.sealed_chunk && self.chunk.length == self.bytes.len() as u64
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn shared_bytes(&self) -> SharedChunkBytes {
        self.bytes.clone()
    }

    /// Heap capacity owned by the staged metadata, excluding the separately
    /// charged final body and shared outboard. Callers
    /// account for the inline StagedChunk/container storage themselves.
    #[cfg(feature = "dfs")]
    pub(crate) fn native_retained_metadata_bytes(&self) -> usize {
        self.operation_id
            .0
            .capacity()
            .saturating_add(self.chunk.id.0.capacity())
            .saturating_add(self.sealed_chunk.id.0.capacity())
            .saturating_add(self.segment_crc.as_ref().map_or(0, |t| {
                std::mem::size_of::<super::super::chunk_crc32c::CrcTable>()
                    + 16
                    + t.checksums.capacity() * 4
            }))
    }
}

#[cfg(feature = "dfs")]
impl AsRef<[u8]> for StagedChunk {
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}
