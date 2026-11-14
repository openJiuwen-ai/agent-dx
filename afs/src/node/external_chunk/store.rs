//! S3ChunkStore: the sink and reader facade over the request worker.

use super::*;

/// One owned runtime of `worker_threads` threads serves every request; each
/// request and each PUT is its own task, so payload signing and checksums of
/// concurrent PUTs run in parallel. All requests share `upload_concurrency`
/// upload permits; completing one PUT immediately makes its permit available
/// to the next chunk, including another batch.
/// The bounded queue applies backpressure rather than reporting a storage
/// failure. At most `upload_concurrency` PUTs hold immutable payload owners,
/// all of them charged to the write pipeline's dirty budget; typed leases stay
/// charged until the last transport/retry owner exits. Producer payload and
/// descriptor reservations belong to the DFS write pipeline.
/// A chunk of at least 256 KiB is stored with a BLAKE3 outboard sidecar
/// object (`<chunk key>.bao`); reads then fetch and prove only the groups
/// they cover. The sidecar is optional derived data: a chunk without a
/// usable one is read whole.
/// Cache entries remain disposable, never durable replicas. Drop drains and
/// joins the worker; timed-out PUTs cannot produce durability receipts.
pub struct S3ChunkStore {
    pub(super) options: S3ChunkStoreOptions,
    pub(super) sender: Option<mpsc::SyncSender<Command>>,
    pub(super) wake: Arc<tokio::sync::Notify>,
    pub(super) worker: Option<thread::JoinHandle<()>>,
    pub(super) cache: Option<Arc<S3ChunkCache>>,
    pub(super) verifier: Arc<S3ChunkVerifier>,
    pub(super) observer: S3GetObserver,
    /// Verified outboard sidecars of recently range-read chunks: a fixed
    /// bound of 256 entries of at most 8 KiB.
    pub(super) sidecars: SidecarCache,
}

impl S3ChunkStore {
    /// Rejects invalid identity, credentials, endpoint or resource budgets.
    pub fn new(options: S3ChunkStoreOptions) -> Result<Self> {
        validate_options(&options)?;
        let credentials = load_credentials(&options.credentials)?;
        // Select the same public TLS provider as ADX without depending on its
        // transport and Platform protocol crates. An installed provider wins.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let retry = RetryConfig {
            max_retries: options.max_retries,
            retry_timeout: options.request_timeout,
            ..Default::default()
        };
        let client = ClientOptions::new()
            .with_timeout(options.request_timeout)
            .with_connect_timeout(options.request_timeout.min(Duration::from_secs(10)))
            .with_pool_max_idle_per_host(options.upload_concurrency + EXTRA_IDLE_CONNECTIONS);
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&options.endpoint)
            .with_bucket_name(&options.bucket)
            .with_region(&options.region)
            .with_virtual_hosted_style_request(false)
            .with_access_key_id(credentials.access_key_id)
            .with_secret_access_key(credentials.secret_access_key)
            .with_retry(retry)
            .with_client_options(client)
            .with_allow_http(options.allow_http)
            .with_unsigned_payload(options.unsigned_payload);
        if options.payload_checksum == crate::config::DfsExternalPayloadChecksum::Crc64nvme {
            builder = builder.with_checksum_algorithm(object_store::aws::Checksum::CRC64NVME);
        }
        if let Some(token) = credentials.session_token {
            builder = builder.with_token(token);
        }
        let backend = builder
            .build()
            .map_err(|_| invalid("cannot construct S3 client from configured settings"))?;
        Self::with_backend(options, Arc::new(backend))
    }

    pub(super) fn with_backend(
        options: S3ChunkStoreOptions,
        backend: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        validate_options(&options)?;
        let verifier = S3ChunkVerifier::new()?;
        let observer = S3GetObserver::new(options.metrics.clone());
        verifier.set_metrics(options.metrics.clone())?;
        let cache = options
            .cache
            .clone()
            .map(|cache_options| {
                S3ChunkCache::new(
                    cache_options,
                    &options.store_id,
                    &options.generation,
                    verifier.clone(),
                )
            })
            .transpose()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(options.worker_threads)
            .thread_name("afs-s3-chunks")
            .enable_all()
            .build()
            .map_err(Error::from)?;
        let (sender, receiver) = mpsc::sync_channel::<Command>(1);
        let identity = Arc::new(options.clone());
        let wake = Arc::new(tokio::sync::Notify::new());
        let worker_wake = wake.clone();
        let worker_verifier = verifier.clone();
        let worker_observer = observer.clone();
        let worker = thread::Builder::new()
            .name("afs-s3-requests".into())
            .spawn(move || {
                runtime.block_on(run_requests(
                    backend,
                    identity,
                    receiver,
                    worker_wake,
                    worker_verifier,
                    worker_observer,
                ));
            })
            .map_err(Error::from)?;
        Ok(Self {
            options,
            sender: Some(sender),
            wake,
            worker: Some(worker),
            cache,
            verifier,
            observer,
            sidecars: SidecarCache::default(),
        })
    }

    fn enqueue(
        &self,
        request: Request,
    ) -> Result<(mpsc::Receiver<Result<Reply>>, &'static str, usize, u64)> {
        let (reply, receiver) = mpsc::sync_channel(1);
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| unavailable("S3 worker stopped"))?;
        let (operation, chunks, bytes) = request_summary(&request);
        let span = tracing::info_span!(
            "node.dfs.s3.request",
            operation,
            chunks,
            bytes,
            max_uploads = self.options.upload_concurrency,
            queue_wait_us = tracing::field::Empty,
        );
        let enqueued_at = std::time::Instant::now();
        // std::sync::mpsc supplies bounded synchronous backpressure even when
        // the caller is a Tokio business thread (blocking_send would panic).
        sender
            .send(Command {
                request,
                reply,
                span: span.clone(),
                enqueued_at,
            })
            .map_err(|_| unavailable("S3 worker stopped"))?;
        self.wake.notify_one();
        Ok((receiver, operation, chunks, bytes))
    }

    fn request(&self, request: Request) -> Result<Reply> {
        let (receiver, operation, chunks, bytes) = self.enqueue(request)?;
        // The worker owns a bounded number of requests and enforces a deadline
        // for each, including waiting for the shared upload permits.
        let wait_span = tracing::info_span!("node.dfs.s3.reply_wait", operation, chunks, bytes,);
        let started = std::time::Instant::now();
        wait_span.in_scope(|| {
            let reply = receiver
                .recv_timeout(
                    self.options.request_timeout * (MAX_ACTIVE_REQUESTS as u32 + 2)
                        + Duration::from_secs(1),
                )
                .map_err(|error| match error {
                    mpsc::RecvTimeoutError::Timeout => {
                        Error::coded(afs_error::IO_TIMEOUT, "S3 worker response timed out")
                    }
                    mpsc::RecvTimeoutError::Disconnected => unavailable("S3 worker stopped"),
                })?;
            self.observer
                .observe_reply_wait(operation, started.elapsed());
            reply
        })
    }

    fn read_verified(&self, chunk: &ChunkObject) -> Result<VerifiedExternalChunk> {
        validate_chunk(chunk)?;
        if let Some(cache) = &self.cache
            && let Some(bytes) = cache.lookup_result(chunk)?
        {
            tracing::info_span!(
                "node.dfs.s3.read_verified",
                branch = "cache_hit",
                chunk_id = %chunk.id.0,
                chunk_bytes = chunk.length,
                copied_bytes = 0_u64,
            )
            .in_scope(|| {});
            return Ok(bytes);
        }
        match self.request(Request::Read(chunk.clone()))? {
            Reply::Read(bytes) => {
                if let Some(cache) = &self.cache {
                    let started = std::time::Instant::now();
                    cache.schedule_from_read(chunk, bytes.clone());
                    self.observer
                        .observe_cache_admit("attempted", started.elapsed());
                    self.observer
                        .observe_cache_activity(cache.activity_snapshot());
                } else {
                    self.observer
                        .observe_cache_admit("no_cache", Duration::ZERO);
                }
                Ok(bytes)
            }
            _ => Err(invalid("S3 worker returned the wrong response")),
        }
    }

    /// Copies `ops` of one chunk into `out`, every byte verified against the
    /// chunk identity first. A chunk with a usable sidecar object is read by
    /// range; anything else, and any range read that fails its proof, takes
    /// the whole object and its full BLAKE3 check, which then decides.
    fn read_ranges_verified(
        &self,
        chunk: &ChunkObject,
        ops: &[ExternalRangeCopy],
        out: &mut [u8],
    ) -> Result<()> {
        validate_chunk(chunk)?;
        // With a cache configured every miss still fetches the whole object,
        // because that is what fills the cache.
        let plan = if self.cache.is_some() {
            None
        } else {
            plan_ranges(chunk, ops)
        };
        let Some(plan) = plan else {
            return self.read_whole(chunk, ops, out, "whole");
        };
        let known = self.sidecars.lookup(&chunk.id);
        if matches!(known, SidecarState::Absent) {
            return self.read_whole(chunk, ops, out, "whole_no_sidecar");
        }
        let fetch_sidecar = matches!(known, SidecarState::Unknown);
        let (parts, fetched) = match self.request(Request::ReadRanges {
            chunk: chunk.clone(),
            ranges: plan,
            sidecar: fetch_sidecar,
        })? {
            Reply::Ranges { parts, sidecar } => (parts, sidecar),
            _ => return Err(invalid("S3 worker returned the wrong response")),
        };
        let sidecar = match (known, fetched) {
            (SidecarState::Cached(sidecar), _) => sidecar,
            (_, Some(SidecarFetch::Found(encoded)))
                if validate_sidecar(chunk, &encoded).is_ok() =>
            {
                Arc::new(encoded)
            }
            (_, Some(SidecarFetch::Found(_))) => {
                self.sidecars.mark_absent(&chunk.id);
                self.observer.observe_sidecar("get_invalid");
                return self.read_whole(chunk, ops, out, "whole_no_sidecar");
            }
            (_, Some(SidecarFetch::Missing) | None) => {
                self.sidecars.mark_absent(&chunk.id);
                self.observer.observe_sidecar("get_missing");
                return self.read_whole(chunk, ops, out, "whole_no_sidecar");
            }
        };
        match verify_and_copy(chunk, &sidecar, &parts, ops, out) {
            Ok(()) => {
                if fetch_sidecar {
                    self.sidecars.insert(&chunk.id, sidecar);
                    self.observer.observe_sidecar("get_ok");
                }
                let bytes = parts.iter().map(|(_, bytes)| bytes.len() as u64).sum();
                self.observer.observe_read_branch("range", bytes);
                Ok(())
            }
            Err(error) if error.code() == afs_error::NODE_TRANSFER_CORRUPT_DATA => {
                // Either the data or the sidecar is damaged; a range proof
                // cannot tell which. The whole object decides: it either
                // verifies (the sidecar was bad) or the read fails as corrupt.
                self.sidecars.mark_absent(&chunk.id);
                self.observer.observe_sidecar("proof_failed");
                self.read_whole(chunk, ops, out, "whole_after_failed_proof")
            }
            Err(error) => Err(error),
        }
    }

    fn read_whole(
        &self,
        chunk: &ChunkObject,
        ops: &[ExternalRangeCopy],
        out: &mut [u8],
        branch: &'static str,
    ) -> Result<()> {
        let verified = self.read_verified(chunk)?;
        if !verified.matches(chunk) {
            return Err(corrupt(
                "verified external Chunk identity does not match the request",
            ));
        }
        self.observer.observe_read_branch(branch, chunk.length);
        copy_external_ranges(verified.as_slice(), ops, out)
    }

    /// Deletes these chunks' objects; missing objects count as deleted.
    pub fn delete_objects(&self, chunks: &[ChunkId]) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        match self.request(Request::Delete(chunks.to_vec()))? {
            Reply::Deleted => Ok(()),
            _ => Err(invalid("S3 worker returned the wrong response")),
        }
    }

    /// One page of this store's objects after `after`, as (chunks, last key).
    /// A key that names neither a generational chunk nor a chunk's sidecar is
    /// not ours to judge and is skipped. An empty page means the listing
    /// reached the end.
    pub fn list_objects(
        &self,
        after: Option<String>,
        limit: usize,
    ) -> Result<(Vec<ChunkObject>, Option<String>)> {
        let keys = match self.request(Request::List { after, limit })? {
            Reply::Listed(keys) => keys,
            _ => return Err(invalid("S3 worker returned the wrong response")),
        };
        // A sidecar object is reported as the chunk it belongs to, so a
        // sidecar whose chunk object is gone still reaches Meta's plan and is
        // deleted with that chunk id. A chunk and its sidecar are one entry.
        let mut seen = std::collections::HashSet::new();
        let chunks = keys
            .iter()
            .filter_map(|key| {
                let name = key.rsplit('/').next()?;
                let id = ChunkId::new(name.strip_suffix(EXTERNAL_OUTBOARD_SUFFIX).unwrap_or(name));
                ChunkObject::from_content_addressed_id(&id)
                    .filter(ChunkObject::is_generational_blake3)
            })
            .filter(|chunk| seen.insert(chunk.id.clone()))
            .collect();
        Ok((chunks, keys.into_iter().next_back()))
    }

    /// Close the private verifier pool. Any stored verifier failure is reported
    /// after all verifier worker threads have been joined.
    pub fn shutdown_verifier(&self) -> Result<()> {
        self.verifier.shutdown()
    }
}

impl Drop for S3ChunkStore {
    fn drop(&mut self) {
        drop(self.cache.take());
        drop(self.sender.take());
        self.wake.notify_one();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        if let Err(error) = self.verifier.shutdown() {
            tracing::warn!(message = %error.message(), "S3 verifier shutdown failed");
        }
    }
}

impl crate::node::gc::ExternalObjects for S3ChunkStore {
    fn delete_objects(&self, chunks: &[ChunkId]) -> Result<()> {
        S3ChunkStore::delete_objects(self, chunks)
    }
    fn list_objects(
        &self,
        after: Option<String>,
        limit: usize,
    ) -> Result<(Vec<ChunkObject>, Option<String>)> {
        S3ChunkStore::list_objects(self, after, limit)
    }
}

impl ChunkSink for S3ChunkStore {
    /// Same envelope as replicated stores (receipt x 8 plus 8 KiB for descriptor
    /// state, layout clones and publication scratch), with the external commit
    /// strings in place of replica ACKs. The object key is derived from store id,
    /// generation and the canonical BLAKE3 chunk id (about 90 bytes).
    fn descriptor_reservation_bytes(&self, operation_id: &OperationId) -> Result<usize> {
        let receipt = std::mem::size_of::<ChunkReceipt>()
            + std::mem::size_of::<ExternalCommitReceipt>()
            + operation_id.0.capacity()
            + 2 * (self.options.store_id.len() + self.options.generation.len())
            + 256;
        receipt
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(8192))
            .ok_or_else(|| invalid("S3 native descriptor reservation overflow"))
    }

    fn put_chunks(&self, staged: Vec<StagedChunk>, _intent: PutIntent) -> Result<Vec<StoredChunk>> {
        if staged.is_empty() {
            return Ok(Vec::new());
        }
        if staged.len() > MAX_BATCH {
            return Err(invalid("S3 batch allows at most 128 chunks"));
        }
        for value in &staged {
            if value.chunk.is_segment_crc32c() {
                return Err(Error::coded(
                    afs_error::IO_NOT_SUPPORTED,
                    "CRC32C external object writes are unsupported",
                ));
            }
            validate_chunk(&value.chunk)?;
            if !value.has_sealed_identity() {
                return Err(invalid("staged Chunk metadata changed after sealing"));
            }
            if let Some(cache) = &self.cache {
                cache.schedule_from_write(&value.chunk, value.bytes());
            }
        }
        match self.request(Request::Put(staged))? {
            Reply::Put(receipts) => Ok(receipts.into_iter().map(StoredChunk::unreleased).collect()),
            _ => Err(invalid("S3 worker returned the wrong response")),
        }
    }
}

#[cfg(test)]
impl S3ChunkStore {
    pub(super) fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize> {
        let chunk = chunk_from_id(chunk_id)?;
        if offset >= chunk.length || out.is_empty() {
            return Ok(0);
        }
        let verified = self.read_verified(&chunk)?;
        let bytes = verified.as_slice();
        let start = usize::try_from(offset).map_err(|_| invalid("chunk offset is too large"))?;
        let length = out.len().min(bytes.len() - start);
        out[..length].copy_from_slice(&bytes[start..start + length]);
        Ok(length)
    }
}

impl S3ChunkStore {
    fn validate_source(&self, chunk: &ChunkObject, source: &SourceCandidate) -> Result<()> {
        let expected =
            external_object_key(&self.options.store_id, &self.options.generation, &chunk.id)?;
        if source.chunk_id != chunk.id
            || source.role != CopyRole::ExternalCommitted
            || source.state != CopyState::Ready
            || !matches!(&source.location, CopyLocation::External { store_id, object_key, object_revision }
                if store_id == &self.options.store_id && object_key == &expected
                    && object_revision == &self.options.generation)
        {
            return Err(invalid(
                "external source does not match the configured immutable object",
            ));
        }
        Ok(())
    }
}

impl ExternalChunkReader for S3ChunkStore {
    fn read_chunk(
        &self,
        chunk: &ChunkObject,
        source: &SourceCandidate,
    ) -> Result<VerifiedExternalChunk> {
        self.validate_source(chunk, source)?;
        self.read_verified(chunk)
    }

    fn read_chunk_ranges(
        &self,
        chunk: &ChunkObject,
        source: &SourceCandidate,
        ranges: &[ExternalRangeCopy],
        out: &mut [u8],
    ) -> Result<()> {
        self.validate_source(chunk, source)?;
        self.read_ranges_verified(chunk, ranges, out)
    }
}
