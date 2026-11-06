//! S3ChunkStore: the sink and reader facade over the request worker.

use super::*;

/// One owned asynchronous worker serves every PUT request.
/// All requests share four upload permits; completing one PUT immediately
/// makes its permit available to the next chunk, including another batch.
/// The bounded queue applies backpressure rather than reporting a storage
/// failure. At most four PUTs hold immutable payload owners; typed leases stay
/// charged until the last transport/retry owner exits. Producer payload and
/// descriptor reservations belong to the DFS write pipeline.
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
            .with_pool_max_idle_per_host(MAX_UPLOADS);
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
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(Error::from)?;
        let (sender, receiver) = mpsc::sync_channel::<Command>(1);
        let identity = options.clone();
        let wake = Arc::new(tokio::sync::Notify::new());
        let worker_wake = wake.clone();
        let worker_verifier = verifier.clone();
        let worker_observer = observer.clone();
        let worker = thread::Builder::new()
            .name("afs-s3-chunks".into())
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
            max_uploads = MAX_UPLOADS,
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

    /// One page of this store's objects after `after`, as (chunk, key). A key
    /// that does not name a generational chunk is not ours to judge and is
    /// skipped. An empty page means the listing reached the end.
    pub fn list_objects(
        &self,
        after: Option<String>,
        limit: usize,
    ) -> Result<(Vec<ChunkObject>, Option<String>)> {
        let keys = match self.request(Request::List { after, limit })? {
            Reply::Listed(keys) => keys,
            _ => return Err(invalid("S3 worker returned the wrong response")),
        };
        let chunks = keys
            .iter()
            .filter_map(|key| {
                let id = ChunkId::new(key.rsplit('/').next()?);
                ChunkObject::from_content_addressed_id(&id)
                    .filter(ChunkObject::is_generational_blake3)
            })
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

impl ExternalChunkReader for S3ChunkStore {
    fn read_chunk(
        &self,
        chunk: &ChunkObject,
        source: &SourceCandidate,
    ) -> Result<VerifiedExternalChunk> {
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
        self.read_verified(chunk)
    }
}
