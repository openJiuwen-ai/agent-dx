//! The object-store request worker.

use super::*;

pub(super) async fn run_requests(
    backend: Arc<dyn ObjectStore>,
    options: Arc<S3ChunkStoreOptions>,
    receiver: mpsc::Receiver<Command>,
    wake: Arc<tokio::sync::Notify>,
    verifier: Arc<S3ChunkVerifier>,
    observer: S3GetObserver,
) {
    let upload_slots = Arc::new(tokio::sync::Semaphore::new(options.upload_concurrency));
    // One task per request: the runtime's worker threads run them in parallel.
    let mut active = tokio::task::JoinSet::new();
    let mut closed = false;
    loop {
        while !closed && active.len() < MAX_ACTIVE_REQUESTS {
            match receiver.try_recv() {
                Ok(command) => {
                    active.spawn(serve(
                        command,
                        backend.clone(),
                        options.clone(),
                        upload_slots.clone(),
                        verifier.clone(),
                        observer.clone(),
                    ));
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => closed = true,
            }
        }
        if active.is_empty() {
            if closed {
                break;
            }
            wake.notified().await;
            continue;
        }
        // A request task that panicked dropped its reply sender; its caller
        // sees a stopped worker.
        if !closed && active.len() < MAX_ACTIVE_REQUESTS {
            tokio::select! {
                _ = active.join_next() => {},
                _ = wake.notified() => {},
            }
        } else {
            let _ = active.join_next().await;
        }
    }
}

async fn serve(
    command: Command,
    backend: Arc<dyn ObjectStore>,
    options: Arc<S3ChunkStoreOptions>,
    upload_slots: Arc<tokio::sync::Semaphore>,
    verifier: Arc<S3ChunkVerifier>,
    observer: S3GetObserver,
) {
    let is_read = matches!(&command.request, Request::Read(_));
    let queue_wait_us = command.enqueued_at.elapsed().as_micros() as u64;
    command.span.record("queue_wait_us", queue_wait_us);
    let mut uploads = UploadBatch {
        tasks: tokio::task::JoinSet::new(),
        stopped: Arc::default(),
        slots: upload_slots,
    };
    let result = async {
        let result = match tokio::time::timeout(
            options.request_timeout,
            execute(
                &backend,
                &options,
                command.request,
                queue_wait_us,
                &mut uploads,
                &verifier,
                &observer,
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                if is_read {
                    observer.observe_get_failure("timeout", "timeout");
                }
                Err(Error::coded(
                    afs_error::IO_TIMEOUT,
                    "S3 operation timed out",
                ))
            }
        };
        // A failed or timed-out batch replies only after every PUT task it
        // started has been cancelled and has released its payload owner. The
        // flag goes first: a cancelled PUT frees its permit, and no waiting
        // PUT of this batch may take it.
        uploads.stopped.store(true, Ordering::Release);
        uploads.tasks.shutdown().await;
        result
    }
    .instrument(command.span)
    .await;
    let _ = command.reply.send(result);
}

/// The PUT tasks of one request and the flag that keeps its waiting PUTs
/// from starting once the batch has failed or timed out.
struct UploadBatch {
    tasks: tokio::task::JoinSet<Result<(usize, ChunkReceipt)>>,
    stopped: Arc<AtomicBool>,
    /// The store-wide upload permits.
    slots: Arc<tokio::sync::Semaphore>,
}

/// Every chunk of the batch is its own task, admitted by the shared upload
/// permits. The first failure fails the batch: PUTs still waiting for a permit
/// never start, and the caller cancels those in flight.
async fn put_batch(
    backend: &Arc<dyn ObjectStore>,
    options: &Arc<S3ChunkStoreOptions>,
    staged: Vec<StagedChunk>,
    uploads: &mut UploadBatch,
    observer: &S3GetObserver,
) -> Result<Reply> {
    let mut receipts = vec![None; staged.len()];
    for (index, value) in staged.into_iter().enumerate() {
        let backend = backend.clone();
        let options = options.clone();
        let upload_slots = uploads.slots.clone();
        let stopped = uploads.stopped.clone();
        let observer = observer.clone();
        uploads.tasks.spawn(
            async move {
                let _permit = upload_slots
                    .acquire()
                    .await
                    .map_err(|_| unavailable("S3 upload scheduler stopped"))?;
                if stopped.load(Ordering::Acquire) {
                    return Err(unavailable("S3 batch already failed"));
                }
                // The flag is set before this task's permit is released, so
                // no waiting PUT of the batch starts after a failure.
                put_one(&*backend, &options, value, &observer)
                    .await
                    .map(|receipt| (index, receipt))
                    .inspect_err(|_| stopped.store(true, Ordering::Release))
            }
            .in_current_span(),
        );
    }
    while let Some(joined) = uploads.tasks.join_next().await {
        let (index, receipt) = joined.map_err(|_| unavailable("S3 PUT task stopped"))??;
        receipts[index] = Some(receipt);
    }
    let receipts = receipts
        .into_iter()
        .map(|receipt| receipt.ok_or_else(|| unavailable("S3 PUT receipt was not produced")))
        .collect::<Result<Vec<_>>>()?;
    Ok(Reply::Put(receipts))
}

async fn execute(
    backend: &Arc<dyn ObjectStore>,
    options: &Arc<S3ChunkStoreOptions>,
    request: Request,
    queue_wait_us: u64,
    uploads: &mut UploadBatch,
    verifier: &S3ChunkVerifier,
    observer: &S3GetObserver,
) -> Result<Reply> {
    let (operation, chunks, bytes) = request_summary(&request);
    let span = tracing::info_span!(
        "node.dfs.s3.execute",
        operation,
        chunks,
        bytes,
        queue_wait_us,
        max_uploads = options.upload_concurrency,
    );
    async move {
        match request {
            Request::Put(staged) => put_batch(backend, options, staged, uploads, observer).await,
            Request::Delete(chunks) => delete_objects(&**backend, options, chunks).await,
            Request::List { after, limit } => list_objects(&**backend, options, after, limit).await,
            Request::ReadRanges {
                chunk,
                ranges,
                sidecar,
            } => read_ranges(&**backend, options, &chunk, ranges, sidecar).await,
            Request::Read(chunk) => {
                let key = external_object_key(&options.store_id, &options.generation, &chunk.id)?;
                // No HEAD, versionId or conditional GET in the normal data path.
                let headers_started = std::time::Instant::now();
                let response = async {
                    backend
                        .get(&ObjectPath::from(key.as_str()))
                        .await
                        .map_err(storage_error)
                }
                .instrument(tracing::info_span!(
                    "node.dfs.s3.get",
                    chunk_id = %chunk.id.0,
                    chunk_bytes = chunk.length,
                ))
                .await
                .inspect_err(|_| observer.observe_get_failure("headers", "error"))?;
                let headers_us = headers_started.elapsed().as_micros() as u64;
                if response.meta.size != chunk.length || response.range != (0..chunk.length) {
                    observer.observe_get_failure("metadata", "corrupt");
                    return Err(corrupt(
                        "S3 object response length does not match Chunk metadata",
                    ));
                }
                let limit =
                    usize::try_from(chunk.length).map_err(|_| invalid("chunk is too large"))?;
                let (bytes, body_timing) = collect_s3_body(response, limit)
                    .instrument(tracing::info_span!(
                        "node.dfs.s3.get_body",
                        chunk_id = %chunk.id.0,
                        chunk_bytes = chunk.length,
                        copied_bytes = tracing::field::Empty,
                        first_poll_wait_us = tracing::field::Empty,
                        poll_wait_total_us = tracing::field::Empty,
                        poll_wait_max_us = tracing::field::Empty,
                        copy_us = tracing::field::Empty,
                        final_eof_wait_us = tracing::field::Empty,
                    ))
                    .await
                    .inspect_err(|error| {
                        let result = if error.code() == afs_error::NODE_TRANSFER_CORRUPT_DATA {
                            "corrupt"
                        } else {
                            "error"
                        };
                        observer.observe_get_failure("body", result);
                    })?;
                let verify_started = std::time::Instant::now();
                let verified = verifier.verify_get(&chunk, bytes).inspect_err(|error| {
                    let result = if error.code() == afs_error::NODE_TRANSFER_CORRUPT_DATA {
                        "corrupt"
                    } else {
                        "error"
                    };
                    observer.observe_get_failure("verify", result);
                })?;
                let timing = S3ReadTiming {
                    queue_wait_us,
                    headers_us,
                    body: body_timing,
                    verify_us: verify_started.elapsed().as_micros() as u64,
                };
                observer.observe_get(&chunk, &timing);
                Ok(Reply::Read(verified))
            }
        }
    }
    .instrument(span)
    .await
}

/// Fetches the planned ranges and, when asked, the sidecar object, all
/// concurrently. Nothing here is verified: every range response must be
/// exactly the bytes requested of an object of the chunk's length, and the
/// caller proves the content.
async fn read_ranges(
    backend: &dyn ObjectStore,
    options: &S3ChunkStoreOptions,
    chunk: &ChunkObject,
    ranges: Vec<std::ops::Range<u64>>,
    sidecar: bool,
) -> Result<Reply> {
    let key = external_object_key(&options.store_id, &options.generation, &chunk.id)?;
    let path = ObjectPath::from(key.as_str());
    let parts = futures_util::future::try_join_all(ranges.into_iter().map(|range| {
        let path = &path;
        async move {
            let response = backend
                .get_opts(
                    path,
                    object_store::GetOptions {
                        range: Some(object_store::GetRange::Bounded(range.clone())),
                        ..Default::default()
                    },
                )
                .await
                .map_err(storage_error)?;
            if response.meta.size != chunk.length || response.range != range {
                return Err(corrupt(
                    "S3 range response does not match the requested Chunk range",
                ));
            }
            let length = usize::try_from(range.end - range.start)
                .map_err(|_| invalid("chunk range is too large"))?;
            let (bytes, _) = collect_s3_body(response, length).await?;
            Ok((range, bytes))
        }
    }));
    let sidecar = async {
        if !sidecar {
            return Ok(None);
        }
        let key = external_outboard_key(&options.store_id, &options.generation, &chunk.id)?;
        let response = match backend.get(&ObjectPath::from(key.as_str())).await {
            Ok(response) => response,
            Err(object_store::Error::NotFound { .. }) => return Ok(Some(SidecarFetch::Missing)),
            Err(error) => return Err(storage_error(error)),
        };
        // An object that cannot be a sidecar is handed on empty: it fails
        // validation and the chunk is read whole.
        let size = usize::try_from(response.meta.size).unwrap_or(usize::MAX);
        if size > crate::node::chunk::outboard::MAX_ENCODED_OUTBOARD_BYTES
            || response.range != (0..response.meta.size)
        {
            return Ok(Some(SidecarFetch::Found(Vec::new())));
        }
        let (bytes, _) = collect_s3_body(response, size).await?;
        Ok(Some(SidecarFetch::Found(bytes)))
    };
    let (parts, sidecar) = futures_util::future::try_join(parts, sidecar)
        .instrument(tracing::info_span!(
            "node.dfs.s3.get_ranges",
            chunk_id = %chunk.id.0,
            chunk_bytes = chunk.length,
        ))
        .await?;
    Ok(Reply::Ranges { parts, sidecar })
}

pub(super) async fn collect_s3_body(
    response: object_store::GetResult,
    limit: usize,
) -> Result<(Vec<u8>, S3BodyTiming)> {
    let mut bytes = Vec::with_capacity(limit);
    let mut first_poll_wait = Duration::ZERO;
    let mut poll_wait_total = Duration::ZERO;
    let mut poll_wait_max = Duration::ZERO;
    let mut copy_total = Duration::ZERO;
    let mut final_eof_wait = Duration::ZERO;
    let mut stream = response.into_stream();
    let mut first_part = true;
    loop {
        let awaiting_eof = bytes.len() == limit;
        let poll_started = std::time::Instant::now();
        let next = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        let poll_wait = poll_started.elapsed();
        if awaiting_eof {
            // Empty transport frames after the exact payload are legal. Their
            // waits, together with terminal None, belong to EOF completion.
            final_eof_wait = final_eof_wait.saturating_add(poll_wait);
        } else {
            poll_wait_total = poll_wait_total.saturating_add(poll_wait);
            poll_wait_max = poll_wait_max.max(poll_wait);
            if first_part {
                first_poll_wait = poll_wait;
                first_part = false;
            }
        }
        match next {
            Some(part) => {
                let part = part.map_err(storage_error)?;
                if part.len() > limit.saturating_sub(bytes.len()) {
                    return Err(corrupt(
                        "S3 response body exceeds the immutable Chunk length",
                    ));
                }
                if !part.is_empty() {
                    let copy_started = std::time::Instant::now();
                    bytes.extend_from_slice(&part);
                    copy_total = copy_total.saturating_add(copy_started.elapsed());
                }
            }
            None => {
                if bytes.len() != limit {
                    return Err(corrupt(
                        "S3 response body ended before immutable Chunk length",
                    ));
                }
                break;
            }
        }
    }
    let timing = S3BodyTiming {
        first_poll_wait_us: micros_u64(first_poll_wait),
        poll_wait_total_us: micros_u64(poll_wait_total),
        poll_wait_max_us: micros_u64(poll_wait_max),
        copy_us: micros_u64(copy_total),
        final_eof_wait_us: micros_u64(final_eof_wait),
        copied_bytes: bytes.len() as u64,
    };
    tracing::Span::current().record("copied_bytes", timing.copied_bytes);
    tracing::Span::current().record("first_poll_wait_us", timing.first_poll_wait_us);
    tracing::Span::current().record("poll_wait_total_us", timing.poll_wait_total_us);
    tracing::Span::current().record("poll_wait_max_us", timing.poll_wait_max_us);
    tracing::Span::current().record("copy_us", timing.copy_us);
    tracing::Span::current().record("final_eof_wait_us", timing.final_eof_wait_us);
    Ok((bytes, timing))
}

/// Batched delete of chunk objects, then of their sidecar objects: a sidecar
/// is never deleted while its chunk object may still exist. An object that
/// is already gone counts as deleted, so a retried batch is idempotent; any
/// other failure fails the batch, leaving at most sidecars the next pass (or
/// the next listing) deletes.
async fn delete_objects(
    backend: &dyn ObjectStore,
    options: &S3ChunkStoreOptions,
    chunks: Vec<ChunkId>,
) -> Result<Reply> {
    type KeyOf = fn(&str, &str, &ChunkId) -> Result<String>;
    for key_of in [external_object_key as KeyOf, external_outboard_key as KeyOf] {
        let paths = chunks
            .iter()
            .map(|chunk| {
                key_of(&options.store_id, &options.generation, chunk)
                    .map(|key| Ok(ObjectPath::from(key.as_str())))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut results = backend.delete_stream(Box::pin(tokio_stream::iter(paths)));
        while let Some(result) = tokio_stream::StreamExt::next(&mut results).await {
            match result {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(storage_error(error)),
            }
        }
    }
    Ok(Reply::Deleted)
}

/// One page of this store's chunk keys in key order, after `after`.
async fn list_objects(
    backend: &dyn ObjectStore,
    options: &S3ChunkStoreOptions,
    after: Option<String>,
    limit: usize,
) -> Result<Reply> {
    let prefix = ObjectPath::from(external_chunk_prefix(
        &options.store_id,
        &options.generation,
    )?);
    let mut listed = match &after {
        Some(after) => backend.list_with_offset(Some(&prefix), &ObjectPath::from(after.as_str())),
        None => backend.list(Some(&prefix)),
    };
    let mut keys = Vec::new();
    while keys.len() < limit {
        match tokio_stream::StreamExt::next(&mut listed).await {
            Some(meta) => keys.push(meta.map_err(storage_error)?.location.to_string()),
            None => break,
        }
    }
    Ok(Reply::Listed(keys))
}

async fn put_one(
    backend: &dyn ObjectStore,
    options: &S3ChunkStoreOptions,
    value: StagedChunk,
    observer: &S3GetObserver,
) -> Result<ChunkReceipt> {
    let key = external_object_key(&options.store_id, &options.generation, &value.chunk.id)?;
    let payload = upload_payload(&value)?;
    let sidecar = sidecar_payload(options, &value)?;
    // Ordinary PUTs only: both keys are content-bound and immutable. The
    // sidecar goes out beside the chunk object, and the receipt waits for
    // both, so a readable chunk's sidecar is either complete or absent.
    let path = ObjectPath::from(key.as_str());
    let (stored, sidecar_stored) = async {
        tokio::join!(backend.put(&path, payload), async {
            match sidecar {
                Some((key, payload)) => {
                    let path = ObjectPath::from(key.as_str());
                    Some(backend.put(&path, payload).await)
                }
                None => None,
            }
        })
    }
    .instrument(tracing::info_span!(
        "node.dfs.s3.put",
        operation_id = %value.operation_id.0,
        chunk_id = %value.chunk.id.0,
        chunk_bytes = value.chunk.length,
        copied_bytes = 0_u64,
    ))
    .await;
    // A failed chunk PUT fails the write; a sidecar it leaves behind is
    // collected through the listing. A failed sidecar PUT only costs later
    // readers the range path: the chunk itself is durable.
    stored.map_err(storage_error)?;
    match sidecar_stored {
        Some(Ok(_)) => observer.observe_sidecar("put_ok"),
        Some(Err(_)) => observer.observe_sidecar("put_failed"),
        None => {}
    }
    Ok(ChunkReceipt {
        operation_id: value.operation_id,
        chunk: value.chunk.clone(),
        placement_revision: 0,
        placement_epoch: 0,
        replica_group_id: ReplicaGroupId::new(""),
        durable_acks: Vec::new(),
        external_commit: Some(ExternalCommitReceipt {
            store_id: options.store_id.clone(),
            generation: options.generation.clone(),
            object_key: key,
            persisted_bytes: value.chunk.length,
            verified_digest: value.chunk.content_digest,
        }),
    })
}

/// The sidecar object of a chunk large enough to be read by range, when
/// sealing built its outboard.
fn sidecar_payload(
    options: &S3ChunkStoreOptions,
    value: &StagedChunk,
) -> Result<Option<(String, object_store::PutPayload)>> {
    let Some(outboard) = value
        .outboard()
        .filter(|_| value.chunk.length >= SIDECAR_MIN_CHUNK_BYTES)
        .filter(|outboard| validate_sidecar(&value.chunk, outboard.encoded()).is_ok())
    else {
        return Ok(None);
    };
    let key = external_outboard_key(&options.store_id, &options.generation, &value.chunk.id)?;
    Ok(Some((
        key,
        object_store::PutPayload::from(outboard.encoded().to_vec()),
    )))
}

fn upload_payload(value: &StagedChunk) -> Result<object_store::PutPayload> {
    if !value.has_sealed_identity() {
        return Err(invalid("staged Chunk metadata changed after sealing"));
    }
    // Bytes pins both the immutable allocation and its typed write leases.
    // Retry/transport clones may outlive the request future or caller ticket;
    // the budget releases only when their last owner is actually dropped.
    Ok(object_store::PutPayload::from_bytes(Bytes::from_owner(
        value.clone(),
    )))
}
