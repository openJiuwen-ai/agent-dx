//! The object-store request worker.

use super::*;

pub(super) async fn run_requests(
    backend: Arc<dyn ObjectStore>,
    options: S3ChunkStoreOptions,
    receiver: mpsc::Receiver<Command>,
    wake: Arc<tokio::sync::Notify>,
    verifier: Arc<S3ChunkVerifier>,
    observer: S3GetObserver,
) {
    let upload_slots = Arc::new(tokio::sync::Semaphore::new(MAX_UPLOADS));
    let mut active: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let mut closed = false;
    loop {
        while !closed && active.len() < MAX_ACTIVE_REQUESTS {
            match receiver.try_recv() {
                Ok(command) => {
                    let backend = backend.clone();
                    let options = options.clone();
                    let upload_slots = upload_slots.clone();
                    let verifier = verifier.clone();
                    let observer = observer.clone();
                    active.push(Box::pin(async move {
                        let is_read = matches!(&command.request, Request::Read(_));
                        let queue_wait_us = command.enqueued_at.elapsed().as_micros() as u64;
                        command.span.record("queue_wait_us", queue_wait_us);
                        let result = async {
                            match tokio::time::timeout(
                                options.request_timeout,
                                execute(
                                    &*backend,
                                    &options,
                                    command.request,
                                    queue_wait_us,
                                    &upload_slots,
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
                            }
                        }
                        .instrument(command.span)
                        .await;
                        let _ = command.reply.send(result);
                    }));
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
        let can_receive = !closed && active.len() < MAX_ACTIVE_REQUESTS;
        let progress = poll_fn(|cx| {
            for index in 0..active.len() {
                if active[index].as_mut().poll(cx).is_ready() {
                    drop(active.swap_remove(index));
                    return Poll::Ready(());
                }
            }
            Poll::Pending
        });
        if can_receive {
            tokio::select! {
                _ = progress => {},
                _ = wake.notified() => {},
            }
        } else {
            progress.await;
        }
    }
}

async fn execute(
    backend: &dyn ObjectStore,
    options: &S3ChunkStoreOptions,
    request: Request,
    queue_wait_us: u64,
    upload_slots: &tokio::sync::Semaphore,
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
        max_uploads = MAX_UPLOADS,
    );
    async move {
        match request {
            Request::Put(staged) => {
                let mut receipts = vec![None; staged.len()];
                let mut staged = staged.into_iter().enumerate();
                type UploadFuture<'a> =
                    Pin<Box<dyn Future<Output = Result<(usize, ChunkReceipt)>> + 'a>>;
                let mut active: Vec<UploadFuture<'_>> = Vec::new();
                loop {
                    while active.len() < MAX_UPLOADS {
                        let Some((index, value)) = staged.next() else {
                            break;
                        };
                        active.push(Box::pin(async move {
                            let _permit = upload_slots
                                .acquire()
                                .await
                                .map_err(|_| unavailable("S3 upload scheduler stopped"))?;
                            put_one(backend, options, value)
                                .await
                                .map(|receipt| (index, receipt))
                        }));
                    }
                    if active.is_empty() {
                        break;
                    }
                    let (index, receipt) = poll_fn(|cx| {
                        let mut current = 0usize;
                        while current < active.len() {
                            match active[current].as_mut().poll(cx) {
                                Poll::Ready(result) => {
                                    drop(active.swap_remove(current));
                                    return Poll::Ready(result);
                                }
                                Poll::Pending => {
                                    current += 1;
                                }
                            }
                        }
                        Poll::Pending
                    })
                    .await?;
                    receipts[index] = Some(receipt);
                }
                let receipts = receipts
                    .into_iter()
                    .map(|receipt| {
                        receipt.ok_or_else(|| unavailable("S3 PUT receipt was not produced"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Reply::Put(receipts))
            }
            Request::Delete(chunks) => delete_objects(backend, options, chunks).await,
            Request::List { after, limit } => list_objects(backend, options, after, limit).await,
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

/// Batched delete. An object that is already gone counts as deleted, so a
/// retried batch is idempotent; any other failure fails the batch.
async fn delete_objects(
    backend: &dyn ObjectStore,
    options: &S3ChunkStoreOptions,
    chunks: Vec<ChunkId>,
) -> Result<Reply> {
    let paths = chunks
        .iter()
        .map(|chunk| {
            external_object_key(&options.store_id, &options.generation, chunk)
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
) -> Result<ChunkReceipt> {
    let key = external_object_key(&options.store_id, &options.generation, &value.chunk.id)?;
    let payload = upload_payload(&value)?;
    // Ordinary PUT only: key identity is content-bound and immutable.
    async {
        backend
            .put(&ObjectPath::from(key.as_str()), payload)
            .await
            .map_err(storage_error)
    }
    .instrument(tracing::info_span!(
        "node.dfs.s3.put",
        operation_id = %value.operation_id.0,
        chunk_id = %value.chunk.id.0,
        chunk_bytes = value.chunk.length,
        copied_bytes = 0_u64,
    ))
    .await?;
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
