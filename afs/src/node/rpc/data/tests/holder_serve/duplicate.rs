use super::*;

const PART: usize = crate::node::chunk_crc32c::SEGMENT;

#[derive(Clone, Copy, Debug)]
enum Loss {
    Displaced,
    Expired,
    Budget,
    Disk,
}

async fn reconcile_group(loss: Loss) {
    use dfs_read_ranges_frame::Body;

    let timeout = std::time::Duration::from_secs(10);
    tokio::time::timeout(timeout, async {
        let first = review_chunk(41);
        let second =
            StagedChunk::new_crc32c(crate::dfs::OperationId::new("second"), vec![19; 4 << 20])
                .unwrap();
        let incoming = StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("displacing"),
            vec![93; 4 << 20],
        )
        .unwrap();
        let chunks = [first, second];
        let size = chunks.iter().map(|c| c.chunk.length).sum();
        let mut h = partial_holder(&chunks, size);
        let engine = checkpoint_engine_with_partial_budget(
            h.local.clone(),
            NamespaceId::new("namespace"),
            "holder",
            9,
            4 << 20,
        );
        h.in_flight = engine.holder_in_flight().unwrap();
        h.service.holders = Some(crate::node::rpc::data::HolderServe::new(
            h.meta.clone(),
            h.registry.clone(),
            h.in_flight.clone(),
            "holder".into(),
            9,
        ));
        let metrics = DfsPayloadMetrics::register(&h.metrics).unwrap();
        h.service.payload_metrics = Some(metrics.clone());
        let stage = |chunk: &StagedChunk| {
            engine.checkpoint_stage(
                &chunk.chunk,
                &h.version,
                &h.layout.id,
                chunk.bytes(),
                0,
                PART,
            );
            engine.checkpoint_assemble();
        };
        let _first = if matches!(loss, Loss::Disk) {
            for chunk in &chunks {
                h.local.put(chunk.clone()).unwrap();
            }
            None
        } else {
            stage(&chunks[1]);
            Some(partial_body(&h, &chunks[0], chunks[0].bytes().len()))
        };

        let writer_local =
            Arc::new(LocalChunkStore::open(h._temp.path().join("durable"), "writer").unwrap());
        for chunk in &chunks {
            writer_local.put(chunk.clone()).unwrap();
        }
        let mut writer = dfs_reuse_test_service(writer_local, Arc::new(AllowDfsTestGrant));
        let writer_metrics = DfsPayloadMetrics::register(&afs_metrics::Registry::new()).unwrap();
        writer.payload_metrics = Some(writer_metrics.clone());

        let ops = chunks
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let mut op = holder_op(i as u32, &c.chunk.id.0, 0, PART as u64, &grant("v", "l"));
                op.destination_offset = (i * PART) as u64;
                op
            })
            .collect();
        let (paused, resume) = h.local.pause_after_holder_op();
        let mut stream = h
            .service
            .read_ranges(Request::new(holder_request(ops)))
            .await
            .unwrap()
            .into_inner();
        let mut received = Vec::new();
        loop {
            match stream.next().await.unwrap().unwrap().body.unwrap() {
                Body::Data(data) => received.extend_from_slice(&data),
                Body::Completion(_) => break,
                _ => {}
            }
        }
        assert_eq!(received, chunks[0].bytes()[..PART]);
        paused.await.unwrap();
        let _held = match loss {
            Loss::Displaced => {
                stage(&incoming);
                assert_eq!(cache_event_count(&h.metrics, "dropped_partial"), 1);
                None
            }
            Loss::Expired => {
                engine.checkpoint_expire_partials();
                assert_eq!(cache_event_count(&h.metrics, "dropped_partial_expired"), 1);
                None
            }
            Loss::Budget => Some(h.local.occupy_peer_payload_budget().unwrap()),
            Loss::Disk => {
                assert_eq!(
                    h.local
                        .collect_generation(1, std::slice::from_ref(&chunks[1].chunk.id))
                        .unwrap(),
                    1
                );
                None
            }
        };
        resume.send(()).unwrap();
        let mut retried = false;
        while let Some(frame) = stream.next().await {
            match frame {
                Ok(frame) => {
                    if let Some(Body::Data(data)) = frame.body {
                        received.extend_from_slice(&data);
                    }
                }
                Err(status) => {
                    not_ready(Err(status));
                    retried = true;
                    // The current ReadRanges consumer retries the entire group on NOT_READY.
                    // The durable service records the bytes it actually enqueues.
                    received.clear();
                    for chunk in &chunks {
                        received.extend(
                            read(
                                &writer,
                                dfs_read_request(chunk.chunk.id.0.clone(), PART as u64),
                            )
                            .await
                            .unwrap(),
                        );
                    }
                    break;
                }
            }
        }
        let expected: Vec<_> = chunks
            .iter()
            .flat_map(|c| c.bytes()[..PART].iter().copied())
            .collect();
        assert_eq!(received, expected);
        let sent =
            |m: &DfsPayloadMetrics| m.bytes.with_label_values(&["grpc", "send", "read"]).get();
        let total = sent(&metrics) + sent(&writer_metrics);
        eprintln!(
            "loss={loss:?} holder={} durable={} received={} duplicate={} retried={retried}",
            sent(&metrics),
            sent(&writer_metrics),
            received.len(),
            total as i64 - received.len() as i64
        );
        assert_eq!(
            total,
            received.len() as u64,
            "duplicate payload after {loss:?}"
        );
    })
    .await
    .expect("group reconciliation deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_2c1_dup_displaced_partial() {
    reconcile_group(Loss::Displaced).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_2c1_dup_expired_partial() {
    reconcile_group(Loss::Expired).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_2c1_dup_payload_budget() {
    reconcile_group(Loss::Budget).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_2c1_dup_disk_eviction() {
    reconcile_group(Loss::Disk).await;
}

#[tokio::test(start_paused = true)]
async fn h2_2c1_dup_partial_backpressure_after_admission_keeps_serving() {
    use dfs_read_ranges_frame::Body;
    let chunk = StagedChunk::new_crc32c(
        crate::dfs::OperationId::new("backpressure"),
        vec![31; 1 << 20],
    )
    .unwrap();
    let mut h = partial_holder(std::slice::from_ref(&chunk), chunk.chunk.length);
    let _body = partial_body(&h, &chunk, chunk.bytes().len());
    let metrics = DfsPayloadMetrics::register(&h.metrics).unwrap();
    h.service.payload_metrics = Some(metrics.clone());
    let op = holder_op(
        0,
        &chunk.chunk.id.0,
        0,
        chunk.chunk.length,
        &grant("v", "l"),
    );
    let mut stream = h
        .service
        .read_ranges(Request::new(holder_request(vec![op])))
        .await
        .unwrap()
        .into_inner();
    // A header and three data frames fill the four-entry response queue.
    let sent = metrics.bytes.with_label_values(&["grpc", "send", "read"]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while sent.get() < 3 * DFS_REPLICA_FRAME_BYTES as u64 {
        assert!(
            std::time::Instant::now() < deadline,
            "response queue did not fill"
        );
        tokio::task::yield_now().await;
    }
    tokio::time::advance(std::time::Duration::from_millis(80)).await;
    tokio::task::yield_now().await;
    let mut received = Vec::new();
    let mut complete = false;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Some(frame) = stream.next().await {
            match frame.unwrap().body.unwrap() {
                Body::Data(data) => received.extend_from_slice(&data),
                Body::Completion(_) => complete = true,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert!(
        complete,
        "admitted response stopped after {} of {} bytes",
        received.len(),
        chunk.bytes().len()
    );
    assert_eq!(received, chunk.bytes());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_2c1_dup_disconnected_reader_counts_the_sent_prefix() {
    let chunks = [review_chunk(77), review_chunk(78)];
    let mut h = partial_holder(&chunks, chunks.iter().map(|c| c.chunk.length).sum());
    let _bodies = chunks
        .each_ref()
        .map(|c| partial_body(&h, c, c.bytes().len()));
    let metrics = DfsPayloadMetrics::register(&h.metrics).unwrap();
    h.service.payload_metrics = Some(metrics);
    let ops = chunks
        .iter()
        .enumerate()
        .map(|(i, c)| holder_op(i as u32, &c.chunk.id.0, 0, PART as u64, &grant("v", "l")))
        .collect();
    let (at_first, resume) = h.local.pause_after_holder_op();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let stream = h
            .service
            .read_ranges(Request::new(holder_request(ops)))
            .await
            .unwrap()
            .into_inner();
        at_first.await.unwrap();
        drop(stream);
        resume.send(()).unwrap();
        while h.service.read_permits.available_permits() != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        cache_event_count(&h.metrics, "holder_abandoned_after_send"),
        1
    );
    assert_eq!(
        metric_value(
            &h.metrics,
            "afs_dfs_holder_abandoned_after_send_bytes_total "
        ),
        PART as u64
    );
}

/// Only CRC Chunks get native holder candidates. Exercise an actual one-operation
/// reader and slow the transport's consumption after admission, keeping the grant live.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h2_2c1_dup_native_crc_reader_under_response_backpressure() {
    let timeout = std::time::Duration::from_secs(4);
    tokio::time::timeout(timeout, async {
        let temp = tempfile::tempdir().unwrap();
        let chunk = crc_chunk_designating_holder("slow-transport", vec![37; 1 << 20], &["holder"]);
        let chunks = std::slice::from_ref(&chunk);
        let mut h = partial_holder(chunks, chunk.chunk.length);
        let _body = partial_body(&h, &chunk, chunk.bytes().len());
        let meter = DfsPayloadMetrics::register(&h.metrics).unwrap();
        h.service.payload_metrics = Some(meter.clone());
        let (reached, at_poll) = tokio::sync::oneshot::channel();
        let (resume, blocked) = std::sync::mpsc::channel();
        holder_serve(&h).on(
            crate::node::rpc::data::HolderTestPoint::ResponsePoll,
            move || {
                let _ = reached.send(());
                blocked
                    .recv_timeout(timeout)
                    .expect("transport gate timed out");
            },
        );
        let (endpoint, server) = serve(h.service.clone()).await;
        let reader = review_reader(temp.path(), chunks, &endpoint, 0).await;
        let hold_queue = async {
            at_poll.await.unwrap();
            let sent = meter.bytes.with_label_values(&["grpc", "send", "read"]);
            until("bounded holder response queue to fill", || {
                sent.get() >= 4 * DFS_REPLICA_FRAME_BYTES as u64
            })
            .await;
            // The admission deadline was taken before this poll. Keep the queue
            // full beyond even a fresh deadline, without exceeding the grant or reader budget.
            let latest = h.local.peer_serve_deadline();
            tokio::time::sleep_until((latest + std::time::Duration::from_millis(50)).into()).await;
            resume.send(()).unwrap();
        };
        let (bytes, ()) = tokio::join!(read_within(&reader, &chunk, timeout), hold_queue);
        let bytes = bytes.unwrap();
        assert_eq!(bytes, chunk.bytes());
        let sent =
            |m: &DfsPayloadMetrics| m.bytes.with_label_values(&["grpc", "send", "read"]).get();
        let holder = sent(&meter);
        let durable = sent(&reader.writer_payload);
        eprintln!(
            "native CRC reader holder={holder} durable={durable} received={} duplicate={}",
            bytes.len(),
            (holder + durable) as i64 - bytes.len() as i64
        );
        assert_eq!(holder + durable, bytes.len() as u64);
        assert_eq!(durable, 0);
        assert_eq!(cache_event_count(&reader.metrics, "holder_demoted"), 0);
        server.abort();
    })
    .await
    .expect("native CRC reader deadline");
}

#[tokio::test]
async fn h2_2c1_dup_oversized_snapshot_returns_not_ready_before_data() {
    let c = review_chunk(80);
    let h = partial_holder(std::slice::from_ref(&c), c.chunk.length);
    let _body = partial_body(&h, &c, c.bytes().len());
    let ops = (0..9)
        .map(|i| holder_op(i, &c.chunk.id.0, 0, PART as u64, &grant("v", "l")))
        .collect();
    let mut stream = h
        .service
        .read_ranges(Request::new(holder_request(ops)))
        .await
        .unwrap()
        .into_inner();
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap();
    not_ready(first.map(|_| Vec::new()));
}

#[tokio::test]
async fn h2_2c1_dup_disk_snapshot_checks_crc_before_publication() {
    use dfs_read_ranges_frame::Body;
    use std::io::{Seek, SeekFrom, Write};
    let mut corrupt_payload = 0;
    let mut rejected = 0;
    for mode in ["off", "receiver"] {
        let c = review_chunk(92);
        let h = partial_holder(std::slice::from_ref(&c), c.chunk.length);
        h.local.put(c.clone()).unwrap();
        let path = walk(h._temp.path())
            .into_iter()
            .find(|p| {
                p.file_name()
                    .is_some_and(|name| name.to_string_lossy() == c.chunk.id.0)
            })
            .unwrap();
        let on_disk = std::fs::read(&path).unwrap();
        let at = on_disk
            .windows(128)
            .position(|s| s == &c.bytes()[..128])
            .unwrap();
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(at as u64)).unwrap();
        file.write_all(&[c.bytes()[0] ^ 1]).unwrap();
        drop(file);
        let mut request = Request::new(holder_request(vec![holder_op(
            0,
            &c.chunk.id.0,
            0,
            PART as u64,
            &grant("v", "l"),
        )]));
        request
            .metadata_mut()
            .insert("x-afs-read-crc-mode", mode.parse().unwrap());
        let mut received = Vec::new();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut stream = h.service.read_ranges(request).await.unwrap().into_inner();
            while let Some(frame) = stream.next().await {
                match frame {
                    Err(status) => {
                        let error = afs_transport::grpc::error_status::status_to_error(status);
                        assert_eq!(error.kind(), afs_error::ErrorKind::DataLoss, "{error:?}");
                        rejected += 1;
                        break;
                    }
                    Ok(frame) => {
                        if let Some(Body::Data(data)) = frame.body {
                            received.extend_from_slice(&data);
                        }
                    }
                }
            }
        })
        .await
        .unwrap();
        if !received.is_empty() {
            assert_ne!(received[0], c.bytes()[0]);
            corrupt_payload += received.len();
        }
        eprintln!(
            "holder disk CRC wire_policy={mode} corrupt_payload={}",
            received.len()
        );
    }
    assert_eq!(corrupt_payload, 0, "corrupt holder bytes published");
    assert_eq!(rejected, 2);
}
