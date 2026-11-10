use super::*;

#[test]
fn ring_wait_timeout_falls_back_only_for_zero() {
    assert_eq!(ring_wait_timeout(0), DEFAULT_RING_TIMEOUT);
    assert_eq!(ring_wait_timeout(250), Duration::from_millis(250));
}

#[test]
fn version_comparison_skips_unpinned_identities() {
    // 任一侧为空都表示“未固定”，跳过比对。
    assert!(!version_changed("", ""));
    assert!(!version_changed("v1", ""));
    assert!(!version_changed("", "v1"));
    // 两侧都有版本时才比对。
    assert!(!version_changed("v1", "v1"));
    assert!(version_changed("v1", "v2"));
}

#[test]
fn completion_bytes_separates_node_errors_from_byte_counts() {
    assert_eq!(
        completion_bytes(&Completion {
            result: 7,
            ..Completion::default()
        })
        .expect("successful completion"),
        7
    );

    let node_error = completion_bytes(&Completion {
        result: -1,
        status: 0x0208_0001,
        ..Completion::default()
    })
    .expect_err("non-zero status must fail");
    assert_eq!(
        node_error.code(),
        afs_error::ErrorCode::from_raw(0x0208_0001)
    );

    let negative = completion_bytes(&Completion {
        result: -1,
        ..Completion::default()
    })
    .expect_err("negative result must fail");
    assert_eq!(negative.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
}

#[test]
fn reserved_block_is_returned_only_when_the_outcome_is_known() {
    let layout = IovLayout::new(4096, 2).expect("iov layout");
    let mut allocator = IovAllocator::new(layout);

    // 预约之后、提交之前走到作用域末尾（预留失败 / 提交前被取消）：请求从未进环，
    // 区块必须还回空闲池。
    let slice = {
        let guard = ReservedBlock::reserve(&mut allocator, 16).expect("reserve");
        guard.slice()
    };
    assert_eq!(slice.len, 16);
    assert_eq!(allocator.free_blocks(), 2);
    assert_eq!(allocator.in_flight(), 0);

    // 操作有明确结果：显式释放。
    {
        let mut guard = ReservedBlock::reserve(&mut allocator, 16).expect("reserve");
        guard.release().expect("release");
    }
    assert_eq!(allocator.free_blocks(), 2);
    assert_eq!(allocator.in_flight(), 0);

    // 一项提交入队后（哪怕还没等到结果）走作用域末尾（外层取消 future）：结果
    // 未知，Drop 不得归还，区块保持占用——这是取消安全的关键钉子。
    {
        let mut guard = ReservedBlock::reserve(&mut allocator, 16).expect("reserve");
        guard.mark_pushed();
    }
    assert_eq!(allocator.free_blocks(), 1);
    assert_eq!(allocator.in_flight(), 1);

    // 结果未知：区块保持占用，避免 node 正在读写的内存被复用。
    {
        let mut guard = ReservedBlock::reserve(&mut allocator, 16).expect("reserve");
        guard.forfeit();
    }
    assert_eq!(allocator.free_blocks(), 0);
    assert_eq!(allocator.in_flight(), 2);
}

#[test]
fn iov_and_ring_errors_carry_stable_codes() {
    assert_eq!(
        LocalClientError::from(IovError::Exhausted).code(),
        afs_error::CLIENT_SHM_UNAVAILABLE
    );
    assert_eq!(
        LocalClientError::from(IovError::SpansBlocks).code(),
        afs_error::CLIENT_ARGUMENT_INVALID
    );
    assert_eq!(
        LocalClientError::from(RingError::SubmissionFull).code(),
        afs_error::CLIENT_SHM_UNAVAILABLE
    );
    assert_eq!(
        LocalClientError::from(RingError::NotInitialized).code(),
        afs_error::CLIENT_PROTOCOL_VIOLATION
    );
}

#[test]
fn ring_io_depth_beyond_entries_is_rejected_upfront() {
    // 「必须攒满」的目标大于环项数时永远凑不满一批：这样的组合必须在创建前被拒绝。
    let layout = RingLayout::new(8).expect("ring layout");
    assert!(layout.validate_io_depth(16).is_err());
    // 边界与另外两种策略不受影响。
    assert!(layout.validate_io_depth(8).is_ok());
    assert!(layout.validate_io_depth(-16).is_ok());
    assert!(layout.validate_io_depth(0).is_ok());
}

/// 构造一对本地 iov + 环，不经过控制面：门铃与完成 eventfd 都在本进程内，
/// `client` 是从不连接的惰性 channel（dispatch 路径不碰它）。
fn local_ring_pair(entries: u32) -> (DfsIov, DfsRing) {
    local_ring_pair_with_layout(entries, IovLayout::new(4096, 4).expect("iov layout"))
}
fn local_ring_pair_with_layout(entries: u32, iov_layout: IovLayout) -> (DfsIov, DfsRing) {
    let iov_region = SharedRegion::create_with_limit(
        "afs-client-test-iov",
        iov_layout.total_bytes(),
        iov_layout.total_bytes(),
    )
    .expect("iov region");
    let iov_mapped = MappedRegion::map(
        iov_region.duplicate_fd_owned().expect("dup iov fd"),
        iov_layout.total_bytes(),
    )
    .expect("map iov");
    // SAFETY: mapping 覆盖 iov_layout.total_bytes() 字节，基址来自 MappedRegion。
    let view =
        unsafe { SharedIov::from_raw_parts(iov_mapped.as_ptr(), iov_layout) }.expect("iov view");
    view.initialize().expect("iov init");

    let ring_layout = RingLayout::new(entries).expect("ring layout");
    let ring_region = SharedRegion::create_with_limit(
        "afs-client-test-ring",
        ring_layout.total_bytes(),
        ring_layout.total_bytes(),
    )
    .expect("ring region");
    let ring_mapped = MappedRegion::map(
        ring_region.duplicate_fd_owned().expect("dup ring fd"),
        ring_layout.total_bytes(),
    )
    .expect("map ring");
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节，基址来自 MappedRegion。
    let ring = unsafe { SharedRing::from_raw_parts(ring_mapped.as_ptr(), ring_layout) }
        .expect("ring view");
    ring.initialize().expect("ring init");

    let doorbell = EventFd::create().expect("doorbell");
    let completion = EventFd::create().expect("completion");
    let ready_fd = completion.try_clone_fd().expect("dup completion");
    completion
        .set_nonblocking()
        .expect("completion nonblocking");
    let completion_ready = tokio::io::unix::AsyncFd::new(ready_fd).expect("completion async fd");
    let client = DfsLocalDataClient::new(traced_channel(
        Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
    ));
    let request_timeout = Duration::from_secs(1);
    (
        DfsIov {
            client: client.clone(),
            request_timeout,
            region: iov_mapped,
            view,
            allocator: IovAllocator::new(iov_layout),
            iov_id: 1,
        },
        DfsRing {
            client,
            request_timeout,
            region: ring_mapped,
            ring,
            ring_id: 1,
            iov_id: 1,
            wait_timeout: Duration::from_secs(1),
            next_userdata: 1,
            io_depth: 0,
            doorbell,
            completion,
            completion_ready,
        },
    )
}

#[tokio::test]
async fn batched_completion_mismatch_forfeits_the_reserved_blocks() {
    // 预埋两个陈旧完成项（userdata 999/998），再提交两项新请求（userdata 1/2）。
    // wait_for 取回的是陈旧完成项：对不上号 = 结果未知，区块必须整体 forfeit，不能走
    // Drop 的归还路径——node 仍可能对它们做后端读写，归还会让内存被下一批复用。
    let (mut iov, mut ring) = local_ring_pair(8);

    // 预埋两个陈旧完成项（userdata 999/998），模拟「上一波超时留下的完成项」：
    // CQ 的完成数不能超过已消费的提交数，因此先把旧提交推入 SQ、由 node 侧消费者
    // 完成它们；客户端不取走，留给这一批的 wait_for 先撞上。
    let stale_submissions = [
        Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id: 1,
            len: 16,
            offset: 0,
            iov_offset: 0,
            userdata: 999,
        },
        Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id: 1,
            len: 16,
            offset: 0,
            iov_offset: 0,
            userdata: 998,
        },
    ];
    let producer = ring.ring.submission_producer();
    for submission in &stale_submissions {
        producer.push(submission).expect("push stale submission");
    }
    let stale_completions = [
        Completion {
            userdata: 999,
            ..Completion::default()
        },
        Completion {
            userdata: 998,
            ..Completion::default()
        },
    ];
    ring.ring
        .ring_consumer()
        .complete_batch(&stale_completions)
        .expect("preload stale completions");

    let lengths = [16_usize, 16];
    let mut batch = ReservedBatch::reserve(&mut iov.allocator, &lengths).expect("reserve");
    let submissions: Vec<Submission> = batch
        .slices()
        .iter()
        .enumerate()
        .map(|(index, slice)| Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id: 1,
            len: slice.len,
            offset: 0,
            iov_offset: slice.iov_offset,
            userdata: index as u64 + 1,
        })
        .collect();

    let error = ring
        .dispatch_batch(&submissions, &mut batch)
        .await
        .expect_err("mismatched completions must fail");
    // batch 仍持有分配器的可变借用；先显式结清再读计数，否则通不过借用检查。
    drop(batch);
    assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
    assert_eq!(
        iov.allocator.free_blocks(),
        2,
        "forfeited blocks stay occupied"
    );
    assert_eq!(
        iov.allocator.in_flight(),
        2,
        "forfeited blocks are in flight"
    );
}

/// 提交后 future 被取消（外层 `timeout`/`select!` 丢弃）：结果未知，区块必须保持占用。
/// node 可能仍在对这些区块做后端读写（读路径往区块写、写路径从区块读），提前归还
/// 会让下一批请求复用同一块内存，造成跨请求数据串扰。
#[tokio::test]
async fn cancelled_batch_after_submit_keeps_blocks_occupied() {
    let (mut iov, mut ring) = local_ring_pair(8);
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    let payloads = [vec![1_u8; 16], vec![2_u8; 16]];
    let items: Vec<(u64, &[u8])> = payloads
        .iter()
        .map(|payload| (0_u64, payload.as_slice()))
        .collect();

    // 没有 worker 消费这条环：write_batch 只会停在等完成项上，被外层超时取消。
    // 300ms 足够提交先落地（进程内只有一次共享环写入与门铃）。
    let outcome = tokio::time::timeout(
        Duration::from_millis(300),
        ring.write_batch(&mut iov, &reg, &items),
    )
    .await;
    assert!(
        outcome.is_err(),
        "no worker exists; the outer timeout must cancel the write"
    );
    assert_eq!(
        ring.ring.ring_consumer().available(),
        2,
        "submissions are in the ring"
    );
    assert_eq!(
        iov.allocator.free_blocks(),
        2,
        "cancelled blocks stay occupied"
    );
    assert_eq!(
        iov.allocator.in_flight(),
        2,
        "cancelled blocks are in flight"
    );
}

/// 单发路径的同一切取消语义。
#[tokio::test]
async fn cancelled_single_after_submit_keeps_block_occupied() {
    let (mut iov, mut ring) = local_ring_pair(8);
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    let data = vec![7_u8; 16];

    let outcome = tokio::time::timeout(
        Duration::from_millis(300),
        ring.write_from(&mut iov, &reg, 0, &data),
    )
    .await;
    assert!(
        outcome.is_err(),
        "no worker exists; the outer timeout must cancel the write"
    );
    assert_eq!(
        ring.ring.ring_consumer().available(),
        1,
        "the submission is in the ring"
    );
    assert_eq!(
        iov.allocator.free_blocks(),
        3,
        "cancelled block stays occupied"
    );
    assert_eq!(iov.allocator.in_flight(), 1, "cancelled block is in flight");
}

/// 提交前失败仍能回收：SQ 满等不到空间时提交从未进环，区块必须归还，调用方才能重试。
#[tokio::test]
async fn submit_failure_before_entering_the_ring_recycles_the_block() {
    let (mut iov, mut ring) = local_ring_pair(2);
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    // 先用两项陈旧提交把 SQ 填满（entries = 2），submit 只能在「等空间」上超时。
    let stale = Submission {
        op: OP_WRITE,
        flags: 0,
        reg_id: 1,
        len: 16,
        offset: 0,
        iov_offset: 0,
        userdata: 900,
    };
    {
        let producer = ring.ring.submission_producer();
        producer.push(&stale).expect("push stale submission 1");
        producer.push(&stale).expect("push stale submission 2");
    }

    let data = vec![7_u8; 16];
    let error = ring
        .write_from(&mut iov, &reg, 0, &data)
        .await
        .expect_err("SQ is full; submit must time out");
    assert!(matches!(error, LocalClientError::RingTimeout));
    assert_eq!(
        ring.ring.ring_consumer().available(),
        2,
        "our submission never entered the ring"
    );
    assert_eq!(
        iov.allocator.free_blocks(),
        4,
        "pre-submit failure recycles the block"
    );
    assert_eq!(iov.allocator.in_flight(), 0);
}

/// 把 SQ 填满 `count` 个陈旧提交（无人消费），使新请求一项也进不了环。
/// userdata 从 100_000 起，远离 SDK 自己的计数，避免与完成项匹配混淆。
fn fill_submission_queue(ring: &DfsRing, count: u32) {
    let producer = ring.ring.submission_producer();
    for index in 0..count {
        producer
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id: 1,
                len: 16,
                offset: 0,
                iov_offset: 0,
                userdata: 100_000 + index as u64,
            })
            .expect("push stale submission");
    }
}

/// 批量零入队超时：SQ 满且无消费者，整批停在「等空间」上直到超时——一项也没进环，
/// 区块必须全部归还；重复同样的失败不得逐次吃掉 iov 容量。
#[tokio::test]
async fn batch_zero_push_timeout_recycles_the_blocks() {
    let (mut iov, mut ring) = local_ring_pair(8);
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    fill_submission_queue(&ring, 8);
    let payloads = [vec![1_u8; 16], vec![2_u8; 16]];
    let items: Vec<(u64, &[u8])> = payloads
        .iter()
        .map(|payload| (0_u64, payload.as_slice()))
        .collect();

    for attempt in 1..=2_u32 {
        let error = ring
            .write_batch(&mut iov, &reg, &items)
            .await
            .expect_err("SQ is full; the batch must time out");
        assert_eq!(
            error.code(),
            afs_error::CLIENT_DEADLINE_EXCEEDED,
            "attempt {attempt}"
        );
        assert_eq!(
            ring.ring.ring_consumer().available(),
            8,
            "no new submissions entered the ring (attempt {attempt})"
        );
        assert_eq!(
            iov.allocator.free_blocks(),
            4,
            "zero-push timeout must recycle the whole batch (attempt {attempt})"
        );
        assert_eq!(
            iov.allocator.in_flight(),
            0,
            "no blocks stay in flight (attempt {attempt})"
        );
    }
}

/// 批量零入队取消：SQ 满、本批停在「等空间」上时被外层丢弃，必须整批归还。
///
/// 执行进度的确定性不靠 sleep 猜测：`timeout` 被 await 时同步 poll 内层，
/// write_batch 的同步段（预留、填块、首推失败）在第一次 poll 内全部完成，
/// future 实际停在等空间的 await 上——不是「从未 poll 就丢弃」的假绿；
/// 外层 300ms 严格早于内部约 1s 的等待截止，取消必然落在等待中。
#[tokio::test]
async fn cancelled_batch_before_any_push_recycles_the_blocks() {
    let (mut iov, mut ring) = local_ring_pair(8);
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    fill_submission_queue(&ring, 8);
    let payloads = [vec![1_u8; 16], vec![2_u8; 16]];
    let items: Vec<(u64, &[u8])> = payloads
        .iter()
        .map(|payload| (0_u64, payload.as_slice()))
        .collect();

    let outcome = tokio::time::timeout(
        Duration::from_millis(300),
        ring.write_batch(&mut iov, &reg, &items),
    )
    .await;
    assert!(
        outcome.is_err(),
        "the outer timeout must cancel the batch mid-wait"
    );
    assert_eq!(
        ring.ring.ring_consumer().available(),
        8,
        "no new submissions entered the ring"
    );
    assert_eq!(
        iov.allocator.free_blocks(),
        4,
        "zero-push cancellation recycles the whole batch"
    );
    assert_eq!(iov.allocator.in_flight(), 0);
}

/// 单发零入队取消：与批量同一条「等空间」等待，取消不得永久扣住区块。
/// 确定性论证同 [`Self::cancelled_batch_before_any_push_recycles_the_blocks`]。
#[tokio::test]
async fn cancelled_single_before_any_push_recycles_the_block() {
    let (mut iov, mut ring) = local_ring_pair(8);
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    fill_submission_queue(&ring, 8);
    let data = vec![7_u8; 16];

    let outcome = tokio::time::timeout(
        Duration::from_millis(300),
        ring.write_from(&mut iov, &reg, 0, &data),
    )
    .await;
    assert!(
        outcome.is_err(),
        "the outer timeout must cancel the write mid-wait"
    );
    assert_eq!(
        ring.ring.ring_consumer().available(),
        8,
        "no new submissions entered the ring"
    );
    assert_eq!(
        iov.allocator.free_blocks(),
        4,
        "zero-push cancellation recycles the block"
    );
    assert_eq!(iov.allocator.in_flight(), 0);
}

/// 部分入队契约：SQ 只剩部分空位时，批量的前几项真实入队、其余进不去。
/// 已入队项结果未知，整批（含未入队项）保持占用——未入队区块不单独放回，
/// 避免按切片释放的复杂度把错误的区块归还空闲池。
#[tokio::test]
async fn partial_push_keeps_the_whole_batch() {
    let (mut iov, mut ring) = local_ring_pair(8);
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    // 只留 1 个空位：本批第 1 项入队、第 2 项进不去。
    fill_submission_queue(&ring, 7);
    let payloads = [vec![1_u8; 16], vec![2_u8; 16]];
    let items: Vec<(u64, &[u8])> = payloads
        .iter()
        .map(|payload| (0_u64, payload.as_slice()))
        .collect();

    let error = ring
        .write_batch(&mut iov, &reg, &items)
        .await
        .expect_err("partial acceptance must surface as an error");
    assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
    // 7 个陈旧提交 + 本批第 1 项；第 2 项从未进环。
    assert_eq!(
        ring.ring.ring_consumer().available(),
        8,
        "the first item entered, the second never did"
    );
    // 整批保持占用：已入队项结果未知；未入队项按契约一并保留。
    assert_eq!(iov.allocator.free_blocks(), 2);
    assert_eq!(iov.allocator.in_flight(), 2);
}

/// 正常完成必须归还：预埋与提交 userdata 匹配的完成项，成功返回后区块回空闲池。
#[tokio::test]
async fn matched_completion_releases_the_block() {
    let (mut iov, mut ring) = local_ring_pair(8);
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    // 载体提交让 CQ 有权收到完成项；预埋的完成项 userdata=1 与 write_from 的
    // 第一项提交（next_userdata 从 1 起）对上号。
    let carrier = Submission {
        op: OP_WRITE,
        flags: 0,
        reg_id: 1,
        len: 16,
        offset: 0,
        iov_offset: 0,
        userdata: 999,
    };
    {
        let producer = ring.ring.submission_producer();
        producer.push(&carrier).expect("push carrier submission");
    }
    ring.ring
        .ring_consumer()
        .complete_batch(&[Completion {
            userdata: 1,
            result: 16,
            ..Completion::default()
        }])
        .expect("preload matching completion");

    let data = vec![7_u8; 16];
    let written = ring
        .write_from(&mut iov, &reg, 0, &data)
        .await
        .expect("write completes against the preloaded completion");
    assert_eq!(written, 16);
    assert_eq!(
        iov.allocator.free_blocks(),
        4,
        "completed block returns to the pool"
    );
    assert_eq!(iov.allocator.in_flight(), 0);
}

/// 匹配的 node 错误是明确结果：区块必须归还（不是 forfeit），调用方能立即重试。
#[tokio::test]
async fn matched_node_error_releases_the_block() {
    let (mut iov, mut ring) = local_ring_pair(8);
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    let carrier = Submission {
        op: OP_WRITE,
        flags: 0,
        reg_id: 1,
        len: 16,
        offset: 0,
        iov_offset: 0,
        userdata: 999,
    };
    {
        let producer = ring.ring.submission_producer();
        producer.push(&carrier).expect("push carrier submission");
    }
    ring.ring
        .ring_consumer()
        .complete_batch(&[Completion {
            userdata: 1,
            result: -1,
            status: afs_error::NODE_VFS_INVALID.raw(),
            ..Completion::default()
        }])
        .expect("preload failing completion");

    let data = vec![7_u8; 16];
    let error = ring
        .write_from(&mut iov, &reg, 0, &data)
        .await
        .expect_err("the node-side error must surface");
    assert_eq!(error.code(), afs_error::NODE_VFS_INVALID);
    assert_eq!(
        iov.allocator.free_blocks(),
        4,
        "a definite node error recycles the block"
    );
    assert_eq!(iov.allocator.in_flight(), 0);
}

/// 一批完成项先出现匹配的 node 错误、后出现对不上号的完成项：结果整体未知，
/// 整批 forfeit。错误累积模式不能让先到的「明确失败」掩盖后到的 mismatch。
#[tokio::test]
async fn node_error_then_mismatch_forfeits_the_whole_batch() {
    let (mut iov, mut ring) = local_ring_pair(8);
    let carrier = Submission {
        op: OP_WRITE,
        flags: 0,
        reg_id: 1,
        len: 16,
        offset: 0,
        iov_offset: 0,
        userdata: 777,
    };
    let carriers = [carrier, carrier];
    {
        let producer = ring.ring.submission_producer();
        for carrier in &carriers {
            producer.push(carrier).expect("push carrier submission");
        }
    }
    // 第一项对上 submissions[0]（userdata 1）但带 node 错误码；第二项对不上任何提交。
    ring.ring
        .ring_consumer()
        .complete_batch(&[
            Completion {
                userdata: 1,
                result: -1,
                status: afs_error::NODE_VFS_INVALID.raw(),
                ..Completion::default()
            },
            Completion {
                userdata: 999,
                ..Completion::default()
            },
        ])
        .expect("preload mixed completions");

    let lengths = [16_usize, 16];
    let mut batch = ReservedBatch::reserve(&mut iov.allocator, &lengths).expect("reserve");
    let submissions: Vec<Submission> = batch
        .slices()
        .iter()
        .enumerate()
        .map(|(index, slice)| Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id: 1,
            len: slice.len,
            offset: 0,
            iov_offset: slice.iov_offset,
            userdata: index as u64 + 1,
        })
        .collect();

    let error = ring
        .dispatch_batch(&submissions, &mut batch)
        .await
        .expect_err("the mismatched completion must fail the batch");
    drop(batch);
    assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
    assert_eq!(
        iov.allocator.free_blocks(),
        2,
        "unknown outcome keeps the whole batch occupied"
    );
    assert_eq!(iov.allocator.in_flight(), 2);
}

fn preload_probe_completions(ring: &DfsRing, completions: &[Completion]) {
    let producer = ring.ring.submission_producer();
    for _ in completions {
        producer.push(&Submission::default()).expect("carrier");
    }
    ring.ring
        .ring_consumer()
        .complete_batch(completions)
        .expect("preload");
}

#[tokio::test]
async fn duplicate_batch_completion_keeps_missing_read_block_and_output() {
    let (mut iov, mut ring) = local_ring_pair(8);
    preload_probe_completions(
        &ring,
        &[
            Completion {
                userdata: 1,
                result: 16,
                op: OP_READ,
                ..Completion::default()
            },
            Completion {
                userdata: 1,
                result: 16,
                op: OP_READ,
                ..Completion::default()
            },
        ],
    );
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    let mut first = [0xA5; 16];
    let mut second = [0x5A; 16];
    let mut items = [(0, first.as_mut_slice()), (16, second.as_mut_slice())];
    let error = ring
        .read_batch(&mut iov, &reg, &mut items)
        .await
        .expect_err("duplicate cannot settle missing request");
    assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
    assert_eq!(iov.free_blocks(), 2, "both unknown blocks stay quarantined");
    assert_eq!(first, [0xA5; 16]);
    assert_eq!(second, [0x5A; 16]);
    assert_eq!(
        ring.ring.ring_consumer().available(),
        2,
        "uncompleted requests remain in flight"
    );
}

#[tokio::test]
async fn oversized_batch_completion_is_protocol_failure_and_keeps_blocks() {
    let (mut iov, mut ring) = local_ring_pair(8);
    preload_probe_completions(
        &ring,
        &[
            Completion {
                userdata: 1,
                result: 17,
                op: OP_WRITE,
                ..Completion::default()
            },
            Completion {
                userdata: 2,
                result: 16,
                op: OP_WRITE,
                ..Completion::default()
            },
        ],
    );
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    let bytes = [7u8; 16];
    let error = ring
        .write_batch(&mut iov, &reg, &[(0, &bytes), (16, &bytes)])
        .await
        .expect_err("oversized result");
    assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
    assert_eq!(
        iov.free_blocks(),
        2,
        "malformed result leaves outcome unknown"
    );
}

#[tokio::test]
async fn batch_completion_order_and_wrapping_userdata_preserve_results() {
    let (mut iov, mut ring) = local_ring_pair(8);
    ring.next_userdata = u64::MAX;
    preload_probe_completions(
        &ring,
        &[
            Completion {
                userdata: 0,
                result: 4,
                op: OP_WRITE,
                ..Completion::default()
            },
            Completion {
                userdata: u64::MAX,
                result: 8,
                op: OP_WRITE,
                ..Completion::default()
            },
        ],
    );
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    let bytes = [1; 16];
    assert_eq!(
        ring.write_batch(&mut iov, &reg, &[(0, &bytes), (16, &bytes)])
            .await
            .expect("ordered lengths"),
        vec![8, 4]
    );
    assert_eq!(iov.free_blocks(), 4);
}

#[tokio::test]
async fn eof_single_returns_zero_and_preserves_output() {
    let (mut iov, mut ring) = local_ring_pair(8);
    preload_probe_completions(
        &ring,
        &[Completion {
            userdata: 1,
            result: 0,
            op: OP_READ,
            ..Completion::default()
        }],
    );
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    let mut out = [0xA5; 8];
    assert_eq!(
        ring.read_into(&mut iov, &reg, 1024, &mut out)
            .await
            .expect("EOF is success"),
        0
    );
    assert_eq!(out, [0xA5; 8]);
    assert_eq!(iov.free_blocks(), 4);
}
#[tokio::test]
async fn eof_batch_returns_zero_and_recycles_all_known_blocks() {
    let (mut iov, mut ring) = local_ring_pair(8);
    preload_probe_completions(
        &ring,
        &[
            Completion {
                userdata: 1,
                result: 0,
                op: OP_READ,
                ..Completion::default()
            },
            Completion {
                userdata: 2,
                result: 4,
                op: OP_READ,
                ..Completion::default()
            },
        ],
    );
    let slice = IovSlice {
        block: 1,
        iov_offset: iov.layout().block_offset(1) as u64,
        len: 4,
    };
    iov.view.write_block(&slice, b"data").expect("data");
    let reg = DfsRegistration {
        reg_id: 1,
        file_version_id: String::new(),
    };
    let mut first = [0xA5; 8];
    let mut second = [0x5A; 8];
    assert_eq!(
        ring.read_batch(
            &mut iov,
            &reg,
            &mut [(1024, first.as_mut_slice()), (0, second.as_mut_slice())]
        )
        .await
        .expect("mixed EOF and data"),
        vec![0, 4]
    );
    assert_eq!(first, [0xA5; 8]);
    assert_eq!(&second[..4], b"data");
    assert_eq!(&second[4..], &[0x5A; 4]);
    assert_eq!(iov.free_blocks(), 4);
}

#[tokio::test]
async fn staged_write_cancellation_tracks_zero_and_published_prefixes() {
    let block = 1024 * 1024;
    for filled in [4, 8] {
        let layout = IovLayout::new(block, 8).expect("layout");
        let (mut iov, mut ring) = local_ring_pair_with_layout(8, layout);
        fill_submission_queue(&ring, filled);
        let reg = DfsRegistration {
            reg_id: 1,
            file_version_id: String::new(),
        };
        let payload = vec![0xA5; block];
        let items: Vec<_> = (0..8)
            .map(|i| ((i * block) as u64, payload.as_slice()))
            .collect();
        let timed = tokio::time::timeout(
            Duration::from_millis(20),
            ring.write_batch(&mut iov, &reg, &items),
        )
        .await;
        assert!(timed.is_err(), "wait for space is cancellable");
        assert_eq!(
            iov.free_blocks(),
            if filled == 8 { 8 } else { 0 },
            "only zero-push cancellation recycles"
        );
        assert_eq!(ring.ring.ring_consumer().available(), 8);
        if filled == 4 {
            let submitted = ring
                .ring
                .ring_consumer()
                .peek_at(4)
                .expect("published prefix");
            assert!(
                iov.view
                    .slice(submitted.iov_offset, block as u64)
                    .expect("bytes")
                    .iter()
                    .all(|b| *b == 0xA5)
            );
            assert!(
                ring.write_from(&mut iov, &reg, 0, b"replacement")
                    .await
                    .is_err(),
                "unknown banks cannot be reused"
            );
        }
    }
}

/// `io_depth` 的支持范围：绝对值不得超过 worker 的单周期执行批量上限（256）。
/// 超出范围必须创建前拒绝，而不是被 worker 截断成 256 后静默改写语义。
#[test]
fn io_depth_outside_the_supported_range_is_rejected_upfront() {
    let layout = RingLayout::new(1024).expect("ring layout");
    for io_depth in [512, 257, -257, -512, i32::MAX, i32::MIN] {
        assert!(
            layout.validate_io_depth(io_depth).is_err(),
            "io_depth {io_depth} must be rejected"
        );
    }
    // 边界与三种策略不受影响。
    assert!(layout.validate_io_depth(256).is_ok());
    assert!(layout.validate_io_depth(-256).is_ok());
    assert!(layout.validate_io_depth(0).is_ok());
    // 原有规则照旧：大于 entries 的正数仍然拒绝。
    let small = RingLayout::new(8).expect("ring layout");
    assert!(small.validate_io_depth(16).is_err());
    assert!(small.validate_io_depth(8).is_ok());
}

/// SDK 建环入口本身必须在本地拒绝非法 `io_depth`：不触发控制面 RPC、不碰 fd broker。
/// 删掉 `create_ring` 里的校验调用时，错误会变成连接/传输类，这个测试必须变红。
#[tokio::test]
async fn create_ring_rejects_invalid_io_depth_locally_without_control_plane() {
    let (iov, _ring) = local_ring_pair(8);
    let client = DfsLocalClient {
        inner: DfsLocalDataClient::new(traced_channel(
            Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        )),
        request_timeout: Duration::from_secs(1),
        namespace_id: DEFAULT_NAMESPACE.to_owned(),
        fd_identity: Arc::new(fd::ClientIdentity::new().unwrap()),
    };

    // 大于 entries：原有规则，走 SDK 入口必须本地拒绝。
    let error = client
        .create_ring(&iov, 8, 16, 0)
        .await
        .expect_err("io_depth beyond entries must be rejected locally");
    assert_eq!(error.code(), afs_error::CLIENT_ARGUMENT_INVALID);

    // 超出执行批量上限（entries 给足）：同样必须本地拒绝。
    let error = client
        .create_ring(&iov, 1024, 512, 0)
        .await
        .expect_err("io_depth beyond the execution batch limit must be rejected locally");
    assert_eq!(error.code(), afs_error::CLIENT_ARGUMENT_INVALID);
}
