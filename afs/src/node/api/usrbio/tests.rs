use super::*;
use crate::node::vfs::types::{
    BackendInode, Entry, FileAttributes, FileHandle, FileKind, OpenOptions,
};
use afs_metrics::{IntCounterVec, Opts, Registry};
use afs_transport::eventfd::EventFd;
use afs_transport::iov::IovAllocator;
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

const FILE_INODE: u64 = 2;
const FILE_NAME: &str = "data.bin";

/// 只实现 usrbio 用到的最小内存后端；用于在没有 DFS meta/chunk 的情况下验证环数据面。
pub(crate) struct MemoryBackend {
    files: StdMutex<HashMap<u64, Vec<u8>>>,
    handles: StdMutex<HashMap<u64, u64>>,
    next_handle: StdMutex<u64>,
    /// 记录后端实际收到的同步屏障，用来断言 SDK 没有把 DataOnly 悄悄降级成 Full 或反之。
    last_sync: StdMutex<Option<types::SyncMode>>,
    /// 记录句柄生命周期调用（flush / release），用来固定 SDK close 的语义。
    handle_calls: StdMutex<Vec<&'static str>>,
    fail_next_flush: StdMutex<bool>,
}

impl MemoryBackend {
    pub(crate) fn new(size: usize) -> Self {
        let mut files = HashMap::new();
        files.insert(FILE_INODE, vec![0_u8; size]);
        Self {
            files: StdMutex::new(files),
            handles: StdMutex::new(HashMap::new()),
            next_handle: StdMutex::new(1),
            last_sync: StdMutex::new(None),
            handle_calls: StdMutex::new(Vec::new()),
            fail_next_flush: StdMutex::new(false),
        }
    }

    fn attributes(&self, inode: u64) -> FileAttributes {
        let size = self
            .files
            .lock()
            .expect("files")
            .get(&inode)
            .map_or(0, |bytes| bytes.len() as u64);
        let now = SystemTime::now();
        FileAttributes {
            kind: FileKind::Regular,
            size,
            blocks: size.div_ceil(512),
            mode: 0o644,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime: now,
            mtime: now,
            ctime: now,
        }
    }

    fn inode_of(&self, handle: FileHandle) -> Result<u64, Error> {
        self.handles
            .lock()
            .expect("handles")
            .get(&handle.0)
            .copied()
            .ok_or_else(|| invalid("unknown handle"))
    }
}

impl Backend for MemoryBackend {
    fn root_inode(&self) -> BackendInode {
        BackendInode { value: 1 }
    }

    fn lookup(
        &self,
        _ctx: &types::RequestContext,
        parent: BackendInode,
        name: &OsStr,
    ) -> Result<Entry, Error> {
        if parent.value != 1 || name != OsStr::new(FILE_NAME) {
            return Err(Error::coded(afs_error::NODE_VFS_NOT_FOUND, "no such entry"));
        }
        Ok(Entry {
            inode: BackendInode { value: FILE_INODE },
            attributes: self.attributes(FILE_INODE),
        })
    }

    fn open_with_options(
        &self,
        _ctx: &types::RequestContext,
        inode: BackendInode,
        _flags: i32,
        _options: OpenOptions,
    ) -> Result<FileHandle, Error> {
        if inode.value != FILE_INODE {
            return Err(Error::coded(afs_error::NODE_VFS_NOT_FOUND, "no such inode"));
        }
        let mut next = self.next_handle.lock().expect("next handle");
        let handle = FileHandle(*next);
        *next += 1;
        self.handles
            .lock()
            .expect("handles")
            .insert(handle.0, inode.value);
        Ok(handle)
    }

    fn getattr(
        &self,
        _ctx: &types::RequestContext,
        inode: BackendInode,
        _handle: Option<FileHandle>,
    ) -> Result<FileAttributes, Error> {
        Ok(self.attributes(inode.value))
    }

    fn read(
        &self,
        _ctx: &types::RequestContext,
        handle: FileHandle,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize, Error> {
        let inode = self.inode_of(handle)?;
        let files = self.files.lock().expect("files");
        let data = files.get(&inode).ok_or_else(|| invalid("no such file"))?;
        let start = usize::try_from(offset).map_err(|_| invalid("offset too large"))?;
        if start >= data.len() {
            return Ok(0);
        }
        let available = (data.len() - start).min(out.len());
        out[..available].copy_from_slice(&data[start..start + available]);
        Ok(available)
    }

    fn write(
        &self,
        _ctx: &types::RequestContext,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, Error> {
        let inode = self.inode_of(handle)?;
        let mut files = self.files.lock().expect("files");
        let target = files
            .get_mut(&inode)
            .ok_or_else(|| invalid("no such file"))?;
        let start = usize::try_from(offset).map_err(|_| invalid("offset too large"))?;
        let end = start
            .checked_add(data.len())
            .ok_or_else(|| invalid("range overflows"))?;
        if end > target.len() {
            target.resize(end, 0);
        }
        target[start..end].copy_from_slice(data);
        Ok(data.len())
    }

    fn fsync(
        &self,
        _ctx: &types::RequestContext,
        _handle: FileHandle,
        mode: types::SyncMode,
    ) -> Result<(), Error> {
        *self.last_sync.lock().expect("last sync") = Some(mode);
        Ok(())
    }

    fn flush(&self, _ctx: &types::RequestContext, _handle: FileHandle) -> Result<(), Error> {
        self.handle_calls
            .lock()
            .expect("handle calls")
            .push("flush");
        if std::mem::take(&mut *self.fail_next_flush.lock().expect("fail flush")) {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "injected flush failure",
            ));
        }
        Ok(())
    }

    fn release(&self, _ctx: &types::RequestContext, handle: FileHandle) -> Result<(), Error> {
        self.handle_calls
            .lock()
            .expect("handle calls")
            .push("release");
        self.handles.lock().expect("handles").remove(&handle.0);
        Ok(())
    }
}

/// flush 失败：close 把错误还给调用者，句柄照样释放（POSIX close 失败后 fd 也已关闭）。
#[test]
fn sdk_path_close_returns_flush_error_and_still_releases() {
    let backend = Arc::new(MemoryBackend::new(16));
    *backend.fail_next_flush.lock().expect("fail flush") = true;
    let hub = UsrbioHub::new(backend.clone(), None);
    let (handle, _) = hub
        .open("test", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let error = hub
        .close(&handle)
        .expect_err("close reports the flush error");
    assert!(error.to_string().contains("injected flush failure"));
    assert_eq!(
        *backend.handle_calls.lock().expect("handle calls"),
        vec!["flush", "release"]
    );
    assert!(backend.handles.lock().expect("handles").is_empty());
    hub.close(&handle).expect_err("the handle is gone");
}

/// SDK 侧与 Node 侧各自映射同一块 memfd，模拟真实的两进程关系。
struct TestRegion {
    _region: shm::SharedRegion,
    mapping: MappedRegion,
}

impl TestRegion {
    fn new(name: &str, len: usize) -> Self {
        let region = shm::SharedRegion::create_with_limit(name, len, len).expect("region");
        let mapping =
            MappedRegion::map(region.duplicate_fd_owned().expect("dup"), len).expect("map");
        Self {
            _region: region,
            mapping,
        }
    }

    fn handle_fd(&self) -> OwnedFd {
        self._region.duplicate_fd_owned().expect("dup")
    }
}

fn wait_for_completion(consumer: &afs_transport::ring::CompletionConsumer<'_>) -> Completion {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(completion) = consumer.pop() {
            return completion;
        }
        assert!(Instant::now() < deadline, "ring completion timed out");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn ring_round_trip_writes_and_reads_through_shared_memory() {
    let backend = Arc::new(MemoryBackend::new(4096));
    let hub = UsrbioHub::new(backend.clone(), Some("test".to_owned()));

    // iov：4 个 4 KiB 区块
    let layout = IovLayout::new(4096, 4).expect("iov layout");
    let iov_region = TestRegion::new("usrbio-iov", layout.total_bytes());
    // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
    let iov_view =
        unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
    iov_view.initialize().expect("iov init");
    let iov_id = hub
        .create_iov(iov_region.handle_fd(), layout)
        .expect("create iov");

    // 环：8 项
    let ring_layout = RingLayout::new(8).expect("ring layout");
    let ring_region = TestRegion::new("usrbio-ring", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("ring init");
    // worker 阻塞在门铃上，测试必须持有同一个 eventfd 的副本才能唤醒它。
    let doorbell = EventFd::create().expect("eventfd");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            0,
            200,
        )
        .expect("create ring");

    let (handle, length) = hub
        .open("test", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    assert_eq!(length, 4096);
    let reg_id = hub.register_file(&handle).expect("register");

    let mut allocator = IovAllocator::new(layout);
    let producer = ring.submission_producer();
    let consumer = ring.completion_consumer();

    // 写：iov 第 0 区块 → 文件偏移 0
    let write_slice = allocator.reserve(5).expect("reserve");
    iov_view
        .write_block(&write_slice, b"hello")
        .expect("write block");
    producer
        .push(&Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id,
            len: 5,
            offset: 0,
            iov_offset: write_slice.iov_offset,
            userdata: 11,
        })
        .expect("push write");
    doorbell.ring();

    let completion = wait_for_completion(&consumer);
    assert_eq!(completion.userdata, 11);
    assert_eq!(completion.status, 0, "write status");
    assert_eq!(completion.result, 5);
    assert_eq!(
        backend.files.lock().expect("files")[&FILE_INODE][..5],
        *b"hello"
    );
    allocator.release(&write_slice).expect("release");

    // 读：文件偏移 0 → iov 第 1 区块
    let read_slice = allocator.reserve(5).expect("reserve");
    producer
        .push(&Submission {
            op: OP_READ,
            flags: 0,
            reg_id,
            len: 5,
            offset: 0,
            iov_offset: read_slice.iov_offset,
            userdata: 12,
        })
        .expect("push read");
    doorbell.ring();

    let completion = wait_for_completion(&consumer);
    assert_eq!(completion.userdata, 12);
    assert_eq!(completion.status, 0, "read status");
    assert_eq!(completion.result, 5);
    assert_eq!(
        iov_view
            .slice(read_slice.iov_offset, 5)
            .expect("read back slice"),
        b"hello"
    );

    hub.unregister_file(reg_id).expect("unregister");
    hub.close(&handle).expect("close");
    hub.destroy_ring(ring_id).expect("destroy ring");
    hub.destroy_iov(iov_id).expect("destroy iov");
}

/// 决策 12（R02）：SDK 路径式 close 按 3FS 语义先 flush（同一次同步提交），再 release。
#[test]
fn sdk_path_close_flushes_then_releases() {
    let backend = Arc::new(MemoryBackend::new(16));
    let hub = UsrbioHub::new(backend.clone(), None);
    let (handle, _) = hub
        .open("test", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    hub.close(&handle).expect("close");
    assert_eq!(
        *backend.handle_calls.lock().expect("handle calls"),
        vec!["flush", "release"]
    );
    assert!(backend.last_sync.lock().expect("last sync").is_none());
}

#[test]
fn create_ring_rejects_io_depth_beyond_the_entry_count() {
    // io_depth > 0 是「必须攒满」：目标大于环项数时这批永远凑不满，环会永久停摆，
    // 创建时就拒绝，而不是留一条永远不工作的环。
    let backend = Arc::new(MemoryBackend::new(64));
    let hub = UsrbioHub::new(backend, None);
    let layout = IovLayout::new(4096, 2).expect("iov layout");
    let iov_region = TestRegion::new("usrbio-iov-depth", layout.total_bytes());
    // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
    let view =
        unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
    view.initialize().expect("init");
    let iov_id = hub
        .create_iov(iov_region.handle_fd(), layout)
        .expect("create iov");

    let ring_layout = RingLayout::new(4).expect("ring layout");
    let ring_region = TestRegion::new("usrbio-ring-depth", ring_layout.total_bytes());
    let doorbell = EventFd::create().expect("doorbell");
    let error = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            8,
            200,
        )
        .expect_err("io_depth beyond entries must be rejected");
    assert_eq!(error.code(), afs_error::NODE_VFS_INVALID);

    // 边界值：io_depth == entries 恰好能攒满一批，仍然合法。
    let ring_region = TestRegion::new("usrbio-ring-depth-ok", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("ring init");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            4,
            200,
        )
        .expect("io_depth equal to entries is allowed");
    hub.destroy_ring(ring_id).expect("destroy ring");
    hub.destroy_iov(iov_id).expect("destroy iov");
}

#[test]
fn destroy_ring_fails_pending_submissions_with_a_completion_doorbell() {
    // 提交不敲门铃，worker 睡在门铃上；io_depth = entries 让它即使扫到这项提交也不会
    // 处理（必须攒满 4 个，只有 1 个）。destroy_ring 必须把这项提交以失败完成，并且在
    // 写入 CQE 之后再敲一次完成 eventfd——join 前那一下可能已被 SDK 读掉。
    let backend = Arc::new(MemoryBackend::new(64));
    let hub = UsrbioHub::new(backend, None);
    let layout = IovLayout::new(4096, 2).expect("iov layout");
    let iov_region = TestRegion::new("usrbio-iov-failpending", layout.total_bytes());
    // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
    let view =
        unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
    view.initialize().expect("init");
    let iov_id = hub
        .create_iov(iov_region.handle_fd(), layout)
        .expect("create iov");

    let ring_layout = RingLayout::new(4).expect("ring layout");
    let ring_region = TestRegion::new("usrbio-ring-failpending", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("init");
    let doorbell = EventFd::create().expect("doorbell");
    let completion = EventFd::create().expect("completion");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: completion.try_clone_fd().expect("dup completion"),
            },
            ring_layout,
            iov_id,
            4,
            200,
        )
        .expect("create ring");

    ring.submission_producer()
        .push(&Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id: 7,
            len: 4,
            offset: 0,
            iov_offset: 0,
            userdata: 42,
        })
        .expect("push pending submission");

    hub.destroy_ring(ring_id).expect("destroy ring");

    // join 返回后计数必须是 2：destroy_ring join 前一次 + fail_pending 写完 CQE 后一次。
    // 只有 1 说明 fail_pending 没敲门，SDK 只能靠超时兜底才拿得到这批失败完成项。
    let mut counter = [0_u8; 8];
    // SAFETY: counter 是本栈帧内 8 字节数组，eventfd 的 read 恰好要求 8 字节缓冲。
    let read = unsafe { libc::read(completion.raw_fd(), counter.as_mut_ptr().cast(), 8) };
    assert_eq!(read, 8, "completion counter read");
    assert_eq!(
        u64::from_ne_bytes(counter),
        2,
        "fail_pending must ring the completion doorbell after writing CQEs"
    );

    let consumer = ring.completion_consumer();
    let failed = wait_for_completion(&consumer);
    assert_eq!(failed.userdata, 42);
    assert_eq!(failed.status, afs_error::NODE_VFS_UNAVAILABLE.raw());
    assert_eq!(failed.result, -1);

    hub.destroy_iov(iov_id).expect("destroy iov");
}

#[test]
fn control_plane_enforces_handle_and_ring_lifetimes() {
    let backend = Arc::new(MemoryBackend::new(64));
    let hub = UsrbioHub::new(backend, None);
    let layout = IovLayout::new(4096, 2).expect("iov layout");
    let iov_region = TestRegion::new("usrbio-iov-lifecycle", layout.total_bytes());
    // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
    let view =
        unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
    view.initialize().expect("init");
    let iov_id = hub
        .create_iov(iov_region.handle_fd(), layout)
        .expect("create iov");

    let ring_layout = RingLayout::new(4).expect("ring layout");
    let ring_region = TestRegion::new("usrbio-ring-lifecycle", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("init");
    let doorbell = EventFd::create().expect("eventfd");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            0,
            100,
        )
        .expect("create ring");

    // iov 仍被环引用时不能销毁
    assert!(hub.destroy_iov(iov_id).is_err());

    let (handle, _) = hub
        .open("default", "/data.bin", OpenAccess::ReadOnly)
        .expect("open");
    let reg_id = hub.register_file(&handle).expect("register");
    // 仍被注册时不能关闭
    assert!(hub.close(&handle).is_err());
    hub.unregister_file(reg_id).expect("unregister");
    hub.close(&handle).expect("close after unregister");
    // 句柄已关闭后注册应失败
    assert!(hub.register_file(&handle).is_err());

    hub.destroy_ring(ring_id).expect("destroy ring");
    hub.destroy_iov(iov_id).expect("destroy iov");
    assert!(hub.destroy_iov(iov_id).is_err());
}

#[test]
fn open_rejects_relative_and_traversing_paths() {
    let backend = Arc::new(MemoryBackend::new(16));
    let hub = UsrbioHub::new(backend, Some("test".to_owned()));
    assert!(hub.open("test", "data.bin", OpenAccess::ReadOnly).is_err());
    assert!(
        hub.open("test", "/../data.bin", OpenAccess::ReadOnly)
            .is_err()
    );
    assert!(
        hub.open("test", "/missing.bin", OpenAccess::ReadOnly)
            .is_err()
    );
    assert!(
        hub.open("other", "/data.bin", OpenAccess::ReadOnly)
            .is_err()
    );
    assert!(hub.open("test", "/", OpenAccess::ReadOnly).is_err());
}

#[test]
fn unknown_registration_produces_failed_completion() {
    let backend = Arc::new(MemoryBackend::new(16));
    let hub = UsrbioHub::new(backend, None);
    let layout = IovLayout::new(4096, 2).expect("iov layout");
    let iov_region = TestRegion::new("usrbio-iov-unknown", layout.total_bytes());
    // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
    let view =
        unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
    view.initialize().expect("init");
    let iov_id = hub
        .create_iov(iov_region.handle_fd(), layout)
        .expect("create iov");

    let ring_layout = RingLayout::new(4).expect("ring layout");
    let ring_region = TestRegion::new("usrbio-ring-unknown", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("init");
    let doorbell = EventFd::create().expect("eventfd");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            0,
            50,
        )
        .expect("create ring");

    let producer = ring.submission_producer();
    let consumer = ring.completion_consumer();
    producer
        .push(&Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id: 999,
            len: 4,
            offset: 0,
            iov_offset: layout.block_offset(0) as u64,
            userdata: 3,
        })
        .expect("push");
    doorbell.ring();

    let completion = wait_for_completion(&consumer);
    assert_eq!(completion.userdata, 3);
    assert_eq!(completion.status, afs_error::NODE_VFS_INVALID.raw());
    assert_eq!(completion.result, -1);

    hub.destroy_ring(ring_id).expect("destroy ring");
    hub.destroy_iov(iov_id).expect("destroy iov");
}

/// 一次非法请求不能拖垮整条环：节点回失败完成项，后续合法请求照常工作。
#[test]
fn invalid_iov_span_fails_the_completion_but_keeps_the_ring_usable() {
    let backend = Arc::new(MemoryBackend::new(16));
    let hub = UsrbioHub::new(backend, None);
    let layout = IovLayout::new(4096, 2).expect("iov layout");
    let iov_region = TestRegion::new("usrbio-iov-span", layout.total_bytes());
    // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
    let view =
        unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
    view.initialize().expect("init");
    let iov_id = hub
        .create_iov(iov_region.handle_fd(), layout)
        .expect("create iov");
    let ring_layout = RingLayout::new(4).expect("ring layout");
    let ring_region = TestRegion::new("usrbio-ring-span", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("init");
    let doorbell = EventFd::create().expect("eventfd");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            0,
            50,
        )
        .expect("create ring");
    let (handle, _) = hub
        .open("test", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let reg_id = hub.register_file(&handle).expect("register");

    let producer = ring.submission_producer();
    let consumer = ring.completion_consumer();
    // 跨度越过块尾：block 0 只有 4096 字节，这里要 4097。
    producer
        .push(&Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id,
            len: 4097,
            offset: 0,
            iov_offset: layout.block_offset(0) as u64,
            userdata: 11,
        })
        .expect("push");
    doorbell.ring();
    let completion = wait_for_completion(&consumer);
    assert_eq!(completion.userdata, 11, "失败完成项要带上原始 userdata");
    assert_ne!(completion.status, 0, "越界跨度必须报错");
    assert_eq!(completion.result, -1);

    // 环没被拖垮：紧接着一条合法写入必须成功。
    producer
        .push(&Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id,
            len: 4,
            offset: 0,
            iov_offset: layout.block_offset(1) as u64,
            userdata: 12,
        })
        .expect("push");
    doorbell.ring();
    let completion = wait_for_completion(&consumer);
    assert_eq!(completion.userdata, 12);
    assert_eq!(completion.status, 0, "坏请求之后环仍应可用");
    assert_eq!(completion.result, 4);

    hub.destroy_ring(ring_id).expect("destroy ring");
    hub.destroy_iov(iov_id).expect("destroy iov");
}

/// 带着未消费的提交销毁环：必须能收尾，不能挂死，也不能影响后续建环。
#[test]
fn destroying_a_ring_with_pending_submissions_completes() {
    let backend = Arc::new(MemoryBackend::new(16));
    let hub = UsrbioHub::new(backend, None);
    let layout = IovLayout::new(4096, 2).expect("iov layout");
    let iov_region = TestRegion::new("usrbio-iov-destroy", layout.total_bytes());
    // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
    let view =
        unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
    view.initialize().expect("init");
    let iov_id = hub
        .create_iov(iov_region.handle_fd(), layout)
        .expect("create iov");
    let ring_layout = RingLayout::new(8).expect("ring layout");
    let ring_region = TestRegion::new("usrbio-ring-destroy", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("init");
    let doorbell = EventFd::create().expect("eventfd");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            0,
            50,
        )
        .expect("create ring");
    let (handle, _) = hub
        .open("test", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let reg_id = hub.register_file(&handle).expect("register");

    // 堆满提交但一条都不等，直接销毁。
    let producer = ring.submission_producer();
    for userdata in 0..8_u64 {
        producer
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id,
                len: 4,
                offset: 0,
                iov_offset: layout.block_offset(0) as u64,
                userdata,
            })
            .expect("push");
    }
    doorbell.ring();
    hub.destroy_ring(ring_id)
        .expect("destroy ring with pending work");

    // hub 还能继续工作：销毁不能留下坏状态。
    let second_layout = IovLayout::new(4096, 2).expect("iov layout");
    let second_region = TestRegion::new("usrbio-iov-after", second_layout.total_bytes());
    // SAFETY: mapping 覆盖 second_layout.total_bytes() 字节。
    let second_view =
        unsafe { SharedIov::from_raw_parts(second_region.mapping.as_ptr(), second_layout) }
            .expect("iov");
    second_view.initialize().expect("init");
    let second_iov = hub
        .create_iov(second_region.handle_fd(), second_layout)
        .expect("create iov");
    let second_ring_layout = RingLayout::new(4).expect("ring layout");
    let second_ring_region = TestRegion::new("usrbio-ring-after", second_ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 second_ring_layout.total_bytes() 字节。
    let second_ring = unsafe {
        SharedRing::from_raw_parts(second_ring_region.mapping.as_ptr(), second_ring_layout)
    }
    .expect("ring");
    second_ring.initialize().expect("init");
    let second_doorbell = EventFd::create().expect("eventfd");
    let second_id = hub
        .create_ring(
            RingFds {
                region: second_ring_region.handle_fd(),
                doorbell: second_doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            second_ring_layout,
            second_iov,
            0,
            50,
        )
        .expect("create second ring");

    let producer = second_ring.submission_producer();
    let consumer = second_ring.completion_consumer();
    producer
        .push(&Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id,
            len: 4,
            offset: 0,
            iov_offset: second_layout.block_offset(0) as u64,
            userdata: 21,
        })
        .expect("push");
    second_doorbell.ring();
    let completion = wait_for_completion(&consumer);
    assert_eq!(completion.userdata, 21);
    assert_eq!(completion.status, 0);

    hub.destroy_ring(second_id).expect("destroy second ring");
    hub.destroy_iov(iov_id).expect("destroy iov");
    hub.destroy_iov(second_iov).expect("destroy second iov");
}

/// 用真实 gRPC 服务 + 真实 SDK 客户端跑一遍 usrbio 往返，后端用内存实现。
#[tokio::test]
async fn local_sdk_round_trips_through_iov_and_ring() {
    use crate::node::api::local::{LocalApiOptions, serve_local_api_with_options};
    use crate::node::diagnostics::DiagnosticStorage;
    use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig};

    let temp = tempfile::tempdir().expect("tempdir");
    let socket_path = temp.path().join("afs-node.sock");
    let storage = DiagnosticStorage::new(temp.path().join("diagnostics")).expect("storage");
    let backend = Arc::new(MemoryBackend::new(4096));
    let server = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(backend.clone() as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");

    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(socket_path))
        .await
        .expect("connect");
    let file = client
        .open("/data.bin", DfsAccess::ReadWrite)
        .await
        .expect("open");
    assert_eq!(file.length, 4096);

    let mut iov = client.create_iov(4096, 4).await.expect("create iov");
    let mut ring = client
        .create_ring(&iov, 8, 0, 500)
        .await
        .expect("create ring");
    let registration = client.register_file(&file).await.expect("register");

    let written = ring
        .write_from(&mut iov, &registration, 0, b"usrbio!")
        .await
        .expect("write through ring");
    assert_eq!(written, 7);
    assert_eq!(
        backend.files.lock().expect("files")[&FILE_INODE][..7],
        *b"usrbio!"
    );

    let mut out = vec![0_u8; 7];
    let read = ring
        .read_into(&mut iov, &registration, 0, &mut out)
        .await
        .expect("read through ring");
    assert_eq!(read, 7);
    assert_eq!(out, b"usrbio!");

    // 超过一个区块的请求必须被拒绝，不能由实现偷偷跨块拼接。
    let oversized = vec![0_u8; 8192];
    assert!(
        ring.write_from(&mut iov, &registration, 0, &oversized)
            .await
            .is_err(),
        "cross-block write must be rejected"
    );
    assert_eq!(ring.pending(), 0);
    assert_eq!(iov.in_flight(), 0);

    client
        .unregister_file(registration)
        .await
        .expect("unregister");
    client.close(file.handle).await.expect("close");
    ring.destroy().await.expect("destroy ring");
    iov.destroy().await.expect("destroy iov");
    server.shutdown().await.expect("shutdown");
}

/// 未装配 DFS 后端时必须报 UNIMPLEMENTED，绝不退回 diagnostics 目录。
#[tokio::test]
async fn local_sdk_without_dfs_backend_is_unimplemented() {
    use crate::node::api::local::serve_local_api;
    use crate::node::diagnostics::DiagnosticStorage;
    use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig};

    let temp = tempfile::tempdir().expect("tempdir");
    let socket_path = temp.path().join("afs-node.sock");
    let storage = DiagnosticStorage::new(temp.path().join("diagnostics")).expect("storage");
    let server = serve_local_api(storage, &socket_path)
        .await
        .expect("serve local api");
    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
        .await
        .expect("connect");
    let error = client
        .open("/data.bin", DfsAccess::ReadOnly)
        .await
        .expect_err("open must fail without a DFS backend");
    assert_eq!(error.code(), afs_error::NODE_VFS_UNIMPLEMENTED);
    server.shutdown().await.expect("shutdown");
}

/// 两种同步屏障必须原样到达后端：不能把 DataOnly 悄悄升级或降级。
#[tokio::test]
async fn sync_modes_reach_the_backend_as_requested() {
    use crate::node::api::local::{LocalApiOptions, serve_local_api_with_options};
    use crate::node::diagnostics::DiagnosticStorage;
    use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig, DfsSyncMode};

    let temp = tempfile::tempdir().expect("tempdir");
    let socket_path = temp.path().join("afs-node.sock");
    let storage = DiagnosticStorage::new(temp.path().join("diagnostics")).expect("storage");
    let backend = Arc::new(MemoryBackend::new(4096));
    let server = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(backend.clone() as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");
    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
        .await
        .expect("connect");

    let file = client
        .open("/data.bin", DfsAccess::ReadWrite)
        .await
        .expect("open");
    client
        .sync(&file.handle, DfsSyncMode::Data)
        .await
        .expect("sync data");
    assert!(
        matches!(
            *backend.last_sync.lock().expect("last sync"),
            Some(types::SyncMode::DataOnly)
        ),
        "DataOnly must not be widened to Full"
    );
    client
        .sync(&file.handle, DfsSyncMode::Full)
        .await
        .expect("sync full");
    assert!(
        matches!(
            *backend.last_sync.lock().expect("last sync"),
            Some(types::SyncMode::Full)
        ),
        "Full must reach the backend as-is"
    );
    // A backend without version identity (the default `fsync_receipt`) reports none.
    let reply = client
        .sync(&file.handle, DfsSyncMode::Full)
        .await
        .expect("sync full");
    assert!(reply.file_version_id.is_empty());
    assert_eq!(reply.durable_write_seq, 0);

    client.close(file.handle.clone()).await.expect("close");
    // 已关闭的句柄不能再同步；必须显式报错，不能当作成功。
    assert!(
        client.sync(&file.handle, DfsSyncMode::Data).await.is_err(),
        "sync on a closed handle must fail"
    );
    server.shutdown().await.expect("shutdown");
}

/// 一次门铃必须能把环里已经排好的多项都处理掉；否则批量提交就没有意义。
#[test]
fn single_doorbell_completes_every_queued_submission() {
    let backend = Arc::new(MemoryBackend::new(4096));
    let hub = UsrbioHub::new(backend.clone(), None);
    let layout = IovLayout::new(4096, 8).expect("iov layout");
    let iov_region = TestRegion::new("usrbio-iov-batch", layout.total_bytes());
    // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
    let view =
        unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
    view.initialize().expect("init");
    let iov_id = hub
        .create_iov(iov_region.handle_fd(), layout)
        .expect("create iov");

    let ring_layout = RingLayout::new(8).expect("ring layout");
    let ring_region = TestRegion::new("usrbio-ring-batch", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("init");
    let doorbell = EventFd::create().expect("eventfd");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            0,
            100,
        )
        .expect("create ring");

    let (handle, _) = hub
        .open("default", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let reg_id = hub.register_file(&handle).expect("register");

    let mut allocator = IovAllocator::new(layout);
    let producer = ring.submission_producer();
    let consumer = ring.completion_consumer();

    // 排 4 项，中间一次门铃都不敲。
    for index in 0..4_u64 {
        let slice = allocator.reserve(4).expect("reserve");
        view.write_block(&slice, b"abc\n").expect("write block");
        producer
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id,
                len: 4,
                offset: index * 4,
                iov_offset: slice.iov_offset,
                userdata: 100 + index,
            })
            .expect("push");
    }
    doorbell.ring();

    let mut seen = Vec::new();
    for _ in 0..4 {
        let completion = wait_for_completion(&consumer);
        assert_eq!(completion.status, 0, "batch write status");
        assert_eq!(completion.result, 4);
        seen.push(completion.userdata);
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![100, 101, 102, 103]);
    assert_eq!(
        backend.files.lock().expect("files")[&FILE_INODE][..8],
        *b"abc\nabc\n"
    );

    let stats = hub.ring_stats(ring_id).expect("ring stats");
    assert_eq!(stats.submitted, 4, "四项都必须被执行过");
    assert_eq!(stats.completed, 4);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.bytes, 16, "4 × 4 字节");
    assert!(stats.wakeups >= 1, "至少被门铃唤醒过一次");

    hub.unregister_file(reg_id).expect("unregister");
    hub.close(&handle).expect("close");
    hub.destroy_ring(ring_id).expect("destroy ring");
    // 环解绑后计数随之消失，不再对外可见。
    assert!(hub.ring_stats(ring_id).is_none());
    hub.destroy_iov(iov_id).expect("destroy iov");
}

/// 计数必须真的进入 registry：口径与命名是运维实际会看到的接口，不能只在内部自洽。
#[test]
fn ring_counters_are_exported_to_the_registry() {
    let registry = Registry::new();
    let io =
        IntCounterVec::new(Opts::new(RingMetrics::NAME, "test"), &["event"]).expect("counter vec");
    registry.register(Box::new(io.clone())).expect("register");
    let metrics = RingMetrics::from_vec(io.clone());

    let backend = Arc::new(MemoryBackend::new(1024));
    let hub = UsrbioHub::new(backend, None).with_metrics(metrics);
    let layout = IovLayout::new(4096, 4).expect("iov layout");
    let iov_region = TestRegion::new("usrbio-iov-metrics", layout.total_bytes());
    // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
    let view =
        unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
    view.initialize().expect("init");
    let iov_id = hub
        .create_iov(iov_region.handle_fd(), layout)
        .expect("create iov");

    let ring_layout = RingLayout::new(4).expect("ring layout");
    let ring_region = TestRegion::new("usrbio-ring-metrics", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("init");
    let doorbell = EventFd::create().expect("eventfd");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            0,
            100,
        )
        .expect("create ring");

    let (handle, _) = hub
        .open("default", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let reg_id = hub.register_file(&handle).expect("register");

    let mut allocator = IovAllocator::new(layout);
    let producer = ring.submission_producer();
    let consumer = ring.completion_consumer();
    for index in 0..2_u64 {
        let slice = allocator.reserve(2).expect("reserve");
        view.write_block(&slice, b"hi").expect("write block");
        producer
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id,
                len: 2,
                offset: index * 2,
                iov_offset: slice.iov_offset,
                userdata: 200 + index,
            })
            .expect("push");
    }
    doorbell.ring();
    for _ in 0..2 {
        assert_eq!(wait_for_completion(&consumer).status, 0);
    }

    assert_eq!(io.with_label_values(&["submitted"]).get(), 2);
    assert_eq!(io.with_label_values(&["completed"]).get(), 2);
    assert_eq!(io.with_label_values(&["failed"]).get(), 0);
    assert_eq!(io.with_label_values(&["bytes"]).get(), 4);
    assert!(io.with_label_values(&["wakeups"]).get() >= 1);
    // 指标名就是运维看到的接口名，固定住它。
    assert_eq!(RingMetrics::NAME, "afs_usrbio_ring_io_total");

    hub.unregister_file(reg_id).expect("unregister");
    hub.close(&handle).expect("close");
    hub.destroy_ring(ring_id).expect("destroy ring");
    hub.destroy_iov(iov_id).expect("destroy iov");
}

/// 一条环 + 它的 iov 与门铃，供批量语义的用例复用。
struct RingFixture {
    hub: UsrbioHub,
    backend: Arc<MemoryBackend>,
    layout: IovLayout,
    view: SharedIov,
    ring: SharedRing,
    doorbell: EventFd,
    iov_id: u64,
    ring_id: u64,
    _iov_region: TestRegion,
    _ring_region: TestRegion,
}

impl RingFixture {
    fn new(name: &str, io_depth: i32, timeout_ms: u32, blocks: u32) -> Self {
        Self::new_with_entries(name, 8, io_depth, timeout_ms, blocks)
    }

    fn new_with_entries(
        name: &str,
        entries: u32,
        io_depth: i32,
        timeout_ms: u32,
        blocks: u32,
    ) -> Self {
        let backend = Arc::new(MemoryBackend::new(16 * 1024));
        let hub = UsrbioHub::new(backend.clone(), None);
        let layout = IovLayout::new(4096, blocks).expect("iov layout");
        let iov_region = TestRegion::new(&format!("{name}-iov"), layout.total_bytes());
        // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
        let view =
            unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
        view.initialize().expect("iov init");
        let iov_id = hub
            .create_iov(iov_region.handle_fd(), layout)
            .expect("create iov");

        let ring_layout = RingLayout::new(entries).expect("ring layout");
        let ring_region = TestRegion::new(&format!("{name}-ring"), ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("ring init");
        let doorbell = EventFd::create().expect("eventfd");
        let ring_id = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                io_depth,
                timeout_ms,
            )
            .expect("create ring");
        Self {
            hub,
            backend,
            layout,
            view,
            ring,
            doorbell,
            iov_id,
            ring_id,
            _iov_region: iov_region,
            _ring_region: ring_region,
        }
    }

    /// 排队一项写；返回分到的 userdata。
    fn queue_write(
        &self,
        allocator: &mut IovAllocator,
        reg_id: u32,
        offset: u64,
        payload: &[u8],
        userdata: u64,
    ) {
        let slice = allocator.reserve(payload.len()).expect("reserve");
        self.view.write_block(&slice, payload).expect("write block");
        self.ring
            .submission_producer()
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id,
                len: payload.len() as u32,
                offset,
                iov_offset: slice.iov_offset,
                userdata,
            })
            .expect("push");
    }
}

/// `io_depth > 0`：必须等满才处理，不能拿半批就走。
#[test]
fn positive_io_depth_waits_for_the_full_batch() {
    let fixture = RingFixture::new("usrbio-depth-positive", 3, 0, 8);
    let (handle, _) = fixture
        .hub
        .open("default", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let reg_id = fixture.hub.register_file(&handle).expect("register");
    let mut allocator = IovAllocator::new(fixture.layout);
    let consumer = fixture.ring.completion_consumer();

    fixture.queue_write(&mut allocator, reg_id, 0, b"aaa", 1);
    fixture.queue_write(&mut allocator, reg_id, 3, b"bbb", 2);
    fixture.doorbell.ring();
    // 只有 2 个，未满 3 个：worker 必须继续等，不能提前执行。
    thread::sleep(Duration::from_millis(200));
    assert_eq!(consumer.ready(), 0, "未满批不得产生完成项");

    fixture.queue_write(&mut allocator, reg_id, 6, b"ccc", 3);
    fixture.doorbell.ring();
    let mut seen = Vec::new();
    for _ in 0..3 {
        let completion = wait_for_completion(&consumer);
        assert_eq!(completion.status, 0, "batch status");
        seen.push(completion.userdata);
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![1, 2, 3]);
    assert_eq!(
        &fixture.backend.files.lock().expect("files")[&FILE_INODE][..9],
        b"aaabbbccc"
    );

    let stats = fixture.hub.ring_stats(fixture.ring_id).expect("stats");
    assert_eq!(stats.submitted, 3);
    assert_eq!(stats.completed, 3);

    fixture.hub.unregister_file(reg_id).expect("unregister");
    fixture.hub.close(&handle).expect("close");
}

/// `io_depth < 0`：等满但受 `timeout_ms` 约束，到点必须放行半批。
#[test]
fn negative_io_depth_releases_a_partial_batch_on_timeout() {
    let fixture = RingFixture::new("usrbio-depth-negative", -4, 100, 8);
    let (handle, _) = fixture
        .hub
        .open("default", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let reg_id = fixture.hub.register_file(&handle).expect("register");
    let mut allocator = IovAllocator::new(fixture.layout);
    let consumer = fixture.ring.completion_consumer();

    fixture.queue_write(&mut allocator, reg_id, 0, b"xy", 11);
    fixture.queue_write(&mut allocator, reg_id, 2, b"zw", 12);
    fixture.doorbell.ring();

    // 2 < 4，但上界会到期，因此必须拿到这 2 项而不是一直等。
    let mut seen = Vec::new();
    for _ in 0..2 {
        let completion = wait_for_completion(&consumer);
        assert_eq!(completion.status, 0, "timeout batch status");
        seen.push(completion.userdata);
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![11, 12]);
    assert!(consumer.ready() == 0, "不应多出完成项");

    fixture.hub.unregister_file(reg_id).expect("unregister");
    fixture.hub.close(&handle).expect("close");
}

/// `io_depth` 的绝对值超过 worker 单周期执行批量上限（256）时，创建即拒绝：
/// 「必须攒满」的目标一旦高于单周期执行批量，攒满后剩下的提交永远到不了下一轮
/// 的门槛，环会停摆；把目标截断成 256 则是静默改写参数语义，同样不可接受。
#[test]
fn create_ring_rejects_io_depth_beyond_the_execution_batch_limit() {
    let backend = Arc::new(MemoryBackend::new(64));
    let hub = UsrbioHub::new(backend, None);
    let layout = IovLayout::new(4096, 2).expect("iov layout");
    let iov_region = TestRegion::new("usrbio-iov-depth-limit", layout.total_bytes());
    // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
    let view =
        unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
    view.initialize().expect("init");
    let iov_id = hub
        .create_iov(iov_region.handle_fd(), layout)
        .expect("create iov");

    // 项数给足（1024），排除 entries 上限的干扰：超范围的凑批目标仍必须被拒绝。
    // 环必须先初始化，否则失败会来自头部校验而不是 io_depth 校验。
    let ring_layout = RingLayout::new(1024).expect("ring layout");
    for io_depth in [512, 257, -257, -512, i32::MAX, i32::MIN] {
        let ring_region = TestRegion::new(
            &format!("usrbio-ring-depth-limit-{io_depth}"),
            ring_layout.total_bytes(),
        );
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("ring init");
        let doorbell = EventFd::create().expect("doorbell");
        let error = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                io_depth,
                200,
            )
            .expect_err("io_depth beyond the execution batch limit must be rejected");
        assert_eq!(
            error.code(),
            afs_error::NODE_VFS_INVALID,
            "io_depth {io_depth}"
        );
    }

    // 边界：绝对值恰为 256（单周期执行批量上限）仍然合法。
    let ring_region = TestRegion::new("usrbio-ring-depth-limit-ok", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("ring init");
    let doorbell = EventFd::create().expect("doorbell");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            256,
            200,
        )
        .expect("io_depth at the execution batch limit is allowed");
    hub.destroy_ring(ring_id).expect("destroy ring");
    hub.destroy_iov(iov_id).expect("destroy iov");
}

/// 边界行为：`io_depth = 256`（单周期执行批量上限）时「必须攒满」逐字生效——
/// 255 个不处理，第 256 个一到整批放行。
#[test]
fn io_depth_at_the_execution_batch_limit_fills_exactly() {
    let fixture = RingFixture::new_with_entries("usrbio-depth-256", 512, 256, 0, 512);
    let (handle, _) = fixture
        .hub
        .open("default", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let reg_id = fixture.hub.register_file(&handle).expect("register");
    let mut allocator = IovAllocator::new(fixture.layout);
    let consumer = fixture.ring.completion_consumer();

    for userdata in 1..=255_u64 {
        fixture.queue_write(&mut allocator, reg_id, (userdata - 1) * 3, b"xyz", userdata);
    }
    fixture.doorbell.ring();
    // 255 < 256：worker 必须继续等，不能拿半批就走。
    thread::sleep(Duration::from_millis(200));
    assert_eq!(consumer.ready(), 0, "未满 256 不得产生完成项");

    fixture.queue_write(&mut allocator, reg_id, 255 * 3, b"xyz", 256);
    fixture.doorbell.ring();
    let mut seen = Vec::new();
    for _ in 0..256 {
        let completion = wait_for_completion(&consumer);
        assert_eq!(completion.status, 0, "batch status");
        seen.push(completion.userdata);
    }
    seen.sort_unstable();
    assert_eq!(seen, (1..=256_u64).collect::<Vec<_>>());

    fixture.hub.unregister_file(reg_id).expect("unregister");
    fixture.hub.close(&handle).expect("close");
}

/// 多条环可以共享同一块 iov，各自的完成项与计数互不干扰。
#[test]
fn two_rings_share_one_iov_without_cross_talk() {
    let fixture = RingFixture::new("usrbio-multi-ring", 0, 100, 8);
    let (handle, _) = fixture
        .hub
        .open("default", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let reg_id = fixture.hub.register_file(&handle).expect("register");

    // 第二条环挂在同一个 iov 上。
    let ring_layout = RingLayout::new(4).expect("ring layout");
    let second_region = TestRegion::new("usrbio-multi-ring-2", ring_layout.total_bytes());
    // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
    let second_ring =
        unsafe { SharedRing::from_raw_parts(second_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
    second_ring.initialize().expect("init");
    let second_doorbell = EventFd::create().expect("eventfd");
    let second_id = fixture
        .hub
        .create_ring(
            RingFds {
                region: second_region.handle_fd(),
                doorbell: second_doorbell.try_clone_fd().expect("dup doorbell"),
                completion: test_completion_fd(),
            },
            ring_layout,
            fixture.iov_id,
            0,
            100,
        )
        .expect("create second ring");

    let mut allocator = IovAllocator::new(fixture.layout);
    fixture.queue_write(&mut allocator, reg_id, 0, b"r1", 21);
    let second_producer = second_ring.submission_producer();
    let slice = allocator.reserve(2).expect("reserve");
    fixture
        .view
        .write_block(&slice, b"r2")
        .expect("write block");
    second_producer
        .push(&Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id,
            len: 2,
            offset: 2,
            iov_offset: slice.iov_offset,
            userdata: 22,
        })
        .expect("push");

    fixture.doorbell.ring();
    second_doorbell.ring();

    let first = wait_for_completion(&fixture.ring.completion_consumer());
    assert_eq!(first.userdata, 21);
    let second = wait_for_completion(&second_ring.completion_consumer());
    assert_eq!(second.userdata, 22);
    assert_eq!(
        &fixture.backend.files.lock().expect("files")[&FILE_INODE][..4],
        b"r1r2"
    );

    // 计数按环隔离：每条环各自 1 项。
    assert_eq!(
        fixture
            .hub
            .ring_stats(fixture.ring_id)
            .expect("stats")
            .submitted,
        1
    );
    assert_eq!(
        fixture.hub.ring_stats(second_id).expect("stats").submitted,
        1
    );

    fixture.hub.unregister_file(reg_id).expect("unregister");
    fixture.hub.close(&handle).expect("close");
    fixture.hub.destroy_ring(fixture.ring_id).expect("destroy");
    fixture.hub.destroy_ring(second_id).expect("destroy");
}

fn posix_fd_fixture(flags: i32) -> (UsrbioHub, Arc<FdBridge>, afs_transport::usrbio_fd::Control) {
    let backend = Arc::new(MemoryBackend::new(4096));
    let ctx = process_identity();
    let handle = backend
        .open_with_options(
            &ctx,
            types::BackendInode { value: FILE_INODE },
            flags,
            types::OpenOptions::default(),
        )
        .unwrap();
    let hub = UsrbioHub::new(backend, None);
    let bridge = hub.fd_bridge();
    bridge.opened(
        7,
        2,
        types::BackendInode { value: FILE_INODE },
        handle,
        ctx.clone(),
        flags,
        false,
    );
    let key = [17; 16];
    let node = bridge.begin_session(&key, ctx.uid).unwrap();
    let mut operation = [3; 16];
    operation[..8].copy_from_slice(&1u64.to_le_bytes());
    let control = afs_transport::usrbio_fd::Control {
        flags: 0,
        node,
        session: key,
        operation,
        registration: 0,
    };
    (hub, bridge, control)
}

#[test]
fn posix_fd_import_preserves_access_and_ring_session() {
    use afs_transport::usrbio_fd as wire;
    let (hub, bridge, request) = posix_fd_fixture(libc::O_RDONLY);
    let (registered, _) = bridge
        .control(7, hub.identity.uid, wire::REGISTER, request)
        .unwrap();
    let import = hub.lock().registrations[&registered.registration]
        .imported
        .clone()
        .unwrap();
    let own = bridge.session(&request.session, hub.identity.uid).unwrap();
    assert!(import.begin(OP_READ, Some(&own)).is_ok());
    assert_eq!(
        import.begin(OP_WRITE, Some(&own)).err().unwrap().code(),
        afs_error::IO_PERMISSION_DENIED
    );
    let other = [18; 16];
    bridge.begin_session(&other, hub.identity.uid).unwrap();
    let other = bridge.session(&other, hub.identity.uid).unwrap();
    assert!(import.begin(OP_READ, Some(&other)).is_err());
    assert!(
        hub.unregister_file(registered.registration).is_err(),
        "generic unregister cannot bypass cached handoff"
    );
}

#[test]
fn posix_fd_registration_is_idempotent_and_cancellation_retires_identity() {
    use afs_transport::usrbio_fd as wire;
    let (hub, bridge, mut request) = posix_fd_fixture(libc::O_RDWR);
    let (first, _) = bridge
        .control(7, hub.identity.uid, wire::REGISTER, request)
        .unwrap();
    let (retry, _) = bridge
        .control(7, hub.identity.uid, wire::REGISTER, request)
        .unwrap();
    assert_eq!(first.registration, retry.registration);
    assert_eq!(hub.lock().registrations.len(), 1);
    let (_, work) = bridge
        .control(7, hub.identity.uid, wire::UNREGISTER, first)
        .unwrap();
    work.unwrap()();
    assert!(hub.lock().registrations.is_empty());
    assert!(matches!(
        bridge.control(7, hub.identity.uid, wire::REGISTER, request),
        Err(libc::ESTALE)
    ));
    request.operation[..8].copy_from_slice(&2u64.to_le_bytes());
    assert_eq!(
        bridge
            .control(7, hub.identity.uid, wire::UNREGISTER, request)
            .unwrap()
            .0
            .flags,
        0
    );
    assert!(matches!(
        bridge.control(7, hub.identity.uid, wire::REGISTER, request),
        Err(libc::ESTALE)
    ));
    request.operation[..8].copy_from_slice(&3u64.to_le_bytes());
    let (next, _) = bridge
        .control(7, hub.identity.uid, wire::REGISTER, request)
        .unwrap();
    assert!(next.registration > first.registration);
}

#[test]
fn posix_fd_final_close_fences_inflight_use_before_releasing() {
    use afs_transport::usrbio_fd as wire;
    let (hub, bridge, request) = posix_fd_fixture(libc::O_RDWR);
    let (registered, _) = bridge
        .control(7, hub.identity.uid, wire::REGISTER, request)
        .unwrap();
    let import = hub.lock().registrations[&registered.registration]
        .imported
        .clone()
        .unwrap();
    let own = bridge.session(&request.session, hub.identity.uid).unwrap();
    let using = import.begin(OP_WRITE, Some(&own)).unwrap();
    let (done, finished) = std::sync::mpsc::channel();
    let close = std::thread::spawn(move || {
        bridge.release_fuse(7);
        done.send(()).unwrap();
    });
    assert!(
        finished.recv_timeout(Duration::from_millis(20)).is_err(),
        "close waits for actual completion"
    );
    drop(using);
    finished.recv_timeout(Duration::from_secs(2)).unwrap();
    close.join().unwrap();
    assert!(import.begin(OP_READ, Some(&own)).is_err());
    assert!(hub.lock().registrations.is_empty());
}

#[test]
fn posix_fd_wrong_node_and_write_only_access_are_rejected() {
    use afs_transport::usrbio_fd as wire;
    let (hub, bridge, mut request) = posix_fd_fixture(libc::O_WRONLY);
    let mut wrong = request;
    wrong.node = [29; 16];
    assert!(matches!(
        bridge.control(7, hub.identity.uid, wire::REGISTER, wrong),
        Err(libc::EXDEV)
    ));
    assert!(
        bridge
            .begin_session(&[22; 16], hub.identity.uid.wrapping_add(1))
            .is_err()
    );
    assert!(bridge.check_control(3, 7).is_err());
    request = bridge
        .control(7, hub.identity.uid, wire::REGISTER, request)
        .unwrap()
        .0;
    let import = hub.lock().registrations[&request.registration]
        .imported
        .clone()
        .unwrap();
    let own = bridge.session(&request.session, hub.identity.uid).unwrap();
    assert!(import.begin(OP_READ, Some(&own)).is_err());
    assert!(import.begin(OP_WRITE, Some(&own)).is_ok());
    let (_, work) = bridge
        .control(7, hub.identity.uid, wire::UNREGISTER, request)
        .unwrap();
    assert!(
        import.begin(OP_WRITE, Some(&own)).is_err(),
        "closing rejects new execution immediately"
    );
    // Queue rejection/lost work must produce a retryable error, not permanent PENDING.
    drop(work);
    assert!(matches!(
        bridge.control(7, hub.identity.uid, wire::UNREGISTER, request),
        Err(libc::EIO)
    ));
    let (_, work) = bridge
        .control(7, hub.identity.uid, wire::UNREGISTER, request)
        .unwrap();
    work.unwrap()();
    assert!(hub.lock().registrations.is_empty());
}

#[test]
fn posix_fd_registration_numbers_never_wrap_to_an_old_submission() {
    let (hub, _, _) = posix_fd_fixture(libc::O_RDONLY);
    let mut state = hub.lock();
    state.next_registration = u32::MAX;
    assert_eq!(state.allocate_registration().unwrap(), u32::MAX);
    assert!(state.allocate_registration().is_err());
}

#[test]
fn posix_fd_cancelled_sessions_respect_the_per_open_budget() {
    use afs_transport::usrbio_fd as wire;
    let (hub, bridge, mut request) = posix_fd_fixture(libc::O_RDONLY);
    for index in 1..=257u64 {
        let mut key = [23; 16];
        key[..8].copy_from_slice(&index.to_le_bytes());
        bridge.begin_session(&key, hub.identity.uid).unwrap();
        request.session = key;
        assert_eq!(
            bridge
                .control(7, hub.identity.uid, wire::UNREGISTER, request)
                .unwrap()
                .0
                .flags,
            0
        );
    }
    assert!(matches!(
        bridge.control(7, hub.identity.uid, wire::REGISTER, request),
        Err(libc::EMFILE)
    ));
    assert!(hub.lock().registrations.is_empty());
}

struct ParallelProbe {
    inner: MemoryBackend,
    allowed: bool,
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    released: StdMutex<bool>,
    entered: std::sync::Condvar,
}
impl Backend for ParallelProbe {
    fn root_inode(&self) -> BackendInode {
        self.inner.root_inode()
    }
    fn lookup(
        &self,
        c: &types::RequestContext,
        p: BackendInode,
        n: &OsStr,
    ) -> Result<Entry, Error> {
        self.inner.lookup(c, p, n)
    }
    fn getattr(
        &self,
        c: &types::RequestContext,
        i: BackendInode,
        h: Option<FileHandle>,
    ) -> Result<FileAttributes, Error> {
        self.inner.getattr(c, i, h)
    }
    fn open_with_options(
        &self,
        c: &types::RequestContext,
        i: BackendInode,
        f: i32,
        o: OpenOptions,
    ) -> Result<FileHandle, Error> {
        self.inner.open_with_options(c, i, f, o)
    }
    fn flush(&self, _c: &types::RequestContext, _h: FileHandle) -> Result<(), Error> {
        Ok(())
    }
    fn release(&self, c: &types::RequestContext, h: FileHandle) -> Result<(), Error> {
        self.inner.release(c, h)
    }
    fn supports_parallel_reads(&self, _: FileHandle) -> bool {
        self.allowed
    }
    fn write(
        &self,
        c: &types::RequestContext,
        h: FileHandle,
        o: u64,
        b: &[u8],
    ) -> Result<usize, Error> {
        self.inner.write(c, h, o, b)
    }
    fn read(
        &self,
        c: &types::RequestContext,
        h: FileHandle,
        o: u64,
        b: &mut [u8],
    ) -> Result<usize, Error> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let mut released = self.released.lock().expect("gate");
        if active >= 2 {
            *released = true;
            self.entered.notify_all();
        }
        if !*released {
            let (mut state, _) = self
                .entered
                .wait_timeout(released, Duration::from_millis(150))
                .expect("gate wait");
            *state = true;
            self.entered.notify_all();
            released = state;
        }
        drop(released);
        let result = self.inner.read(c, h, o, b);
        self.active.fetch_sub(1, Ordering::SeqCst);
        result
    }
}
fn parallel_read_probe(allowed: bool, overlap: bool, with_write: bool) -> usize {
    let block = 1024 * 1024;
    let backend = Arc::new(ParallelProbe {
        inner: MemoryBackend::new(block * 4),
        allowed,
        active: std::sync::atomic::AtomicUsize::new(0),
        peak: std::sync::atomic::AtomicUsize::new(0),
        released: StdMutex::new(false),
        entered: std::sync::Condvar::new(),
    });
    {
        let mut files = backend.inner.files.lock().expect("files");
        for (i, b) in files
            .get_mut(&FILE_INODE)
            .expect("file")
            .chunks_mut(block)
            .enumerate()
        {
            b.fill((i + 1) as u8);
        }
    }
    let hub = UsrbioHub::new(backend.clone(), None);
    let layout = IovLayout::new(block, 8).expect("layout");
    let region = TestRegion::new("parallel-iov", layout.total_bytes());
    // SAFETY: mapping covers the layout and stays alive until worker join below.
    let view = unsafe { SharedIov::from_raw_parts(region.mapping.as_ptr(), layout) }.expect("view");
    view.initialize().expect("init");
    let iov_id = hub.create_iov(region.handle_fd(), layout).expect("iov");
    let ring_layout = RingLayout::new(8).expect("ring layout");
    let ring_region = TestRegion::new("parallel-ring", ring_layout.total_bytes());
    // SAFETY: ring mapping covers the layout and lives until destroy_ring joins.
    let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
        .expect("ring");
    ring.initialize().expect("init");
    let doorbell = EventFd::create().expect("bell");
    let ring_id = hub
        .create_ring(
            RingFds {
                region: ring_region.handle_fd(),
                doorbell: doorbell.try_clone_fd().expect("dup"),
                completion: test_completion_fd(),
            },
            ring_layout,
            iov_id,
            4,
            1000,
        )
        .expect("create ring");
    let (handle, _) = hub
        .open("default", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let reg = hub.register_file(&handle).expect("register");
    hub.lock().registrations[&reg]
        .read_cost
        .observe(block as u32, Duration::from_millis(1));
    let write_slice = afs_transport::iov::IovSlice {
        block: 2,
        iov_offset: layout.block_offset(2) as u64,
        len: block as u32,
    };
    if with_write {
        view.write_block(&write_slice, &vec![0xEE; block])
            .expect("write source");
    }
    for i in 0..4 {
        let op = if with_write && i == 2 {
            OP_WRITE
        } else {
            OP_READ
        };
        let offset = if with_write && i >= 2 { 0 } else { i * block };
        let bank = if overlap { 0 } else { i };
        ring.submission_producer()
            .push(&Submission {
                op,
                flags: 0,
                reg_id: reg,
                len: block as u32,
                offset: offset as u64,
                iov_offset: layout.block_offset(bank as u32) as u64,
                userdata: i as u64,
            })
            .expect("submit");
    }
    doorbell.ring();
    let cq = ring.completion_consumer();
    for _ in 0..4 {
        let c = wait_for_completion(&cq);
        assert_eq!(c.status, 0);
        assert_eq!(c.result, block as i64);
    }
    if !overlap {
        for i in [0usize, 1, 3] {
            let want = if with_write && i == 3 {
                0xEE
            } else {
                (i + 1) as u8
            };
            assert!(
                view.slice(layout.block_offset(i as u32) as u64, block as u64)
                    .expect("bytes")
                    .iter()
                    .all(|b| *b == want)
            );
        }
    }
    hub.unregister_file(reg).expect("unregister");
    hub.close(&handle).expect("close");
    hub.destroy_ring(ring_id).expect("join");
    hub.destroy_iov(iov_id).expect("destroy");
    assert_eq!(
        backend.active.load(Ordering::SeqCst),
        0,
        "all reads joined before unmapping"
    );
    backend.peak.load(Ordering::SeqCst)
}
#[test]
fn fast_read_samples_do_not_create_or_enable_the_pool() {
    let executor = ReadExecutor::default();
    let cost = ReadCost::default();
    cost.observe(4 * 1024 * 1024, Duration::from_micros(100));
    assert!(!cost.is_slow());
    assert!(executor.pool.get().is_none());
    cost.observe(4 * 1024 * 1024, Duration::from_millis(8));
    assert!(cost.is_slow());
    for _ in 0..64 {
        cost.observe(4 * 1024 * 1024, Duration::from_micros(100));
    }
    assert!(!cost.is_slow());
    assert!(executor.pool.get().is_none());
}
#[test]
fn read_latency_samples_are_registration_local() {
    let hub = UsrbioHub::new(Arc::new(MemoryBackend::new(4096)), None);
    let (handle, _) = hub
        .open("default", "/data.bin", OpenAccess::ReadOnly)
        .expect("open");
    let first = hub.register_file(&handle).expect("first");
    let second = hub.register_file(&handle).expect("second");
    let slow = hub.lock().registrations[&first].read_cost.clone();
    let fast = hub.lock().registrations[&second].read_cost.clone();
    slow.observe(4 * 1024 * 1024, Duration::from_millis(8));
    fast.observe(4 * 1024 * 1024, Duration::from_micros(100));
    assert!(slow.is_slow());
    assert!(!fast.is_slow());
    hub.unregister_file(first).expect("unregister");
    hub.unregister_file(second).expect("unregister");
    hub.close(&handle).expect("close");
}
#[test]
fn eligible_ring_reads_execute_concurrently() {
    let peak = parallel_read_probe(true, false, false);
    assert!(
        (2..=4).contains(&peak),
        "bounded actual parallelism, got {peak}"
    );
}
#[test]
fn unsupported_ring_reads_remain_serial() {
    assert_eq!(parallel_read_probe(false, false, false), 1);
}
#[test]
fn overlapping_iov_reads_remain_serial() {
    assert_eq!(parallel_read_probe(true, true, false), 1);
}
#[test]
fn parallel_read_groups_preserve_intervening_write_order() {
    assert!(parallel_read_probe(true, false, true) >= 2);
}

/// 批量入口：一次门铃写多项、再批量读回，区块必须全部归还。
#[tokio::test]
async fn local_sdk_batches_writes_and_reads() {
    use crate::node::api::local::{LocalApiOptions, serve_local_api_with_options};
    use crate::node::diagnostics::DiagnosticStorage;
    use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig};

    let temp = tempfile::tempdir().expect("tempdir");
    let socket_path = temp.path().join("afs-node.sock");
    let storage = DiagnosticStorage::new(temp.path().join("diagnostics")).expect("storage");
    let backend = Arc::new(MemoryBackend::new(4096));
    let server = serve_local_api_with_options(
        storage,
        &socket_path,
        LocalApiOptions {
            metrics_registry: None,
            dfs_backend: Some(backend.clone() as Arc<dyn Backend>),
            dfs_namespace: Some("default".to_owned()),
        },
    )
    .await
    .expect("serve local api");

    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
        .await
        .expect("connect");
    let file = client
        .open("/data.bin", DfsAccess::ReadWrite)
        .await
        .expect("open");
    let mut iov = client.create_iov(4096, 4).await.expect("create iov");
    let mut ring = client
        .create_ring(&iov, 8, 0, 500)
        .await
        .expect("create ring");
    let registration = client.register_file(&file).await.expect("register");

    let items: [(u64, &[u8]); 3] = [(0, b"first"), (64, b"second"), (128, b"third")];
    let written = ring
        .write_batch(&mut iov, &registration, &items)
        .await
        .expect("write batch");
    assert_eq!(written, vec![5, 6, 5]);
    // 三项各写各的区间；中间的空隙必须没被动过，否则说明某项越界覆盖了邻居。
    {
        let files = backend.files.lock().expect("files");
        let content = &files[&FILE_INODE];
        assert_eq!(&content[..5], b"first");
        assert_eq!(&content[64..70], b"second");
        assert_eq!(&content[128..133], b"third");
        assert!(
            content[5..64].iter().all(|byte| *byte == 0),
            "填充区被越界写入"
        );
        assert!(
            content[70..128].iter().all(|byte| *byte == 0),
            "填充区被越界写入"
        );
    }

    let mut first = [0_u8; 5];
    let mut second = [0_u8; 6];
    let mut third = [0_u8; 5];
    let mut reads: [(u64, &mut [u8]); 3] = [(0, &mut first), (64, &mut second), (128, &mut third)];
    let read = ring
        .read_batch(&mut iov, &registration, &mut reads)
        .await
        .expect("read batch");
    assert_eq!(read, vec![5, 6, 5]);
    assert_eq!(&first, b"first");
    assert_eq!(&second, b"second");
    assert_eq!(&third, b"third");

    // 全部结算后不应有任何区块还挂在在途状态。
    assert_eq!(iov.in_flight(), 0, "批量完成后区块必须归还");
    assert_eq!(iov.free_blocks(), 4, "所有区块应重新可用");

    client
        .unregister_file(registration)
        .await
        .expect("unregister");
    client.close(file.handle).await.expect("close");
    ring.destroy().await.expect("destroy ring");
    iov.destroy().await.expect("destroy iov");
    server.shutdown().await.expect("shutdown");
}

/// A client process that can be killed: `sleep` holds a pid until SIGKILL.
fn client_process() -> std::process::Child {
    std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("spawn client process")
}

fn owned_count(hub: &UsrbioHub) -> (usize, usize, usize, usize) {
    let state = hub.lock();
    (
        state.rings.len(),
        state.registrations.len(),
        state.opened.len(),
        state.iovs.len(),
    )
}

fn wait_owned(hub: &UsrbioHub, expected: (usize, usize, usize, usize)) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if owned_count(hub) == expected {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// R18: everything a client created is released once it is `kill -9`ed, in
/// the safe order: the ring worker stops (and fails what was queued) before
/// its registration and iov go, and the handle is flushed then released.
#[test]
fn a_killed_client_has_its_ring_registration_handle_and_iov_reclaimed() {
    let f = RingFixture::new("usrbio-reclaim", 0, 100, 4);
    let (handle, _) = f
        .hub
        .open("default", "/data.bin", OpenAccess::ReadWrite)
        .expect("open");
    let reg_id = f.hub.register_file(&handle).expect("register");
    let mut client = client_process();
    let pid = Some(i32::try_from(client.id()).expect("pid"));
    f.hub.adopt(pid, Owned::Handle(handle.clone()));
    f.hub.adopt(pid, Owned::Registration(reg_id));
    f.hub.adopt(pid, Owned::Iov(f.iov_id));
    f.hub.adopt(pid, Owned::Ring(f.ring_id));
    // A write queued but never run: the reclaimed ring must complete it as failed.
    let mut allocator = IovAllocator::new(f.layout);
    f.queue_write(&mut allocator, reg_id, 0, b"lost", 7);
    assert_eq!(owned_count(&f.hub), (1, 1, 1, 1));

    client.kill().expect("kill -9 the client");
    client.wait().expect("reap the client");

    assert!(
        wait_owned(&f.hub, (0, 0, 0, 0)),
        "a dead client's resources must be reclaimed: {:?}",
        owned_count(&f.hub)
    );
    assert!(f.hub.ring_stats(f.ring_id).is_none());
    let calls = f.backend.handle_calls.lock().expect("calls").clone();
    assert_eq!(calls, ["flush", "release"]);
    // Explicit calls on reclaimed ids report unknown ids, never panic.
    assert!(f.hub.destroy_ring(f.ring_id).is_err());
    assert!(f.hub.unregister_file(reg_id).is_err());
    assert!(f.hub.close(&handle).is_err());
    assert!(f.hub.destroy_iov(f.iov_id).is_err());
}

/// A live client keeps its resources, and one that released them explicitly
/// leaves nothing for the reclaim to touch.
#[test]
fn live_clients_keep_their_resources_and_explicit_release_is_not_repeated() {
    let f = RingFixture::new("usrbio-live", 0, 100, 4);
    let (handle, _) = f
        .hub
        .open("default", "/data.bin", OpenAccess::ReadOnly)
        .expect("open");
    let me = Some(i32::try_from(std::process::id()).expect("pid"));
    f.hub.adopt(me, Owned::Handle(handle.clone()));
    f.hub.adopt(me, Owned::Iov(f.iov_id));
    f.hub.adopt(me, Owned::Ring(f.ring_id));
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        owned_count(&f.hub),
        (1, 0, 1, 1),
        "a live client must not be reclaimed"
    );

    let mut client = client_process();
    let pid = Some(i32::try_from(client.id()).expect("pid"));
    let (other, _) = f
        .hub
        .open("default", "/data.bin", OpenAccess::ReadOnly)
        .expect("open");
    f.hub.adopt(pid, Owned::Handle(other.clone()));
    f.hub.close(&other).expect("explicit close");
    client.kill().expect("kill -9 the client");
    client.wait().expect("reap the client");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(owned_count(&f.hub), (1, 0, 1, 1));
    let calls = f.backend.handle_calls.lock().expect("calls").clone();
    assert_eq!(calls, ["flush", "release"], "the explicit close ran once");
}

/// A client that is already gone when its first resource is recorded is
/// reclaimed right away.
#[test]
fn a_client_gone_before_it_is_watched_is_reclaimed_at_once() {
    let backend = Arc::new(MemoryBackend::new(64));
    let hub = UsrbioHub::new(backend.clone(), None);
    let mut client = client_process();
    let pid = Some(i32::try_from(client.id()).expect("pid"));
    client.kill().expect("kill -9 the client");
    client.wait().expect("reap the client");
    let (handle, _) = hub
        .open("default", "/data.bin", OpenAccess::ReadOnly)
        .expect("open");
    hub.adopt(pid, Owned::Handle(handle));
    assert!(wait_owned(&hub, (0, 0, 0, 0)));
}
