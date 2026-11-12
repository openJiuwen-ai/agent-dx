//! usrbio 共享提交/完成环。
//!
//! 与 3FS 的 `ior` 同构：一条环由「提交队列 SQ」和「完成队列 CQ」组成，客户端与
//! node 通过共享内存映射同一段字节，靠单调递增的原子 head/tail 做单生产者单消费者
//! 同步。全程无锁、无 CAS：SQ 的 head 只由生产者写、tail 只由消费者写，索引天然
//! 单调，回绕只在取 slot 下标时用掩码完成，因此「满」与「空」不会歧义。
//!
//! 并发契约（与 3FS 一致，必须由调用方保证）：
//!
//! - 一条环只有一个 SQ 生产者；多线程必须各自开环（见 3FS `UsrbIo.md` 的并发建议）。
//! - 一条环只有一个 SQ 消费者，即 node 侧环 worker。
//! - CQ 方向相反：node 是生产者、SDK 是消费者，同样各自单实例。
//! - 提交项引用的 iov 区块在收到对应完成项之前不得复用；环本身不管理 iov 生命周期。
//!
//! 本模块不发起任何系统调用，也不理解请求语义。「版本固定」「写序号」「权限」这类
//! 约束属于上层协议，环只负责把 64 字节的提交项安全交给对端、再把 64 字节的完成项
//! 交回来。

#![allow(unsafe_code)]

use std::{
    fmt, ptr,
    sync::atomic::{AtomicU64, Ordering},
};

/// 环头部魔数：`AFSIRING` 前缀，用于跨进程确认对端映射的是同一种结构。
pub const RING_MAGIC: u64 = 0x4146_5352_494e_4700;
/// 当前环布局版本。任何字段偏移或语义变化都必须同时抬高该值。
pub const RING_VERSION: u32 = 1;
/// 单个提交项/完成项的固定字节数，同时也是 cacheline 粒度。
pub const SLOT_BYTES: usize = 64;
/// 环最少的项数：至少能区分一次「满」与一次「空」。
pub const MIN_ENTRIES: u32 = 2;
/// 环最多的项数，防止一次映射过大或索引掩码溢出。
pub const MAX_ENTRIES: u32 = 65_536;
/// 攒批目标（`io_depth` 的绝对值）的支持上限。
///
/// 环 worker 每个执行周期最多消费这么多提交（node 侧的 `MAX_BATCH` 与之同源）。
/// 「必须攒满」的目标一旦超过单周期执行批量，攒满后剩下的提交永远到不了下一轮的
/// 门槛，环会停摆；把目标静默截断成上限则是改写参数语义。因此两端创建环时都直接
/// 拒绝绝对值超出该上限的 `io_depth`。
pub const MAX_IO_DEPTH: i32 = 256;

/// 提交项操作码：读。
pub const OP_READ: u32 = 1;
/// 提交项操作码：写。
pub const OP_WRITE: u32 = 2;

// 头部布局固定，每个需要独立所有权的原子量独占一条 cacheline，避免生产者与消费者
// 互相把对方的缓存行打失效。
const MAGIC_OFFSET: usize = 0;
const VERSION_OFFSET: usize = 8;
const ENTRIES_OFFSET: usize = 16;
const SQ_HEAD_OFFSET: usize = 64;
const SQ_TAIL_OFFSET: usize = 128;
const CQ_HEAD_OFFSET: usize = 192;
const CQ_TAIL_OFFSET: usize = 256;
const DOORBELL_OFFSET: usize = 320;
/// 头部总字节数，同时也是 SQ 第一项的起始偏移。
const HEADER_BYTES: usize = 384;

/// 环布局错误。
#[derive(Debug)]
pub enum RingError {
    /// 参数不合法。
    InvalidArgument {
        field: &'static str,
        reason: &'static str,
    },
    /// 共享内存基址没有按 cacheline 对齐。
    Misaligned,
    /// 头部尚未由创建方初始化。
    NotInitialized,
    /// 对端映射的结构与本端不一致。
    VersionMismatch { version: u32, entries: u32 },
    /// 提交队列已满，调用方应等待完成项后重试。
    SubmissionFull,
    /// 完成队列已满，消费者必须先取走完成项，不能继续处理提交项。
    CompletionFull,
}

impl fmt::Display for RingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument { field, reason } => {
                write!(formatter, "invalid {field}: {reason}")
            }
            Self::Misaligned => formatter.write_str("ring base is not cacheline aligned"),
            Self::NotInitialized => formatter.write_str("ring header is not initialized"),
            Self::VersionMismatch { version, entries } => {
                write!(
                    formatter,
                    "ring layout mismatch: version {version}, entries {entries}"
                )
            }
            Self::SubmissionFull => formatter.write_str("submission queue is full"),
            Self::CompletionFull => formatter.write_str("completion queue is full"),
        }
    }
}

impl std::error::Error for RingError {}

/// 环的内存布局：项数决定总字节数，两个队列紧跟头部之后。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RingLayout {
    entries: u32,
    sq_offset: usize,
    cq_offset: usize,
    total_bytes: usize,
}

impl RingLayout {
    /// 按项数计算布局。项数必须是 2 的幂，掩码取模才有意义。
    pub fn new(entries: u32) -> Result<Self, RingError> {
        if entries < MIN_ENTRIES {
            return Err(RingError::InvalidArgument {
                field: "entries",
                reason: "must be at least 2",
            });
        }
        if entries > MAX_ENTRIES {
            return Err(RingError::InvalidArgument {
                field: "entries",
                reason: "must not exceed 65536",
            });
        }
        if !entries.is_power_of_two() {
            return Err(RingError::InvalidArgument {
                field: "entries",
                reason: "must be a power of two",
            });
        }
        let entries = entries as usize;
        let sq_offset = HEADER_BYTES;
        let cq_offset = sq_offset + entries * SLOT_BYTES;
        let total_bytes = cq_offset + entries * SLOT_BYTES;
        Ok(Self {
            entries: entries as u32,
            sq_offset,
            cq_offset,
            total_bytes,
        })
    }

    /// 校验攒批策略与这条环的匹配性。
    ///
    /// `io_depth > 0` 表示「必须攒满」：目标大于项数时这批永远凑不满，环会永久停摆，
    /// 应当在创建时拒绝，而不是留下一条永远不工作的环。`io_depth <= 0` 的策略总能处理
    /// 已有提交，与项数无关。任何模式的绝对值都不得超过 [`MAX_IO_DEPTH`]（见该常量的
    /// 说明），超范围的值同样在创建时拒绝。
    pub fn validate_io_depth(self, io_depth: i32) -> Result<(), RingError> {
        // 绝对值用 unsigned_abs：i32::MIN 的 |x| 超出 i32 正数域，普通取反会溢出。
        if io_depth.unsigned_abs() > MAX_IO_DEPTH as u32 {
            return Err(RingError::InvalidArgument {
                field: "io_depth",
                reason: "batching target must not exceed 256 in absolute value",
            });
        }
        // io_depth 已限定为正数，i32 正值转 u32 不会溢出。
        if io_depth > 0 && io_depth as u32 > self.entries {
            return Err(RingError::InvalidArgument {
                field: "io_depth",
                reason: "must-fill batching cannot exceed the ring entry count",
            });
        }
        Ok(())
    }

    /// 项数。
    pub fn entries(self) -> u32 {
        self.entries
    }

    /// SQ 起始偏移。
    pub fn sq_offset(self) -> usize {
        self.sq_offset
    }

    /// CQ 起始偏移。
    pub fn cq_offset(self) -> usize {
        self.cq_offset
    }

    /// 整条环（头部 + SQ + CQ）占用的字节数，调用方据此创建共享内存。
    pub fn total_bytes(self) -> usize {
        self.total_bytes
    }
}

/// 一个提交项。
///
/// `iov_offset`/`len` 指向 iov 中的一段，`offset` 是文件内偏移，`reg_id` 是注册句柄
/// 编号。四者都由上层解释，环不校验语义。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Submission {
    pub op: u32,
    pub flags: u32,
    pub reg_id: u32,
    pub len: u32,
    pub offset: u64,
    pub iov_offset: u64,
    pub userdata: u64,
}

/// 一个完成项。
///
/// `status` 为 0 表示成功，否则是 AFS 的编码错误号；`result` 在成功时是实际字节数。
/// 把两者分开是为了让「部分成功」与「带错误码的失败」都能如实回传，不会互相覆盖。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Completion {
    pub userdata: u64,
    pub result: i64,
    pub write_seq: u64,
    pub status: u32,
    pub op: u32,
}

/// 与共享内存二进制布局一致的提交项。
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct RawSubmission {
    op: u32,
    flags: u32,
    reg_id: u32,
    len: u32,
    offset: u64,
    iov_offset: u64,
    userdata: u64,
    reserved: [u64; 3],
}

/// 与共享内存二进制布局一致的完成项。
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct RawCompletion {
    userdata: u64,
    result: i64,
    write_seq: u64,
    status: u32,
    op: u32,
    reserved: [u64; 4],
}

const _: () = assert!(std::mem::size_of::<RawSubmission>() == SLOT_BYTES);
const _: () = assert!(std::mem::size_of::<RawCompletion>() == SLOT_BYTES);
const _: () = assert!(HEADER_BYTES.is_multiple_of(SLOT_BYTES));

impl From<&Submission> for RawSubmission {
    fn from(submission: &Submission) -> Self {
        Self {
            op: submission.op,
            flags: submission.flags,
            reg_id: submission.reg_id,
            len: submission.len,
            offset: submission.offset,
            iov_offset: submission.iov_offset,
            userdata: submission.userdata,
            reserved: [0; 3],
        }
    }
}

impl From<RawSubmission> for Submission {
    fn from(raw: RawSubmission) -> Self {
        Self {
            op: raw.op,
            flags: raw.flags,
            reg_id: raw.reg_id,
            len: raw.len,
            offset: raw.offset,
            iov_offset: raw.iov_offset,
            userdata: raw.userdata,
        }
    }
}

impl From<&Completion> for RawCompletion {
    fn from(completion: &Completion) -> Self {
        Self {
            userdata: completion.userdata,
            result: completion.result,
            write_seq: completion.write_seq,
            status: completion.status,
            op: completion.op,
            reserved: [0; 4],
        }
    }
}

impl From<RawCompletion> for Completion {
    fn from(raw: RawCompletion) -> Self {
        Self {
            userdata: raw.userdata,
            result: raw.result,
            write_seq: raw.write_seq,
            status: raw.status,
            op: raw.op,
        }
    }
}

/// 映射在共享内存上的一条环。
///
/// 该类型只持有裸指针，所有状态都在共享内存里；因此 `Send`/`Sync` 的安全性完全依赖
/// 上面「单生产者单消费者」的并发契约：任意时刻只有约定的那个角色会写对应的原子量
/// 与 slot 区间。
pub struct SharedRing {
    base: *mut u8,
    layout: RingLayout,
}

// SAFETY: 环内全部可变状态都是原子量，或由原子索引分隔的 POD slot；按角色分发的
// 句柄保证同一 slot 区间在同一时刻只被一个角色访问。
unsafe impl Send for SharedRing {}
// SAFETY: 同上，多线程各自持有不同角色的句柄时不会发生数据竞争。
unsafe impl Sync for SharedRing {}

impl SharedRing {
    /// 在已映射的共享内存上构造环视图。
    ///
    /// # Safety
    ///
    /// - `base` 必须 64 字节对齐；
    /// - 该地址起必须至少可读写 `layout.total_bytes()` 字节；
    /// - 在环被双方使用期间，这块映射不得被移动、缩容或释放。
    pub unsafe fn from_raw_parts(base: *mut u8, layout: RingLayout) -> Result<Self, RingError> {
        if base.is_null() || !(base as usize).is_multiple_of(SLOT_BYTES) {
            return Err(RingError::Misaligned);
        }
        Ok(Self { base, layout })
    }

    /// 本环的布局。
    pub fn layout(&self) -> RingLayout {
        self.layout
    }

    fn atomic(&self, offset: usize) -> &AtomicU64 {
        // SAFETY: offset 均在 HEADER_BYTES 之内，且映射长度由 layout 保证。
        unsafe { &*self.base.add(offset).cast::<AtomicU64>() }
    }

    fn sq_slot_ptr(&self, index: u32) -> *mut RawSubmission {
        let offset = self.layout.sq_offset + index as usize * SLOT_BYTES;
        // SAFETY: index 由掩码限制在 entries 内，偏移必落在 SQ 区间。
        unsafe { self.base.add(offset).cast::<RawSubmission>() }
    }

    fn cq_slot_ptr(&self, index: u32) -> *mut RawCompletion {
        let offset = self.layout.cq_offset + index as usize * SLOT_BYTES;
        // SAFETY: 同上，偏移必落在 CQ 区间。
        unsafe { self.base.add(offset).cast::<RawCompletion>() }
    }

    /// 由创建方在把环交给对端之前调用一次：写入魔数、版本、项数，并把四个游标归零。
    ///
    /// 调用时必须还没有任何对端在读这条环，否则会覆盖对方的游标。
    pub fn initialize(&self) -> Result<(), RingError> {
        self.atomic(ENTRIES_OFFSET)
            .store(self.layout.entries as u64, Ordering::Relaxed);
        self.atomic(VERSION_OFFSET)
            .store(RING_VERSION as u64, Ordering::Relaxed);
        self.atomic(SQ_HEAD_OFFSET).store(0, Ordering::Relaxed);
        self.atomic(SQ_TAIL_OFFSET).store(0, Ordering::Relaxed);
        self.atomic(CQ_HEAD_OFFSET).store(0, Ordering::Relaxed);
        self.atomic(CQ_TAIL_OFFSET).store(0, Ordering::Relaxed);
        self.atomic(DOORBELL_OFFSET).store(0, Ordering::Relaxed);
        // 魔数最后写：对端读到正确魔数时，其余字段必然已经就绪。
        self.atomic(MAGIC_OFFSET)
            .store(RING_MAGIC, Ordering::Release);
        Ok(())
    }

    /// 校验对端映射的是同一版本、同一项数的环。
    pub fn validate(&self) -> Result<(), RingError> {
        let magic = self.atomic(MAGIC_OFFSET).load(Ordering::Acquire);
        if magic != RING_MAGIC {
            return Err(RingError::NotInitialized);
        }
        let version = self.atomic(VERSION_OFFSET).load(Ordering::Relaxed) as u32;
        let entries = self.atomic(ENTRIES_OFFSET).load(Ordering::Relaxed) as u32;
        if version != RING_VERSION || entries != self.layout.entries {
            return Err(RingError::VersionMismatch { version, entries });
        }
        Ok(())
    }

    /// 取提交侧句柄（SDK 使用）。每条环只允许一个。
    pub fn submission_producer(&self) -> SubmissionProducer<'_> {
        SubmissionProducer { ring: self }
    }

    /// 取完成侧句柄（SDK 使用）。每条环只允许一个。
    pub fn completion_consumer(&self) -> CompletionConsumer<'_> {
        CompletionConsumer { ring: self }
    }

    /// 取消费侧句柄（node 环 worker 使用）。每条环只允许一个。
    pub fn ring_consumer(&self) -> RingConsumer<'_> {
        RingConsumer { ring: self }
    }

    fn entries_mask(&self) -> u64 {
        self.layout.entries as u64 - 1
    }
}

/// 提交侧：写 SQ 的 head。
pub struct SubmissionProducer<'a> {
    ring: &'a SharedRing,
}

impl SubmissionProducer<'_> {
    /// 还能写入多少项（含正在被 node 处理、尚未回填完成项的部分）。
    pub fn capacity(&self) -> u32 {
        let head = self.ring.atomic(SQ_HEAD_OFFSET).load(Ordering::Relaxed);
        let tail = self.ring.atomic(SQ_TAIL_OFFSET).load(Ordering::Acquire);
        let used = head.wrapping_sub(tail) as u32;
        self.ring.layout.entries.saturating_sub(used)
    }

    /// 写入一个提交项并推进 head。
    ///
    /// 返回该项在 SQ 数组中的下标（`0..entries`）。调用方可用 `userdata` 自行把完成项
    /// 与请求对应起来，不需要依赖这个下标。
    pub fn push(&self, submission: &Submission) -> Result<u32, RingError> {
        let head = self.ring.atomic(SQ_HEAD_OFFSET).load(Ordering::Relaxed);
        let tail = self.ring.atomic(SQ_TAIL_OFFSET).load(Ordering::Acquire);
        if head.wrapping_sub(tail) >= self.ring.layout.entries as u64 {
            return Err(RingError::SubmissionFull);
        }
        let index = (head & self.ring.entries_mask()) as u32;
        let raw = RawSubmission::from(submission);
        // SAFETY: index 落在 SQ 区间，且该 slot 尚未被消费者看到（head 还没推进）。
        unsafe { ptr::write(self.ring.sq_slot_ptr(index), raw) };
        // Release：保证上面的 slot 写入先于 head 对消费者可见。
        self.ring
            .atomic(SQ_HEAD_OFFSET)
            .store(head.wrapping_add(1), Ordering::Release);
        Ok(index)
    }

    /// 敲门：把门铃计数加一，供 node 侧廉价判断「有新活」。
    ///
    /// 真正的唤醒（UDS 通知）由上层负责；本方法只更新共享内存里的计数。
    pub fn ring_doorbell(&self) -> u64 {
        self.ring
            .atomic(DOORBELL_OFFSET)
            .fetch_add(1, Ordering::Release)
            .wrapping_add(1)
    }
}

/// 消费侧：推进 SQ 的 tail、写 CQ 的 tail。
pub struct RingConsumer<'a> {
    ring: &'a SharedRing,
}

impl RingConsumer<'_> {
    /// 已提交但尚未回填完成项的项数。
    pub fn available(&self) -> u32 {
        let head = self.ring.atomic(SQ_HEAD_OFFSET).load(Ordering::Acquire);
        let tail = self.ring.atomic(SQ_TAIL_OFFSET).load(Ordering::Relaxed);
        head.wrapping_sub(tail) as u32
    }

    /// CQ 还能写入多少项。处理一批之前必须先确认它覆盖得住这一批。
    pub fn completion_space(&self) -> u32 {
        let tail = self.ring.atomic(CQ_TAIL_OFFSET).load(Ordering::Relaxed);
        let head = self.ring.atomic(CQ_HEAD_OFFSET).load(Ordering::Acquire);
        let used = tail.wrapping_sub(head) as u32;
        self.ring.layout.entries.saturating_sub(used)
    }

    /// 读取相对 tail 的第 `offset` 个提交项，不推进 tail。
    ///
    /// 推进只发生在 [`Self::complete`]/[`Self::complete_batch`]：这样未完成的项始终被
    /// 计入「已用」，生产者无法覆盖它们正在使用的 iov 区块，天然形成背压。
    pub fn peek_at(&self, offset: u32) -> Option<Submission> {
        let tail = self.ring.atomic(SQ_TAIL_OFFSET).load(Ordering::Relaxed);
        let head = self.ring.atomic(SQ_HEAD_OFFSET).load(Ordering::Acquire);
        if offset as u64 >= head.wrapping_sub(tail) {
            return None;
        }
        let index = ((tail + offset as u64) & self.ring.entries_mask()) as u32;
        // SAFETY: 该 slot 已被 head 覆盖、尚未被 tail 回收，生产者不会重写。
        let raw = unsafe { ptr::read(self.ring.sq_slot_ptr(index)) };
        Some(raw.into())
    }

    /// 回填一个完成项并回收一个提交槽位。
    pub fn complete(&self, completion: &Completion) -> Result<(), RingError> {
        self.complete_batch(std::slice::from_ref(completion))
    }

    /// 批量回填完成项：先写入全部 CQE，再一次性推进 SQ tail。
    ///
    /// 批量推进只做一次 Release store，避免逐项推进时的重复内存栅栏。
    pub fn complete_batch(&self, completions: &[Completion]) -> Result<(), RingError> {
        if completions.is_empty() {
            return Ok(());
        }
        if completions.len() > self.available() as usize {
            return Err(RingError::InvalidArgument {
                field: "completions",
                reason: "more completions than pending submissions",
            });
        }
        if completions.len() > self.completion_space() as usize {
            return Err(RingError::CompletionFull);
        }
        let mut tail = self.ring.atomic(CQ_TAIL_OFFSET).load(Ordering::Relaxed);
        for completion in completions {
            let index = (tail & self.ring.entries_mask()) as u32;
            let raw = RawCompletion::from(completion);
            // SAFETY: CQ slot 由消费者独占写入，下标掩码限制在 CQ 区间内。
            unsafe { ptr::write(self.ring.cq_slot_ptr(index), raw) };
            tail = tail.wrapping_add(1);
        }
        // Release：CQE 内容先于 tail 对 SDK 可见。
        self.ring
            .atomic(CQ_TAIL_OFFSET)
            .store(tail, Ordering::Release);
        let sq_tail = self.ring.atomic(SQ_TAIL_OFFSET).load(Ordering::Relaxed);
        // Release：本次处理读过的 iov 区块先于 tail 推进对生产者可见。
        self.ring.atomic(SQ_TAIL_OFFSET).store(
            sq_tail.wrapping_add(completions.len() as u64),
            Ordering::Release,
        );
        Ok(())
    }

    /// 读取最近一次门铃计数。node 侧轮询时用它做廉价的变化判断。
    pub fn doorbell(&self) -> u64 {
        self.ring.atomic(DOORBELL_OFFSET).load(Ordering::Acquire)
    }
}

/// 完成侧：推进 CQ 的 head（SDK 使用）。
pub struct CompletionConsumer<'a> {
    ring: &'a SharedRing,
}

impl CompletionConsumer<'_> {
    /// 已就绪的完成项数量。
    pub fn ready(&self) -> u32 {
        let tail = self.ring.atomic(CQ_TAIL_OFFSET).load(Ordering::Acquire);
        let head = self.ring.atomic(CQ_HEAD_OFFSET).load(Ordering::Relaxed);
        tail.wrapping_sub(head) as u32
    }

    /// 取走一个完成项并推进 head。
    pub fn pop(&self) -> Option<Completion> {
        let head = self.ring.atomic(CQ_HEAD_OFFSET).load(Ordering::Relaxed);
        let tail = self.ring.atomic(CQ_TAIL_OFFSET).load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let index = (head & self.ring.entries_mask()) as u32;
        // SAFETY: 该 CQE 已被 tail 覆盖、尚未被 head 回收，node 不会重写。
        let raw = unsafe { ptr::read(self.ring.cq_slot_ptr(index)) };
        // Release：读走 CQE 之后才允许 node 覆盖该 slot。
        self.ring
            .atomic(CQ_HEAD_OFFSET)
            .store(head.wrapping_add(1), Ordering::Release);
        Some(raw.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, Instant},
    };

    /// 64 字节对齐的堆缓冲区，模拟一段共享内存。
    #[repr(C, align(64))]
    struct Aligned16k([u8; 16 * 1024]);

    fn make_ring(entries: u32) -> (Box<Aligned16k>, SharedRing) {
        let layout = RingLayout::new(entries).expect("layout");
        assert!(
            layout.total_bytes() <= 16 * 1024,
            "test buffer must fit the layout"
        );
        let mut buffer = Box::new(Aligned16k([0u8; 16 * 1024]));
        // SAFETY: repr(align(64)) 保证 64 字节对齐，容量由上面的断言保证。
        let ring = unsafe { SharedRing::from_raw_parts(buffer.0.as_mut_ptr(), layout) }
            .expect("build ring");
        ring.initialize().expect("initialize");
        ring.validate().expect("validate");
        (buffer, ring)
    }

    #[test]
    fn layout_rejects_invalid_entry_counts() {
        assert!(RingLayout::new(0).is_err());
        assert!(RingLayout::new(1).is_err());
        assert!(RingLayout::new(3).is_err());
        assert!(RingLayout::new(7).is_err());
        assert!(RingLayout::new(MAX_ENTRIES + 1).is_err());

        let layout = RingLayout::new(64).expect("layout");
        assert_eq!(layout.entries(), 64);
        assert_eq!(layout.total_bytes(), HEADER_BYTES + 64 * SLOT_BYTES * 2);
        assert_eq!(layout.sq_offset() % SLOT_BYTES, 0);
        assert_eq!(layout.cq_offset() % SLOT_BYTES, 0);
    }

    #[test]
    fn push_and_complete_round_trip_preserves_fields() {
        let (_buffer, ring) = make_ring(8);
        let producer = ring.submission_producer();
        let consumer = ring.ring_consumer();
        let completions = ring.completion_consumer();

        let submission = Submission {
            op: OP_WRITE,
            flags: 3,
            reg_id: 7,
            len: 4096,
            offset: 1 << 30,
            iov_offset: 1024,
            userdata: 0xdead_beef,
        };
        assert_eq!(producer.push(&submission).expect("push"), 0);
        assert_eq!(consumer.available(), 1);

        let observed = consumer.peek_at(0).expect("peek");
        assert_eq!(observed, submission);
        assert!(consumer.peek_at(1).is_none());

        consumer
            .complete(&Completion {
                userdata: submission.userdata,
                result: 4096,
                write_seq: 11,
                status: 0,
                op: OP_WRITE,
            })
            .expect("complete");
        assert_eq!(consumer.available(), 0);

        let completion = completions.pop().expect("completion");
        assert_eq!(completion.userdata, submission.userdata);
        assert_eq!(completion.result, 4096);
        assert_eq!(completion.write_seq, 11);
        assert_eq!(completion.status, 0);
        assert_eq!(completion.op, OP_WRITE);
        assert!(completions.pop().is_none());
    }

    #[test]
    fn ring_capacity_is_bounded_and_reports_full() {
        let (_buffer, ring) = make_ring(4);
        let producer = ring.submission_producer();
        let consumer = ring.ring_consumer();

        assert_eq!(producer.capacity(), 4);
        for index in 0..4u64 {
            producer
                .push(&Submission {
                    userdata: index,
                    ..Submission::default()
                })
                .expect("push");
        }
        assert_eq!(producer.capacity(), 0);
        assert!(matches!(
            producer.push(&Submission::default()),
            Err(RingError::SubmissionFull)
        ));

        // 未回填完成项之前 tail 不推进，容量不会凭空恢复。
        assert_eq!(consumer.available(), 4);
        consumer
            .complete(&Completion {
                userdata: 0,
                ..Completion::default()
            })
            .expect("complete");
        assert_eq!(producer.capacity(), 1);
    }

    #[test]
    fn complete_batch_rejects_more_completions_than_pending() {
        let (_buffer, ring) = make_ring(4);
        let producer = ring.submission_producer();
        let consumer = ring.ring_consumer();

        for _ in 0..2 {
            producer.push(&Submission::default()).expect("push");
        }
        let completions = [Completion::default(); 3];
        assert!(matches!(
            consumer.complete_batch(&completions),
            Err(RingError::InvalidArgument { .. })
        ));
    }

    #[test]
    fn completion_queue_reports_full_when_sdk_stops_draining() {
        let (_buffer, ring) = make_ring(2);
        let producer = ring.submission_producer();
        let consumer = ring.ring_consumer();

        for _ in 0..2 {
            producer.push(&Submission::default()).expect("push");
        }
        // SDK 一次都没取走完成项，CQ 被填满。
        consumer
            .complete_batch(&[Completion::default(); 2])
            .expect("complete batch");
        assert_eq!(consumer.completion_space(), 0);
        assert_eq!(consumer.available(), 0);

        // 生产者可以继续提交（tail 已回收），但消费者必须先等 SDK 取走 CQE。
        producer.push(&Submission::default()).expect("push");
        assert!(matches!(
            consumer.complete(&Completion::default()),
            Err(RingError::CompletionFull)
        ));
    }

    #[test]
    fn indices_wrap_around_without_aliasing() {
        let (_buffer, ring) = make_ring(4);
        let producer = ring.submission_producer();
        let consumer = ring.ring_consumer();
        let completions = ring.completion_consumer();

        for round in 0..100u64 {
            producer
                .push(&Submission {
                    userdata: round,
                    ..Submission::default()
                })
                .expect("push");
            assert_eq!(consumer.peek_at(0).expect("peek").userdata, round);
            consumer
                .complete(&Completion {
                    userdata: round,
                    ..Completion::default()
                })
                .expect("complete");
            assert_eq!(completions.pop().expect("completion").userdata, round);
        }
    }

    #[test]
    fn validate_rejects_uninitialized_or_foreign_layout() {
        let layout = RingLayout::new(8).expect("layout");
        let mut buffer = Box::new(Aligned16k([0u8; 16 * 1024]));
        // SAFETY: 对齐与容量均满足前置条件。
        let ring = unsafe { SharedRing::from_raw_parts(buffer.0.as_mut_ptr(), layout) }
            .expect("build ring");
        assert!(matches!(ring.validate(), Err(RingError::NotInitialized)));

        ring.initialize().expect("initialize");
        ring.validate().expect("validate");

        // 用不同项数的布局复用同一段内存，必须被识别为不匹配。
        let other = RingLayout::new(16).expect("layout");
        // SAFETY: 16 项的布局同样不超过测试缓冲区容量。
        let foreign = unsafe { SharedRing::from_raw_parts(buffer.0.as_mut_ptr(), other) }
            .expect("build ring");
        assert!(matches!(
            foreign.validate(),
            Err(RingError::VersionMismatch { .. })
        ));
    }

    #[test]
    fn rejects_unaligned_base() {
        let layout = RingLayout::new(8).expect("layout");
        let mut buffer = Box::new(Aligned16k([0u8; 16 * 1024]));
        let base = buffer.0.as_mut_ptr();
        // SAFETY: 指针本身指向有效缓冲区；这里故意制造非 cacheline 对齐的基址。
        let shifted = unsafe { base.add(8) };
        // SAFETY: 即使非法，构造也必须安全地返回错误而不解引用。
        let result = unsafe { SharedRing::from_raw_parts(shifted, layout) };
        assert!(matches!(result, Err(RingError::Misaligned)));
    }

    #[test]
    fn single_producer_single_consumer_streams_without_loss() {
        const ITEMS: u64 = 20_000;
        const DEADLINE: Duration = Duration::from_secs(60);

        let (buffer, ring) = make_ring(32);
        let layout = ring.layout();
        let base = buffer.0.as_ptr() as usize;

        let consumed = std::sync::Arc::new(AtomicU64::new(0));

        let producer = std::thread::spawn(move || {
            // SAFETY: 生产者独占 SQ head 与 SQ slot 写入；缓冲区由外层保持存活。
            let ring =
                unsafe { SharedRing::from_raw_parts(base as *mut u8, layout) }.expect("build ring");
            let producer = ring.submission_producer();
            for sequence in 0..ITEMS {
                let submission = Submission {
                    op: OP_READ,
                    len: 4096,
                    userdata: sequence,
                    ..Submission::default()
                };
                let deadline = Instant::now() + DEADLINE;
                loop {
                    match producer.push(&submission) {
                        Ok(_) => break,
                        Err(RingError::SubmissionFull) if Instant::now() < deadline => {
                            std::hint::spin_loop();
                        }
                        Err(error) => panic!("push failed at {sequence}: {error}"),
                    }
                }
            }
        });

        let consumer_consumed = consumed.clone();
        let consumer = std::thread::spawn(move || {
            // SAFETY: 消费者独占 SQ tail 与 CQ 两个游标；与生产者角色不重叠。
            let ring =
                unsafe { SharedRing::from_raw_parts(base as *mut u8, layout) }.expect("build ring");
            let consumer = ring.ring_consumer();
            let completions = ring.completion_consumer();
            let deadline = Instant::now() + DEADLINE;
            let mut expected = 0;
            while expected < ITEMS {
                match consumer.peek_at(0) {
                    Some(submission) => {
                        assert_eq!(submission.userdata, expected, "submission order must hold");
                        consumer
                            .complete(&Completion {
                                userdata: submission.userdata,
                                result: submission.len as i64,
                                ..Completion::default()
                            })
                            .expect("complete");
                        assert_eq!(completions.pop().expect("completion").userdata, expected);
                        expected += 1;
                        consumer_consumed.store(expected, Ordering::Release);
                    }
                    None => {
                        assert!(Instant::now() < deadline, "consumer stalled at {expected}");
                        std::hint::spin_loop();
                    }
                }
            }
        });

        producer.join().expect("producer thread");
        consumer.join().expect("consumer thread");
        assert_eq!(consumed.load(Ordering::Acquire), ITEMS);
        drop(buffer);
    }
}
