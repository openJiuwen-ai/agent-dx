//! usrbio 持久 iov：一块常驻共享内存，按固定大小切成区块。
//!
//! 与 3FS 的 `iov` 对应：应用把要写的正文直接放进这块内存，提交项只携带
//! `(iov_offset, len)`，node 侧据此拿到同一段字节的切片交给 DFS 后端，因此每次操作
//! 不再需要新建 memfd、传递 fd 或在线程间搬运数据。
//!
//! 两条必须守住的约束（与 3FS `UsrbIo.md` 一致）：
//!
//! - **一次操作不得跨区块**。跨区块意味着后端要做临时拼接，那就凭空多一次拷贝；由
//!   调用方拆成两个提交项，而不是在下面偷偷拼缓冲。
//! - **未收到完成项之前，该区块不得复用**。复用会让 node 读到已被改写的字节；
//!   分配器的 `reserve`/`release` 就是这个约束的显式表达。
//!
//! 本模块不做 I/O，也不解释请求语义；它只负责「一块共享内存的布局、校验和区块分配」。

#![allow(unsafe_code)]

use std::{
    fmt, ptr,
    sync::atomic::{AtomicU64, Ordering},
};

/// iov 头部魔数，用于跨进程确认对端映射的是同一种结构。
pub const IOV_MAGIC: u64 = 0x4146_5352_494f_5600;
/// 当前 iov 布局版本。字段偏移或语义变化必须同时抬高该值。
pub const IOV_VERSION: u32 = 1;
/// 头部字节数，同时也是第一个区块的起始偏移。
pub const IOV_HEADER_BYTES: usize = 64;
/// 单个区块的最小大小。取 4 KiB 是为了让后续可能的 O_DIRECT 直读具备对齐前提。
pub const MIN_BLOCK_SIZE: usize = 4096;
/// 单个区块的最大大小。
pub const MAX_BLOCK_SIZE: usize = 64 * 1024 * 1024;
/// 区块数上限，防止一次映射过大或下标溢出。
pub const MAX_BLOCKS: u32 = 1 << 20;

const MAGIC_OFFSET: usize = 0;
const VERSION_OFFSET: usize = 8;
const BLOCK_SIZE_OFFSET: usize = 16;
const BLOCKS_OFFSET: usize = 24;

/// iov 布局或访问错误。
#[derive(Debug, Eq, PartialEq)]
pub enum IovError {
    /// 参数不合法。
    InvalidArgument {
        field: &'static str,
        reason: &'static str,
    },
    /// 基址为空或未按 8 字节对齐，原子头部无法安全读写。
    Misaligned,
    /// 头部尚未初始化。
    NotInitialized,
    /// 对端布局与本端不一致。
    VersionMismatch {
        version: u32,
        block_size: u64,
        blocks: u64,
    },
    /// 请求区间越界。
    OutOfRange,
    /// 请求跨越了区块边界。
    SpansBlocks,
    /// 没有空闲区块，调用方应等待完成项后重试。
    Exhausted,
    /// 区块编号不合法或已经被释放。
    InvalidBlock(u32),
}

impl fmt::Display for IovError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument { field, reason } => {
                write!(formatter, "invalid {field}: {reason}")
            }
            Self::Misaligned => formatter.write_str("iov base is not 8-byte aligned"),
            Self::NotInitialized => formatter.write_str("iov header is not initialized"),
            Self::VersionMismatch {
                version,
                block_size,
                blocks,
            } => write!(
                formatter,
                "iov layout mismatch: version {version}, block_size {block_size}, blocks {blocks}"
            ),
            Self::OutOfRange => formatter.write_str("iov range is outside the region"),
            Self::SpansBlocks => {
                formatter.write_str("iov range spans a block boundary; split the request")
            }
            Self::Exhausted => formatter.write_str("no free iov block is available"),
            Self::InvalidBlock(block) => write!(formatter, "iov block {block} is invalid or freed"),
        }
    }
}

impl std::error::Error for IovError {}

/// iov 的布局。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IovLayout {
    block_size: usize,
    blocks: u32,
    total_bytes: usize,
}

impl IovLayout {
    /// 按区块大小与区块数计算布局。
    pub fn new(block_size: usize, blocks: u32) -> Result<Self, IovError> {
        if block_size < MIN_BLOCK_SIZE {
            return Err(IovError::InvalidArgument {
                field: "block_size",
                reason: "must be at least 4096 bytes",
            });
        }
        if block_size > MAX_BLOCK_SIZE {
            return Err(IovError::InvalidArgument {
                field: "block_size",
                reason: "must not exceed 64 MiB",
            });
        }
        if !block_size.is_power_of_two() {
            return Err(IovError::InvalidArgument {
                field: "block_size",
                reason: "must be a power of two",
            });
        }
        if blocks == 0 {
            return Err(IovError::InvalidArgument {
                field: "blocks",
                reason: "must be at least 1",
            });
        }
        if blocks > MAX_BLOCKS {
            return Err(IovError::InvalidArgument {
                field: "blocks",
                reason: "must not exceed 1048576",
            });
        }
        let data_bytes =
            block_size
                .checked_mul(blocks as usize)
                .ok_or(IovError::InvalidArgument {
                    field: "blocks",
                    reason: "block_size * blocks overflows usize",
                })?;
        let total_bytes =
            IOV_HEADER_BYTES
                .checked_add(data_bytes)
                .ok_or(IovError::InvalidArgument {
                    field: "blocks",
                    reason: "total size overflows usize",
                })?;
        Ok(Self {
            block_size,
            blocks,
            total_bytes,
        })
    }

    /// 区块大小。
    pub fn block_size(self) -> usize {
        self.block_size
    }

    /// 区块数。
    pub fn blocks(self) -> u32 {
        self.blocks
    }

    /// 整块 iov（头部 + 全部区块）的字节数。
    pub fn total_bytes(self) -> usize {
        self.total_bytes
    }

    /// 第 `block` 个区块的起始偏移。
    pub fn block_offset(self, block: u32) -> usize {
        IOV_HEADER_BYTES + block as usize * self.block_size
    }

    /// 校验一个 `(offset, len)` 区间：必须落在整块内存内，且不得跨区块。
    pub fn validate_range(self, iov_offset: u64, len: u64) -> Result<(), IovError> {
        if len == 0 {
            return Err(IovError::InvalidArgument {
                field: "len",
                reason: "must be greater than zero",
            });
        }
        let end = iov_offset
            .checked_add(len)
            .ok_or(IovError::InvalidArgument {
                field: "len",
                reason: "iov_offset + len overflows u64",
            })?;
        if end > self.total_bytes as u64 {
            return Err(IovError::OutOfRange);
        }
        if iov_offset < IOV_HEADER_BYTES as u64 {
            return Err(IovError::OutOfRange);
        }
        let first = (iov_offset - IOV_HEADER_BYTES as u64) / self.block_size as u64;
        let last = (end - 1 - IOV_HEADER_BYTES as u64) / self.block_size as u64;
        if first != last {
            return Err(IovError::SpansBlocks);
        }
        Ok(())
    }
}

/// 映射在共享内存上的 iov 视图。
///
/// 与 [`crate::ring::SharedRing`] 相同，只持有裸指针，所有状态都在共享内存里。跨线程
/// 共享时由上层保证「不同请求的区间互不重叠」：SDK 的分配器保证同一区块同一时刻只有
/// 一个在途请求，因此每个 `(offset, len)` 区间只有一个写者。
pub struct SharedIov {
    base: *mut u8,
    layout: IovLayout,
}

// SAFETY: 所有可变访问都通过显式裸指针交给调用方，并由调用方保证区间独占；本类型
// 自身只做算术与头部原子量读写。
unsafe impl Send for SharedIov {}
// SAFETY: 同上，不同线程只会访问互不重叠的区间。
unsafe impl Sync for SharedIov {}

impl SharedIov {
    /// 在已映射的共享内存上构造 iov 视图。
    ///
    /// # Safety
    ///
    /// - `base` 必须 8 字节对齐；
    /// - 该地址起必须至少可读写 `layout.total_bytes()` 字节；
    /// - 使用期间这块映射不得被移动、缩容或释放。
    pub unsafe fn from_raw_parts(base: *mut u8, layout: IovLayout) -> Result<Self, IovError> {
        if base.is_null() || !(base as usize).is_multiple_of(8) {
            return Err(IovError::Misaligned);
        }
        Ok(Self { base, layout })
    }

    /// 本 iov 的布局。
    pub fn layout(&self) -> IovLayout {
        self.layout
    }

    fn atomic(&self, offset: usize) -> &AtomicU64 {
        // SAFETY: offset 均在 IOV_HEADER_BYTES 内，映射长度由 layout 保证。
        unsafe { &*self.base.add(offset).cast::<AtomicU64>() }
    }

    /// 由创建方在把 iov 交给对端之前调用一次。
    pub fn initialize(&self) -> Result<(), IovError> {
        self.atomic(BLOCK_SIZE_OFFSET)
            .store(self.layout.block_size as u64, Ordering::Relaxed);
        self.atomic(BLOCKS_OFFSET)
            .store(self.layout.blocks as u64, Ordering::Relaxed);
        self.atomic(VERSION_OFFSET)
            .store(IOV_VERSION as u64, Ordering::Relaxed);
        // 魔数最后写：对端读到正确魔数时，其余字段必然已就绪。
        self.atomic(MAGIC_OFFSET)
            .store(IOV_MAGIC, Ordering::Release);
        Ok(())
    }

    /// 校验对端映射的是同一版本、同一布局的 iov。
    pub fn validate(&self) -> Result<(), IovError> {
        let magic = self.atomic(MAGIC_OFFSET).load(Ordering::Acquire);
        if magic != IOV_MAGIC {
            return Err(IovError::NotInitialized);
        }
        let version = self.atomic(VERSION_OFFSET).load(Ordering::Relaxed) as u32;
        let block_size = self.atomic(BLOCK_SIZE_OFFSET).load(Ordering::Relaxed);
        let blocks = self.atomic(BLOCKS_OFFSET).load(Ordering::Relaxed);
        if version != IOV_VERSION
            || block_size != self.layout.block_size as u64
            || blocks != self.layout.blocks as u64
        {
            return Err(IovError::VersionMismatch {
                version,
                block_size,
                blocks,
            });
        }
        Ok(())
    }

    /// 校验区间并返回只读字节切片。
    pub fn slice(&self, iov_offset: u64, len: u64) -> Result<&[u8], IovError> {
        self.layout.validate_range(iov_offset, len)?;
        // SAFETY: 区间已校验落在映射内，且调用方保证期间没有写者触碰该区间。
        Ok(unsafe { std::slice::from_raw_parts(self.base.add(iov_offset as usize), len as usize) })
    }

    /// 把字节写进一个已预约的区块。
    ///
    /// 区间由布局校验，`IovSlice` 来自分配器，因此写入不会越界也不会跨块。
    pub fn write_block(&self, slice: &IovSlice, data: &[u8]) -> Result<(), IovError> {
        if data.len() != slice.len as usize {
            return Err(IovError::InvalidArgument {
                field: "data",
                reason: "length does not match the reserved slice",
            });
        }
        let destination = self.write_ptr(slice.iov_offset, slice.len as u64)?;
        // SAFETY: write_ptr 已按布局校验区间落在映射内；目标区块由调用方独占，
        // 且源区间来自独立的 Rust slice，二者不重叠。
        unsafe {
            ptr::copy_nonoverlapping(data.as_ptr(), destination, data.len());
        }
        Ok(())
    }

    /// 校验区间并返回可写区间的裸指针。
    ///
    /// 返回裸指针而不是 `&mut [u8]`，是为了不在 `&self` 上伪造独占引用：调用方必须
    /// 用 `from_raw_parts_mut` 自行建立切片，并对「该区间归本次请求独占」负责。
    pub fn write_ptr(&self, iov_offset: u64, len: u64) -> Result<*mut u8, IovError> {
        self.layout.validate_range(iov_offset, len)?;
        // SAFETY: 区间已校验落在映射内；仅做指针加法，不构造引用。
        Ok(unsafe { self.base.add(iov_offset as usize) })
    }
}

/// SDK 侧的区块分配器。
///
/// 只做一件事：把「哪个区块当前空闲」这件事和「在途请求」绑在一起。区块只有在对应
/// 完成项被取走之后才会被 `release`，因此不会出现 node 正在读、应用已经改写的情况。
#[derive(Debug)]
pub struct IovAllocator {
    layout: IovLayout,
    used: Vec<u64>,
    cursor: u32,
    in_flight: u32,
}

impl IovAllocator {
    /// 按布局创建分配器，初始全部区块空闲。
    pub fn new(layout: IovLayout) -> Self {
        let words = layout.blocks.div_ceil(64) as usize;
        Self {
            layout,
            used: vec![0; words.max(1)],
            cursor: 0,
            in_flight: 0,
        }
    }

    /// 布局。
    pub fn layout(&self) -> IovLayout {
        self.layout
    }

    /// 当前空闲区块数。
    pub fn free_blocks(&self) -> u32 {
        let used: u32 = self.used.iter().map(|word| word.count_ones()).sum();
        self.layout.blocks - used
    }

    /// 在途请求数。
    pub fn in_flight(&self) -> u32 {
        self.in_flight
    }

    fn is_used(&self, block: u32) -> bool {
        let word = (block / 64) as usize;
        let bit = block % 64;
        self.used[word] & (1 << bit) != 0
    }

    fn set_used(&mut self, block: u32, value: bool) {
        let word = (block / 64) as usize;
        let bit = block % 64;
        if value {
            self.used[word] |= 1 << bit;
        } else {
            self.used[word] &= !(1 << bit);
        }
    }

    /// 为一个请求分配区块。
    ///
    /// `len` 超过一个区块时必须由调用方自行拆分；这里直接返回 [`IovError::SpansBlocks`]
    /// 对应的拒绝语义，避免在下面偷偷拼接缓冲。
    pub fn reserve(&mut self, len: usize) -> Result<IovSlice, IovError> {
        if len == 0 {
            return Err(IovError::InvalidArgument {
                field: "len",
                reason: "must be greater than zero",
            });
        }
        if len > self.layout.block_size {
            return Err(IovError::SpansBlocks);
        }
        let blocks = self.layout.blocks;
        for step in 0..blocks {
            let block = (self.cursor + step) % blocks;
            if !self.is_used(block) {
                self.set_used(block, true);
                self.cursor = (block + 1) % blocks;
                self.in_flight += 1;
                return Ok(IovSlice {
                    block,
                    iov_offset: self.layout.block_offset(block) as u64,
                    len: len as u32,
                });
            }
        }
        Err(IovError::Exhausted)
    }

    /// 释放一个区块。重复释放或编号非法都会报错，避免静默出现的双写。
    pub fn release(&mut self, slice: &IovSlice) -> Result<(), IovError> {
        if slice.block >= self.layout.blocks || !self.is_used(slice.block) {
            return Err(IovError::InvalidBlock(slice.block));
        }
        self.set_used(slice.block, false);
        self.in_flight = self.in_flight.saturating_sub(1);
        Ok(())
    }
}

/// 一个已分配的区块。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IovSlice {
    pub block: u32,
    pub iov_offset: u64,
    pub len: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout() -> IovLayout {
        IovLayout::new(4096, 4).expect("layout")
    }

    #[test]
    fn layout_rejects_invalid_parameters() {
        assert!(IovLayout::new(2048, 4).is_err());
        assert!(IovLayout::new(8192, 0).is_err());
        assert!(IovLayout::new(MAX_BLOCK_SIZE + 1, 1).is_err());
        assert!(IovLayout::new(4096, MAX_BLOCKS + 1).is_err());
        let valid = layout();
        assert_eq!(valid.total_bytes(), IOV_HEADER_BYTES + 4 * 4096);
        assert_eq!(valid.block_offset(0), IOV_HEADER_BYTES);
        assert_eq!(valid.block_offset(3), IOV_HEADER_BYTES + 3 * 4096);
    }

    #[test]
    fn range_validation_rejects_crossing_and_overflow() {
        let layout = layout();
        layout
            .validate_range(IOV_HEADER_BYTES as u64, 4096)
            .expect("full block");
        assert_eq!(
            layout.validate_range(IOV_HEADER_BYTES as u64, 4097),
            Err(IovError::SpansBlocks)
        );
        assert_eq!(
            layout.validate_range(IOV_HEADER_BYTES as u64 + 4096 - 8, 16),
            Err(IovError::SpansBlocks)
        );
        assert_eq!(
            layout.validate_range(IOV_HEADER_BYTES as u64, 0),
            Err(IovError::InvalidArgument {
                field: "len",
                reason: "must be greater than zero"
            })
        );
        assert_eq!(
            layout.validate_range(u64::MAX, 2),
            Err(IovError::InvalidArgument {
                field: "len",
                reason: "iov_offset + len overflows u64"
            })
        );
        assert_eq!(
            layout.validate_range(layout.total_bytes() as u64, 1),
            Err(IovError::OutOfRange)
        );
        assert_eq!(layout.validate_range(0, 8), Err(IovError::OutOfRange));
    }

    #[test]
    fn allocator_hands_out_distinct_blocks_and_backpressures() {
        let layout = layout();
        let mut allocator = IovAllocator::new(layout);
        assert_eq!(allocator.free_blocks(), 4);

        let first = allocator.reserve(4096).expect("reserve");
        let second = allocator.reserve(1024).expect("reserve");
        assert_ne!(first.block, second.block);
        assert_eq!(allocator.free_blocks(), 2);
        assert_eq!(allocator.in_flight(), 2);

        let third = allocator.reserve(16).expect("reserve");
        let fourth = allocator.reserve(16).expect("reserve");
        assert_eq!(allocator.free_blocks(), 0);
        assert_eq!(allocator.reserve(16), Err(IovError::Exhausted));

        allocator.release(&first).expect("release");
        assert_eq!(allocator.free_blocks(), 1);
        assert_eq!(allocator.in_flight(), 3);
        assert_eq!(allocator.release(&first), Err(IovError::InvalidBlock(0)));

        let fifth = allocator.reserve(4096).expect("reserve");
        assert_eq!(fifth.block, first.block);
        assert_ne!(fifth.block, third.block);
        assert_ne!(fifth.block, fourth.block);
        assert_ne!(fifth.block, second.block);
    }

    #[test]
    fn allocator_rejects_oversized_and_empty_requests() {
        let mut allocator = IovAllocator::new(layout());
        assert_eq!(allocator.reserve(4097), Err(IovError::SpansBlocks));
        assert!(matches!(
            allocator.reserve(0),
            Err(IovError::InvalidArgument { .. })
        ));
    }

    #[test]
    fn block_writes_land_at_the_reserved_offset() {
        let layout = layout();
        let mut allocator = IovAllocator::new(layout);
        let slice = allocator.reserve(8).expect("reserve");
        let mut aligned = Aligned::new(layout.total_bytes());
        // SAFETY: 基址按 8 字节对齐，容量等于布局总字节数。
        let iov = unsafe { SharedIov::from_raw_parts(aligned.as_mut_ptr(), layout) }.expect("iov");
        iov.initialize().expect("initialize");

        iov.write_block(&slice, b"usrbio!!").expect("write block");
        assert_eq!(iov.slice(slice.iov_offset, 8).expect("slice"), b"usrbio!!");
        assert!(matches!(
            iov.write_block(&slice, b"short"),
            Err(IovError::InvalidArgument { field: "data", .. })
        ));
    }

    /// 保证 8 字节对齐的测试缓冲区。
    struct Aligned(Vec<u64>);

    impl Aligned {
        fn new(bytes: usize) -> Self {
            Self(vec![0; bytes.div_ceil(8)])
        }
        fn as_mut_ptr(&mut self) -> *mut u8 {
            self.0.as_mut_ptr().cast()
        }
    }

    #[test]
    fn shared_iov_initializes_validates_and_rejects_foreign_layout() {
        let layout = layout();
        let mut buffer = Aligned::new(layout.total_bytes());
        // SAFETY: 基址 8 字节对齐，容量等于布局总字节数。
        let iov = unsafe { SharedIov::from_raw_parts(buffer.as_mut_ptr(), layout) }.expect("iov");
        assert!(matches!(iov.validate(), Err(IovError::NotInitialized)));
        iov.initialize().expect("initialize");
        iov.validate().expect("validate");

        let other = IovLayout::new(8192, 2).expect("layout");
        // SAFETY: 另一布局的总字节数与这里相同（64 + 16384），仍在容量之内。
        let foreign =
            unsafe { SharedIov::from_raw_parts(buffer.as_mut_ptr(), other) }.expect("iov");
        assert!(matches!(
            foreign.validate(),
            Err(IovError::VersionMismatch { .. })
        ));
    }

    #[test]
    fn shared_iov_slice_bounds_are_enforced() {
        let layout = layout();
        let mut buffer = Aligned::new(layout.total_bytes());
        // SAFETY: 基址对齐且容量充足。
        let iov = unsafe { SharedIov::from_raw_parts(buffer.as_mut_ptr(), layout) }.expect("iov");
        iov.initialize().expect("initialize");
        assert_eq!(
            iov.slice(IOV_HEADER_BYTES as u64, 4096)
                .expect("slice")
                .len(),
            4096
        );
        assert_eq!(
            iov.slice(IOV_HEADER_BYTES as u64, 8192),
            Err(IovError::SpansBlocks)
        );
        assert!(iov.write_ptr(0, 8).is_err());
    }
}
