//! 共享内存的 mmap 视图。
//!
//! `shm::SharedRegion` 的读写走 `pread`/`pwrite`，适合「一次搬运一段字节」的旧路径。
//! 持久 iov 与共享环不一样：它们要在同一块映射上反复读写不同位置，环的头部还要用原子
//! 操作推进；每次访问都发系统调用既慢也不满足原子语义。因此这里提供一份 mmap 视图。
//!
//! 映射本身不解释内容，也不做同步；谁负责写哪一段由上层协议约定（见 [`crate::iov`] 与
//! [`crate::ring`]）。

#![allow(unsafe_code)]

use std::{
    fmt,
    os::fd::{AsRawFd, OwnedFd},
    ptr,
};

use crate::shm::ShmError;

/// 一块已映射的共享内存。
///
/// 持有 fd 是为了让映射有效期的所有权清楚：fd 关闭不会撤销映射，但保留它可以让
/// 「这块内存来自哪个区域」在生命周期内可追溯，也避免调用方过早关闭 fd。
pub struct MappedRegion {
    base: *mut u8,
    len: usize,
    fd: OwnedFd,
}

// SAFETY: 映射区是进程共享内存，跨线程访问的互斥由上层协议保证；本类型只暴露裸指针
// 和不可变切片，不提供跨线程的可变别名。
unsafe impl Send for MappedRegion {}
// SAFETY: 同上，只读切片可跨线程共享。
unsafe impl Sync for MappedRegion {}

impl MappedRegion {
    /// 以读写方式映射整块 fd。
    ///
    /// `len` 必须等于对端声明的区域长度；映射长度和 fd 实际大小不一致会让越界访问变成
    /// SIGBUS，所以调用方先用 `shm::validate_region_fd` 校验，再由这里保证长度一致。
    pub fn map(fd: OwnedFd, len: usize) -> Result<Self, ShmError> {
        if len == 0 {
            return Err(ShmError::InvalidArgument {
                field: "len",
                reason: "must be greater than zero",
            });
        }
        // SAFETY: fd 由调用方保证有效且已按 len 设置大小；PROT/MAP 常量固定，
        // offset 为 0，返回值在下面显式与 MAP_FAILED 比较。
        let mapped = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if mapped == libc::MAP_FAILED {
            return Err(ShmError::Syscall("mmap", std::io::Error::last_os_error()));
        }
        Ok(Self {
            base: mapped.cast(),
            len,
            fd,
        })
    }

    /// 映射起始地址。
    pub fn as_ptr(&self) -> *mut u8 {
        self.base
    }

    /// 整块映射的只读视图。
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: base 指向长度为 len 的有效映射，生命周期受 &self 约束。
        unsafe { std::slice::from_raw_parts(self.base, self.len) }
    }

    /// 映射长度。
    pub fn len(&self) -> usize {
        self.len
    }

    /// 长度是否为 0；构造时已拒绝 0，因此恒为 false。
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// 映射来源 fd 的原始句柄，供再次传递或核对使用。
    pub fn raw_fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }
}

impl fmt::Debug for MappedRegion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MappedRegion")
            .field("base", &format_args!("{:p}", self.base))
            .field("len", &self.len)
            .finish()
    }
}

impl Drop for MappedRegion {
    fn drop(&mut self) {
        // SAFETY: base/len 是 mmap 成功时记录的一对参数，且只在这里撤销一次。
        unsafe {
            libc::munmap(self.base.cast(), self.len);
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::shm::SharedRegion;

    #[test]
    fn mapping_round_trips_through_the_shared_region() {
        let region = SharedRegion::create("mapped-test", 4096).expect("region");
        let mapped =
            MappedRegion::map(region.duplicate_fd_owned().expect("dup"), 4096).expect("map");
        assert_eq!(mapped.len(), 4096);
        assert!(!mapped.is_empty());

        // SAFETY: 映射长度为 4096，写入 5 字节在范围内。
        unsafe {
            ptr::copy_nonoverlapping(b"hello".as_ptr(), mapped.as_ptr(), 5);
        }
        assert_eq!(&mapped.as_slice()[..5], b"hello");
    }

    #[test]
    fn zero_length_mapping_is_rejected() {
        let region = SharedRegion::create("mapped-zero", 4096).expect("region");
        let error = MappedRegion::map(region.duplicate_fd_owned().expect("dup"), 0)
            .expect_err("zero length must fail");
        assert!(matches!(
            error,
            ShmError::InvalidArgument { field: "len", .. }
        ));
    }

    #[test]
    fn two_mappings_of_one_region_observe_each_other() {
        let region = SharedRegion::create("mapped-shared", 4096).expect("region");
        let first =
            MappedRegion::map(region.duplicate_fd_owned().expect("dup"), 4096).expect("map");
        let second =
            MappedRegion::map(region.duplicate_fd_owned().expect("dup"), 4096).expect("map");
        // SAFETY: 两个映射长度均为 4096，写入 4 字节在范围内。
        unsafe {
            ptr::copy_nonoverlapping(b"sync".as_ptr(), first.as_ptr(), 4);
        }
        assert_eq!(&second.as_slice()[..4], b"sync");
    }
}
