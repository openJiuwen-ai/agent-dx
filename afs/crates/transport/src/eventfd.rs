//! 跨进程 eventfd 门铃。
//!
//! 共享环本身不需要内核参与：SDK 把提交写进共享内存，Node 的环 worker 去取。但「什么时候去取」
//! 需要一个唤醒机制。固定间隔轮询的代价是每条环每秒醒来两万次，而且每批多付最多一个间隔的
//! 延迟；对 MiB 级小 IO 来说这个延迟不可忽略。
//!
//! eventfd 可以通过 fd 传递在进程间共享：两边各持同一个 eventfd 的一个 fd 副本，SDK 写、
//! Node 等。本模块只提供这对操作，不参与环的布局。

#![allow(unsafe_code)]

use std::fmt;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Duration;

/// eventfd 的计数值固定是 8 字节。
const COUNTER_BYTES: usize = 8;

/// 门铃创建失败。
#[derive(Debug)]
pub enum EventFdError {
    /// 内核拒绝创建 eventfd。
    Create(std::io::Error),
}

impl fmt::Display for EventFdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Create(error) => write!(formatter, "failed to create the eventfd: {error}"),
        }
    }
}

impl std::error::Error for EventFdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Create(error) => Some(error),
        }
    }
}

/// 一个 eventfd：既能被写入（敲门），也能被阻塞等待（应门）。
///
/// 两端通常分属两个进程，因此读写都不保证对方在场：敲门失败只意味着唤醒丢失，等待超时也
/// 不代表对方没提交。调用方必须把两种结果都当作「可能没同步」处理。
#[derive(Debug)]
pub struct EventFd {
    fd: OwnedFd,
}

impl EventFd {
    /// 创建一个初始计数为 0、阻塞语义的 eventfd。
    pub fn create() -> Result<Self, EventFdError> {
        // SAFETY: eventfd 只接受整数标志，不涉及指针；返回 -1 时按错误处理。
        let fd = unsafe { libc::eventfd(0, 0) };
        if fd < 0 {
            return Err(EventFdError::Create(std::io::Error::last_os_error()));
        }
        // SAFETY: fd 由上面的 eventfd 调用新建，所有权在此刻转交给 OwnedFd。
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self { fd: owned })
    }

    /// 接管一个从 fd broker 收到的 fd。
    ///
    /// 不做「是不是 eventfd」的探测：普通文件读 8 字节同样会成功，这种检查只会给出虚假的
    /// 安全感。fd 的来源由 broker 的 token 与会话标识保证，调用方必须传入对端 [`EventFd`]
    /// 的副本；若传错对象，表现为 `wait` 立刻返回或永久阻塞，而不是数据错误。
    pub fn from_owned_fd(fd: OwnedFd) -> Self {
        Self { fd }
    }

    /// 复制一份 fd，用于通过 fd broker 交给另一个进程。
    pub fn try_clone_fd(&self) -> std::io::Result<OwnedFd> {
        self.fd.try_clone()
    }

    pub fn raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// 设为非阻塞。
    ///
    /// `AsyncFd` 要求注册的 fd 处于非阻塞模式：它靠 reactor 判断可读，但真正 `read` 时不能阻塞。
    pub fn set_nonblocking(&self) -> Result<(), EventFdError> {
        let fd = self.fd.as_raw_fd();
        // SAFETY: fd 由本结构持有且有效；F_GETFL/F_SETFL 只读写 fd 状态标志，不涉及指针。
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 {
            return Err(EventFdError::Create(std::io::Error::last_os_error()));
        }
        // SAFETY: 同上；O_NONBLOCK 与取回的既有标志按位或，不改变其他位。
        if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(EventFdError::Create(std::io::Error::last_os_error()));
        }
        Ok(())
    }

    /// 敲一次门铃。
    ///
    /// 失败不返回错误：门铃只是优化，丢了最多是对方晚一点看到共享环里的提交，不是数据错误。
    pub fn ring(&self) {
        let value: u64 = 1;
        // SAFETY: eventfd 要求恰好 8 字节写入；指针指向本栈帧内有效的 u64。
        let _ = unsafe {
            libc::write(
                self.fd.as_raw_fd(),
                (&value as *const u64).cast(),
                COUNTER_BYTES,
            )
        };
    }

    /// 等到有人敲门或超时；`None` 表示一直等。
    ///
    /// 返回是否真的被敲醒过。超时返回 `false` 不代表环里没有提交——调用方仍应再扫一次环。
    pub fn wait(&self, timeout: Option<Duration>) -> bool {
        let mut poll_fd = libc::pollfd {
            fd: self.fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let millis = match timeout {
            None => -1,
            Some(limit) => limit.as_millis().min(i32::MAX as u128) as i32,
        };
        // SAFETY: poll 只读数组里的一个有效 pollfd，超时值已收敛到 i32 范围内。
        let ready = unsafe { libc::poll(&mut poll_fd, 1, millis) };
        if ready <= 0 {
            return false;
        }
        self.drain();
        true
    }

    /// 把计数器读空，避免一次敲门被后续多次等待重复消费。
    pub fn drain(&self) {
        let mut counter = [0_u8; COUNTER_BYTES];
        // SAFETY: 读取缓冲正好 8 字节，指针指向本栈帧内的有效数组。
        let _ = unsafe {
            libc::read(
                self.fd.as_raw_fd(),
                counter.as_mut_ptr().cast(),
                COUNTER_BYTES,
            )
        };
    }

    /// 交出底层 fd（例如通过 fd broker 传给另一个进程）。
    pub fn into_owned_fd(self) -> OwnedFd {
        self.fd
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_and_wait_observe_each_other() {
        let doorbell = EventFd::create().expect("create");
        // 没人敲门时不阻塞：超时必须立刻返回 false。
        assert!(!doorbell.wait(Some(Duration::from_millis(10))));
        doorbell.ring();
        assert!(doorbell.wait(Some(Duration::from_millis(10))));
        // 计数已被读空，再次等待必须超时。
        assert!(!doorbell.wait(Some(Duration::from_millis(10))));
    }

    #[test]
    fn many_rings_collapse_into_one_wakeup() {
        let doorbell = EventFd::create().expect("create");
        for _ in 0..64 {
            doorbell.ring();
        }
        // eventfd 累加计数，一次 drain 就把 64 次敲门收掉。
        assert!(doorbell.wait(Some(Duration::from_millis(10))));
        assert!(!doorbell.wait(Some(Duration::from_millis(10))));
    }

    #[test]
    fn adopted_eventfd_shares_the_counter() {
        let doorbell = EventFd::create().expect("create");
        let copy = doorbell.try_clone_fd().expect("dup");
        let adopted = EventFd::from_owned_fd(copy);
        doorbell.ring();
        // 两个 fd 指向同一个 eventfd 对象，计数共享。
        assert!(adopted.wait(Some(Duration::from_millis(10))));
    }
}
