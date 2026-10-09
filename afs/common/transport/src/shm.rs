//! 本机一次性 SHM fd 授权与传递。
//!
//! 这里服务 Local SDK 数据面：调用方创建 sealed memfd，控制面只传 grant，接收方
//! 通过 Unix socket + SCM_RIGHTS 拿到 fd，再用 `pread/pwrite` copy bytes。
//!
//! 第一版刻意不暴露“安全的 mmap slice”：另一个进程仍可能写同一 fd，Rust 无法用
//! 普通借用类型表达跨进程别名约束。byte-copy helper 更保守，也更容易解释生命周期。
//!
//! 重要边界：seal 只防止 fd 被 shrink/grow，避免 resize 导致 SIGBUS/越界；它不代表
//! 内容不可变。当前不是 zero-copy，只是避免内容走 gRPC payload。

#![allow(unsafe_code)]

use std::{
    collections::HashMap,
    ffi::CString,
    fmt,
    fs::{self, File},
    io::{self, IoSlice, IoSliceMut, Read, Write},
    mem,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::{
            fs::{FileExt, FileTypeExt, MetadataExt, PermissionsExt},
            net::{UnixListener, UnixStream},
        },
    },
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

// broker 握手是本机短路径：超时要足够短，避免取消/失败请求长期占用线程和 slot。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_REGION_BYTES: usize = 1024 * 1024;
const MAX_GRANTS: usize = 64;
const MAX_TOKEN_BYTES: usize = 256;
const MAX_PATH_BYTES: usize = 4096;
const REQUIRED_SEALS: i32 = libc::F_SEAL_SEAL | libc::F_SEAL_SHRINK | libc::F_SEAL_GROW;

#[derive(Debug)]
pub enum ShmError {
    InvalidArgument {
        field: &'static str,
        reason: &'static str,
    },
    InvalidToken,
    Protocol(&'static str),
    Unsupported,
    Syscall(&'static str, io::Error),
    Poisoned,
}

impl fmt::Display for ShmError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument { field, reason } => {
                write!(formatter, "invalid {field}: {reason}")
            }
            Self::InvalidToken => formatter.write_str("invalid or expired fd broker token"),
            Self::Protocol(reason) => write!(formatter, "fd broker protocol error: {reason}"),
            Self::Unsupported => formatter.write_str("shared-memory transport is unsupported"),
            Self::Syscall(name, error) => write!(formatter, "{name} failed: {error}"),
            Self::Poisoned => formatter.write_str("fd broker state is poisoned"),
        }
    }
}

impl std::error::Error for ShmError {}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BrokerToken(Vec<u8>);

impl BrokerToken {
    pub fn new(value: Vec<u8>) -> Result<Self, ShmError> {
        if value.is_empty() {
            return Err(ShmError::InvalidArgument {
                field: "token",
                reason: "must not be empty",
            });
        }
        if value.len() > MAX_TOKEN_BYTES {
            return Err(ShmError::InvalidArgument {
                field: "token",
                reason: "too long",
            });
        }
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
/// fd 领取请求。
///
/// token 是一次性随机串；session_id/region_id 用来避免不同操作误配。
/// 这不是远端身份认证，只是 trusted-host 内的最小授权键。
pub struct FdRequest {
    pub token: BrokerToken,
    pub session_id: u64,
    pub region_id: u64,
}

impl FdRequest {
    pub fn new(token: BrokerToken, session_id: u64, region_id: u64) -> Self {
        Self {
            token,
            session_id,
            region_id,
        }
    }
}

/// broker 保存的待领取 fd。
///
/// grant 被成功领取后会从 HashMap 删除，所以同一个 token 不能重放。TTL 用来清理
/// 调用方取消或 node 未连接造成的悬挂授权。
pub struct FdGrant {
    request: FdRequest,
    fd: OwnedFd,
    expires_at: Option<Instant>,
}

impl FdGrant {
    pub fn new(request: FdRequest, fd: OwnedFd) -> Self {
        Self {
            request,
            fd,
            expires_at: None,
        }
    }

    pub fn with_ttl(request: FdRequest, fd: OwnedFd, ttl: Duration) -> Self {
        Self {
            request,
            fd,
            expires_at: Some(Instant::now() + ttl),
        }
    }

    fn expired(&self) -> bool {
        self.expires_at
            .is_some_and(|deadline| Instant::now() > deadline)
    }
}

/// SDK 侧一次操作的 memfd 区域。
///
/// `SharedRegion::create` 会设置固定大小并加 seal；之后仍允许读写内容，只是不允许
/// 改变文件大小。读写 helper 当前都执行 copy。
pub struct SharedRegion {
    fd: OwnedFd,
    len: usize,
}

impl SharedRegion {
    pub fn create(name: &str, len: usize) -> Result<Self, ShmError> {
        // 1 MiB 是第一版 Local SDK 单次传输上限，防止本机 API 被误用成大对象通道。
        if len == 0 || len > MAX_REGION_BYTES {
            return Err(ShmError::InvalidArgument {
                field: "len",
                reason: "must be within 1..=1 MiB",
            });
        }
        let fd = create_memfd(name)?;
        set_file_len(fd.as_raw_fd(), len)?;
        add_required_seals(fd.as_raw_fd())?;
        Ok(Self { fd, len })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn duplicate_fd_owned(&self) -> Result<OwnedFd, ShmError> {
        duplicate_fd_owned(self.fd.as_raw_fd())
    }

    pub fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<(), ShmError> {
        check_range(offset, data.len(), self.len)?;
        write_fd_at(duplicate_fd_owned(self.fd.as_raw_fd())?, offset, data)
    }

    pub fn read_at(&self, offset: usize, len: usize) -> Result<Vec<u8>, ShmError> {
        check_range(offset, len, self.len)?;
        read_fd_at(duplicate_fd_owned(self.fd.as_raw_fd())?, offset, len)
    }
}

#[derive(Clone)]
/// 一次操作的 fd broker server。
///
/// SDK 先 bind 本机 socket，再把 socket path/token 放进 gRPC grant。node 连接该 socket
/// 后，broker 用 SCM_RIGHTS 发送 memfd fd。broker clone 共享同一个 inner，只有最后
/// 一个 owner drop 时才会删除 socket。
pub struct FdBrokerServer {
    inner: Arc<FdBrokerInner>,
}

struct FdBrokerInner {
    path: PathBuf,
    identity: SocketIdentity,
    listener: UnixListener,
    grants: Mutex<HashMap<FdRequest, FdGrant>>,
}

#[derive(Clone, Copy)]
struct SocketIdentity {
    dev: u64,
    ino: u64,
}

impl FdBrokerServer {
    pub fn bind(path: impl AsRef<Path>) -> Result<Self, ShmError> {
        // socket 必须是新路径，避免误删或接管其它进程的 socket。
        let path = path.as_ref().to_path_buf();
        if path.as_os_str().as_encoded_bytes().len() > MAX_PATH_BYTES {
            return Err(ShmError::InvalidArgument {
                field: "path",
                reason: "too long",
            });
        }
        if path.exists() {
            return Err(ShmError::InvalidArgument {
                field: "path",
                reason: "socket already exists",
            });
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| ShmError::Syscall("create_dir_all", error))?;
        }
        let listener =
            UnixListener::bind(&path).map_err(|error| ShmError::Syscall("bind", error))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .map_err(|error| ShmError::Syscall("set_permissions", error))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| ShmError::Syscall("set_nonblocking", error))?;
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| ShmError::Syscall("metadata", error))?;
        let identity = SocketIdentity {
            dev: metadata.dev(),
            ino: metadata.ino(),
        };
        Ok(Self {
            inner: Arc::new(FdBrokerInner {
                path,
                identity,
                listener,
                grants: Mutex::new(HashMap::new()),
            }),
        })
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn register(&self, grant: FdGrant) -> Result<(), ShmError> {
        // pending grant 有上限；过期 grant 在注册新 grant 时顺手清理。
        let mut grants = self.inner.grants.lock().map_err(|_| ShmError::Poisoned)?;
        grants.retain(|_, grant| !grant.expired());
        if grants.len() >= MAX_GRANTS || grants.contains_key(&grant.request) {
            return Err(ShmError::Protocol("grant limit or duplicate grant"));
        }
        grants.insert(grant.request.clone(), grant);
        Ok(())
    }

    pub fn serve_one(&self) -> Result<(), ShmError> {
        // 只服务一次 accept。SDK worker 必须 join 这个线程，确保错误/取消路径也收尾。
        let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
        loop {
            match self.inner.listener.accept() {
                Ok((stream, _)) => return self.handle(stream),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    wait_fd_ready(self.inner.listener.as_raw_fd(), libc::POLLIN, deadline)?;
                }
                Err(error) => return Err(ShmError::Syscall("accept", error)),
            }
        }
    }

    fn handle(&self, stream: UnixStream) -> Result<(), ShmError> {
        // 收到请求后立即 remove grant：无论发送 fd 成功与否，同一 token 都不能再用。
        stream
            .set_nonblocking(true)
            .map_err(|error| ShmError::Syscall("set_nonblocking", error))?;
        let mut stream = DeadlineStream::new(stream, Instant::now() + HANDSHAKE_TIMEOUT);
        let request = read_request(&mut stream)?;
        let grant = {
            let mut grants = self.inner.grants.lock().map_err(|_| ShmError::Poisoned)?;
            match grants.remove(&request) {
                Some(grant) if !grant.expired() => grant,
                Some(_) | None => return Err(ShmError::InvalidToken),
            }
        };
        validate_sealed_fd(grant.fd.as_raw_fd(), 0)?;
        send_fd(stream.stream(), grant.fd.as_raw_fd())
    }
}

impl Drop for FdBrokerInner {
    fn drop(&mut self) {
        let _ = remove_owned_socket_sync(&self.path, self.identity);
    }
}

pub struct FdBrokerClient;

impl FdBrokerClient {
    pub fn request_fd(path: impl AsRef<Path>, request: &FdRequest) -> Result<OwnedFd, ShmError> {
        // node 端调用：连接 SDK broker，写入领取请求，然后等待一个完整 fd。
        let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
        let stream = connect_before(path.as_ref(), deadline)?;
        stream
            .set_nonblocking(true)
            .map_err(|error| ShmError::Syscall("set_nonblocking", error))?;
        let mut stream = DeadlineStream::new(stream, deadline);
        write_request(&mut stream, request)?;
        recv_fd_before(stream.stream(), deadline)
    }
}

/// 从收到的 memfd fd copy 出 bytes。
///
/// 会验证大小与 seal；返回 Vec 表明当前实现不是 zero-copy。
pub fn read_fd_at(fd: OwnedFd, offset: usize, len: usize) -> Result<Vec<u8>, ShmError> {
    let required_len = offset.checked_add(len).ok_or(ShmError::InvalidArgument {
        field: "range",
        reason: "overflows",
    })?;
    validate_sealed_fd(fd.as_raw_fd(), required_len)?;
    let file = File::from(fd);
    let mut data = vec![0; len];
    file.read_exact_at(&mut data, offset as u64)
        .map_err(|error| ShmError::Syscall("pread", error))?;
    Ok(data)
}

/// 把 bytes copy 到收到的 memfd fd。
///
/// 用于 read 路径由 node 回填 SDK buffer；seal 不阻止写内容，只阻止 resize。
pub fn write_fd_at(fd: OwnedFd, offset: usize, data: &[u8]) -> Result<(), ShmError> {
    let required_len = offset
        .checked_add(data.len())
        .ok_or(ShmError::InvalidArgument {
            field: "range",
            reason: "overflows",
        })?;
    validate_sealed_fd(fd.as_raw_fd(), required_len)?;
    let file = File::from(fd);
    file.write_all_at(data, offset as u64)
        .map_err(|error| ShmError::Syscall("pwrite", error))
}

pub fn duplicate_fd_owned(raw_fd: RawFd) -> Result<OwnedFd, ShmError> {
    // SAFETY: fcntl only consumes the integer descriptor; the kernel validates it and
    // returns a fresh duplicate.
    let duplicated = unsafe { libc::fcntl(raw_fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicated < 0 {
        return Err(ShmError::Syscall(
            "fcntl(F_DUPFD_CLOEXEC)",
            io::Error::last_os_error(),
        ));
    }
    // SAFETY: F_DUPFD_CLOEXEC succeeded; this fresh descriptor is transferred into exactly
    // one owner.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

fn create_memfd(name: &str) -> Result<OwnedFd, ShmError> {
    let name = CString::new(name).map_err(|_| ShmError::InvalidArgument {
        field: "name",
        reason: "contains NUL byte",
    })?;
    #[cfg(target_os = "linux")]
    {
        // SAFETY: name is a live NUL-terminated CString; memfd_create retains no Rust pointer.
        let fd = unsafe {
            libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
        };
        if fd < 0 {
            return Err(ShmError::Syscall(
                "memfd_create",
                io::Error::last_os_error(),
            ));
        }
        // SAFETY: memfd_create succeeded; its fresh descriptor is transferred into
        // exactly one owner.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = name;
        Err(ShmError::Unsupported)
    }
}

fn set_file_len(fd: RawFd, len: usize) -> Result<(), ShmError> {
    let len = i64::try_from(len).map_err(|_| ShmError::InvalidArgument {
        field: "len",
        reason: "too large",
    })?;
    // SAFETY: ftruncate consumes an integer descriptor and checked off_t length; the kernel
    // validates the descriptor.
    let result = unsafe { libc::ftruncate(fd, len) };
    if result < 0 {
        return Err(ShmError::Syscall("ftruncate", io::Error::last_os_error()));
    }
    Ok(())
}

// 添加 shrink/grow/seal 三类 seal：防止大小变化和后续撤销 seal，
// 但不添加 F_SEAL_WRITE，因为 read 路径需要 node 写入 target memfd。
fn add_required_seals(fd: RawFd) -> Result<(), ShmError> {
    // SAFETY: F_ADD_SEALS takes an integer bitmask and no pointer; kernel errors are propagated.
    let result = unsafe { libc::fcntl(fd, libc::F_ADD_SEALS, REQUIRED_SEALS) };
    if result < 0 {
        return Err(ShmError::Syscall(
            "fcntl(F_ADD_SEALS)",
            io::Error::last_os_error(),
        ));
    }
    validate_seals(fd)
}

// 接收方重新验证 fd：不能信任 gRPC grant 里的 length/path，也不能信任对端没换 fd。
fn validate_sealed_fd(fd: RawFd, required_len: usize) -> Result<(), ShmError> {
    if required_len > MAX_REGION_BYTES {
        return Err(ShmError::InvalidArgument {
            field: "range",
            reason: "exceeds 1 MiB transfer limit",
        });
    }
    validate_file_len(fd, required_len)?;
    validate_seals(fd)
}

fn validate_file_len(fd: RawFd, required_len: usize) -> Result<(), ShmError> {
    let mut stat = mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: stat points to writable, properly aligned storage for one libc::stat
    // throughout this call.
    let result = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
    if result < 0 {
        return Err(ShmError::Syscall("fstat", io::Error::last_os_error()));
    }
    // SAFETY: successful fstat initialized every field before this value is read.
    let stat = unsafe { stat.assume_init() };
    let file_len = usize::try_from(stat.st_size).map_err(|_| ShmError::InvalidArgument {
        field: "fd",
        reason: "negative size",
    })?;
    if required_len > file_len {
        return Err(ShmError::InvalidArgument {
            field: "fd",
            reason: "range exceeds fd size",
        });
    }
    Ok(())
}

fn validate_seals(fd: RawFd) -> Result<(), ShmError> {
    // SAFETY: F_GET_SEALS takes no pointer; the kernel validates the integer descriptor.
    let seals = unsafe { libc::fcntl(fd, libc::F_GET_SEALS) };
    if seals < 0 {
        return Err(ShmError::Syscall(
            "fcntl(F_GET_SEALS)",
            io::Error::last_os_error(),
        ));
    }
    if seals & REQUIRED_SEALS != REQUIRED_SEALS {
        return Err(ShmError::InvalidArgument {
            field: "fd",
            reason: "missing required seals",
        });
    }
    Ok(())
}

fn check_range(offset: usize, len: usize, capacity: usize) -> Result<(), ShmError> {
    let end = offset.checked_add(len).ok_or(ShmError::InvalidArgument {
        field: "range",
        reason: "overflows",
    })?;
    if end > capacity {
        return Err(ShmError::InvalidArgument {
            field: "range",
            reason: "exceeds region length",
        });
    }
    Ok(())
}

// broker 私有小协议：AFS1 + token length + token + session + region。
// 保留版本字节，避免后续扩展时老进程把新格式误解析成合法请求。
fn write_request(stream: &mut DeadlineStream, request: &FdRequest) -> Result<(), ShmError> {
    let token = request.token.as_bytes();
    let token_len = u32::try_from(token.len()).map_err(|_| ShmError::InvalidArgument {
        field: "token",
        reason: "too long",
    })?;
    stream.write_all(b"AFS1")?;
    stream.write_all(&token_len.to_be_bytes())?;
    stream.write_all(token)?;
    stream.write_all(&request.session_id.to_be_bytes())?;
    stream.write_all(&request.region_id.to_be_bytes())
}

fn read_request(stream: &mut DeadlineStream) -> Result<FdRequest, ShmError> {
    let mut version = [0; 4];
    stream.read_exact(&mut version)?;
    if &version != b"AFS1" {
        return Err(ShmError::Protocol("unsupported broker protocol"));
    }
    let mut len_buf = [0; 4];
    stream.read_exact(&mut len_buf)?;
    let token_len = u32::from_be_bytes(len_buf) as usize;
    if token_len == 0 || token_len > MAX_TOKEN_BYTES {
        return Err(ShmError::Protocol("invalid token length"));
    }
    let mut token = vec![0; token_len];
    stream.read_exact(&mut token)?;
    let mut session = [0; 8];
    let mut region = [0; 8];
    stream.read_exact(&mut session)?;
    stream.read_exact(&mut region)?;
    Ok(FdRequest::new(
        BrokerToken::new(token)?,
        u64::from_be_bytes(session),
        u64::from_be_bytes(region),
    ))
}

// DeadlineStream 给阻塞式 UnixStream 操作套上 poll deadline，避免 broker 线程永久等待。
struct DeadlineStream {
    stream: UnixStream,
    deadline: Instant,
}

impl DeadlineStream {
    fn new(stream: UnixStream, deadline: Instant) -> Self {
        Self { stream, deadline }
    }

    fn stream(&self) -> &UnixStream {
        &self.stream
    }

    fn read_exact(&mut self, mut buf: &mut [u8]) -> Result<(), ShmError> {
        while !buf.is_empty() {
            match self.stream.read(buf) {
                Ok(0) => return Err(ShmError::Protocol("unexpected EOF")),
                Ok(bytes) => {
                    let tmp = buf;
                    buf = &mut tmp[bytes..];
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    wait_fd_ready(self.stream.as_raw_fd(), libc::POLLIN, self.deadline)?;
                }
                Err(error) => return Err(ShmError::Syscall("read", error)),
            }
        }
        Ok(())
    }

    fn write_all(&mut self, mut buf: &[u8]) -> Result<(), ShmError> {
        while !buf.is_empty() {
            match self.stream.write(buf) {
                Ok(0) => return Err(ShmError::Protocol("short write")),
                Ok(bytes) => buf = &buf[bytes..],
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    wait_fd_ready(self.stream.as_raw_fd(), libc::POLLOUT, self.deadline)?;
                }
                Err(error) => return Err(ShmError::Syscall("write", error)),
            }
        }
        Ok(())
    }
}

fn wait_fd_ready(fd: RawFd, events: i16, deadline: Instant) -> Result<(), ShmError> {
    let now = Instant::now();
    if now >= deadline {
        return Err(ShmError::Protocol("deadline exceeded"));
    }
    let timeout = deadline.saturating_duration_since(now);
    let timeout_ms = i32::try_from(timeout.as_millis().max(1)).unwrap_or(i32::MAX);
    let mut pollfd = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    // SAFETY: pollfd is one initialized writable pollfd and remains live for the synchronous call.
    let result = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
    if result < 0 {
        return Err(ShmError::Syscall("poll", io::Error::last_os_error()));
    }
    if result == 0 {
        return Err(ShmError::Protocol("deadline exceeded"));
    }
    Ok(())
}

fn connect_before(path: &Path, deadline: Instant) -> Result<UnixStream, ShmError> {
    use nix::sys::socket::{AddressFamily, SockFlag, SockType, UnixAddr, connect, socket};
    let address = UnixAddr::new(path).map_err(|e| ShmError::Syscall("address", e.into()))?;
    let fd = socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::SOCK_NONBLOCK | SockFlag::SOCK_CLOEXEC,
        None,
    )
    .map_err(|e| ShmError::Syscall("socket", e.into()))?;
    match connect(fd.as_raw_fd(), &address) {
        Ok(()) => (),
        Err(nix::errno::Errno::EINPROGRESS) => {
            wait_fd_ready(fd.as_raw_fd(), libc::POLLOUT, deadline)?;
            let error = nix::sys::socket::getsockopt(&fd, nix::sys::socket::sockopt::SocketError)
                .map_err(|e| ShmError::Syscall("getsockopt", e.into()))?;
            if error != 0 {
                return Err(ShmError::Syscall(
                    "connect",
                    io::Error::from_raw_os_error(error),
                ));
            }
        }
        Err(e) => return Err(ShmError::Syscall("connect", e.into())),
    }
    Ok(UnixStream::from(fd))
}

fn send_fd(stream: &UnixStream, fd: RawFd) -> Result<(), ShmError> {
    use nix::sys::socket::{ControlMessage, MsgFlags, sendmsg};
    let bytes = [0u8];
    let iov = [IoSlice::new(&bytes)];
    let fds = [fd];
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    loop {
        match sendmsg::<()>(
            stream.as_raw_fd(),
            &iov,
            &[ControlMessage::ScmRights(&fds)],
            MsgFlags::MSG_NOSIGNAL | MsgFlags::MSG_DONTWAIT,
            None,
        ) {
            Ok(1) => return Ok(()),
            Ok(_) => return Err(ShmError::Protocol("short fd send")),
            Err(nix::errno::Errno::EAGAIN) => {
                wait_fd_ready(stream.as_raw_fd(), libc::POLLOUT, deadline)?
            }
            Err(e) => return Err(ShmError::Syscall("sendmsg", e.into())),
        }
    }
}

// 接收 SCM_RIGHTS fd。异常消息里的多余 fd 会先被 OwnedFd 接管再随 Vec drop 关闭，
// 防止恶意/buggy 对端通过多 fd 消息泄漏描述符。
fn recv_fd_before(stream: &UnixStream, deadline: Instant) -> Result<OwnedFd, ShmError> {
    use nix::sys::socket::{ControlMessageOwned, MsgFlags, recvmsg};
    loop {
        let mut byte = [0u8];
        let mut iov = [IoSliceMut::new(&mut byte)];
        // Linux permits at most 253 SCM_RIGHTS descriptors per message. Provision
        // room for all of them so malformed multi-fd replies can be closed safely.
        let mut control = nix::cmsg_space!([RawFd; 256]);
        let message = match recvmsg::<()>(
            stream.as_raw_fd(),
            &mut iov,
            Some(&mut control),
            MsgFlags::MSG_CMSG_CLOEXEC | MsgFlags::MSG_DONTWAIT,
        ) {
            Ok(message) => message,
            Err(nix::errno::Errno::EAGAIN) => {
                wait_fd_ready(stream.as_raw_fd(), libc::POLLIN, deadline)?;
                continue;
            }
            Err(e) => return Err(ShmError::Syscall("recvmsg", e.into())),
        };
        let mut descriptors = Vec::new();
        for item in message
            .cmsgs()
            .map_err(|e| ShmError::Syscall("ancillary", e.into()))?
        {
            if let ControlMessageOwned::ScmRights(fds) = item {
                for fd in fds {
                    // recvmsg transfers ownership of each newly installed fd to us.
                    // SAFETY: recvmsg installed each SCM_RIGHTS descriptor
                    // for this receiver; each is adopted once.
                    descriptors.push(unsafe { OwnedFd::from_raw_fd(fd) });
                }
            }
        }
        if message.bytes != 1
            || descriptors.len() != 1
            || message.flags.contains(MsgFlags::MSG_CTRUNC)
        {
            return Err(ShmError::Protocol("expected exactly one complete fd grant"));
        }
        return Ok(descriptors.pop().expect("one fd checked"));
    }
}

// 只删除 dev/ino 匹配的 socket，避免路径被复用后误删别人创建的新 socket。
fn remove_owned_socket_sync(path: &Path, identity: SocketIdentity) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_socket()
                && metadata.dev() == identity.dev
                && metadata.ino() == identity.ino =>
        {
            fs::remove_file(path)
        }
        Ok(_) | Err(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs::OpenOptions, os::fd::IntoRawFd, thread};

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "afs-shm-{name}-{}-{}.sock",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        ))
    }

    fn request(id: u64) -> FdRequest {
        FdRequest::new(
            BrokerToken::new(format!("token-{id}").into_bytes()).unwrap(),
            id,
            id,
        )
    }

    #[test]
    fn shared_region_copies_bytes_through_fd() {
        let mut region = SharedRegion::create("afs-test", 8).unwrap();
        region.write_at(0, b"abcdefgh").unwrap();
        assert_eq!(region.read_at(2, 3).unwrap(), b"cde");
        let fd = region.duplicate_fd_owned().unwrap();
        write_fd_at(fd, 1, b"ZZ").unwrap();
        assert_eq!(region.read_at(0, 4).unwrap(), b"aZZd");
    }

    #[test]
    fn fd_broker_rejects_replay_and_removes_owned_socket_on_last_drop() {
        let path = test_path("replay");
        let server = FdBrokerServer::bind(&path).unwrap();
        let region = SharedRegion::create("afs-test", 4).unwrap();
        let request = request(1);
        server
            .register(FdGrant::new(
                request.clone(),
                region.duplicate_fd_owned().unwrap(),
            ))
            .unwrap();

        let first = {
            let broker = server.clone();
            thread::spawn(move || broker.serve_one())
        };
        let fd = FdBrokerClient::request_fd(&path, &request).unwrap();
        assert!(fd.as_raw_fd() >= 0);
        first.join().unwrap().unwrap();

        let second = {
            let broker = server.clone();
            thread::spawn(move || broker.serve_one())
        };
        let err = FdBrokerClient::request_fd(&path, &request).unwrap_err();
        assert!(matches!(
            err,
            ShmError::Protocol(_) | ShmError::Syscall(_, _)
        ));
        assert!(matches!(
            second.join().unwrap(),
            Err(ShmError::InvalidToken)
        ));

        let clone = server.clone();
        drop(server);
        assert!(path.exists());
        drop(clone);
        assert!(!path.exists());
    }

    #[test]
    fn expired_grant_is_rejected() {
        let path = test_path("expired");
        let server = FdBrokerServer::bind(&path).unwrap();
        let region = SharedRegion::create("afs-test", 4).unwrap();
        let request = request(2);
        server
            .register(FdGrant::with_ttl(
                request.clone(),
                region.duplicate_fd_owned().unwrap(),
                Duration::from_millis(1),
            ))
            .unwrap();
        thread::sleep(Duration::from_millis(10));
        let broker = server.clone();
        let handle = thread::spawn(move || broker.serve_one());
        let err = FdBrokerClient::request_fd(&path, &request).unwrap_err();
        assert!(matches!(
            err,
            ShmError::Protocol(_) | ShmError::Syscall(_, _)
        ));
        assert!(matches!(
            handle.join().unwrap(),
            Err(ShmError::InvalidToken)
        ));
    }

    #[test]
    fn byte_helpers_reject_unsealed_and_oversized_fds() {
        let path = std::env::temp_dir().join(format!("afs-shm-unsealed-{}", std::process::id()));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(8).unwrap();
        // SAFETY: name is a live NUL-terminated CString; memfd_create retains no Rust pointer.
        // SAFETY: into_raw_fd relinquishes the File ownership, which is immediately
        // transferred to OwnedFd.
        let fd = unsafe { OwnedFd::from_raw_fd(file.into_raw_fd()) };
        let error = read_fd_at(fd, 0, 1).unwrap_err();
        assert!(matches!(
            error,
            ShmError::Syscall(_, _) | ShmError::InvalidArgument { .. }
        ));
        let _ = fs::remove_file(&path);

        let region = SharedRegion::create("afs-test", 8).unwrap();
        let error = read_fd_at(region.duplicate_fd_owned().unwrap(), 0, 9).unwrap_err();
        assert!(matches!(
            error,
            ShmError::InvalidArgument { field: "fd", .. }
        ));
    }

    #[test]
    fn unexpected_multiple_fds_are_rejected() {
        use nix::sys::socket::{ControlMessage, MsgFlags, sendmsg};
        let (sender, receiver) = UnixStream::pair().unwrap();
        let region = SharedRegion::create("afs-test", 8).unwrap();
        let fd = region.duplicate_fd_owned().unwrap();
        let descriptors = [fd.as_raw_fd(), fd.as_raw_fd()];
        sendmsg::<()>(
            sender.as_raw_fd(),
            &[IoSlice::new(&[0])],
            &[ControlMessage::ScmRights(&descriptors)],
            MsgFlags::empty(),
            None,
        )
        .unwrap();
        assert!(matches!(
            recv_fd_before(&receiver, Instant::now() + HANDSHAKE_TIMEOUT),
            Err(ShmError::Protocol("expected exactly one complete fd grant"))
        ));
    }

    #[test]
    fn region_size_and_pending_grants_are_bounded() {
        assert!(SharedRegion::create("too-large", MAX_REGION_BYTES + 1).is_err());
        let server = FdBrokerServer::bind(test_path("bounded")).unwrap();
        let region = SharedRegion::create("afs-test", 8).unwrap();
        for id in 0..MAX_GRANTS {
            server
                .register(FdGrant::new(
                    request(id as u64),
                    region.duplicate_fd_owned().unwrap(),
                ))
                .unwrap();
        }
        assert!(
            server
                .register(FdGrant::new(
                    request(1000),
                    region.duplicate_fd_owned().unwrap()
                ))
                .is_err()
        );
    }

    #[test]
    fn serve_one_times_out_without_client() {
        let path = test_path("timeout");
        let server = FdBrokerServer::bind(&path).unwrap();
        let error = server.serve_one().unwrap_err();
        assert!(matches!(error, ShmError::Protocol("deadline exceeded")));
    }
}
