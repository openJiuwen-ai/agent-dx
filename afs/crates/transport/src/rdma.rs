//! RDMA 传输机制边界。
//!
//! 这一层只是 Rust 对 libibverbs 的很薄封装：创建 PD/CQ/QP，注册一块 MR，
//! 暴露本端描述符，并执行单边 RDMA READ/WRITE。它不是 native 文件系统、
//! 不是后台独立进程，也不理解路径、inode、OwnerFs、DFS 或 Proto 语义。
//!
//! 分层关系：
//! - gRPC/proto 仍承载“读哪个文件、写哪个文件、offset/len 是多少”等命令；
//! - 本模块只承载“把当前 endpoint 的本地 MR 和对端 MR 之间搬 len 字节”；
//! - 文件成功、失败、是否可重试，由 node/rpc/data.rs 与 peer.rs 判定。
//!
//! 重要生命周期：post 成功不是完成，只有 CQ completion 成功才代表 DMA 完成。
//! 调用方取消等待不能缩短 MR/QP/CQ 的生命周期；在途操作完成或 endpoint 安全
//! 关闭前，内存不能复用、注销或释放。请求编号也不能代替 DMA 围栏。

#![allow(unsafe_code)]

use std::ops::Deref;

#[cfg(has_native_rdma)]
use std::{
    ffi::CString,
    ptr::NonNull,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

pub const INFO_BYTES: usize = 38;
/// Historical diagnostic default MR size. `open()` keeps this value for compatibility.
pub const CAPACITY: usize = 1024 * 1024;
/// Hard ceiling for one endpoint MR. This intentionally matches the first DFS
/// max-chunk target without allowing unbounded pinned memory.
pub const MAX_CAPACITY: usize = 4 * 1024 * 1024;
const ERR_BYTES: usize = 256;
#[cfg(has_native_rdma)]
const RETAINED_DRAIN: i32 = 6; // AFS_RDMA_RETAINED_DRAIN in native/rdma.h
#[cfg(has_native_rdma)]
const MAX_DRAIN_RETRIES_PER_SWEEP: usize = 8;

#[derive(Debug)]
pub struct RdmaError(pub String);

impl std::fmt::Display for RdmaError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RdmaError {}

#[cfg(has_native_rdma)]
mod ffi {
    // 这里声明的是 native/rdma.c 提供的小型 C FFI shim。
    // C 代码只做 libibverbs 调用适配；它不启动进程、不挂载文件系统、不处理 Proto。
    use std::ffi::{c_char, c_void};

    // SAFETY: these declarations mirror the native shim ABI. Callers validate
    // buffer sizes and keep all pointers live for each synchronous C call.
    unsafe extern "C" {
        pub fn afs_rdma_open(
            device: *const c_char,
            capacity: u32,
            owned: *mut *mut c_void,
            registered_bytes: *mut usize,
            err: *mut c_char,
            errlen: usize,
        ) -> i32;
        pub fn afs_rdma_info(
            ep: *mut c_void,
            out: *mut u8,
            outlen: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> i32;
        pub fn afs_rdma_connect(
            ep: *mut c_void,
            peer: *const u8,
            len: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> i32;
        pub fn afs_rdma_prepare_probe(ep: *mut c_void, err: *mut c_char, errlen: usize) -> i32;
        pub fn afs_rdma_send_probe(
            ep: *mut c_void,
            timeout_ms: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> i32;
        pub fn afs_rdma_wait_probe(
            ep: *mut c_void,
            timeout_ms: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> i32;
        pub fn afs_rdma_put_local(
            ep: *mut c_void,
            data: *const u8,
            len: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> i32;
        pub fn afs_rdma_get_local(
            ep: *mut c_void,
            data: *mut u8,
            len: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> i32;
        pub fn afs_rdma_transfer(
            ep: *mut c_void,
            operation: i32,
            len: u32,
            timeout_ms: u32,
            err: *mut c_char,
            errlen: usize,
        ) -> i32;
        pub fn afs_rdma_close(ep: *mut c_void, retained_registered_bytes: *mut usize) -> i32;
    }
}

/// 一个 RDMA 会话端点：拥有一组 PD/CQ/QP/MR 和一块固定大小本地 buffer。
///
/// 双方通过 `info()` 交换 QP 编号、LID/GID、本地 MR 地址、rkey 和容量后，
/// 各自调用 `connect()` 把 QP 切到 RTS。之后 `transfer_read/write` 才能
/// 对对端 MR 发起单边操作。
pub struct RdmaEndpoint {
    #[cfg(has_native_rdma)]
    ep: Option<NonNull<std::ffi::c_void>>,
    #[cfg(has_native_rdma)]
    registered_bytes: usize,
    capacity: usize,
    remote_capacity: Option<usize>,
}

// SAFETY: the native endpoint exclusively owns its QP/MR; moving ownership does not share it.
// The native endpoint owns a QP and MR that are not internally synchronized.
// Callers may move it into a blocking task, but concurrent methods require an
// external mutex and every operation takes `&mut self`.
unsafe impl Send for RdmaEndpoint {}

impl RdmaEndpoint {
    /// 打开一个本地 RDMA device，并创建 PD/CQ/QP/MR。
    ///
    /// 这是资源初始化，不涉及对端；对端信息必须通过 gRPC control negotiate 交换。
    pub fn open(device: &str) -> Result<Self, RdmaError> {
        Self::open_with_capacity(device, CAPACITY)
    }

    /// 打开一个自定义容量的 endpoint。
    ///
    /// 容量仍然是固定 MR 大小，不是动态 buffer pool。调用方必须选择一个明确
    /// 的、受上限约束的值；默认诊断路径继续使用 `open()` 的 1 MiB。
    pub fn open_with_capacity(device: &str, capacity: usize) -> Result<Self, RdmaError> {
        Self::open_with_admission(device, capacity, ()).map(|(endpoint, ())| endpoint)
    }

    fn open_with_admission<P: Send + 'static>(
        device: &str,
        capacity: usize,
        admission: P,
    ) -> Result<(Self, P), RdmaError> {
        validate_capacity(capacity)?;
        #[cfg(has_native_rdma)]
        {
            let device = CString::new(device).map_err(|error| RdmaError(error.to_string()))?;
            let mut err = ErrBuf::default();
            let mut owned = std::ptr::null_mut();
            let mut registered_bytes = 0;
            // SAFETY: device is NUL-terminated, capacity was validated, and the
            // writable outputs remain live for this synchronous call.
            let status = unsafe {
                ffi::afs_rdma_open(
                    device.as_ptr(),
                    capacity as u32,
                    &mut owned,
                    &mut registered_bytes,
                    err.ptr(),
                    ERR_BYTES,
                )
            };
            let mut endpoint = Self {
                ep: NonNull::new(owned),
                registered_bytes,
                capacity,
                remote_capacity: None,
            };
            REGISTERED_BYTES.fetch_add(registered_bytes, Ordering::Relaxed);
            if status != 0 {
                // The provider can fail after allocating an MR or QP. Keep the
                // admission tied to their retirement even on this error path.
                endpoint.retire_with(admission);
                return Err(RdmaError(err.message()));
            }
            if endpoint.ep.is_none() {
                return Err(RdmaError("RDMA open returned no endpoint".into()));
            }
            ENDPOINT_OPENS.fetch_add(1, Ordering::Relaxed);
            Ok((endpoint, admission))
        }
        #[cfg(not(has_native_rdma))]
        {
            let _ = (device, admission);
            Err(RdmaError("native RDMA is not linked".into()))
        }
    }

    #[cfg(has_native_rdma)]
    fn native_ptr(&self) -> *mut std::ffi::c_void {
        // All I/O borrows an active endpoint. Only retirement takes its pointer.
        self.ep.expect("live RDMA endpoint").as_ptr()
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// 导出本端连接描述符。描述符通过 control proto 传给对端。
    ///
    /// 格式当前固定为 38 字节：QP number + LID + GID + MR address + rkey + capacity。
    pub fn info(&mut self) -> Result<[u8; INFO_BYTES], RdmaError> {
        #[cfg(has_native_rdma)]
        {
            let mut info = [0u8; INFO_BYTES];
            let mut err = ErrBuf::default();
            // SAFETY: the exclusively borrowed endpoint and supplied input/output
            // buffers remain live for the synchronous native call.
            let code = unsafe {
                ffi::afs_rdma_info(
                    self.native_ptr(),
                    info.as_mut_ptr(),
                    INFO_BYTES as u32,
                    err.ptr(),
                    ERR_BYTES,
                )
            };
            check(code, err)?;
            Ok(info)
        }
        #[cfg(not(has_native_rdma))]
        {
            Err(RdmaError("native RDMA is not linked".into()))
        }
    }

    pub fn connect(&mut self, peer: &[u8]) -> Result<(), RdmaError> {
        let peer_capacity = validate_peer_descriptor(peer)?;
        #[cfg(has_native_rdma)]
        {
            let mut err = ErrBuf::default();
            // SAFETY: the exclusively borrowed endpoint and supplied input/output
            // buffers remain live for the synchronous native call.
            let code = unsafe {
                ffi::afs_rdma_connect(
                    self.native_ptr(),
                    peer.as_ptr(),
                    peer.len() as u32,
                    err.ptr(),
                    ERR_BYTES,
                )
            };
            check(code, err)?;
            self.remote_capacity = Some(peer_capacity);
            Ok(())
        }
        #[cfg(not(has_native_rdma))]
        {
            let _ = peer_capacity;
            Err(RdmaError("native RDMA is not linked".into()))
        }
    }

    /// 服务端在把 `server_info` 返回给客户端前调用，预先投递一个 0-byte RECV。
    ///
    /// 这是 3FS 风格 connect probe 的接收槽：客户端随后发 `SEND_WITH_IMM`。
    /// 如果没有先投递 RECV，客户端 probe 可能到达时没有接收 WQE，连接会因为 RNR
    /// 或 CQ 错误变成不确定状态。
    pub fn prepare_probe(&mut self) -> Result<(), RdmaError> {
        #[cfg(has_native_rdma)]
        {
            let mut err = ErrBuf::default();
            let code =
                // SAFETY: the exclusively borrowed endpoint and writable error
                // buffer remain live throughout the native call.
                unsafe { ffi::afs_rdma_prepare_probe(self.native_ptr(), err.ptr(), ERR_BYTES) };
            check(code, err)
        }
        #[cfg(not(has_native_rdma))]
        {
            Err(RdmaError("native RDMA is not linked".into()))
        }
    }

    /// 客户端 QP 连接完成后发送 0-byte `SEND_WITH_IMM` 并等待本地 send CQ。
    ///
    /// 这里证明“客户端发出的 RDMA SEND 已完成”；服务端是否真正收到，必须由
    /// `wait_probe` 消费接收 CQ。两者合起来替代旧的 ReadyData gRPC。
    pub fn send_probe(&mut self, timeout_ms: u32) -> Result<(), RdmaError> {
        if timeout_ms == 0 {
            return Err(RdmaError("probe timeout must be non-zero".into()));
        }
        #[cfg(has_native_rdma)]
        {
            let mut err = ErrBuf::default();
            // SAFETY: the exclusively borrowed endpoint and supplied input/output
            // buffers remain live for the synchronous native call.
            let code = unsafe {
                ffi::afs_rdma_send_probe(self.native_ptr(), timeout_ms, err.ptr(), ERR_BYTES)
            };
            check(code, err)
        }
        #[cfg(not(has_native_rdma))]
        {
            Err(RdmaError("native RDMA is not linked".into()))
        }
    }

    /// 服务端等待并校验客户端的 connect probe 接收完成。
    ///
    /// 成功后才允许 data.rs 使用该 session 做文件读写和单边搬运。失败会 poison QP，
    /// 调用方应把 session 标记为不可复用。
    pub fn wait_probe(&mut self, timeout_ms: u32) -> Result<(), RdmaError> {
        if timeout_ms == 0 {
            return Err(RdmaError("probe timeout must be non-zero".into()));
        }
        #[cfg(has_native_rdma)]
        {
            let mut err = ErrBuf::default();
            // SAFETY: the exclusively borrowed endpoint and supplied input/output
            // buffers remain live for the synchronous native call.
            let code = unsafe {
                ffi::afs_rdma_wait_probe(self.native_ptr(), timeout_ms, err.ptr(), ERR_BYTES)
            };
            check(code, err)
        }
        #[cfg(not(has_native_rdma))]
        {
            Err(RdmaError("native RDMA is not linked".into()))
        }
    }

    /// 把 Rust bytes 放入本 endpoint 的本地 MR。
    ///
    /// 客户端写文件时先 `put_local(data)`，随后服务端执行 RDMA READ，
    /// 从客户端 MR 拉取内容。
    pub fn put_local(&mut self, data: &[u8]) -> Result<(), RdmaError> {
        if data.len() > self.capacity {
            return Err(RdmaError(format!(
                "local put exceeds endpoint capacity {}",
                self.capacity
            )));
        }
        #[cfg(has_native_rdma)]
        {
            let mut err = ErrBuf::default();
            // SAFETY: the exclusively borrowed endpoint and supplied input/output
            // buffers remain live for the synchronous native call.
            let code = unsafe {
                ffi::afs_rdma_put_local(
                    self.native_ptr(),
                    data.as_ptr(),
                    data.len() as u32,
                    err.ptr(),
                    ERR_BYTES,
                )
            };
            check(code, err)
        }
        #[cfg(not(has_native_rdma))]
        {
            let _ = data;
            Err(RdmaError("native RDMA is not linked".into()))
        }
    }

    /// 从本 endpoint 的本地 MR 拷贝出 Rust bytes。
    ///
    /// 客户端读文件时服务端先执行 RDMA WRITE，把文件内容推到客户端 MR，
    /// 客户端再 `get_local(len)` 返回给上层。
    pub fn get_local(&mut self, len: usize) -> Result<Vec<u8>, RdmaError> {
        if len > self.capacity {
            return Err(RdmaError(format!(
                "local get exceeds endpoint capacity {}",
                self.capacity
            )));
        }
        #[cfg(has_native_rdma)]
        {
            let mut data = vec![0u8; len];
            let mut err = ErrBuf::default();
            // SAFETY: the exclusively borrowed endpoint and supplied input/output
            // buffers remain live for the synchronous native call.
            let code = unsafe {
                ffi::afs_rdma_get_local(
                    self.native_ptr(),
                    data.as_mut_ptr(),
                    len as u32,
                    err.ptr(),
                    ERR_BYTES,
                )
            };
            check(code, err)?;
            Ok(data)
        }
        #[cfg(not(has_native_rdma))]
        {
            let _ = len;
            Err(RdmaError("native RDMA is not linked".into()))
        }
    }

    /// 发起单边 RDMA READ：本端从对端 MR 拉取数据到本地 MR。
    ///
    /// 在 AFS 写文件路径里，“服务端”调用它读取“客户端”已放入 MR 的写入内容。
    pub fn transfer_read(&mut self, len: usize) -> Result<(), RdmaError> {
        self.transfer(1, len)
    }

    /// 发起单边 RDMA WRITE：本端把本地 MR 内容推到对端 MR。
    ///
    /// 在 AFS 读文件路径里，“服务端”调用它把文件内容写进“客户端”MR。
    pub fn transfer_write(&mut self, len: usize) -> Result<(), RdmaError> {
        self.transfer(2, len)
    }

    fn transfer(&mut self, operation: i32, len: usize) -> Result<(), RdmaError> {
        validate_transfer_capacity(self.capacity, self.remote_capacity, len)?;
        #[cfg(has_native_rdma)]
        {
            let mut err = ErrBuf::default();
            // SAFETY: the exclusively borrowed endpoint and supplied input/output
            // buffers remain live for the synchronous native call.
            let code = unsafe {
                ffi::afs_rdma_transfer(
                    self.native_ptr(),
                    operation,
                    len as u32,
                    5000,
                    err.ptr(),
                    ERR_BYTES,
                )
            };
            check(code, err)
        }
        #[cfg(not(has_native_rdma))]
        {
            let _ = (operation, len);
            Err(RdmaError("native RDMA is not linked".into()))
        }
    }
}

/// Owns a registered endpoint and its admission permit through retirement.
/// The permit is returned only after the provider releases the endpoint.
pub struct RdmaLease<P: Send + 'static> {
    endpoint: RdmaEndpoint,
    admission: Option<P>,
}

impl<P: Send + 'static> RdmaLease<P> {
    pub fn open(device: &str, capacity: usize, admission: P) -> Result<Self, RdmaError> {
        let (endpoint, admission) = RdmaEndpoint::open_with_admission(device, capacity, admission)?;
        Ok(Self {
            endpoint,
            admission: Some(admission),
        })
    }

    /// Attempts synchronous retirement. Failure quarantines the native endpoint
    /// together with its permit. The sweeper can retry an incomplete drain.
    #[must_use]
    pub fn retire(mut self) -> RdmaRetireOutcome {
        self.retire_once()
    }

    fn retire_once(&mut self) -> RdmaRetireOutcome {
        match self.admission.take() {
            Some(admission) => self.endpoint.retire_with(admission),
            None => RdmaRetireOutcome::Released,
        }
    }
}

impl<P: Send + 'static> Deref for RdmaLease<P> {
    type Target = RdmaEndpoint;

    fn deref(&self) -> &Self::Target {
        &self.endpoint
    }
}

impl<P: Send + 'static> RdmaLease<P> {
    // Do not expose &mut RdmaEndpoint: its native owner and this permit must
    // not be separable, including via mem::replace on a mutable dereference.
    pub fn info(&mut self) -> Result<[u8; INFO_BYTES], RdmaError> {
        self.endpoint.info()
    }

    pub fn connect(&mut self, peer: &[u8]) -> Result<(), RdmaError> {
        self.endpoint.connect(peer)
    }

    pub fn prepare_probe(&mut self) -> Result<(), RdmaError> {
        self.endpoint.prepare_probe()
    }

    pub fn send_probe(&mut self, timeout_ms: u32) -> Result<(), RdmaError> {
        self.endpoint.send_probe(timeout_ms)
    }

    pub fn wait_probe(&mut self, timeout_ms: u32) -> Result<(), RdmaError> {
        self.endpoint.wait_probe(timeout_ms)
    }

    pub fn put_local(&mut self, bytes: &[u8]) -> Result<(), RdmaError> {
        self.endpoint.put_local(bytes)
    }

    pub fn get_local(&mut self, len: usize) -> Result<Vec<u8>, RdmaError> {
        self.endpoint.get_local(len)
    }

    pub fn transfer_read(&mut self, len: usize) -> Result<(), RdmaError> {
        self.endpoint.transfer_read(len)
    }

    pub fn transfer_write(&mut self, len: usize) -> Result<(), RdmaError> {
        self.endpoint.transfer_write(len)
    }
}

impl<P: Send + 'static> Drop for RdmaLease<P> {
    fn drop(&mut self) {
        #[cfg(has_native_rdma)]
        if self.admission.is_some()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            if let Some(admission) = self.admission.take()
                && let Some(retirement) = self.endpoint.take_retirement(admission)
            {
                // Holding both resources in this guard also handles shutdown
                // discarding the blocking closure before it starts.
                drop(runtime.spawn_blocking(move || drop(retirement)));
            }
            return;
        }
        self.retire_once();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RdmaRetireOutcome {
    Released,
    Quarantined { stage: i32, registered_bytes: usize },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RdmaResourceStats {
    pub endpoint_opens: usize,
    pub registered_bytes: usize,
    pub failed_retirements: usize,
    pub retained_endpoints: usize,
    pub retained_registered_bytes: usize,
}

#[cfg(has_native_rdma)]
struct RetainedEndpoint {
    ptr: NonNull<std::ffi::c_void>,
}

// SAFETY: the pointer is uniquely owned. Only the current owner may invoke close.
#[cfg(has_native_rdma)]
unsafe impl Send for RetainedEndpoint {}

#[cfg(has_native_rdma)]
struct QuarantinedEndpoint {
    native: RetainedEndpoint,
    _admission: Box<dyn Send>,
    stage: i32,
    registered_bytes: usize,
}

#[cfg(has_native_rdma)]
#[derive(Default)]
struct Quarantine {
    records: Vec<QuarantinedEndpoint>,
    failed_retirements: usize,
    retrying_endpoints: usize,
    retrying_bytes: usize,
}

#[cfg(has_native_rdma)]
impl Quarantine {
    fn retained_endpoints(&self) -> usize {
        self.records.len() + self.retrying_endpoints
    }

    fn retained_bytes(&self) -> usize {
        self.records
            .iter()
            .map(|record| record.registered_bytes)
            .sum::<usize>()
            + self.retrying_bytes
    }
}

#[cfg(has_native_rdma)]
static QUARANTINE: Mutex<Quarantine> = Mutex::new(Quarantine {
    records: Vec::new(),
    failed_retirements: 0,
    retrying_endpoints: 0,
    retrying_bytes: 0,
});

#[cfg(has_native_rdma)]
static ENDPOINT_OPENS: AtomicUsize = AtomicUsize::new(0);

#[cfg(has_native_rdma)]
static REGISTERED_BYTES: AtomicUsize = AtomicUsize::new(0);

#[cfg(has_native_rdma)]
#[must_use]
pub fn has_quarantined_drains() -> bool {
    REGISTERED_BYTES.load(Ordering::Relaxed) != 0
        && QUARANTINE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .records
            .iter()
            .any(|record| record.stage == RETAINED_DRAIN)
}

pub fn resource_stats() -> RdmaResourceStats {
    #[cfg(has_native_rdma)]
    {
        let quarantine = QUARANTINE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        RdmaResourceStats {
            endpoint_opens: ENDPOINT_OPENS.load(Ordering::Relaxed),
            registered_bytes: REGISTERED_BYTES.load(Ordering::Relaxed),
            failed_retirements: quarantine.failed_retirements,
            retained_endpoints: quarantine.retained_endpoints(),
            retained_registered_bytes: quarantine.retained_bytes(),
        }
    }
    #[cfg(not(has_native_rdma))]
    RdmaResourceStats::default()
}

/// Retry a bounded batch of endpoints whose drain could not be confirmed.
/// Call from a blocking worker. Permanent provider failures stay quarantined.
#[cfg(has_native_rdma)]
pub fn retry_quarantined_drains() {
    retry_drains(&QUARANTINE, &REGISTERED_BYTES, close_native);
}

#[cfg(has_native_rdma)]
fn retry_drains(
    ledger: &Mutex<Quarantine>,
    registered: &AtomicUsize,
    close: impl Fn(&RetainedEndpoint) -> (i32, usize),
) {
    let retries: Vec<_> = {
        let mut quarantine = ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if quarantine.retrying_endpoints != 0 {
            return;
        }
        let retries: Vec<_> = quarantine
            .records
            .extract_if(.., |record| record.stage == RETAINED_DRAIN)
            .take(MAX_DRAIN_RETRIES_PER_SWEEP)
            .collect();
        quarantine.retrying_endpoints = retries.len();
        quarantine.retrying_bytes = retries.iter().map(|record| record.registered_bytes).sum();
        retries
    };

    for mut record in retries {
        let (stage, bytes) = close(&record.native);
        let mut quarantine = ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registered.fetch_sub(record.registered_bytes - bytes, Ordering::Relaxed);
        quarantine.retrying_endpoints -= 1;
        quarantine.retrying_bytes -= record.registered_bytes;
        if stage != 0 {
            record.stage = stage;
            record.registered_bytes = bytes;
            // Rotate still-incomplete drains behind the other records.
            quarantine.records.push(record);
            drop(quarantine);
            if stage != RETAINED_DRAIN {
                eprintln!(
                    "AFS_RDMA_TEARDOWN permanent stage={stage} retained_registered_bytes={bytes}"
                );
            }
        } else {
            drop(quarantine);
            drop(record);
        }
    }
}

#[cfg(has_native_rdma)]
fn close_native(native: &RetainedEndpoint) -> (i32, usize) {
    let mut bytes = 0;
    // SAFETY: the caller uniquely owns this live endpoint. A nonzero result
    // transfers the remaining resources back to that same owner.
    let stage = unsafe { ffi::afs_rdma_close(native.ptr.as_ptr(), &mut bytes) };
    (stage, bytes)
}

impl RdmaEndpoint {
    fn retire_with<P: Send + 'static>(&mut self, admission: P) -> RdmaRetireOutcome {
        #[cfg(has_native_rdma)]
        {
            self.take_retirement(admission)
                .map_or(RdmaRetireOutcome::Released, |mut retirement| {
                    retirement.finish()
                })
        }
        #[cfg(not(has_native_rdma))]
        {
            drop(admission);
            RdmaRetireOutcome::Released
        }
    }

    #[cfg(has_native_rdma)]
    fn take_retirement<P: Send + 'static>(&mut self, admission: P) -> Option<PendingRetirement<P>> {
        let ptr = self.ep.take()?;
        Some(PendingRetirement {
            owned: Some((RetainedEndpoint { ptr }, admission)),
            registered_bytes: self.registered_bytes,
        })
    }
}

#[cfg(has_native_rdma)]
struct PendingRetirement<P: Send + 'static> {
    owned: Option<(RetainedEndpoint, P)>,
    registered_bytes: usize,
}

#[cfg(has_native_rdma)]
impl<P: Send + 'static> PendingRetirement<P> {
    fn finish(&mut self) -> RdmaRetireOutcome {
        let Some((native, admission)) = self.owned.take() else {
            return RdmaRetireOutcome::Released;
        };
        let (stage, bytes) = close_native(&native);
        REGISTERED_BYTES.fetch_sub(self.registered_bytes - bytes, Ordering::Relaxed);
        if stage != 0 {
            return quarantine(&QUARANTINE, native, admission, stage, bytes);
        }
        drop(admission);
        RdmaRetireOutcome::Released
    }
}

#[cfg(has_native_rdma)]
impl<P: Send + 'static> Drop for PendingRetirement<P> {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(has_native_rdma)]
fn quarantine<P: Send + 'static>(
    ledger: &Mutex<Quarantine>,
    native: RetainedEndpoint,
    admission: P,
    stage: i32,
    registered_bytes: usize,
) -> RdmaRetireOutcome {
    let mut ledger = ledger
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    ledger.failed_retirements += 1;
    ledger.records.push(QuarantinedEndpoint {
        native,
        _admission: Box::new(admission),
        stage,
        registered_bytes,
    });
    drop(ledger);
    let policy = if stage == RETAINED_DRAIN {
        "retry"
    } else {
        "permanent"
    };
    eprintln!(
        "AFS_RDMA_TEARDOWN {policy} stage={stage} retained_registered_bytes={registered_bytes}"
    );
    RdmaRetireOutcome::Quarantined {
        stage,
        registered_bytes,
    }
}

#[cfg(has_native_rdma)]
impl Drop for RdmaEndpoint {
    fn drop(&mut self) {
        self.retire_with(());
    }
}

#[derive(Clone, Debug)]
pub struct RdmaCapability {
    pub device: Option<String>,
}

impl RdmaCapability {
    #[must_use]
    pub fn available(&self) -> bool {
        self.device.is_some() && cfg!(has_native_rdma)
    }
}

#[derive(Clone)]
struct ErrBuf([u8; ERR_BYTES]);

impl Default for ErrBuf {
    fn default() -> Self {
        Self([0; ERR_BYTES])
    }
}

impl ErrBuf {
    fn ptr(&mut self) -> *mut std::ffi::c_char {
        self.0.as_mut_ptr().cast()
    }

    fn message(&self) -> String {
        let bytes = self
            .0
            .iter()
            .take_while(|byte| **byte != 0)
            .copied()
            .collect::<Vec<_>>();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

fn validate_capacity(capacity: usize) -> Result<(), RdmaError> {
    if capacity == 0 || capacity > MAX_CAPACITY || capacity > u32::MAX as usize {
        return Err(RdmaError(format!(
            "RDMA capacity must be 1..{} bytes",
            MAX_CAPACITY
        )));
    }
    Ok(())
}

fn validate_peer_descriptor(peer: &[u8]) -> Result<usize, RdmaError> {
    if peer.len() != INFO_BYTES {
        return Err(RdmaError("peer info must be 38 bytes".into()));
    }
    let qpn = u32::from_be_bytes(peer[0..4].try_into().expect("slice length"));
    let capacity = u32::from_be_bytes(peer[34..38].try_into().expect("slice length")) as usize;
    if qpn == 0 {
        return Err(RdmaError("peer descriptor has empty QP number".into()));
    }
    validate_capacity(capacity)?;
    Ok(capacity)
}

fn validate_transfer_capacity(
    local_capacity: usize,
    remote_capacity: Option<usize>,
    len: usize,
) -> Result<(), RdmaError> {
    if len > local_capacity {
        return Err(RdmaError(format!(
            "transfer exceeds local endpoint capacity {local_capacity}"
        )));
    }
    if let Some(remote_capacity) = remote_capacity
        && len > remote_capacity
    {
        return Err(RdmaError(format!(
            "transfer exceeds remote endpoint capacity {remote_capacity}"
        )));
    }
    Ok(())
}

fn check(code: i32, err: ErrBuf) -> Result<(), RdmaError> {
    if code == 0 {
        Ok(())
    } else {
        Err(RdmaError(err.message()))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CAPACITY, INFO_BYTES, MAX_CAPACITY, validate_capacity, validate_peer_descriptor,
        validate_transfer_capacity,
    };

    #[cfg(has_native_rdma)]
    #[test]
    fn rdma_retirement_failure_retains_admission_for_qp_mr_and_drain() {
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        };

        struct Admission(Arc<AtomicUsize>);

        impl Drop for Admission {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let ledger = Mutex::new(super::Quarantine::default());
        let released = Arc::new(AtomicUsize::new(0));
        for stage in [1, 2, 6] {
            let outcome = super::quarantine(
                &ledger,
                super::RetainedEndpoint {
                    ptr: std::ptr::NonNull::dangling(),
                },
                Admission(released.clone()),
                stage,
                super::CAPACITY,
            );
            assert_eq!(
                outcome,
                super::RdmaRetireOutcome::Quarantined {
                    stage,
                    registered_bytes: super::CAPACITY,
                }
            );
        }
        assert_eq!(released.load(Ordering::Relaxed), 0);
        {
            let state = ledger.lock().unwrap();
            assert_eq!(state.failed_retirements, 3);
            assert_eq!(state.retained_endpoints(), 3);
            assert_eq!(state.retained_bytes(), 3 * CAPACITY);
        }
        let registered = AtomicUsize::new(3 * CAPACITY);
        super::retry_drains(&ledger, &registered, |_| (0, 0));
        {
            let state = ledger.lock().unwrap();
            assert_eq!(state.failed_retirements, 3);
            assert_eq!(state.retained_endpoints(), 2);
            assert_eq!(state.retained_bytes(), 2 * CAPACITY);
            assert_eq!(released.load(Ordering::Relaxed), 1);
        }
        super::retry_drains(&ledger, &registered, |_| {
            panic!("permanent failure was retried")
        });
        drop(ledger);
        assert_eq!(released.load(Ordering::Relaxed), 3);
    }

    #[cfg(has_native_rdma)]
    #[test]
    fn rdma_retirement_retry_returns_admission_after_late_flush() {
        use std::sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        };

        let ledger = Mutex::new(super::Quarantine::default());
        let admission = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = admission.clone().try_acquire_owned().unwrap();
        let registered = AtomicUsize::new(CAPACITY);
        super::quarantine(
            &ledger,
            super::RetainedEndpoint {
                ptr: std::ptr::NonNull::dangling(),
            },
            permit,
            super::RETAINED_DRAIN,
            CAPACITY,
        );
        super::retry_drains(&ledger, &registered, |_| {
            let during = ledger.lock().unwrap();
            assert_eq!(during.retained_endpoints(), 1);
            assert_eq!(during.retained_bytes(), CAPACITY);
            assert_eq!(admission.available_permits(), 0);
            (super::RETAINED_DRAIN, CAPACITY)
        });
        assert_eq!(admission.available_permits(), 0);
        super::retry_drains(&ledger, &registered, |_| (0, 0));
        let after = ledger.lock().unwrap();
        assert_eq!(after.retained_endpoints(), 0);
        assert_eq!(after.retained_bytes(), 0);
        assert_eq!(registered.load(Ordering::Relaxed), 0);
        assert_eq!(after.failed_retirements, 1);
        assert_eq!(admission.available_permits(), 1);
    }

    #[test]
    fn capacity_validation_is_bounded_and_keeps_default_valid() {
        validate_capacity(CAPACITY).expect("default capacity stays valid");
        validate_capacity(MAX_CAPACITY).expect("DFS max chunk capacity is valid");
        assert!(validate_capacity(0).is_err());
        assert!(validate_capacity(MAX_CAPACITY + 1).is_err());
    }

    #[test]
    fn peer_descriptor_capacity_is_validated_before_connect() {
        let mut info = [0_u8; INFO_BYTES];
        info[0..4].copy_from_slice(&7_u32.to_be_bytes());
        info[34..38].copy_from_slice(&(MAX_CAPACITY as u32).to_be_bytes());
        assert_eq!(validate_peer_descriptor(&info).unwrap(), MAX_CAPACITY);

        info[34..38].copy_from_slice(&0_u32.to_be_bytes());
        assert!(validate_peer_descriptor(&info).is_err());

        info[34..38].copy_from_slice(&((MAX_CAPACITY as u32) + 1).to_be_bytes());
        assert!(validate_peer_descriptor(&info).is_err());

        info[34..38].copy_from_slice(&(CAPACITY as u32).to_be_bytes());
        info[0..4].copy_from_slice(&0_u32.to_be_bytes());
        assert!(validate_peer_descriptor(&info).is_err());
    }

    #[test]
    fn transfer_capacity_checks_local_and_remote_bounds() {
        validate_transfer_capacity(MAX_CAPACITY, Some(CAPACITY), CAPACITY)
            .expect("length within both sides is accepted");
        assert!(validate_transfer_capacity(CAPACITY, Some(MAX_CAPACITY), CAPACITY + 1).is_err());
        assert!(validate_transfer_capacity(MAX_CAPACITY, Some(CAPACITY), CAPACITY + 1).is_err());
        validate_transfer_capacity(MAX_CAPACITY, None, MAX_CAPACITY)
            .expect("remote capacity is absent before connect");
    }
}
