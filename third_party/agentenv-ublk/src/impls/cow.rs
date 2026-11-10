use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use futures_util::stream::{FuturesOrdered, FuturesUnordered};
use futures_util::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use io_uring::{opcode, types::Fixed};
use std::os::fd::AsFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use storage_util::AlignedBuffer;
use tokio::sync::Notify;

use crate::{IOBuffer, UVMUblkTarget, UblkDescOperation};
use storage_util::io_ring::AsyncIoRing;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum ChunkState {
    /// Chunk in the origin file
    Origin = 0,
    /// Chunk is still copying from origin file to the cow file
    /// OR there is a full chunk write in progress.
    Copying = 1,
    /// Chunk has been copied into the Cow
    Cow = 2,
}

struct ChunkMeta(AtomicU8);

pub struct BasicCowTarget {
    pub origin_file: std::fs::File,
    pub cow_file: std::fs::File,
    /// (1 << chunkshift) indicates the chunksize in bytes
    pub chunkshift: u8,
    /// Store the state of each chunk (i.e., [ChunkState]).
    chunk_meta: Vec<ChunkMeta>,
    /// A dynamic allocated Notify for each chunk
    chunk_notify: DashMap<usize, Arc<Notify>>,

    size: FileSizeStat,

    // TODO: support multi queue
    /// Index of the origin file in io_uring registered fds
    origin_idx: OnceLock<u32>,
    /// Index of the cow file in io_uring registered fds
    cow_idx: OnceLock<u32>,
}

#[derive(Debug)]
struct FileSizeStat {
    /// The size of the origin file, aligned at the granularity of 512B.
    pub origin_size: u64,
    /// The size of the cow, which is also the size of the ublk block device.
    pub size: u64,
    // batch size shift
    pub logical_bs_shift: u8,

    pub physical_bs_shift: u8,
}

fn ublk_file_size(origin: &std::fs::File, cow: &std::fs::File) -> Result<FileSizeStat> {
    let origin_meta = origin.metadata().context("get origin file metadata")?;
    if !origin_meta.file_type().is_file() {
        bail!("only support regular-file origin");
    }
    let cow_meta = cow.metadata().context("get cow file metadata")?;
    if !cow_meta.file_type().is_file() {
        bail!("only support regular-file cow");
    }
    // NOTE:We might meet the cases where the origin file is not aligned
    // at the granularity of 512 B. The loop block device in kernel just
    // ignore the last non-full sector.
    // As already did in the [BasicCowTarget::ublk_dev_params], we will
    // trim the last non-full sector.
    // So we do not trouble the corner case of the last non-full sector.
    Ok(FileSizeStat {
        origin_size: origin_meta.len() & !(512 - 1),
        size: cow_meta.len() & !(512 - 1),
        logical_bs_shift: 9,
        physical_bs_shift: 12,
    })
}

impl BasicCowTarget {
    pub fn new(config: &BasicCowConfig) -> Result<Self> {
        // the target size is the same as cow
        let chunksize = config.chunksize_kb * 1024;
        if chunksize <= 4096 || !chunksize.is_power_of_two() {
            bail!("invalid chunksize {chunksize}");
        }
        let chunkshift = chunksize.trailing_zeros() as u8;
        // origin is read only
        let origin_file = {
            let mut opt = std::fs::OpenOptions::new();
            opt.read(true);
            if config.origin_dio {
                opt.custom_flags(libc::O_DIRECT);
            }
            opt.open(&config.origin).context("open origin file")?
        };
        let cow_file = {
            let mut opt = std::fs::OpenOptions::new();
            opt.read(true).write(true);
            if config.cow_dio {
                opt.custom_flags(libc::O_DIRECT);
            }
            opt.open(&config.cow).context("open cow file")?
        };
        let size = ublk_file_size(&origin_file, &cow_file).context("stat for origin file size")?;
        if size.origin_size == 0 {
            bail!("found empty origin file");
        }
        if size.size < size.origin_size {
            bail!(
                "cow file is too small {}, expect at least {}",
                size.size,
                size.origin_size
            );
        }
        // number of chunks that owned by origin
        let nr_origin_chunks = size.origin_size.div_ceil(chunksize as u64);
        // number of chunks that owned by cow
        // NOTE: the last partial chunk of origin belongs to origin, instead of cow
        let nr_cow_chunks = size.size.div_ceil(chunksize as u64) - nr_origin_chunks;
        let chunk_meta = (0..nr_origin_chunks)
            .map(|_| ChunkMeta(AtomicU8::new(ChunkState::Origin as _)))
            .chain((0..nr_cow_chunks).map(|_| ChunkMeta(AtomicU8::new(ChunkState::Cow as _))))
            .collect::<Vec<_>>();
        tracing::info!(
            origin = ?config.origin,
            cow = ?config.cow,
            size = size.size,
            "create a ublk cow device"
        );
        let this = Self {
            origin_file,
            cow_file,
            chunkshift,
            chunk_meta,
            chunk_notify: DashMap::new(),
            size,
            origin_idx: OnceLock::new(),
            cow_idx: OnceLock::new(),
        };
        this.fix_last_partial_chunk(config.origin_dio || config.cow_dio)
            .context("fix_last_partial_chunk")?;
        Ok(this)
    }

    /// When cow file is larger than origin, we expose the ublk device with the size of cow file.
    /// However, the last chunk in origin is hard to handle in this cases.
    /// Because some of the data in the last chunk locates in origin, the remaining located in cow.
    /// To prevent from dealing with corner case, we copy the last partial chunk from origin into cow,
    /// at the begining.
    #[tracing::instrument(skip_all, err(Debug))]
    fn fix_last_partial_chunk(&self, dio: bool) -> Result<()> {
        if self.size.size <= self.size.origin_size {
            return Ok(());
        }
        tracing::info!("cow larger than origin, fix the last partial chunk");
        let chunk_size = 1 << self.chunkshift;
        if self.size.origin_size.is_multiple_of(chunk_size) {
            // the last chunk of origin is not partial chunk (i.e., the origin is aligned at chunk
            // size)
            return Ok(());
        }
        // last chunk idx of the origin file
        let last_chunk_idx = self.size.origin_size >> self.chunkshift;
        let last_chunk_startoff = last_chunk_idx << self.chunkshift;
        let last_origin_chunk_size = (self.size.origin_size - last_chunk_startoff) as usize;
        if dio {
            let mut chunk = AlignedBuffer::new(last_origin_chunk_size, 512)
                .context("allocate aligned buffer")?;
            self.origin_file
                .read_exact_at(chunk.as_mut(), last_chunk_startoff)
                .context("read from last partial chunk of origin")?;
            self.cow_file
                .write_all_at(chunk.as_ref(), last_chunk_startoff)
                .context("write last partial chunk into cow")?;
        } else {
            let mut chunk = vec![0u8; last_origin_chunk_size];
            self.origin_file
                .read_exact_at(&mut chunk, last_chunk_startoff)
                .context("read from last partial chunk of origin")?;
            self.cow_file
                .write_all_at(&chunk, last_chunk_startoff)
                .context("write last partial chunk into cow")?;
        }
        // update the metadata, now the last chunk is owned by cow
        // SAFETY: the size of chunk_meta is allocated with respect to size of cow.
        self.chunk_meta
            .get(last_chunk_idx as usize)
            .unwrap()
            .set_state(ChunkState::Cow);
        Ok(())
    }
}

impl ChunkMeta {
    #[inline]
    pub fn state(&self) -> ChunkState {
        match self.0.load(Ordering::SeqCst) {
            0 => ChunkState::Origin,
            1 => ChunkState::Copying,
            2 => ChunkState::Cow,
            x => panic!("invalid chunk state {}", x),
        }
    }

    /// Try to do copying on write. When concurrent copying happens, only one
    /// will succeed (return true). When contend failed, return false.
    #[inline]
    pub fn try_copying(&self) -> bool {
        self.0
            .compare_exchange(
                ChunkState::Origin as _,
                ChunkState::Copying as _,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    #[inline]
    pub fn set_state(&self, state: ChunkState) {
        self.0.store(state as u8, Ordering::SeqCst);
    }
}

impl Default for ChunkMeta {
    fn default() -> Self {
        Self(AtomicU8::new(ChunkState::Origin as u8))
    }
}

#[derive(Debug, clap::Args)]
pub struct BasicCowConfig {
    /// Path to the origin file
    #[arg(long)]
    pub origin: PathBuf,
    /// Path to the cow file (i.e., store the update/write).
    #[arg(long)]
    pub cow: PathBuf,
    /// Enable direct io for origin file
    #[arg(long, default_value_t = false)]
    pub origin_dio: bool,
    /// Enable direct io for cow file
    #[arg(long, default_value_t = false)]
    pub cow_dio: bool,
    /// The copy-on-write unit. If write less than chunksize_kb,
    /// the cow device still does copy-on-write in the `chunksize_kb`.
    #[arg(long, default_value_t = 32)]
    pub chunksize_kb: u32,
}

/// A helper struct for coalescing read request
#[derive(Debug, Clone)]
struct ReadRequest {
    /// Inclusive
    start: u64,
    /// Exclusive
    end: u64,
    /// Which file to read. The index is about the registered fds in io uring.
    file_idx: u32,
}

// Optimization: coalescing the read request. We have some probability to create
// larger read size.
fn coalescing_read_ranges(requests: Vec<ReadRequest>) -> Vec<ReadRequest> {
    let mut res = vec![];
    let mut current_request = None;
    for req in requests {
        let Some(curr) = current_request else {
            current_request = Some(req);
            continue;
        };
        if curr.file_idx == req.file_idx && curr.end == req.start {
            current_request = Some(ReadRequest {
                start: curr.start,
                end: req.end,
                file_idx: req.file_idx,
            });
        } else {
            res.push(curr);
            current_request = Some(req);
        }
    }
    if let Some(req) = current_request {
        res.push(req);
    }
    res
}

impl BasicCowTarget {
    // handle read request
    async fn read(self: &Arc<Self>, buffer: &mut IOBuffer, off: u64, total_len: u32) -> i32 {
        let mut read_requests = vec![];
        let mut start = off;
        let upper = off + total_len as u64;
        while start < upper {
            let chunk_idx = start >> self.chunkshift;
            let end = {
                let end = (chunk_idx + 1) << self.chunkshift;
                end.min(upper)
            };
            let file_idx = match self.chunk_meta.get(chunk_idx as usize).map(|m| m.state()) {
                Some(ChunkState::Cow) => self.cow_idx.get().cloned().unwrap(),
                Some(_) => self.origin_idx.get().cloned().unwrap(),
                None => {
                    tracing::error!(
                        off,
                        total_len,
                        chunk_idx,
                        "access bitmap out of range when handle read op"
                    );
                    return -libc::ENODATA;
                }
            };
            // [start, end)
            read_requests.push(ReadRequest {
                start,
                end,
                file_idx,
            });
            start = end;
        }
        let read_requests = coalescing_read_ranges(read_requests);
        let mut futs = read_requests
            .into_iter()
            .map(
                |ReadRequest {
                     start,
                     end,
                     file_idx,
                 }| {
                    let len = (end - start) as u32;
                    Self::read_once(start, end, file_idx, buffer, start - off)
                        .map(move |res| res.map(|size| (len as usize, size)))
                },
            )
            .collect::<FuturesOrdered<_>>();

        let mut read_bytes = 0;
        while let Some(result) = futs.try_next().await.transpose() {
            match result {
                Ok((expect, read)) => {
                    read_bytes += read;
                    // partial read, we return immediately
                    if expect != read {
                        return read_bytes as i32;
                    }
                }
                Err(err) => {
                    tracing::error!(?err, "read failed");
                    return -err.raw_os_error().unwrap_or(libc::EIO);
                }
            }
        }

        read_bytes as i32
    }

    /// Read from the file of `file_idx` (The index of the uring's register fds).
    /// The range is [`start`, `end`)
    ///
    /// This function will try its best to retry when meet the short read.
    async fn read_once(
        mut start: u64,
        end: u64,
        file_idx: u32,
        buf: &IOBuffer,
        mut buf_offset: u64,
    ) -> std::io::Result<usize> {
        tracing::debug!(start, end, file_idx, "read_once");
        assert!(start < end);
        let ring = buf.ring();
        // since read might return partial values, we try for at most 4 times
        let mut read_bytes = 0;
        for attempt in 1..=4 {
            let len = (end - start) as u32;
            let sqe = match buf {
                IOBuffer::AutoReg(auto_buf) => opcode::ReadFixed::new(
                    Fixed(file_idx),
                    buf_offset as _,
                    len,
                    auto_buf.as_ublk_auto_buf_reg().index,
                ),
                IOBuffer::User(user_buf) => {
                    let slice = user_buf.subslice_mut(buf_offset, len as _);
                    opcode::ReadFixed::new(
                        Fixed(file_idx),
                        slice.as_mut_ptr(),
                        len,
                        user_buf.uring_buf_idx(),
                    )
                }
            }
            .offset(start)
            .build();
            let res = ring.send(sqe)?.await;
            if res == -libc::EINTR {
                continue;
            } else if res < 0 {
                return Err(std::io::Error::from_raw_os_error(-res));
            }
            start += res as u64;
            buf_offset += res as u64;
            read_bytes += res as usize;
            if start >= end {
                if attempt > 1 {
                    tracing::debug!(attempt, "read_once succeed with retry");
                }
                return Ok(read_bytes);
            }
        }
        tracing::warn!("read_once partial read after 4 retries");
        // still partial read
        Ok(read_bytes)
    }

    // handle write request
    async fn write(
        self: &Arc<Self>,
        buf: &mut IOBuffer,
        extra_buf: &mut IOBuffer,
        off: u64,
        total_len: u32,
    ) -> i32 {
        let mut start = off;
        let upper = off + total_len as u64;

        let mut futs = FuturesOrdered::new();
        while start < upper {
            let chunk_idx = start >> self.chunkshift;
            let end = ((chunk_idx + 1) << self.chunkshift).min(upper);
            // len means the write size of current iteration
            futs.push_back(
                self.write_within_chunk(
                    start,
                    end,
                    buf,
                    start - off,
                    extra_buf,
                    if start == off {
                        0
                    } else if end >= upper {
                        1 << self.chunkshift
                    } else {
                        0
                    },
                )
                .map_ok(move |res| ((end - start) as usize, res)),
            );
            start = end;
        }

        let mut written_bytes = 0;
        while let Some(result) = futs.try_next().await.transpose() {
            match result {
                Ok((expect, write)) => {
                    written_bytes += write;
                    if expect != write {
                        return written_bytes as i32;
                    }
                }
                Err(err) => {
                    tracing::error!(?err, "write failed");
                    return -err.raw_os_error().unwrap_or(libc::EIO);
                }
            }
        }
        written_bytes as i32
    }

    async fn write_within_chunk(
        &self,
        start: u64,
        end: u64,
        buf: &IOBuffer,
        buf_offset: u64,
        extra_buf: &IOBuffer,
        extra_buf_offset: u64,
    ) -> std::io::Result<usize> {
        let chunk_idx = start >> self.chunkshift;
        let chunk_start = chunk_idx << self.chunkshift;
        let chunk_end = (chunk_idx + 1) << self.chunkshift;
        if end > chunk_end || start >= end {
            tracing::error!(start, end, "write_within_chunk meet invalid range");
            return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
        }
        let chunk_idx = chunk_idx as usize;

        let full_write = start == chunk_start && end == chunk_end.min(self.size.size);
        let meta = self.chunk_meta.get(chunk_idx).ok_or_else(|| {
            tracing::error!(
                chunk_idx,
                "access chunk meta out of range when handle write op"
            );
            std::io::Error::from_raw_os_error(libc::ENODATA)
        })?;
        loop {
            match meta.state() {
                ChunkState::Cow => {
                    // chunk already located in cow file: write to cow chunk directly
                    let cow_idx = self.cow_idx.get().cloned().unwrap();
                    return Self::write_once(start, end, cow_idx, buf, buf_offset).await;
                    // no need to update bitmap, as it has already been set
                }
                ChunkState::Copying => {
                    // first insert notify
                    let notify = self
                        .chunk_notify
                        .entry(chunk_idx)
                        .or_insert_with(|| Arc::new(Notify::new()))
                        .clone()
                        .notified_owned();
                    // rechecking after create a notified
                    let state = meta.state();
                    if state == ChunkState::Copying {
                        notify.await;
                        // after await, just recheck and suppose to see Cow
                    } else {
                        assert_eq!(state, ChunkState::Cow, "invalid chunk state {state:?}");
                    }
                    // we can remove the chunk_notify
                    self.chunk_notify.remove(&chunk_idx);
                }
                ChunkState::Origin => {
                    // chunk has located in origin file
                    if !meta.try_copying() {
                        // if try acquire copying state failed, continuously loop and rechecking
                        // state
                        continue;
                    }
                    // there are two conditions:
                    // 1. The write is a full write -> just write to cow.
                    // 2. The write is a partial write (write to part of the chunk) -> first read
                    //    from origin to cow, and then write to cow.
                    let written_bytes = if full_write {
                        // Case 1
                        let cow_idx = self.cow_idx.get().cloned().unwrap();
                        match Self::write_once(start, end, cow_idx, buf, buf_offset).await {
                            Ok(n) => n,
                            Err(err) => {
                                // write failed, go back to origin
                                meta.set_state(ChunkState::Origin);
                                if let Some(notify) = self.chunk_notify.get(&chunk_idx) {
                                    notify.notify_waiters();
                                }
                                return Err(err);
                            }
                        }
                    } else {
                        // Case 2
                        match self
                            .partial_write(start, end, buf, buf_offset, extra_buf, extra_buf_offset)
                            .await
                        {
                            Ok(_) => {
                                // NOTE: although partial_write actually write an entire chunk.
                                // We returns the written bytes as it only writes [start, end).
                                (end - start) as usize
                            }
                            Err(err) => {
                                // partial write failed, go back to origin
                                meta.set_state(ChunkState::Origin);
                                if let Some(notify) = self.chunk_notify.get(&chunk_idx) {
                                    notify.notify_waiters();
                                }
                                return Err(err);
                            }
                        }
                    };
                    meta.set_state(ChunkState::Cow);
                    if let Some(notify) = self.chunk_notify.get(&chunk_idx) {
                        notify.notify_waiters();
                    }
                    return Ok(written_bytes);
                }
            }
        }
    }

    /// Partial means this write request does not fill a full chunk.
    /// chunk is the minial tracking unit of the copy on write device, whose size is
    /// controlled by the `chunkshift`.
    /// Specifically, `partial_write` needs to read from origin into cow first,
    /// then write to the cow file.
    ///
    /// - [`start`, `end`) must be within a single chunk
    /// - `buf`: only contains the to-be-written data
    /// - `extra_buf`: a temporary buffer (exact the chunk size), to read an entire chunk from origin
    async fn partial_write(
        &self,
        start: u64,
        end: u64,
        buf: &IOBuffer,
        buf_offset: u64,
        extra_buf: &IOBuffer,
        extra_buf_offset: u64,
    ) -> std::io::Result<()> {
        let chunk_idx = start >> self.chunkshift;
        let chunk_start = chunk_idx << self.chunkshift;
        // do not write beyound the end of the block device
        let chunk_end = ((chunk_idx + 1) << self.chunkshift).min(self.size.size);

        assert!(end <= chunk_end);
        assert!(start < end);

        // first read from origin to extra buffer
        let res = Self::read_once(
            chunk_start,
            chunk_end,
            self.origin_idx.get().cloned().unwrap(),
            extra_buf,
            extra_buf_offset,
        )
        .await
        .inspect_err(|err| tracing::error!(?err, "read the chunk from origin file failed"))?;
        if res < (chunk_end - chunk_start) as usize {
            tracing::error!(res, "read only partial the chunk from origin file");
            return Err(std::io::Error::from_raw_os_error(libc::EIO));
        }

        let cow_idx = self.cow_idx.get().cloned().unwrap();
        // Then we start three possible write, to the cow file
        // [chunk_start, start): extra_buf
        // [start, end): buf
        // [end, chunk_end): extra_buf
        let mut futs = FuturesUnordered::new();
        for (range, b, bo, mark) in [
            (chunk_start..start, extra_buf, extra_buf_offset, "1st"),
            (start..end, buf, buf_offset, "2nd"),
            (
                end..chunk_end,
                extra_buf,
                extra_buf_offset + (end - chunk_start),
                "3rd",
            ),
        ] {
            if range.is_empty() {
                continue;
            }
            let target_res = (range.end - range.start) as usize;
            let fut = Self::write_once(range.start, range.end, cow_idx, b, bo)
                .map(move |res| (res, target_res, mark));
            futs.push(fut);
        }

        let mut write_bytes = 0;
        while let Some((res, target, mark)) = futs.next().await {
            match res {
                Ok(res) if res == target => {
                    write_bytes += res;
                }
                Ok(res) => {
                    tracing::error!(res, target, mark, "write to the cow file only partially");
                    return Err(std::io::Error::from_raw_os_error(libc::EIO));
                }
                Err(err) => {
                    tracing::error!(?err, target, mark, "write to the cow file failed");
                    return Err(err);
                }
            }
        }
        if write_bytes < (chunk_end - chunk_start) as usize {
            tracing::error!(write_bytes, "write to the cow file only partially");
            // FIXME: treat this as error?
            return Err(std::io::Error::from_raw_os_error(libc::EIO));
        }
        Ok(())
    }

    /// Write the file of `file_idx` (The index of the uring's register fds).
    /// The range is [`start`, `end`)
    ///
    /// This function will try its best to retry when meet the short write.
    async fn write_once(
        mut start: u64,
        end: u64,
        file_idx: u32,
        buf: &IOBuffer,
        mut buf_offset: u64,
    ) -> std::io::Result<usize> {
        tracing::debug!(start, end, file_idx, "write_once");
        assert!(start < end);
        // since write might return partial values, we try for at most 4 times
        let ring = buf.ring();
        let mut write_bytes = 0;
        for attempt in 1..=4 {
            let len = (end - start) as u32;
            let sqe = match buf {
                IOBuffer::AutoReg(auto_buf) => opcode::WriteFixed::new(
                    Fixed(file_idx),
                    buf_offset as _,
                    len,
                    auto_buf.as_ublk_auto_buf_reg().index,
                ),
                IOBuffer::User(user_buf) => {
                    let slice = user_buf.subslice_mut(buf_offset, len as _);
                    opcode::WriteFixed::new(
                        Fixed(file_idx),
                        slice.as_mut_ptr(),
                        len,
                        user_buf.uring_buf_idx(),
                    )
                }
            }
            .offset(start)
            .build();
            let res = ring.send(sqe)?.await;
            if res == -libc::EINTR {
                continue;
            } else if res < 0 {
                return Err(std::io::Error::from_raw_os_error(-res));
            }
            start += res as u64;
            buf_offset += res as u64;
            write_bytes += res as usize;
            if start >= end {
                if attempt > 1 {
                    tracing::info!(attempt, "write_once succeed with retry");
                }
                return Ok(write_bytes);
            }
        }
        tracing::warn!("write_once partial write after 4 retries");
        // still partial write
        Ok(write_bytes)
    }
}

#[async_trait(?Send)]
impl UVMUblkTarget for BasicCowTarget {
    const DEV_NAME: &str = "dsec-cow";

    // TODO: support multi-queue
    async fn init(&self, _qid: u16, ring: &AsyncIoRing) -> Result<()> {
        let origin_idx = ring
            .register_fd(self.origin_file.as_fd())
            .context("register origin file")?;
        self.origin_idx
            .set(origin_idx)
            .map_err(|_| anyhow!("origin idx already set"))?;
        let cow_idx = ring
            .register_fd(self.cow_file.as_fd())
            .context("register cw file")?;
        self.cow_idx
            .set(cow_idx)
            .map_err(|_| anyhow!("cow idx already set"))?;
        Ok(())
    }

    fn per_slot_extra_buf_len(&self) -> Option<usize> {
        Some(2 << self.chunkshift)
    }

    /// Return the ublk dev params. This will be send to ublkdrv (UBLK_U_CMD_SET_PARAMS).
    fn ublk_dev_params(&self, dev_info: &ublk_sys::ublksrv_ctrl_dev_info) -> ublk_sys::ublk_params {
        ublk_sys::ublk_params {
            types: ublk_sys::UBLK_PARAM_TYPE_BASIC,
            len: std::mem::size_of::<ublk_sys::ublk_params>() as u32,
            basic: ublk_sys::ublk_param_basic {
                attrs: ublk_sys::UBLK_ATTR_VOLATILE_CACHE,
                logical_bs_shift: self.size.logical_bs_shift,
                physical_bs_shift: self.size.physical_bs_shift,
                // optimal io size
                io_opt_shift: self.chunkshift,
                io_min_shift: self.size.logical_bs_shift,
                max_sectors: (dev_info.max_io_buf_bytes >> 9),
                dev_sectors: (self.size.size >> 9),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Handle the request, return the result.
    /// Since multiple queue and slot within each queue, will call this method concurrently,
    /// we can only get `&self` here.
    ///
    /// - `qid`: The queue id.
    /// - `tag`: The slot within the queue.
    /// - `io_desc`: the io descriptor received from ublkdrv at (qid, tag).
    /// - `buf`: The io buffer passed to ublksrv_io_cmd.
    /// - `extra_buf`: COW block device need an extra buffer, to do copy up.
    // FIXME: remove buf, and use those in `io_desc`?
    async fn handle_io_request(
        self: &Arc<Self>,
        qid: u16,
        tag: u16,
        io_desc: ublk_sys::ublksrv_io_desc,
        buf: &mut IOBuffer,
        extra_buf: Option<&mut IOBuffer>,
        _io_ring: &AsyncIoRing,
    ) -> i32 {
        let offset = io_desc.start_sector << 9;
        let len = io_desc.nr_sectors << 9;
        let op = match UblkDescOperation::try_from(io_desc.op_flags & 0xff) {
            Ok(op) => op,
            Err(_) => {
                tracing::error!(op = io_desc.op_flags, qid, tag, "encounter invalid op");
                return -libc::EINVAL;
            }
        };
        tracing::debug!(qid, tag, offset, len, ?op);
        match op {
            // read request does not need extra buffers
            UblkDescOperation::Read => self.read(buf, offset, len).await,
            UblkDescOperation::Write => {
                let extra_buf = extra_buf.unwrap();
                self.write(buf, extra_buf, offset, len).await
            }
            UblkDescOperation::Discard => {
                tracing::warn!(offset, len, "receive discard op");
                0
            }
            UblkDescOperation::WriteZeroes => {
                tracing::warn!(offset, len, "receive write zeros op");
                -libc::EOPNOTSUPP
            }
            UblkDescOperation::Flush => {
                // just ignore
                0
            }
            _ => {
                tracing::error!(qid, tag, "receive unsupported operation {op:?}");
                -libc::EOPNOTSUPP
            }
        }
    }
}

#[cfg(test)]
mod test {
    use rand::{self, rngs::SmallRng, RngExt, SeedableRng};
    use std::cell::OnceCell;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::{io::Write, os::unix::fs::FileExt};
    use ublk_sys::ublksrv_io_desc;

    use crate::{setup_tracing, LevelFilter};
    use crate::{BasicCowConfig, BasicCowTarget, IOBuffer, UVMUblkTarget, UserBuffer};
    use storage_util::io_ring::{AsyncIoRing, AsyncIoRingBuilder};

    const IO_BUF_SIZE_KB: usize = 128;

    thread_local! {
        static TEST_URING: OnceCell<AsyncIoRing> = const { OnceCell::new() };
    }

    struct TestEnv {
        cfg: BasicCowConfig,
        tgt: Arc<BasicCowTarget>,
        ring: AsyncIoRing,
        /// Size of the ublk device (e.g., cow file size)
        size: usize,
        buf: IOBuffer,
        extra: IOBuffer,
        /// Path to the golden file
        golden_file: std::fs::File,
        golden_path: String,
    }

    impl TestEnv {
        pub async fn new(
            prefix: &str,
            origin_size: usize,
            cow_size: usize,
            ring: AsyncIoRing,
        ) -> Self {
            assert!(cow_size >= origin_size, "cow too small");
            const CHUNK_SIZE_KB: usize = 16;
            let origin_path = format!("{}-origin", prefix);
            let mut origin = std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&origin_path)
                .unwrap();
            let mut rng = rand::rngs::SmallRng::seed_from_u64(0xdeadbeef);
            let mut remaining = origin_size;
            let mut buf = vec![0u8; 1024 * 1024];

            while remaining > 0 {
                let fill_size = remaining.min(1024 * 1024);
                rng.fill(buf.as_mut_slice());
                origin.write_all(&buf[..fill_size]).unwrap();
                remaining -= fill_size;
            }

            let cow_path = format!("{}-cow", prefix);
            let cow = std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&cow_path)
                .unwrap();
            cow.set_len(cow_size as u64).unwrap();

            let golden_path = format!("{}-golden", prefix);
            std::fs::copy(&origin_path, &golden_path).unwrap();
            let golden_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&golden_path)
                .unwrap();
            if cow_size > origin_size {
                golden_file.set_len(cow_size as u64).unwrap();
            }

            let cfg = BasicCowConfig {
                origin: PathBuf::from(origin_path),
                origin_dio: true,
                cow: PathBuf::from(cow_path),
                cow_dio: false,
                chunksize_kb: CHUNK_SIZE_KB as _,
            };
            let tgt = BasicCowTarget::new(&cfg).unwrap();
            tgt.init(0, &ring).await.unwrap();
            let buf = IOBuffer::User(
                UserBuffer::new(ring.clone(), IO_BUF_SIZE_KB << 10, 512)
                    .await
                    .unwrap(),
            );
            let extra = IOBuffer::User(
                UserBuffer::new(ring.clone(), 2 * CHUNK_SIZE_KB * 1024, 512)
                    .await
                    .unwrap(),
            );
            Self {
                cfg,
                tgt: Arc::new(tgt),
                ring,
                size: cow_size,
                buf,
                extra,
                golden_file,
                golden_path,
            }
        }

        /// - `read`: This operation is read (false means write operation).
        /// - `start_sector`: The begining of the operation.
        /// - `nr_sector`: The number of sectors of this operation.
        ///
        /// For read, we will verify the data read from golden and the ublk target.
        /// For write, caller should fill the [Self::buf] by themselves.
        pub async fn do_operation(&mut self, read: bool, start_sector: u64, nr_sectors: usize) {
            // 1 KB = 2 sectors
            assert!(nr_sectors <= IO_BUF_SIZE_KB * 2, "operation too large");
            let io_desc = ublksrv_io_desc {
                op_flags: if read {
                    ublk_sys::UBLK_IO_OP_READ
                } else {
                    ublk_sys::UBLK_IO_OP_WRITE
                },
                nr_sectors: nr_sectors as u32,
                start_sector,
                addr: self.buf.uring_buf_idx() as u64,
            };
            let res = self
                .tgt
                .handle_io_request(
                    0,
                    2,
                    io_desc,
                    &mut self.buf,
                    Some(&mut self.extra),
                    &self.ring,
                )
                .await;
            assert_eq!(res, nr_sectors as i32 * 512);
            let buf = match &self.buf {
                IOBuffer::User(buf) => buf.subslice_mut(0, nr_sectors * 512),
                _ => panic!("invalid IOBuffer type"),
            };
            if read {
                let mut golden_buf = vec![0u8; nr_sectors * 512];
                self.golden_file
                    .read_exact_at(&mut golden_buf, start_sector * 512)
                    .unwrap();
                assert_eq!(buf, golden_buf);
            } else {
                self.golden_file
                    .write_all_at(buf, start_sector * 512)
                    .unwrap();
            }
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.cfg.origin);
            let _ = std::fs::remove_file(&self.cfg.cow);
            let _ = std::fs::remove_file(&self.golden_path);
        }
    }

    fn init_runtime() -> (tokio::runtime::Runtime, tokio::task::LocalSet) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .on_thread_park(|| {
                TEST_URING.with(|uring| {
                    if let Some(uring) = uring.get() {
                        uring.borrow().submit().unwrap();
                    }
                })
            })
            .build()
            .unwrap();
        let ring = AsyncIoRingBuilder::new()
            .nr_sparse_buffer(4)
            .nr_sparse_file(4)
            .sqe_entries(16)
            .cqe_entries(32)
            .build()
            .unwrap();
        TEST_URING.with(|uring| uring.set(ring.clone()).map_err(|_| ()).unwrap());
        let local_set = tokio::task::LocalSet::new();
        local_set.spawn_local(async move {
            ring.handle_completion().await.unwrap();
        });
        (rt, local_set)
    }

    #[test]
    fn test_cow_dev() {
        setup_tracing(None, LevelFilter::Debug).unwrap();
        let (rt, local_set) = init_runtime();
        local_set.block_on(&rt, async {
            let ring = TEST_URING.with(|uring| uring.get().unwrap().clone());
            let mut env = TestEnv::new("cow-basic", 128 << 20, 128 << 20, ring).await;
            let mut rng = SmallRng::seed_from_u64(0xdeadbeef);

            for _ in 0..128 {
                let nr_sectors = rng.random_range(4..=IO_BUF_SIZE_KB * 2);
                let start_sector = rng.random_range(0..(env.size / 512 - nr_sectors) as u64);
                let read_op = rng.random_bool(0.5);
                if !read_op {
                    // random fill the write buffer
                    let buf = match &env.buf {
                        IOBuffer::User(buf) => buf.subslice_mut(0, IO_BUF_SIZE_KB << 10),
                        _ => panic!("invalid IOBuffer type"),
                    };
                    let rand_sector = {
                        let mut data = [0u8; 512];
                        rng.fill(data.as_mut_slice());
                        data
                    };
                    for idx in 0..nr_sectors {
                        buf[idx * 512..(idx + 1) * 512].copy_from_slice(&rand_sector);
                    }
                }
                env.do_operation(read_op, start_sector, nr_sectors).await;
            }
        })
    }

    #[test]
    fn test_enlarge_cow_dev() {
        setup_tracing(None, LevelFilter::Debug).unwrap();
        let (rt, local_set) = init_runtime();
        local_set.block_on(&rt, async {
            // base is 127 MB + 12 KB
            let base_size = (127 << 20) + (12 << 10);
            // cow is 130 MB + 10 KB
            let cow_size = (130 << 20) + (10 << 10);
            let ring = TEST_URING.with(|uring| uring.get().unwrap().clone());
            let mut env = TestEnv::new("cow-enlarge", base_size, cow_size, ring).await;

            let mut rng = SmallRng::seed_from_u64(0xdeadbeef);

            for id in 0..256 {
                let (nr_sectors, start_sector) = if id < 16 {
                    // for the first 16, we operate at the last of origin file
                    let nr_sectors = rng.random_range(4..=24);
                    let start_sector = (127 << 20) / 512;
                    (nr_sectors, start_sector)
                } else if id < 32 {
                    // for the next 16, we operation at the end of cow file
                    let nr_sectors = rng.random_range(4..=20);
                    let start_sector = (130 << 20) / 512;
                    (nr_sectors, start_sector)
                } else {
                    let nr_sectors = rng.random_range(4..=IO_BUF_SIZE_KB * 2);
                    let start_sector = rng.random_range(0..(env.size / 512 - nr_sectors) as u64);
                    (nr_sectors, start_sector)
                };
                let read_op = rng.random_bool(0.5);
                if !read_op {
                    // random fill the write buffer
                    let buf = match &env.buf {
                        IOBuffer::User(buf) => buf.subslice_mut(0, IO_BUF_SIZE_KB << 10),
                        _ => panic!("invalid IOBuffer type"),
                    };
                    let rand_sector = {
                        let mut data = [0u8; 512];
                        rng.fill(data.as_mut_slice());
                        data
                    };
                    for idx in 0..nr_sectors {
                        buf[idx * 512..(idx + 1) * 512].copy_from_slice(&rand_sector);
                    }
                }
                env.do_operation(read_op, start_sector, nr_sectors).await;
            }
        });
    }
}
