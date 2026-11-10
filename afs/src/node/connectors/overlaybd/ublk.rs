use super::{lsmt::SECTOR_SIZE, reader::Image};
use futures_util::{Stream, stream::FuturesUnordered};
use libublk::{
    BufDesc, UblkError, UblkFlags,
    ctrl::{UblkCtrl, ublk_init_ctrl_task_ring},
    helpers::IoBuf,
    io::{UblkDev, UblkQueue, ublk_init_task_ring, with_task_io_ring, with_task_io_ring_mut},
    sys,
    uring_async::{ublk_reap_io_events_with_update_queue, ublk_wake_task},
};
use std::{
    cell::{Cell, RefCell},
    future::{Future, poll_fn},
    io::{self, Read},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::PathBuf,
    pin::{Pin, pin},
    sync::Arc,
    task::Poll,
    thread,
};
use tokio::{
    io::unix::AsyncFd,
    runtime::Handle,
    sync::{oneshot, watch},
    task::{JoinHandle, spawn_blocking},
    time::{Instant, timeout_at},
};

const NAME: &str = "afs-overlaybd";
const QUEUE_DEPTH: u32 = 16;
const IO_BYTES: u32 = 256 * 1024;
const RECOVERY_FLAGS: u64 = (sys::UBLK_F_USER_RECOVERY | sys::UBLK_F_USER_RECOVERY_REISSUE) as u64;

struct Buffers(Vec<IoBuf<u8>>);

impl Drop for Buffers {
    fn drop(&mut self) {
        // A failed or abandoned STOP can leave kernel commands with the
        // buffer addresses. Only the connector can confirm their release.
        std::mem::forget(std::mem::take(&mut self.0));
    }
}

struct Queue<'a> {
    queue: UblkQueue<'a>,
    idle_ring: Option<io_uring::IoUring>,
}

impl<'a> Queue<'a> {
    fn open(raw: &'a UblkDev) -> io::Result<Self> {
        let idle_ring = io_uring::IoUring::new(2)?;
        idle_ring
            .submitter()
            .register_files(&raw.tgt.fds[..raw.tgt.nr_fds as usize])?;
        // The active ring must deliver completions while its owning thread
        // waits on the Tokio reactor instead of inside io_uring_enter.
        ublk_init_task_ring(|ring| {
            let queue = io_uring::IoUring::builder()
                .setup_cqsize(raw.tgt.cq_depth as u32)
                .build(raw.tgt.sq_depth as u32)?;
            ring.set(RefCell::new(queue))
                .map_err(|_| UblkError::InvalidVal)
        })
        .map_err(driver_error)?;
        let queue = UblkQueue::new_for_thread(0, raw, 0).map_err(driver_error)?;
        Ok(Self {
            queue,
            idle_ring: Some(idle_ring),
        })
    }
}

impl Drop for Queue<'_> {
    fn drop(&mut self) {
        // FETCH requests can keep the registered character FD busy.
        // Close the active ring before libublk's queue drop unregisters
        // files. It then unregisters from the idle ring and unmaps the queue.
        if let Some(idle) = self.idle_ring.take() {
            with_task_io_ring_mut(|ring| *ring = idle);
        }
    }
}

pub(super) struct Device {
    id: u32,
    control: Arc<UblkCtrl>,
    image: Arc<Image>,
    raw: Option<Arc<UblkDev>>,
    start: Option<JoinHandle<io::Result<()>>>,
    queue: Option<JoinHandle<io::Result<Buffers>>>,
    stop: watch::Sender<bool>,
    failure: watch::Sender<bool>,
    retired: Option<Buffers>,
}

impl Device {
    pub(super) async fn create(image: Image, recoverable: bool) -> io::Result<Self> {
        spawn_blocking(move || {
            if recoverable
                && UblkCtrl::get_features()
                    .is_none_or(|flags| flags & RECOVERY_FLAGS != RECOVERY_FLAGS)
            {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "ublk recovery with reissue unavailable",
                ));
            }
            let flags = if recoverable { RECOVERY_FLAGS } else { 0 };
            let ctrl = UblkCtrl::new(
                Some(NAME.to_owned()),
                -1,
                1,
                QUEUE_DEPTH,
                IO_BYTES,
                flags,
                0,
                UblkFlags::UBLK_DEV_F_ADD_DEV,
            )
            .map_err(driver_error)?;
            Self::open(image, ctrl)
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(super) async fn recover(id: u32, image: Image) -> io::Result<Self> {
        spawn_blocking(move || {
            let ctrl = UblkCtrl::new(
                Some(NAME.to_owned()),
                device_number(id)?,
                0,
                0,
                0,
                0,
                0,
                UblkFlags::UBLK_DEV_F_RECOVER_DEV,
            )
            .map_err(driver_error)?;
            if ctrl.dev_info().flags & RECOVERY_FLAGS != RECOVERY_FLAGS {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "ublk device cannot reissue reads",
                ));
            }
            check_result(ctrl.start_user_recover())?;
            Self::open(image, ctrl)
        })
        .await
        .map_err(io::Error::other)?
    }

    fn open(image: Image, control: UblkCtrl) -> io::Result<Self> {
        let id = control.dev_info().dev_id;
        let raw = UblkDev::new(
            NAME.to_owned(),
            |raw| {
                if raw.dev_info.nr_hw_queues != 1 {
                    return Err(UblkError::InvalidVal);
                }
                let sectors = image.sectors();
                raw.tgt.dev_size = sectors * SECTOR_SIZE;
                raw.tgt.params = sys::ublk_params {
                    types: sys::UBLK_PARAM_TYPE_BASIC,
                    basic: sys::ublk_param_basic {
                        attrs: sys::UBLK_ATTR_READ_ONLY,
                        logical_bs_shift: 9,
                        physical_bs_shift: 12,
                        io_min_shift: 9,
                        io_opt_shift: 12,
                        max_sectors: raw.dev_info.max_io_buf_bytes >> 9,
                        dev_sectors: sectors,
                        ..Default::default()
                    },
                    ..Default::default()
                };
                Ok(())
            },
            &control,
        )
        .map_err(driver_error)?;
        // Device removal is gated by the connector journal, including on worker failure.
        control.disown();
        Ok(Self {
            id,
            control: Arc::new(control),
            image: Arc::new(image),
            raw: Some(Arc::new(raw)),
            start: None,
            queue: None,
            stop: watch::channel(false).0,
            failure: watch::channel(false).0,
            retired: None,
        })
    }

    pub(super) async fn start(&mut self, deadline: Instant) -> io::Result<()> {
        let raw = self
            .raw
            .clone()
            .ok_or_else(|| io::Error::other("ublk device is closed"))?;
        let (ready, initialized) = oneshot::channel();
        let image = self.image.clone();
        let worker_raw = raw.clone();
        let stop = self.stop.subscribe();
        let failure = self.failure.clone();
        let runtime = Handle::current();
        let worker = thread::Builder::new().name(NAME.into()).spawn(move || {
            let mut initialized = worker_raw.alloc_queue_io_bufs();
            for buffer in &mut initialized {
                buffer.zero_buf();
            }
            let mut buffers = Buffers(initialized);
            let result = runtime.block_on(serve_queue(
                &worker_raw,
                &image,
                &mut buffers.0,
                stop,
                &failure,
                ready,
            ));
            if let Err(error) = result {
                failure.send_replace(true);
                afs_logging::error!("overlaybd.ublk_queue"; "error" => error.to_string());
            }
            buffers
        })?;
        let failure = self.failure.clone();
        self.queue = Some(spawn_blocking(move || {
            worker.join().map_err(|_| {
                failure.send_replace(true);
                io::Error::other("ublk queue thread panicked")
            })
        }));
        timeout_at(deadline, initialized)
            .await
            .map_err(timed_out)?
            .map_err(io::Error::other)?;

        let control = self.control.clone();
        let task = self.start.insert(spawn_blocking(move || {
            // Device creation may run on another thread. libublk's control
            // ring has to exist on the thread that issues START.
            ublk_init_ctrl_task_ring(|ring| {
                if ring.is_none() {
                    *ring = Some(io_uring::IoUring::builder().build(16)?);
                }
                Ok(())
            })
            .map_err(driver_error)?;
            check_result(control.start_dev(&raw))
        }));
        let started = timeout_at(deadline, task).await.map_err(timed_out)?;
        self.start = None;
        started.map_err(io::Error::other)??;
        if !self.queues_are_healthy() {
            return Err(io::Error::other("ublk queue failed during device startup"));
        }
        Ok(())
    }

    pub(super) fn dev_id(&self) -> u32 {
        self.id
    }

    pub(super) fn device_path(&self) -> PathBuf {
        PathBuf::from(self.control.get_bdev_path())
    }

    pub(super) fn queues_are_healthy(&self) -> bool {
        !*self.failure.borrow()
            && self
                .queue
                .as_ref()
                .is_some_and(|worker| !worker.is_finished())
    }

    pub(super) fn queue_failure(&self) -> watch::Receiver<bool> {
        self.failure.subscribe()
    }

    pub(super) async fn stop_queue_workers(&mut self, deadline: Instant) -> io::Result<()> {
        self.stop.send_replace(true);
        if let Some(worker) = self.queue.as_mut() {
            let result = timeout_at(deadline, worker).await.map_err(timed_out)?;
            self.queue = None;
            match result {
                Ok(Ok(buffers)) => self.retired = Some(buffers),
                Ok(Err(error)) => {
                    afs_logging::error!("overlaybd.ublk_join"; "error" => error.to_string());
                }
                Err(error) => {
                    afs_logging::error!("overlaybd.ublk_join"; "error" => error.to_string());
                }
            }
        }
        if let Some(start) = self.start.as_mut() {
            let _ = timeout_at(deadline, start).await.map_err(timed_out)?;
            self.start = None;
        }
        // The queue has exited and its thread-local ring is closed. Release the
        // character device before sending STOP; retain buffers until STOP succeeds.
        self.raw = None;
        Ok(())
    }

    pub(super) fn release_stopped_buffers(&mut self) {
        if let Some(mut buffers) = self.retired.take() {
            drop(std::mem::take(&mut buffers.0));
        }
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

pub(super) async fn stop(id: u32) -> io::Result<()> {
    control_device(id, UblkCtrl::stop_dev).await
}

pub(super) async fn delete(id: u32) -> io::Result<()> {
    control_device(id, UblkCtrl::del_dev).await
}

async fn control_device(
    id: u32,
    command: fn(&UblkCtrl) -> Result<i32, UblkError>,
) -> io::Result<()> {
    spawn_blocking(move || {
        let result = match UblkCtrl::new_simple(device_number(id)?) {
            Ok(ctrl) => check_result(command(&ctrl)),
            Err(error) => Err(driver_error(error)),
        };
        match result {
            Err(error)
                if error.raw_os_error() == Some(libc::EOPNOTSUPP)
                    && ["c", "b"].iter().all(|kind| {
                        std::fs::symlink_metadata(format!("/dev/ublk{kind}{id}"))
                            .is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
                    }) =>
            {
                Err(io::Error::from_raw_os_error(libc::ENODEV))
            }
            result => result,
        }
    })
    .await
    .map_err(io::Error::other)?
}

async fn serve_queue(
    raw: &UblkDev,
    image: &Image,
    buffers: &mut [IoBuf<u8>],
    mut stop: watch::Receiver<bool>,
    failure: &watch::Sender<bool>,
    ready: oneshot::Sender<()>,
) -> io::Result<()> {
    let worker = Queue::open(raw)?;
    let queue = &worker.queue;
    let event = completion_event()?;
    let closing = Cell::new(false);
    let active = Cell::new(0);
    let mut slots = FuturesUnordered::new();
    for (tag, buf) in queue.tags().zip(buffers) {
        slots.push(serve_slot(queue, image, tag, buf, &closing, &active));
    }
    let mut ready = Some(ready);
    let mut stop = pin!(stop.wait_for(|stop| *stop));
    poll_fn(|cx| {
        if !closing.get() && stop.as_mut().poll(cx).is_ready() {
            closing.set(true);
        }
        // Drain the event counter to WouldBlock and keep polling readiness
        // until Tokio registers this task's waker for the next completion.
        while let Poll::Ready(readiness) = event.poll_read_ready(cx) {
            if let Ok(read) = readiness?.try_io(|fd| fd.get_ref().read_exact(&mut [0; 8])) {
                read?;
            }
        }
        ublk_reap_io_events_with_update_queue(queue, false, None, |cqe| {
            if cqe.result() < 0 {
                failure.send_replace(true);
                closing.set(true);
            }
            ublk_wake_task(cqe.user_data(), cqe);
        })
        .map_err(driver_error)?;
        while let Poll::Ready(Some(result)) = Pin::new(&mut slots).poll_next(cx) {
            if let Err(error) = result {
                failure.send_replace(true);
                closing.set(true);
                afs_logging::error!("overlaybd.ublk_completion"; "error" => error.to_string());
            }
        }
        if closing.get() && active.get() == 0 {
            return Poll::Ready(Ok(()));
        }
        with_task_io_ring(|ring| ring.submit())?;
        if let Some(ready) = ready.take() {
            // The connector journal owns recovery metadata. Leave the
            // library's optional /run/ublksrvd JSON export unconfigured.
            let _ = ready.send(());
        }
        Poll::Pending
    })
    .await
}

fn completion_event() -> io::Result<AsyncFd<std::fs::File>> {
    // SAFETY: eventfd takes only a counter and flags. A successful call
    // returns a new descriptor owned by this thread.
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fd is the newly allocated eventfd and has no other owner.
    let file = std::fs::File::from(unsafe { OwnedFd::from_raw_fd(fd) });
    with_task_io_ring(|ring| ring.submitter().register_eventfd(file.as_raw_fd()))?;
    AsyncFd::with_interest(file, tokio::io::Interest::READABLE)
}

async fn serve_slot(
    queue: &UblkQueue<'_>,
    image: &Image,
    tag: u16,
    buffer: &mut IoBuf<u8>,
    closing: &Cell<bool>,
    active: &Cell<usize>,
) -> io::Result<()> {
    check_result(
        queue
            .submit_io_prep_cmd(tag, BufDesc::Slice(buffer.as_slice()), 0, Some(buffer))
            .await,
    )?;
    while !closing.get() {
        let request = *queue.get_iod(tag);
        active.set(active.get() + 1);
        let result = read_request(image, &request, buffer.as_mut_slice()).await;
        active.set(active.get() - 1);
        if closing.get() {
            break;
        }
        check_result(
            queue
                .submit_io_commit_cmd(tag, BufDesc::Slice(buffer.as_slice()), result)
                .await,
        )?;
    }
    Ok(())
}

async fn read_request(image: &Image, request: &sys::ublksrv_io_desc, buffer: &mut [u8]) -> i32 {
    match request.op_flags & 0xff {
        sys::UBLK_IO_OP_READ => {
            let Some(offset) = request.start_sector.checked_mul(SECTOR_SIZE) else {
                return -libc::EIO;
            };
            let length = request.nr_sectors as usize * SECTOR_SIZE as usize;
            let Some(output) = buffer.get_mut(..length) else {
                return -libc::EIO;
            };
            match image.read_at(offset, output).await {
                Ok(()) => length as i32,
                Err(error) => {
                    afs_logging::warn!("overlaybd.block_read_failed"; "offset" => offset, "bytes" => length, "error" => error.to_string());
                    -libc::EIO
                }
            }
        }
        sys::UBLK_IO_OP_FLUSH => 0,
        _ => -libc::EROFS,
    }
}

fn device_number(id: u32) -> io::Result<i32> {
    i32::try_from(id).map_err(io::Error::other)
}

fn timed_out(error: tokio::time::error::Elapsed) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, error)
}

fn check_result(result: Result<i32, UblkError>) -> io::Result<()> {
    match result {
        Ok(code) if code >= 0 => Ok(()),
        Ok(code) => Err(io::Error::from_raw_os_error(code.saturating_neg())),
        Err(error) => Err(driver_error(error)),
    }
}

fn driver_error(error: UblkError) -> io::Error {
    match error {
        UblkError::OtherError(code) | UblkError::UringIOError(code) => {
            io::Error::from_raw_os_error(if code < 0 {
                code.saturating_neg()
            } else {
                code
            })
        }
        UblkError::IOError(error) => error,
        other => io::Error::other(other),
    }
}
