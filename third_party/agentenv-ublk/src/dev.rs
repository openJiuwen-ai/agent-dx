use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::sync::{oneshot, watch};
use tokio::time::{timeout_at, Duration, Instant};

use crate::io_buffer::UserBufferMemory;
use crate::{AutoRegBuffer, IOBuffer, UVMUblkCtrl, UVMUblkQueue, UserBuffer, UBLK_QUEUE_URING};
use storage_util::io_ring::{AsyncIoRing, RingFuture};

pub struct UVMUblkDevBuilder<T: UVMUblkTarget> {
    ctrl: UVMUblkCtrl,
    tgt_name: &'static str,
    tgt: Option<T>,
}

impl<T: UVMUblkTarget> UVMUblkDevBuilder<T> {
    pub fn new(ctrl: UVMUblkCtrl) -> Self {
        Self {
            ctrl,
            tgt_name: T::DEV_NAME,
            tgt: None,
        }
    }

    pub fn set_target(mut self, tgt: T) -> Self {
        self.tgt = Some(tgt);
        self
    }

    /// Create the new Ublk device.
    /// It will send ADD command to ublkdrv, to create /dev/ublcX device.
    pub async fn build(self) -> Result<UVMUblkDev<T>> {
        let tgt = self.tgt.ok_or_else(|| anyhow!("do not set target"))?;

        let mut ctrl = self.ctrl;
        ctrl.ensure_recovery_supported().await?;
        tracing::info!(
            requested_dev_id = ctrl.dev_info.dev_id,
            zero_copy = (ctrl.dev_info.flags & (ublk_sys::UBLK_F_SUPPORT_ZERO_COPY as u64)) != 0,
            "try add_dev"
        );
        ctrl.add_dev()
            .await
            .context("add dev when build UVMUblkDev")?;
        tracing::info!(
            dev_id = ctrl.dev_info.dev_id,
            zero_copy = (ctrl.dev_info.flags & (ublk_sys::UBLK_F_SUPPORT_ZERO_COPY as u64)) != 0,
            "device has been added to ublkdrv"
        );
        Self::open_device(ctrl, self.tgt_name, tgt, true).await
    }

    /// Reattach to the existing block device. The caller must verify that the
    /// target maps precisely the same bytes and geometry as the original one.
    pub async fn recover(self, deadline: Instant) -> Result<UVMUblkDev<T>> {
        let tgt = self.tgt.ok_or_else(|| anyhow!("do not set target"))?;
        let mut ctrl = self.ctrl;
        // GET_DEV_INFO lends kernel memory owned by ctrl. Keep it alive until
        // completion even when the caller's deadline expires.
        ctrl = timeout_at(
            deadline,
            tokio::spawn(async move {
                ctrl.get_dev_info().await?;
                Ok::<_, anyhow::Error>(ctrl)
            }),
        )
        .await
        .context("ublk device info deadline")???;
        if !ctrl.can_recover() {
            bail!("device was created without user recovery and reissue");
        }
        // The old task can have exited before the kernel finishes closing
        // its character device or quiescing the queue.
        loop {
            match timeout_at(deadline, ctrl.start_user_recovery())
                .await
                .context("ublk recovery start deadline")?
            {
                Ok(()) => {
                    return timeout_at(deadline, Self::open_device(ctrl, self.tgt_name, tgt, false))
                        .await
                        .context("ublk recovery device open deadline")?
                }
                Err(error)
                    if error
                        .root_cause()
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|cause| {
                            matches!(cause.raw_os_error(), Some(libc::EBUSY | libc::EINTR))
                        }) =>
                {
                    tokio::time::sleep_until(
                        deadline.min(Instant::now() + Duration::from_millis(100)),
                    )
                    .await;
                    if Instant::now() >= deadline {
                        return Err(error).context("timed out waiting for ublk device to quiesce");
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn open_device(
        mut ctrl: UVMUblkCtrl,
        tgt_name: &'static str,
        tgt: T,
        delete_on_open_failure: bool,
    ) -> Result<UVMUblkDev<T>> {
        let params = tgt.ublk_dev_params(&ctrl.dev_info);
        // open the char device, retry to sync with udev
        let cdev_path = ctrl.get_cdev_path();
        let cdev = match Self::open_character(&cdev_path).await {
            Ok(file) => file,
            Err(error) => {
                if delete_on_open_failure {
                    match timeout_at(Instant::now() + Duration::from_secs(5), ctrl.del_dev()).await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(cleanup)) => {
                            return Err(error
                                .context(format!("delete new ublk device failed: {cleanup:#}")))
                        }
                        Err(cleanup) => {
                            return Err(error
                                .context(format!("delete new ublk device deadline: {cleanup}")))
                        }
                    }
                }
                return Err(error);
            }
        };

        Ok(UVMUblkDev {
            ctrl,
            tgt_name,
            params,
            inner: Arc::new(UVMUblkDevInner {
                cdev_file: Mutex::new(Some(cdev)),
                queue_threads: Mutex::new(vec![]),
                queue_stop: watch::channel(false).0,
                queue_failed: watch::channel(false).0,
                buffers: Mutex::new(Vec::new()),
            }),
            tgt: Arc::new(tgt),
        })
    }

    async fn open_character(cdev_path: &std::path::Path) -> Result<std::fs::File> {
        let mut attempt = 0;
        loop {
            match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(cdev_path)
            {
                Ok(file) => return Ok(file),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                    ) =>
                {
                    if attempt >= 64 {
                        bail!("try open {cdev_path:?} for {attempt} but still failed {err:?}");
                    }
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(err) => {
                    bail!("try open {cdev_path:?} failed: {err:?}");
                }
            }
        }
    }
}

/// The basic abstraction of ublk device.
/// The implementation can embed this structure into its device implementation.
pub struct UVMUblkDev<T: UVMUblkTarget> {
    pub ctrl: UVMUblkCtrl,
    params: ublk_sys::ublk_params,
    tgt_name: &'static str,
    pub inner: Arc<UVMUblkDevInner>,
    tgt: Arc<T>,
}

pub struct UVMUblkDevInner {
    /// /dev/ublkcN
    cdev_file: Mutex<Option<std::fs::File>>,
    pub queue_threads: Mutex<Vec<oneshot::Receiver<Result<()>>>>,
    queue_stop: watch::Sender<bool>,
    queue_failed: watch::Sender<bool>,
    buffers: Mutex<Vec<Arc<UserBufferMemory>>>,
}

impl Drop for UVMUblkDevInner {
    fn drop(&mut self) {
        let buffers = self
            .buffers
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        if !buffers.is_empty() {
            // An unconfirmed stop can leave these addresses in kernel use.
            // Keep those allocations until process exit.
            std::mem::forget(std::mem::take(buffers));
        }
        tracing::debug!("UVMUblkDevInner has dropped");
    }
}

impl UVMUblkDevInner {
    fn signal_queue_failure(&self) {
        if self.queue_stop.send_if_modified(|stop| {
            if *stop {
                false
            } else {
                *stop = true;
                true
            }
        }) {
            self.queue_failed.send_replace(true);
        }
    }
}

impl UVMUblkQueue {
    async fn wait_for_fetch(fetch: RingFuture, stop: &mut watch::Receiver<bool>) -> Option<i32> {
        tokio::select! {
            biased;
            res = fetch => (!*stop.borrow()).then_some(res),
            _ = async { let _ = stop.wait_for(|stopping| *stopping).await; } => {
                // ublk commands are canceled when the queue ring closes.
                // Their allocations stay with the device until STOP completes.
                None
            }
        }
    }

    /// The job for each slot within the queue.
    /// - `tag`: a slot id within the the queue (corresponding to the depth).
    async fn slot_task<T: UVMUblkTarget>(
        self: Rc<Self>,
        tag: u16,
        tgt: Arc<T>,
        readiness: Option<Rc<QueueReadiness>>,
        device: Arc<UVMUblkDevInner>,
    ) -> Result<()> {
        let mut stop = device.queue_stop.subscribe();
        if *stop.borrow() {
            return Ok(());
        }
        let bufsize = self.dev_info.max_io_buf_bytes;
        let mut buf = if self.is_zero_copy() {
            IOBuffer::AutoReg(
                AutoRegBuffer::new(self.ring.clone(), bufsize as _)
                    .await
                    .context("allocate io auto_ref buffer")?,
            )
        } else {
            IOBuffer::User(
                UserBuffer::new(self.ring.clone(), bufsize as _, 512)
                    .await
                    .context("allocate io user buffer")?,
            )
        };

        let mut extra = match tgt.per_slot_extra_buf_len() {
            Some(size) if size.is_multiple_of(512) => Some(IOBuffer::User(
                UserBuffer::new(self.ring.clone(), size, 512)
                    .await
                    .context("allocate extra buffer")?,
            )),
            Some(size) => {
                bail!("extra buffer length ({size}) not aligned");
            }
            None => None,
        };
        {
            let mut buffers = device
                .buffers
                .lock()
                .map_err(|_| anyhow!("ublk retained buffers lock poisoned"))?;
            for buffer in std::iter::once(&mut buf).chain(extra.as_mut()) {
                if let IOBuffer::User(user) = buffer {
                    buffers.push(user.retain_for_device());
                }
            }
        }
        // FIXME: should we handle EINTR?
        let pending = self.prep_slot(tag, &buf).context("prep_slot")?;
        if let Some(readiness) = readiness {
            let remaining = readiness.remaining.get() - 1;
            readiness.remaining.set(remaining);
            if remaining == 0 {
                let submitted = self
                    .ring
                    .borrow()
                    .submit()
                    .context("submit recovery fetches");
                if let Err(error) = submitted {
                    device.signal_queue_failure();
                    return Err(error);
                }
                if let Some(sender) = readiness.sender.borrow_mut().take() {
                    let _ = sender.send(());
                }
            }
        }
        let Some(res) = Self::wait_for_fetch(pending, &mut stop).await else {
            return Ok(());
        };
        if res < 0 {
            tracing::debug!(
                tag,
                dev_id=self.dev_info.dev_id,
                err = ?std::io::Error::from_raw_os_error(-res),
                "prepare slot failed"
            );
            bail!(
                "prep slot ({tag}) get result from ring failed: {:?}",
                std::io::Error::from_raw_os_error(-res)
            );
        }

        loop {
            let io_desc = self
                .io_desc(tag)
                .ok_or_else(|| anyhow!("cannot get io_dsec at {tag}"))?;
            // TODO: update auto buffer's size according to io_desc
            let res = tgt
                .handle_io_request(
                    self.queue_id,
                    tag,
                    io_desc,
                    &mut buf,
                    extra.as_mut(),
                    &self.ring,
                )
                .await;
            // Let target I/O finish without dropping its borrowed buffers.
            if *stop.borrow() {
                return Ok(());
            }
            let fetch = self
                .submit_and_fetch_for_slot(tag, &buf, res)
                .context("submit and fetch from ring")?;
            let Some(res) = Self::wait_for_fetch(fetch, &mut stop).await else {
                return Ok(());
            };
            // FIXME: should we handle EINTR?
            if res < 0 {
                tracing::debug!(
                    tag,
                    dev_id=self.dev_info.dev_id,
                    err = ?std::io::Error::from_raw_os_error(-res),
                    "submit and fetch for slot failed"
                );
                bail!(
                    "submit and fetch for slot from ring failed: {:?}",
                    std::io::Error::from_raw_os_error(-res)
                );
            }
        }
    }
}

struct QueueReadiness {
    remaining: Cell<usize>,
    sender: RefCell<Option<oneshot::Sender<()>>>,
}

impl<T: UVMUblkTarget> UVMUblkDev<T> {
    pub async fn resume(&self, deadline: Instant) -> Result<()> {
        let readiness = Self::spawn_queue_workers(
            &self.inner,
            &self.tgt,
            self.ctrl.dev_info,
            true,
            Self::spawn_queue_worker_thread,
        )?;
        // Do not issue a kernel command waiting for queues that could not
        // open, initialize or submit their first fetches.
        let complete = async {
            Self::wait_for_queue_readiness(readiness).await?;
            self.ctrl.end_user_recovery().await
        };
        Self::wait_for_queue_start(&self.inner, deadline, complete).await
    }

    fn spawn_queue_worker_thread(
        inner: Arc<UVMUblkDevInner>,
        tgt: Arc<T>,
        qid: u16,
        dev_info: ublk_sys::ublksrv_ctrl_dev_info,
        tx: oneshot::Sender<Result<()>>,
        ready: Option<oneshot::Sender<()>>,
    ) -> std::io::Result<()> {
        thread::Builder::new()
            .name(format!("ublk-q-{}-{qid}", dev_info.dev_id))
            .spawn(move || {
                let res = Self::queue_work(inner.clone(), tgt, qid, dev_info, ready);
                inner.signal_queue_failure();
                let _ = tx.send(res);
            })
            .map(|_| ())
    }

    fn queue_work(
        inner: Arc<UVMUblkDevInner>,
        tgt: Arc<T>,
        qid: u16,
        dev_info: ublk_sys::ublksrv_ctrl_dev_info,
        ready: Option<oneshot::Sender<()>>,
    ) -> Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .on_thread_park(move || {
                UBLK_QUEUE_URING.with(|uring| {
                    if let Some(uring) = uring.borrow().as_ref() {
                        let _ = uring
                            .borrow()
                            .submit()
                            .with_context(|| format!("queue {qid} submit for io uring"))
                            .inspect_err(|err| {
                                tracing::error!(?err, "on_thread_park callback failed")
                            });
                    }
                })
            })
            .build()
            .context("build tokio runtime when spawn queue workers")?;
        let local_set = tokio::task::LocalSet::new();
        let result = local_set.block_on(&rt, async {
            if *inner.queue_stop.borrow() {
                return Ok(());
            }
            let queue = {
                let file = inner
                    .cdev_file
                    .lock()
                    .map_err(|_| anyhow!("ublk character device lock poisoned"))?;
                let file = file
                    .as_ref()
                    .ok_or_else(|| anyhow!("ublk character device closed"))?;
                Rc::new(UVMUblkQueue::new(qid, dev_info, file).context("create uvm ublk queue")?)
            };
            tgt.init(qid, &queue.ring)
                .await
                .context("init target for uvm ublk queue")?;
            let readiness = ready.map(|sender| {
                Rc::new(QueueReadiness {
                    remaining: Cell::new(queue.iodepth()),
                    sender: RefCell::new(Some(sender)),
                })
            });
            let mut slots = tokio::task::JoinSet::new();
            for tag in 0..queue.iodepth() as u16 {
                slots.spawn_local(queue.clone().slot_task(
                    tag,
                    tgt.clone(),
                    readiness.clone(),
                    inner.clone(),
                ));
            }
            drop(readiness);
            let mut first_error = None;
            while let Some(result) = slots.join_next().await {
                if let Err(error) = result.context("join ublk slot").and_then(|result| result) {
                    inner.signal_queue_failure();
                    first_error.get_or_insert(error);
                }
            }
            first_error.map_or(Ok(()), Err)
        });
        // Drop the completion task and the ring (including its fixed-file
        // reference) before reporting that the queue has stopped.
        drop(local_set);
        UBLK_QUEUE_URING.with(|uring| drop(uring.borrow_mut().take()));
        result
    }

    async fn wait_for_queue_readiness(readiness: Vec<oneshot::Receiver<()>>) -> Result<()> {
        for queue in readiness {
            queue
                .await
                .context("ublk queue exited before submitting fetches")?;
        }
        Ok(())
    }

    async fn wait_for_queue_start(
        inner: &UVMUblkDevInner,
        deadline: Instant,
        control: impl std::future::Future<Output = Result<()>>,
    ) -> Result<()> {
        let mut failure = inner.queue_failed.subscribe();
        tokio::select! {
            biased;
            _ = failure.wait_for(|failed| *failed) => bail!("ublk queue failed during activation"),
            result = timeout_at(deadline, control) => result.context("ublk queue activation deadline")?,
        }
    }

    fn spawn_queue_workers(
        inner: &Arc<UVMUblkDevInner>,
        tgt: &Arc<T>,
        dev_info: ublk_sys::ublksrv_ctrl_dev_info,
        report_ready: bool,
        mut spawn_one: impl FnMut(
            Arc<UVMUblkDevInner>,
            Arc<T>,
            u16,
            ublk_sys::ublksrv_ctrl_dev_info,
            oneshot::Sender<Result<()>>,
            Option<oneshot::Sender<()>>,
        ) -> std::io::Result<()>,
    ) -> Result<Vec<oneshot::Receiver<()>>> {
        let mut result_rxs = vec![];
        let mut readiness = vec![];
        for qid in 0..dev_info.nr_hw_queues {
            let (tx, rx) = oneshot::channel();
            let ready = report_ready.then(|| {
                let (tx, rx) = oneshot::channel();
                readiness.push(rx);
                tx
            });
            // NOTE: we create a separate io uring for each queue.
            // I decide to not share one io uring among multiple devices.
            // When I use shared io uring + register ublk char device, it will stucked when
            // DEL_DEV. Since unregister fd only dec the refcount of current rsrc_node in kernel,
            // other devices might hold ref on the rsrc_node (even using different slot in sparse
            // file table), prevening the ublk char devices been put.
            let spawn_result = spawn_one(inner.clone(), tgt.clone(), qid, dev_info, tx, ready);
            match spawn_result {
                Ok(()) => result_rxs.push(rx),
                Err(err) => {
                    inner.queue_threads.lock().unwrap().extend(result_rxs);
                    tracing::error!(
                        dev_id = dev_info.dev_id,
                        qid,
                        nr_hw_queues = dev_info.nr_hw_queues,
                        queue_depth = dev_info.queue_depth,
                        raw_os_error = ?err.raw_os_error(),
                        error_kind = ?err.kind(),
                        error = %err,
                        "failed to spawn ublk queue worker"
                    );
                    return Err(err).with_context(|| {
                        format!(
                            "spawn ublk queue worker dev_id={} qid={} nr_hw_queues={} queue_depth={}",
                            dev_info.dev_id,
                            qid,
                            dev_info.nr_hw_queues,
                            dev_info.queue_depth,
                        )
                    });
                }
            }
        }
        inner.queue_threads.lock().unwrap().extend(result_rxs);
        Ok(readiness)
    }

    /// Start the ublk device.
    /// 1. Create queue workers.
    /// 2. Send set_params command to ublkdrv.
    /// 3. Send start_dev command to ublkdrv.
    ///
    /// After this function return, the /dev/ublkbX has already been created.
    #[tracing::instrument(skip_all, fields(dev_id), err(Debug))]
    pub async fn start(&mut self, deadline: Instant) -> Result<()> {
        // check the device state
        let dev_info = {
            // update the newest dev info
            self.ctrl
                .get_dev_info()
                .await
                .context("read dev info when start UVMUblkDev")?;
            if self.ctrl.dev_info.state != ublk_sys::UBLK_S_DEV_DEAD as u16 {
                bail!("device state is not dead: {}", self.ctrl.dev_info.state);
            }
            self.ctrl.dev_info
        };
        tracing::Span::current().record("dev_id", dev_info.dev_id);
        // Set parameters before starting workers. Only a submitted queue
        // can make START_DEV progress, and an early CQE error wins the race.
        self.ctrl
            .set_params(self.params)
            .await
            .context("set params when start UVMUblkDev")?;
        let readiness = Self::spawn_queue_workers(
            &self.inner,
            &self.tgt,
            dev_info,
            true,
            Self::spawn_queue_worker_thread,
        )?;
        let start = async {
            Self::wait_for_queue_readiness(readiness).await?;
            self.ctrl
                .start_dev()
                .await
                .context("start the device when start UVMUblkDev")
        };
        Self::wait_for_queue_start(&self.inner, deadline, start).await
    }

    pub fn target_name(&self) -> String {
        self.tgt_name.to_string()
    }

    /// Delete the device, which will send DEL_DEV request to ublkdrv and
    /// remove the `/dev/ublkcX` char device.
    ///
    /// The kernel will wait for all reference of ublkc device has released,
    /// to make sure that when DEL_DEV request return, the dev_id is freed.
    /// So to del a device, you should either:
    /// 1. Stop the old device process.
    /// 2. Call [Self::stop] first.
    #[tracing::instrument(skip_all, fields(dev_id=self.ctrl.dev_info.dev_id), err(Debug))]
    pub async fn del(&mut self) -> Result<()> {
        let dev_id = self.ctrl.dev_info.dev_id;
        self.ctrl
            .del_dev()
            .await
            .inspect_err(|err| tracing::error!(?err, dev_id, "delete device failed"))
    }

    pub fn device_path(&self) -> PathBuf {
        PathBuf::from(format!("/dev/ublkb{}", self.ctrl.dev_info.dev_id))
    }

    pub fn dev_id(&self) -> u32 {
        self.ctrl.dev_info.dev_id
    }

    /// Sticky fault state, visible even to a subscriber installed after activation.
    pub fn queue_failure(&self) -> watch::Receiver<bool> {
        self.inner.queue_failed.subscribe()
    }

    pub fn queues_are_healthy(&self) -> bool {
        !*self.inner.queue_failed.borrow() && !*self.inner.queue_stop.borrow()
    }

    pub fn begin_queue_stop(&self) {
        self.inner.queue_stop.send_replace(true);
    }

    pub async fn stop_queue_workers(&self, deadline: Instant) -> Result<()> {
        self.begin_queue_stop();
        loop {
            let task = self
                .inner
                .queue_threads
                .lock()
                .map_err(|_| anyhow!("ublk queue workers lock poisoned"))?
                .pop();
            let Some(mut task) = task else {
                drop(
                    self.inner
                        .cdev_file
                        .lock()
                        .map_err(|_| anyhow!("ublk character device lock poisoned"))?
                        .take(),
                );
                return Ok(());
            };
            match timeout_at(deadline, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(err))) => tracing::error!(?err, "queue task failed"),
                Ok(Err(err)) => tracing::error!(?err, "recv from queue channel failed"),
                Err(error) => {
                    self.inner
                        .queue_threads
                        .lock()
                        .map_err(|_| anyhow!("ublk queue workers lock poisoned"))?
                        .push(task);
                    return Err(error).context("ublk queue workers did not stop");
                }
            }
        }
    }

    /// Release slot allocations retained across character device close.
    ///
    /// # Safety
    /// The caller must have closed the queue workers and received successful
    /// STOP or proof from the kernel that the device no longer exists.
    pub unsafe fn release_stopped_buffers(&self) -> Result<()> {
        self.inner
            .buffers
            .lock()
            .map_err(|_| anyhow!("ublk retained buffers lock poisoned"))?
            .clear();
        Ok(())
    }

    /// Return the device target implementation
    pub fn target(&self) -> &Arc<T> {
        &self.tgt
    }
}

#[async_trait(?Send)]
pub trait UVMUblkTarget: Send + Sync + 'static {
    const DEV_NAME: &str;
    /// Init the target. For example, the target might register fd on to the io_ring.
    async fn init(&self, qid: u16, io_ring: &AsyncIoRing) -> Result<()>;

    /// The target can ask for an extra IO buffer, for each slot.
    /// Typically, the returned size should be multiple of 512.
    fn per_slot_extra_buf_len(&self) -> Option<usize>;

    /// Return the ublk dev params. This will be send to ublkdrv (UBLK_U_CMD_SET_PARAMS).
    fn ublk_dev_params(&self, dev_info: &ublk_sys::ublksrv_ctrl_dev_info) -> ublk_sys::ublk_params;

    /// Handle the request, return the result.
    /// Since multiple queue and slot within each queue, will call this method concurrently,
    /// we can only get `&self` here.
    /// `extra` is an extra buffer allocated according to [UVMUblkTarget::per_slot_extra_buf_len].
    async fn handle_io_request(
        self: &Arc<Self>,
        qid: u16,
        tag: u16,
        io_desc: ublk_sys::ublksrv_io_desc,
        buf: &mut IOBuffer,
        extra: Option<&mut IOBuffer>,
        io_ring: &AsyncIoRing,
    ) -> i32;
}
