use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::sync::oneshot;
use tokio::time::{timeout_at, Duration, Instant};

use crate::{AutoRegBuffer, IOBuffer, UVMUblkCtrl, UVMUblkQueue, UserBuffer, UBLK_QUEUE_URING};
use storage_util::io_ring::AsyncIoRing;

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
        Self::open_device(ctrl, self.tgt_name, tgt).await
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
                    return timeout_at(deadline, Self::open_device(ctrl, self.tgt_name, tgt))
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
        ctrl: UVMUblkCtrl,
        tgt_name: &'static str,
        tgt: T,
    ) -> Result<UVMUblkDev<T>> {
        let params = tgt.ublk_dev_params(&ctrl.dev_info);
        // open the char device, retry to sync with udev
        let cdev_path = ctrl.get_cdev_path();
        let mut attempt = 0;
        let cdev = loop {
            match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&cdev_path)
            {
                Ok(file) => break file,
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
        };

        Ok(UVMUblkDev {
            ctrl,
            tgt_name,
            params,
            inner: Arc::new(UVMUblkDevInner {
                cdev_file: cdev,
                queue_threads: Mutex::new(vec![]),
            }),
            tgt: Arc::new(tgt),
        })
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
    pub cdev_file: std::fs::File,
    pub queue_threads: Mutex<Vec<oneshot::Receiver<Result<()>>>>,
}

impl Drop for UVMUblkDevInner {
    fn drop(&mut self) {
        tracing::debug!("UVMUblkDevInner has dropped");
    }
}

impl UVMUblkQueue {
    /// The job for each slot within the queue.
    /// - `tag`: a slot id within the the queue (corresponding to the depth).
    async fn slot_task<T: UVMUblkTarget>(
        self: Rc<Self>,
        tag: u16,
        tgt: Arc<T>,
        readiness: Option<Rc<QueueReadiness>>,
    ) -> Result<()> {
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
        // FIXME: should we handle EINTR?
        let pending = self.prep_slot(tag, &buf).context("prep_slot")?;
        if let Some(readiness) = readiness {
            let remaining = readiness.remaining.get() - 1;
            readiness.remaining.set(remaining);
            if remaining == 0 {
                self.ring
                    .borrow()
                    .submit()
                    .context("submit recovery fetches")?;
                if let Some(sender) = readiness.sender.borrow_mut().take() {
                    let _ = sender.send(());
                }
            }
        }
        let res = pending.await;
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
            let res = self
                .submit_and_fetch_for_slot(tag, &buf, res)
                .context("submit and fetch from ring")?
                .await;
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
        for queue in readiness {
            timeout_at(deadline, queue)
                .await
                .context("ublk recovery queue readiness deadline")?
                .context("ublk recovery queue exited before submitting fetches")?;
        }
        timeout_at(deadline, self.ctrl.end_user_recovery())
            .await
            .context("ublk recovery end deadline")?
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
                let res = Self::queue_work(inner, tgt, qid, dev_info, ready);
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
                    if let Some(uring) = uring.get() {
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
        local_set.block_on(&rt, async {
            let queue = UVMUblkQueue::new(qid, dev_info, &inner.cdev_file)
                .context("create uvm ublk queue")?;
            tgt.init(qid, &queue.ring)
                .await
                .context("init target for uvm ublk queue")?;
            let queue = Rc::new(queue);
            let readiness = ready.map(|sender| {
                Rc::new(QueueReadiness {
                    remaining: Cell::new(queue.iodepth()),
                    sender: RefCell::new(Some(sender)),
                })
            });
            let mut join_set = tokio::task::JoinSet::new();
            for tag in 0..queue.iodepth() as u16 {
                let tgt = tgt.clone();
                let queue = queue.clone();
                join_set.spawn_local(queue.slot_task(tag, tgt, readiness.clone()));
            }
            drop(readiness);
            // we only wait for the slot_task, after that, we just drop the runtime and
            // background task, to close the UBLK_QUEUE_URING.
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        tracing::info!(err = format_args!("{err:#}"), "queue slot task finished");
                    }
                    Err(err) => {
                        tracing::error!(?err, "join slot_task failed");
                    }
                }
            }
            Ok(())
        })
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
    pub async fn start(&mut self) -> Result<()> {
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
        // spawn queue workers
        Self::spawn_queue_workers(
            &self.inner,
            &self.tgt,
            dev_info,
            false,
            Self::spawn_queue_worker_thread,
        )?;
        // start the device, send command to ublkdrv
        {
            self.ctrl
                .set_params(self.params)
                .await
                .context("set params when start UVMUblkDev")?;
            self.ctrl
                .start_dev()
                .await
                .context("start the device when start UVMUblkDev")?;
        }
        Ok(())
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

    pub async fn wait_for_bg_tasks(&mut self) {
        let tasks = {
            let mut tasks = self.inner.queue_threads.lock().unwrap();
            std::mem::take(&mut *tasks)
        };
        for task in tasks {
            match task.await {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => {
                    tracing::error!(?err, "queue task failed");
                }
                Err(err) => {
                    tracing::error!(?err, "recv from queue channel failed");
                }
            }
        }
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
