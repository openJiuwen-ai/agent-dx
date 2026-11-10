use super::{
    OverlaybdConfig,
    lsmt::SECTOR_SIZE,
    reader::Image,
    ublk::{self, Device},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{self, Write},
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::process::Command;
use tokio::time::{Instant, timeout, timeout_at};
use tokio::{sync::Mutex, task::JoinHandle};
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(60);
const IMAGE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const ACQUIRE_MAX_AGE_MS: u64 = 120_000;
const STOP_TIMEOUT: Duration = Duration::from_secs(5);
const QUEUE_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const QUEUE_STOP_RETRY_DELAY: Duration = Duration::from_millis(250);

#[derive(Clone, Serialize, Deserialize)]
struct Journal {
    device_id: u32,
    character: CharacterIdentity,
    refs: BTreeSet<String>,
    #[serde(default)]
    recovery: Option<RecoveryBinding>,
}

#[derive(Clone, Serialize, Deserialize)]
struct RecoveryBinding {
    configuration_sha256: String,
    disk_sequence: String,
    sectors: u64,
    block_size: u64,
}

// The number alone is recycled by the kernel. Compare the node identity
// and the boot before deleting an orphan from a previous process.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CharacterIdentity {
    boot: String,
    filesystem: u64,
    inode: u64,
}

impl CharacterIdentity {
    fn read(id: u32) -> io::Result<Self> {
        let metadata = fs::symlink_metadata(format!("/dev/ublkc{id}"))?;
        if !metadata.file_type().is_char_device() {
            return Err(io::Error::other("unexpected ublk character device"));
        }
        Ok(Self {
            boot: fs::read_to_string("/proc/sys/kernel/random/boot_id")?,
            filesystem: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[derive(Default, Serialize, Deserialize)]
struct Released {
    boot_id: String,
    deadlines: BTreeMap<String, u64>,
}

pub(super) struct Devices {
    config: OverlaybdConfig,
    journal: Option<Journal>,
    owned_id: Option<u32>,
    device: Option<Device>,
    serving: bool,
    owner: Weak<Mutex<Self>>,
    monitor: Option<QueueMonitor>,
    pending_stop: Option<JoinHandle<io::Result<bool>>>,
    pending_delete: Option<JoinHandle<io::Result<()>>>,
    released: Released,
}

struct QueueMonitor(JoinHandle<()>);

impl Drop for QueueMonitor {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Devices {
    pub(super) fn shared(config: OverlaybdConfig) -> io::Result<Arc<Mutex<Self>>> {
        let mut devices = Self::open(config)?;
        Ok(Arc::new_cyclic(|owner| {
            devices.owner = owner.clone();
            Mutex::new(devices)
        }))
    }

    fn open(config: OverlaybdConfig) -> io::Result<Self> {
        let journal = match fs::read(config.directory.join("state.json")) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        let released = match fs::read(config.directory.join("released.json")) {
            Ok(bytes) => {
                let released: Released = serde_json::from_slice(&bytes)?;
                if released.boot_id == boot_id {
                    released
                } else {
                    Released {
                        boot_id,
                        ..Released::default()
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Released {
                boot_id,
                ..Released::default()
            },
            Err(error) => return Err(error),
        };
        Ok(Self {
            config,
            released,
            journal,
            owned_id: None,
            device: None,
            serving: false,
            owner: Weak::new(),
            monitor: None,
            pending_stop: None,
            pending_delete: None,
        })
    }

    fn can_serve(&self) -> bool {
        self.serving && self.device.as_ref().is_some_and(Device::queues_are_healthy)
    }

    fn has_failed_queue_monitor(&self) -> bool {
        self.monitor.is_some() && !self.can_serve()
    }

    fn publish_serving(&mut self) -> io::Result<()> {
        let device = self
            .device
            .as_ref()
            .filter(|device| device.queues_are_healthy())
            .ok_or_else(|| io::Error::other("OverlayBD queue failed before serving"))?;
        let id = device.dev_id();
        let mut failure = device.queue_failure();
        let owner = self.owner.clone();
        self.monitor = Some(QueueMonitor(tokio::spawn(async move {
            if failure.wait_for(|failed| *failed).await.is_err() {
                return;
            }
            loop {
                {
                    let Some(owner) = owner.upgrade() else {
                        return;
                    };
                    let mut devices = owner.lock().await;
                    if devices.owned_id != Some(id) {
                        return;
                    }
                    if devices.serving {
                        devices.serving = false;
                        afs_logging::error!("overlaybd.queue_failed"; "device_id" => id);
                    }
                    match devices.stop_failed_device(id).await {
                        Ok(()) => {
                            if devices
                                .journal
                                .as_ref()
                                .is_some_and(|entry| entry.refs.is_empty())
                                && let Err(error) = devices.cleanup().await
                            {
                                afs_logging::error!("overlaybd.queue_cleanup_failed"; "device_id" => id, "error" => error.to_string());
                            }
                            devices.monitor = None;
                            return;
                        }
                        Err(error) if error.kind() == io::ErrorKind::TimedOut => {
                            afs_logging::warn!("overlaybd.queue_stop_retry"; "device_id" => id, "error" => error.to_string());
                        }
                        Err(error) => {
                            afs_logging::error!("overlaybd.queue_stop_failed"; "device_id" => id, "error" => error.to_string());
                            devices.monitor = None;
                            return;
                        }
                    }
                }
                // Let acquire reject and release persist references between attempts.
                tokio::time::sleep(QUEUE_STOP_RETRY_DELAY).await;
            }
        })));
        self.serving = true;
        Ok(())
    }

    pub(super) async fn acquire(
        &mut self,
        runtime_id: String,
        image: &str,
        deadline: u64,
    ) -> io::Result<Option<PathBuf>> {
        if image != self.config.image {
            return Ok(None);
        }
        let now = boot_time_ms()?;
        if deadline <= now
            || deadline > now.saturating_add(ACQUIRE_MAX_AGE_MS)
            || self
                .released
                .deadlines
                .get(&runtime_id)
                .is_some_and(|expiry| *expiry >= now)
        {
            return Err(io::Error::other(
                "expired or already released OverlayBD acquire",
            ));
        }
        if self
            .journal
            .as_ref()
            .is_some_and(|entry| entry.refs.is_empty())
            && !self.has_failed_queue_monitor()
        {
            self.cleanup().await?;
        }
        if self.journal.is_none() {
            self.create().await?;
        }
        let mut journal = self
            .journal
            .clone()
            .ok_or_else(|| io::Error::other("missing device journal"))?;
        // The mount alone does not prove that its userspace queue is alive.
        if !self.can_serve() {
            return Err(io::Error::other(
                "OverlayBD backend has no active device; recovery pending or old instances must be released",
            ));
        }
        let mount = self.mount_path();
        let expected = device_path(journal.device_id);
        if mounted_source(&mount).await?.as_deref() != Some(expected.as_path()) {
            return Err(io::Error::other("OverlayBD mount no longer matches device"));
        }
        if boot_time_ms()? >= deadline {
            if journal.refs.is_empty() {
                self.cleanup().await?;
            }
            return Err(io::Error::other(
                "OverlayBD acquire expired during device startup",
            ));
        }
        if !self.can_serve() {
            return Err(io::Error::other("OverlayBD queue failed during acquire"));
        }
        if journal.refs.insert(runtime_id) {
            self.persist(journal)?;
        }
        Ok(Some(mount))
    }

    pub(super) async fn release(&mut self, runtime_id: &str) -> io::Result<()> {
        // Persist the negative result even if an earlier acquire has not yet
        // reached this lock. No admitted acquire can survive the marker window.
        let now = boot_time_ms()?;
        self.released.deadlines.retain(|_, expiry| *expiry >= now);
        self.released.deadlines.insert(
            runtime_id.to_owned(),
            now.saturating_add(ACQUIRE_MAX_AGE_MS),
        );
        self.persist_json("released", &self.released)?;
        if let Some(mut journal) = self.journal.clone() {
            if journal.refs.remove(runtime_id) {
                self.persist(journal)?;
            }
            if self
                .journal
                .as_ref()
                .is_some_and(|entry| entry.refs.is_empty())
                && !self.has_failed_queue_monitor()
            {
                self.cleanup().await?;
            }
        }
        Ok(())
    }

    pub(super) async fn recover(&mut self) -> io::Result<()> {
        let Some(journal) = self
            .journal
            .as_ref()
            .filter(|entry| !entry.refs.is_empty())
            .cloned()
        else {
            return Ok(());
        };
        let Some(binding) = journal.recovery.as_ref() else {
            return Ok(());
        };
        let deadline = Instant::now() + RECOVERY_TIMEOUT;
        let id = journal.device_id;
        if !Self::is_recorded_device(&journal, binding)?
            || timeout_at(deadline, mounted_source(&self.mount_path()))
                .await
                .map_err(backend_error)??
                .as_deref()
                != Some(device_path(id).as_path())
        {
            return Err(io::Error::other(
                "OverlayBD device or mount identity changed",
            ));
        }
        if let Err(error) = self.recover_recorded_device(id, binding, deadline).await {
            // Never stop an ID that may now name a different device.
            if Self::is_recorded_device(&journal, binding).unwrap_or(false)
                && let Err(stop) = self.stop_failed_device(id).await
            {
                return Err(io::Error::other(format!("{error}; stop failed: {stop}")));
            }
            return Err(error);
        }
        afs_logging::info!("overlaybd.recovered"; "device_id" => id);
        Ok(())
    }

    fn is_recorded_device(journal: &Journal, binding: &RecoveryBinding) -> io::Result<bool> {
        let id = journal.device_id;
        Ok(CharacterIdentity::read(id)? == journal.character
            && disk_sequence(id)? == binding.disk_sequence)
    }

    async fn recover_recorded_device(
        &mut self,
        id: u32,
        binding: &RecoveryBinding,
        deadline: Instant,
    ) -> io::Result<()> {
        if self.configuration_sha256()? != binding.configuration_sha256 {
            return Err(io::Error::other("OverlayBD recovery configuration changed"));
        }

        // This task runs alongside Node startup. The same process's blob
        // routes must start serving before the target can reopen its layers.
        let image = loop {
            let attempt = deadline.min(Instant::now() + IMAGE_ATTEMPT_TIMEOUT);
            let result = timeout_at(attempt, self.open_image())
                .await
                .map_err(backend_error)
                .and_then(|result| result);
            match result {
                Ok(image) => break image,
                Err(error) if Instant::now() < deadline => {
                    afs_logging::warn!("overlaybd.recovery_waiting_for_image"; "error" => error.to_string());
                    tokio::time::sleep_until(
                        deadline.min(Instant::now() + Duration::from_millis(250)),
                    )
                    .await;
                }
                Err(error) => return Err(error),
            }
        };
        if image.sectors() != binding.sectors
            || SECTOR_SIZE != binding.block_size
            || self.configuration_sha256()? != binding.configuration_sha256
        {
            return Err(io::Error::other(
                "OverlayBD recovery image geometry or configuration changed",
            ));
        }
        let mut device = Device::recover(id, image).await?;
        let resumed = device.start(deadline).await;
        self.owned_id = Some(id);
        self.device = Some(device);
        resumed?;
        self.publish_serving()
    }

    async fn stop_failed_device(&mut self, id: u32) -> io::Result<()> {
        // STOP completes queued reads with EIO. DEL can wait for the sandbox
        // to close its rootfs, so leave deletion to the last release.
        self.stop_and_drain(id).await?;
        self.device = None;
        afs_logging::warn!("overlaybd.failed_device_stopped"; "device_id" => id);
        Ok(())
    }

    async fn stop_and_drain(&mut self, id: u32) -> io::Result<()> {
        if let Some(device) = &mut self.device {
            // The kernel aborts requests left owned by userspace on character
            // device release. STOP can wait indefinitely for one of those
            // requests if we send it while the character device is still open.
            device
                .stop_queue_workers(Instant::now() + QUEUE_DRAIN_TIMEOUT)
                .await?;
        }
        if self.pending_stop.is_none() {
            self.pending_stop = Some(tokio::spawn(async move {
                match ublk::stop(id).await {
                    Ok(()) => Ok(true),
                    Err(error) => match error.raw_os_error() {
                        Some(libc::ENODEV) => Ok(true),
                        Some(libc::EINVAL) => Ok(false),
                        _ => Err(error),
                    },
                }
            }));
        }
        self.await_pending_stop().await
    }

    async fn await_pending_stop(&mut self) -> io::Result<()> {
        if let Some(stop) = self.pending_stop.as_mut() {
            // Keep the same command and its control fd alive on timeout. The
            // next cleanup must finish it before deleting/reusing the ID.
            let result = timeout(STOP_TIMEOUT, stop)
                .await
                .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?;
            self.pending_stop = None;
            let confirmed = result.map_err(backend_error)??;
            if confirmed && let Some(device) = &mut self.device {
                device.release_stopped_buffers();
            }
        }
        Ok(())
    }

    fn configuration_sha256(&self) -> io::Result<String> {
        let mut digest = Sha256::new();
        digest.update(Sha256::digest(self.config.image.as_bytes()));
        for name in ["image.json", "global.json"] {
            digest.update(Sha256::digest(fs::read(self.config.directory.join(name))?));
        }
        Ok(format!("{:x}", digest.finalize()))
    }

    async fn open_image(&self) -> io::Result<Image> {
        Image::open(&self.config.directory.join("image.json")).await
    }

    async fn create(&mut self) -> io::Result<()> {
        let mount = self.mount_path();
        fs::create_dir_all(&mount)?;
        if mounted_source(&mount).await?.is_some() {
            return Err(io::Error::other("unexpected pre-existing rootfs mount"));
        }
        let configuration_sha256 = self
            .config
            .recover_on_restart
            .then(|| self.configuration_sha256())
            .transpose()?;
        let image = self.open_image().await?;
        let recovery = configuration_sha256.map(|configuration_sha256| RecoveryBinding {
            configuration_sha256,
            disk_sequence: String::new(),
            sectors: image.sectors(),
            block_size: SECTOR_SIZE,
        });
        let device = Device::create(image, recovery.is_some()).await?;
        let id = device.dev_id();
        self.device = Some(device);
        self.owned_id = Some(id);
        if let Err(error) = self
            .activate(mount, id, recovery)
            .await
            .and_then(|()| self.publish_serving())
        {
            if let Err(cleanup) = self.cleanup().await {
                return Err(io::Error::other(format!("{error}; cleanup: {cleanup}")));
            }
            return Err(error);
        }
        Ok(())
    }

    async fn activate(
        &mut self,
        mount: PathBuf,
        id: u32,
        recovery: Option<RecoveryBinding>,
    ) -> io::Result<()> {
        let mut journal = Journal {
            device_id: id,
            character: CharacterIdentity::read(id)?,
            refs: BTreeSet::new(),
            recovery: None,
        };
        self.persist(journal.clone())?;
        let device = self
            .device
            .as_mut()
            .ok_or_else(|| io::Error::other("missing new device"))?;
        device.start(Instant::now() + RECOVERY_TIMEOUT).await?;
        let path = device.device_path();
        for _ in 0..50 {
            if path
                .metadata()
                .is_ok_and(|meta| meta.file_type().is_block_device())
            {
                command(
                    Command::new("mount")
                        .args(["-t", "ext4", "-o", "ro,noload", "--"])
                        .arg(path)
                        .arg(mount),
                )
                .await?;
                if let Some(mut binding) = recovery {
                    binding.disk_sequence = disk_sequence(id)?;
                    journal.recovery = Some(binding);
                    self.persist(journal)?;
                }
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(io::Error::other("OverlayBD device node did not appear"))
    }

    async fn cleanup(&mut self) -> io::Result<()> {
        self.serving = false;
        self.await_pending_stop().await?;
        let mount = self.mount_path();
        if let Some(source) = mounted_source(&mount).await? {
            let id = self.journal.as_ref().map(|journal| journal.device_id);
            if id.is_none_or(|id| source != device_path(id)) {
                return Err(io::Error::other("refusing to unmount a foreign device"));
            }
            command(Command::new("umount").arg("--").arg(&mount)).await?;
        }
        let orphan_id = match &self.journal {
            Some(journal) if self.owned_id.is_none() => {
                match CharacterIdentity::read(journal.device_id) {
                    Ok(identity) if identity == journal.character => Some(journal.device_id),
                    Ok(_) => None,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                    Err(error) => return Err(error),
                }
            }
            _ => None,
        };
        if let Some(id) = self.owned_id.or(orphan_id) {
            if self.device.is_some() {
                self.stop_and_drain(id).await?;
                self.device = None;
            }
            let delete = self
                .pending_delete
                .get_or_insert_with(|| tokio::spawn(ublk::delete(id)));
            let deleted = timeout(STOP_TIMEOUT, delete)
                .await
                .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?;
            self.pending_delete = None;
            if let Err(error) = deleted.map_err(backend_error)?
                && error.raw_os_error() != Some(libc::ENODEV)
            {
                return Err(error);
            }
            self.owned_id = None;
        }
        // No new device can be installed while Devices is locked. Cancel the
        // old monitor before another request can acquire a recycled ID.
        self.monitor = None;
        match fs::remove_file(self.config.directory.join("state.json")) {
            Ok(()) => File::open(&self.config.directory)?.sync_all()?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        self.journal = None;
        Ok(())
    }

    fn persist(&mut self, journal: Journal) -> io::Result<()> {
        self.persist_json("state", &journal)?;
        self.journal = Some(journal);
        Ok(())
    }

    fn persist_json(&self, name: &str, value: &impl Serialize) -> io::Result<()> {
        let temporary = self.config.directory.join(format!("{name}.tmp"));
        let mut file = File::create(&temporary)?;
        file.write_all(&serde_json::to_vec(value)?)?;
        file.sync_all()?;
        fs::rename(
            temporary,
            self.config.directory.join(format!("{name}.json")),
        )?;
        File::open(&self.config.directory)?.sync_all()
    }

    fn mount_path(&self) -> PathBuf {
        self.config.directory.join("rootfs")
    }
}

pub(super) fn boot_time_ms() -> io::Result<u64> {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes the writable timespec on success.
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, time.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: clock_gettime returned success and initialized the timespec.
    let time = unsafe { time.assume_init() };
    Ok((time.tv_sec as u64).saturating_mul(1000) + time.tv_nsec as u64 / 1_000_000)
}

fn disk_sequence(id: u32) -> io::Result<String> {
    fs::read_to_string(format!("/sys/block/ublkb{id}/diskseq"))
}

fn device_path(id: u32) -> PathBuf {
    PathBuf::from(format!("/dev/ublkb{id}"))
}

async fn mounted_source(path: &Path) -> io::Result<Option<PathBuf>> {
    let output = Command::new("findmnt")
        .kill_on_drop(true)
        .args(["-rn", "-o", "SOURCE,OPTIONS", "--mountpoint"])
        .arg(path)
        .output()
        .await?;
    if output.status.code() == Some(1) && output.stderr.is_empty() {
        return Ok(None);
    }
    if !output.status.success() {
        return Err(backend_error(String::from_utf8_lossy(&output.stderr)));
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    let mut parts = raw.split_whitespace();
    match (parts.next(), parts.next(), parts.next()) {
        (Some(source), Some(options), None) if options.split(',').any(|option| option == "ro") => {
            Ok(Some(PathBuf::from(source)))
        }
        _ => Err(io::Error::other("unexpected OverlayBD rootfs mount")),
    }
}

async fn command(command: &mut Command) -> io::Result<()> {
    let result = timeout(RECOVERY_TIMEOUT, command.kill_on_drop(true).output())
        .await
        .map_err(backend_error)??;
    if !result.status.success() {
        return Err(backend_error(String::from_utf8_lossy(&result.stderr)));
    }
    Ok(())
}

fn backend_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delayed_acquire_cannot_recreate_a_released_reference_after_restart() {
        let temp = tempfile::tempdir().unwrap();
        let config = OverlaybdConfig {
            enabled: true,
            directory: temp.path().to_path_buf(),
            image: "registry/test@sha256:fixture".into(),
            ..OverlaybdConfig::default()
        };
        let mut devices = Devices::open(config.clone()).unwrap();
        let deadline = boot_time_ms().unwrap() + 5000;
        devices.release("released-runtime").await.unwrap();
        drop(devices);
        let mut devices = Devices::open(config.clone()).unwrap();
        let error = devices
            .acquire("released-runtime".into(), &config.image, deadline)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("already released"), "{error}");
        assert!(!temp.path().join("state.json").exists());
    }

    #[tokio::test]
    async fn persisted_reference_prevents_reuse_after_restart() {
        let directory = tempfile::tempdir().unwrap();
        let config = OverlaybdConfig {
            enabled: true,
            recover_on_restart: true,
            directory: directory.path().to_owned(),
            image: "example/overlaybd:fixture".to_owned(),
        };
        let mut original = Devices::open(config.clone()).unwrap();
        original
            .persist(Journal {
                device_id: 123,
                character: CharacterIdentity {
                    boot: "test".into(),
                    filesystem: 1,
                    inode: 2,
                },
                refs: BTreeSet::from(["old-runtime".to_owned()]),
                recovery: None,
            })
            .unwrap();
        let mut restarted = Devices::open(config.clone()).unwrap();
        assert!(
            restarted
                .acquire(
                    "new-runtime".into(),
                    &config.image,
                    boot_time_ms().unwrap() + 5000
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("no active device")
        );
        assert_eq!(
            restarted.journal.unwrap().refs,
            BTreeSet::from(["old-runtime".into()])
        );
    }
}
