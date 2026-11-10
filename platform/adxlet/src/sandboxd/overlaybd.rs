use adx_core::{Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{fs, io::AsyncWriteExt, sync::Mutex};

use super::{overlaybd_request, unavailable};

pub(super) struct OverlaybdConnector {
    client: reqwest::Client,
    journal: PathBuf,
    releases: Mutex<()>,
}

#[derive(Serialize, Deserialize)]
struct Reference {
    runtime_id: String,
    release_pending: bool,
}

impl OverlaybdConnector {
    pub(super) fn new(socket: PathBuf, rpc_timeout: Duration) -> Result<Arc<Self>> {
        let parent = socket
            .parent()
            .filter(|_| socket.is_absolute())
            .ok_or_else(|| {
                Error::Invalid("ADX_OVERLAYBD_ROOTFS_SOCKET must be an absolute socket path".into())
            })?;
        let journal = parent.join(".adxlet-overlaybd-refs");
        adx_transport::install_crypto_provider();
        let client = reqwest::Client::builder()
            .unix_socket(socket)
            .timeout(rpc_timeout)
            .build()
            .map_err(unavailable)?;
        Ok(Arc::new(Self {
            client,
            journal,
            releases: Mutex::new(()),
        }))
    }

    pub(super) fn start_replay(self: &Arc<Self>) {
        let connector = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                {
                    let Some(connector) = connector.upgrade() else {
                        break;
                    };
                    if let Err(error) = connector.replay_pending().await {
                        adx_observability::warn!(%error, "OverlayBD release replay deferred");
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    pub(super) async fn acquire(&self, runtime_id: &str, image: &str) -> Result<Option<PathBuf>> {
        let deadline = acquire_deadline_ms()?;
        let path = self.record_path(runtime_id);
        let guard = self.releases.lock().await;
        match Self::read(&path).await? {
            Some(record) if record.release_pending => {
                return Err(unavailable("OverlayBD reference is still being released"));
            }
            Some(_) => {}
            None => {
                self.save(
                    &path,
                    &Reference {
                        runtime_id: runtime_id.to_owned(),
                        release_pending: false,
                    },
                )
                .await?
            }
        }
        drop(guard);
        // Record the intent before sending. A lost reply can still leave a
        // reference on the connector, so keep the record on ambiguous errors.
        let response = overlaybd_request(
            &self.client,
            "acquire",
            runtime_id,
            Some(image),
            Some(deadline),
        )
        .await?;
        let bytes = response.bytes().await.map_err(unavailable)?;
        let rootfs = serde_json::from_slice(&bytes).map_err(unavailable)?;
        if Option::<PathBuf>::is_none(&rootfs) {
            let _guard = self.releases.lock().await;
            if Self::read(&path)
                .await?
                .is_some_and(|reference| !reference.release_pending)
            {
                self.clear(&path).await?;
            }
        }
        Ok(rootfs)
    }

    pub(super) async fn release_after_delete(&self, runtime_id: &str) -> Result<()> {
        let path = self.record_path(runtime_id);
        let guard = self.releases.lock().await;
        let Some(mut reference) = Self::read(&path).await? else {
            return Ok(());
        };
        if !reference.release_pending {
            reference.release_pending = true;
            self.save(&path, &reference).await?;
        }
        drop(guard);
        // The durable record makes cleanup independent of connector uptime.
        // Keep the normal fast path when the connector is already reachable.
        if let Ok(Err(error)) =
            tokio::time::timeout(Duration::from_secs(1), self.replay_one(&path)).await
        {
            adx_observability::warn!(%error, runtime_id, "OverlayBD release queued for replay");
        }
        Ok(())
    }

    async fn replay_pending(&self) -> Result<()> {
        let mut entries = match fs::read_dir(&self.journal).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(unavailable(error)),
        };
        while let Some(entry) = entries.next_entry().await.map_err(unavailable)? {
            if entry.path().extension().and_then(|value| value.to_str()) == Some("json") {
                self.replay_one(&entry.path()).await?;
            }
        }
        Ok(())
    }

    async fn replay_one(&self, path: &Path) -> Result<()> {
        let _guard = self.releases.lock().await;
        let Some(reference) = Self::read(path).await? else {
            return Ok(());
        };
        if reference.release_pending {
            overlaybd_request(&self.client, "release", &reference.runtime_id, None, None).await?;
            self.clear(path).await?;
        }
        Ok(())
    }

    fn record_path(&self, runtime_id: &str) -> PathBuf {
        self.journal
            .join(format!("{:x}.json", Sha256::digest(runtime_id.as_bytes())))
    }

    async fn read(path: &Path) -> Result<Option<Reference>> {
        match fs::read(path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(unavailable),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(unavailable(error)),
        }
    }

    async fn save(&self, path: &Path, reference: &Reference) -> Result<()> {
        match fs::create_dir(&self.journal).await {
            Ok(()) => {
                let parent = self
                    .journal
                    .parent()
                    .ok_or_else(|| unavailable("invalid OverlayBD journal path"))?;
                Self::sync_dir(parent).await?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(unavailable(error)),
        }
        let temp = path.with_extension("tmp");
        let mut file = fs::File::create(&temp).await.map_err(unavailable)?;
        let bytes = serde_json::to_vec(reference).map_err(unavailable)?;
        file.write_all(&bytes).await.map_err(unavailable)?;
        file.sync_all().await.map_err(unavailable)?;
        fs::rename(temp, path).await.map_err(unavailable)?;
        Self::sync_dir(&self.journal).await
    }

    async fn clear(&self, path: &Path) -> Result<()> {
        match fs::remove_file(path).await {
            Ok(()) => Self::sync_dir(&self.journal).await,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(unavailable(error)),
        }
    }

    async fn sync_dir(path: &Path) -> Result<()> {
        fs::File::open(path)
            .await
            .map_err(unavailable)?
            .sync_all()
            .await
            .map_err(unavailable)
    }
}

#[cfg(target_os = "linux")]
fn acquire_deadline_ms() -> Result<u64> {
    const MAX_AGE_MS: u64 = 120_000;
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes the writable timespec on success.
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, time.as_mut_ptr()) } != 0 {
        return Err(unavailable(io::Error::last_os_error()));
    }
    // SAFETY: clock_gettime returned success and initialized the timespec.
    let time = unsafe { time.assume_init() };
    Ok((time.tv_sec as u64).saturating_mul(1000) + time.tv_nsec as u64 / 1_000_000 + MAX_AGE_MS)
}

#[cfg(not(target_os = "linux"))]
fn acquire_deadline_ms() -> Result<u64> {
    Err(unavailable("OverlayBD control requires Linux"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn offline_release_survives_adxlet_restart_without_enrolling_unrelated_sandbox() {
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("offline.sock");
        let connector = OverlaybdConnector::new(socket.clone(), Duration::from_secs(1)).unwrap();
        connector.release_after_delete("unrelated").await.unwrap();
        assert!(!connector.journal.exists());
        assert!(connector.acquire("managed", "image").await.is_err());
        connector.release_after_delete("managed").await.unwrap();
        drop(connector);
        let restarted = OverlaybdConnector::new(socket, Duration::from_secs(1)).unwrap();
        let pending = OverlaybdConnector::read(&restarted.record_path("managed"))
            .await
            .unwrap()
            .unwrap();
        assert!(pending.release_pending);
        assert_eq!(pending.runtime_id, "managed");
        assert!(restarted.replay_pending().await.is_err());
        assert!(restarted.record_path("managed").exists());
    }
}
