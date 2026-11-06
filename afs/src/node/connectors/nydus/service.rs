//! Image preparation uses file operations; body fill and ownership stay in DFS.
use std::{
    ffi::OsStr,
    sync::Arc,
    time::{Duration, Instant},
};

use super::{
    NydusConfig, lazy_fill,
    registry::{self, ImagePlan, RegistryProvider},
};
use crate::node::{
    connectors::ConnectorContext,
    vfs::{
        Backend, PreparedPublication,
        lazy_fill::LazyFileSpec,
        types::{BackendInode, Entry, FileHandle, RequestContext, SyncMode},
    },
};
use afs_error::{Error, ErrorKind, Result};
use prost::bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

const IO_BATCH: usize = 256 * 1024;
const PLAN_LIMIT: usize = 4 * 1024 * 1024;

pub struct NydusService {
    backend: Arc<dyn Backend>,
    root: BackendInode,
    config: NydusConfig,
    provider: Arc<RegistryProvider>,
    ctx: RequestContext,
    requests: Arc<Semaphore>,
}

pub struct BlobStat {
    pub entry: Entry,
    pub size: u64,
}

impl NydusService {
    pub fn new(
        context: ConnectorContext,
        config: NydusConfig,
        provider: Arc<RegistryProvider>,
    ) -> Self {
        Self {
            backend: context.backend,
            root: context.root,
            provider,
            ctx: RequestContext {
                uid: config.uid,
                gid: config.gid,
                pid: 0,
                umask: 0o027,
                supplementary_gids: Vec::new(),
            },
            requests: Arc::new(Semaphore::new(config.max_requests)),
            config,
        }
    }
    pub async fn stop(&self) {
        self.requests.close();
        while self.requests.available_permits() != self.config.max_requests {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    pub fn deadline(&self) -> Instant {
        Instant::now() + Duration::from_millis(self.config.timeout_ms)
    }
    pub async fn admit(&self, deadline: Instant) -> Result<Arc<OwnedSemaphorePermit>> {
        let permit = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.requests.clone().acquire_owned(),
        )
        .await
        .map_err(|_| unavailable("Nydus request admission expired"))?
        .map_err(|_| unavailable("Nydus request admission closed"))?;
        Ok(Arc::new(permit))
    }

    pub async fn stat_blob(
        self: &Arc<Self>,
        key: String,
        manifest: String,
        blob: String,
        deadline: Instant,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<BlobStat> {
        registry::validate_component(&key)?;
        let manifest = registry::digest(&manifest)?;
        let blob = registry::digest(&blob)?;
        let plan = self
            .ensure_prepared(key.clone(), manifest.clone(), deadline, permit.clone())
            .await?;
        let descriptor = plan
            .blobs
            .iter()
            .find(|b| b.blob_digest == blob)
            .ok_or_else(|| {
                Error::coded(afs_error::IO_NOT_FOUND, "blob is not part of this image")
            })?;
        let size = descriptor.size;
        let this = self.clone();
        blocking(move || {
            let _permit = permit;
            check_deadline(deadline)?;
            let parent = this.image_parent(&key, false)?;
            let dir = this
                .backend
                .lookup(&this.ctx, parent, OsStr::new(&manifest[7..]))?;
            let blobs = this
                .backend
                .lookup(&this.ctx, dir.inode, OsStr::new("blobs"))?;
            let entry = this
                .backend
                .lookup(&this.ctx, blobs.inode, OsStr::new(&blob[7..]))?;
            if entry.attributes.size != size {
                return Err(invalid("prepared blob length differs from manifest"));
            }
            Ok(BlobStat { entry, size })
        })
        .await
    }

    async fn ensure_prepared(
        self: &Arc<Self>,
        key: String,
        manifest: String,
        deadline: Instant,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<ImagePlan> {
        let this = self.clone();
        let k = key.clone();
        let m = manifest.clone();
        let first_permit = permit.clone();
        if let Some(plan) = blocking(move || {
            let _permit = first_permit;
            check_deadline(deadline)?;
            this.read_plan(&k, &m)
        })
        .await?
        {
            self.provider.validate_plan(&plan, &key, &manifest)?;
            return Ok(plan);
        }
        let plan = self.provider.describe(&key, &manifest, deadline).await?;
        let this = self.clone();
        blocking(move || {
            let _permit = permit;
            check_deadline(deadline)?;
            if let Some(winner) = this.read_plan(&plan.source_key, &plan.manifest_digest)? {
                if winner != plan {
                    return Err(invalid("concurrent image preparation conflicts"));
                }
                return Ok(winner);
            }
            this.publish_plan(plan, deadline)
        })
        .await
    }

    fn image_parent(&self, key: &str, create: bool) -> Result<BackendInode> {
        let mut parent = self.root;
        for name in [&self.config.root_name, "images", key] {
            let entry = if create {
                self.mkdir(parent, name)?
            } else {
                self.backend.lookup(&self.ctx, parent, OsStr::new(name))?
            };
            parent = entry.inode;
        }
        Ok(parent)
    }
    fn mkdir(&self, parent: BackendInode, name: &str) -> Result<Entry> {
        match self.backend.lookup(&self.ctx, parent, OsStr::new(name)) {
            Ok(entry) => return Ok(entry),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        match self
            .backend
            .mkdir(&self.ctx, parent, OsStr::new(name), 0o750)
        {
            Ok(entry) => Ok(entry),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                self.backend.lookup(&self.ctx, parent, OsStr::new(name))
            }
            Err(error) => Err(error),
        }
    }
    fn read_plan(&self, key: &str, manifest: &str) -> Result<Option<ImagePlan>> {
        let dir = match self.image_parent(key, false).and_then(|parent| {
            self.backend
                .lookup(&self.ctx, parent, OsStr::new(&manifest[7..]))
        }) {
            Ok(entry) => entry,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let entry = self
            .backend
            .lookup(&self.ctx, dir.inode, OsStr::new("descriptor.json"))?;
        if entry.attributes.size > PLAN_LIMIT as u64 {
            return Err(invalid("prepared descriptor is oversized"));
        }
        let handle = self.backend.open(&self.ctx, entry.inode, libc::O_RDONLY)?;
        let result = (|| {
            let mut body = Vec::new();
            while body.len() < (entry.attributes.size as usize) {
                let count = (entry.attributes.size as usize - body.len()).min(IO_BATCH);
                let part = self
                    .backend
                    .read_reply(&self.ctx, handle, body.len() as u64, count)?;
                if part.len() != count {
                    return Err(invalid("incomplete image descriptor"));
                }
                body.extend_from_slice(&part);
            }
            serde_json::from_slice(&body)
                .map(Some)
                .map_err(|_| invalid("invalid prepared image descriptor"))
        })();
        let closed = self.backend.release(&self.ctx, handle);
        let plan = result?;
        closed?;
        Ok(plan)
    }

    fn publish_plan(&self, plan: ImagePlan, deadline: Instant) -> Result<ImagePlan> {
        check_deadline(deadline)?;
        let parent = self.image_parent(&plan.source_key, true)?;
        let staging = format!(".preparing-{}", uuid::Uuid::new_v4());
        let dir = self
            .backend
            .mkdir(&self.ctx, parent, OsStr::new(&staging), 0o750)?;
        let mut blobs = None;
        let mut names = Vec::new();
        let mut descriptor_created = false;
        let mut publication_attempted = false;
        let result = (|| {
            let directory = self
                .backend
                .mkdir(&self.ctx, dir.inode, OsStr::new("blobs"), 0o750)?;
            blobs = Some(directory.inode);
            for blob in &plan.blobs {
                check_deadline(deadline)?;
                let name = &blob.blob_digest[7..];
                self.backend.prepare_lazy_file(
                    &self.ctx,
                    directory.inode,
                    OsStr::new(name),
                    &LazyFileSpec {
                        length: blob.size,
                        binding: lazy_fill::binding(blob)?,
                    },
                )?;
                names.push(name.to_string());
            }
            let body = serde_json::to_vec(&plan)
                .map_err(|_| invalid("cannot serialize image descriptor"))?;
            if body.len() > PLAN_LIMIT {
                return Err(invalid("image descriptor is oversized"));
            }
            let file = self.backend.create(
                &self.ctx,
                dir.inode,
                OsStr::new("descriptor.json"),
                0o440,
                libc::O_RDWR | libc::O_EXCL,
            )?;
            descriptor_created = true;
            let write = (|| {
                for (index, part) in body.chunks(IO_BATCH).enumerate() {
                    if self.backend.write(
                        &self.ctx,
                        file.handle,
                        (index * IO_BATCH) as u64,
                        part,
                    )? != part.len()
                    {
                        return Err(unavailable("short image descriptor write"));
                    }
                }
                self.backend.fsync(&self.ctx, file.handle, SyncMode::Full)
            })();
            let closed = self.backend.release(&self.ctx, file.handle);
            write?;
            closed?;
            check_deadline(deadline)?;
            publication_attempted = true;
            self.backend.publish_prepared(
                &self.ctx,
                parent,
                OsStr::new(&staging),
                OsStr::new(&plan.manifest_digest[7..]),
                dir.inode,
            )
        })();
        if (!publication_attempted && result.is_err())
            || matches!(result, Ok(PreparedPublication::Occupied))
        {
            // Never delete by child inode after an indeterminate publication:
            // those same inodes may now belong to the public image tree.
            if let Some(blobs) = blobs {
                for name in names {
                    self.backend.unlink(&self.ctx, blobs, OsStr::new(&name))?;
                }
                self.backend
                    .rmdir(&self.ctx, dir.inode, OsStr::new("blobs"))?;
            }
            if descriptor_created {
                self.backend
                    .unlink(&self.ctx, dir.inode, OsStr::new("descriptor.json"))?;
            }
            self.backend
                .rmdir(&self.ctx, parent, OsStr::new(&staging))?;
        }
        match result {
            Ok(PreparedPublication::Published) => Ok(plan),
            Ok(PreparedPublication::Occupied) => {
                let winner = self
                    .read_plan(&plan.source_key, &plan.manifest_digest)?
                    .ok_or_else(|| unavailable("published image disappeared"))?;
                if winner != plan {
                    return Err(invalid("concurrent image preparation conflicts"));
                }
                Ok(winner)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn read_blob(
        self: &Arc<Self>,
        entry: Entry,
        offset: u64,
        length: u64,
        deadline: Instant,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<mpsc::Receiver<Result<Bytes>>> {
        let (tx, rx) = mpsc::channel(2);
        let this = self.clone();
        tokio::spawn(async move {
            let _permit = permit.clone();
            let owner = this.clone();
            let handle = match blocking(move || {
                check_deadline(deadline)?;
                owner.backend.open(&owner.ctx, entry.inode, libc::O_RDONLY)
            })
            .await
            {
                Ok(handle) => handle,
                Err(error) => {
                    let _ = tx.try_send(Err(error));
                    return;
                }
            };
            let result = this
                .produce(handle, offset, length, deadline, permit, &tx)
                .await;
            if let Err(error) = result {
                let _ = tx.try_send(Err(error));
            }
            let backend = this.backend.clone();
            let ctx = this.ctx.clone();
            if let Err(error) = blocking(move || backend.release(&ctx, handle)).await {
                let _ = tx.try_send(Err(error));
            }
        });
        Ok(rx)
    }
    async fn produce(
        &self,
        handle: FileHandle,
        mut offset: u64,
        mut remaining: u64,
        deadline: Instant,
        permit: Arc<OwnedSemaphorePermit>,
        tx: &mpsc::Sender<Result<Bytes>>,
    ) -> Result<()> {
        while remaining > 0 {
            check_deadline(deadline)?;
            if tx.is_closed() {
                break;
            }
            let count = remaining.min(IO_BATCH as u64) as usize;
            let lifetime: crate::node::read_pipeline::ReadLifetime = permit.clone();
            let piece = match self.backend.clone().read_reply_async(
                self.ctx.clone(),
                handle,
                offset,
                count,
                deadline,
                lifetime,
            )? {
                Some(future) => future.await?,
                None => {
                    let backend = self.backend.clone();
                    let ctx = self.ctx.clone();
                    blocking(move || backend.read_reply(&ctx, handle, offset, count)).await?
                }
            };
            if piece.len() != count {
                return Err(unavailable("short AFS blob read"));
            }
            if tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), tx.send(Ok(piece)))
                .await
                .map_err(|_| unavailable("HTTP body consumer expired"))?
                .is_err()
            {
                break;
            }
            offset += count as u64;
            remaining -= count as u64;
        }
        Ok(())
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| unavailable("Nydus file operation worker failed"))?
}
fn check_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(unavailable("Nydus request expired"))
    } else {
        Ok(())
    }
}
fn invalid(message: &str) -> Error {
    Error::coded(afs_error::NODE_VFS_INVALID, message)
}
fn unavailable(message: &str) -> Error {
    Error::coded(afs_error::NODE_VFS_UNAVAILABLE, message)
}

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
