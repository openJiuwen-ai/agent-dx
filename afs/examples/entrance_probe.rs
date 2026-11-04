//! Diagnostic only. Same Backend behind unmodified AFS FUSE and USRBIO frontends.
use afs::node::{
    api::local::{LocalApiOptions, serve_local_api_with_options},
    fuse::{FuseMountOptions, mount_dfs_with_fd_bridge},
    storage::Storage,
    vfs::{Backend, types::*},
};
use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig, DfsSyncMode};
use serde_json::json;
use std::{
    ffi::OsStr,
    fs::{self, OpenOptions},
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime},
};
#[allow(dead_code)]
#[path = "entrance_probe/native.rs"]
mod native;
type Any = Box<dyn std::error::Error + Send + Sync>;
const SHARD: usize = 4 << 20;
fn payload(n: usize) -> Vec<u8> {
    let mut v = vec![0; n];
    let mut x = 0x5eedd24c15812345u64;
    for c in v.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        c.copy_from_slice(&x.to_le_bytes()[..c.len()]);
    }
    v
}
struct Mem {
    shards: Vec<RwLock<Vec<u8>>>,
    n: usize,
    null: bool,
    reads: AtomicU64,
    writes: AtomicU64,
    bytes: AtomicU64,
    ns: AtomicU64,
}
impl Mem {
    fn new(n: usize, null: bool, write: bool) -> Self {
        let v = if null {
            vec![]
        } else if write {
            vec![0; n]
        } else {
            payload(n)
        };
        Self {
            shards: v.chunks(SHARD).map(|s| RwLock::new(s.to_vec())).collect(),
            n,
            null,
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            ns: AtomicU64::new(0),
        }
    }
    fn attrs(&self, root: bool) -> FileAttributes {
        FileAttributes {
            kind: if root {
                FileKind::Directory
            } else {
                FileKind::Regular
            },
            size: if root { 0 } else { self.n as u64 },
            blocks: (self.n / 512) as u64,
            mode: 0o777,
            // SAFETY: identity queries take no pointers and do not mutate process memory.
            uid: unsafe { libc::getuid() },
            // SAFETY: identity query takes no pointers and does not mutate memory.
            gid: unsafe { libc::getgid() },
            nlink: 1,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
        }
    }
    fn stats(&self) -> serde_json::Value {
        json!({"reads":self.reads.load(Ordering::Relaxed),"writes":self.writes.load(Ordering::Relaxed),
 "bytes":self.bytes.load(Ordering::Relaxed),"backend_service_ns":self.ns.load(Ordering::Relaxed),
 "hash":if self.null{String::new()}else{let mut h=blake3::Hasher::new();for s in &self.shards{h.update(&s.read().unwrap());}h.finalize().to_hex().to_string()}})
    }
}
impl Backend for Mem {
    fn root_inode(&self) -> BackendInode {
        BackendInode { value: 1 }
    }
    // Fixture attrs have no suid/sgid bits, so this has no privilege side effect to emulate.
    fn write_with_options(
        &self,
        c: &RequestContext,
        h: FileHandle,
        o: u64,
        d: &[u8],
        _: WriteOptions,
    ) -> afs_error::Result<usize> {
        self.write(c, h, o, d)
    }

    fn supports_parallel_reads(&self, _: FileHandle) -> bool {
        true
    }
    fn lookup(
        &self,
        _: &RequestContext,
        p: BackendInode,
        name: &OsStr,
    ) -> afs_error::Result<Entry> {
        if p.value != 1 || name != OsStr::new("data.bin") {
            return Err(afs_error::Error::coded(
                afs_error::NODE_VFS_NOT_FOUND,
                "absent",
            ));
        }
        Ok(Entry {
            inode: BackendInode { value: 2 },
            attributes: self.attrs(false),
        })
    }
    fn getattr(
        &self,
        _: &RequestContext,
        i: BackendInode,
        _: Option<FileHandle>,
    ) -> afs_error::Result<FileAttributes> {
        Ok(self.attrs(i.value == 1))
    }
    fn open(&self, _: &RequestContext, _: BackendInode, _: i32) -> afs_error::Result<FileHandle> {
        Ok(FileHandle(1))
    }
    fn flush(&self, _: &RequestContext, _: FileHandle) -> afs_error::Result<()> {
        Ok(())
    }
    fn fsync(&self, _: &RequestContext, _: FileHandle, _: SyncMode) -> afs_error::Result<()> {
        Ok(())
    }
    fn release(&self, _: &RequestContext, _: FileHandle) -> afs_error::Result<()> {
        Ok(())
    }
    fn write(
        &self,
        _: &RequestContext,
        _: FileHandle,
        off: u64,
        data: &[u8],
    ) -> afs_error::Result<usize> {
        let t = Instant::now();
        let off = off as usize;
        assert!(off + data.len() <= self.n);
        if !self.null {
            for (j, b) in data.chunks(SHARD - (off % SHARD)).enumerate() {
                // Calls in this fixture are aligned and <=SHARD; keep assertion explicit.
                assert_eq!(j, 0);
                self.shards[off / SHARD].write().unwrap()[off % SHARD..off % SHARD + b.len()]
                    .copy_from_slice(b);
            }
        }
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.ns
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        Ok(data.len())
    }
    fn read(
        &self,
        _: &RequestContext,
        _: FileHandle,
        off: u64,
        out: &mut [u8],
    ) -> afs_error::Result<usize> {
        let t = Instant::now();
        let off = off as usize;
        let n = out.len().min(self.n.saturating_sub(off));
        if !self.null && n > 0 {
            assert!(off % SHARD + n <= SHARD);
            out[..n].copy_from_slice(
                &self.shards[off / SHARD].read().unwrap()[off % SHARD..off % SHARD + n],
            );
        }
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(n as u64, Ordering::Relaxed);
        self.ns
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        Ok(n)
    }
}
fn ctx() -> RequestContext {
    RequestContext {
        uid: 0,
        gid: 0,
        pid: 0,
        umask: 0,
        supplementary_gids: vec![],
    }
}
struct Buf {
    p: *mut u8,
    n: usize,
}
impl Buf {
    fn new(n: usize) -> Self {
        let mut p = std::ptr::null_mut();
        // SAFETY: writable pointer slot; valid power-of-two alignment and nonzero size.
        assert_eq!(unsafe { libc::posix_memalign(&mut p, 4096, n) }, 0);
        // SAFETY: posix_memalign returned n exclusive bytes. Prefault them.
        unsafe { std::ptr::write_bytes(p, 0, n) };
        Self { p: p.cast(), n }
    }
    // SAFETY: self exclusively owns an initialized allocation of n bytes.
    fn bytes(&self) -> &[u8] {
        // SAFETY: self owns this initialized allocation for the returned borrow.
        unsafe { std::slice::from_raw_parts(self.p, self.n) }
    }
    // SAFETY: exclusive self borrow prevents aliases into this live allocation.
    fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: exclusive self borrow prevents aliases to this allocation.
        unsafe { std::slice::from_raw_parts_mut(self.p, self.n) }
    }
}
impl Drop for Buf {
    fn drop(&mut self) {
        // SAFETY: this pointer came from posix_memalign and has not been freed.
        unsafe { libc::free(self.p.cast()) }
    }
}
fn cpu() -> f64 {
    let mut r = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: valid writable rusage storage; successful getrusage initializes it.
    unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, r.as_mut_ptr()), 0);
        let r = r.assume_init();
        r.ru_utime.tv_sec as f64
            + r.ru_utime.tv_usec as f64 / 1e6
            + r.ru_stime.tv_sec as f64
            + r.ru_stime.tv_usec as f64 / 1e6
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Any> {
    let a: Vec<String> = std::env::args().collect();
    let mode = &a[1];
    let root = Path::new(&a[2]);
    let n: usize = a[3].parse()?;
    if mode == "server" {
        fs::create_dir_all(root.join("mnt"))?;
        let null = a[4] == "null";
        let b = Arc::new(Mem::new(n, null, a[5] == "write"));
        let server = serve_local_api_with_options(
            Storage::new(root.join("diag"))?,
            root.join("node.sock"),
            LocalApiOptions {
                metrics_registry: None,
                dfs_backend: Some(b.clone()),
                dfs_namespace: Some("default".into()),
            },
        )
        .await?;
        let mount = mount_dfs_with_fd_bridge(
            b.clone(),
            &root.join("mnt"),
            FuseMountOptions::default(),
            server.fd_bridge().ok_or("missing fd bridge")?,
        )?;
        println!("{}", json!({"ready":true,"pid":std::process::id()}));
        while !root.join("stop").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        mount.join()?;
        server.shutdown().await?;
        println!("{}", b.stats());
        return Ok(());
    }
    let op = &a[4];
    let block: usize = a[5].parse()?;
    let q: usize = a[6].parse()?;
    let null = a[7] == "null";
    assert!(n.is_multiple_of(block) && block <= SHARD && SHARD.is_multiple_of(block) && q > 0);
    let mut input_buf = Buf::new(n);
    input_buf.bytes_mut().copy_from_slice(&payload(n));
    let input = input_buf.bytes();
    let expected = blake3::hash(input).to_hex().to_string();
    let mut output_buf = Buf::new(n);
    let output = output_buf.bytes_mut();
    let write = op == "write";
    let setup = Instant::now();
    let c0 = cpu();
    let t: Instant;
    let io_s: f64;
    let sync_s: f64;
    if mode == "sdk_multi" {
        let count = (n / block).div_ceil(q) * block;
        let workers = output.chunks_mut(count).count();
        let start_gate = Arc::new(std::sync::Barrier::new(workers + 1));
        let done_gate = Arc::new(std::sync::Barrier::new(workers + 1));
        let (setup_s, measured) = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for (worker, dst) in output.chunks_mut(count).enumerate() {
                let begin = start_gate.clone();
                let done = done_gate.clone();
                let src = input;
                handles.push(scope.spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    rt.block_on(async move {
                        let client = DfsLocalClient::connect(DfsLocalClientConfig::new(
                            root.join("node.sock"),
                        ))
                        .await
                        .unwrap();
                        let file = client
                            .open(
                                "/data.bin",
                                if write {
                                    DfsAccess::ReadWrite
                                } else {
                                    DfsAccess::ReadOnly
                                },
                            )
                            .await
                            .unwrap();
                        let mut iov = client.create_iov(block, 1).await.unwrap();
                        let mut ring = client.create_ring(&iov, 128, 0, 30000).await.unwrap();
                        let reg = client.register_file(&file).await.unwrap();
                        begin.wait();
                        let base = worker * count;
                        for (j, out) in dst.chunks_mut(block).enumerate() {
                            let off = base + j * block;
                            if write {
                                assert_eq!(
                                    ring.write_from(
                                        &mut iov,
                                        &reg,
                                        off as u64,
                                        &src[off..off + block]
                                    )
                                    .await
                                    .unwrap(),
                                    block
                                );
                            } else {
                                assert_eq!(
                                    ring.read_into(&mut iov, &reg, off as u64, out)
                                        .await
                                        .unwrap(),
                                    block
                                );
                            }
                        }
                        done.wait();
                        client.unregister_file(reg).await.unwrap();
                        client.close(file.handle).await.unwrap();
                        ring.destroy().await.unwrap();
                        iov.destroy().await.unwrap();
                    });
                }));
            }
            let setup_s = setup.elapsed().as_secs_f64();
            let start = Instant::now();
            start_gate.wait();
            done_gate.wait();
            let measured = start.elapsed().as_secs_f64();
            for h in handles {
                h.join().unwrap();
            }
            (setup_s, measured)
        });
        io_s = measured;
        sync_s = 0.;
        assert!(!write, "multi-ring probe only qualifies immutable reads");
        println!(
            "{}",
            json!({"setup_s":setup_s,"io_s":io_s,"sync_s":sync_s,"total_s":setup_s+io_s,"cpu_s":cpu()-c0,"rings":workers})
        );
    } else if mode == "sdk" || mode == "sdk_fd" {
        let client =
            DfsLocalClient::connect(DfsLocalClientConfig::new(root.join("node.sock"))).await?;
        use std::os::fd::AsFd;
        let posix = if mode == "sdk_fd" {
            Some(
                OpenOptions::new()
                    .read(true)
                    .write(write)
                    .open(root.join("mnt/data.bin"))?,
            )
        } else {
            None
        };
        let file = if posix.is_none() {
            Some(
                client
                    .open(
                        "/data.bin",
                        if write {
                            DfsAccess::ReadWrite
                        } else {
                            DfsAccess::ReadOnly
                        },
                    )
                    .await?,
            )
        } else {
            None
        };
        let mut iov = client.create_iov(block, q as u32).await?;
        let mut ring = client.create_ring(&iov, 128, 0, 30000).await?;
        let mut imported = if let Some(posix) = &posix {
            Some(client.register_fd(posix.as_fd()).await?)
        } else {
            None
        };
        let reg = if let Some(imported) = &imported {
            imported.registration().clone()
        } else {
            client
                .register_file(file.as_ref().ok_or("missing path handle")?)
                .await?
        };
        let setup_s = setup.elapsed().as_secs_f64();
        let passes: usize = a
            .get(8)
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(1);
        assert!((1..=3).contains(&passes));
        let mut pass_io_s = Vec::new();
        for _ in 0..passes {
            let pass_start = Instant::now();
            for base in (0..n).step_by(block * q) {
                let end = (base + block * q).min(n);
                if write {
                    let items: Vec<_> = input[base..end]
                        .chunks(block)
                        .enumerate()
                        .map(|(j, s)| ((base + j * block) as u64, s))
                        .collect();
                    let sizes = ring.write_batch(&mut iov, &reg, &items).await?;
                    assert!(sizes.iter().all(|s| *s == block));
                } else {
                    let mut items: Vec<_> = output[base..end]
                        .chunks_mut(block)
                        .enumerate()
                        .map(|(j, s)| ((base + j * block) as u64, s))
                        .collect();
                    let sizes = ring.read_batch(&mut iov, &reg, &mut items).await?;
                    assert!(sizes.iter().all(|s| *s == block));
                }
            }
            pass_io_s.push(pass_start.elapsed().as_secs_f64());
            if !write && !null {
                assert_eq!(&*output, input, "FULL BYTE MISMATCH");
            }
        }
        let first_io_s = pass_io_s[0];
        let steady_io_s = if passes > 1 {
            pass_io_s[1..].iter().sum::<f64>() / (passes - 1) as f64
        } else {
            first_io_s
        };
        io_s = steady_io_s;
        let all_io_s = pass_io_s.iter().sum::<f64>();
        let s = Instant::now();
        if write {
            if let Some(imported) = &imported {
                imported.sync_all().await?;
            } else {
                client
                    .sync(
                        &file.as_ref().ok_or("missing handle")?.handle,
                        DfsSyncMode::Full,
                    )
                    .await?;
            }
        }
        sync_s = s.elapsed().as_secs_f64();
        let cleanup_start = Instant::now();
        if let Some(imported) = &mut imported {
            client.unregister_fd(imported).await?;
        } else {
            client.unregister_file(reg).await?;
            client.close(file.ok_or("missing handle")?.handle).await?;
        }
        ring.destroy().await?;
        iov.destroy().await?;
        drop(posix);
        let cleanup_s = cleanup_start.elapsed().as_secs_f64();
        println!(
            "{}",
            json!({"setup_s":setup_s,"io_s":io_s,"sync_s":sync_s,"cleanup_s":cleanup_s,"total_s":setup.elapsed().as_secs_f64(),"io_total_s":all_io_s,"first_io_s":first_io_s,"steady_io_s":steady_io_s,"pass_io_s":pass_io_s,"passes":passes,"cpu_s":cpu()-c0})
        );
    } else if mode == "native" {
        let client = native::DfsLocalClient::connect(native::DfsLocalClientConfig::new(
            root.join("node.sock"),
        ))
        .await?;
        let file = client
            .open(
                "/data.bin",
                if write {
                    native::DfsAccess::ReadWrite
                } else {
                    native::DfsAccess::ReadOnly
                },
            )
            .await?;
        let mut iov = client.create_iov(block, q as u32).await?;
        let mut ring = client.create_ring(&iov, 128, 0, 30000).await?;
        let reg = client.register_file(&file).await?;
        let setup_s = setup.elapsed().as_secs_f64();
        t = Instant::now();
        ring.probe_transfer(&mut iov, &reg, input, output, (block, q, write))
            .await?;
        io_s = t.elapsed().as_secs_f64();
        let s = Instant::now();
        if write {
            client.sync(&file.handle, native::DfsSyncMode::Full).await?;
        }
        sync_s = s.elapsed().as_secs_f64();
        println!(
            "{}",
            json!({"setup_s":setup_s,"io_s":io_s,"sync_s":sync_s,"total_s":setup_s+io_s+sync_s,"cpu_s":cpu()-c0,"free_blocks":iov.free_blocks()})
        );
        client.unregister_file(reg).await?;
        client.close(file.handle).await?;
        ring.destroy().await?;
        iov.destroy().await?;
    } else if mode == "direct" {
        let b = Mem::new(n, null, write);
        let setup_s = setup.elapsed().as_secs_f64();
        t = Instant::now();
        if write {
            for (i, s) in input.chunks(block).enumerate() {
                b.write(&ctx(), FileHandle(1), (i * block) as u64, s)?;
            }
        } else {
            for (i, s) in output.chunks_mut(block).enumerate() {
                b.read(&ctx(), FileHandle(1), (i * block) as u64, s)?;
            }
        }
        io_s = t.elapsed().as_secs_f64();
        sync_s = 0.;
        println!(
            "{}",
            json!({"setup_s":setup_s,"io_s":io_s,"sync_s":sync_s,"total_s":setup_s+io_s,"stats":b.stats(),"cpu_s":cpu()-c0})
        );
    } else {
        let file = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(write)
                .custom_flags(if mode == "fuse_direct" {
                    libc::O_DIRECT
                } else {
                    0
                })
                .open(root.join("mnt/data.bin"))?,
        );
        let chunk = (n / block).div_ceil(q) * block;
        let gate = Arc::new(std::sync::Barrier::new(
            output.chunks_mut(chunk).count() + 1,
        ));
        let (setup_s, measured) = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for (worker, dst) in output.chunks_mut(chunk).enumerate() {
                let f = file.clone();
                let gate = gate.clone();
                let src = input;
                handles.push(scope.spawn(move || {
                    gate.wait();
                    let base = worker * chunk;
                    for (j, out) in dst.chunks_mut(block).enumerate() {
                        let off = base + j * block;
                        if write {
                            f.write_all_at(&src[off..off + block], off as u64).unwrap();
                        } else {
                            f.read_exact_at(out, off as u64).unwrap();
                        }
                    }
                }));
            }
            let setup_s = setup.elapsed().as_secs_f64();
            let start = Instant::now();
            gate.wait();
            for h in handles {
                h.join().unwrap();
            }
            (setup_s, start.elapsed().as_secs_f64())
        });
        io_s = measured;
        let s = Instant::now();
        if write {
            file.sync_all()?;
        }
        sync_s = s.elapsed().as_secs_f64();
        println!(
            "{}",
            json!({"setup_s":setup_s,"io_s":io_s,"sync_s":sync_s,"total_s":setup_s+io_s+sync_s,"cpu_s":cpu()-c0})
        );
    }
    if !write && !null && mode != "sdk" && mode != "sdk_fd" {
        assert_eq!(&*output, input, "FULL BYTE MISMATCH");
    }
    println!(
        "{}",
        json!({"mode":mode,"op":op,"bytes":n,"block":block,"q":q,"null":null,"verified":!write&&!null,"expected_hash":expected})
    );
    Ok(())
}
