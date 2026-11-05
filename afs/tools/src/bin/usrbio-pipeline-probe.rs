//! Controlled USRBIO-only benchmark. RAM implementation shared with entrance_probe.
use afs::node::{
    api::local::{LocalApiOptions, serve_local_api_with_options},
    diagnostics::DiagnosticStorage,
    vfs::{Backend, types::*},
};
use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig, DfsSyncMode};
use serde_json::json;
use std::{
    ffi::OsStr,
    fs,
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
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    delay_us: u64,
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
            active: std::sync::atomic::AtomicUsize::new(0),
            peak: std::sync::atomic::AtomicUsize::new(0),
            delay_us: std::env::var("CADX_READ_DELAY_US")
                .ok()
                .map(|v| v.parse().unwrap())
                .unwrap_or(0),
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
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(active, Ordering::Relaxed);
        if self.delay_us > 0 {
            std::thread::sleep(std::time::Duration::from_micros(self.delay_us));
        }

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
        self.active.fetch_sub(1, Ordering::Relaxed);
        Ok(n)
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
#[derive(Clone, Copy)]
struct Usage {
    cpu: f64,
    minor: i64,
    major: i64,
    rss_kb: i64,
}
fn usage() -> Usage {
    let mut r = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: r is valid writable rusage storage; getrusage initializes it on success.
    unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, r.as_mut_ptr()), 0);
        let r = r.assume_init();
        Usage {
            cpu: r.ru_utime.tv_sec as f64
                + r.ru_utime.tv_usec as f64 / 1e6
                + r.ru_stime.tv_sec as f64
                + r.ru_stime.tv_usec as f64 / 1e6,
            minor: r.ru_minflt,
            major: r.ru_majflt,
            rss_kb: r.ru_maxrss,
        }
    }
}
fn refill(v: &mut [u8], pass: usize) {
    let mut x = 0x5eedd24c15812345u64 ^ (pass as u64).wrapping_mul(0x9e3779b97f4a7c15);
    for c in v.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        c.copy_from_slice(&x.to_le_bytes()[..c.len()]);
    }
}
struct Meter {
    socket: std::os::unix::net::UnixDatagram,
    path: std::path::PathBuf,
}
impl Meter {
    fn new(root: &Path) -> Self {
        let path = root.join("meter-client");
        let socket = std::os::unix::net::UnixDatagram::bind(&path).unwrap();
        socket.connect(root.join("meter")).unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        Self { socket, path }
    }
    fn ask(&self, cmd: &str) -> serde_json::Value {
        self.socket.send(cmd.as_bytes()).unwrap();
        let mut b = [0u8; 8192];
        let n = self.socket.recv(&mut b).unwrap();
        serde_json::from_slice(&b[..n]).unwrap()
    }
}
impl Drop for Meter {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
fn report(
    identity: (&str, &str, usize, usize, usize, usize),
    measure: (f64, f64, Usage, Usage),
    start: serde_json::Value,
    end: serde_json::Value,
    expected: &str,
    profile: serde_json::Value,
) {
    let (mode, op, n, block, q, pass) = identity;
    let (setup, elapsed, a, b) = measure;
    println!(
        "{}",
        json!({"kind":"pass","build_flavor":option_env!("CADX_PROBE_BUILD_FLAVOR"),"mode":mode,"op":op,"bytes":n,"block":block,"q":q,"pass":pass,"setup_s":setup,"io_s":elapsed,"first_complete_s":setup+elapsed,"cpu_s":b.cpu-a.cpu,"minor_faults":b.minor-a.minor,"major_faults":b.major-a.major,"client_peak_rss_kb":b.rss_kb,"backend_ns":end["backend_service_ns"].as_u64().unwrap()-start["backend_service_ns"].as_u64().unwrap(),"backend_calls":end["calls"].as_u64().unwrap()-start["calls"].as_u64().unwrap(),"backend_bytes":end["bytes"].as_u64().unwrap()-start["bytes"].as_u64().unwrap(),"read_peak":end["read_peak"],"server_peak_rss_kb":end["peak_rss_kb"],"server_cpu_s":end["cpu_s"].as_f64().unwrap()-start["cpu_s"].as_f64().unwrap(),"expected_hash":expected,"verified":true,"profile":profile})
    );
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Any> {
    let a: Vec<String> = std::env::args().collect();
    let mode = &a[1];
    let root = Path::new(&a[2]);
    let n: usize = a[3].parse()?;
    if mode == "server" {
        fs::create_dir_all(root)?;
        let backend = Arc::new(Mem::new(n, false, false));
        let api = serve_local_api_with_options(
            DiagnosticStorage::new(root.join("diag"))?,
            root.join("node.sock"),
            LocalApiOptions {
                metrics_registry: None,
                dfs_backend: Some(backend.clone()),
                dfs_namespace: Some("default".into()),
            },
        )
        .await?;
        let socket = tokio::net::UnixDatagram::bind(root.join("meter"))?;
        println!("{}", json!({"ready":true}));
        let mut bytes = [0; 128];
        loop {
            let (len, addr) = socket.recv_from(&mut bytes).await?;
            let cmd = std::str::from_utf8(&bytes[..len])?;
            if cmd == "start" {
                backend.peak.store(0, Ordering::Relaxed);
            }
            let u = usage();
            let hash = if cmd == "end" {
                backend.stats()["hash"].as_str().unwrap().to_owned()
            } else {
                String::new()
            };
            let d = json!({"cpu_s":u.cpu,"peak_rss_kb":u.rss_kb,"minor":u.minor,"major":u.major,"backend_service_ns":backend.ns.load(Ordering::Relaxed),"calls":backend.reads.load(Ordering::Relaxed)+backend.writes.load(Ordering::Relaxed),"bytes":backend.bytes.load(Ordering::Relaxed),"hash":hash,"read_peak":backend.peak.load(Ordering::Relaxed)});
            socket
                .send_to(&serde_json::to_vec(&d)?, addr.as_pathname().unwrap())
                .await?;
            if cmd == "stop" {
                break;
            }
        }
        api.shutdown().await?;
        return Ok(());
    }
    let op = &a[4];
    let block: usize = a[5].parse()?;
    let q: usize = a[6].parse()?;
    let passes: usize = a[7].parse()?;
    let write = op == "write";
    assert!(n.is_multiple_of(block) && SHARD.is_multiple_of(block) && q <= 128 && q > 0);
    let mut input = Buf::new(n);
    let mut output = Buf::new(n);
    refill(input.bytes_mut(), 0);
    if mode == "copy2" {
        let backend = Mem::new(n, false, false);
        let mut bank = vec![0u8; block * q];
        let setup = 0.;
        for pass in 0..passes {
            if write {
                refill(input.bytes_mut(), pass + 1);
            } else {
                output.bytes_mut().fill(0xA5);
            }
            let expected = blake3::hash(input.bytes()).to_hex().to_string();
            let a = usage();
            let t = Instant::now();
            let before = backend.ns.load(Ordering::Relaxed);
            let ctx = RequestContext {
                uid: 501,
                gid: 1000,
                pid: 0,
                umask: 0,
                supplementary_gids: vec![],
            };
            for base in (0..n).step_by(block * q) {
                let end = (base + block * q).min(n);
                if write {
                    bank[..end - base].copy_from_slice(&input.bytes()[base..end]);
                    for (j, s) in bank[..end - base].chunks(block).enumerate() {
                        backend.write(&ctx, FileHandle(1), (base + j * block) as u64, s)?;
                    }
                } else {
                    for (j, s) in bank[..end - base].chunks_mut(block).enumerate() {
                        backend.read(&ctx, FileHandle(1), (base + j * block) as u64, s)?;
                    }
                    output.bytes_mut()[base..end].copy_from_slice(&bank[..end - base]);
                }
            }
            let elapsed = t.elapsed().as_secs_f64();
            let b = usage();
            let backend_ns = backend.ns.load(Ordering::Relaxed) - before;
            if write {
                assert_eq!(backend.stats()["hash"], expected);
            } else {
                assert_eq!(output.bytes(), input.bytes());
            }
            report(
                (mode, op, n, block, q, pass),
                (setup, elapsed, a, b),
                json!({"backend_service_ns":0,"calls":0,"bytes":0,"cpu_s":0.}),
                json!({"backend_service_ns":backend_ns,"calls":n/block,"bytes":n,"cpu_s":0.}),
                &expected,
                json!({"same_process_payload_floor":true}),
            );
        }
        return Ok(());
    }
    let meter = Meter::new(root);
    let setup = Instant::now();
    let mut io_sum = 0.;
    if mode.starts_with("sdk") {
        let client =
            DfsLocalClient::connect(DfsLocalClientConfig::new(root.join("node.sock"))).await?;
        let file = client
            .open(
                "/data.bin",
                if write {
                    DfsAccess::ReadWrite
                } else {
                    DfsAccess::ReadOnly
                },
            )
            .await?;
        let mut iov = client.create_iov(block, q as u32).await?;
        let mut ring = client.create_ring(&iov, 128, 0, 30000).await?;
        let reg = client.register_file(&file).await?;
        let setup_s = setup.elapsed().as_secs_f64();
        if mode == "sdk_canary" {
            let mut out = [0xA5; 8];
            let eof = ring.read_into(&mut iov, &reg, n as u64, &mut out).await;
            println!(
                "{}",
                json!({"kind":"canary","eof_supported":matches!(eof,Ok(0))&&out==[0xA5;8],"build_flavor":option_env!("CADX_PROBE_BUILD_FLAVOR")})
            );
        }

        for pass in 0..passes {
            if write {
                refill(input.bytes_mut(), pass + 1);
            } else {
                output.bytes_mut().fill(0xA5);
            }
            let expected = blake3::hash(input.bytes()).to_hex().to_string();
            let start = meter.ask("start");
            let a = usage();
            let t = Instant::now();
            for base in (0..n).step_by(block * q) {
                let end = (base + block * q).min(n);
                if write {
                    let items: Vec<_> = input.bytes()[base..end]
                        .chunks(block)
                        .enumerate()
                        .map(|(j, s)| ((base + j * block) as u64, s))
                        .collect();
                    let result = ring.write_batch(&mut iov, &reg, &items).await?;
                    assert!(result.iter().all(|v| *v == block));
                } else {
                    let mut items: Vec<_> = output.bytes_mut()[base..end]
                        .chunks_mut(block)
                        .enumerate()
                        .map(|(j, s)| ((base + j * block) as u64, s))
                        .collect();
                    let result = ring.read_batch(&mut iov, &reg, &mut items).await?;
                    assert!(result.iter().all(|v| *v == block));
                }
            }
            let elapsed = t.elapsed().as_secs_f64();
            io_sum += elapsed;
            let b = usage();
            let end = meter.ask("end");
            assert_eq!(
                end["bytes"].as_u64().unwrap() - start["bytes"].as_u64().unwrap(),
                n as u64
            );
            if write {
                assert_eq!(end["hash"], expected);
            } else {
                assert_eq!(output.bytes(), input.bytes());
            }
            assert_eq!(iov.free_blocks(), q as u32);
            report(
                (mode, op, n, block, q, pass),
                (setup_s, elapsed, a, b),
                start,
                end,
                &expected,
                json!({}),
            );
        }
        let cleanup = Instant::now();
        if write {
            client.sync(&file.handle, DfsSyncMode::Full).await?;
        }
        client.unregister_file(reg).await?;
        client.close(file.handle).await?;
        ring.destroy().await?;
        iov.destroy().await?;
        let cleanup_s = cleanup.elapsed().as_secs_f64();
        println!(
            "{}",
            json!({"kind":"session","setup_s":setup_s,"transfer_sum_s":io_sum,"cleanup_s":cleanup_s,"component_session_s":setup_s+io_sum+cleanup_s,"passes":passes})
        );
    } else {
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
        for pass in 0..passes {
            if write {
                refill(input.bytes_mut(), pass + 1);
            } else {
                output.bytes_mut().fill(0xA5);
            }
            let expected = blake3::hash(input.bytes()).to_hex().to_string();
            let start = meter.ask("start");
            let a = usage();
            let t = Instant::now();
            let profile = if mode.ends_with("async") {
                ring.probe_wire_async(
                    &mut iov,
                    &reg,
                    input.bytes(),
                    output.bytes_mut(),
                    (block, q, write, mode == "wire_pipeline_async"),
                )
                .await?
            } else {
                ring.probe_wire(
                    &mut iov,
                    &reg,
                    input.bytes(),
                    output.bytes_mut(),
                    (block, q, write, mode == "wire_pipeline"),
                )?
            };
            let elapsed = t.elapsed().as_secs_f64();
            io_sum += elapsed;
            let b = usage();
            let end = meter.ask("end");
            if write {
                assert_eq!(end["hash"], expected);
            } else {
                assert_eq!(output.bytes(), input.bytes());
            }
            assert_eq!(iov.free_blocks(), q as u32);
            report(
                (mode, op, n, block, q, pass),
                (setup_s, elapsed, a, b),
                start,
                end,
                &expected,
                profile,
            );
        }
        let cleanup = Instant::now();
        if write {
            client.sync(&file.handle, native::DfsSyncMode::Full).await?;
        }
        client.unregister_file(reg).await?;
        client.close(file.handle).await?;
        ring.destroy().await?;
        iov.destroy().await?;
        let cleanup_s = cleanup.elapsed().as_secs_f64();
        println!(
            "{}",
            json!({"kind":"session","setup_s":setup_s,"transfer_sum_s":io_sum,"cleanup_s":cleanup_s,"component_session_s":setup_s+io_sum+cleanup_s,"passes":passes})
        );
    }
    Ok(())
}
