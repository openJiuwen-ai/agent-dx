//! afs-bench —— AFS 双臂基准工具：框架 v1 与首个 workload（多文件并发读写）。
//!
//! 双臂同尺：FUSE 臂走挂载点（POSIX 语义），SDK 臂走节点 uds（usrbio 数据面），
//! 同一确定性 payload、同口径三段计时（setup/io/sync），结果直接可比。
//!
//! 用法：
//!   afs-bench <mountpoint> [--sock <uds>] [--arm fuse|sdk|both]
//!            [--mode rw|write|read|mixed] [--files N] [--file-bytes B]
//!            [--block B] [--threads N] [--q Q] [--rounds R]
//!            [--settle-secs S] [--json <path>] [--gc-log <node.log>]
//!            [--node-cgroup <dir>] [--max-node-mem MiB] [--keep]
//!
//! 方法论与合同（细节见 docs/testing/afs.md 的 afs-bench 节）：
//! - 完整文件基准：写前把每个文件建立为精确的 `--file-bytes` 长度（清旧尾部）；
//!   读相位逐块校验整个 [0, file_bytes) 区间；纯读模式不修改输入，只校验
//!   存在与精确长度，不符即报错。mixed 的只读集合在计时外预写 payload。
//! - payload 按 (file, block) 确定性生成（xorshift64），跨臂跨轮字节一致，
//!   防块去重骗数字；生成与校验都计入 io 段计时。
//! - 三段计时之和不是 end-to-end wall：线程/运行时创建、缓冲分配、SDK
//!   销毁等在段外、计入总 wall。每相位 wall 在业务完成时冻结，指标线程
//!   回收不延长测量窗口。
//! - GC 平静：给了 --gc-log 则轮询日志停增（缺失/不可读报错；预算耗尽
//!   如实报告后继续），否则盲睡 --settle-secs；轮间保留原始值，偶数轮
//!   取上中位数。
//! - 节点内存：默认按 afs-node 进程 RSS 之和观测；--node-cgroup 改读指定
//!   cgroup 的 memory.current。--max-node-mem 启用时目标必须可观测。
//! - 任一读块不匹配即整轮失败：输出 JSON 与失败行后以非零码退出，默认
//!   保留 bench 文件供诊断。人类输出区分 ok / FAIL / -（未校验）。
//! - 输出携带环境身份（主机/内核/架构/argv）与 node/meta 进程 CPU 采样；
//!   SDK 线程 CPU 在异步任务两端采样，只能作近似参考。
//! - v1 零新增依赖（手写参数解析与 JSON 输出）。
//!
//! 扩展指南（给后续贡献者）：实现 `Workload` trait（name + run），在 `registry()`
//! 注册即可，CLI 与输出层无需改动。臂与计时纪律由 `BenchCtx` 统一提供。

use std::{
    fs::{File, OpenOptions},
    io::Write as IoWrite,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    process::ExitCode,
    time::{Duration, Instant},
};

use afs_client::{
    DfsAccess, DfsIov, DfsLocalClient, DfsLocalClientConfig, DfsOpenFile, DfsRegistration, DfsRing,
    DfsSyncMode,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type DispatchResults = Vec<(Job, Result<ThreadStat, String>)>;

const DEFAULT_FILES: usize = 16;
const DEFAULT_FILE_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_BLOCK: u64 = 4 * 1024 * 1024;
const DEFAULT_THREADS: usize = 16;
const DEFAULT_Q: usize = 1;
const DEFAULT_ROUNDS: usize = 3;
const DEFAULT_SETTLE_SECS: u64 = 65;
/// 单个 SDK 批次（--block × --q）的上限，也是每线程批次缓冲的分配上限：
/// 超出直接在参数层拒绝，而不是把溢出/中止留到运行期。
const MAX_BATCH_BYTES: u64 = 1 << 30;
/// 与同包 usrbio_bench 相同的环参数：128 项、io_depth 0（立即消费，避免末尾
/// 不足批永远凑不满）、30s 超时。
const RING_ENTRIES: u32 = 128;
const RING_IO_DEPTH: i32 = 0;
const RING_TIMEOUT_MS: u32 = 30_000;
/// 节点内存采样周期；停止采样用可唤醒等待，不把收尾等待计入业务 wall。
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arm {
    Fuse,
    Sdk,
    Both,
}

impl Arm {
    fn labels(self) -> Vec<&'static str> {
        match self {
            Arm::Fuse => vec!["fuse"],
            Arm::Sdk => vec!["sdk"],
            Arm::Both => vec!["fuse", "sdk"],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Rw,
    Write,
    Read,
    Mixed,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Mode::Rw => "rw",
            Mode::Write => "write",
            Mode::Read => "read",
            Mode::Mixed => "mixed",
        }
    }
}

#[derive(Debug, Clone)]
struct Args {
    mountpoint: PathBuf,
    sock: Option<PathBuf>,
    arm: Arm,
    mode: Mode,
    files: usize,
    file_bytes: u64,
    block: u64,
    threads: usize,
    q: usize,
    rounds: usize,
    settle_secs: u64,
    json: Option<PathBuf>,
    gc_log: Option<PathBuf>,
    node_cgroup: Option<PathBuf>,
    max_node_mem_mib: u64,
    keep: bool,
}

fn parse_size(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let unit_len = text
        .chars()
        .rev()
        .take_while(char::is_ascii_alphabetic)
        .count();
    let split = text
        .len()
        .checked_sub(unit_len)
        .ok_or_else(|| format!("invalid size: {text}"))?;
    let (digits, unit) = text.split_at(split);
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("invalid size: {text}"))?;
    match unit.to_ascii_lowercase().as_str() {
        "" | "b" => Ok(value),
        "k" | "ki" => value
            .checked_mul(1024)
            .ok_or_else(|| "size overflow".to_string()),
        "m" | "mi" => value
            .checked_mul(1024 * 1024)
            .ok_or_else(|| "size overflow".to_string()),
        "g" | "gi" => value
            .checked_mul(1024 * 1024 * 1024)
            .ok_or_else(|| "size overflow".to_string()),
        _ => Err(format!("unknown size unit: {unit}")),
    }
}

fn parse_args(argv: &[String]) -> Result<Args, String> {
    let mut mountpoint: Option<PathBuf> = None;
    let mut sock = None;
    let mut arm = Arm::Both;
    let mut mode = Mode::Rw;
    let mut files = DEFAULT_FILES;
    let mut file_bytes = DEFAULT_FILE_BYTES;
    let mut block = DEFAULT_BLOCK;
    let mut threads = DEFAULT_THREADS;
    let mut q = DEFAULT_Q;
    let mut rounds = DEFAULT_ROUNDS;
    let mut settle_secs = DEFAULT_SETTLE_SECS;
    let mut json = None;
    let mut gc_log = None;
    let mut node_cgroup: Option<PathBuf> = None;
    let mut max_node_mem_mib: u64 = 0;
    let mut keep = false;
    let mut index = 0;
    while index < argv.len() {
        let flag = argv[index].clone();
        let value = |index: &mut usize| -> Result<String, String> {
            *index += 1;
            argv.get(*index)
                .cloned()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag.as_str() {
            "--sock" => sock = Some(PathBuf::from(value(&mut index)?)),
            "--arm" => match value(&mut index)?.as_str() {
                "fuse" => arm = Arm::Fuse,
                "sdk" => arm = Arm::Sdk,
                "both" => arm = Arm::Both,
                other => return Err(format!("unknown arm: {other}")),
            },
            "--mode" => match value(&mut index)?.as_str() {
                "rw" => mode = Mode::Rw,
                "write" => mode = Mode::Write,
                "read" => mode = Mode::Read,
                "mixed" => mode = Mode::Mixed,
                other => return Err(format!("unknown mode: {other}")),
            },
            "--files" => {
                files = value(&mut index)?
                    .parse()
                    .map_err(|_| "invalid --files".to_string())?
            }
            "--file-bytes" => file_bytes = parse_size(&value(&mut index)?)?,
            "--block" => block = parse_size(&value(&mut index)?)?,
            "--threads" => {
                threads = value(&mut index)?
                    .parse()
                    .map_err(|_| "invalid --threads".to_string())?
            }
            "--q" => {
                q = value(&mut index)?
                    .parse()
                    .map_err(|_| "invalid --q".to_string())?
            }
            "--rounds" => {
                rounds = value(&mut index)?
                    .parse()
                    .map_err(|_| "invalid --rounds".to_string())?
            }
            "--settle-secs" => {
                settle_secs = value(&mut index)?
                    .parse()
                    .map_err(|_| "invalid --settle-secs".to_string())?
            }
            "--json" => json = Some(PathBuf::from(value(&mut index)?)),
            "--gc-log" => gc_log = Some(PathBuf::from(value(&mut index)?)),
            "--node-cgroup" => node_cgroup = Some(PathBuf::from(value(&mut index)?)),
            "--max-node-mem" => {
                max_node_mem_mib = value(&mut index)?
                    .parse()
                    .map_err(|_| "invalid --max-node-mem".to_string())?
            }
            "--keep" => keep = true,
            "--help" | "-h" => return Err(String::new()),
            other if other.starts_with("--") => return Err(format!("unknown flag: {other}")),
            other => {
                if mountpoint.is_some() {
                    return Err(format!("unexpected extra argument: {other}"));
                }
                mountpoint = Some(PathBuf::from(other));
            }
        }
        index += 1;
    }
    let mountpoint = mountpoint.ok_or_else(|| "missing <mountpoint>".to_string())?;
    if file_bytes == 0 {
        return Err("--file-bytes must be positive".into());
    }
    if block == 0 || !file_bytes.is_multiple_of(block) {
        return Err("--block must be positive and divide --file-bytes".into());
    }
    if files == 0 || threads == 0 || q == 0 || rounds == 0 {
        return Err("--files/--threads/--q/--rounds must be positive".into());
    }
    if max_node_mem_mib.checked_mul(1024 * 1024).is_none() {
        return Err("--max-node-mem overflows the supported byte range".into());
    }
    // SDK 的块计数是 u32：在 CLI 边界拒绝越界 q，禁止窄化后与本地算术不一致。
    if q > u32::MAX as usize {
        return Err(format!(
            "--q exceeds the SDK block-count limit {}",
            u32::MAX
        ));
    }
    // 批次大小同时约束每线程批次缓冲的分配：提前用 checked 算术拒绝。
    let batch_bytes = block
        .checked_mul(q as u64)
        .ok_or_else(|| "--q x --block overflows".to_string())?;
    if batch_bytes > MAX_BATCH_BYTES {
        return Err(format!(
            "--q x --block = {batch_bytes} exceeds the supported batch size {MAX_BATCH_BYTES}"
        ));
    }
    Ok(Args {
        mountpoint,
        sock,
        arm,
        mode,
        files,
        file_bytes,
        block,
        threads,
        q,
        rounds,
        settle_secs,
        json,
        gc_log,
        node_cgroup,
        max_node_mem_mib,
        keep,
    })
}

fn usage() -> &'static str {
    "usage: afs-bench <mountpoint> [--sock <uds>] [--arm fuse|sdk|both] [--mode rw|write|read|mixed]\n\
     [--files N] [--file-bytes B] [--block B] [--threads N] [--q Q] [--rounds R]\n\
     [--settle-secs S] [--json <path>] [--gc-log <node.log>] [--node-cgroup <dir>]\n\
     [--max-node-mem MiB] [--keep]"
}

// ---------------------------------------------------------------------------
// 确定性 payload：按 (file, block) 生成，跨臂跨轮一致，防块去重。
// ---------------------------------------------------------------------------

fn xorshift_fill(seed: u64, out: &mut [u8]) {
    let mut x = seed | 1;
    for chunk in out.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let bytes = x.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

fn block_seed(file_index: usize, block_index: u64) -> u64 {
    (file_index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ block_index.wrapping_mul(0xBF58_476D_1CE4_E5B9)
        ^ 0x5EED_D24C_1581_2345
}

fn payload_block(file_index: usize, block_index: u64, block: &mut [u8]) {
    xorshift_fill(block_seed(file_index, block_index), block);
}

// ---------------------------------------------------------------------------
// 环境身份与资源采样
// ---------------------------------------------------------------------------

struct EnvIdentity {
    host: String,
    kernel: String,
    arch: String,
    nproc: usize,
    argv: String,
}

impl EnvIdentity {
    fn capture(argv: &[String]) -> Self {
        let read_trim = |path: &str| {
            std::fs::read_to_string(path)
                .unwrap_or_default()
                .trim()
                .to_string()
        };
        EnvIdentity {
            host: read_trim("/proc/sys/kernel/hostname"),
            kernel: read_trim("/proc/sys/kernel/osrelease"),
            arch: std::env::consts::ARCH.into(),
            nproc: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            argv: argv.join(" "),
        }
    }
}

fn proc_pids_by_name(name: &str) -> Vec<u32> {
    let mut pids = Vec::new();
    let entries = match std::fs::read_dir("/proc") {
        Ok(entries) => entries,
        Err(_) => return pids,
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|text| text.parse::<u32>().ok())
        else {
            continue;
        };
        let matches = std::fs::read_to_string(entry.path().join("comm"))
            .map(|comm| comm.trim() == name)
            .unwrap_or(false);
        if matches {
            pids.push(pid);
        }
    }
    pids
}

fn pids_cpu_seconds(pids: &[u32]) -> f64 {
    let mut total = 0.0;
    for pid in pids {
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        let fields: Vec<&str> = stat.split_whitespace().collect();
        if fields.len() >= 15 {
            let utime: f64 = fields[13].parse().unwrap_or(0.0);
            let stime: f64 = fields[14].parse().unwrap_or(0.0);
            total += (utime + stime) / 100.0;
        }
    }
    total
}

fn thread_cpu_seconds() -> f64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes into the valid timespec `ts`; CLOCK_THREAD_CPUTIME_ID
    // has no failure mode on supported Linux kernels and the return code is checked.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return 0.0;
    }
    ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
}

// ---------------------------------------------------------------------------
// Workload 框架
// ---------------------------------------------------------------------------

struct BenchCtx {
    args: Args,
    node_pids: Vec<u32>,
    meta_pids: Vec<u32>,
    memory_target: MemoryTarget,
}

impl BenchCtx {
    fn args(&self) -> &Args {
        &self.args
    }
}

#[derive(Debug, Default, Clone)]
struct ThreadStat {
    setup_s: f64,
    io_s: f64,
    sync_s: f64,
    cpu_s: f64,
    verified: Option<bool>,
}

#[derive(Debug, Default, Clone)]
struct PhaseStat {
    wall_s: f64,
    bytes: u64,
    throughput_bytes_per_s: f64,
    node_cpu_s: f64,
    meta_cpu_s: f64,
    node_mem_peak_bytes: Option<u64>,
    node_mem_observation_complete: bool,
    node_mem_source: String,
    verified: Option<bool>,
    threads: Vec<ThreadStat>,
}

#[derive(Debug)]
struct CaseReport {
    name: &'static str,
    arm: &'static str,
    phase: &'static str,
    mode: &'static str,
    rounds: Vec<PhaseStat>,
}

impl CaseReport {
    fn median_throughput(&self) -> f64 {
        let values: Vec<f64> = self
            .rounds
            .iter()
            .map(|round| round.throughput_bytes_per_s)
            .collect();
        median(&values)
    }

    /// 任一轮读校验失败即为损坏：false 不会被后续轮的 true 覆盖。
    fn has_corruption(&self) -> bool {
        self.rounds
            .iter()
            .any(|round| round.verified == Some(false))
    }
}

trait Workload {
    fn name(&self) -> &'static str;
    fn run(&self, ctx: &BenchCtx) -> Result<Vec<CaseReport>, BoxError>;
}

fn registry() -> Vec<Box<dyn Workload>> {
    vec![Box::new(MultiFileRw)]
}

fn settle(ctx: &BenchCtx, label: &str) -> Result<(), BoxError> {
    let seconds = ctx.args.settle_secs;
    if let Some(log) = &ctx.args.gc_log {
        let budget = Duration::from_secs(seconds.max(10));
        match wait_gc_quiet(log, budget, GC_POLL_INTERVAL)? {
            GcQuiet::Quiet { polls } => {
                eprintln!("[afs-bench] gc log quiet for {polls} polls before {label}; grace 5s");
            }
            GcQuiet::BudgetExhausted => {
                eprintln!(
                    "[afs-bench] gc-quiet budget of {seconds}s exhausted before {label} (no observed quiet); continuing after grace 5s"
                );
            }
        }
        std::thread::sleep(Duration::from_secs(5));
        return Ok(());
    }
    eprintln!("[afs-bench] settle {seconds}s before {label} (no --gc-log; blind sleep)");
    std::thread::sleep(Duration::from_secs(seconds));
    Ok(())
}

/// GC 日志观察结果：只有 `Quiet` 表示真正观察到日志停增。
enum GcQuiet {
    Quiet { polls: u32 },
    BudgetExhausted,
}

const GC_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// 观察节点日志停增（gc_collected 每 5s 一条）。日志缺失或中途不可读是
/// 观察失败，按错误处理；预算耗尽是没观察到安静，如实报告后放行。
fn wait_gc_quiet(log: &Path, budget: Duration, poll: Duration) -> Result<GcQuiet, BoxError> {
    wait_gc_quiet_source(budget, poll, || {
        std::fs::metadata(log)
            .map(|meta| meta.len())
            .map_err(|error| format!("{}: {error}", log.display()))
    })
}

/// 轮询长度源直到两次相等（Quiet）、预算耗尽（BudgetExhausted）或观察失败（Err）。
/// 长度回缩视为轮转/截断：重置基线继续观察，不计为安静。
fn wait_gc_quiet_source(
    budget: Duration,
    poll: Duration,
    mut len: impl FnMut() -> Result<u64, String>,
) -> Result<GcQuiet, BoxError> {
    let mut last_len = len()?;
    let deadline = Instant::now() + budget;
    let mut polls: u32 = 0;
    loop {
        std::thread::sleep(poll);
        let len = len()?;
        polls = polls.saturating_add(1);
        if len == last_len {
            return Ok(GcQuiet::Quiet { polls });
        }
        last_len = len;
        if Instant::now() >= deadline {
            return Ok(GcQuiet::BudgetExhausted);
        }
    }
}

fn median(values: &[f64]) -> f64 {
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    sorted[sorted.len() / 2]
}

fn bench_file(mountpoint: &Path, file_index: usize) -> PathBuf {
    mountpoint.join(format!("bench-{file_index:03}.bin"))
}

fn dfs_path(file_index: usize) -> String {
    format!("/bench-{file_index:03}.bin")
}

// ---------------------------------------------------------------------------
// FUSE 臂
// ---------------------------------------------------------------------------

struct FuseIo;

impl FuseIo {
    /// 完整文件基准：把给定文件集合建立为精确的 `--file-bytes` 长度（计时外）。
    /// `set_len` 同时清掉旧尾部，避免残留数据躲过 [0, file_bytes) 的读校验。
    fn ensure_files(args: &Args, files: &[usize]) -> Result<(), BoxError> {
        for file_index in files {
            let path = bench_file(&args.mountpoint, *file_index);
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)?;
            file.set_len(args.file_bytes)?;
        }
        Ok(())
    }

    /// mixed 的只读集合在计时外写入确定性 payload 并 sync；
    /// 纯读模式不修改输入，只校验每个文件存在且长度精确。
    fn prepare_read_set(args: &Args, files: &[usize]) -> Result<(), BoxError> {
        for file_index in files {
            write_expected_payload(&args.mountpoint, *file_index, args.file_bytes, args.block)?;
        }
        Ok(())
    }

    fn verify_input_files(args: &Args) -> Result<(), BoxError> {
        let mut problems = Vec::new();
        for file_index in 0..args.files {
            let path = bench_file(&args.mountpoint, file_index);
            match std::fs::metadata(&path) {
                Ok(meta) if meta.len() == args.file_bytes => {}
                Ok(meta) => problems.push(format!("{} ({} bytes)", path.display(), meta.len())),
                Err(error) => problems.push(format!("{} ({error})", path.display())),
            }
        }
        if problems.is_empty() {
            return Ok(());
        }
        Err(format!(
            "read mode measures existing inputs and does not modify them; \
             expected {} files of exactly {} bytes: {}",
            args.files,
            args.file_bytes,
            problems.join("; ")
        )
        .into())
    }

    fn write_one(args: &Args, file_index: usize) -> Result<ThreadStat, BoxError> {
        let path = bench_file(&args.mountpoint, file_index);
        let started = Instant::now();
        let cpu0 = thread_cpu_seconds();
        let file = OpenOptions::new().write(true).open(&path)?;
        let setup_s = started.elapsed().as_secs_f64();
        let blocks = args.file_bytes / args.block;
        let mut block_buf = vec![0u8; args.block as usize];
        let io_started = Instant::now();
        for block_index in 0..blocks {
            payload_block(file_index, block_index, &mut block_buf);
            file.write_all_at(&block_buf, block_index * args.block)?;
        }
        let io_s = io_started.elapsed().as_secs_f64();
        let sync_started = Instant::now();
        file.sync_all()?;
        let sync_s = sync_started.elapsed().as_secs_f64();
        drop(file);
        Ok(ThreadStat {
            setup_s,
            io_s,
            sync_s,
            cpu_s: thread_cpu_seconds() - cpu0,
            verified: None,
        })
    }

    fn read_one(args: &Args, file_index: usize) -> Result<ThreadStat, BoxError> {
        let path = bench_file(&args.mountpoint, file_index);
        let started = Instant::now();
        let cpu0 = thread_cpu_seconds();
        let file = File::open(&path)?;
        let setup_s = started.elapsed().as_secs_f64();
        let blocks = args.file_bytes / args.block;
        let mut block_buf = vec![0u8; args.block as usize];
        let mut expected = vec![0u8; args.block as usize];
        let io_started = Instant::now();
        let mut verified = true;
        for block_index in 0..blocks {
            file.read_exact_at(&mut block_buf, block_index * args.block)?;
            payload_block(file_index, block_index, &mut expected);
            verified &= block_buf == expected;
        }
        let io_s = io_started.elapsed().as_secs_f64();
        Ok(ThreadStat {
            setup_s,
            io_s,
            sync_s: 0.0,
            cpu_s: thread_cpu_seconds() - cpu0,
            verified: Some(verified),
        })
    }
}

/// mixed 的写入集合是前 ceil(files/2)，其余为只读集合；其他模式全部文件
/// 归写入集合（read 模式全部只读）。两个集合恰好覆盖每个文件一次。
fn mixed_partition(files: usize, mode: Mode) -> (Vec<usize>, Vec<usize>) {
    match mode {
        Mode::Mixed => {
            let writers = files.div_ceil(2);
            ((0..writers).collect(), (writers..files).collect())
        }
        Mode::Read => (Vec::new(), (0..files).collect()),
        Mode::Rw | Mode::Write => ((0..files).collect(), Vec::new()),
    }
}

/// 计时外为只读集合写入确定性 payload 并 sync（mixed 准备；两臂共用同一挂载）。
fn write_expected_payload(
    mountpoint: &Path,
    file_index: usize,
    file_bytes: u64,
    block: u64,
) -> Result<(), BoxError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(bench_file(mountpoint, file_index))?;
    file.set_len(file_bytes)?;
    let block_len = usize::try_from(block).map_err(|_| "--block overflows usize")?;
    let mut block_buf = vec![0u8; block_len];
    for block_index in 0..file_bytes / block {
        payload_block(file_index, block_index, &mut block_buf);
        file.write_all_at(&block_buf, block_index * block)?;
    }
    file.sync_all()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// SDK 臂（usrbio 数据面）
// ---------------------------------------------------------------------------

struct SdkIo;

impl SdkIo {
    async fn write_one(
        args: &Args,
        sock: &Path,
        file_index: usize,
    ) -> Result<ThreadStat, BoxError> {
        let started = Instant::now();
        let cpu0 = thread_cpu_seconds();
        let client = DfsLocalClient::connect(DfsLocalClientConfig::new(sock.to_path_buf())).await?;
        // 每步失败都带着已建资源走有序尽力清理：这些句柄活在长寿命 Node 的
        // UsrbioHub 里，进程退出不等于回收（评审 R132-03 实测累积）。
        let file = match client
            .open(&dfs_path(file_index), DfsAccess::ReadWrite)
            .await
        {
            Ok(file) => file,
            Err(error) => return Err(teardown(&client, None, None, None, None, error.into()).await),
        };
        let mut iov = match client
            .create_iov(usize::try_from(args.block)?, u32::try_from(args.q)?)
            .await
        {
            Ok(iov) => iov,
            Err(error) => {
                return Err(teardown(&client, None, Some(file), None, None, error.into()).await);
            }
        };
        let mut ring = match client
            .create_ring(&iov, RING_ENTRIES, RING_IO_DEPTH, RING_TIMEOUT_MS)
            .await
        {
            Ok(ring) => ring,
            Err(error) => {
                return Err(
                    teardown(&client, None, Some(file), None, Some(iov), error.into()).await,
                );
            }
        };
        let reg = match client.register_file(&file).await {
            Ok(reg) => reg,
            Err(error) => {
                return Err(teardown(
                    &client,
                    None,
                    Some(file),
                    Some(ring),
                    Some(iov),
                    error.into(),
                )
                .await);
            }
        };
        let setup_s = started.elapsed().as_secs_f64();
        let batch_bytes = args.block * args.q as u64;
        let batch_len = usize::try_from(batch_bytes)?;
        let block_len = usize::try_from(args.block)?;
        let mut batch_buf = vec![0u8; batch_len];
        let io_started = Instant::now();
        for base in (0..args.file_bytes).step_by(batch_len) {
            let end = (base + batch_bytes).min(args.file_bytes);
            let count = ((end - base) / args.block) as usize;
            for slot in 0..count {
                payload_block(
                    file_index,
                    base / args.block + slot as u64,
                    &mut batch_buf[slot * block_len..(slot + 1) * block_len],
                );
            }
            let items: Vec<(u64, &[u8])> = (0..count)
                .map(|slot| {
                    (
                        base + slot as u64 * args.block,
                        &batch_buf[slot * block_len..(slot + 1) * block_len],
                    )
                })
                .collect();
            let batch = match ring.write_batch(&mut iov, &reg, &items).await {
                Ok(batch) => batch,
                Err(error) => {
                    return Err(teardown(
                        &client,
                        Some(reg),
                        Some(file),
                        Some(ring),
                        Some(iov),
                        error.into(),
                    )
                    .await);
                }
            };
            if batch.iter().any(|size| *size as u64 != args.block) {
                let error: BoxError = "short write in ring batch".into();
                return Err(
                    teardown(&client, Some(reg), Some(file), Some(ring), Some(iov), error).await,
                );
            }
        }
        let io_s = io_started.elapsed().as_secs_f64();
        let sync_started = Instant::now();
        if let Err(error) = client.sync(&file.handle, DfsSyncMode::Full).await {
            return Err(teardown(
                &client,
                Some(reg),
                Some(file),
                Some(ring),
                Some(iov),
                error.into(),
            )
            .await);
        }
        let sync_s = sync_started.elapsed().as_secs_f64();
        cleanup_sdk(&client, Some(reg), Some(file), Some(ring), Some(iov)).await?;
        Ok(ThreadStat {
            setup_s,
            io_s,
            sync_s,
            cpu_s: thread_cpu_seconds() - cpu0,
            verified: None,
        })
    }

    async fn read_one(args: &Args, sock: &Path, file_index: usize) -> Result<ThreadStat, BoxError> {
        let started = Instant::now();
        let cpu0 = thread_cpu_seconds();
        let client = DfsLocalClient::connect(DfsLocalClientConfig::new(sock.to_path_buf())).await?;
        let file = match client
            .open(&dfs_path(file_index), DfsAccess::ReadOnly)
            .await
        {
            Ok(file) => file,
            Err(error) => return Err(teardown(&client, None, None, None, None, error.into()).await),
        };
        let mut iov = match client
            .create_iov(usize::try_from(args.block)?, u32::try_from(args.q)?)
            .await
        {
            Ok(iov) => iov,
            Err(error) => {
                return Err(teardown(&client, None, Some(file), None, None, error.into()).await);
            }
        };
        let mut ring = match client
            .create_ring(&iov, RING_ENTRIES, RING_IO_DEPTH, RING_TIMEOUT_MS)
            .await
        {
            Ok(ring) => ring,
            Err(error) => {
                return Err(
                    teardown(&client, None, Some(file), None, Some(iov), error.into()).await,
                );
            }
        };
        let reg = match client.register_file(&file).await {
            Ok(reg) => reg,
            Err(error) => {
                return Err(teardown(
                    &client,
                    None,
                    Some(file),
                    Some(ring),
                    Some(iov),
                    error.into(),
                )
                .await);
            }
        };
        let setup_s = started.elapsed().as_secs_f64();
        let batch_bytes = args.block * args.q as u64;
        let batch_len = usize::try_from(batch_bytes)?;
        let block_len = usize::try_from(args.block)?;
        let mut batch_buf = vec![0u8; batch_len];
        let mut expected = vec![0u8; block_len];
        let io_started = Instant::now();
        let mut verified = true;
        for base in (0..args.file_bytes).step_by(batch_len) {
            let end = (base + batch_bytes).min(args.file_bytes);
            let count = ((end - base) / args.block) as usize;
            let mut items: Vec<(u64, &mut [u8])> = Vec::with_capacity(count);
            for (slot, slice) in batch_buf[..count * block_len]
                .chunks_mut(block_len)
                .enumerate()
            {
                items.push((base + slot as u64 * args.block, slice));
            }
            let batch = match ring.read_batch(&mut iov, &reg, &mut items).await {
                Ok(batch) => batch,
                Err(error) => {
                    return Err(teardown(
                        &client,
                        Some(reg),
                        Some(file),
                        Some(ring),
                        Some(iov),
                        error.into(),
                    )
                    .await);
                }
            };
            if batch.iter().any(|size| *size as u64 != args.block) {
                let error: BoxError = "short read in ring batch".into();
                return Err(
                    teardown(&client, Some(reg), Some(file), Some(ring), Some(iov), error).await,
                );
            }
            for slot in 0..count {
                let block_index = base / args.block + slot as u64;
                payload_block(file_index, block_index, &mut expected);
                verified &= batch_buf[slot * block_len..(slot + 1) * block_len] == expected;
            }
        }
        let io_s = io_started.elapsed().as_secs_f64();
        cleanup_sdk(&client, Some(reg), Some(file), Some(ring), Some(iov)).await?;
        Ok(ThreadStat {
            setup_s,
            io_s,
            sync_s: 0.0,
            cpu_s: thread_cpu_seconds() - cpu0,
            verified: Some(verified),
        })
    }
}

/// Cleanup errors retain the failing operation so the caller can distinguish a
/// business failure from incomplete resource retirement.
#[derive(Debug, thiserror::Error)]
#[error("SDK cleanup failed: {0}")]
struct SdkCleanupError(String);

/// Both successful I/O and business errors use the same cleanup path. Retire the
/// ring first: a failed/unknown I/O must stop using registrations and iov memory
/// before those resources can be released.
async fn cleanup_sdk(
    client: &DfsLocalClient,
    registration: Option<DfsRegistration>,
    file: Option<DfsOpenFile>,
    ring: Option<DfsRing>,
    iov: Option<DfsIov>,
) -> Result<(), SdkCleanupError> {
    let mut errors = Vec::new();
    if let Some(ring) = ring
        && let Err(error) = ring.destroy().await
    {
        // The worker may still be using these resources. Do not release its
        // registration, backend handle or iov without confirmed retirement.
        return Err(SdkCleanupError(format!(
            "ring destroy failed: {error}; registration, file and iov retained because ring retirement is unconfirmed"
        )));
    }
    if let Some(reg) = registration
        && let Err(error) = client.unregister_file(reg.clone()).await
    {
        errors.push(format!("unregister_file failed: {error}"));
        // This retries only resource retirement, never data I/O. The first RPC
        // may have failed before reaching Node; a single bounded retry also
        // lets a transient cleanup error release the backend handle below.
        if let Err(retry) = client.unregister_file(reg).await {
            errors.push(format!("unregister_file retry failed: {retry}"));
        }
    }
    if let Some(file) = file
        && let Err(error) = client.close(file.handle).await
    {
        errors.push(format!("close failed: {error}"));
    }
    if let Some(iov) = iov
        && let Err(error) = iov.destroy().await
    {
        errors.push(format!("iov destroy failed: {error}"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(SdkCleanupError(errors.join("; ")))
    }
}

/// Preserve the original business error; report cleanup failures separately.
async fn teardown(
    client: &DfsLocalClient,
    registration: Option<DfsRegistration>,
    file: Option<DfsOpenFile>,
    ring: Option<DfsRing>,
    iov: Option<DfsIov>,
    error: BoxError,
) -> BoxError {
    match cleanup_sdk(client, registration, file, ring, iov).await {
        Ok(()) => error,
        Err(cleanup) => {
            #[derive(Debug, thiserror::Error)]
            #[error("{primary}; {cleanup}")]
            struct BusinessAndCleanupError {
                #[source]
                primary: BoxError,
                cleanup: SdkCleanupError,
            }
            Box::new(BusinessAndCleanupError {
                primary: error,
                cleanup,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// 多文件并发读写 workload
// ---------------------------------------------------------------------------

struct MultiFileRw;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Write,
    Read,
}

impl Phase {
    fn label(self) -> &'static str {
        match self {
            Phase::Write => "write",
            Phase::Read => "read",
        }
    }
}

impl Workload for MultiFileRw {
    fn name(&self) -> &'static str {
        "multifile"
    }

    fn run(&self, ctx: &BenchCtx) -> Result<Vec<CaseReport>, BoxError> {
        let args = ctx.args();
        if args.mode == Mode::Mixed && args.files < 2 {
            return Err("--mode mixed requires at least 2 files".into());
        }
        // 完整文件基准的准备全部在计时外：write 集合建立精确长度（清旧尾部），
        // mixed 的只读集合写入确定性 payload，纯读模式只校验输入不动文件。
        let (write_set, read_set) = mixed_partition(args.files, args.mode);
        if args.mode == Mode::Read {
            FuseIo::verify_input_files(args)?;
        } else {
            FuseIo::ensure_files(args, &write_set)?;
            FuseIo::prepare_read_set(args, &read_set)?;
        }
        let mut reports = Vec::new();
        for arm in args.arm.labels() {
            match args.mode {
                Mode::Write => reports.push(run_rounds(ctx, arm, Phase::Write)?),
                Mode::Read => reports.push(run_rounds(ctx, arm, Phase::Read)?),
                Mode::Rw => {
                    reports.push(run_rounds(ctx, arm, Phase::Write)?);
                    reports.push(run_rounds(ctx, arm, Phase::Read)?);
                }
                Mode::Mixed => reports.extend(run_mixed_rounds(ctx, arm)?),
            }
        }
        Ok(reports)
    }
}

/// 一个 (phase, file 集合) 任务；同任务内文件串行，跨任务并发。
#[derive(Clone)]
struct Job {
    phase: Phase,
    files: Vec<usize>,
}

fn run_rounds(ctx: &BenchCtx, arm: &'static str, phase: Phase) -> Result<CaseReport, BoxError> {
    let args = ctx.args();
    let mut report = CaseReport {
        name: "multifile",
        arm,
        phase: phase.label(),
        mode: args.mode.label(),
        rounds: Vec::new(),
    };
    for round in 0..args.rounds {
        settle(ctx, &format!("{arm} {} round {}", phase.label(), round + 1))?;
        let jobs = plan_jobs(ctx, phase);
        let raw = dispatch(ctx, arm, jobs)?;
        guard_node_memory(ctx, &raw.memory)?;
        report
            .rounds
            .push(phase_stat(&raw, phase, ctx.args().file_bytes)?);
    }
    Ok(report)
}

fn run_mixed_rounds(ctx: &BenchCtx, arm: &'static str) -> Result<Vec<CaseReport>, BoxError> {
    let args = ctx.args();
    let writers = args.files.div_ceil(2);
    let mut write_report = CaseReport {
        name: "multifile",
        arm,
        phase: "write",
        mode: args.mode.label(),
        rounds: Vec::new(),
    };
    let mut read_report = CaseReport {
        name: "multifile",
        arm,
        phase: "read",
        mode: args.mode.label(),
        rounds: Vec::new(),
    };
    for round in 0..args.rounds {
        settle(ctx, &format!("{arm} mixed round {}", round + 1))?;
        let half = args.threads.div_ceil(2).max(1);
        let write_files: Vec<usize> = (0..writers).collect();
        let read_files: Vec<usize> = (writers..args.files).collect();
        let mut jobs: Vec<Job> = spread(half, &write_files)
            .into_iter()
            .map(|files| Job {
                phase: Phase::Write,
                files,
            })
            .collect();
        jobs.extend(spread(half, &read_files).into_iter().map(|files| Job {
            phase: Phase::Read,
            files,
        }));
        let raw = dispatch(ctx, arm, jobs)?;
        guard_node_memory(ctx, &raw.memory)?;
        write_report
            .rounds
            .push(phase_stat(&raw, Phase::Write, ctx.args().file_bytes)?);
        read_report
            .rounds
            .push(phase_stat(&raw, Phase::Read, ctx.args().file_bytes)?);
    }
    Ok(vec![write_report, read_report])
}

fn plan_jobs(ctx: &BenchCtx, phase: Phase) -> Vec<Job> {
    let args = ctx.args();
    let files: Vec<usize> = (0..args.files).collect();
    spread(args.threads, &files)
        .into_iter()
        .map(|files| Job { phase, files })
        .collect()
}

fn spread(threads: usize, files: &[usize]) -> Vec<Vec<usize>> {
    let count = threads.min(files.len()).max(1);
    let mut groups: Vec<Vec<usize>> = vec![Vec::new(); count];
    for (position, file) in files.iter().enumerate() {
        groups[position % count].push(*file);
    }
    groups
}

struct RawRun {
    wall_s: f64,
    node_cpu_s: f64,
    meta_cpu_s: f64,
    memory: MemoryObservation,
    memory_source: String,
    results: DispatchResults,
}

fn dispatch(ctx: &BenchCtx, arm: &'static str, jobs: Vec<Job>) -> Result<RawRun, BoxError> {
    let mem_sampler = MemorySampler::start(ctx.memory_target.clone());
    let node_cpu0 = pids_cpu_seconds(&ctx.node_pids);
    let meta_cpu0 = pids_cpu_seconds(&ctx.meta_pids);
    let started = Instant::now();
    let results = if arm == "fuse" {
        Ok(dispatch_fuse(ctx, jobs))
    } else {
        dispatch_sdk(ctx, jobs)
    };
    // 业务完成即冻结相位 wall；采样线程的回收等待不得延长测量窗口（R132-01）。
    let wall_s = started.elapsed().as_secs_f64();
    let node_cpu_s = pids_cpu_seconds(&ctx.node_pids) - node_cpu0;
    let meta_cpu_s = pids_cpu_seconds(&ctx.meta_pids) - meta_cpu0;
    let memory = mem_sampler.stop();
    Ok(RawRun {
        wall_s,
        node_cpu_s,
        meta_cpu_s,
        memory,
        memory_source: ctx.memory_target.describe(),
        results: results?,
    })
}

/// 节点内存观测目标。RSS 之和与 cgroup memory.current 是不同口径的指标，
/// 不可静默互换；观测不可达时返回 None，调用方不得以 0 冒充观测值。
#[derive(Clone)]
enum MemoryTarget {
    /// 每个 afs-node 进程的驻留内存（/proc/<pid>/statm resident）之和。
    NodeRssSum { pids: Vec<u32> },
    /// 显式给出的节点 cgroup 根目录（读其 memory.current）。
    NodeCgroup { path: PathBuf },
}

impl MemoryTarget {
    fn describe(&self) -> String {
        match self {
            MemoryTarget::NodeRssSum { pids } => {
                format!("node-rss-sum over afs-node pids {pids:?}")
            }
            MemoryTarget::NodeCgroup { path } => {
                format!("cgroup memory.current under {}", path.display())
            }
        }
    }

    fn sample(&self) -> Option<u64> {
        match self {
            MemoryTarget::NodeCgroup { path } => {
                std::fs::read_to_string(path.join("memory.current"))
                    .ok()?
                    .trim()
                    .parse::<u64>()
                    .ok()
            }
            MemoryTarget::NodeRssSum { pids } => {
                let page = page_size_bytes()?;
                if pids.is_empty() {
                    return None;
                }
                let mut total: u64 = 0;
                for pid in pids {
                    // An unreadable member makes the selected sum incomplete;
                    // do not silently substitute the sum of remaining PIDs.
                    let statm = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
                    let resident = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
                    total = total.checked_add(resident.checked_mul(page)?)?;
                }
                Some(total)
            }
        }
    }
}

fn page_size_bytes() -> Option<u64> {
    // SAFETY: sysconf(_SC_PAGESIZE) 只读查询一页的字节数，无内存写入或
    // 全局状态修改；返回值 <= 0 视为失败，不会作为页大小使用。
    let value = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if value <= 0 { None } else { Some(value as u64) }
}

/// A sampled maximum and its coverage are separate facts: a positive old peak
/// cannot prove that a later failed observation remained below the limit.
#[derive(Debug, Default, Clone, Copy)]
struct MemoryObservation {
    peak_bytes: Option<u64>,
    complete: bool,
}

/// Take the first observation before dispatch, then sample every 100ms. A
/// wakeable wait keeps sampler retirement out of the business interval.
struct MemorySampler {
    peak: std::sync::Arc<std::sync::atomic::AtomicU64>,
    observed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    failed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    stop: std::sync::Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl MemorySampler {
    fn start(target: MemoryTarget) -> Self {
        let initial = target.sample();
        let peak = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(initial.unwrap_or(0)));
        let observed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(initial.is_some()));
        let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(initial.is_none()));
        let stop = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let sampler_peak = peak.clone();
        let sampler_observed = observed.clone();
        let sampler_failed = failed.clone();
        let sampler_stop = stop.clone();
        let handle = std::thread::spawn(move || {
            let (lock, signal) = &*sampler_stop;
            let mut stopped = lock.lock().unwrap_or_else(|poison| poison.into_inner());
            while !*stopped {
                match target.sample() {
                    Some(value) => {
                        sampler_peak.fetch_max(value, std::sync::atomic::Ordering::Relaxed);
                        sampler_observed.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    None => sampler_failed.store(true, std::sync::atomic::Ordering::Relaxed),
                }
                let (guard, _) = signal
                    .wait_timeout(stopped, SAMPLE_INTERVAL)
                    .unwrap_or_else(|poison| poison.into_inner());
                stopped = guard;
            }
        });
        Self {
            peak,
            observed,
            failed,
            stop,
            handle: Some(handle),
        }
    }

    fn stop(mut self) -> MemoryObservation {
        {
            let (lock, signal) = &*self.stop;
            let mut stopped = lock.lock().unwrap_or_else(|poison| poison.into_inner());
            *stopped = true;
            signal.notify_all();
        }
        if let Some(handle) = self.handle.take()
            && handle.join().is_err()
        {
            self.failed
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        MemoryObservation {
            peak_bytes: self
                .observed
                .load(std::sync::atomic::Ordering::Relaxed)
                .then(|| self.peak.load(std::sync::atomic::Ordering::Relaxed)),
            complete: !self.failed.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum MemoryGuardError {
    #[error("--max-node-mem overflows the supported byte range")]
    LimitOverflow,
    #[error(
        "node memory observation is incomplete or unavailable while --max-node-mem is set; refusing to continue without the guard"
    )]
    Unobservable,
    #[error(
        "node memory peak {peak_mib:.0} MiB exceeds --max-node-mem {limit_mib} MiB; aborting before the next phase"
    )]
    Exceeded { peak_mib: f64, limit_mib: u64 },
}

fn guard_node_memory(ctx: &BenchCtx, memory: &MemoryObservation) -> Result<(), MemoryGuardError> {
    let limit = ctx
        .args
        .max_node_mem_mib
        .checked_mul(1024 * 1024)
        .ok_or(MemoryGuardError::LimitOverflow)?;
    if limit == 0 {
        return Ok(());
    }
    let peak_bytes = memory
        .peak_bytes
        .filter(|_| memory.complete)
        .ok_or(MemoryGuardError::Unobservable)?;
    if peak_bytes > limit {
        return Err(MemoryGuardError::Exceeded {
            peak_mib: peak_bytes as f64 / (1024.0 * 1024.0),
            limit_mib: ctx.args.max_node_mem_mib,
        });
    }
    Ok(())
}

fn phase_stat(raw: &RawRun, phase: Phase, file_bytes: u64) -> Result<PhaseStat, BoxError> {
    let mut stat = PhaseStat {
        wall_s: raw.wall_s,
        node_cpu_s: raw.node_cpu_s,
        meta_cpu_s: raw.meta_cpu_s,
        node_mem_peak_bytes: raw.memory.peak_bytes,
        node_mem_observation_complete: raw.memory.complete,
        node_mem_source: raw.memory_source.clone(),
        ..Default::default()
    };
    let mut verified: Option<bool> = None;
    for (job, result) in &raw.results {
        if job.phase != phase {
            continue;
        }
        stat.bytes += job.files.len() as u64 * file_bytes;
        let thread_stat = match result {
            Ok(stat) => stat.clone(),
            Err(text) => return Err(text.clone().into()),
        };
        if let Some(flag) = thread_stat.verified {
            verified = Some(verified.unwrap_or(true) && flag);
        }
        stat.threads.push(thread_stat);
    }
    stat.throughput_bytes_per_s = if stat.wall_s > 0.0 {
        stat.bytes as f64 / stat.wall_s
    } else {
        0.0
    };
    stat.verified = verified;
    Ok(stat)
}

fn dispatch_fuse(ctx: &BenchCtx, jobs: Vec<Job>) -> DispatchResults {
    let handles: Vec<_> =
        jobs.into_iter()
            .map(|job| {
                let args = ctx.args().clone();
                let files = job.files.clone();
                let phase = job.phase;
                let handle = std::thread::spawn(move || -> Result<ThreadStat, String> {
                    let mut merged = ThreadStat::default();
                    let mut verified: Option<bool> = None;
                    for file in files {
                        let stat =
                            match phase {
                                Phase::Write => FuseIo::write_one(&args, file)
                                    .map_err(|error| error.to_string())?,
                                Phase::Read => FuseIo::read_one(&args, file)
                                    .map_err(|error| error.to_string())?,
                            };
                        merged.setup_s += stat.setup_s;
                        merged.io_s += stat.io_s;
                        merged.sync_s += stat.sync_s;
                        merged.cpu_s += stat.cpu_s;
                        if let Some(flag) = stat.verified {
                            verified = Some(verified.unwrap_or(true) && flag);
                        }
                    }
                    merged.verified = verified;
                    Ok(merged)
                });
                (job, handle)
            })
            .collect();
    handles
        .into_iter()
        .map(|(job, handle)| {
            let result = handle
                .join()
                .unwrap_or_else(|_| Err("fuse worker panicked".to_string()));
            (job, result)
        })
        .collect()
}

fn dispatch_sdk(ctx: &BenchCtx, jobs: Vec<Job>) -> Result<DispatchResults, BoxError> {
    let args = ctx.args();
    let sock = args
        .sock
        .clone()
        .ok_or_else(|| "--sock is required for the sdk arm".to_string())?;
    let worker_threads = jobs.len().clamp(1, 8);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?;
    let mut tasks = Vec::new();
    for job in jobs {
        let args = args.clone();
        let sock = sock.clone();
        let phase = job.phase;
        let files = job.files.clone();
        tasks.push((
            job,
            runtime.spawn(async move {
                let mut merged = ThreadStat::default();
                let mut verified: Option<bool> = None;
                for file in files {
                    let stat = match phase {
                        Phase::Write => SdkIo::write_one(&args, &sock, file)
                            .await
                            .map_err(|error| error.to_string())?,
                        Phase::Read => SdkIo::read_one(&args, &sock, file)
                            .await
                            .map_err(|error| error.to_string())?,
                    };
                    merged.setup_s += stat.setup_s;
                    merged.io_s += stat.io_s;
                    merged.sync_s += stat.sync_s;
                    merged.cpu_s += stat.cpu_s;
                    if let Some(flag) = stat.verified {
                        verified = Some(verified.unwrap_or(true) && flag);
                    }
                }
                merged.verified = verified;
                Ok::<ThreadStat, String>(merged)
            }),
        ));
    }
    let mut results = Vec::new();
    for (job, task) in tasks {
        let outcome = runtime.block_on(task);
        let result = match outcome {
            Ok(Ok(stat)) => Ok(stat),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(format!("sdk worker join failed: {error}")),
        };
        results.push((job, result));
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// 输出
// ---------------------------------------------------------------------------

fn json_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}

fn emit_report(identity: &EnvIdentity, reports: &[CaseReport]) -> String {
    let mut out = String::new();
    out.push_str("{\n  \"tool\": \"afs-bench\",\n  \"schema\": 2,\n");
    out.push_str(&format!(
        "  \"env\": {{\"host\": \"{}\", \"kernel\": \"{}\", \"arch\": \"{}\", \"nproc\": {}, \"argv\": \"{}\"}},\n",
        json_escape(&identity.host),
        json_escape(&identity.kernel),
        identity.arch,
        identity.nproc,
        json_escape(&identity.argv)
    ));
    out.push_str("  \"cases\": [\n");
    for (position, report) in reports.iter().enumerate() {
        out.push_str(&format!(
            "    {{\"name\": \"{}\", \"arm\": \"{}\", \"phase\": \"{}\", \"mode\": \"{}\", \"median_throughput_bytes_per_s\": {:.1},\n",
            report.name,
            report.arm,
            report.phase,
            report.mode,
            report.median_throughput()
        ));
        out.push_str("     \"rounds\": [\n");
        for (round_position, round) in report.rounds.iter().enumerate() {
            let threads: Vec<String> = round
                .threads
                .iter()
                .map(|thread| {
                    format!(
                        "{{\"setup_s\": {:.6}, \"io_s\": {:.6}, \"sync_s\": {:.6}, \"cpu_s\": {:.6}, \"verified\": {}}}",
                        thread.setup_s,
                        thread.io_s,
                        thread.sync_s,
                        thread.cpu_s,
                        thread.verified.map(|flag| flag.to_string()).unwrap_or_else(|| "null".into())
                    )
                })
                .collect();
            out.push_str(&format!(
                "       {{\"wall_s\": {:.6}, \"bytes\": {}, \"throughput_bytes_per_s\": {:.1}, \"node_cpu_s\": {:.3}, \"meta_cpu_s\": {:.3}, \"node_mem_peak_bytes\": {}, \"node_mem_observation_complete\": {}, \"node_mem_source\": \"{}\", \"verified\": {}, \"threads\": [{}]}}{}\n",
                round.wall_s,
                round.bytes,
                round.throughput_bytes_per_s,
                round.node_cpu_s,
                round.meta_cpu_s,
                round.node_mem_peak_bytes.map(|value| value.to_string()).unwrap_or_else(|| "null".into()),
                round.node_mem_observation_complete,
                json_escape(&round.node_mem_source),
                round.verified.map(|flag| flag.to_string()).unwrap_or_else(|| "null".into()),
                threads.join(", "),
                if round_position + 1 < report.rounds.len() { "," } else { "" }
            ));
        }
        out.push_str(&format!(
            "     ]\n    }}{}\n",
            if position + 1 < reports.len() {
                ","
            } else {
                ""
            }
        ));
    }
    out.push_str("  ]\n}\n");
    out
}

fn print_human(reports: &[CaseReport]) {
    println!(
        "{:<12} {:<6} {:<7} {:<7} {:>10} {:>8} {:>9} {:>8} {:>9}",
        "case", "arm", "mode", "phase", "MiB/s", "wall_s", "node_cpu", "mem_MiB", "verified"
    );
    for report in reports {
        let throughput = report.median_throughput() / (1024.0 * 1024.0);
        let wall: f64 = report.rounds.iter().map(|round| round.wall_s).sum();
        let node_cpu: f64 = report.rounds.iter().map(|round| round.node_cpu_s).sum();
        let has_fail = report
            .rounds
            .iter()
            .any(|round| round.verified == Some(false));
        let has_pass = report
            .rounds
            .iter()
            .any(|round| round.verified == Some(true));
        let verified = if has_fail {
            "FAIL"
        } else if has_pass {
            "ok"
        } else {
            "-"
        };
        let peak = report
            .rounds
            .iter()
            .filter_map(|round| round.node_mem_peak_bytes)
            .max();
        let complete = report
            .rounds
            .iter()
            .all(|round| round.node_mem_observation_complete);
        let mem_mib = match peak {
            Some(bytes) => format!(
                "{:.0}{}",
                bytes as f64 / (1024.0 * 1024.0),
                if complete { "" } else { "?" }
            ),
            None => "-".into(),
        };
        println!(
            "{:<12} {:<6} {:<7} {:<7} {:>10.1} {:>8.2} {:>9.2} {:>9} {:>8}",
            report.name,
            report.arm,
            report.mode,
            report.phase,
            throughput,
            wall,
            node_cpu,
            mem_mib,
            verified
        );
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse_args(&argv) {
        Ok(args) => args,
        Err(error) if error.is_empty() => {
            println!("{}", usage());
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("afs-bench: {error}\n\n{}", usage());
            return ExitCode::from(2);
        }
    };
    if let Err(error) = run(&argv, args) {
        eprintln!("afs-bench: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run(argv: &[String], args: Args) -> Result<(), BoxError> {
    if args.arm != Arm::Fuse && args.sock.is_none() {
        return Err("--sock is required when the sdk arm is enabled".into());
    }
    let identity = EnvIdentity::capture(argv);
    let node_pids = proc_pids_by_name("afs-node");
    let meta_pids = proc_pids_by_name("afs-meta");
    let memory_target = match &args.node_cgroup {
        Some(path) => MemoryTarget::NodeCgroup { path: path.clone() },
        None => MemoryTarget::NodeRssSum {
            pids: node_pids.clone(),
        },
    };
    eprintln!(
        "[afs-bench] host={} kernel={} files={} file_bytes={} block={} threads={} rounds={} node_pids={} meta_pids={}",
        identity.host,
        identity.kernel,
        args.files,
        args.file_bytes,
        args.block,
        args.threads,
        args.rounds,
        node_pids.len(),
        meta_pids.len()
    );
    eprintln!("[afs-bench] mem source={}", memory_target.describe());
    // 内存保护启用时必须先证明目标可观测：观测不到就放行等于没有保护（R132-05）。
    if args.max_node_mem_mib > 0 && memory_target.sample().is_none() {
        return Err(
            "node memory is not observable (no afs-node process found and no --node-cgroup given); \
             --max-node-mem requires an observable target"
                .into(),
        );
    }
    let ctx = BenchCtx {
        args,
        node_pids,
        meta_pids,
        memory_target,
    };
    let mut reports = Vec::new();
    for workload in registry() {
        eprintln!("[afs-bench] workload {}", workload.name());
        reports.extend(workload.run(&ctx)?);
    }
    // 先出全部结果再定成败：失败时也保留可用 JSON、失败状态与已完成轮次（R132-02）。
    print_human(&reports);
    if let Some(path) = &ctx.args().json {
        let mut file = File::create(path)?;
        file.write_all(emit_report(&identity, &reports).as_bytes())?;
        eprintln!("[afs-bench] json written to {}", path.display());
    }
    let corrupted: Vec<&CaseReport> = reports
        .iter()
        .filter(|report| report.has_corruption())
        .collect();
    if corrupted.is_empty() {
        if !ctx.args().keep {
            cleanup(ctx.args())?;
        }
        return Ok(());
    }
    if !ctx.args().keep {
        eprintln!(
            "[afs-bench] content verification failed in {} case(s); bench files kept under {} for diagnosis",
            corrupted.len(),
            ctx.args().mountpoint.display()
        );
    }
    Err(format!(
        "content verification failed (verified=false in {} case(s)); results are recorded above",
        corrupted.len()
    )
    .into())
}

fn cleanup(args: &Args) -> Result<(), BoxError> {
    for file_index in 0..args.files {
        let path = bench_file(&args.mountpoint, file_index);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_is_deterministic_per_file_and_block() {
        let mut left = vec![0u8; 4096];
        let mut right = vec![0u8; 4096];
        payload_block(3, 7, &mut left);
        payload_block(3, 7, &mut right);
        assert_eq!(left, right);
        payload_block(3, 8, &mut right);
        assert_ne!(left, right);
        payload_block(4, 7, &mut right);
        assert_ne!(left, right);
    }

    #[test]
    fn payload_is_not_degenerate() {
        let mut block = vec![0u8; 4096];
        payload_block(0, 0, &mut block);
        assert!(block.iter().any(|byte| *byte != 0));
        assert!(block.iter().any(|byte| *byte != 0xFF));
        let mut other = vec![0u8; 4096];
        payload_block(1, 0, &mut other);
        let differing = block
            .iter()
            .zip(other.iter())
            .filter(|(a, b)| a != b)
            .count();
        assert!(
            differing > 4096 / 4,
            "adjacent files should differ substantially, got {differing}"
        );
    }

    #[test]
    fn parse_size_accepts_units() {
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert_eq!(parse_size("4k").unwrap(), 4096);
        assert_eq!(parse_size("4M").unwrap(), 4 * 1024 * 1024);
        assert_eq!(parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert!(parse_size("1x").is_err());
        assert!(parse_size("abc").is_err());
    }

    #[test]
    fn parse_args_requires_mountpoint_and_validates() {
        let build = |args: &[&str]| {
            parse_args(&args.iter().map(|text| text.to_string()).collect::<Vec<_>>())
        };
        assert!(build(&["/mnt"]).is_ok());
        assert!(build(&[]).is_err());
        assert!(
            build(&["--sock", "/tmp/s.sock", "/mnt"])
                .unwrap()
                .sock
                .is_some()
        );
        assert_eq!(build(&["/mnt", "--arm", "fuse"]).unwrap().arm, Arm::Fuse);
        assert!(build(&["/mnt", "--arm", "nope"]).is_err());
        assert!(build(&["/mnt", "--file-bytes", "1M", "--block", "4097"]).is_err());
        assert!(build(&["/mnt", "--threads", "0"]).is_err());
    }

    #[test]
    fn median_picks_middle() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[4.0, 2.0, 1.0, 3.0]), 3.0);
    }

    #[test]
    fn json_escape_handles_specials() {
        assert_eq!(json_escape("a\"b\\c\n"), "a\\\"b\\\\c\\n");
    }

    #[test]
    fn emit_report_contains_case_fields() {
        let report = CaseReport {
            name: "multifile",
            arm: "fuse",
            phase: "write",
            mode: "rw",
            rounds: vec![PhaseStat {
                wall_s: 1.0,
                bytes: 1024,
                throughput_bytes_per_s: 1024.0,
                threads: vec![ThreadStat::default()],
                ..Default::default()
            }],
        };
        let identity = EnvIdentity {
            host: "h".into(),
            kernel: "k".into(),
            arch: "x".into(),
            nproc: 1,
            argv: "a".into(),
        };
        let json = emit_report(&identity, &[report]);
        assert!(json.contains("\"name\": \"multifile\""));
        assert!(json.contains("\"median_throughput_bytes_per_s\""));
        assert!(json.contains("\"host\": \"h\""));
    }

    #[test]
    fn spread_groups_all_files() {
        let groups = spread(4, &[0, 1, 2, 3, 4, 5]);
        let total: usize = groups.iter().map(|group| group.len()).sum();
        assert_eq!(total, 6);
        assert!(
            groups
                .iter()
                .all(|group| !group.is_empty() || groups.len() == 1)
        );
    }
}

// Reviewer counterexamples only; production implementation remains unchanged.
#[cfg(test)]
mod review132_regressions {
    use super::*;

    fn args(root: &Path, mode: &str) -> Args {
        parse_args(&[
            root.display().to_string(),
            "--arm".into(),
            "fuse".into(),
            "--mode".into(),
            mode.into(),
            "--files".into(),
            "1".into(),
            "--file-bytes".into(),
            "4096".into(),
            "--block".into(),
            "4096".into(),
            "--threads".into(),
            "1".into(),
            "--rounds".into(),
            "1".into(),
            "--settle-secs".into(),
            "0".into(),
        ])
        .unwrap()
    }

    #[test]
    fn review132_reject_zero_file_bytes() {
        let argv = ["/mnt", "--arm", "fuse", "--file-bytes", "0"].map(str::to_owned);
        assert!(
            parse_args(&argv).is_err(),
            "zero bytes must not become verified=true with no reads"
        );
    }

    #[test]
    fn review132_reject_q_narrowing() {
        let argv = ["/mnt", "--sock", "/tmp/node.sock", "--q", "4294967297"].map(str::to_owned);
        assert!(
            parse_args(&argv).is_err(),
            "q must fit the SDK u32 block-count before casting"
        );
    }

    #[test]
    fn review132_write_establishes_requested_file_length() {
        let tmp = tempfile::tempdir().unwrap();
        let args = args(tmp.path(), "rw");
        std::fs::write(bench_file(tmp.path(), 0), vec![0xA5; 8192]).unwrap();
        FuseIo::ensure_files(&args, &[0]).unwrap();
        FuseIo::write_one(&args, 0).unwrap();
        let stat = FuseIo::read_one(&args, 0).unwrap();
        assert_eq!(stat.verified, Some(true));
        assert_eq!(
            std::fs::metadata(bench_file(tmp.path(), 0)).unwrap().len(),
            args.file_bytes,
            "verified prefix hides an old tail outside the advertised file size"
        );
    }

    #[test]
    fn review132_corruption_must_fail_run_and_preserve_file() {
        let tmp = tempfile::tempdir().unwrap();
        let args = args(tmp.path(), "read");
        std::fs::write(bench_file(tmp.path(), 0), vec![0xA5; 4096]).unwrap();
        let result = run(&[], args);
        assert!(
            result.is_err(),
            "verified=false is a failed run, not success"
        );
        assert!(
            bench_file(tmp.path(), 0).exists(),
            "preserve the corrupt file for diagnosis"
        );
    }

    #[test]
    fn review132_fresh_mixed_must_prepare_read_set() {
        let tmp = tempfile::tempdir().unwrap();
        let mut args = args(tmp.path(), "mixed");
        args.files = 4;
        args.threads = 4;
        let ctx = BenchCtx {
            args,
            node_pids: vec![],
            meta_pids: vec![],
            memory_target: MemoryTarget::NodeRssSum { pids: vec![] },
        };
        let result = MultiFileRw.run(&ctx);
        assert!(
            result.is_ok(),
            "fresh mixed should initialize read-side payload before timing: {result:?}"
        );
    }

    #[test]
    fn review132_sampler_stop_must_not_add_sleep_to_phase_wall() {
        let sampler = MemorySampler::start(MemoryTarget::NodeRssSum {
            pids: vec![std::process::id()],
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while sampler.peak.load(std::sync::atomic::Ordering::Relaxed) == 0
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            sampler.peak.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "Linux sample required"
        );
        let start = Instant::now();
        sampler.stop();
        assert!(
            start.elapsed() < Duration::from_millis(25),
            "stopping metrics adds nearly 100ms to dispatch.wall_s: {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn review132_verification_false_survives_all_fuse_merges_control() {
        let tmp = tempfile::tempdir().unwrap();
        let mut args = args(tmp.path(), "rw");
        args.files = 4;
        args.threads = 2;
        FuseIo::ensure_files(&args, &(0..args.files).collect::<Vec<_>>()).unwrap();
        for index in 0..args.files {
            FuseIo::write_one(&args, index).unwrap();
        }
        File::options()
            .write(true)
            .open(bench_file(tmp.path(), 0))
            .unwrap()
            .write_all_at(&[0xA5], 0)
            .unwrap();
        let ctx = BenchCtx {
            args,
            node_pids: vec![],
            meta_pids: vec![],
            memory_target: MemoryTarget::NodeRssSum { pids: vec![] },
        };
        let raw = dispatch(&ctx, "fuse", plan_jobs(&ctx, Phase::Read)).unwrap();
        let stat = phase_stat(&raw, Phase::Read, 4096).unwrap();
        assert_eq!(stat.verified, Some(false));
        assert_eq!(stat.bytes, 4 * 4096);
    }

    #[test]
    fn review132_mixed_denominators_are_per_phase_control() {
        let raw = RawRun {
            wall_s: 2.0,
            node_cpu_s: 0.0,
            meta_cpu_s: 0.0,
            memory: MemoryObservation {
                peak_bytes: None,
                complete: false,
            },
            memory_source: "unavailable test source".into(),
            results: vec![
                (
                    Job {
                        phase: Phase::Write,
                        files: vec![0, 1, 2],
                    },
                    Ok(ThreadStat::default()),
                ),
                (
                    Job {
                        phase: Phase::Read,
                        files: vec![3, 4],
                    },
                    Ok(ThreadStat {
                        verified: Some(true),
                        ..Default::default()
                    }),
                ),
            ],
        };
        let write = phase_stat(&raw, Phase::Write, 4096).unwrap();
        let read = phase_stat(&raw, Phase::Read, 4096).unwrap();
        assert_eq!(write.bytes, 3 * 4096);
        assert_eq!(read.bytes, 2 * 4096);
        assert_eq!(
            write.throughput_bytes_per_s + read.throughput_bytes_per_s,
            5.0 * 4096.0 / 2.0
        );
        assert_eq!(write.verified, None);
        assert_eq!(read.verified, Some(true));
    }
}

// 修复方补充的针对性回归：R132-01…09 的合同面。
#[cfg(test)]
mod fix_regressions {
    use super::*;

    fn ctx_with_mem_limit(root: &Path, max_node_mem_mib: u64) -> BenchCtx {
        let args = parse_args(&[
            root.display().to_string(),
            "--arm".into(),
            "fuse".into(),
            "--file-bytes".into(),
            "4096".into(),
            "--block".into(),
            "4096".into(),
            "--max-node-mem".into(),
            max_node_mem_mib.to_string(),
        ])
        .unwrap();
        BenchCtx {
            args,
            node_pids: vec![],
            meta_pids: vec![],
            memory_target: MemoryTarget::NodeRssSum { pids: vec![] },
        }
    }

    #[test]
    fn gc_quiet_source_distinguishes_outcomes() {
        let poll = Duration::from_millis(1);
        let budget = Duration::from_millis(30);
        // 静止源：观察到安静。
        let quiet = wait_gc_quiet_source(budget, poll, || Ok(7)).unwrap();
        assert!(matches!(quiet, GcQuiet::Quiet { polls: 1 }));
        // 持续增长：预算耗尽，不冒充安静。
        let mut growing = 0u64;
        let exhausted = wait_gc_quiet_source(budget, poll, move || {
            growing += 1;
            Ok(growing * 100)
        })
        .unwrap();
        assert!(matches!(exhausted, GcQuiet::BudgetExhausted));
        // 轮转/回缩：重置基线，只有在回缩后再次观察到相等才算安静。
        let sequence = [10u64, 3, 3];
        let mut position = 0;
        let rotated = wait_gc_quiet_source(budget, poll, move || {
            let value = sequence[position.min(sequence.len() - 1)];
            position += 1;
            Ok(value)
        })
        .unwrap();
        assert!(
            matches!(rotated, GcQuiet::Quiet { polls: 2 }),
            "rotation resets the baseline instead of counting as quiet"
        );
        // 观察失败：错误，不是安静。
        let broken = wait_gc_quiet_source(budget, poll, || Err("log vanished".to_string()));
        assert!(broken.is_err());
    }

    #[test]
    fn wait_gc_quiet_missing_log_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("absent-node.log");
        let outcome = wait_gc_quiet(
            &missing,
            Duration::from_millis(20),
            Duration::from_millis(1),
        );
        assert!(
            outcome.is_err(),
            "an unreadable GC log is an observation failure, not quiet"
        );
    }

    #[test]
    fn mixed_partition_covers_every_file_exactly_once() {
        for (files, mode) in [
            (2, Mode::Mixed),
            (3, Mode::Mixed),
            (7, Mode::Mixed),
            (4, Mode::Rw),
            (4, Mode::Write),
            (4, Mode::Read),
        ] {
            let (write_set, read_set) = mixed_partition(files, mode);
            let mut covered = write_set.clone();
            covered.extend(read_set.iter().copied());
            covered.sort();
            assert_eq!(covered, (0..files).collect::<Vec<_>>(), "mode {mode:?}");
            match mode {
                Mode::Read => assert!(write_set.is_empty()),
                Mode::Mixed => assert_eq!(write_set.len(), files.div_ceil(2)),
                Mode::Rw | Mode::Write => assert!(read_set.is_empty()),
            }
        }
    }

    #[test]
    fn read_mode_rejects_wrong_sized_or_missing_inputs() {
        let tmp = tempfile::tempdir().unwrap();
        let args = parse_args(&[
            tmp.path().display().to_string(),
            "--arm".into(),
            "fuse".into(),
            "--mode".into(),
            "read".into(),
            "--files".into(),
            "2".into(),
            "--file-bytes".into(),
            "4096".into(),
            "--block".into(),
            "4096".into(),
        ])
        .unwrap();
        // 缺失输入：报错。
        assert!(FuseIo::verify_input_files(&args).is_err());
        // 精确尺寸：通过。
        std::fs::write(bench_file(tmp.path(), 0), vec![0u8; 4096]).unwrap();
        std::fs::write(bench_file(tmp.path(), 1), vec![0u8; 4096]).unwrap();
        assert!(FuseIo::verify_input_files(&args).is_ok());
        // 尺寸不符：报错，且不修改输入。
        std::fs::write(bench_file(tmp.path(), 1), vec![0u8; 8192]).unwrap();
        assert!(FuseIo::verify_input_files(&args).is_err());
        assert_eq!(
            std::fs::metadata(bench_file(tmp.path(), 1)).unwrap().len(),
            8192,
            "read mode must not modify its inputs"
        );
    }

    #[test]
    fn parse_rejects_oversized_batch_and_caps_q() {
        let oversized = [
            "/mnt",
            "--sock",
            "/tmp/s.sock",
            "--q",
            "512",
            "--block",
            "4M",
        ]
        .map(str::to_owned);
        assert!(
            parse_args(&oversized).is_err(),
            "512 x 4MiB exceeds the supported batch size"
        );
        let boundary = [
            "/mnt",
            "--sock",
            "/tmp/s.sock",
            "--q",
            "256",
            "--block",
            "4M",
        ]
        .map(str::to_owned);
        assert!(
            parse_args(&boundary).is_ok(),
            "256 x 4MiB is the cap itself"
        );
    }

    #[test]
    fn parse_accepts_minimal_nonzero_config() {
        let argv = [
            "/mnt",
            "--arm",
            "fuse",
            "--files",
            "1",
            "--file-bytes",
            "4096",
            "--block",
            "4096",
            "--threads",
            "1",
            "--q",
            "1",
            "--rounds",
            "1",
        ]
        .map(str::to_owned);
        assert_eq!(parse_args(&argv).unwrap().file_bytes, 4096);
        let zero_bytes = ["/mnt", "--file-bytes", "0"].map(str::to_owned);
        assert!(parse_args(&zero_bytes).is_err());
        let zero_block = ["/mnt", "--file-bytes", "4096", "--block", "0"].map(str::to_owned);
        assert!(parse_args(&zero_block).is_err());
    }

    #[test]
    fn memory_target_rss_sum_reports_self_and_unavailable() {
        // 可观测：自身进程的驻留内存之和。
        let target = MemoryTarget::NodeRssSum {
            pids: vec![std::process::id()],
        };
        assert!(target.sample().unwrap_or(0) > 0, "Linux statm required");
        // 空目标：不可观测是 None，不是 0。
        let unavailable = MemoryTarget::NodeRssSum { pids: vec![] };
        assert!(unavailable.sample().is_none());
        // cgroup 路径缺失：不可观测。
        let missing = MemoryTarget::NodeCgroup {
            path: PathBuf::from("/nonexistent-mr132-cgroup"),
        };
        assert!(missing.sample().is_none());
    }

    #[test]
    fn guard_node_memory_requires_observable_target_when_armed() {
        let tmp = tempfile::tempdir().unwrap();
        // 启用保护但观测不到（peak=0）：拒绝继续，不得声称保护有效。
        let armed = ctx_with_mem_limit(tmp.path(), 16);
        assert!(guard_node_memory(&armed, &MemoryObservation::default()).is_err());
        // 低峰值通过，超峰值拒绝。
        assert!(
            guard_node_memory(
                &armed,
                &MemoryObservation {
                    peak_bytes: Some(8 * 1024 * 1024),
                    complete: true
                }
            )
            .is_ok()
        );
        assert!(
            guard_node_memory(
                &armed,
                &MemoryObservation {
                    peak_bytes: Some(32 * 1024 * 1024),
                    complete: true
                }
            )
            .is_err()
        );
        // 未启用：不做检查。
        let unarmed = ctx_with_mem_limit(tmp.path(), 0);
        assert!(guard_node_memory(&unarmed, &MemoryObservation::default()).is_ok());
    }
}

#[cfg(test)]
mod review132v2 {
    use super::*;

    fn args(root: &Path, mode: &str) -> Args {
        parse_args(&[
            root.display().to_string(),
            "--arm".into(),
            "fuse".into(),
            "--mode".into(),
            mode.into(),
            "--files".into(),
            "4".into(),
            "--file-bytes".into(),
            "4096".into(),
            "--block".into(),
            "4096".into(),
            "--threads".into(),
            "4".into(),
            "--rounds".into(),
            "1".into(),
            "--settle-secs".into(),
            "0".into(),
        ])
        .unwrap()
    }

    #[test]
    fn review132v2_mixed_read_set_must_remove_old_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let args = args(tmp.path(), "mixed");
        for index in 2..4 {
            std::fs::write(bench_file(tmp.path(), index), vec![0xA5; 8192]).unwrap();
        }
        let ctx = BenchCtx {
            args,
            node_pids: vec![],
            meta_pids: vec![],
            memory_target: MemoryTarget::NodeRssSum { pids: vec![] },
        };
        let reports = MultiFileRw.run(&ctx).unwrap();
        assert_eq!(reports[1].rounds[0].verified, Some(true));
        for index in 2..4 {
            let actual = std::fs::metadata(bench_file(tmp.path(), index))
                .unwrap()
                .len();
            eprintln!("mixed reader {index}: actual_len={actual}, declared=4096, verified=true");
            assert_eq!(
                actual, 4096,
                "mixed read-set preparation must establish exact full-file length"
            );
        }
    }

    #[test]
    fn review132v2_lost_memory_observation_must_not_pass_guard() {
        let tmp = tempfile::tempdir().unwrap();
        let current = tmp.path().join("memory.current");
        std::fs::write(&current, "1048576\n").unwrap();
        let target = MemoryTarget::NodeCgroup {
            path: tmp.path().to_path_buf(),
        };
        let sampler = MemorySampler::start(target.clone());
        let deadline = Instant::now() + Duration::from_secs(2);
        while sampler.peak.load(std::sync::atomic::Ordering::Relaxed) == 0
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            sampler.peak.load(std::sync::atomic::Ordering::Relaxed),
            1048576
        );
        std::fs::remove_file(&current).unwrap();
        std::thread::sleep(SAMPLE_INTERVAL * 3);
        assert!(target.sample().is_none());
        let peak = sampler.stop();
        let mut args = args(tmp.path(), "write");
        args.max_node_mem_mib = 16;
        let ctx = BenchCtx {
            args,
            node_pids: vec![],
            meta_pids: vec![],
            memory_target: target,
        };
        eprintln!("memory source vanished; retained observation={peak:?}; limit=16MiB");
        assert!(
            guard_node_memory(&ctx, &peak).is_err(),
            "a stale positive peak must not mask loss of observation while the guard is armed"
        );
    }

    #[test]
    fn review132v2_reject_memory_unit_overflow_at_cli_boundary() {
        let argv = [
            "/mnt",
            "--arm",
            "fuse",
            "--max-node-mem",
            "18446744073709551615",
        ]
        .map(str::to_owned);
        assert!(
            parse_args(&argv).is_err(),
            "overflowing MiB-to-byte conversion is a parameter error before any files are modified"
        );
    }
}

#[cfg(test)]
mod finish_regressions {
    use super::*;

    #[test]
    fn rss_sum_is_unknown_when_any_selected_pid_is_missing() {
        let target = MemoryTarget::NodeRssSum {
            pids: vec![std::process::id(), u32::MAX],
        };
        assert!(
            target.sample().is_none(),
            "a partial sum is not the selected Node RSS sum"
        );
    }

    #[test]
    fn memory_limit_accepts_last_convertible_value_and_rejects_next() {
        let limit = u64::MAX / (1024 * 1024);
        let argv = |value: u64| {
            vec![
                "/mnt".to_string(),
                "--arm".into(),
                "fuse".into(),
                "--max-node-mem".into(),
                value.to_string(),
            ]
        };
        assert!(parse_args(&argv(limit)).is_ok());
        assert!(parse_args(&argv(limit + 1)).is_err());
    }

    #[test]
    fn partial_memory_peak_is_explicit_in_json() {
        let identity = EnvIdentity {
            host: "h".into(),
            kernel: "k".into(),
            arch: "aarch64".into(),
            nproc: 1,
            argv: String::new(),
        };
        let report = CaseReport {
            name: "multifile",
            arm: "fuse",
            phase: "read",
            mode: "read",
            rounds: vec![PhaseStat {
                wall_s: 1.0,
                node_mem_peak_bytes: Some(1024),
                node_mem_observation_complete: false,
                node_mem_source: "test source".into(),
                ..Default::default()
            }],
        };
        let json = emit_report(&identity, &[report]);
        assert!(json.contains("\"schema\": 2"));
        assert!(json.contains("\"node_mem_peak_bytes\": 1024"));
        assert!(json.contains("\"node_mem_observation_complete\": false"));
        assert!(json.contains("\"node_mem_source\": \"test source\""));
        let unknown = CaseReport {
            name: "multifile",
            arm: "fuse",
            phase: "read",
            mode: "read",
            rounds: vec![PhaseStat::default()],
        };
        assert!(emit_report(&identity, &[unknown]).contains("\"node_mem_peak_bytes\": null"));
    }
}
