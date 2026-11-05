//! POSIX create/open → register_fd → shared rings → fsync → unregister → POSIX readback.
//! Run against the mounted DFS path, not a path relative to its namespace:
//! cargo run -p afs-client --example usrbio_round_trip -- /run/afs/node.sock /mnt/afs/data.bin
//! During the SDK phase do not overlap POSIX data IO or mmap. Metadata operations
//! remain POSIX; the SDK reuses the original open identity without reopening it.

use std::{os::fd::AsFd, time::Instant};

use afs_client::{DfsLocalClient, DfsLocalClientConfig};

/// iov 块大小：必须 ≥ 4096，且一次 IO 不能跨块边界（由 SDK 内部保证）。
const BLOCK_SIZE: usize = 64 * 1024;
/// iov 里放多少块，决定同时在途的数据量上限。
const BLOCKS: u32 = 32;
/// 环深度：SQ/CQ 各多少条目。
const ENTRIES: u32 = 64;
/// 一次提交多少条：批量是这套接口的核心，单条提交会浪费掉绝大部分收益。
const WAVE: usize = 16;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let socket = args.next().unwrap_or_else(|| {
        eprintln!("用法: usrbio_round_trip <node.sock> <已存在的文件路径>");
        std::process::exit(2);
    });
    let path = args.next().unwrap_or_else(|| {
        eprintln!("用法: usrbio_round_trip <node.sock> <已存在的文件路径>");
        std::process::exit(2);
    });

    let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket)).await?;
    let file = tokio::task::spawn_blocking(move || {
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
    })
    .await??;
    let mut iov = client.create_iov(BLOCK_SIZE, BLOCKS).await?;
    let mut ring = client.create_ring(&iov, ENTRIES, 0, 5_000).await?;
    let mut imported = client.register_fd(file.as_fd()).await?;

    // 准备一批互不重叠的数据：每条一个块，长度正好一块。
    let payloads: Vec<Vec<u8>> = (0..WAVE)
        .map(|index| vec![index as u8 + 1; BLOCK_SIZE])
        .collect();
    let items: Vec<(u64, &[u8])> = payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| (index as u64 * BLOCK_SIZE as u64, payload.as_slice()))
        .collect();

    // ---- 写 ----
    let started = Instant::now();
    let written = ring
        .write_batch(&mut iov, imported.registration(), &items)
        .await?;
    let write_elapsed = started.elapsed();

    let total: usize = written.iter().sum();
    println!(
        "写入 {WAVE} 条 / {:.1} KiB 用时 {:?}（{:.1} MiB/s）",
        total as f64 / 1024.0,
        write_elapsed,
        mib_per_second(total, write_elapsed),
    );

    // ---- 落盘 ----
    // 对原来的 POSIX 打开对象 fsync；SDK 持有的 dup 支持原 fd 提前关闭。
    imported.sync_all().await?;

    // ---- 读回校验 ----
    let mut reads: Vec<Vec<u8>> = (0..WAVE).map(|_| vec![0_u8; BLOCK_SIZE]).collect();
    let started = Instant::now();
    {
        let mut targets: Vec<(u64, &mut [u8])> = reads
            .iter_mut()
            .enumerate()
            .map(|(index, buffer)| (index as u64 * BLOCK_SIZE as u64, buffer.as_mut_slice()))
            .collect();
        ring.read_batch(&mut iov, imported.registration(), &mut targets)
            .await?;
    }
    let read_elapsed = started.elapsed();
    println!(
        "读回 {WAVE} 条 / {:.1} KiB 用时 {:?}（{:.1} MiB/s）",
        total as f64 / 1024.0,
        read_elapsed,
        mib_per_second(total, read_elapsed),
    );

    for (index, buffer) in reads.iter().enumerate() {
        let expected = index as u8 + 1;
        if buffer.iter().any(|byte| *byte != expected) {
            eprintln!("第 {index} 条内容与写入不一致");
            std::process::exit(1);
        }
    }
    println!("校验通过；iov 剩余可用块 {}/{}", iov.free_blocks(), BLOCKS);

    // ---- 收尾 ----
    // 顺序有意义：先撤注册，再关句柄，最后销毁环与 iov（环绑定了 iov id）。
    client.unregister_fd(&mut imported).await?;
    // 注销成功后才把数据访问交回 POSIX，并核对原 fd 看到的内容。
    tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; BLOCK_SIZE];
        for index in 0..WAVE {
            file.read_exact_at(&mut data, (index * BLOCK_SIZE) as u64)?;
            if data.iter().any(|b| *b != index as u8 + 1) {
                return Err(std::io::Error::other("POSIX readback mismatch"));
            }
        }
        Ok::<_, std::io::Error>(())
    })
    .await??;
    ring.destroy().await?;
    iov.destroy().await?;
    Ok(())
}

/// 把字节数与耗时换成 MiB/s。
fn mib_per_second(bytes: usize, elapsed: std::time::Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0)
}
